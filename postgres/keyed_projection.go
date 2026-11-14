package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	eventstore "github.com/DenBeke/collie"
	"github.com/DenBeke/collie/keyedprojection"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// KeyedProjection stores one value per key in projection_rows and serves them
// back.
//
// It supersedes RowProjection, which it shares a table with, and differs in
// four ways that matter to anyone writing a read endpoint:
//
//   - List returns keys. RowProjection.List hands back []V and drops the key,
//     which is unusable whenever the folded state does not contain its own id.
//   - You can filter on a field of the value, not only on tags. There was no
//     way to express "status = open" before.
//   - Nothing a caller supplies is concatenated into SQL. Field names are
//     bound as parameters; the only identifier ever interpolated is the
//     projection's own name, in MigrateIndexes.
//   - Apply is safe to run concurrently, and safe to run from two processes.
//
// A row's tags accumulate: they are the union over every event folded into it,
// so a row does not drop out of a tag filter because its most recent event did
// not happen to carry that tag.
type KeyedProjection[V any] struct {
	db      *gorm.DB
	name    string
	query   eventstore.QueryItem
	handler keyedprojection.Handler[V]

	mu       sync.Mutex
	position uint64
}

var _ keyedprojection.Projection[any] = (*KeyedProjection[any])(nil)

// NewKeyedProjection opens a keyed projection, loading its checkpoint.
func NewKeyedProjection[V any](db *gorm.DB, name string, query eventstore.QueryItem, handler keyedprojection.Handler[V]) (*KeyedProjection[V], error) {
	p := &KeyedProjection[V]{db: db, name: name, query: query, handler: handler}
	if err := p.loadCheckpoint(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *KeyedProjection[V]) EventQuery() eventstore.QueryItem { return p.query }

func (p *KeyedProjection[V]) After() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.position
}

// checkpointGap is how far the head may move past the position before
// Checkpoint records it without an applied event.
const checkpointGap = 10_000

// Checkpoint records upTo once it is checkpointGap past the position. Apply
// already persists the position with each row it writes, so this only covers
// stretches of the log without a matching event. GREATEST keeps the stored
// position from moving back when another process got further.
func (p *KeyedProjection[V]) Checkpoint(ctx context.Context, upTo uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if upTo < p.position+checkpointGap {
		return nil
	}
	err := p.db.WithContext(ctx).Exec(`INSERT INTO projection_checkpoints (projection_name, position) VALUES (?, ?)
		ON CONFLICT (projection_name) DO UPDATE SET position = GREATEST(projection_checkpoints.position, EXCLUDED.position)`,
		p.name, int64(upTo)).Error
	if err != nil {
		return fmt.Errorf("could not write checkpoint for %q: %w", p.name, err)
	}
	p.position = upTo
	return nil
}

// Apply folds one event into its row.
//
// Three things guard it, for three different failure modes:
//
//   - the mutex, because two catch-ups in one process otherwise race on
//     position and on each other's reads;
//   - the sequence check inside the transaction, because two *processes*
//     otherwise both apply the same event to the same row and double-count it;
//   - assigning position only after the transaction commits, because doing it
//     inside the closure means a rolled-back transaction leaves the position
//     advanced and that event silently skipped until the process restarts.
func (p *KeyedProjection[V]) Apply(ctx context.Context, e eventstore.SequencedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e.SequenceNumber <= p.position {
		return nil
	}

	key, err := p.handler.Key(e)
	if err != nil {
		return fmt.Errorf("could not extract key from %s event: %w", e.Type, err)
	}

	err = p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Re-check under the row lock: another instance may have applied this
		// event between our read of position and this transaction.
		var cp projectionCheckpoint
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("projection_name = ?", p.name).First(&cp).Error
		switch {
		case err == gorm.ErrRecordNotFound:
			// First event for this projection.
		case err != nil:
			return fmt.Errorf("could not lock checkpoint: %w", err)
		case uint64(cp.Position) >= e.SequenceNumber:
			return errAlreadyApplied
		}

		current, tags, err := p.loadRow(tx, key)
		if err != nil {
			return err
		}
		next, err := p.handler.Apply(e, current)
		if err != nil {
			return fmt.Errorf("could not apply %s event to row %q: %w", e.Type, key, err)
		}

		// The checkpoint carries a foreign key from the rows, so it goes first.
		// GREATEST: with no checkpoint row there was nothing to lock above, so
		// another process may have written a later position in the meantime.
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "projection_name"}},
			DoUpdates: clause.Set{{
				Column: clause.Column{Name: "position"},
				Value:  gorm.Expr("GREATEST(projection_checkpoints.position, EXCLUDED.position)"),
			}},
		}).Create(&projectionCheckpoint{
			ProjectionName: p.name,
			Position:       int64(e.SequenceNumber),
		}).Error; err != nil {
			return fmt.Errorf("could not write checkpoint: %w", err)
		}

		if next == nil {
			return tx.Where("projection_name = ? AND key = ?", p.name, key).
				Delete(&projectionRow{}).Error
		}

		value, err := json.Marshal(next)
		if err != nil {
			return fmt.Errorf("could not marshal row %q: %w", key, err)
		}
		merged, err := json.Marshal(mergeRowTags(tags, e.Tags))
		if err != nil {
			return fmt.Errorf("could not marshal tags for row %q: %w", key, err)
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "projection_name"}, {Name: "key"}},
			DoUpdates: clause.AssignmentColumns([]string{"value", "tags"}),
		}).Create(&projectionRow{
			ProjectionName: p.name, Key: key, Value: value, Tags: merged,
		}).Error
	})
	switch {
	case err == errAlreadyApplied:
		p.position = e.SequenceNumber
		return nil
	case err != nil:
		return err
	}

	p.position = e.SequenceNumber
	return nil
}

// errAlreadyApplied rolls the transaction back without being a failure: another
// instance got there first.
var errAlreadyApplied = fmt.Errorf("event already applied")

func (p *KeyedProjection[V]) Get(ctx context.Context, key string) (keyedprojection.Entry[V], bool, error) {
	var row projectionRow
	err := p.db.WithContext(ctx).
		Where("projection_name = ? AND key = ?", p.name, key).First(&row).Error
	if err == gorm.ErrRecordNotFound {
		return keyedprojection.Entry[V]{}, false, nil
	}
	if err != nil {
		return keyedprojection.Entry[V]{}, false, fmt.Errorf("could not get row %q: %w", key, err)
	}
	var v V
	if err := json.Unmarshal(row.Value, &v); err != nil {
		return keyedprojection.Entry[V]{}, false, fmt.Errorf("could not unmarshal row %q: %w", key, err)
	}
	return keyedprojection.Entry[V]{Key: row.Key, Value: v}, true, nil
}

// DefaultPageSize bounds a List that does not ask for a limit.
const DefaultPageSize = 100

func (p *KeyedProjection[V]) List(ctx context.Context, q keyedprojection.ListQuery) ([]keyedprojection.Entry[V], error) {
	db := p.db.WithContext(ctx).
		Model(&projectionRow{}).
		Where("projection_name = ?", p.name)

	// Field names are bound, not interpolated: Postgres accepts a parameter as
	// the right operand of ->>, so nothing a caller supplies reaches the query
	// text.
	for _, w := range q.Where {
		if err := checkField(w.Field); err != nil {
			return nil, err
		}
		db = db.Where("value->>? = ?", w.Field, fmt.Sprint(w.Value))
	}
	for _, t := range q.Tags {
		if err := checkField(t.Key); err != nil {
			return nil, err
		}
		db = db.Where("tags->>? = ?", t.Key, t.Value)
	}

	for _, s := range q.Order {
		// ORDER BY takes an expression, and a bare parameter there would be
		// read as a column ordinal — so this is the one place an identifier is
		// interpolated, and checkField is what makes that safe.
		if err := checkField(s.Field); err != nil {
			return nil, err
		}
		dir := "ASC"
		if s.Desc {
			dir = "DESC"
		}
		db = db.Order(fmt.Sprintf("value->>'%s' %s", s.Field, dir))
	}
	db = db.Order("key ASC") // stable tiebreaker, always last

	limit := q.Limit
	if limit <= 0 || limit > DefaultPageSize {
		limit = DefaultPageSize
	}
	db = db.Offset(q.Offset).Limit(limit)

	var rows []projectionRow
	if err := db.Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("could not list projection %q: %w", p.name, err)
	}
	out := make([]keyedprojection.Entry[V], 0, len(rows))
	for _, row := range rows {
		var v V
		if err := json.Unmarshal(row.Value, &v); err != nil {
			return nil, fmt.Errorf("could not unmarshal row %q: %w", row.Key, err)
		}
		out = append(out, keyedprojection.Entry[V]{Key: row.Key, Value: v})
	}
	return out, nil
}

// MigrateFieldIndexes creates an expression index per field a caller filters or
// sorts on. Without them List scans the projection's rows and parses jsonb for
// each one.
func (p *KeyedProjection[V]) MigrateFieldIndexes(fields ...string) error {
	return p.migrateIndexes("value", fields)
}

// MigrateTagIndexes does the same for tag filters.
func (p *KeyedProjection[V]) MigrateTagIndexes(keys ...string) error {
	return p.migrateIndexes("tags", keys)
}

func (p *KeyedProjection[V]) migrateIndexes(column string, names []string) error {
	for _, name := range names {
		if err := checkField(name); err != nil {
			return err
		}
		// The projection name reaches the index name and the WHERE clause, so
		// it has to be an identifier too — a dash here is a syntax error.
		if err := checkField(p.name); err != nil {
			return fmt.Errorf("projection name %q cannot be used in an index name: %w", p.name, err)
		}
		sql := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_%s_%s_%s ON projection_rows ((%s->>'%s')) WHERE projection_name = '%s'`,
			p.name, column, name, column, name, p.name,
		)
		if err := p.db.Exec(sql).Error; err != nil {
			return fmt.Errorf("could not index %s.%s for projection %q: %w", column, name, p.name, err)
		}
	}
	return nil
}

// Reset drops every row and the checkpoint, so the next catch-up rebuilds.
// Stop every other catch-up of this projection first, in this process and in
// others: one that is still running writes its position back and the rebuild
// skips everything up to it.
func (p *KeyedProjection[V]) Reset(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("projection_name = ?", p.name).Delete(&projectionRow{}).Error; err != nil {
			return err
		}
		return tx.Where("projection_name = ?", p.name).Delete(&projectionCheckpoint{}).Error
	})
	if err != nil {
		return fmt.Errorf("could not reset projection %q: %w", p.name, err)
	}
	p.position = 0
	return nil
}

func (p *KeyedProjection[V]) loadCheckpoint() error {
	var cp projectionCheckpoint
	err := p.db.Where("projection_name = ?", p.name).First(&cp).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not load checkpoint for %q: %w", p.name, err)
	}
	p.position = uint64(cp.Position)
	return nil
}

func (p *KeyedProjection[V]) loadRow(tx *gorm.DB, key string) (*V, map[string]string, error) {
	var row projectionRow
	err := tx.Where("projection_name = ? AND key = ?", p.name, key).First(&row).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("could not query row %q: %w", key, err)
	}
	var v V
	if err := json.Unmarshal(row.Value, &v); err != nil {
		return nil, nil, fmt.Errorf("could not unmarshal row %q: %w", key, err)
	}
	tags := map[string]string{}
	if len(row.Tags) > 0 {
		if err := json.Unmarshal(row.Tags, &tags); err != nil {
			return nil, nil, fmt.Errorf("could not unmarshal tags of row %q: %w", key, err)
		}
	}
	return &v, tags, nil
}

// mergeRowTags accumulates an event's tags into what the row already carries.
//
// Overwriting instead would mean a row only ever matches its most recent
// event's tags, so a candidacy row would drop out of a jobId filter as soon as
// an event arrived that was scoped to the profile alone.
func mergeRowTags(existing map[string]string, tags []eventstore.Tag) map[string]string {
	out := make(map[string]string, len(existing)+len(tags))
	for k, v := range existing {
		out[k] = v
	}
	for _, t := range tags {
		if _, seen := out[t.Key]; !seen {
			out[t.Key] = t.Value
		}
	}
	return out
}

// checkField rejects anything that is not a bare identifier, so a name can be
// used where a bound parameter is not possible.
func checkField(name string) error {
	if name == "" {
		return fmt.Errorf("empty field name")
	}
	for _, r := range name {
		ok := r == '_' ||
			(r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9')
		if !ok {
			return fmt.Errorf("field name %q is not a plain identifier", name)
		}
	}
	return nil
}
