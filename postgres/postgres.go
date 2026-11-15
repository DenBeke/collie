// Package postgres provides a PostgreSQL-backed event store implementation.
package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"iter"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/DenBeke/collie"

	"gorm.io/gorm"
)

// appendLock is the advisory lock ID used to serialize appends.
// 0x4443425F617070 is ASCII for "DCB_app".
const appendLock int64 = 0x4443425F617070

// ErrUnkeyableCondition is eventstore.ErrUnkeyableCondition, so errors.Is
// matches either. Append returns it in staging mode (the default) when the condition
// cannot be reduced to a finite set of (type,tag,value) boundaries: a match-all
// (empty Query) or any item missing a type or a tag. Such a condition would
// need the global lock, which does NOT compose with the per-boundary locks (the
// two share no advisory key, so a conflict between a global-lock append and a
// per-boundary append would go unnoticed). Give every query item at least one
// type AND one tag (a constant tag on every event of a kind names a boundary
// over all of them), or use WithGlobalLock.
var ErrUnkeyableCondition = eventstore.ErrUnkeyableCondition

// Compile-time check that Store implements eventstore.Store.
var _ eventstore.Store = (*Store)(nil)

const defaultBatchSize = 1000

// event is the committed log row. The envelope columns are nullable and hold
// pointers for it: a NULL actor, cause or recorded_at means the row predates the
// envelope, which is a different thing from an anonymous or uncaused append and
// is worth being able to tell apart.
type event struct {
	SequenceNumber uint64     `gorm:"primaryKey;autoIncrement"`
	Type           string     `gorm:"not null"`
	Data           []byte     `gorm:"type:bytea"`
	Tags           []byte     `gorm:"type:jsonb;default:'[]'"`
	Metadata       []byte     `gorm:"type:jsonb"`
	Actor          *string    `gorm:"type:text"`
	Cause          *string    `gorm:"type:text"`
	RecordedAt     *time.Time `gorm:"type:timestamptz;default:now()"`
}

// staging is the write-buffer table for the staging append. Appends land here
// concurrently (per-boundary locked); the singleton flusher (RunFlusher) moves
// rows into events in StagingID order via one atomic CTE, so a single writer
// assigns events.sequence_number gap-free and monotonic. StagingID (insert order)
// is the order key the flusher sorts by; same-boundary appends are serialized by
// the per-boundary lock, so their StagingID order is their append order.
//
// recorded_at is written here, when the append lands in the buffer, and carried
// over by the flusher: acceptance is when the store took the event, not when it
// got around to sequencing it, so the flush interval must not shift it.
type staging struct {
	StagingID  uint64     `gorm:"primaryKey;autoIncrement"`
	Type       string     `gorm:"not null"`
	Data       []byte     `gorm:"type:bytea"`
	Tags       []byte     `gorm:"type:jsonb;default:'[]'"`
	Metadata   []byte     `gorm:"type:jsonb"`
	Actor      *string    `gorm:"type:text"`
	Cause      *string    `gorm:"type:text"`
	RecordedAt *time.Time `gorm:"type:timestamptz"`
}

// TableName pins the table name to "staging" (not GORM's default "stagings"), so
// the SQL function and flush CTE can reference it directly.
func (staging) TableName() string { return "staging" }

// eventTag is the normalized tag index: one row per distinct (key,value) per
// event. The composite primary key (key, value, sequence_number) doubles as the
// seek index for filtered, sequence-ordered reads. Written only by the append
// function and the flusher; read only by the query/condition SQL.
type eventTag struct {
	Key            string `gorm:"primaryKey"`
	Value          string `gorm:"primaryKey"`
	SequenceNumber uint64 `gorm:"primaryKey"`
}

// TableName pins the table name so the SQL functions can reference it directly.
func (eventTag) TableName() string { return "event_tags" }

// Store is a PostgreSQL-backed event store.
type Store struct {
	db        *gorm.DB
	batchSize int
	mode      appendMode
	flusher   bool
	flushOpts FlushOptions
	logger    *slog.Logger

	closeOnce sync.Once
	stop      context.CancelFunc
	stopped   chan struct{}
}

// Option configures a Store.
type Option func(*Store)

// WithBatchSize sets the number of events fetched per database round-trip
// during Read. Defaults to 1000.
func WithBatchSize(n int) Option {
	return func(s *Store) { s.batchSize = n }
}

// New creates a PostgreSQL event store. By default Append goes through
// staging, and New starts a flusher that moves staged appends into events:
// across processes one flusher leads and the others wait, so every process can
// start one. Close stops it. WithGlobalLock and WithoutFlusher change that.
func New(db *gorm.DB, opts ...Option) *Store {
	s := &Store{db: db, batchSize: defaultBatchSize, mode: modeStaging, flusher: true, logger: slog.Default()}
	for _, opt := range opts {
		opt(s)
	}
	if s.mode == modeStaging && s.flusher {
		ctx, cancel := context.WithCancel(context.Background())
		s.stop, s.stopped = cancel, make(chan struct{})
		go s.keepFlushing(ctx)
	}
	return s
}

// addRecordedAt adds events.recorded_at ahead of AutoMigrate, deliberately
// without a default, and setRecordedAtDefault gives it one afterwards. The order
// is the whole point, and it is not a style preference.
//
// PostgreSQL evaluates a VOLATILE default such as now() for every existing row
// when the column is added, so adding recorded_at with its default in one step
// would rewrite an entire log with the moment of the migration — history forged
// by a deployment. Added bare, every historical row stays NULL, which reads
// correctly as "this event predates the envelope". SET DEFAULT afterwards is a
// catalog change only: it touches no row, and every append from then on is
// stamped. Both statements are no-ops the second time, so Migrate stays
// idempotent; on a fresh database AutoMigrate creates the column with the
// default already on it, where there is no history to forge.
const (
	addRecordedAt        = `ALTER TABLE IF EXISTS events ADD COLUMN IF NOT EXISTS recorded_at timestamptz`
	setRecordedAtDefault = `ALTER TABLE events ALTER COLUMN recorded_at SET DEFAULT now()`
)

// created_at was the application-clock timestamp recorded_at replaces. It is
// dropped, not backfilled: an application clock is not what recorded_at means,
// so historical rows keep a NULL recorded_at and the old data goes with the
// column. Dropping it takes its index along.
//
// Both statements are no-ops the second time, so Migrate stays idempotent. They
// must run AFTER the dcb_* functions have been replaced with bodies that no
// longer name the column. PostgreSQL will not stop the wrong order: a PL/pgSQL
// body is stored as text and is not dependency-checked, so the drop succeeds
// either way and only the live functions notice, failing every append that lands
// between the drop and their replacement.
const (
	dropEventsCreatedAt  = `ALTER TABLE IF EXISTS events DROP COLUMN IF EXISTS created_at`
	dropStagingCreatedAt = `ALTER TABLE IF EXISTS staging DROP COLUMN IF EXISTS created_at`
)

// Migrate creates or updates the events, staging and event_tags tables, the
// dcb_append / dcb_append_staging / dcb_flush functions, and the supporting
// indexes. Tag filtering goes through the event_tags (key,value,sequence_number)
// primary key; staging keeps a GIN index for its jsonb condition check.
//
// Migrate also records the store's append mode in the database, the first time,
// and installs only that mode's functions. When the database records the other
// mode, Migrate fails with ErrAppendModeMismatch: two modes writing to one
// database would each break the other's guarantees. SwitchAppendMode changes
// the recorded mode on purpose.
func (s *Store) Migrate() error {
	err := s.db.Exec(addRecordedAt).Error
	if err != nil {
		return fmt.Errorf("could not add the recorded_at column: %w", err)
	}
	if err := s.db.AutoMigrate(&event{}, &staging{}, &eventTag{}); err != nil {
		return err
	}
	err = s.db.Exec(setRecordedAtDefault).Error
	if err != nil {
		return fmt.Errorf("could not default the recorded_at column: %w", err)
	}
	err = s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(?)`, migrateLock).Error; err != nil {
			return fmt.Errorf("could not lock the append mode: %w", err)
		}
		recorded, err := recordedMode(tx)
		if err != nil {
			return err
		}
		if recorded != "" && recorded != s.mode {
			return fmt.Errorf("%w: it records %q and this store is configured for %q; SwitchAppendMode changes it", ErrAppendModeMismatch, recorded, s.mode)
		}
		return installMode(tx, s.mode)
	})
	if err != nil {
		return err
	}
	err = s.db.Exec(dropEventsCreatedAt).Error
	if err != nil {
		return fmt.Errorf("could not drop the events created_at column: %w", err)
	}
	err = s.db.Exec(dropStagingCreatedAt).Error
	if err != nil {
		return fmt.Errorf("could not drop the staging created_at column: %w", err)
	}
	// Composite index for type-only query items (no tag to anchor on): an ordered
	// scan by sequence_number within a type.
	if err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_events_type_seq ON events (type, sequence_number)`).Error; err != nil {
		return fmt.Errorf("could not create type index: %w", err)
	}
	if err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_staging_tags ON staging USING GIN (tags jsonb_path_ops)`).Error; err != nil {
		return fmt.Errorf("could not create staging tags index: %w", err)
	}
	return nil
}

// Read returns an iterator over events matching the query with a sequence
// number greater than after, and the head of the log. Events are fetched in
// batches to keep memory usage bounded even for large event streams.
func (s *Store) Read(ctx context.Context, query eventstore.Query, after uint64) (iter.Seq2[eventstore.SequencedEvent, error], uint64, error) {
	// Sequence order is commit order (one writer), so everything up to max is committed.
	var head uint64
	err := s.db.WithContext(ctx).Raw("SELECT COALESCE(max(sequence_number), 0) FROM events").Scan(&head).Error
	if err != nil {
		return nil, 0, fmt.Errorf("could not read the head of the log: %w", err)
	}
	return func(yield func(eventstore.SequencedEvent, error) bool) {
		cursor := after
		for {
			var batch []event
			sql, args := buildReadSQL(query, cursor, s.batchSize)
			if err := s.db.WithContext(ctx).Raw(sql, args...).Scan(&batch).Error; err != nil {
				yield(eventstore.SequencedEvent{}, fmt.Errorf("could not read events: %w", err))
				return
			}

			for _, e := range batch {
				se, err := toSequencedEvent(e)
				if err != nil {
					if !yield(eventstore.SequencedEvent{}, err) {
						return
					}
					continue
				}
				if !yield(se, nil) {
					return
				}
				cursor = e.SequenceNumber
			}

			if len(batch) < s.batchSize {
				return
			}
		}
	}, head, nil
}

// dcbItemConflictEvents is the per-query-item conflict predicate against the
// committed events table, via the normalized event_tags index. An item WITH tags
// is anchored on event_tags — each query (key,value) pair is an index seek on the
// (key,value,sequence_number) PK, and GROUP BY ... HAVING count(*) = N requires the
// event to carry ALL of the item's tags. A tag-LESS item (type-only / match-all)
// falls back to an events-by-type scan. v_after, item, events and event_tags are in
// scope in both dcb_append and dcb_append_staging.
const dcbItemConflictEvents = `(
                    EXISTS (
                        SELECT 1
                        FROM jsonb_array_elements(item->'tags') AS qt
                        JOIN event_tags t
                          ON t.key = qt->>'key' AND t.value = qt->>'value' AND t.sequence_number > v_after
                        WHERE item->'types' IS NULL
                           OR jsonb_typeof(item->'types') <> 'array'
                           OR jsonb_array_length(item->'types') = 0
                           OR EXISTS (
                               SELECT 1 FROM events e
                               WHERE e.sequence_number = t.sequence_number
                                 AND e.type IN (SELECT jsonb_array_elements_text(item->'types'))
                           )
                        GROUP BY t.sequence_number
                        HAVING count(*) = jsonb_array_length(item->'tags')
                    )
                    OR (
                        COALESCE(jsonb_array_length(item->'tags'), 0) = 0
                        AND EXISTS (
                            SELECT 1 FROM events e
                            WHERE e.sequence_number > v_after
                              AND (
                                  item->'types' IS NULL
                                  OR jsonb_typeof(item->'types') <> 'array'
                                  OR jsonb_array_length(item->'types') = 0
                                  OR e.type IN (SELECT jsonb_array_elements_text(item->'types'))
                              )
                        )
                    )
                )`

// dcbCheckBody is the DCB condition-check head of dcb_append: given v_after and
// p_query it sets v_conflict and RETURNs false if a matching event exists after
// v_after. (dcb_append_staging has its own variant that checks staging ∪ events
// in one snapshot — see dcbAppendStagingFnTemplate.)
//
// dcb_append is VOLATILE so that, under READ COMMITTED, each statement
// re-snapshots: the check after the lock observes appends that committed while we
// were waiting for the lock. That re-snapshot is what makes lock+check+insert
// race-free in one call (a client-side CTE could not — one statement holds a
// single pre-lock snapshot).
//
// The check is ITEMS-OUTER (for each query item, search committed events via the
// event_tags index — see dcbItemConflictEvents).
const dcbCheckBody = `
    -- p_query IS NULL => unconditional. '[]' => match all events after v_after.
    IF p_query IS NOT NULL THEN
        IF jsonb_array_length(p_query) = 0 THEN
            SELECT EXISTS (SELECT 1 FROM events WHERE sequence_number > v_after) INTO v_conflict;
        ELSE
            SELECT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p_query) AS item
                WHERE ` + dcbItemConflictEvents + `
            ) INTO v_conflict;
        END IF;
        IF v_conflict THEN
            RETURN false;
        END IF;
    END IF;
`

// dcbInsertBody is the tail of dcb_append: the batched insert of p_events into
// events, then RETURN true. It is appended after dcbCheckBody. recorded_at is
// left out of the column list so the table default (now()) stamps acceptance;
// an absent or empty actor/cause lands as NULL rather than an empty string.
const dcbInsertBody = `
    WITH ins AS (
        INSERT INTO events (type, data, tags, metadata, actor, cause)
        SELECT elem->>'type',
               decode(elem->>'data', 'base64'),
               COALESCE(elem->'tags', '[]'::jsonb),
               CASE WHEN elem->'metadata' IS NULL OR jsonb_typeof(elem->'metadata') = 'null'
                    THEN NULL ELSE elem->'metadata' END,
               NULLIF(elem->>'actor', ''),
               NULLIF(elem->>'cause', '')
        FROM jsonb_array_elements(p_events) AS elem
        RETURNING sequence_number, tags
    )
    INSERT INTO event_tags (key, value, sequence_number)
    SELECT DISTINCT tag->>'key', tag->>'value', ins.sequence_number
    FROM ins CROSS JOIN LATERAL jsonb_array_elements(ins.tags) AS tag;

    RETURN true;
END;
$func$;`

// dcbAppendFnTemplate is the DDL for the dcb_append PL/pgSQL function created in
// Migrate. %d is the advisory-lock key (appendLock — single source of truth).
// It takes ONE global advisory lock, so every append serializes.
const dcbAppendFnTemplate = `CREATE OR REPLACE FUNCTION dcb_append(p_events jsonb, p_query jsonb, p_after bigint)
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $func$
DECLARE
    v_after bigint := COALESCE(p_after, 0);
    v_conflict boolean;
BEGIN
    PERFORM pg_advisory_xact_lock(%d);
` + dcbCheckBody + dcbInsertBody

// dcbAppendStagingFnTemplate is the DDL for dcb_append_staging — the sequencer/
// outbox append. It per-boundary-locks (or takes the global %d lock when no keys
// are supplied — i.e. an unconditional append of tag-less events; unkeyable
// conditions are rejected by appendStaging before they reach here), runs the
// conflict check across BOTH tables in ONE statement (one snapshot, so the atomic
// flush can't hide a row from both),
// and inserts into STAGING (never events). The singleton flusher (RunFlusher)
// later moves staging→events in staging_id order, so events.sequence_number stays
// gap-free and monotonic and the plain uint64 cursor keeps working unchanged.
// %d is appendLock.
const dcbAppendStagingFnTemplate = `CREATE OR REPLACE FUNCTION dcb_append_staging(p_events jsonb, p_query jsonb, p_after bigint, p_keys bigint[])
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $func$
DECLARE
    v_after bigint := COALESCE(p_after, 0);
    v_conflict boolean;
    v_key bigint;
BEGIN
    IF array_length(p_keys, 1) IS NULL THEN
        PERFORM pg_advisory_xact_lock(%d);
    ELSE
        FOR v_key IN SELECT DISTINCT k FROM unnest(p_keys) AS t(k) ORDER BY k LOOP
            PERFORM pg_advisory_xact_lock(v_key);
        END LOOP;
    END IF;

    -- p_query IS NULL => unconditional. '[]' => match all. A matching row in
    -- staging (any — every un-flushed event is 'after' the main cursor) OR a
    -- matching row in events with sequence_number > v_after is a conflict.
    IF p_query IS NOT NULL THEN
        IF jsonb_array_length(p_query) = 0 THEN
            SELECT EXISTS (SELECT 1 FROM staging)
                OR EXISTS (SELECT 1 FROM events WHERE sequence_number > v_after)
              INTO v_conflict;
        ELSE
            SELECT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p_query) AS item
                WHERE EXISTS (
                    SELECT 1 FROM staging s
                    WHERE (
                          item->'types' IS NULL
                          OR jsonb_typeof(item->'types') <> 'array'
                          OR jsonb_array_length(item->'types') = 0
                          OR s.type IN (SELECT jsonb_array_elements_text(item->'types'))
                      )
                      AND s.tags @> COALESCE(item->'tags', '[]'::jsonb)
                )
                OR ` + dcbItemConflictEvents + `
            ) INTO v_conflict;
        END IF;
        IF v_conflict THEN
            RETURN false;
        END IF;
    END IF;

    INSERT INTO staging (type, data, tags, metadata, actor, cause, recorded_at)
    SELECT elem->>'type',
           decode(elem->>'data', 'base64'),
           COALESCE(elem->'tags', '[]'::jsonb),
           CASE WHEN elem->'metadata' IS NULL OR jsonb_typeof(elem->'metadata') = 'null'
                THEN NULL ELSE elem->'metadata' END,
           NULLIF(elem->>'actor', ''),
           NULLIF(elem->>'cause', ''),
           now()
    FROM jsonb_array_elements(p_events) AS elem;

    RETURN true;
END;
$func$;`

// appendEvent is the JSON wire shape for one event passed to dcb_append. Data is
// base64-encoded because it is arbitrary bytes (e.g. encryption output) that
// cannot be represented directly in JSON.
type appendEvent struct {
	Type     string            `json:"type"`
	Data     string            `json:"data"`
	Tags     []eventstore.Tag  `json:"tags"`
	Actor    string            `json:"actor,omitempty"`
	Cause    string            `json:"cause,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// queryItemDTO is the JSON wire shape for one append-condition query item.
// Lowercase, omitempty keys decouple the DB contract from the core QueryItem
// type and match what dcb_append reads (item->'types', item->'tags'); omitempty
// makes an absent Types/Tags a SQL NULL (= no constraint) rather than json null.
type queryItemDTO struct {
	Types []string         `json:"types,omitempty"`
	Tags  []eventstore.Tag `json:"tags,omitempty"`
}

// appendArgs marshals events and an optional condition into the arguments shared
// by the dcb_append / dcb_append_staging functions: the events as a JSON array
// string, the query as a JSON string (or nil => SQL NULL => unconditional), and
// the After cursor. Data is base64-encoded because it is arbitrary bytes.
func appendArgs(events []eventstore.Event, condition *eventstore.AppendCondition) (eventsJSON string, queryArg any, after int64, err error) {
	wire := make([]appendEvent, len(events))
	for i, e := range events {
		tags := e.Tags
		if tags == nil {
			tags = []eventstore.Tag{}
		}
		wire[i] = appendEvent{
			Type:     e.Type,
			Data:     base64.StdEncoding.EncodeToString(e.Data),
			Tags:     tags,
			Actor:    e.Actor,
			Cause:    e.Cause,
			Metadata: e.Metadata,
		}
	}
	eventsBytes, err := json.Marshal(wire)
	if err != nil {
		return "", nil, 0, fmt.Errorf("could not marshal events: %w", err)
	}

	if condition != nil {
		after = int64(condition.After)
		dto := make([]queryItemDTO, len(condition.Query))
		for i, item := range condition.Query {
			dto[i] = queryItemDTO{Types: item.Types, Tags: item.Tags}
		}
		queryBytes, err := json.Marshal(dto)
		if err != nil {
			return "", nil, 0, fmt.Errorf("could not marshal append condition: %w", err)
		}
		queryArg = string(queryBytes)
	}
	return string(eventsBytes), queryArg, after, nil
}

// Append atomically persists events. If condition is non-nil, the append fails
// with ErrAppendConditionViolated when the store contains matching events after
// the condition's sequence number.
//
// In staging mode (the default) the events land in the staging table and become
// readable once a flusher has moved them into events, within milliseconds; the
// condition already sees them while staged. The condition must be keyable (see
// ErrUnkeyableCondition). With WithGlobalLock they are readable as soon as
// Append returns, and every append serializes on one lock.
func (s *Store) Append(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	if s.mode == modeGlobal {
		return s.appendGlobal(ctx, events, condition)
	}
	return s.appendStaging(ctx, events, condition)
}

// appendGlobal persists events in a single database round-trip via the
// dcb_append function, which acquires the global advisory lock and performs the
// check + insert atomically; it is called in autocommit (no surrounding
// transaction) so the whole append is one round-trip.
//
// Two other strategies were prototyped and benchmarked but deliberately NOT kept:
// per-boundary locks (fast but positions commit out of order, so multi-boundary
// conditions are gappy without xid8 cursors) and a per-boundary-check + global-write
// "hybrid" (consistent but lands at ~global throughput). See appendbench-conclusion.md
// for the full comparison and why staging won; the implementations live in this
// repo's git history (the commit before the staging consolidation).
func (s *Store) appendGlobal(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	if len(events) == 0 && condition == nil {
		return nil
	}
	eventsJSON, queryArg, after, err := appendArgs(events, condition)
	if err != nil {
		return err
	}

	var appended bool
	err = s.db.WithContext(ctx).
		Raw("SELECT dcb_append(?::jsonb, ?::jsonb, ?::bigint)", eventsJSON, queryArg, after).
		Scan(&appended).Error
	if err != nil {
		return fmt.Errorf("could not append events: %w", modeError(err))
	}
	if !appended {
		return eventstore.ErrAppendConditionViolated
	}
	return nil
}

// lockKey hashes a (eventType, tagKey, tagValue) boundary into a 64-bit advisory
// lock key. FNV-1a is deterministic and seed-free, so the same boundary maps to
// the same key in every process (required for a consistent global lock order).
// 0x1f is an unambiguous field separator. The uint64→int64 conversion is fine:
// pg_advisory_xact_lock takes a signed bigint and negative keys are valid.
func lockKey(eventType, tagKey, tagValue string) int64 {
	h := fnv.New64a()
	h.Write([]byte(eventType))
	h.Write([]byte{0x1f})
	h.Write([]byte(tagKey))
	h.Write([]byte{0x1f})
	h.Write([]byte(tagValue))
	return int64(h.Sum64())
}

// boundaryLockKeys returns the deduped, ascending advisory-lock key set for an
// append and whether the condition is keyable. It keys BOTH the condition's
// (type,tag) triples AND the events' (type,tag) triples, because a conflict can
// arise from my-events vs your-condition or your-events vs my-condition.
//
// A condition is NOT keyable — and the staging append rejects it with
// ErrUnkeyableCondition — when it would lock nothing meaningful: a match-all
// (empty Query) or any item lacking a type or a tag. Those can't be expressed as a
// finite (type,tag,value) boundary set, so they'd need the global lock, which does
// not compose with the per-boundary locks.
func boundaryLockKeys(events []eventstore.Event, cond *eventstore.AppendCondition) (keys []int64, keyable bool) {
	set := make(map[int64]struct{})
	if cond != nil {
		if !cond.Keyable() {
			return nil, false // match-all, type-less or tag-less => global lock
		}
		for _, item := range cond.Query {
			for _, t := range item.Types {
				for _, tag := range item.Tags {
					set[lockKey(t, tag.Key, tag.Value)] = struct{}{}
				}
			}
		}
	}
	for _, e := range events {
		for _, tag := range e.Tags {
			set[lockKey(e.Type, tag.Key, tag.Value)] = struct{}{}
		}
	}
	keys = make([]int64, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys, true
}

// bigintArrayLiteral renders keys as a PostgreSQL bigint[] array literal
// ("{1,-2,3}"), bound as a string and cast with ?::bigint[] at the call site.
func bigintArrayLiteral(keys []int64) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatInt(k, 10))
	}
	b.WriteByte('}')
	return b.String()
}

// appendStaging is the sequencer/outbox append: it per-boundary-locks (rejecting
// unkeyable conditions with ErrUnkeyableCondition), checks the condition across
// staging and events in one snapshot, and inserts into the staging buffer. Appends
// to disjoint boundaries run concurrently (per-boundary locks + group commit),
// while a singleton flusher assigns events.sequence_number, so it is the
// high-throughput path that still keeps full consistency.
//
// A successful append is durable (committed to staging) but not visible to Read
// until the flusher moves it into events — eventual read-visibility bounded by the
// flush interval. It is fully consistent for multi-boundary conditions AND keeps the
// plain uint64 cursor, because events is written only by the single flusher.
func (s *Store) appendStaging(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	if len(events) == 0 && condition == nil {
		return nil
	}
	keys, keyable := boundaryLockKeys(events, condition)
	if !keyable {
		return ErrUnkeyableCondition
	}

	eventsJSON, queryArg, after, err := appendArgs(events, condition)
	if err != nil {
		return err
	}

	var appended bool
	err = s.db.WithContext(ctx).Raw(
		"SELECT dcb_append_staging(?::jsonb, ?::jsonb, ?::bigint, ?::bigint[])",
		eventsJSON, queryArg, after, bigintArrayLiteral(keys),
	).Scan(&appended).Error
	if err != nil {
		return fmt.Errorf("could not append events (staging): %w", modeError(err))
	}
	if !appended {
		return eventstore.ErrAppendConditionViolated
	}
	return nil
}

// flushLeaderKey is the session advisory-lock key that elects the single live
// flusher across processes. Distinct from appendLock. ASCII "DCB_fls".
const flushLeaderKey int64 = 0x4443425F666C73

// FlushOptions configures RunFlusher.
type FlushOptions struct {
	// Interval is how long to wait when staging is empty before checking again.
	Interval time.Duration
	// BatchSize is the max number of staging rows moved into events per flush.
	BatchSize int
}

// dcbFlushFn moves the oldest p_batch staging rows into events in staging_id
// order AND populates event_tags, in one atomic statement, returning the number
// of events moved. One writer (the flusher) ⇒ events.sequence_number is assigned
// gap-free and monotonic; the ORDER BY preserves same-boundary append order.
// recorded_at is carried over from staging (append time, not flush time). The
// returned count comes from `ins` (events moved), not the event_tags insert, so
// it stays correct even for batches whose events have no tags.
const dcbFlushFn = `CREATE OR REPLACE FUNCTION dcb_flush(p_batch int)
RETURNS bigint LANGUAGE plpgsql AS $func$
DECLARE
    v_moved bigint;
BEGIN
    WITH moved AS (
        DELETE FROM staging
        WHERE staging_id IN (SELECT staging_id FROM staging ORDER BY staging_id LIMIT p_batch)
        RETURNING *
    ),
    ins AS (
        INSERT INTO events (type, data, tags, metadata, actor, cause, recorded_at)
        SELECT type, data, tags, metadata, actor, cause, recorded_at
        FROM moved ORDER BY staging_id
        RETURNING sequence_number, tags
    ),
    tags_ins AS (
        INSERT INTO event_tags (key, value, sequence_number)
        SELECT DISTINCT tag->>'key', tag->>'value', ins.sequence_number
        FROM ins CROSS JOIN LATERAL jsonb_array_elements(ins.tags) AS tag
        RETURNING 1
    )
    SELECT count(*) FROM ins INTO v_moved;
    RETURN v_moved;
END;
$func$;`

// RunFlusher runs the singleton staging→events flusher until ctx is cancelled.
// The store New returns already runs one unless WithoutFlusher was given; call
// RunFlusher only to run it yourself. It holds a session-level advisory lock on
// a dedicated connection so that across N processes exactly one flusher is live
// (the lock auto-releases — and another caller takes over — when the holder's
// connection dies). It then loops: move a batch; if rows moved, drain again
// immediately; otherwise sleep opts.Interval. When ctx is cancelled, the leader
// first moves what is still staged (best effort, bounded by drainTimeout).
//
// Only the flusher writes events in staging mode. On a store in global-lock mode
// RunFlusher returns ErrAppendModeMismatch, and the database refuses a flush.
func (s *Store) RunFlusher(ctx context.Context, opts FlushOptions) error {
	if s.mode == modeGlobal {
		return fmt.Errorf("%w: a store in global-lock mode has no flusher", ErrAppendModeMismatch)
	}
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Millisecond
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 5000
	}

	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("flusher: could not get sql.DB: %w", err)
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("flusher: could not get connection: %w", err)
	}
	defer conn.Close()

	// Acquire leadership (blocks until held or ctx cancelled). The lock belongs
	// to the session, and conn.Close only hands the connection back to the pool
	// with the session — and the lock — still alive, so the connection is
	// discarded instead: that ends the session and lets another flusher lead.
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", flushLeaderKey); err != nil {
		return fmt.Errorf("flusher: could not acquire leadership: %w", err)
	}
	defer func() { _ = conn.Raw(func(any) error { return driver.ErrBadConn }) }()

	err = flushUntilDone(ctx, conn, opts)
	if ctx.Err() != nil {
		drain(conn, opts.BatchSize)
		return ctx.Err()
	}
	return err
}

// flushUntilDone is the leader's loop: move a batch, again at once when rows
// moved, otherwise after opts.Interval, until ctx is cancelled or a flush fails.
func flushUntilDone(ctx context.Context, conn *sql.Conn, opts FlushOptions) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var moved int64
		if err := conn.QueryRowContext(ctx, "SELECT dcb_flush($1)", opts.BatchSize).Scan(&moved); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("flusher: flush failed: %w", modeError(err))
		}
		if moved == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.Interval):
			}
		}
	}
}

// drainTimeout bounds how long a stopping leader keeps moving staged appends.
const drainTimeout = 5 * time.Second

// drain moves what is still staged before a leader lets go, so a process that
// stops leaves nothing unreadable until the next leader takes over. It uses the
// leader's own connection, the only one allowed to flush; if that connection
// went down with the cancelled query, the lock is gone with it and another
// leader takes over instead.
func drain(conn *sql.Conn, batch int) {
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	for {
		var moved int64
		if err := conn.QueryRowContext(ctx, "SELECT dcb_flush($1)", batch).Scan(&moved); err != nil || moved == 0 {
			return
		}
	}
}

// flush moves up to batchSize staging rows into events (one atomic CTE) and
// returns the number moved. Tests drain staging with it; anywhere else, a
// second writer next to the flusher would break the gap-free ordering.
func (s *Store) flush(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 5000
	}
	sqlDB, err := s.db.DB()
	if err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}
	var moved int64
	if err := sqlDB.QueryRowContext(ctx, "SELECT dcb_flush($1)", batchSize).Scan(&moved); err != nil {
		return 0, fmt.Errorf("flush: %w", modeError(err))
	}
	return moved, nil
}

// buildReadSQL builds one keyset-paginated read batch. For each query item it
// produces an index-ordered, LIMIT-bounded candidate set of sequence_numbers
// (each item fully type-coupled), UNIONs them (which dedups), and fetches the
// matching events in sequence order. An empty query reads straight off the events
// primary key. Placeholders are GORM "?" (rewritten to $N by the postgres
// driver); args are appended in placeholder order.
func buildReadSQL(query eventstore.Query, cursor uint64, limit int) (string, []any) {
	const cols = "e.sequence_number, e.type, e.data, e.tags, e.metadata, " +
		"e.actor, e.cause, e.recorded_at"

	var args []any
	ph := func(v any) string {
		args = append(args, v)
		return "?"
	}

	if len(query) == 0 {
		sql := "SELECT " + cols + " FROM events e WHERE e.sequence_number > " + ph(cursor) +
			" ORDER BY e.sequence_number LIMIT " + ph(limit)
		return sql, args
	}

	arms := make([]string, len(query))
	for i, item := range query {
		arms[i] = buildReadItem(item, cursor, limit, ph)
	}
	sql := "WITH cand AS (\n" + strings.Join(arms, "\nUNION\n") + "\n)\n" +
		"SELECT " + cols + " FROM events e " +
		"JOIN (SELECT sequence_number FROM cand ORDER BY sequence_number LIMIT " + ph(limit) + ") c " +
		"ON c.sequence_number = e.sequence_number ORDER BY e.sequence_number"
	return sql, args
}

// buildReadItem builds one UNION arm: the ordered, bounded candidate
// sequence_numbers matching a single query item. A tag-less item scans events
// (optionally by type); a tagged item anchors on the first tag via event_tags,
// requires the rest via EXISTS, and filters type with a primary-key join.
func buildReadItem(item eventstore.QueryItem, cursor uint64, limit int, ph func(any) string) string {
	typeIn := func(col string) string {
		phs := make([]string, len(item.Types))
		for i, t := range item.Types {
			phs[i] = ph(t)
		}
		return col + " IN (" + strings.Join(phs, ", ") + ")"
	}

	if len(item.Tags) == 0 {
		where := "sequence_number > " + ph(cursor)
		if len(item.Types) > 0 {
			where += " AND " + typeIn("type")
		}
		return "(SELECT sequence_number FROM events WHERE " + where +
			" ORDER BY sequence_number LIMIT " + ph(limit) + ")"
	}

	anchor := item.Tags[0]
	from := "event_tags t"
	where := "t.key = " + ph(anchor.Key) + " AND t.value = " + ph(anchor.Value) +
		" AND t.sequence_number > " + ph(cursor)
	if len(item.Types) > 0 {
		from += " JOIN events e ON e.sequence_number = t.sequence_number"
		where += " AND " + typeIn("e.type")
	}
	for _, extra := range item.Tags[1:] {
		where += " AND EXISTS (SELECT 1 FROM event_tags t2 WHERE t2.sequence_number = t.sequence_number" +
			" AND t2.key = " + ph(extra.Key) + " AND t2.value = " + ph(extra.Value) + ")"
	}
	return "(SELECT t.sequence_number FROM " + from + " WHERE " + where +
		" ORDER BY t.sequence_number LIMIT " + ph(limit) + ")"
}

func toSequencedEvent(e event) (eventstore.SequencedEvent, error) {
	var tags []eventstore.Tag
	if len(e.Tags) > 0 {
		if err := json.Unmarshal(e.Tags, &tags); err != nil {
			return eventstore.SequencedEvent{}, fmt.Errorf("could not unmarshal event tags: %w", err)
		}
	}
	var metadata map[string]string
	if len(e.Metadata) > 0 {
		if err := json.Unmarshal(e.Metadata, &metadata); err != nil {
			return eventstore.SequencedEvent{}, fmt.Errorf("could not unmarshal event metadata: %w", err)
		}
	}
	return eventstore.SequencedEvent{
		Event: eventstore.Event{
			Type:     e.Type,
			Data:     e.Data,
			Tags:     tags,
			Actor:    deref(e.Actor),
			Cause:    deref(e.Cause),
			Metadata: metadata,
		},
		SequenceNumber: e.SequenceNumber,
		RecordedAt:     deref(e.RecordedAt),
	}, nil
}

// deref reads a nullable column into its Go zero value, which is what a caller
// sees for an event that predates the envelope.
func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
