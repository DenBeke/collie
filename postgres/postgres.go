// Package postgres provides a PostgreSQL-backed event store implementation.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"strings"
	"time"

	"eventstore"

	"gorm.io/gorm"
)

// appendLock is the advisory lock ID used to serialize appends.
// 0x4443425F617070 is ASCII for "DCB_app".
const appendLock int64 = 0x4443425F617070

// Compile-time check that Store implements eventstore.Store.
var _ eventstore.Store = (*Store)(nil)

const defaultBatchSize = 1000

type event struct {
	SequenceNumber uint64    `gorm:"primaryKey;autoIncrement"`
	Type           string    `gorm:"not null;index"`
	Data           []byte    `gorm:"type:bytea"`
	Tags           []byte    `gorm:"type:jsonb;default:'{}';index:,type:gin"`
	CreatedAt      time.Time `gorm:"autoCreateTime;index"`
}

// Store is a PostgreSQL-backed event store.
type Store struct {
	db          *gorm.DB
	batchSize   int
	indexedTags map[string]bool // tag keys with expression indexes
}

// Option configures a Store.
type Option func(*Store)

// WithBatchSize sets the number of events fetched per database round-trip
// during Read. Defaults to 1000.
func WithBatchSize(n int) Option {
	return func(s *Store) { s.batchSize = n }
}

// WithIndexedTags registers tag keys that get composite B-tree expression
// indexes on (tags->>'key', sequence_number). This makes filtered reads
// fast even at billions of rows. Call Migrate() after setting this option.
func WithIndexedTags(keys ...string) Option {
	return func(s *Store) {
		for _, k := range keys {
			s.indexedTags[k] = true
		}
	}
}

// New creates a PostgreSQL event store.
func New(db *gorm.DB, opts ...Option) *Store {
	s := &Store{db: db, batchSize: defaultBatchSize, indexedTags: make(map[string]bool)}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Migrate creates or updates the events table and expression indexes for
// any tag keys registered with WithIndexedTags.
func (s *Store) Migrate() error {
	if err := s.db.AutoMigrate(&event{}); err != nil {
		return err
	}
	for key := range s.indexedTags {
		sql := fmt.Sprintf(
			`CREATE INDEX IF NOT EXISTS idx_events_tag_%s_seq ON events ((tags->>'%s'), sequence_number)`,
			key, key,
		)
		if err := s.db.Exec(sql).Error; err != nil {
			return fmt.Errorf("could not create index for tag %q: %w", key, err)
		}
	}
	return nil
}

// Read returns an iterator over events matching the query with a sequence
// number greater than after. Events are fetched in batches to keep memory
// usage bounded even for large event streams.
func (s *Store) Read(ctx context.Context, query eventstore.Query, after uint64) iter.Seq2[eventstore.SequencedEvent, error] {
	return func(yield func(eventstore.SequencedEvent, error) bool) {
		cursor := after
		for {
			var batch []event
			db := s.db.WithContext(ctx).
				Where("sequence_number > ?", cursor).
				Order("sequence_number ASC").
				Limit(s.batchSize)

			db = s.applyQuery(db, query)

			if err := db.Find(&batch).Error; err != nil {
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
	}
}

// Append atomically persists events. If condition is non-nil, the append fails
// with ErrAppendConditionViolated when the store contains matching events after
// the condition's sequence number. Uses an advisory lock to serialize appends.
func (s *Store) Append(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("SELECT pg_advisory_xact_lock(?)", appendLock).Error; err != nil {
			return fmt.Errorf("could not acquire append lock: %w", err)
		}

		if condition != nil {
			db := tx.Model(&event{}).Where("sequence_number > ?", condition.After)
			db = s.applyQuery(db, condition.Query)

			var count int64
			if err := db.Limit(1).Count(&count).Error; err != nil {
				return fmt.Errorf("could not check append condition: %w", err)
			}
			if count > 0 {
				return eventstore.ErrAppendConditionViolated
			}
		}

		for _, e := range events {
			tagsJSON, err := json.Marshal(e.Tags)
			if err != nil {
				return fmt.Errorf("could not marshal event tags: %w", err)
			}
			record := event{
				Type: e.Type,
				Data: e.Data,
				Tags: tagsJSON,
			}
			if err := tx.Create(&record).Error; err != nil {
				return fmt.Errorf("could not insert event: %w", err)
			}
		}

		return nil
	})
}

// applyQuery adds WHERE clauses for an eventstore.Query to a GORM query.
// Each QueryItem is OR'd; within a QueryItem, Types are OR'd and Tags are AND'd.
// For tag keys registered with WithIndexedTags, uses tags->>'key' = ? (B-tree
// friendly). For other tag keys, falls back to tags @> ?::jsonb (GIN).
func (s *Store) applyQuery(db *gorm.DB, query eventstore.Query) *gorm.DB {
	if len(query) == 0 {
		return db
	}

	var clauses []string
	var args []any

	for _, item := range query {
		var parts []string

		if len(item.Types) > 0 {
			parts = append(parts, "type IN ?")
			args = append(args, item.Types)
		}

		for k, v := range item.Tags {
			if s.indexedTags[k] {
				parts = append(parts, fmt.Sprintf("tags->>'%s' = ?", k))
				args = append(args, v)
			} else {
				parts = append(parts, "tags @> ?::jsonb")
				tagJSON, _ := json.Marshal(map[string]string{k: v})
				args = append(args, string(tagJSON))
			}
		}

		if len(parts) > 0 {
			clauses = append(clauses, "("+strings.Join(parts, " AND ")+")")
		}
	}

	if len(clauses) > 0 {
		return db.Where(strings.Join(clauses, " OR "), args...)
	}
	return db
}

func toSequencedEvent(e event) (eventstore.SequencedEvent, error) {
	var tags map[string]string
	if len(e.Tags) > 0 {
		if err := json.Unmarshal(e.Tags, &tags); err != nil {
			return eventstore.SequencedEvent{}, fmt.Errorf("could not unmarshal event tags: %w", err)
		}
	}
	return eventstore.SequencedEvent{
		Event: eventstore.Event{
			Type: e.Type,
			Data: e.Data,
			Tags: tags,
		},
		SequenceNumber: e.SequenceNumber,
		CreatedAt:      e.CreatedAt,
	}, nil
}
