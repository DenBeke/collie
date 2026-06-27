// Package postgres provides a PostgreSQL-backed event store implementation.
package postgres

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"iter"
	"sort"
	"strconv"
	"strings"
	"time"

	"eventstore"

	"gorm.io/gorm"
)

// appendLock is the advisory lock ID used to serialize appends.
// 0x4443425F617070 is ASCII for "DCB_app".
const appendLock int64 = 0x4443425F617070

// ErrUnkeyableCondition is returned by AppendPerBoundary, AppendHybrid, and
// AppendStaging when the condition cannot be reduced to a finite set of
// (type,tag,value) boundaries — a match-all (empty Query) or any item missing a
// type or a tag. Such a condition would need the global lock, which does NOT
// compose with the per-boundary locks (the two share no advisory key, so a
// conflict between a global-lock append and a per-boundary append would go
// unnoticed). Give every query item at least one type AND one tag, or run the
// whole workload on the global-lock Append.
var ErrUnkeyableCondition = errors.New("append condition is not keyable: the query must be non-empty and every item needs at least one type and one tag")

// Compile-time check that Store implements eventstore.Store.
var _ eventstore.Store = (*Store)(nil)

const defaultBatchSize = 1000

type event struct {
	SequenceNumber uint64    `gorm:"primaryKey;autoIncrement"`
	Type           string    `gorm:"not null;index"`
	Data           []byte    `gorm:"type:bytea"`
	Tags           []byte    `gorm:"type:jsonb;default:'{}'"`
	Metadata       []byte    `gorm:"type:jsonb"`
	CreatedAt      time.Time `gorm:"autoCreateTime;index"`
}

// staging is the write-buffer table for AppendStaging. Appends land here
// concurrently (per-boundary locked); the singleton flusher (RunFlusher) moves
// rows into events in StagingID order via one atomic CTE, so a single writer
// assigns events.sequence_number gap-free and monotonic. StagingID (insert order)
// is the order key the flusher sorts by; same-boundary appends are serialized by
// the per-boundary lock, so their StagingID order is their append order.
type staging struct {
	StagingID uint64    `gorm:"primaryKey;autoIncrement"`
	Type      string    `gorm:"not null"`
	Data      []byte    `gorm:"type:bytea"`
	Tags      []byte    `gorm:"type:jsonb;default:'{}'"`
	Metadata  []byte    `gorm:"type:jsonb"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
}

// TableName pins the table name to "staging" (not GORM's default "stagings"), so
// the SQL function and flush CTE can reference it directly.
func (staging) TableName() string { return "staging" }

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
	if err := s.db.AutoMigrate(&event{}, &staging{}); err != nil {
		return err
	}
	if err := s.db.Exec(fmt.Sprintf(dcbAppendFnTemplate, appendLock)).Error; err != nil {
		return fmt.Errorf("could not create dcb_append function: %w", err)
	}
	if err := s.db.Exec(dcbAppendBoundaryFnTemplate).Error; err != nil {
		return fmt.Errorf("could not create dcb_append_boundary function: %w", err)
	}
	if err := s.db.Exec(fmt.Sprintf(dcbAppendHybridFnTemplate, appendLock)).Error; err != nil {
		return fmt.Errorf("could not create dcb_append_hybrid function: %w", err)
	}
	if err := s.db.Exec(fmt.Sprintf(dcbAppendStagingFnTemplate, appendLock)).Error; err != nil {
		return fmt.Errorf("could not create dcb_append_staging function: %w", err)
	}
	if err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_events_tags ON events USING GIN (tags jsonb_path_ops)`).Error; err != nil {
		return fmt.Errorf("could not create tags index: %w", err)
	}
	if err := s.db.Exec(`CREATE INDEX IF NOT EXISTS idx_staging_tags ON staging USING GIN (tags jsonb_path_ops)`).Error; err != nil {
		return fmt.Errorf("could not create staging tags index: %w", err)
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

// dcbCheckBody is the shared DCB condition-check head of all three append
// functions: given v_after and p_query it sets v_conflict and RETURNs false if a
// matching event exists after v_after. It is identical across dcb_append (global
// lock), dcb_append_boundary (per-boundary locks) and dcb_append_hybrid
// (per-boundary check + global write-lock), so the functions differ ONLY in how
// they lock — keeping any A/B comparison honest.
//
// All functions are VOLATILE so that, under READ COMMITTED, each statement
// re-snapshots: the check after the lock observes appends that committed while we
// were waiting for the lock. That re-snapshot is what makes lock+check+insert
// race-free in one call (a client-side CTE could not — one statement holds a
// single pre-lock snapshot).
//
// The condition check is structured ITEMS-OUTER (for each query item, search
// events) so the per-item `e.tags @> item.tags` is a parameterized GIN index
// scan against the jsonb_path_ops index on tags.
const dcbCheckBody = `
    -- p_query IS NULL => unconditional. '[]' => match all events after v_after.
    IF p_query IS NOT NULL THEN
        IF jsonb_array_length(p_query) = 0 THEN
            SELECT EXISTS (SELECT 1 FROM events WHERE sequence_number > v_after) INTO v_conflict;
        ELSE
            SELECT EXISTS (
                SELECT 1 FROM jsonb_array_elements(p_query) AS item
                WHERE EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.sequence_number > v_after
                      AND (
                          item->'types' IS NULL
                          OR jsonb_typeof(item->'types') <> 'array'
                          OR jsonb_array_length(item->'types') = 0
                          OR e.type IN (SELECT jsonb_array_elements_text(item->'types'))
                      )
                      AND e.tags @> COALESCE(item->'tags', '{}'::jsonb)
                )
            ) INTO v_conflict;
        END IF;
        IF v_conflict THEN
            RETURN false;
        END IF;
    END IF;
`

// dcbInsertBody is the shared tail of all three append functions: the batched
// insert of p_events, then RETURN true. It is appended after dcbCheckBody (and,
// for the hybrid, after the global write-lock has been taken).
const dcbInsertBody = `
    INSERT INTO events (type, data, tags, metadata, created_at)
    SELECT elem->>'type',
           decode(elem->>'data', 'base64'),
           COALESCE(elem->'tags', '{}'::jsonb),
           CASE WHEN elem->'metadata' IS NULL OR jsonb_typeof(elem->'metadata') = 'null'
                THEN NULL ELSE elem->'metadata' END,
           now()
    FROM jsonb_array_elements(p_events) AS elem;

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

// dcbAppendBoundaryFnTemplate is the DDL for dcb_append_boundary: instead of one
// global lock it acquires a caller-supplied set of per-boundary advisory keys, so
// appends to disjoint boundaries run concurrently. Keys are acquired in ascending
// order (a single global lock order ⇒ deadlock-free). The check + insert tail is
// identical to dcb_append. (Experimental — see AppendPerBoundary for the
// correctness caveat on multi-boundary conditions.)
const dcbAppendBoundaryFnTemplate = `CREATE OR REPLACE FUNCTION dcb_append_boundary(p_events jsonb, p_query jsonb, p_after bigint, p_keys bigint[])
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $func$
DECLARE
    v_after bigint := COALESCE(p_after, 0);
    v_conflict boolean;
    v_key bigint;
BEGIN
    FOR v_key IN SELECT DISTINCT k FROM unnest(p_keys) AS t(k) ORDER BY k LOOP
        PERFORM pg_advisory_xact_lock(v_key);
    END LOOP;
` + dcbCheckBody + dcbInsertBody

// dcbAppendHybridFnTemplate is the DDL for dcb_append_hybrid: the fully-consistent
// per-boundary variant. It acquires the per-boundary keys (like dcb_append_boundary)
// and runs the condition CHECK under them — so checks for disjoint boundaries run
// concurrently — then takes the ONE global advisory lock (%d) only for the INSERT.
// Holding the global lock from the insert through commit re-serializes the writes,
// so sequence_number is assigned in commit order (gap-free) while the expensive
// check stays parallel. Result: correct for MULTI-boundary conditions AND keeps the
// plain sequence-number cursor (no xid8) — unlike dcb_append_boundary, which trades
// that gap for group-commit throughput. Per-boundary keys are taken in ascending
// order and the global lock last, so the lock order is consistent (deadlock-free).
// %d is appendLock.
const dcbAppendHybridFnTemplate = `CREATE OR REPLACE FUNCTION dcb_append_hybrid(p_events jsonb, p_query jsonb, p_after bigint, p_keys bigint[])
RETURNS boolean LANGUAGE plpgsql VOLATILE AS $func$
DECLARE
    v_after bigint := COALESCE(p_after, 0);
    v_conflict boolean;
    v_key bigint;
BEGIN
    FOR v_key IN SELECT DISTINCT k FROM unnest(p_keys) AS t(k) ORDER BY k LOOP
        PERFORM pg_advisory_xact_lock(v_key);
    END LOOP;
` + dcbCheckBody + `
    PERFORM pg_advisory_xact_lock(%d);
` + dcbInsertBody

// dcbAppendStagingFnTemplate is the DDL for dcb_append_staging — the sequencer/
// outbox append. It per-boundary-locks (or takes the global %d lock when no keys
// are supplied — i.e. an unconditional append of tag-less events; unkeyable
// conditions are rejected by AppendStaging before they reach here), runs the
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
                      AND s.tags @> COALESCE(item->'tags', '{}'::jsonb)
                )
                OR EXISTS (
                    SELECT 1 FROM events e
                    WHERE e.sequence_number > v_after
                      AND (
                          item->'types' IS NULL
                          OR jsonb_typeof(item->'types') <> 'array'
                          OR jsonb_array_length(item->'types') = 0
                          OR e.type IN (SELECT jsonb_array_elements_text(item->'types'))
                      )
                      AND e.tags @> COALESCE(item->'tags', '{}'::jsonb)
                )
            ) INTO v_conflict;
        END IF;
        IF v_conflict THEN
            RETURN false;
        END IF;
    END IF;

    INSERT INTO staging (type, data, tags, metadata, created_at)
    SELECT elem->>'type',
           decode(elem->>'data', 'base64'),
           COALESCE(elem->'tags', '{}'::jsonb),
           CASE WHEN elem->'metadata' IS NULL OR jsonb_typeof(elem->'metadata') = 'null'
                THEN NULL ELSE elem->'metadata' END,
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
	Tags     map[string]string `json:"tags"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// queryItemDTO is the JSON wire shape for one append-condition query item.
// Lowercase, omitempty keys decouple the DB contract from the core QueryItem
// type and match what dcb_append reads (item->'types', item->'tags'); omitempty
// makes an absent Types/Tags a SQL NULL (= no constraint) rather than json null.
type queryItemDTO struct {
	Types []string          `json:"types,omitempty"`
	Tags  map[string]string `json:"tags,omitempty"`
}

// appendArgs marshals events and an optional condition into the arguments shared
// by the dcb_append / dcb_append_boundary functions: the events as a JSON array
// string, the query as a JSON string (or nil => SQL NULL => unconditional), and
// the After cursor. Data is base64-encoded because it is arbitrary bytes.
func appendArgs(events []eventstore.Event, condition *eventstore.AppendCondition) (eventsJSON string, queryArg any, after int64, err error) {
	wire := make([]appendEvent, len(events))
	for i, e := range events {
		tags := e.Tags
		if tags == nil {
			tags = map[string]string{}
		}
		wire[i] = appendEvent{
			Type:     e.Type,
			Data:     base64.StdEncoding.EncodeToString(e.Data),
			Tags:     tags,
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

// Append atomically persists events in a single database round-trip via the
// dcb_append function. If condition is non-nil, the append fails with
// ErrAppendConditionViolated when the store contains matching events after the
// condition's sequence number. The function acquires the global advisory lock
// and performs the check + insert atomically; it is called in autocommit (no
// surrounding transaction) so the whole append is one round-trip.
func (s *Store) Append(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
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
		return fmt.Errorf("could not append events: %w", err)
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
// A condition is NOT keyable — and the per-boundary callers reject it with
// ErrUnkeyableCondition — when it would lock nothing meaningful: a match-all
// (empty Query) or any item lacking a type or a tag. Those can't be expressed as a
// finite (type,tag,value) boundary set, so they'd need the global lock, which does
// not compose with the per-boundary locks.
func boundaryLockKeys(events []eventstore.Event, cond *eventstore.AppendCondition) (keys []int64, keyable bool) {
	set := make(map[int64]struct{})
	if cond != nil {
		if len(cond.Query) == 0 {
			return nil, false // match-all => global lock
		}
		for _, item := range cond.Query {
			if len(item.Types) == 0 || len(item.Tags) == 0 {
				return nil, false // type-less or tag-less => global lock
			}
			for _, t := range item.Types {
				for k, v := range item.Tags {
					set[lockKey(t, k, v)] = struct{}{}
				}
			}
		}
	}
	for _, e := range events {
		for k, v := range e.Tags {
			set[lockKey(e.Type, k, v)] = struct{}{}
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

// AppendPerBoundary is an EXPERIMENTAL alternative to Append that serializes on
// per-(eventType,tagKey,tagValue) advisory locks instead of one global lock, so
// appends to disjoint boundaries run concurrently. Same one-round-trip shape as
// Append. Conditions that aren't keyable (match-all, or any item missing a type or
// a tag) are rejected with ErrUnkeyableCondition — they would need the global lock,
// which does not compose with the per-boundary locks (see ErrUnkeyableCondition).
//
// CORRECTNESS CAVEAT: this is safe for single-boundary conditions, but a
// condition spanning MULTIPLE boundaries can miss an event under concurrency. The
// decision-model read (which computes After) is lock-free and runs before this
// call; because per-boundary locking lets BIGSERIAL positions commit out of
// order, that read can skip a lower-positioned event that commits later, and the
// `sequence_number > After` check then can't catch it. The global Append does not
// have this gap. A gap-safe fix needs commit-order cursors (transaction_id xid8).
func (s *Store) AppendPerBoundary(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
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
		"SELECT dcb_append_boundary(?::jsonb, ?::jsonb, ?::bigint, ?::bigint[])",
		eventsJSON, queryArg, after, bigintArrayLiteral(keys),
	).Scan(&appended).Error
	if err != nil {
		return fmt.Errorf("could not append events (per-boundary): %w", err)
	}
	if !appended {
		return eventstore.ErrAppendConditionViolated
	}
	return nil
}

// AppendHybrid is the fully-consistent per-boundary append: it acquires the
// per-(eventType,tagKey,tagValue) advisory locks and runs the condition check
// under them (so checks for disjoint boundaries run concurrently), then takes the
// global advisory lock only for the insert. Serializing the write under the global
// lock keeps sequence_number in commit order, so — unlike AppendPerBoundary — it is
// correct for MULTI-boundary conditions and keeps the plain sequence-number cursor
// (no xid8). The trade-off is that it gives up group-commit, so it is slower than
// AppendPerBoundary but parallelizes the (often expensive) condition check that the
// global Append serializes. Same one-round-trip shape as Append; conditions that
// aren't keyable are rejected with ErrUnkeyableCondition (see that error).
//
// NOTE: the commit-order guarantee holds only if every append serializes its write
// on the global lock — i.e. mix AppendHybrid with the global Append, not with the
// gap-prone AppendPerBoundary.
func (s *Store) AppendHybrid(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
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
		"SELECT dcb_append_hybrid(?::jsonb, ?::jsonb, ?::bigint, ?::bigint[])",
		eventsJSON, queryArg, after, bigintArrayLiteral(keys),
	).Scan(&appended).Error
	if err != nil {
		return fmt.Errorf("could not append events (hybrid): %w", err)
	}
	if !appended {
		return eventstore.ErrAppendConditionViolated
	}
	return nil
}

// AppendStaging is the sequencer/outbox append: it per-boundary-locks (rejecting
// unkeyable conditions with ErrUnkeyableCondition), checks the condition across
// staging and events in one snapshot, and inserts into the staging buffer.
// A successful append is durable (committed to staging) but not visible to Read
// until the flusher (RunFlusher) moves it into events — eventual read-visibility
// bounded by the flush interval. Unlike AppendPerBoundary it is fully consistent
// for multi-boundary conditions AND keeps the plain uint64 cursor, because events
// is written only by the single flusher. A RunFlusher must be running for appends
// to become visible.
func (s *Store) AppendStaging(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
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
		return fmt.Errorf("could not append events (staging): %w", err)
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

// flushSQL moves the oldest BatchSize staging rows into events in staging_id
// order, in one atomic CTE. One writer (the flusher) ⇒ events.sequence_number is
// assigned gap-free and monotonic; the ORDER BY preserves same-boundary append
// order. created_at is carried over from staging (append time, not flush time).
const flushSQL = `WITH moved AS (
    DELETE FROM staging
    WHERE staging_id IN (SELECT staging_id FROM staging ORDER BY staging_id LIMIT $1)
    RETURNING *
)
INSERT INTO events (type, data, tags, metadata, created_at)
SELECT type, data, tags, metadata, created_at FROM moved ORDER BY staging_id`

// RunFlusher runs the singleton staging→events flusher until ctx is cancelled. It
// holds a session-level advisory lock on a dedicated connection so that across N
// processes exactly one flusher is live (the lock auto-releases — and another
// caller takes over — when the holder's connection dies). It then loops: move a
// batch; if rows moved, drain again immediately; otherwise sleep opts.Interval.
//
// Only the flusher writes events for the staging strategy; running it alongside
// the direct-write Append/AppendPerBoundary/AppendHybrid would add a second writer
// to events and break the gap-free ordering.
func (s *Store) RunFlusher(ctx context.Context, opts FlushOptions) error {
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

	// Acquire leadership (blocks until held or ctx cancelled). The session-level
	// lock releases automatically when conn closes.
	if _, err := conn.ExecContext(ctx, "SELECT pg_advisory_lock($1)", flushLeaderKey); err != nil {
		return fmt.Errorf("flusher: could not acquire leadership: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		res, err := conn.ExecContext(ctx, flushSQL, opts.BatchSize)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("flusher: flush failed: %w", err)
		}
		moved, _ := res.RowsAffected()
		if moved == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(opts.Interval):
			}
		}
	}
}

// Flush moves up to batchSize staging rows into events (one atomic CTE) and
// returns the number moved. It is the single-batch primitive RunFlusher loops on;
// call it directly only when no RunFlusher is active (e.g. tests, manual drain) —
// two concurrent writers to events would break the gap-free ordering.
func (s *Store) Flush(ctx context.Context, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = 5000
	}
	sqlDB, err := s.db.DB()
	if err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}
	res, err := sqlDB.ExecContext(ctx, flushSQL, batchSize)
	if err != nil {
		return 0, fmt.Errorf("flush: %w", err)
	}
	return res.RowsAffected()
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
			Metadata: metadata,
		},
		SequenceNumber: e.SequenceNumber,
		CreatedAt:      e.CreatedAt,
	}, nil
}
