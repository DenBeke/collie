package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/envelope"
	pgstore "github.com/DenBeke/collie/postgres"
)

// stamped wires the envelope middleware in front of a postgres store, the way an
// application does.
func stamped(store *pgstore.Store, options ...envelope.Option) *eventstore.MiddlewareStore {
	return eventstore.NewMiddlewareStore(store,
		eventstore.WithAppendMiddleware(envelope.New(options...).OnAppend),
	)
}

func readAll(t *testing.T, store eventstore.Store) []eventstore.SequencedEvent {
	t.Helper()
	var events []eventstore.SequencedEvent
	stream, _, err := store.Read(context.Background(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		events = append(events, e)
	}
	return events
}

func TestPGEnvelopeRoundTrip(t *testing.T) {
	inner := setupStore(t)
	store := stamped(inner)
	ctx := eventstore.ContextWithCause(eventstore.ContextWithActor(context.Background(), "student:s1"), "request:8f3a")

	e, _ := eventstore.NewEvent("StudentRegistered", "Ada", eventstore.NewTags("student", "s1"))
	err := store.Append(ctx, []eventstore.Event{e}, nil)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	events := readAll(t, store)
	if len(events) != 1 {
		t.Fatalf("read %d events, want 1", len(events))
	}
	if events[0].Actor != "student:s1" {
		t.Errorf("actor = %q, want %q", events[0].Actor, "student:s1")
	}
	if events[0].Cause != "request:8f3a" {
		t.Errorf("cause = %q, want %q", events[0].Cause, "request:8f3a")
	}
	if events[0].RecordedAt.IsZero() {
		t.Error("recorded_at is zero: the store did not stamp acceptance")
	}
}

// TestPGAnonymousAppendStoresNull: an empty actor is a NULL column, not an empty
// string, so it reads the same as a row that predates the envelope.
func TestPGAnonymousAppendStoresNull(t *testing.T) {
	inner := setupStore(t)
	store := stamped(inner, envelope.AllowAnonymous())
	ctx := context.Background()

	e, _ := eventstore.NewEvent("SystemBooted", "v1", nil)
	err := store.Append(ctx, []eventstore.Event{e}, nil)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	events := readAll(t, store)
	if len(events) != 1 || events[0].Actor != "" || events[0].Cause != "" {
		t.Fatalf("read %v, want one event with an empty envelope", events)
	}
}

// TestPGStagedRecordedAtIsAppendTime is the reason recorded_at is written into
// the staging buffer rather than by the flusher: acceptance is when the store
// took the event, and the flush interval must not move it.
func TestPGStagedRecordedAtIsAppendTime(t *testing.T) {
	inner := setupStore(t)
	store := stamped(inner)
	ctx := eventstore.ContextWithActor(context.Background(), "student:s1")

	e, _ := eventstore.NewEvent("StudentRegistered", "Ada", eventstore.NewTags("student", "s1"))
	err := store.Append(ctx, []eventstore.Event{e}, &eventstore.AppendCondition{
		Query: eventstore.Query{{
			Types: []string{"StudentRegistered"},
			Tags:  eventstore.NewTags("student", "s1"),
		}},
	})
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	// The middleware store has no AppendStaging, so the staged append goes
	// straight at the inner store with an already-stamped event.
	staged := e
	staged.Actor = "student:s2"
	staged.Tags = eventstore.NewTags("student", "s2")
	appendedAt := time.Now()
	err = inner.AppendStaging(ctx, []eventstore.Event{staged}, &eventstore.AppendCondition{
		Query: eventstore.Query{{
			Types: []string{"StudentRegistered"},
			Tags:  eventstore.NewTags("student", "s2"),
		}},
	})
	if err != nil {
		t.Fatalf("append staging: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	moved, err := inner.Flush(ctx, 100)
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	if moved != 1 {
		t.Fatalf("flushed %d events, want 1", moved)
	}

	events := readAll(t, store)
	if len(events) != 2 {
		t.Fatalf("read %d events, want 2", len(events))
	}
	flushed := events[1]
	if flushed.Actor != "student:s2" {
		t.Errorf("actor = %q, want %q", flushed.Actor, "student:s2")
	}
	if gap := flushed.RecordedAt.Sub(appendedAt); gap > 200*time.Millisecond {
		t.Errorf("recorded_at is %v after the append: the flush moved it", gap)
	}
}

// TestPGMigrateLeavesOldRowsUnstamped is the migration gotcha, staged: an events
// table that predates the envelope is one without the columns. Adding
// recorded_at with a volatile default would evaluate now() for every existing
// row, so the assertion is that the old rows come back NULL — zero on the Go
// side — while the next append is stamped.
func TestPGMigrateLeavesOldRowsUnstamped(t *testing.T) {
	db := setupDB(t)
	inner := pgstore.New(db)
	err := inner.Migrate()
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}

	ctx := context.Background()
	old, _ := eventstore.NewEvent("StudentRegistered", "Ada", eventstore.NewTags("student", "s1"))
	err = inner.Append(ctx, []eventstore.Event{old}, nil)
	if err != nil {
		t.Fatalf("append the historical event: %v", err)
	}

	// Turn the log back into a pre-envelope one.
	for _, column := range []string{"actor", "cause", "recorded_at"} {
		err = db.Exec("ALTER TABLE events DROP COLUMN " + column).Error
		if err != nil {
			t.Fatalf("drop %s: %v", column, err)
		}
	}

	err = inner.Migrate()
	if err != nil {
		t.Fatalf("re-migrate: %v", err)
	}

	store := stamped(inner)
	fresh, _ := eventstore.NewEvent("StudentRegistered", "Bob", eventstore.NewTags("student", "s2"))
	err = store.Append(eventstore.ContextWithActor(ctx, "student:s2"), []eventstore.Event{fresh}, nil)
	if err != nil {
		t.Fatalf("append after the migration: %v", err)
	}

	events := readAll(t, store)
	if len(events) != 2 {
		t.Fatalf("read %d events, want 2", len(events))
	}
	if !events[0].RecordedAt.IsZero() {
		t.Errorf("the historical event is stamped %v: the migration forged its history", events[0].RecordedAt)
	}
	if events[0].Actor != "" {
		t.Errorf("the historical event has actor %q, want none", events[0].Actor)
	}
	if events[1].RecordedAt.IsZero() {
		t.Error("the new event is unstamped: the default did not survive the migration")
	}
	if events[1].Actor != "student:s2" {
		t.Errorf("actor = %q, want %q", events[1].Actor, "student:s2")
	}
}

// oldShape restores the two-timestamp schema recorded_at replaced: created_at,
// its index, and one historical row carrying a created_at but no recorded_at.
var oldShape = []string{
	`ALTER TABLE events ADD COLUMN created_at timestamptz`,
	`ALTER TABLE staging ADD COLUMN created_at timestamptz`,
	`CREATE INDEX idx_events_created_at ON events (created_at)`,
	`INSERT INTO events (type, data, tags, recorded_at, created_at) VALUES ('StudentRegistered',
		'"Ada"'::bytea, '[{"key":"student","value":"s1"}]'::jsonb, NULL, now() - interval '1 year')`,
	`INSERT INTO event_tags (key, value, sequence_number) SELECT 'student', 's1', max(sequence_number) FROM events`,
}

// TestPGMigrateDropsCreatedAt is the other half of the consolidation: created_at
// was the application-clock timestamp, recorded_at is the database one, and only
// the second is kept. Migrate drops the column on both tables — deliberately
// without backfilling recorded_at, so a historical row stays unstamped rather
// than being credited with a clock that never meant acceptance — and every write
// path keeps working, which requires the dcb_* functions to have stopped naming
// the column before it went.
func TestPGMigrateDropsCreatedAt(t *testing.T) {
	db := setupDB(t)
	inner := pgstore.New(db)
	err := inner.Migrate()
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	for _, statement := range oldShape {
		err = db.Exec(statement).Error
		if err != nil {
			t.Fatalf("restore the old shape (%s): %v", statement, err)
		}
	}

	// Twice: dropping a column that is already gone must not break a redeploy.
	for range 2 {
		err = inner.Migrate()
		if err != nil {
			t.Fatalf("re-migrate: %v", err)
		}
	}

	var remaining []string
	err = db.Raw(`SELECT table_name || '.' || column_name FROM information_schema.columns
		WHERE table_name IN ('events', 'staging') AND column_name = 'created_at'`).Scan(&remaining).Error
	if err != nil {
		t.Fatalf("inspect the columns: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("created_at survived the migration on %v", remaining)
	}

	ctx := context.Background()
	direct, _ := eventstore.NewEvent("StudentRegistered", "Bob", eventstore.NewTags("student", "s2"))
	err = inner.Append(ctx, []eventstore.Event{direct}, nil)
	if err != nil {
		t.Fatalf("append after the migration: %v", err)
	}

	staged, _ := eventstore.NewEvent("StudentRegistered", "Cleo", eventstore.NewTags("student", "s3"))
	err = inner.AppendStaging(ctx, []eventstore.Event{staged}, nil)
	if err != nil {
		t.Fatalf("staged append after the migration: %v", err)
	}
	moved, err := inner.Flush(ctx, 100)
	if err != nil {
		t.Fatalf("flush after the migration: %v", err)
	}
	if moved != 1 {
		t.Fatalf("flushed %d events, want 1", moved)
	}

	events := readAll(t, inner)
	if len(events) != 3 {
		t.Fatalf("read %d events, want 3", len(events))
	}
	if !events[0].RecordedAt.IsZero() {
		t.Errorf("the historical event is stamped %v: created_at was backfilled", events[0].RecordedAt)
	}
	for _, e := range events[1:] {
		if e.RecordedAt.IsZero() {
			t.Errorf("event %d is unstamped after the migration", e.SequenceNumber)
		}
	}
}
