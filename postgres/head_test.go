package postgres_test

import (
	"context"
	"testing"

	"gorm.io/gorm"

	eventstore "github.com/DenBeke/collie"
	pgstore "github.com/DenBeke/collie/postgres"
)

func pgAppend(t *testing.T, store *pgstore.Store, eventType string, n int) {
	t.Helper()
	events := make([]eventstore.Event, n)
	for i := range events {
		e, err := eventstore.NewEvent(eventType, nil, eventstore.NewTags("id", eventType))
		if err != nil {
			t.Fatal(err)
		}
		events[i] = e
	}
	if err := store.Append(context.Background(), events, nil); err != nil {
		t.Fatal(err)
	}
}

func migratedStore(t *testing.T) (*pgstore.Store, *gorm.DB) {
	t.Helper()
	db := setupDB(t)
	store := pgstore.New(db)
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := pgstore.MigrateProjections(db); err != nil {
		t.Fatal(err)
	}
	return store, db
}

func TestReadReturnsHead(t *testing.T) {
	store, _ := migratedStore(t)
	ctx := context.Background()

	_, head, err := store.Read(ctx, nil, 0)
	if err != nil || head != 0 {
		t.Fatalf("empty log: head=%d err=%v, want 0 and nil", head, err)
	}

	pgAppend(t, store, "Other", 2)
	events, head, err := store.Read(ctx, eventstore.Query{{Types: []string{"Wanted"}}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("head=%d, want 2 even though nothing matches", head)
	}

	// Appended after Read, before iterating: yielded, and above the head.
	pgAppend(t, store, "Wanted", 1)
	var last uint64
	for e, err := range events {
		if err != nil {
			t.Fatal(err)
		}
		last = e.SequenceNumber
	}
	if last != 3 {
		t.Fatalf("last=%d, want 3", last)
	}
}

func TestReadReturnsTheHeadError(t *testing.T) {
	store, _ := migratedStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	events, head, err := store.Read(ctx, nil, 0)
	if err == nil {
		t.Fatal("expected an error for a head that could not be read")
	}
	if events != nil || head != 0 {
		t.Fatalf("events=%v head=%d, want nil and 0 on error", events != nil, head)
	}
}

// Staged events have no sequence number until the flusher moves them, so they
// are not part of the head.
func TestReadHeadIgnoresStaging(t *testing.T) {
	store, _ := migratedStore(t)
	ctx := context.Background()

	e, err := eventstore.NewEvent("Staged", nil, eventstore.NewTags("id", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AppendStaging(ctx, []eventstore.Event{e, e}, nil); err != nil {
		t.Fatal(err)
	}
	if _, head, err := store.Read(ctx, nil, 0); err != nil || head != 0 {
		t.Fatalf("before flush: head=%d err=%v, want 0", head, err)
	}

	if _, err := store.Flush(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, head, err := store.Read(ctx, nil, 0); err != nil || head != 2 {
		t.Fatalf("after flush: head=%d err=%v, want 2", head, err)
	}
}

func TestKeyedProjectionRecordsHeadPastTheGap(t *testing.T) {
	_, db := migratedStore(t)
	ctx := context.Background()
	open := func() *pgstore.KeyedProjection[kpJob] {
		t.Helper()
		p, err := pgstore.NewKeyedProjection(db, "jobs", eventstore.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	a, stale := open(), open()
	if err := a.Checkpoint(ctx, 9_999); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 0 {
		t.Fatalf("below the gap: stored=%d, want 0", got)
	}

	if err := a.Checkpoint(ctx, 20_000); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 20_000 {
		t.Fatalf("past the gap: stored=%d, want 20000", got)
	}

	// An instance that is behind must not move the stored position back.
	if err := stale.Checkpoint(ctx, 15_000); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 20_000 {
		t.Fatalf("after a stale checkpoint: stored=%d, want 20000", got)
	}
}

func TestKeyedProjectionAdvancesOverASparseLog(t *testing.T) {
	store, db := migratedStore(t)
	ctx := context.Background()
	pgAppend(t, store, "Other", 10_001)

	p, err := pgstore.NewKeyedProjection(db, "jobs", eventstore.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, p); err != nil {
		t.Fatal(err)
	}
	if p.After() != 10_001 {
		t.Fatalf("after=%d, want the head (10001)", p.After())
	}

	reopened, err := pgstore.NewKeyedProjection(db, "jobs", eventstore.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.After() != 10_001 {
		t.Fatalf("reopened after=%d, want 10001", reopened.After())
	}
}
