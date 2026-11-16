// Tests use testcontainers-go with PostgreSQL 17.
//
// With Colima (macOS), set these environment variables before running:
//
//	export DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock"
//	export TESTCONTAINERS_RYUK_DISABLED=true
//
// Then: go test ./postgres/ -timeout 300s
package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/encryption"
	pgstore "github.com/DenBeke/collie/postgres"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gormPG "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func setupDB(tb testing.TB) *gorm.DB {
	tb.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx, "postgres:17",
		postgres.WithDatabase("github.com/DenBeke/collie_test"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(
			wait.ForAll(
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(30*time.Second),
				wait.ForListeningPort("5432/tcp"),
			),
		),
	)
	if err != nil {
		tb.Fatalf("could not start postgres container: %v", err)
	}
	tb.Cleanup(func() { _ = container.Terminate(ctx) })

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		tb.Fatalf("could not get connection string: %v", err)
	}

	// Retry connection — Colima port forwarding from VM to host can lag
	// behind the container being ready.
	var db *gorm.DB
	for range 10 {
		db, err = gorm.Open(gormPG.Open(connStr), &gorm.Config{})
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		tb.Fatalf("could not connect to database: %v", err)
	}

	return db
}

// setupStore is a store in global-lock mode: most tests here are about what
// Append and Read mean, which is the same in both modes, and need an event to
// be readable as soon as Append returns. The staging tests use
// setupStagingStore.
func setupStore(tb testing.TB, opts ...pgstore.Option) *pgstore.Store {
	tb.Helper()
	db := setupDB(tb)
	store := pgstore.New(db, append([]pgstore.Option{pgstore.WithGlobalLock()}, opts...)...)
	if err := store.Migrate(); err != nil {
		tb.Fatalf("could not migrate: %v", err)
	}
	return store
}

// setupStagingStore is a store in staging mode without its own flusher, so a
// test decides when staged appends move into events (flushAll).
func setupStagingStore(tb testing.TB, opts ...pgstore.Option) *pgstore.Store {
	tb.Helper()
	db := setupDB(tb)
	store := pgstore.New(db, append([]pgstore.Option{pgstore.WithoutFlusher()}, opts...)...)
	if err := store.Migrate(); err != nil {
		tb.Fatalf("could not migrate: %v", err)
	}
	return store
}

func setupEncryptedStore(tb testing.TB) (*collie.MiddlewareStore, *encryption.EventEncryptor) {
	tb.Helper()
	db := setupDB(tb)
	if err := pgstore.MigrateKeyStore(db); err != nil {
		tb.Fatalf("could not migrate key store: %v", err)
	}
	keys := pgstore.NewKeyStore(db)
	enc := encryption.NewEventEncryptor(keys)
	store := pgstore.New(db, pgstore.WithGlobalLock())
	if err := store.Migrate(); err != nil {
		tb.Fatalf("could not migrate: %v", err)
	}
	mw := collie.NewMiddlewareStore(store,
		collie.WithAppendMiddleware(enc.OnAppend),
		collie.WithReadMiddleware(enc.OnRead),
	)
	return mw, enc
}

func TestAppendAndRead(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags("username", "bob"))

	if err := store.Append(ctx, []collie.Event{e1, e2}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if events[0].Type != "UserSignedUp" {
		t.Errorf("expected type UserSignedUp, got %s", events[0].Type)
	}
	if vs := events[0].TagValues("username"); len(vs) != 1 || vs[0] != "alice" {
		t.Errorf("expected tag username=alice, got %v", vs)
	}
	if vs := events[1].TagValues("username"); len(vs) != 1 || vs[0] != "bob" {
		t.Errorf("expected tag username=bob, got %v", vs)
	}
	if events[0].SequenceNumber >= events[1].SequenceNumber {
		t.Errorf("sequence numbers should be increasing")
	}
}

func TestAppendConditionPasses(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
	if err := store.Append(ctx, []collie.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	condition := &collie.AppendCondition{
		Query: collie.Query{{
			Types: []string{"UserSignedUp"},
			Tags:  collie.NewTags("username", "bob"),
		}},
		After: 0,
	}

	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags("username", "bob"))
	if err := store.Append(ctx, []collie.Event{e2}, condition); err != nil {
		t.Fatalf("expected append to succeed, got: %v", err)
	}
}

func TestAppendConditionViolated(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
	if err := store.Append(ctx, []collie.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	condition := &collie.AppendCondition{
		Query: collie.Query{{
			Types: []string{"UserSignedUp"},
			Tags:  collie.NewTags("username", "alice"),
		}},
		After: 0,
	}

	e2, _ := collie.NewEvent("UserSignedUp", "alice2", collie.NewTags("username", "alice"))
	err := store.Append(ctx, []collie.Event{e2}, condition)
	if err != collie.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated, got: %v", err)
	}
}

func TestAppendConditionEmptyQueryMatchesAll(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
	if err := store.Append(ctx, []collie.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// A non-nil condition with an empty Query matches ALL events, so appending
	// with After: 0 is rejected because an event already exists after 0.
	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags("username", "bob"))
	err := store.Append(ctx, []collie.Event{e2}, &collie.AppendCondition{Query: collie.Query{}, After: 0})
	if err != collie.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated for empty-query match-all, got: %v", err)
	}

	// The same empty-query condition passes once After is at the head (no events
	// exist after it).
	var head uint64
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for ev, err := range stream {
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		head = ev.SequenceNumber
	}
	if err := store.Append(ctx, []collie.Event{e2}, &collie.AppendCondition{Query: collie.Query{}, After: head}); err != nil {
		t.Fatalf("expected append to succeed when After is at head, got: %v", err)
	}
}

func TestTagsFiltering(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice", "tenant", "t1"))
	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags("username", "bob", "tenant", "t2"))
	e3, _ := collie.NewEvent("UserSignedUp", "charlie", collie.NewTags("username", "charlie", "tenant", "t1"))
	if err := store.Append(ctx, []collie.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	query := collie.Query{{
		Types: []string{"UserSignedUp"},
		Tags:  collie.NewTags("tenant", "t1"),
	}}

	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, query, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events for tenant t1, got %d", len(events))
	}
}

func TestReadAfter(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("A", nil, nil)
	e2, _ := collie.NewEvent("B", nil, nil)
	e3, _ := collie.NewEvent("C", nil, nil)
	if err := store.Append(ctx, []collie.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	var all []collie.SequencedEvent
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		all = append(all, e)
	}

	var after []collie.SequencedEvent
	stream, _, err = store.Read(ctx, nil, all[1].SequenceNumber)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		after = append(after, e)
	}

	if len(after) != 1 {
		t.Fatalf("expected 1 event after seq %d, got %d", all[1].SequenceNumber, len(after))
	}
	if after[0].Type != "C" {
		t.Errorf("expected type C, got %s", after[0].Type)
	}
}

func TestBatching(t *testing.T) {
	store := setupStore(t, pgstore.WithBatchSize(2))
	ctx := context.Background()

	for i := range 5 {
		e, _ := collie.NewEvent("Event", fmt.Sprintf("data-%d", i), nil)
		if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 5 {
		t.Fatalf("expected 5 events with batch size 2, got %d", len(events))
	}
}

func TestMultiTagEventFiltering(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("tenant_id", "t1", "username", "alice"))
	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags("tenant_id", "t2", "username", "bob"))
	e3, _ := collie.NewEvent("UserSignedUp", "charlie", collie.NewTags("tenant_id", "t1", "username", "charlie"))
	if err := store.Append(ctx, []collie.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	query := collie.Query{{
		Tags: collie.NewTags("tenant_id", "t1"),
	}}

	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, query, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events for tenant_id t1, got %d", len(events))
	}

	query2 := collie.Query{{
		Tags: collie.NewTags("username", "bob"),
	}}

	var events2 []collie.SequencedEvent
	stream, _, err = store.Read(ctx, query2, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events2 = append(events2, e)
	}

	if len(events2) != 1 {
		t.Fatalf("expected 1 event for username bob, got %d", len(events2))
	}
}

// TestMultiValueTags exercises the jsonb array-containment path for events that
// carry the same key with multiple values (a transfer touching two accounts).
func TestMultiValueTags(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	transfer, _ := collie.NewEvent("Transferred", nil, collie.NewTags("account", "A", "account", "B"))
	other, _ := collie.NewEvent("Transferred", nil, collie.NewTags("account", "C", "account", "D"))
	if err := store.Append(ctx, []collie.Event{transfer, other}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	read := func(q collie.Query) int {
		t.Helper()
		n := 0
		stream, _, err := store.Read(ctx, q, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, err := range stream {
			if err != nil {
				t.Fatalf("read error: %v", err)
			}
			n++
		}
		return n
	}

	if n := read(collie.Query{{Tags: collie.NewTags("account", "A")}}); n != 1 {
		t.Errorf("touching A: got %d, want 1", n)
	}
	if n := read(collie.Query{{Tags: collie.NewTags("account", "B")}}); n != 1 {
		t.Errorf("touching B: got %d, want 1", n)
	}
	if n := read(collie.Query{{Tags: collie.NewTags("account", "A", "account", "B")}}); n != 1 {
		t.Errorf("touching A AND B (same event): got %d, want 1", n)
	}
	if n := read(collie.Query{{Tags: collie.NewTags("account", "A", "account", "C")}}); n != 0 {
		t.Errorf("touching A AND C (no single event has both): got %d, want 0", n)
	}
	if n := read(collie.Query{{Tags: collie.NewTags("account", "Z")}}); n != 0 {
		t.Errorf("touching Z: got %d, want 0", n)
	}
}

// TestMultiValueConditionConflict checks the DCB condition fires when a new
// append's guard query shares a value with a previously-written multi-value tag.
func TestMultiValueConditionConflict(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	t1, _ := collie.NewEvent("Transferred", nil, collie.NewTags("account", "A", "account", "B"))
	if err := store.Append(ctx, []collie.Event{t1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	t2, _ := collie.NewEvent("Transferred", nil, collie.NewTags("account", "A", "account", "C"))

	// Guarded by "no Transferred touching account A since 0" — must be rejected,
	// because t1 touched A.
	condA := &collie.AppendCondition{
		Query: collie.Query{{Types: []string{"Transferred"}, Tags: collie.NewTags("account", "A")}},
		After: 0,
	}
	if err := store.Append(ctx, []collie.Event{t2}, condA); err != collie.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated for shared account-A boundary, got: %v", err)
	}

	// Guarded on an untouched account D — must succeed.
	condD := &collie.AppendCondition{
		Query: collie.Query{{Types: []string{"Transferred"}, Tags: collie.NewTags("account", "D")}},
		After: 0,
	}
	if err := store.Append(ctx, []collie.Event{t2}, condD); err != nil {
		t.Fatalf("expected append guarded on untouched account D to succeed, got: %v", err)
	}
}

func TestAppendConditionConcurrency(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	const n = 10
	errs := make(chan error, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			e, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
			condition := &collie.AppendCondition{
				Query: collie.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  collie.NewTags("username", "alice"),
				}},
				After: 0,
			}
			errs <- store.Append(ctx, []collie.Event{e}, condition)
		}()
	}
	wg.Wait()
	close(errs)

	var succeeded int
	for err := range errs {
		if err == nil {
			succeeded++
		} else if err != collie.ErrAppendConditionViolated {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful append, got %d", succeeded)
	}

	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event in store, got %d", len(events))
	}
}

// --- Staging (sequencer/outbox) tests ---

// flushAll drains the staging buffer into events synchronously (test helper).
func flushAll(t testing.TB, store *pgstore.Store) {
	t.Helper()
	ctx := context.Background()
	for {
		n, err := store.FlushForTest(ctx, 10000)
		if err != nil {
			t.Fatalf("flush: %v", err)
		}
		if n == 0 {
			return
		}
	}
}

// TestStagingSameBoundaryConcurrency: 10 racing same-boundary conditional appends
// land in staging, but the per-boundary lock + the staging side of the check let
// exactly 1 win (the others see the first one's pending staging row).
func TestStagingSameBoundaryConcurrency(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()

	const n = 10
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			e, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags("username", "alice"))
			condition := &collie.AppendCondition{
				Query: collie.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  collie.NewTags("username", "alice"),
				}},
				After: 0,
			}
			errs <- store.Append(ctx, []collie.Event{e}, condition)
		}()
	}
	wg.Wait()
	close(errs)

	var succeeded int
	for err := range errs {
		if err == nil {
			succeeded++
		} else if err != collie.ErrAppendConditionViolated {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful append, got %d", succeeded)
	}
}

// TestStagingDisjointConcurrency: N disjoint appends all succeed; after a flush,
// events holds exactly N rows.
func TestStagingDisjointConcurrency(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()

	const n = 20
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			user := fmt.Sprintf("user-%d", i)
			e, _ := collie.NewEvent("UserSignedUp", user, collie.NewTags("username", user))
			condition := &collie.AppendCondition{
				Query: collie.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  collie.NewTags("username", user),
				}},
				After: 0,
			}
			errs <- store.Append(ctx, []collie.Event{e}, condition)
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Errorf("expected all disjoint appends to succeed, got: %v", err)
		}
	}
	flushAll(t, store)
	var count int
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		count++
	}
	if count != n {
		t.Fatalf("expected %d events after flush, got %d", n, count)
	}
}

// TestStagingFlushMonotonic: many concurrent disjoint appends, then flush; events
// must hold every appended event exactly once with strictly increasing sequence
// numbers (single-writer flush in staging_id order ⇒ gap-free monotonic).
func TestStagingFlushMonotonic(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			ent := fmt.Sprintf("e-%d", i)
			e, _ := collie.NewEvent("Thing", ent, collie.NewTags("entity", ent))
			if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
				t.Errorf("append %s: %v", ent, err)
			}
		}(i)
	}
	wg.Wait()
	flushAll(t, store)

	var prev uint64
	var count int
	seen := make(map[string]bool)
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		if e.SequenceNumber <= prev {
			t.Fatalf("sequence numbers not strictly increasing: %d after %d", e.SequenceNumber, prev)
		}
		prev = e.SequenceNumber
		if vs := e.TagValues("entity"); len(vs) > 0 {
			ent := vs[0]
			if seen[ent] {
				t.Fatalf("duplicate event for %s", ent)
			}
			seen[ent] = true
		}
		count++
	}
	if count != n {
		t.Fatalf("expected %d events after flush (no loss), got %d", n, count)
	}
}

// TestStagingMultiBoundaryConsistent: an event pending in staging (not yet
// flushed) is still seen by a later multi-boundary condition check — the
// out-of-order-commit position gap that makes plain per-boundary locking unsafe
// (see appendbench-conclusion.md) cannot form here, because the staging check reads
// staging ∪ events in one snapshot.
func TestStagingMultiBoundaryConsistent(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()

	// X lands in staging (course c2), unconditional — not yet flushed to events.
	x, _ := collie.NewEvent("Enrolled", nil, collie.NewTags("course", "c2"))
	if err := store.Append(ctx, []collie.Event{x}, nil); err != nil {
		t.Fatalf("append X: %v", err)
	}

	// Y guarded by (Enrolled@course:c2 OR Enrolled@student:s1) from After=0. X
	// matches course:c2 and is pending in staging, so Y must conflict — even though
	// X is not yet in events.
	y, _ := collie.NewEvent("Enrolled", nil, collie.NewTags("student", "s1"))
	cond := &collie.AppendCondition{
		Query: collie.Query{
			{Types: []string{"Enrolled"}, Tags: collie.NewTags("course", "c2")},
			{Types: []string{"Enrolled"}, Tags: collie.NewTags("student", "s1")},
		},
		After: 0,
	}
	if err := store.Append(ctx, []collie.Event{y}, cond); err != collie.ErrAppendConditionViolated {
		t.Fatalf("expected conflict from the pending staging event (multi-boundary), got: %v", err)
	}
}

// TestStagingNoDeadlock: overlapping multi-key appends acquire boundary keys in
// sorted order, so nothing deadlocks (40P01).
func TestStagingNoDeadlock(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()

	const n = 40
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			tags := collie.NewTags(
				fmt.Sprintf("slot%d", i%4), "x",
				fmt.Sprintf("slot%d", (i+1)%4), "x",
			)
			e, _ := collie.NewEvent("Slot", nil, tags)
			errs <- store.Append(ctx, []collie.Event{e}, nil)
		}(i)
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected error (possible deadlock): %v", err)
		}
	}
}

// TestUnkeyableConditionRejected: the staging Append rejects conditions that can't be
// keyed (match-all, type-less, tag-less) up front with ErrUnkeyableCondition,
// instead of silently falling back to the global lock (which doesn't compose with
// per-boundary locks). A keyable condition is still accepted.
func TestUnkeyableConditionRejected(t *testing.T) {
	store := setupStagingStore(t)
	ctx := context.Background()
	e, _ := collie.NewEvent("Thing", nil, collie.NewTags("k", "v"))

	unkeyable := []*collie.AppendCondition{
		{Query: nil, After: 0}, // match-all (empty query)
		{Query: collie.Query{{Tags: collie.NewTags("email", "x")}}, After: 0}, // type-less
		{Query: collie.Query{{Types: []string{"Thing"}}}, After: 0},           // tag-less
	}
	for _, c := range unkeyable {
		if err := store.Append(ctx, []collie.Event{e}, c); !errors.Is(err, pgstore.ErrUnkeyableCondition) {
			t.Errorf("staging Append with unkeyable condition %+v: want ErrUnkeyableCondition, got %v", c.Query, err)
		}
	}

	// A keyable condition is still accepted.
	keyable := &collie.AppendCondition{
		Query: collie.Query{{Types: []string{"Thing"}, Tags: collie.NewTags("k", "v")}},
		After: 0,
	}
	if err := store.Append(ctx, []collie.Event{e}, keyable); err != nil {
		t.Errorf("keyable condition should be accepted, got %v", err)
	}
}

// --- Encryption integration tests ---

func collectPGEvents(t *testing.T, store collie.Store, query collie.Query) []collie.SequencedEvent {
	t.Helper()
	ctx := context.Background()
	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, query, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		events = append(events, e)
	}
	return events
}

func TestPGEncryptedRoundTrip(t *testing.T) {
	store, _ := setupEncryptedStore(t)
	ctx := context.Background()

	type user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	userData := user{Name: "Alice", Email: "alice@example.com"}

	e, err := collie.NewEvent("UserSignedUp", userData, collie.NewTags(
		"userId", "user-123",
	), encryption.WithEncryptionKeys("userId"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := collie.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if data.Name != "Alice" || data.Email != "alice@example.com" {
		t.Fatalf("unexpected data: %+v", data)
	}
}

func TestPGEncryptedAtRest(t *testing.T) {
	db := setupDB(t)
	if err := pgstore.MigrateKeyStore(db); err != nil {
		t.Fatalf("could not migrate key store: %v", err)
	}
	keys := pgstore.NewKeyStore(db)
	enc := encryption.NewEventEncryptor(keys)
	raw := pgstore.New(db, pgstore.WithGlobalLock())
	if err := raw.Migrate(); err != nil {
		t.Fatalf("could not migrate: %v", err)
	}
	store := collie.NewMiddlewareStore(raw,
		collie.WithAppendMiddleware(enc.OnAppend),
		collie.WithReadMiddleware(enc.OnRead),
	)

	ctx := context.Background()
	e, _ := collie.NewEvent("UserSignedUp", "secret-data", collie.NewTags(
		"userId", "user-123",
	), encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Query raw database to verify data is encrypted.
	var rawData struct{ Data []byte }
	if err := db.Table("events").Select("data").First(&rawData).Error; err != nil {
		t.Fatalf("could not read raw event: %v", err)
	}
	if string(rawData.Data) == `"secret-data"` {
		t.Fatal("data should be encrypted at rest, but found plaintext")
	}
}

func TestPGForgetRedactsData(t *testing.T) {
	store, enc := setupEncryptedStore(t)
	ctx := context.Background()

	e, _ := collie.NewEvent("UserSignedUp", "alice-data", collie.NewTags(
		"userId", "user-123",
	), encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, collie.NewTags("userId", "user-123")); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected event to be redacted after forget")
	}
	if events[0].Type != "UserSignedUp" {
		t.Fatalf("expected type preserved, got %s", events[0].Type)
	}
	if vs := events[0].TagValues("userId"); len(vs) != 1 || vs[0] != "user-123" {
		t.Fatalf("expected tags preserved, got %v", events[0].Tags)
	}
}

func TestPGForgetDoesNotAffectOtherSubjects(t *testing.T) {
	store, enc := setupEncryptedStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags(
		"userId", "user-1",
	), encryption.WithEncryptionKeys("userId"))
	e2, _ := collie.NewEvent("UserSignedUp", "bob", collie.NewTags(
		"userId", "user-2",
	), encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []collie.Event{e1, e2}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, collie.NewTags("userId", "user-1")); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected user-1 to be redacted")
	}
	data, err := collie.EventData[string](events[1])
	if err != nil {
		t.Fatalf("unexpected error reading user-2: %v", err)
	}
	if data != "bob" {
		t.Fatalf("expected bob, got %s", data)
	}
}

func TestPGSupersetForget(t *testing.T) {
	store, enc := setupEncryptedStore(t)
	ctx := context.Background()

	e, _ := collie.NewEvent("FriendRequest", "request-data", collie.NewTags(
		"senderId", "user-1",
		"receiverId", "user-2",
	), encryption.WithEncryptionKeys("senderId", "receiverId"))
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, collie.NewTags("senderId", "user-1")); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected event to be redacted after superset forget")
	}
}

func TestPGMixedEncryptedAndPlainEvents(t *testing.T) {
	store, enc := setupEncryptedStore(t)
	ctx := context.Background()

	plain, _ := collie.NewEvent("SystemBooted", "v1.0", nil)
	encrypted, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags(
		"userId", "user-1",
	), encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []collie.Event{plain, encrypted}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, collie.NewTags("userId", "user-1")); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	data, err := collie.EventData[string](events[0])
	if err != nil || data != "v1.0" {
		t.Fatalf("plain event should be readable: data=%s, err=%v", data, err)
	}
	if !encryption.IsRedacted(events[1]) {
		t.Fatal("expected encrypted event to be redacted after forget")
	}
}

func TestPGAppendConditionThroughEncryption(t *testing.T) {
	store, _ := setupEncryptedStore(t)
	ctx := context.Background()

	e1, _ := collie.NewEvent("UserSignedUp", "alice", collie.NewTags(
		"userId", "user-1",
	), encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []collie.Event{e1}, nil); err != nil {
		t.Fatal(err)
	}

	condition := &collie.AppendCondition{
		Query: collie.Query{
			collie.QueryItem{Types: []string{"UserSignedUp"}, Tags: collie.NewTags("userId", "user-1")},
		},
		After: 0,
	}

	e2, _ := collie.NewEvent("UserSignedUp", "alice-again", collie.NewTags(
		"userId", "user-1",
	), encryption.WithEncryptionKeys("userId"))
	err := store.Append(ctx, []collie.Event{e2}, condition)
	if err != collie.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated, got %v", err)
	}
}

func BenchmarkAppendConcurrent(b *testing.B) {
	store := setupStore(b)
	ctx := context.Background()
	var counter atomic.Int64

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			username := fmt.Sprintf("user-%d", id)
			e, _ := collie.NewEvent("UserSignedUp", username, collie.NewTags("username", username))
			condition := &collie.AppendCondition{
				Query: collie.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  collie.NewTags("username", username),
				}},
				After: 0,
			}
			if err := store.Append(ctx, []collie.Event{e}, condition); err != nil {
				b.Errorf("append failed: %v", err)
			}
		}
	})
}

// TestNewEventFromRoundTrip: tags derived from struct fields come back from
// Postgres in declaration order, repeated key included, and the payload still
// decodes.
func TestNewEventFromRoundTrip(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	type messageSent struct {
		From string `json:"from" dcb:"tag:user"`
		To   string `json:"to"   dcb:"tag:user"`
		Body string `json:"body"`
	}
	sent, err := collie.NewEventFrom("MessageSent", messageSent{From: "alice", To: "bob", Body: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{sent}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	count := 0
	stream, _, err := store.Read(ctx, collie.Query{{Tags: collie.NewTags("user", "bob")}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		count++
		if want := collie.NewTags("user", "alice", "user", "bob"); !slices.Equal(e.Tags, want) {
			t.Errorf("tags = %v, want %v", e.Tags, want)
		}
		msg, err := collie.EventData[messageSent](e)
		if err != nil {
			t.Fatal(err)
		}
		if msg.From != "alice" || msg.To != "bob" || msg.Body != "hi" {
			t.Errorf("payload = %+v", msg)
		}
	}
	if count != 1 {
		t.Errorf("user bob: got %d events, want 1", count)
	}
}

func pgAppend(t *testing.T, store *pgstore.Store, eventType string, n int) {
	t.Helper()
	events := make([]collie.Event, n)
	for i := range events {
		e, err := collie.NewEvent(eventType, nil, collie.NewTags("id", eventType))
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
	store := pgstore.New(db, pgstore.WithGlobalLock())
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
	events, head, err := store.Read(ctx, collie.Query{{Types: []string{"Wanted"}}}, 0)
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
	store := setupStagingStore(t)
	ctx := context.Background()

	e, err := collie.NewEvent("Staged", nil, collie.NewTags("id", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e, e}, nil); err != nil {
		t.Fatal(err)
	}
	if _, head, err := store.Read(ctx, nil, 0); err != nil || head != 0 {
		t.Fatalf("before flush: head=%d err=%v, want 0", head, err)
	}

	if _, err := store.FlushForTest(ctx, 0); err != nil {
		t.Fatal(err)
	}
	if _, head, err := store.Read(ctx, nil, 0); err != nil || head != 2 {
		t.Fatalf("after flush: head=%d err=%v, want 2", head, err)
	}
}
