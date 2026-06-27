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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"eventstore"
	"eventstore/encryption"
	pgstore "eventstore/postgres"

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
		postgres.WithDatabase("eventstore_test"),
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

func setupStore(tb testing.TB, opts ...pgstore.Option) *pgstore.Store {
	tb.Helper()
	db := setupDB(tb)
	store := pgstore.New(db, opts...)
	if err := store.Migrate(); err != nil {
		tb.Fatalf("could not migrate: %v", err)
	}
	return store
}

func setupEncryptedStore(tb testing.TB) (*eventstore.MiddlewareStore, *encryption.EventEncryptor) {
	tb.Helper()
	db := setupDB(tb)
	if err := pgstore.MigrateKeyStore(db); err != nil {
		tb.Fatalf("could not migrate key store: %v", err)
	}
	keys := pgstore.NewKeyStore(db)
	enc := encryption.NewEventEncryptor(keys)
	store := pgstore.New(db)
	if err := store.Migrate(); err != nil {
		tb.Fatalf("could not migrate: %v", err)
	}
	mw := eventstore.NewMiddlewareStore(store,
		eventstore.WithAppendMiddleware(enc.OnAppend),
		eventstore.WithReadMiddleware(enc.OnRead),
	)
	return mw, enc
}

func TestAppendAndRead(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{"username": "bob"})

	if err := store.Append(ctx, []eventstore.Event{e1, e2}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, nil, 0) {
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
	if events[0].Tags["username"] != "alice" {
		t.Errorf("expected tag username=alice, got %s", events[0].Tags["username"])
	}
	if events[1].Tags["username"] != "bob" {
		t.Errorf("expected tag username=bob, got %s", events[1].Tags["username"])
	}
	if events[0].SequenceNumber >= events[1].SequenceNumber {
		t.Errorf("sequence numbers should be increasing")
	}
}

func TestAppendConditionPasses(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
	if err := store.Append(ctx, []eventstore.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	condition := &eventstore.AppendCondition{
		Query: eventstore.Query{{
			Types: []string{"UserSignedUp"},
			Tags:  map[string]string{"username": "bob"},
		}},
		After: 0,
	}

	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{"username": "bob"})
	if err := store.Append(ctx, []eventstore.Event{e2}, condition); err != nil {
		t.Fatalf("expected append to succeed, got: %v", err)
	}
}

func TestAppendConditionViolated(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
	if err := store.Append(ctx, []eventstore.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	condition := &eventstore.AppendCondition{
		Query: eventstore.Query{{
			Types: []string{"UserSignedUp"},
			Tags:  map[string]string{"username": "alice"},
		}},
		After: 0,
	}

	e2, _ := eventstore.NewEvent("UserSignedUp", "alice2", map[string]string{"username": "alice"})
	err := store.Append(ctx, []eventstore.Event{e2}, condition)
	if err != eventstore.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated, got: %v", err)
	}
}

func TestAppendConditionEmptyQueryMatchesAll(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
	if err := store.Append(ctx, []eventstore.Event{e1}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	// A non-nil condition with an empty Query matches ALL events, so appending
	// with After: 0 is rejected because an event already exists after 0.
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{"username": "bob"})
	err := store.Append(ctx, []eventstore.Event{e2}, &eventstore.AppendCondition{Query: eventstore.Query{}, After: 0})
	if err != eventstore.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated for empty-query match-all, got: %v", err)
	}

	// The same empty-query condition passes once After is at the head (no events
	// exist after it).
	var head uint64
	for ev, err := range store.Read(ctx, nil, 0) {
		if err != nil {
			t.Fatalf("read failed: %v", err)
		}
		head = ev.SequenceNumber
	}
	if err := store.Append(ctx, []eventstore.Event{e2}, &eventstore.AppendCondition{Query: eventstore.Query{}, After: head}); err != nil {
		t.Fatalf("expected append to succeed when After is at head, got: %v", err)
	}
}

func TestTagsFiltering(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice", "tenant": "t1"})
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{"username": "bob", "tenant": "t2"})
	e3, _ := eventstore.NewEvent("UserSignedUp", "charlie", map[string]string{"username": "charlie", "tenant": "t1"})
	if err := store.Append(ctx, []eventstore.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	query := eventstore.Query{{
		Types: []string{"UserSignedUp"},
		Tags:  map[string]string{"tenant": "t1"},
	}}

	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, query, 0) {
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

	e1, _ := eventstore.NewEvent("A", nil, nil)
	e2, _ := eventstore.NewEvent("B", nil, nil)
	e3, _ := eventstore.NewEvent("C", nil, nil)
	if err := store.Append(ctx, []eventstore.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	var all []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, nil, 0) {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		all = append(all, e)
	}

	var after []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, nil, all[1].SequenceNumber) {
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
		e, _ := eventstore.NewEvent("Event", fmt.Sprintf("data-%d", i), nil)
		if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
			t.Fatalf("append failed: %v", err)
		}
	}

	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, nil, 0) {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 5 {
		t.Fatalf("expected 5 events with batch size 2, got %d", len(events))
	}
}

func TestIndexedTags(t *testing.T) {
	store := setupStore(t, pgstore.WithIndexedTags("tenant_id"))
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"tenant_id": "t1", "username": "alice"})
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{"tenant_id": "t2", "username": "bob"})
	e3, _ := eventstore.NewEvent("UserSignedUp", "charlie", map[string]string{"tenant_id": "t1", "username": "charlie"})
	if err := store.Append(ctx, []eventstore.Event{e1, e2, e3}, nil); err != nil {
		t.Fatalf("append failed: %v", err)
	}

	query := eventstore.Query{{
		Tags: map[string]string{"tenant_id": "t1"},
	}}

	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, query, 0) {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events = append(events, e)
	}

	if len(events) != 2 {
		t.Fatalf("expected 2 events for tenant_id t1, got %d", len(events))
	}

	query2 := eventstore.Query{{
		Tags: map[string]string{"username": "bob"},
	}}

	var events2 []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, query2, 0) {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		events2 = append(events2, e)
	}

	if len(events2) != 1 {
		t.Fatalf("expected 1 event for username bob, got %d", len(events2))
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
			e, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
			condition := &eventstore.AppendCondition{
				Query: eventstore.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  map[string]string{"username": "alice"},
				}},
				After: 0,
			}
			errs <- store.Append(ctx, []eventstore.Event{e}, condition)
		}()
	}
	wg.Wait()
	close(errs)

	var succeeded int
	for err := range errs {
		if err == nil {
			succeeded++
		} else if err != eventstore.ErrAppendConditionViolated {
			t.Errorf("unexpected error: %v", err)
		}
	}

	if succeeded != 1 {
		t.Fatalf("expected exactly 1 successful append, got %d", succeeded)
	}

	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, nil, 0) {
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
		n, err := store.Flush(ctx, 10000)
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
	store := setupStore(t)
	ctx := context.Background()

	const n = 10
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			e, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{"username": "alice"})
			condition := &eventstore.AppendCondition{
				Query: eventstore.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  map[string]string{"username": "alice"},
				}},
				After: 0,
			}
			errs <- store.AppendStaging(ctx, []eventstore.Event{e}, condition)
		}()
	}
	wg.Wait()
	close(errs)

	var succeeded int
	for err := range errs {
		if err == nil {
			succeeded++
		} else if err != eventstore.ErrAppendConditionViolated {
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
	store := setupStore(t)
	ctx := context.Background()

	const n = 20
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			user := fmt.Sprintf("user-%d", i)
			e, _ := eventstore.NewEvent("UserSignedUp", user, map[string]string{"username": user})
			condition := &eventstore.AppendCondition{
				Query: eventstore.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  map[string]string{"username": user},
				}},
				After: 0,
			}
			errs <- store.AppendStaging(ctx, []eventstore.Event{e}, condition)
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
	for _, err := range store.Read(ctx, nil, 0) {
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
	store := setupStore(t)
	ctx := context.Background()

	const n = 64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			ent := fmt.Sprintf("e-%d", i)
			e, _ := eventstore.NewEvent("Thing", ent, map[string]string{"entity": ent})
			if err := store.AppendStaging(ctx, []eventstore.Event{e}, nil); err != nil {
				t.Errorf("append %s: %v", ent, err)
			}
		}(i)
	}
	wg.Wait()
	flushAll(t, store)

	var prev uint64
	var count int
	seen := make(map[string]bool)
	for e, err := range store.Read(ctx, nil, 0) {
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		if e.SequenceNumber <= prev {
			t.Fatalf("sequence numbers not strictly increasing: %d after %d", e.SequenceNumber, prev)
		}
		prev = e.SequenceNumber
		if ent, ok := e.Tags["entity"]; ok {
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
	store := setupStore(t)
	ctx := context.Background()

	// X lands in staging (course c2), unconditional — not yet flushed to events.
	x, _ := eventstore.NewEvent("Enrolled", nil, map[string]string{"course": "c2"})
	if err := store.AppendStaging(ctx, []eventstore.Event{x}, nil); err != nil {
		t.Fatalf("append X: %v", err)
	}

	// Y guarded by (Enrolled@course:c2 OR Enrolled@student:s1) from After=0. X
	// matches course:c2 and is pending in staging, so Y must conflict — even though
	// X is not yet in events.
	y, _ := eventstore.NewEvent("Enrolled", nil, map[string]string{"student": "s1"})
	cond := &eventstore.AppendCondition{
		Query: eventstore.Query{
			{Types: []string{"Enrolled"}, Tags: map[string]string{"course": "c2"}},
			{Types: []string{"Enrolled"}, Tags: map[string]string{"student": "s1"}},
		},
		After: 0,
	}
	if err := store.AppendStaging(ctx, []eventstore.Event{y}, cond); err != eventstore.ErrAppendConditionViolated {
		t.Fatalf("expected conflict from the pending staging event (multi-boundary), got: %v", err)
	}
}

// TestStagingNoDeadlock: overlapping multi-key appends acquire boundary keys in
// sorted order, so nothing deadlocks (40P01).
func TestStagingNoDeadlock(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()

	const n = 40
	errs := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func(i int) {
			defer wg.Done()
			tags := map[string]string{
				fmt.Sprintf("slot%d", i%4):     "x",
				fmt.Sprintf("slot%d", (i+1)%4): "x",
			}
			e, _ := eventstore.NewEvent("Slot", nil, tags)
			errs <- store.AppendStaging(ctx, []eventstore.Event{e}, nil)
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

// TestUnkeyableConditionRejected: AppendStaging rejects conditions that can't be
// keyed (match-all, type-less, tag-less) up front with ErrUnkeyableCondition,
// instead of silently falling back to the global lock (which doesn't compose with
// per-boundary locks). A keyable condition is still accepted.
func TestUnkeyableConditionRejected(t *testing.T) {
	store := setupStore(t)
	ctx := context.Background()
	e, _ := eventstore.NewEvent("Thing", nil, map[string]string{"k": "v"})

	unkeyable := []*eventstore.AppendCondition{
		{Query: nil, After: 0}, // match-all (empty query)
		{Query: eventstore.Query{{Tags: map[string]string{"email": "x"}}}, After: 0}, // type-less
		{Query: eventstore.Query{{Types: []string{"Thing"}}}, After: 0},              // tag-less
	}
	for _, c := range unkeyable {
		if err := store.AppendStaging(ctx, []eventstore.Event{e}, c); !errors.Is(err, pgstore.ErrUnkeyableCondition) {
			t.Errorf("AppendStaging with unkeyable condition %+v: want ErrUnkeyableCondition, got %v", c.Query, err)
		}
	}

	// A keyable condition is still accepted.
	keyable := &eventstore.AppendCondition{
		Query: eventstore.Query{{Types: []string{"Thing"}, Tags: map[string]string{"k": "v"}}},
		After: 0,
	}
	if err := store.AppendStaging(ctx, []eventstore.Event{e}, keyable); err != nil {
		t.Errorf("keyable condition should be accepted, got %v", err)
	}
}

// --- Encryption integration tests ---

func collectPGEvents(t *testing.T, store eventstore.Store, query eventstore.Query) []eventstore.SequencedEvent {
	t.Helper()
	ctx := context.Background()
	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, query, 0) {
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

	e, err := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := eventstore.EventData[user](events[0])
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
	raw := pgstore.New(db)
	if err := raw.Migrate(); err != nil {
		t.Fatalf("could not migrate: %v", err)
	}
	store := eventstore.NewMiddlewareStore(raw,
		eventstore.WithAppendMiddleware(enc.OnAppend),
		eventstore.WithReadMiddleware(enc.OnRead),
	)

	ctx := context.Background()
	e, _ := eventstore.NewEvent("UserSignedUp", "secret-data", map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
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

	e, _ := eventstore.NewEvent("UserSignedUp", "alice-data", map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-123"}); err != nil {
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
	if events[0].Tags["userId"] != "user-123" {
		t.Fatalf("expected tags preserved, got %v", events[0].Tags)
	}
}

func TestPGForgetDoesNotAffectOtherSubjects(t *testing.T) {
	store, enc := setupEncryptedStore(t)
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{
		"userId": "user-2",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e1, e2}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected user-1 to be redacted")
	}
	data, err := eventstore.EventData[string](events[1])
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

	e, _ := eventstore.NewEvent("FriendRequest", "request-data", map[string]string{
		"senderId":   "user-1",
		"receiverId": "user-2",
	}, encryption.WithEncryptionKeys("senderId", "receiverId"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"senderId": "user-1"}); err != nil {
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

	plain, _ := eventstore.NewEvent("SystemBooted", "v1.0", nil)
	encrypted, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{plain, encrypted}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events := collectPGEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}
	data, err := eventstore.EventData[string](events[0])
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

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e1}, nil); err != nil {
		t.Fatal(err)
	}

	condition := &eventstore.AppendCondition{
		Query: eventstore.Query{
			eventstore.QueryItem{Types: []string{"UserSignedUp"}, Tags: map[string]string{"userId": "user-1"}},
		},
		After: 0,
	}

	e2, _ := eventstore.NewEvent("UserSignedUp", "alice-again", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	err := store.Append(ctx, []eventstore.Event{e2}, condition)
	if err != eventstore.ErrAppendConditionViolated {
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
			e, _ := eventstore.NewEvent("UserSignedUp", username, map[string]string{"username": username})
			condition := &eventstore.AppendCondition{
				Query: eventstore.Query{{
					Types: []string{"UserSignedUp"},
					Tags:  map[string]string{"username": username},
				}},
				After: 0,
			}
			if err := store.Append(ctx, []eventstore.Event{e}, condition); err != nil {
				b.Errorf("append failed: %v", err)
			}
		}
	})
}
