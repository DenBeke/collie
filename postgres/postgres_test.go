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
