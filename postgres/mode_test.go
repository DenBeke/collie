package postgres_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"

	"github.com/DenBeke/collie"
	pgstore "github.com/DenBeke/collie/postgres"
)

func studentEvent(t *testing.T, id string) collie.Event {
	t.Helper()
	e, err := collie.NewEvent("StudentRegistered", nil, collie.NewTags("student", id))
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func staged(t *testing.T, db *gorm.DB) int64 {
	t.Helper()
	var n int64
	if err := db.Raw(`SELECT count(*) FROM staging`).Scan(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

func headOf(t *testing.T, store collie.Store) uint64 {
	t.Helper()
	_, head, err := store.Read(context.Background(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	return head
}

// eventually polls cond for up to two seconds.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatalf("not within 2s: %s", what)
}

// The default store stages its appends and flushes them itself; after Close
// nothing moves them any more.
func TestDefaultStoreFlushesItself(t *testing.T) {
	db := setupDB(t)
	store := pgstore.New(db)
	t.Cleanup(func() { _ = store.Close() })
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	cond := &collie.AppendCondition{Query: collie.Query{{Types: []string{"StudentRegistered"}, Tags: collie.NewTags("student", "s1")}}}
	if err := store.Append(ctx, []collie.Event{studentEvent(t, "s1")}, cond); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the append is readable", func() bool { return headOf(t, store) == 1 })

	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("a second Close: %v", err)
	}
	if err := store.Append(ctx, []collie.Event{studentEvent(t, "s2")}, nil); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if n := staged(t, db); n != 1 {
		t.Fatalf("staged=%d after Close, want 1: nothing flushes any more", n)
	}
}

// A leader that is stopped first moves what is still staged.
func TestStoppedFlusherDrains(t *testing.T) {
	db := setupDB(t)
	store := pgstore.New(db, pgstore.WithoutFlusher())
	if err := store.Migrate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error)
	// An hour between empty flushes: after its first look, only the drain moves anything.
	go func() { done <- store.RunFlusher(ctx, pgstore.FlushOptions{Interval: time.Hour}) }()
	time.Sleep(300 * time.Millisecond)

	if err := store.Append(context.Background(), []collie.Event{studentEvent(t, "s1")}, nil); err != nil {
		t.Fatal(err)
	}
	if n := staged(t, db); n != 1 {
		t.Fatalf("staged=%d before the stop, want 1", n)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunFlusher returned %v, want context.Canceled", err)
	}
	if n := staged(t, db); n != 0 {
		t.Fatalf("staged=%d after the stop, want 0", n)
	}
	if head := headOf(t, store); head != 1 {
		t.Fatalf("head=%d, want 1", head)
	}
}

// Migrate records the first mode and refuses the other one; the functions of
// the other mode refuse appends from a process that skipped Migrate.
func TestMigrateRecordsTheAppendMode(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	staging := pgstore.New(db, pgstore.WithoutFlusher())
	if err := staging.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := staging.Migrate(); err != nil {
		t.Fatalf("a second Migrate in the same mode: %v", err)
	}

	global := pgstore.New(db, pgstore.WithGlobalLock())
	if err := global.Migrate(); !errors.Is(err, pgstore.ErrAppendModeMismatch) {
		t.Fatalf("Migrate in the other mode: got %v, want ErrAppendModeMismatch", err)
	}
	if err := global.Append(ctx, []collie.Event{studentEvent(t, "s1")}, nil); !errors.Is(err, pgstore.ErrAppendModeMismatch) {
		t.Fatalf("a global-lock append on a staging database: got %v, want ErrAppendModeMismatch", err)
	}
	if err := global.RunFlusher(ctx, pgstore.FlushOptions{}); !errors.Is(err, pgstore.ErrAppendModeMismatch) {
		t.Fatalf("RunFlusher on a global-lock store: got %v, want ErrAppendModeMismatch", err)
	}
	if head := headOf(t, staging); head != 0 {
		t.Fatalf("head=%d, want 0: the refused append wrote nothing", head)
	}
}

// SwitchAppendMode changes the recorded mode on purpose: to the global lock
// only with nothing staged, and back.
func TestSwitchAppendMode(t *testing.T) {
	db := setupDB(t)
	ctx := context.Background()
	staging := pgstore.New(db, pgstore.WithoutFlusher())
	if err := staging.Migrate(); err != nil {
		t.Fatal(err)
	}
	if err := staging.Append(ctx, []collie.Event{studentEvent(t, "s1")}, nil); err != nil {
		t.Fatal(err)
	}

	global := pgstore.New(db, pgstore.WithGlobalLock())
	if err := global.SwitchAppendMode(ctx); !errors.Is(err, pgstore.ErrAppendModeMismatch) {
		t.Fatalf("switching with an append still staged: got %v, want ErrAppendModeMismatch", err)
	}
	flushAll(t, staging)
	if err := global.SwitchAppendMode(ctx); err != nil {
		t.Fatalf("switch to the global lock: %v", err)
	}
	if err := global.Migrate(); err != nil {
		t.Fatalf("Migrate after the switch: %v", err)
	}
	if err := global.Append(ctx, []collie.Event{studentEvent(t, "s2")}, nil); err != nil {
		t.Fatalf("global-lock append after the switch: %v", err)
	}
	if head := headOf(t, global); head != 2 {
		t.Fatalf("head=%d, want 2: readable at once", head)
	}
	if err := staging.Append(ctx, []collie.Event{studentEvent(t, "s3")}, nil); !errors.Is(err, pgstore.ErrAppendModeMismatch) {
		t.Fatalf("a staging append after the switch: got %v, want ErrAppendModeMismatch", err)
	}

	if err := staging.SwitchAppendMode(ctx); err != nil {
		t.Fatalf("switch back to staging: %v", err)
	}
	if err := staging.Append(ctx, []collie.Event{studentEvent(t, "s3")}, nil); err != nil {
		t.Fatalf("staging append after switching back: %v", err)
	}
	flushAll(t, staging)
	if head := headOf(t, staging); head != 3 {
		t.Fatalf("head=%d, want 3", head)
	}
}
