package postgres_test

import (
	"context"
	"testing"

	"github.com/DenBeke/collie"
	pgstore "github.com/DenBeke/collie/postgres"
	"github.com/DenBeke/collie/snapshot"
)

// TestSnapshotStore_ResumesFromSnapshot proves the postgres SnapshotStore
// persists and restores a StoredProjection's state+position: a fresh instance
// loads the snapshot written by an earlier one, no replay.
func TestSnapshotStore_ResumesFromSnapshot(t *testing.T) {
	db := setupDB(t)
	store := pgstore.New(db, pgstore.WithGlobalLock())
	if err := store.Migrate(); err != nil {
		t.Fatalf("migrate store: %v", err)
	}
	if err := pgstore.MigrateProjections(db); err != nil {
		t.Fatalf("migrate projections: %v", err)
	}
	ctx := context.Background()
	snapshotStore := pgstore.NewSnapshotStore(db)

	for range 3 {
		event, err := collie.NewEvent("UserSignedUp", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, []collie.Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}

	projectionDef := collie.DefaultProjection[int]{
		Query: collie.QueryItem{Types: []string{"UserSignedUp"}},
		Handlers: map[string]collie.HandleFunc[int]{
			"UserSignedUp": func(count int, _ collie.SequencedEvent) (int, error) { return count + 1, nil },
		},
	}

	// First instance catches up to head and persists its snapshot to postgres.
	projection, err := snapshot.NewProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if err := collie.UpdateProjection(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if projection.State != 3 || projection.After() != 3 {
		t.Fatalf("first instance state=%d after=%d, want 3/3", projection.State, projection.After())
	}

	// A fresh instance loads the persisted snapshot — no replay.
	resumed, err := snapshot.NewProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != 3 || resumed.After() != 3 {
		t.Fatalf("resumed instance state=%d after=%d, want 3/3", resumed.State, resumed.After())
	}

	// Reset clears the persisted snapshot.
	if err := resumed.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	afterReset, err := snapshot.NewProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if afterReset.State != 0 || afterReset.After() != 0 {
		t.Fatalf("after reset state=%d after=%d, want 0/0", afterReset.State, afterReset.After())
	}
}
