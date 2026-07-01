package eventstore_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/memory"
)

// TestStoredProjection_ResumesFromSnapshot proves the persistence seam: a
// StoredProjection saves its snapshot through a SnapshotStore as it applies
// events, and a fresh instance over the same store resumes from that snapshot
// without replaying — then applies only the delta on the next decision.
func TestStoredProjection_ResumesFromSnapshot(t *testing.T) {
	ctx := context.Background()
	snapshotStore := memory.NewSnapshotStore()
	store := seed(t, 3, "UserSignedUp", nil)

	projectionDef := eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
		Handlers: map[string]eventstore.HandleFunc[int]{
			"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) { return count + 1, nil },
		},
	}

	// First instance catches up to head and persists its snapshot.
	projection, err := eventstore.NewStoredProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if projection.State != 3 || projection.After() != 3 {
		t.Fatalf("first instance state=%d after=%d, want 3/3", projection.State, projection.After())
	}

	// A fresh instance over the same snapshot store resumes WITHOUT replaying.
	resumed, err := eventstore.NewStoredProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != 3 || resumed.After() != 3 {
		t.Fatalf("resumed instance state=%d after=%d, want 3/3", resumed.State, resumed.After())
	}

	// Two more events: the resumed instance applies only the delta since its checkpoint.
	for range 2 {
		event, err := eventstore.NewEvent("UserSignedUp", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, []eventstore.Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}
	condition, err := eventstore.BuildDecisionModel(ctx, store, resumed)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != 5 {
		t.Fatalf("resumed after delta state=%d, want 5", resumed.State)
	}
	if condition.After != 5 {
		t.Fatalf("condition.After=%d, want 5", condition.After)
	}

	// Reset clears the persisted snapshot.
	if err := resumed.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	afterReset, err := eventstore.NewStoredProjection(ctx, snapshotStore, "users", projectionDef)
	if err != nil {
		t.Fatal(err)
	}
	if afterReset.State != 0 || afterReset.After() != 0 {
		t.Fatalf("after reset state=%d after=%d, want 0/0", afterReset.State, afterReset.After())
	}
}
