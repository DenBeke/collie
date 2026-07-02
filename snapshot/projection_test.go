package snapshot_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/memory"
	"eventstore/snapshot"
)

// seed appends n events of the given type and returns the store.
func seed(t *testing.T, n int, eventType string, tags []eventstore.Tag) *memory.Store {
	t.Helper()
	store := memory.New()
	ctx := context.Background()
	for range n {
		event, err := eventstore.NewEvent(eventType, nil, tags)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, []eventstore.Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}
	return store
}

// countDef counts events of the given type; partition tags scope which ones.
// Note there is no Query: the event types to read derive from the handler keys.
func countDef(eventType string) eventstore.DefaultProjection[int] {
	return eventstore.DefaultProjection[int]{
		Handlers: map[string]eventstore.HandleFunc[int]{
			eventType: func(count int, _ eventstore.SequencedEvent) (int, error) { return count + 1, nil },
		},
	}
}

// TestProjection_ResumesFromSnapshot proves the persistence seam: a Projection
// saves its snapshot through a Store as it checkpoints, and a fresh instance
// over the same store resumes from that snapshot without replaying — then
// applies only the delta on the next decision.
func TestProjection_ResumesFromSnapshot(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 3, "UserSignedUp", nil)

	// First instance catches up to head and persists its snapshot.
	projection, err := snapshot.NewProjection(ctx, snapshots, "users", countDef("UserSignedUp"))
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
	resumed, err := snapshot.NewProjection(ctx, snapshots, "users", countDef("UserSignedUp"))
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
	afterReset, err := snapshot.NewProjection(ctx, snapshots, "users", countDef("UserSignedUp"))
	if err != nil {
		t.Fatal(err)
	}
	if afterReset.State != 0 || afterReset.After() != 0 {
		t.Fatalf("after reset state=%d after=%d, want 0/0", afterReset.State, afterReset.After())
	}
}

// TestNewProjection_ScopesQueryToPartition proves the partition tags are AND-ed
// into the query: only events carrying them are applied.
func TestNewProjection_ScopesQueryToPartition(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	for _, room := range []string{"r1", "r1", "r2", "r2", "r2"} {
		event, err := eventstore.NewEvent("SeatReserved", nil, eventstore.NewTags("room", room))
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, []eventstore.Event{event}, nil); err != nil {
			t.Fatal(err)
		}
	}

	projection, err := snapshot.NewProjection(ctx, memory.NewSnapshotStore(),
		"seats", countDef("SeatReserved"), eventstore.NewTag("room", "r1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if projection.State != 2 {
		t.Fatalf("expected 2 events for room r1, got %d", projection.State)
	}
}

// TestNewProjection_TagOrderInsensitive proves that the same partition written
// with tags in a different order resolves to the same snapshot.
func TestNewProjection_TagOrderInsensitive(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 3, "StudentSubscribed", eventstore.NewTags("student", "s1", "course", "c1"))

	first, err := snapshot.NewProjection(ctx, snapshots, "subscription", countDef("StudentSubscribed"),
		eventstore.NewTag("student", "s1"), eventstore.NewTag("course", "c1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, first); err != nil {
		t.Fatal(err)
	}
	if first.State != 3 || first.After() != 3 {
		t.Fatalf("first: state=%d after=%d, want 3/3", first.State, first.After())
	}

	// Same partition, reversed tag order: must resume from the same snapshot.
	resumed, err := snapshot.NewProjection(ctx, snapshots, "subscription", countDef("StudentSubscribed"),
		eventstore.NewTag("course", "c1"), eventstore.NewTag("student", "s1"))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != 3 || resumed.After() != 3 {
		t.Fatalf("resumed: state=%d after=%d, want 3/3 (same snapshot)", resumed.State, resumed.After())
	}
}

// TestNewProjection_NoCollisionOnSeparatorValues proves a tag value containing
// the name separators cannot masquerade as a different tag set, and a raw name
// cannot masquerade as a partition.
func TestNewProjection_NoCollisionOnSeparatorValues(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 2, "Ev", eventstore.NewTags("a", "1|b=2"))

	tricky, err := snapshot.NewProjection(ctx, snapshots, "p", countDef("Ev"),
		eventstore.NewTag("a", "1|b=2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, tricky); err != nil {
		t.Fatal(err)
	}
	if tricky.After() != 2 {
		t.Fatalf("tricky: after=%d, want 2", tricky.After())
	}

	// The naive encoding "p|a=1|b=2" would collide; both must be fresh.
	other, err := snapshot.NewProjection(ctx, snapshots, "p", countDef("Ev"),
		eventstore.NewTag("a", "1"), eventstore.NewTag("b", "2"))
	if err != nil {
		t.Fatal(err)
	}
	if other.State != 0 || other.After() != 0 {
		t.Fatalf("other: state=%d after=%d, want 0/0 (distinct snapshot)", other.State, other.After())
	}
	rawName, err := snapshot.NewProjection(ctx, snapshots, "p|a=1|b=2", countDef("Ev"))
	if err != nil {
		t.Fatal(err)
	}
	if rawName.State != 0 || rawName.After() != 0 {
		t.Fatalf("rawName: state=%d after=%d, want 0/0 (distinct snapshot)", rawName.State, rawName.After())
	}
}

// countingSnapshotStore counts Save calls to observe checkpoint persistence.
type countingSnapshotStore struct {
	snapshot.Store
	saves int
}

func (s *countingSnapshotStore) Save(ctx context.Context, name string, state []byte, position uint64) error {
	s.saves++
	return s.Store.Save(ctx, name, state, position)
}

// TestProjection_SavesOncePerCatchUp proves a catch-up over N events writes one
// snapshot (in Checkpoint), not N — and none when there is nothing new.
func TestProjection_SavesOncePerCatchUp(t *testing.T) {
	ctx := context.Background()
	snapshots := &countingSnapshotStore{Store: memory.NewSnapshotStore()}
	store := seed(t, 5, "UserSignedUp", nil)

	projection, err := snapshot.NewProjection(ctx, snapshots, "users", countDef("UserSignedUp"))
	if err != nil {
		t.Fatal(err)
	}

	if err := eventstore.UpdateProjection(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if projection.State != 5 {
		t.Fatalf("state=%d, want 5", projection.State)
	}
	if snapshots.saves != 1 {
		t.Fatalf("expected 1 save for 5 applied events, got %d", snapshots.saves)
	}

	// Nothing new: no snapshot write.
	if err := eventstore.UpdateProjection(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if snapshots.saves != 1 {
		t.Fatalf("expected no additional save on empty catch-up, got %d", snapshots.saves)
	}

	// BuildDecisionModel also checkpoints exactly once.
	if _, err := eventstore.BuildDecisionModel(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if snapshots.saves != 1 {
		t.Fatalf("expected no save when decision model saw no new events, got %d", snapshots.saves)
	}

	event, err := eventstore.NewEvent("UserSignedUp", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{event}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := eventstore.BuildDecisionModel(ctx, store, projection); err != nil {
		t.Fatal(err)
	}
	if projection.State != 6 {
		t.Fatalf("state=%d, want 6", projection.State)
	}
	if snapshots.saves != 2 {
		t.Fatalf("expected 1 additional save after new event, got %d total", snapshots.saves)
	}
}
