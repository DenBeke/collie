package eventstore_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/memory"
)

// countDef counts events of the given type; the partition tags scope which ones.
func countDef(eventType string) eventstore.DefaultProjection[int] {
	return eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{Types: []string{eventType}},
		Handlers: map[string]eventstore.HandleFunc[int]{
			eventType: func(count int, _ eventstore.SequencedEvent) (int, error) { return count + 1, nil },
		},
	}
}

func TestPartitionedProjection_RequiresPartition(t *testing.T) {
	_, err := eventstore.NewPartitionedProjection(context.Background(), memory.NewSnapshotStore(),
		"seats", nil, countDef("SeatReserved"))
	if err == nil {
		t.Fatal("expected error for empty partition")
	}
}

// TestPartitionedProjection_ScopesQueryToPartition proves the partition tags
// are AND-ed into the query: only events carrying them are applied.
func TestPartitionedProjection_ScopesQueryToPartition(t *testing.T) {
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

	projection, err := eventstore.NewPartitionedProjection(ctx, memory.NewSnapshotStore(),
		"seats", eventstore.NewTags("room", "r1"), countDef("SeatReserved"))
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

// TestPartitionedProjection_TagOrderInsensitive proves that the same partition
// written with tags in a different order resolves to the same snapshot.
func TestPartitionedProjection_TagOrderInsensitive(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 3, "StudentSubscribed", eventstore.NewTags("student", "s1", "course", "c1"))

	first, err := eventstore.NewPartitionedProjection(ctx, snapshots, "subscription",
		eventstore.NewTags("student", "s1", "course", "c1"), countDef("StudentSubscribed"))
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
	resumed, err := eventstore.NewPartitionedProjection(ctx, snapshots, "subscription",
		eventstore.NewTags("course", "c1", "student", "s1"), countDef("StudentSubscribed"))
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != 3 || resumed.After() != 3 {
		t.Fatalf("resumed: state=%d after=%d, want 3/3 (same snapshot)", resumed.State, resumed.After())
	}
}

// TestPartitionedProjection_NoCollisionOnSeparatorValues proves a tag value
// containing the name separators cannot masquerade as a different tag set.
func TestPartitionedProjection_NoCollisionOnSeparatorValues(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 2, "Ev", eventstore.NewTags("a", "1|b=2"))

	tricky, err := eventstore.NewPartitionedProjection(ctx, snapshots, "p",
		eventstore.NewTags("a", "1|b=2"), countDef("Ev"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, tricky); err != nil {
		t.Fatal(err)
	}
	if tricky.After() != 2 {
		t.Fatalf("tricky: after=%d, want 2", tricky.After())
	}

	// The naive encoding "p|a=1|b=2" would collide; this partition must be fresh.
	other, err := eventstore.NewPartitionedProjection(ctx, snapshots, "p",
		eventstore.NewTags("a", "1", "b", "2"), countDef("Ev"))
	if err != nil {
		t.Fatal(err)
	}
	if other.State != 0 || other.After() != 0 {
		t.Fatalf("other: state=%d after=%d, want 0/0 (distinct snapshot)", other.State, other.After())
	}
}

// TestPartitionedProjection_VersionSeparatesSnapshots proves WithVersion
// abandons snapshots built by older handler logic.
func TestPartitionedProjection_VersionSeparatesSnapshots(t *testing.T) {
	ctx := context.Background()
	snapshots := memory.NewSnapshotStore()
	store := seed(t, 3, "SeatReserved", eventstore.NewTags("room", "r1"))

	v1, err := eventstore.NewPartitionedProjection(ctx, snapshots, "seats",
		eventstore.NewTags("room", "r1"), countDef("SeatReserved"))
	if err != nil {
		t.Fatal(err)
	}
	if err := eventstore.UpdateProjection(ctx, store, v1); err != nil {
		t.Fatal(err)
	}
	if v1.After() != 3 {
		t.Fatalf("v1: after=%d, want 3", v1.After())
	}

	v2, err := eventstore.NewPartitionedProjection(ctx, snapshots, "seats",
		eventstore.NewTags("room", "r1"), countDef("SeatReserved"), eventstore.WithVersion(2))
	if err != nil {
		t.Fatal(err)
	}
	if v2.State != 0 || v2.After() != 0 {
		t.Fatalf("v2: state=%d after=%d, want 0/0 (fresh under new version)", v2.State, v2.After())
	}
}

// countingSnapshotStore counts Save calls to observe end-of-batch persistence.
type countingSnapshotStore struct {
	eventstore.SnapshotStore
	saves int
}

func (s *countingSnapshotStore) Save(ctx context.Context, name string, state []byte, position uint64) error {
	s.saves++
	return s.SnapshotStore.Save(ctx, name, state, position)
}

// TestStoredProjection_SavesOncePerCatchUp proves a catch-up over N events
// writes one snapshot (in Checkpoint), not N — and none when there is nothing new.
func TestStoredProjection_SavesOncePerCatchUp(t *testing.T) {
	ctx := context.Background()
	snapshots := &countingSnapshotStore{SnapshotStore: memory.NewSnapshotStore()}
	store := seed(t, 5, "UserSignedUp", nil)

	projection, err := eventstore.NewStoredProjection(ctx, snapshots, "users", countDef("UserSignedUp"))
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

	// BuildDecisionModel also flushes exactly once.
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
