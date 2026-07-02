package eventstore_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/memory"
)

// The library's own stored read-model projection must satisfy
// CheckpointProjection so BuildDecisionModel recognizes it as a decision model
// and skips already-processed events. Regression guard for the projector-type
// consolidation: *eventstore.StoredProjection must implement it.
var _ eventstore.CheckpointProjection = (*eventstore.StoredProjection[int])(nil)

// storedProjection wraps a DefaultProjection with a checkpoint, implementing
// CheckpointProjection so BuildDecisionModel skips already-processed events.
type storedProjection[S any] struct {
	eventstore.DefaultProjection[S]
	after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }

// Checkpoint is a no-op: this test double keeps state in memory only.
func (s *storedProjection[S]) Checkpoint(context.Context) error { return nil }

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

func TestBuildDecisionModel_BackwardCompatible(t *testing.T) {
	store := seed(t, 3, "UserSignedUp", eventstore.NewTags("username", "alice"))
	ctx := context.Background()

	counter := eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
		State: 0,
		Handlers: map[string]eventstore.HandleFunc[int]{
			"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
				return count + 1, nil
			},
		},
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, &counter)
	if err != nil {
		t.Fatal(err)
	}

	if counter.State != 3 {
		t.Fatalf("expected state 3, got %d", counter.State)
	}
	if condition.After != 3 {
		t.Fatalf("expected After=3, got %d", condition.After)
	}
}

func TestBuildDecisionModel_StoredProjectionSkipsOldEvents(t *testing.T) {
	store := seed(t, 5, "UserSignedUp", nil)
	ctx := context.Background()

	// Pre-loaded state: already counted 3 events (at checkpoint 3).
	projection := &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
			State: 3,
			Handlers: map[string]eventstore.HandleFunc[int]{
				"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		},
		after: 3,
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, projection)
	if err != nil {
		t.Fatal(err)
	}

	// Should have applied only events 4 and 5.
	if projection.State != 5 {
		t.Fatalf("expected state 5, got %d", projection.State)
	}
	if condition.After != 5 {
		t.Fatalf("expected After=5, got %d", condition.After)
	}
}

func TestBuildDecisionModel_NoNewEventsAfterCheckpoint(t *testing.T) {
	store := seed(t, 3, "UserSignedUp", nil)
	ctx := context.Background()

	// Stored projection already at the end of the stream.
	projection := &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
			State: 3,
			Handlers: map[string]eventstore.HandleFunc[int]{
				"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		},
		after: 3,
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, projection)
	if err != nil {
		t.Fatal(err)
	}

	// State unchanged — no new events.
	if projection.State != 3 {
		t.Fatalf("expected state 3, got %d", projection.State)
	}
	// After must be 3 (from maxAfter init), not 0.
	if condition.After != 3 {
		t.Fatalf("expected After=3, got %d", condition.After)
	}
}

func TestBuildDecisionModel_MixedStoredAndFresh(t *testing.T) {
	store := seed(t, 5, "UserSignedUp", nil)
	ctx := context.Background()

	// Stored projection at checkpoint 3.
	stored := &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
			State: 3,
			Handlers: map[string]eventstore.HandleFunc[int]{
				"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		},
		after: 3,
	}

	// Fresh projection — no checkpoint.
	fresh := &eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
		State: 0,
		Handlers: map[string]eventstore.HandleFunc[int]{
			"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
				return count + 1, nil
			},
		},
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, stored, fresh)
	if err != nil {
		t.Fatal(err)
	}

	// Fresh forces read from 0, so stored sees all 5 but skips 1-3.
	if stored.State != 5 {
		t.Fatalf("stored: expected state 5, got %d", stored.State)
	}
	// Fresh sees all 5.
	if fresh.State != 5 {
		t.Fatalf("fresh: expected state 5, got %d", fresh.State)
	}
	if condition.After != 5 {
		t.Fatalf("expected After=5, got %d", condition.After)
	}
}

func TestBuildDecisionModel_MultipleStoredAtDifferentCheckpoints(t *testing.T) {
	store := seed(t, 10, "UserSignedUp", nil)
	ctx := context.Background()

	// p1 at checkpoint 3, p2 at checkpoint 7.
	p1 := &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
			State: 3,
			Handlers: map[string]eventstore.HandleFunc[int]{
				"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		},
		after: 3,
	}
	p2 := &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
			State: 7,
			Handlers: map[string]eventstore.HandleFunc[int]{
				"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		},
		after: 7,
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, p1, p2)
	if err != nil {
		t.Fatal(err)
	}

	// p1 gets events 4-10 (7 new), p2 gets events 8-10 (3 new).
	if p1.State != 10 {
		t.Fatalf("p1: expected state 10, got %d", p1.State)
	}
	if p2.State != 10 {
		t.Fatalf("p2: expected state 10, got %d", p2.State)
	}
	if condition.After != 10 {
		t.Fatalf("expected After=10, got %d", condition.After)
	}
}

// TestBuildDecisionModel_RealStoredProjectionIsCheckpointed proves the checkpoint
// path using the library's own *eventstore.StoredProjection (not the test-only
// storedProjection): a caught-up read model reused as a decision model must apply
// only the events appended since its checkpoint, not replay from 0 and double-count.
func TestBuildDecisionModel_RealStoredProjectionIsCheckpointed(t *testing.T) {
	store := seed(t, 3, "UserSignedUp", nil)
	ctx := context.Background()

	proj, err := eventstore.NewStoredProjection(ctx, memory.NewSnapshotStore(), "total-users", eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
		State: 0,
		Handlers: map[string]eventstore.HandleFunc[int]{
			"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
				return count + 1, nil
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Catch the read model up to the current head (checkpoint advances to 3).
	if err := eventstore.UpdateProjection(ctx, store, proj); err != nil {
		t.Fatal(err)
	}
	if proj.State != 3 || proj.After() != 3 {
		t.Fatalf("after catch-up: state=%d after=%d, want 3/3", proj.State, proj.After())
	}

	// Two more events arrive after the checkpoint.
	for range 2 {
		e, err := eventstore.NewEvent("UserSignedUp", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
			t.Fatal(err)
		}
	}

	condition, err := eventstore.BuildDecisionModel(ctx, store, proj)
	if err != nil {
		t.Fatal(err)
	}
	if proj.State != 5 {
		t.Fatalf("expected state 5 (checkpoint honored, only events 4-5 applied), got %d", proj.State)
	}
	if condition.After != 5 {
		t.Fatalf("expected After=5, got %d", condition.After)
	}
}
