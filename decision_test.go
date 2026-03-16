package eventstore_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/memory"
)

// storedProjection wraps a Projection with a checkpoint, implementing
// CheckpointedProjector so BuildDecisionModel skips already-processed events.
type storedProjection[S any] struct {
	eventstore.Projection[S]
	after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }

// seed appends n events of the given type and returns the store.
func seed(t *testing.T, n int, eventType string, tags map[string]string) *memory.Store {
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
	store := seed(t, 3, "UserSignedUp", map[string]string{"username": "alice"})
	ctx := context.Background()

	counter := eventstore.Projection[int]{
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
		Projection: eventstore.Projection[int]{
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
		Projection: eventstore.Projection[int]{
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
		Projection: eventstore.Projection[int]{
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
	fresh := &eventstore.Projection[int]{
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
		Projection: eventstore.Projection[int]{
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
		Projection: eventstore.Projection[int]{
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
