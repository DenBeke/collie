package eventstore_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/snapshot"
)

// The library's own stored read-model projection must satisfy
// CheckpointProjection so BuildDecisionModel recognizes it as a decision model
// and skips already-processed events. Regression guard for the projector-type
// consolidation: *snapshot.Projection must implement it.
var _ eventstore.CheckpointProjection = (*snapshot.Projection[int])(nil)

// BuildDecisionModel takes plain Projections, so a test double that drifts from
// CheckpointProjection would silently become a fresh projection instead of
// failing to compile.
var _ eventstore.CheckpointProjection = (*storedProjection[int])(nil)

// storedProjection wraps a DefaultProjection with a checkpoint, implementing
// CheckpointProjection so BuildDecisionModel skips already-processed events.
type storedProjection[S any] struct {
	eventstore.DefaultProjection[S]
	after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }

// Checkpoint moves the in-memory checkpoint: this test double persists nothing.
func (s *storedProjection[S]) Checkpoint(_ context.Context, upTo uint64) error {
	s.after = max(s.after, upTo)
	return nil
}

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
	// After must be the head (3), not 0.
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
// path using the library's own *snapshot.Projection (not the test-only
// storedProjection): a caught-up read model reused as a decision model must apply
// only the events appended since its checkpoint, not replay from 0 and double-count.
func TestBuildDecisionModel_RealStoredProjectionIsCheckpointed(t *testing.T) {
	store := seed(t, 3, "UserSignedUp", nil)
	ctx := context.Background()

	proj, err := snapshot.NewProjection(ctx, memory.NewSnapshotStore(), "total-users", eventstore.DefaultProjection[int]{
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

func TestBuildDecisionModel_AfterIsHeadWithoutMatches(t *testing.T) {
	store := seed(t, 5, "Other", nil)

	condition, err := eventstore.BuildDecisionModel(context.Background(), store, wanted())
	if err != nil {
		t.Fatal(err)
	}
	if condition.After != 5 {
		t.Fatalf("After=%d, want the head (5)", condition.After)
	}
}

func TestBuildDecisionModel_HeadConditionStillDetectsConflicts(t *testing.T) {
	store := seed(t, 5, "Other", nil)
	ctx := context.Background()

	// Unrelated writes after the read do not conflict.
	condition, err := eventstore.BuildDecisionModel(ctx, store, wanted())
	if err != nil {
		t.Fatal(err)
	}
	if err := appendOne(t, store, "Other", nil); err != nil {
		t.Fatal(err)
	}
	if err := appendOne(t, store, "Wanted", condition); err != nil {
		t.Fatalf("unrelated write must not conflict: %v", err)
	}

	// A matching write after the read does.
	condition, err = eventstore.BuildDecisionModel(ctx, store, wanted())
	if err != nil {
		t.Fatal(err)
	}
	if err := appendOne(t, store, "Wanted", nil); err != nil {
		t.Fatal(err)
	}
	if err := appendOne(t, store, "Wanted", condition); !errors.Is(err, eventstore.ErrAppendConditionViolated) {
		t.Fatalf("expected a conflict, got %v", err)
	}
}

// counting is a stored projection counting events of one type from a checkpoint.
func counting(eventType string, after uint64) *storedProjection[int] {
	return &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Handlers: map[string]eventstore.HandleFunc[int]{
				eventType: func(n int, _ eventstore.SequencedEvent) (int, error) { return n + 1, nil },
			},
		},
		after: after,
	}
}

// One read from the lowest checkpoint serves both projections: each applies
// only what lies past its own checkpoint, and both end at the head, past a tail
// neither of them matches.
func TestBuildDecisionModel_StoredProjectionsAtDifferentCheckpoints(t *testing.T) {
	store := memory.New()
	for _, eventType := range []string{"A", "B", "A", "B", "A", "B", "A", "B", "Other", "Other", "Other"} {
		if err := appendOne(t, store, eventType, nil); err != nil {
			t.Fatal(err)
		}
	}
	a, b := counting("A", 2), counting("B", 6)

	condition, err := eventstore.BuildDecisionModel(context.Background(), store, a, b)
	if err != nil {
		t.Fatal(err)
	}
	if a.State != 3 || b.State != 1 {
		t.Fatalf("states a=%d b=%d, want 3 (A at 3, 5, 7) and 1 (B at 8)", a.State, b.State)
	}
	if a.After() != 11 || b.After() != 11 || condition.After != 11 {
		t.Fatalf("a=%d b=%d After=%d, want all at the head (11)", a.After(), b.After(), condition.After)
	}
}

func TestBuildDecisionModel_KeepsCheckpointsOnError(t *testing.T) {
	t.Run("apply", func(t *testing.T) {
		store := memory.New()
		for _, eventType := range []string{"A", "B", "Other"} {
			if err := appendOne(t, store, eventType, nil); err != nil {
				t.Fatal(err)
			}
		}
		a, b := counting("A", 0), counting("B", 0)
		b.Handlers["B"] = func(int, eventstore.SequencedEvent) (int, error) { return 0, errors.New("poison") }

		if _, err := eventstore.BuildDecisionModel(context.Background(), store, a, b); err == nil {
			t.Fatal("expected the apply error")
		}
		if a.After() != 0 || b.After() != 0 {
			t.Fatalf("a=%d b=%d, want both still at 0", a.After(), b.After())
		}
	})

	for name, store := range map[string]eventstore.Store{"head": headlessStore{}, "iteration": brokenStore{}} {
		t.Run(name, func(t *testing.T) {
			a, b := counting("A", 2), counting("B", 4)
			if _, err := eventstore.BuildDecisionModel(context.Background(), store, a, b); err == nil {
				t.Fatal("expected the read error")
			}
			if a.After() != 2 || b.After() != 4 {
				t.Fatalf("a=%d b=%d, want 2 and 4", a.After(), b.After())
			}
		})
	}
}
