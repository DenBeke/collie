package eventstore_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
)

// wanted is a stored projection over a type the seeded log never contains.
func wanted() *storedProjection[int] {
	return &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Wanted": func(n int, _ eventstore.SequencedEvent) (int, error) { return n + 1, nil },
			},
		},
	}
}

func appendOne(t *testing.T, store eventstore.Store, eventType string, condition *eventstore.AppendCondition) error {
	t.Helper()
	event, err := eventstore.NewEvent(eventType, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store.Append(context.Background(), []eventstore.Event{event}, condition)
}

func TestUpdateProjection_AdvancesToHeadWithoutMatches(t *testing.T) {
	store := seed(t, 3, "Other", nil)
	p := wanted()

	if err := eventstore.UpdateProjection(context.Background(), store, p); err != nil {
		t.Fatal(err)
	}
	if p.After() != 3 {
		t.Fatalf("checkpoint=%d, want the head (3)", p.After())
	}
}

func TestUpdateProjection_KeepsCheckpointOnApplyError(t *testing.T) {
	store := seed(t, 3, "Other", nil)
	if err := appendOne(t, store, "Wanted", nil); err != nil {
		t.Fatal(err)
	}
	p := wanted()
	p.Handlers["Wanted"] = func(int, eventstore.SequencedEvent) (int, error) { return 0, errors.New("poison") }

	if err := eventstore.UpdateProjection(context.Background(), store, p); err == nil {
		t.Fatal("expected the apply error")
	}
	if p.After() != 0 {
		t.Fatalf("checkpoint=%d, want 0: a failed catch-up must not move it", p.After())
	}
}

// brokenStore reports a head, then fails during iteration.
type brokenStore struct{ eventstore.Store }

func (brokenStore) Read(context.Context, eventstore.Query, uint64) (iter.Seq2[eventstore.SequencedEvent, error], uint64, error) {
	return func(yield func(eventstore.SequencedEvent, error) bool) {
		yield(eventstore.SequencedEvent{}, errors.New("connection lost"))
	}, 10, nil
}

// headlessStore cannot read the head.
type headlessStore struct{ eventstore.Store }

func (headlessStore) Read(context.Context, eventstore.Query, uint64) (iter.Seq2[eventstore.SequencedEvent, error], uint64, error) {
	return nil, 0, errors.New("connection refused")
}

func TestUpdateProjection_KeepsCheckpointOnReadError(t *testing.T) {
	for name, store := range map[string]eventstore.Store{"head": headlessStore{}, "iteration": brokenStore{}} {
		t.Run(name, func(t *testing.T) {
			p := wanted()
			p.after = 2
			if err := eventstore.UpdateProjection(context.Background(), store, p); err == nil {
				t.Fatal("expected the read error")
			}
			if p.After() != 2 {
				t.Fatalf("checkpoint=%d, want 2", p.After())
			}
		})
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

func TestMemoryRead_HeadIsTakenAtCall(t *testing.T) {
	store := memory.New()
	for range 2 {
		if err := appendOne(t, store, "Other", nil); err != nil {
			t.Fatal(err)
		}
	}

	events, head, err := store.Read(context.Background(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("head=%d, want 2", head)
	}

	// Appended after Read, before iterating: yielded, and above the head.
	if err := appendOne(t, store, "Other", nil); err != nil {
		t.Fatal(err)
	}
	var last uint64
	for e, err := range events {
		if err != nil {
			t.Fatal(err)
		}
		last = e.SequenceNumber
	}
	if last != 3 {
		t.Fatalf("last=%d, want 3", last)
	}
}

func TestMiddlewareStore_PassesHeadAndError(t *testing.T) {
	store := seed(t, 4, "Other", nil)
	noop := func(context.Context, *eventstore.SequencedEvent) error { return nil }

	wrapped := eventstore.NewMiddlewareStore(store, eventstore.WithReadMiddleware(noop))
	if _, head, err := wrapped.Read(context.Background(), nil, 0); err != nil || head != 4 {
		t.Fatalf("head=%d err=%v, want 4 and nil", head, err)
	}

	broken := eventstore.NewMiddlewareStore(headlessStore{}, eventstore.WithReadMiddleware(noop))
	if _, _, err := broken.Read(context.Background(), nil, 0); err == nil {
		t.Fatal("expected the head error to pass through")
	}
}
