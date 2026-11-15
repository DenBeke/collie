package eventstore_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/DenBeke/collie"
)

// wanted is a stored projection over a type the seeded log never contains.
// tested is the boundary appendOne puts every event on, and wanted decides on:
// a condition needs a tag to be keyable.
var tested = eventstore.NewTags("tested", "all")

func wanted() *storedProjection[int] {
	return &storedProjection[int]{
		DefaultProjection: eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Tags: tested},
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Wanted": func(n int, _ eventstore.SequencedEvent) (int, error) { return n + 1, nil },
			},
		},
	}
}

func appendOne(t *testing.T, store eventstore.Store, eventType string, condition *eventstore.AppendCondition) error {
	t.Helper()
	event, err := eventstore.NewEvent(eventType, nil, tested)
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
