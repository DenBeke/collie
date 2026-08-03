package envelope_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/envelope"
	"github.com/DenBeke/collie/memory"
)

// newStore wires the stamper the way an application does: one append middleware
// in front of a store, so the tests assert on what was persisted.
func newStore(options ...envelope.Option) *eventstore.MiddlewareStore {
	return eventstore.NewMiddlewareStore(memory.New(),
		eventstore.WithAppendMiddleware(envelope.New(options...).OnAppend),
	)
}

func appendOne(ctx context.Context, store eventstore.Store, e eventstore.Event) error {
	return store.Append(ctx, []eventstore.Event{e}, nil)
}

// only reads the single event a test appended.
func only(t *testing.T, store eventstore.Store) eventstore.SequencedEvent {
	t.Helper()
	var events []eventstore.SequencedEvent
	for e, err := range store.Read(context.Background(), nil, 0) {
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		events = append(events, e)
	}
	if len(events) != 1 {
		t.Fatalf("read %d events, want 1", len(events))
	}
	return events[0]
}

func TestStampsActorAndCauseFromContext(t *testing.T) {
	store := newStore()
	ctx := eventstore.ContextWithCause(eventstore.ContextWithActor(context.Background(), "student:s1"), "request:8f3a")

	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}
	err = appendOne(ctx, store, e)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	stamped := only(t, store)
	if stamped.Actor != "student:s1" {
		t.Errorf("actor = %q, want %q", stamped.Actor, "student:s1")
	}
	if stamped.Cause != "request:8f3a" {
		t.Errorf("cause = %q, want %q", stamped.Cause, "request:8f3a")
	}
	// The store assigns the read-side half of the envelope.
	if stamped.RecordedAt.IsZero() {
		t.Error("recorded at the zero time: the store did not stamp acceptance")
	}
}

// TestStampOverwritesWhatTheCallerWrote: the envelope is what the context said.
// A command that sets Actor itself is not trusted, it is corrected.
func TestStampOverwritesWhatTheCallerWrote(t *testing.T) {
	store := newStore()
	ctx := eventstore.ContextWithCause(eventstore.ContextWithActor(context.Background(), "student:s1"), "request:8f3a")

	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}
	e.Actor = "student:impostor"
	e.Cause = "request:made-up"

	err = appendOne(ctx, store, e)
	if err != nil {
		t.Fatalf("append: %v", err)
	}

	stamped := only(t, store)
	if stamped.Actor != "student:s1" {
		t.Errorf("actor = %q, want the one from the context", stamped.Actor)
	}
	if stamped.Cause != "request:8f3a" {
		t.Errorf("cause = %q, want the one from the context", stamped.Cause)
	}
}

// TestStrictRefusesAnAnonymousAppend pins the message as well as the failure:
// it is the whole teaching value of refusing.
func TestStrictRefusesAnAnonymousAppend(t *testing.T) {
	stamper := envelope.New()
	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}

	err = stamper.OnAppend(context.Background(), &e)
	if !errors.Is(err, envelope.ErrNoActor) {
		t.Fatalf("append without an actor: %v, want ErrNoActor", err)
	}
	const want = "append has no actor: set one with eventstore.ContextWithActor, " +
		"or allow anonymous appends explicitly with envelope.AllowAnonymous()"
	if err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
	}
}

// TestStrictRefusalReachesTheCaller: the refusal is not swallowed by the
// middleware chain, and nothing is persisted.
func TestStrictRefusalReachesTheCaller(t *testing.T) {
	store := newStore()
	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}

	err = appendOne(context.Background(), store, e)
	if !errors.Is(err, envelope.ErrNoActor) {
		t.Fatalf("append without an actor: %v, want ErrNoActor", err)
	}
	for range store.Read(context.Background(), nil, 0) {
		t.Fatal("a refused append persisted an event")
	}
}

// TestStrictRefusesACauseWithoutAnActor: strictness is about the actor only, so
// a cause on its own does not buy an append its way in.
func TestStrictRefusesACauseWithoutAnActor(t *testing.T) {
	store := newStore()
	ctx := eventstore.ContextWithCause(context.Background(), "event:7")

	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}
	err = appendOne(ctx, store, e)
	if !errors.Is(err, envelope.ErrNoActor) {
		t.Fatalf("append with only a cause: %v, want ErrNoActor", err)
	}
}

func TestAllowAnonymousPermitsAnEmptyActor(t *testing.T) {
	store := newStore(envelope.AllowAnonymous())
	ctx := eventstore.ContextWithCause(context.Background(), "request:8f3a")

	e, err := eventstore.NewEvent("StudentRegistered", nil, nil)
	if err != nil {
		t.Fatalf("build the event: %v", err)
	}
	err = appendOne(ctx, store, e)
	if err != nil {
		t.Fatalf("anonymous append: %v", err)
	}

	stamped := only(t, store)
	if stamped.Actor != "" {
		t.Errorf("actor = %q, want empty", stamped.Actor)
	}
	// An anonymous append still gets its causation.
	if stamped.Cause != "request:8f3a" {
		t.Errorf("cause = %q, want %q", stamped.Cause, "request:8f3a")
	}
}
