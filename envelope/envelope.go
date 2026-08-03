// Package envelope stamps every appended event with who caused it and what it
// followed from, read from the context just before the event is persisted.
//
// An event's envelope is three typed fields and one free map. Actor and Cause
// sit on the write-side [eventstore.Event] and are filled in here. RecordedAt
// sits on [eventstore.SequencedEvent] and is assigned by the store at
// acceptance, so it is a fact about the log rather than a claim by the writer.
// Metadata stays what it always was: the escape hatch, and the channel other
// middleware coordinate through. Nothing more is typed, on one razor — record
// only what cannot be derived later. A log is immutable, so every field on it
// is a field forever.
//
// Application and domain code never set Actor or Cause. Event constructors
// leave them zero and this middleware fills them, which is why a command cannot
// forget to.
//
// Both are opaque strings in a "kind:id" grammar the application owns:
// "student:s1", "request:8f3a", "job:nightly-billing". The framework reserves
// exactly two prefixes, the two it produces itself — "reactor:<registered-name>"
// is the actor of a command a reactor dispatched, and "event:<sequence>" is the
// cause it hands that command.
//
// Strict by default: an append whose context carries no actor fails with
// [ErrNoActor], because a forgotten actor must fail the first request in
// development rather than flow silently into an immutable log. [AllowAnonymous]
// is the way out — one greppable line admitting the choice. Cause is always
// optional; plenty of appends follow from nothing.
//
// Register this middleware BEFORE encryption, so events are stamped and then
// encrypted:
//
//	stamp := envelope.New().OnAppend
//	store := eventstore.NewMiddlewareStore(inner,
//	    eventstore.WithAppendMiddleware(stamp, encryptor.OnAppend),
//	    eventstore.WithReadMiddleware(encryptor.OnRead),
//	)
package envelope

import (
	"context"
	"errors"

	"github.com/DenBeke/collie"
)

// ErrNoActor is returned by [Stamper.OnAppend] when the context carries no
// actor and the stamper is strict, which it is unless [AllowAnonymous] says
// otherwise.
var ErrNoActor = errors.New("append has no actor: set one with eventstore.ContextWithActor, or allow anonymous appends explicitly with envelope.AllowAnonymous()")

// Stamper writes the envelope onto events on their way to the store. Register
// [Stamper.OnAppend] with an [eventstore.MiddlewareStore].
type Stamper struct {
	allowAnonymous bool
}

// Option configures a Stamper.
type Option func(*Stamper)

// AllowAnonymous lets appends through with no actor in the context, leaving
// Actor empty. It is deliberately explicit and easy to grep for: it marks every
// place that has decided to write anonymous history.
func AllowAnonymous() Option {
	return func(s *Stamper) { s.allowAnonymous = true }
}

// New creates a Stamper. It is strict about the actor unless [AllowAnonymous]
// is passed.
func New(options ...Option) *Stamper {
	s := &Stamper{}
	for _, option := range options {
		option(s)
	}
	return s
}

// OnAppend is an [eventstore.AppendMiddleware] that copies the actor and cause
// from ctx onto the event, overwriting whatever was there: the envelope is what
// the context says, never what the caller wrote.
func (s *Stamper) OnAppend(ctx context.Context, e *eventstore.Event) error {
	actor := eventstore.ActorFromContext(ctx)
	if actor == "" && !s.allowAnonymous {
		return ErrNoActor
	}
	e.Actor = actor
	e.Cause = eventstore.CauseFromContext(ctx)
	return nil
}
