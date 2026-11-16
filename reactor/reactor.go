// Package reactor is the reacting edge of a DCB application: handlers that watch
// the event log and respond to what already happened, with a side effect, with
// the commands that should follow from the fact, or with both.
//
// A reactor never appends. It returns commands, and a command is the only writer
// in this framework, so a reaction still passes the same rules and is still
// guarded by an append condition covering exactly what it read.
//
// A refusal is a normal outcome. A reactor always decides from state the log has
// already moved past, so "the course is full" or "already subscribed" are
// answers, not failures. Only an infrastructure failure is worth retrying.
//
// Delivery is at least once: the effect and the checkpoint recording it cannot
// be atomic, so a crash between them replays the event and [Reactor.React] must
// tolerate seeing it twice. The command half is free — the rules refuse the
// duplicate — but a raw effect is the handler's to make idempotent.
//
// Effects live at the two ends of a decision: [command.Preparer] produces its
// inputs exactly once before, a Reactor responds to the facts it produced at
// least once after. Commands decide and append, they never "do": there must
// never be a command whose purpose is a side effect, fired once per optimistic
// retry and guarding nothing that matters.
//
// Loops are yours to avoid. A reactor reacting to a type it can itself cause
// must be convergent — each round strictly closer to returning nothing — or it
// has built a loop the runner will drive forever. Nothing here detects it.
package reactor

import (
	"context"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
)

// Reactor handles one event at a time, in log order, and says what should follow
// from it.
type Reactor interface {
	// React is called once per event, for every event in the log; filtering is
	// the reactor's own job.
	//
	// No commands and a nil error means "not interested" or "done", and the
	// runner advances. A non-nil error holds the checkpoint: the event is
	// redelivered next tick and the reactor sees nothing newer until it passes.
	// Order is a guarantee, so a poisoned event stalls this reactor loudly. React
	// runs synchronously in the runner's pass; honour the ctx it is handed.
	React(ctx context.Context, event collie.SequencedEvent) ([]command.Command, error)
}

// ReactorFunc adapts a plain function to [Reactor], as http.HandlerFunc does for
// http.Handler: a reactor with no dependencies should not need a struct.
//
//	runner.Register("orientation/v1", reactor.ReactorFunc(subscribeToOrientation))
type ReactorFunc func(ctx context.Context, event collie.SequencedEvent) ([]command.Command, error)

var _ Reactor = ReactorFunc(nil)

// React calls f.
func (f ReactorFunc) React(ctx context.Context, event collie.SequencedEvent) ([]command.Command, error) {
	return f(ctx, event)
}
