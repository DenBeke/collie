package reactor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/envelope"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/reactor"
)

// quiet keeps the expected failures in these tests out of the test output: the
// runner logs everything it absorbs, and half of these tests absorb something.
func quiet() reactor.Option {
	return reactor.WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// noop is a command the executor never actually runs — the fake executor below
// records it instead — so it only has to satisfy the interface.
type noop struct {
	Tag string
}

func (cmd *noop) Projections(context.Context) ([]eventstore.Projection, error) { return nil, nil }

func (cmd *noop) Decide(context.Context) ([]eventstore.Event, error) { return nil, nil }

// executor is a CommandExecutor that remembers what it was asked to run and
// answers with whatever the test needs it to.
type executor struct {
	mu       sync.Mutex
	executed []command.Command
	err      error
}

func (e *executor) Execute(_ context.Context, cmd command.Command) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.executed = append(e.executed, cmd)
	return e.err
}

func (e *executor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.executed)
}

func (e *executor) answer(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.err = err
}

// appendEvents writes one event per type, unconditionally.
func appendEvents(t *testing.T, store eventstore.Store, types ...string) {
	t.Helper()
	for _, eventType := range types {
		event, err := eventstore.NewEvent(eventType, nil, eventstore.NewTags("id", eventType))
		if err != nil {
			t.Fatalf("could not build a %s event: %v", eventType, err)
		}
		err = store.Append(context.Background(), []eventstore.Event{event}, nil)
		if err != nil {
			t.Fatalf("could not append a %s event: %v", eventType, err)
		}
	}
}

// collector records the types it was handed and returns the commands the test
// gave it. Every reactor in this file is a collector, so what a test asserts on
// is always "which events reached the handler, in what order".
type collector struct {
	seen     []string
	commands []command.Command
	fail     error
}

func (c *collector) React(_ context.Context, event eventstore.SequencedEvent) ([]command.Command, error) {
	if c.fail != nil {
		return nil, c.fail
	}
	c.seen = append(c.seen, event.Type)
	return c.commands, nil
}

func (c *collector) assertSeen(t *testing.T, want ...string) {
	t.Helper()
	if strings.Join(c.seen, ",") != strings.Join(want, ",") {
		t.Fatalf("reactor saw %v, want %v", c.seen, want)
	}
}

// checkpoint reads a reactor's persisted position, which is the only durable
// trace a pass leaves.
func checkpoint(t *testing.T, checkpoints *memory.CheckpointStore, name string) (uint64, bool) {
	t.Helper()
	position, found, err := checkpoints.Load(context.Background(), name)
	if err != nil {
		t.Fatalf("could not load the checkpoint: %v", err)
	}
	return position, found
}

func tick(t *testing.T, runner *reactor.Runner) {
	t.Helper()
	err := runner.Tick(context.Background())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
}

func register(t *testing.T, runner *reactor.Runner, name string, r reactor.Reactor, options ...reactor.RegisterOption) {
	t.Helper()
	err := runner.Register(name, r, options...)
	if err != nil {
		t.Fatalf("register %q: %v", name, err)
	}
}

// TestRunnerStartsFromNow is the default that keeps a new reactor from mailing
// everyone about last year.
func TestRunnerStartsFromNow(t *testing.T) {
	store := memory.New()
	appendEvents(t, store, "Old", "Older")

	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), &executor{}, quiet())
	watcher := &collector{}
	register(t, runner, "watcher/v1", watcher)

	// The first pass anchors the checkpoint at the head, so history is skipped.
	tick(t, runner)
	watcher.assertSeen(t)

	appendEvents(t, store, "New")
	tick(t, runner)
	watcher.assertSeen(t, "New")
}

func TestRunnerFromBeginningReplays(t *testing.T) {
	store := memory.New()
	appendEvents(t, store, "Old", "Older")

	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), &executor{}, quiet())
	watcher := &collector{}
	register(t, runner, "watcher/v1", watcher, reactor.FromBeginning())

	tick(t, runner)
	watcher.assertSeen(t, "Old", "Older")

	// And it keeps going from there rather than replaying again.
	appendEvents(t, store, "New")
	tick(t, runner)
	watcher.assertSeen(t, "Old", "Older", "New")
}

// TestRunnerResumesFromThePersistedCheckpoint is the restart: a second runner
// over the same checkpoint store picks up where the first one stopped, because
// the stored row — not the in-memory copy — is the truth.
func TestRunnerResumesFromThePersistedCheckpoint(t *testing.T) {
	store := memory.New()
	checkpoints := memory.NewCheckpointStore()
	appendEvents(t, store, "First", "Second")

	before := reactor.NewRunner(store, checkpoints, &executor{}, quiet())
	firstWatcher := &collector{}
	register(t, before, "watcher/v1", firstWatcher, reactor.FromBeginning())
	tick(t, before)
	firstWatcher.assertSeen(t, "First", "Second")

	// The process dies here. Events keep arriving while nothing is running.
	appendEvents(t, store, "Third")

	after := reactor.NewRunner(store, checkpoints, &executor{}, quiet())
	secondWatcher := &collector{}
	// No FromBeginning this time: the stored checkpoint decides, not the option.
	register(t, after, "watcher/v1", secondWatcher)
	tick(t, after)
	secondWatcher.assertSeen(t, "Third")
}

// TestRunnerRedeliversAfterAHandlerError pins the at-least-once contract: a
// failing handler holds its checkpoint, sees the same event again, and only
// then moves on.
func TestRunnerRedeliversAfterAHandlerError(t *testing.T) {
	store := memory.New()
	checkpoints := memory.NewCheckpointStore()
	runner := reactor.NewRunner(store, checkpoints, &executor{}, quiet())

	watcher := &collector{fail: errors.New("the mail server is down")}
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting", "AlsoInteresting")
	tick(t, runner)
	watcher.assertSeen(t)
	if position, _ := checkpoint(t, checkpoints, "watcher/v1"); position != 0 {
		t.Fatalf("checkpoint advanced past a failed handler to %d", position)
	}

	watcher.fail = nil
	tick(t, runner)
	watcher.assertSeen(t, "Interesting", "AlsoInteresting")
	if position, _ := checkpoint(t, checkpoints, "watcher/v1"); position != 2 {
		t.Fatalf("checkpoint = %d, want 2", position)
	}

	// Nothing is delivered twice once the checkpoint is past it.
	tick(t, runner)
	watcher.assertSeen(t, "Interesting", "AlsoInteresting")
}

// TestRunnerAbsorbsRefusals: a refused command is a normal outcome, so the pass
// continues and the checkpoint moves.
func TestRunnerAbsorbsRefusals(t *testing.T) {
	store := memory.New()
	checkpoints := memory.NewCheckpointStore()
	commands := &executor{err: &command.DecisionError{Err: errors.New("already subscribed")}}
	runner := reactor.NewRunner(store, checkpoints, commands, quiet())

	watcher := &collector{commands: []command.Command{&noop{Tag: "follow-up"}}}
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting")
	tick(t, runner)

	if commands.count() != 1 {
		t.Fatalf("executed %d commands, want 1", commands.count())
	}
	if position, _ := checkpoint(t, checkpoints, "watcher/v1"); position != 1 {
		t.Fatalf("checkpoint = %d, want the refusal absorbed and the checkpoint at 1", position)
	}

	// Absorbed means done: the event is not tried again.
	tick(t, runner)
	if commands.count() != 1 {
		t.Fatalf("executed %d commands after a second tick, want 1", commands.count())
	}
}

// TestRunnerHoldsTheCheckpointOnTransientFailures is the other half of the
// classification: an infrastructure failure is not an answer, so the event comes
// back.
func TestRunnerHoldsTheCheckpointOnTransientFailures(t *testing.T) {
	store := memory.New()
	checkpoints := memory.NewCheckpointStore()
	commands := &executor{err: errors.New("the store is unreachable")}
	runner := reactor.NewRunner(store, checkpoints, commands, quiet())

	watcher := &collector{commands: []command.Command{&noop{Tag: "follow-up"}}}
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting")
	tick(t, runner)
	if position, _ := checkpoint(t, checkpoints, "watcher/v1"); position != 0 {
		t.Fatalf("checkpoint advanced past a transient failure to %d", position)
	}

	commands.answer(nil)
	tick(t, runner)
	if position, _ := checkpoint(t, checkpoints, "watcher/v1"); position != 1 {
		t.Fatalf("checkpoint = %d, want 1 once the command succeeded", position)
	}
	if commands.count() != 2 {
		t.Fatalf("executed %d commands, want the failed one retried once", commands.count())
	}
}

// TestRunnerExecutesEveryCommandFromOneEvent: one fact can imply several.
func TestRunnerExecutesEveryCommandFromOneEvent(t *testing.T) {
	store := memory.New()
	commands := &executor{}
	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), commands, quiet())

	watcher := &collector{commands: []command.Command{&noop{Tag: "a"}, &noop{Tag: "b"}, &noop{Tag: "c"}}}
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting")
	tick(t, runner)

	if commands.count() != 3 {
		t.Fatalf("executed %d commands, want 3", commands.count())
	}
}

// appender is a CommandExecutor that writes to the same store the runner reads,
// which is what a real one does — through a command — and what the runner must
// therefore survive: an append made from inside the delivery of an event.
type appender struct {
	store eventstore.Store
	calls int
}

func (a *appender) Execute(ctx context.Context, _ command.Command) error {
	a.calls++
	event, err := eventstore.NewEvent("Reacted", nil, eventstore.NewTags("id", "reacted"))
	if err != nil {
		return err
	}
	return a.store.Append(ctx, []eventstore.Event{event}, nil)
}

// TestRunnerAppendsWhileReading pins the shape of a pass: the events are read
// before any of them is handled, so a command that appends does not deadlock
// against the read that delivered the event it is reacting to.
func TestRunnerAppendsWhileReading(t *testing.T) {
	store := memory.New()
	commands := &appender{store: store}
	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), commands, quiet())

	// Reacts to Interesting only, so the event its own command appends does not
	// feed it again — a reactor that answers what it causes must converge.
	watcher := reactor.ReactorFunc(func(_ context.Context, event eventstore.SequencedEvent) ([]command.Command, error) {
		if event.Type != "Interesting" {
			return nil, nil
		}
		return []command.Command{&noop{Tag: event.Type}}, nil
	})
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting", "Interesting")
	tick(t, runner)

	if commands.calls != 2 {
		t.Fatalf("executed %d commands, want 2", commands.calls)
	}
	// The two appended events are delivered on the next pass and settle there.
	tick(t, runner)
	if commands.calls != 2 {
		t.Fatalf("executed %d commands after the reactions were delivered, want 2", commands.calls)
	}
}

// TestRunnerStampsTheEnvelopeOnItsCommands: what a reaction appends carries the
// reactor's name and the event it answered, and neither the reactor nor the
// command it returned mentions either.
func TestRunnerStampsTheEnvelopeOnItsCommands(t *testing.T) {
	// AllowAnonymous, because the event that triggers the reaction is seeded by
	// the test and has no actor of its own.
	store := eventstore.NewMiddlewareStore(memory.New(),
		eventstore.WithAppendMiddleware(envelope.New(envelope.AllowAnonymous()).OnAppend))
	appendEvents(t, store, "Interesting")

	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), &appender{store: store}, quiet())
	watcher := reactor.ReactorFunc(func(_ context.Context, event eventstore.SequencedEvent) ([]command.Command, error) {
		if event.Type != "Interesting" {
			return nil, nil
		}
		return []command.Command{&noop{Tag: event.Type}}, nil
	})
	register(t, runner, "watcher/v1", watcher, reactor.FromBeginning())
	tick(t, runner)

	var reacted []eventstore.SequencedEvent
	query := eventstore.Query{eventstore.QueryItem{Types: []string{"Reacted"}}}
	for event, err := range store.Read(context.Background(), query, 0) {
		if err != nil {
			t.Fatalf("read the log: %v", err)
		}
		reacted = append(reacted, event)
	}

	if len(reacted) != 1 {
		t.Fatalf("the reaction appended %d events, want 1", len(reacted))
	}
	if reacted[0].Actor != "reactor:watcher/v1" {
		t.Errorf("actor = %q, want %q", reacted[0].Actor, "reactor:watcher/v1")
	}
	if reacted[0].Cause != "event:1" {
		t.Errorf("cause = %q, want %q, the event it answered", reacted[0].Cause, "event:1")
	}
}

// TestRunnerIsolatesAPanickingReactor: a handler that panics fails like one that
// errors, and the reactors after it still run.
func TestRunnerIsolatesAPanickingReactor(t *testing.T) {
	store := memory.New()
	checkpoints := memory.NewCheckpointStore()
	runner := reactor.NewRunner(store, checkpoints, &executor{}, quiet())

	register(t, runner, "panicking/v1", reactor.ReactorFunc(
		func(context.Context, eventstore.SequencedEvent) ([]command.Command, error) {
			panic("nil map, probably")
		}))
	watcher := &collector{}
	register(t, runner, "watcher/v1", watcher)
	tick(t, runner)

	appendEvents(t, store, "Interesting")
	tick(t, runner)

	watcher.assertSeen(t, "Interesting")
	if position, _ := checkpoint(t, checkpoints, "panicking/v1"); position != 0 {
		t.Fatalf("the panicking reactor advanced its checkpoint to %d", position)
	}
}

func TestRunnerRejectsBadRegistrations(t *testing.T) {
	runner := reactor.NewRunner(memory.New(), memory.NewCheckpointStore(), &executor{}, quiet())

	err := runner.Register("", &collector{})
	if err == nil {
		t.Fatal("expected an empty name to be rejected")
	}

	register(t, runner, "watcher/v1", &collector{})
	err = runner.Register("watcher/v1", &collector{})
	if err == nil {
		t.Fatal("expected a duplicate name to be rejected")
	}
	if !strings.Contains(err.Error(), "watcher/v1") {
		t.Fatalf("expected the name in the message, got %v", err)
	}
}

// TestRunnerRejectsRegistrationAfterRun also covers Run's shape: it ticks
// immediately, keeps ticking, and returns nil when the context is cancelled.
func TestRunnerRejectsRegistrationAfterRun(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := memory.New()
	appendEvents(t, store, "Interesting")
	runner := reactor.NewRunner(store, memory.NewCheckpointStore(), &executor{},
		reactor.WithInterval(time.Millisecond), quiet())

	reacted := make(chan struct{}, 1)
	register(t, runner, "watcher/v1", reactor.ReactorFunc(
		func(context.Context, eventstore.SequencedEvent) ([]command.Command, error) {
			select {
			case reacted <- struct{}{}:
			default:
			}
			return nil, nil
		}), reactor.FromBeginning())

	stopped := make(chan error, 1)
	go func() {
		stopped <- runner.Run(ctx)
	}()

	// Waiting for the first delivery is waiting for Run to be under way, without
	// guessing at a sleep.
	<-reacted

	err := runner.Register("late/v1", &collector{})
	if err == nil {
		t.Fatal("expected a registration after Run to be rejected")
	}

	cancel()
	err = <-stopped
	if err != nil {
		t.Fatalf("Run returned %v, want nil on cancellation", err)
	}
}
