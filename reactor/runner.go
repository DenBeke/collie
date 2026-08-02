package reactor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
)

// everyEvent is the match-all query: one item with no types and no tags
// constrains nothing, so the whole log is delivered and each reactor filters for
// itself. The runner cannot know which types a handler cares about, and a query
// derived from a guess would silently skip the event that mattered.
var everyEvent = eventstore.Query{eventstore.QueryItem{}}

// readBatchSize bounds how many events one read pulls into memory before any of
// them is handled. A pass keeps reading batches until the log runs out, so this
// bounds the memory held, not how far a reactor catches up in one tick.
const readBatchSize = 1000

// CommandExecutor runs the commands a reactor returns. *[command.Registry]
// satisfies it, so a reaction passes the same dependency injection, validation
// and retry policy as a write arriving over HTTP, and classifies errors alike.
type CommandExecutor interface {
	Execute(ctx context.Context, cmd command.Command) error
}

var _ CommandExecutor = (*command.Registry)(nil)

// Runner hands each registered reactor the events after its checkpoint, one at a
// time, executes what comes back, and saves the checkpoint after each. One
// checkpoint per reactor, so a stuck reactor holds up only itself.
type Runner struct {
	store       eventstore.Store
	checkpoints CheckpointStore
	commands    CommandExecutor
	interval    time.Duration
	logger      *slog.Logger

	// mu guards the registration table; a pass copies it and works outside the
	// lock, so one reactor's work never blocks another goroutine.
	mu            sync.Mutex
	registrations []*registration
	started       bool

	// passes serializes ticks, so a manual Tick during Run cannot deliver the
	// same event twice or interleave two writers on one checkpoint.
	passes sync.Mutex
}

// registration is one reactor plus the checkpoint that belongs to its name.
type registration struct {
	name          string
	reactor       Reactor
	fromBeginning bool

	// position is the last sequence number fully handled, cached after the first
	// load. The store row stays the durable truth; this copy spares a read.
	position uint64
	loaded   bool
}

// Option configures a Runner.
type Option func(*Runner)

// NewRunner reads from store, keeps positions in checkpoints, and executes
// reactions through commands. It ticks once a second unless [WithInterval] says
// otherwise, and does nothing until [Runner.Tick] or [Runner.Run] is called.
func NewRunner(store eventstore.Store, checkpoints CheckpointStore, commands CommandExecutor, options ...Option) *Runner {
	runner := &Runner{
		store:       store,
		checkpoints: checkpoints,
		commands:    commands,
		interval:    1 * time.Second,
		logger:      slog.Default(),
	}
	for _, option := range options {
		option(runner)
	}
	return runner
}

// WithInterval sets how long [Runner.Run] waits between passes: the latency of
// every reaction, traded against how often an idle log is polled. Zero or less
// is ignored, so a caller can pass an unset config field and get the default.
func WithInterval(d time.Duration) Option {
	return func(runner *Runner) {
		if d > 0 {
			runner.interval = d
		}
	}
}

// WithLogger sets the logger, defaulting to slog.Default(). A reaction has no
// caller waiting on it, so a refused command or a failing handler is only ever
// visible in the log.
func WithLogger(logger *slog.Logger) Option {
	return func(runner *Runner) {
		if logger != nil {
			runner.logger = logger
		}
	}
}

// RegisterOption configures a single registration.
type RegisterOption func(*registration)

// FromBeginning starts a reactor at the head of history rather than the head of
// the log, replaying every event the store holds and re-firing every effect. Use
// it only where that is safe wholesale: the default exists to stop a new handler
// from mailing ten thousand people about things that happened last year.
func FromBeginning() RegisterOption {
	return func(reg *registration) {
		reg.fromBeginning = true
	}
}

// Register adds a reactor under name; reactors tick in registration order.
//
// The name keys the checkpoint, so renaming one abandons its progress and
// starts fresh at the head of the log. Convention: kebab-case with a version
// suffix, "welcome-mailer/v1". An empty name, a duplicate, or registering
// after Run has started is an error — wiring mistakes belong at boot.
func (r *Runner) Register(name string, reactor Reactor, options ...RegisterOption) error {
	if name == "" {
		return errors.New("a reactor needs a name: the name is the identity of its checkpoint")
	}
	if reactor == nil {
		return fmt.Errorf("reactor %q: no reactor given", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.started {
		return fmt.Errorf("reactor %q: cannot register after the runner has started", name)
	}
	for _, existing := range r.registrations {
		if existing.name == name {
			return fmt.Errorf("reactor %q: already registered — two reactors sharing a name would share a checkpoint", name)
		}
	}

	reg := &registration{name: name, reactor: reactor}
	for _, option := range options {
		option(reg)
	}
	r.registrations = append(r.registrations, reg)
	return nil
}

// Run ticks immediately, then once per interval until ctx is cancelled, and
// returns nil when it is: cancellation is how this stops, not a failure. It
// blocks, and anything a pass goes wrong at is logged and retried on the next.
func (r *Runner) Run(ctx context.Context) error {
	r.mu.Lock()
	r.started = true
	r.mu.Unlock()

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		err := r.Tick(ctx)
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("could not run reactors: %w", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Tick makes one pass over every reactor in registration order, exported so a
// test can drive the reacting edge without a clock. A failing reactor ends its
// own pass and nothing else; the only error returned is a cancelled context.
func (r *Runner) Tick(ctx context.Context) error {
	r.passes.Lock()
	defer r.passes.Unlock()

	for _, reg := range r.snapshot() {
		err := r.tick(ctx, reg)
		if err != nil {
			return err
		}
	}
	return nil
}

// snapshot copies the registration table so a pass runs without holding the lock
// across every handler and every command it triggers.
func (r *Runner) snapshot() []*registration {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*registration(nil), r.registrations...)
}

// tick runs one pass for one reactor: every event after its checkpoint in order,
// each fully handled before the checkpoint moves past it. Whatever goes wrong
// leaves the checkpoint where it is; that is what makes delivery at least once.
func (r *Runner) tick(ctx context.Context, reg *registration) error {
	loadErr := r.load(ctx, reg)
	if loadErr != nil {
		r.logger.ErrorContext(ctx, "could not load the reactor checkpoint", "reactor", reg.name, "error", loadErr)
		return nil
	}

	for {
		events, err := r.read(ctx, reg.position)
		if err != nil {
			// Nothing was handled, so the same events arrive again next tick.
			r.logger.ErrorContext(ctx, "could not read events for reactor", "reactor", reg.name, "error", err)
			return nil
		}

		for _, event := range events {
			cancelled := ctx.Err()
			if cancelled != nil {
				return cancelled
			}
			if !r.deliver(ctx, reg, event) {
				return nil
			}
			err = r.save(ctx, reg, event.SequenceNumber)
			if err != nil {
				// The effect happened and the checkpoint did not move, so this
				// event is delivered again — the case handlers must tolerate.
				r.logger.ErrorContext(ctx, "could not save the reactor checkpoint", "reactor", reg.name,
					"sequence", event.SequenceNumber, "error", err)
				return nil
			}
		}

		// A short batch means the log is caught up. Anything appended after this
		// point, including by the commands just executed, belongs to the next
		// tick, so a reactor cannot spin here forever.
		if len(events) < readBatchSize {
			return nil
		}
	}
}

// read drains up to readBatchSize events after position into memory and — the
// point of it — finishes the read before anything is handled. A reactor's
// command appends to the store being read, and a store that holds a read lock
// across iteration, as the in-memory one does, would deadlock against it.
func (r *Runner) read(ctx context.Context, after uint64) ([]eventstore.SequencedEvent, error) {
	events := make([]eventstore.SequencedEvent, 0, 64)
	for event, err := range r.store.Read(ctx, everyEvent, after) {
		if err != nil {
			return nil, err
		}
		events = append(events, event)
		if len(events) == readBatchSize {
			break
		}
	}
	return events, nil
}

// deliver runs one event through one reactor and reports whether the checkpoint
// may advance past it: only when React succeeded and every command reached a
// verdict, appended or refused for a reason repeating cannot change.
func (r *Runner) deliver(ctx context.Context, reg *registration, event eventstore.SequencedEvent) bool {
	commands, err := r.react(ctx, reg, event)
	if err != nil {
		r.logger.ErrorContext(ctx, "reactor failed", "reactor", reg.name, "event", event.Type,
			"sequence", event.SequenceNumber, "error", err)
		return false
	}

	for _, cmd := range commands {
		err := r.commands.Execute(ctx, cmd)
		switch {
		case err == nil:
		case errors.Is(err, command.ErrCommandRejected):
			// The rules doing their job, a redelivery included: absorb it.
			r.logger.InfoContext(ctx, "reactor command refused", "reactor", reg.name,
				"command", fmt.Sprintf("%T", cmd), "sequence", event.SequenceNumber, "reason", err)
		case errors.Is(err, command.ErrValidationFailed):
			// A bug in the reactor, deterministic like a refusal: absorb it too.
			r.logger.ErrorContext(ctx, "reactor built an invalid command", "reactor", reg.name,
				"command", fmt.Sprintf("%T", cmd), "sequence", event.SequenceNumber, "error", err)
		default:
			// The answer may differ next time, so hold the checkpoint.
			r.logger.ErrorContext(ctx, "could not execute a reactor command", "reactor", reg.name,
				"command", fmt.Sprintf("%T", cmd), "sequence", event.SequenceNumber, "error", err)
			return false
		}
	}
	return true
}

// react calls the handler and turns a panic into an error, so one reactor cannot
// take down the runner and every other reactor with it. A panic is at least as
// suspect as a returned error: do not advance.
func (r *Runner) react(ctx context.Context, reg *registration, event eventstore.SequencedEvent) (commands []command.Command, err error) {
	defer func() {
		recovered := recover()
		if recovered != nil {
			commands = nil
			err = fmt.Errorf("reactor panicked: %v", recovered)
		}
	}()
	return reg.reactor.React(ctx, event)
}

// load resolves a reactor's starting position once per process: the persisted
// checkpoint, else the head of the log — or 0 for a deliberate replay. The
// chosen position is written straight back, so the head scan is paid once per
// name ever and a restart in between resumes rather than skips.
func (r *Runner) load(ctx context.Context, reg *registration) error {
	if reg.loaded {
		return nil
	}

	position, found, err := r.checkpoints.Load(ctx, reg.name)
	if err != nil {
		return fmt.Errorf("could not load the checkpoint for reactor %q: %w", reg.name, err)
	}
	if !found {
		position = 0
		if !reg.fromBeginning {
			position, err = r.head(ctx)
			if err != nil {
				return err
			}
		}
		err = r.checkpoints.Save(ctx, reg.name, position)
		if err != nil {
			return fmt.Errorf("could not save the checkpoint for reactor %q: %w", reg.name, err)
		}
	}

	reg.position = position
	reg.loaded = true
	return nil
}

// head returns the sequence number of the last event by scanning the log once.
// The Store interface exposes no "latest": this is the price of starting from
// now, and [Runner.load] pays it a single time per reactor name.
func (r *Runner) head(ctx context.Context) (uint64, error) {
	var head uint64
	for event, err := range r.store.Read(ctx, everyEvent, 0) {
		if err != nil {
			return 0, fmt.Errorf("could not read the event log: %w", err)
		}
		head = event.SequenceNumber
	}
	return head, nil
}

// save records that everything up to and including position has been handled. A
// position is a reactor's entire memory — anything else it would want to
// remember is a fact, and facts belong in events. The in-memory copy moves only
// after the durable one.
func (r *Runner) save(ctx context.Context, reg *registration, position uint64) error {
	err := r.checkpoints.Save(ctx, reg.name, position)
	if err != nil {
		return err
	}
	reg.position = position
	return nil
}
