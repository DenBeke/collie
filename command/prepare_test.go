package command_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
)

// prepareState is shared across attempts through a pointer field — the
// documented way to carry bookkeeping over Execute's per-attempt copies.
type prepareState struct {
	prepared int      // how many times Prepare ran
	decided  []string // the prepared token as each attempt's Decide saw it
	injected bool
}

// preparingCommand does its work in Prepare, stashes the result in a field, and
// forces exactly one append-condition conflict in Decide so the command runs
// twice. The token embeds the Prepare count, so a second Prepare would be
// visible as a changed token rather than only as a changed counter.
type preparingCommand struct {
	store      eventstore.Store
	state      *prepareState
	prepareErr error

	token string // filled by Prepare, read by Decide
}

func newPreparingCommand(store eventstore.Store) *preparingCommand {
	return &preparingCommand{store: store, state: &prepareState{}}
}

func (cmd *preparingCommand) Prepare(context.Context) error {
	cmd.state.prepared++
	if cmd.prepareErr != nil {
		return cmd.prepareErr
	}
	cmd.token = fmt.Sprintf("token-%d", cmd.state.prepared)
	return nil
}

func (cmd *preparingCommand) Projections(context.Context) ([]eventstore.Projection, error) {
	return []eventstore.Projection{
		&eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Tags: touched},
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Touched": func(seen int, _ eventstore.SequencedEvent) (int, error) { return seen + 1, nil },
			},
		},
	}, nil
}

func (cmd *preparingCommand) Decide(ctx context.Context) ([]eventstore.Event, error) {
	cmd.state.decided = append(cmd.state.decided, cmd.token)
	if !cmd.state.injected {
		cmd.state.injected = true
		competing, err := eventstore.NewEvent("Touched", nil, touched)
		if err != nil {
			return nil, fmt.Errorf("could not create competing event: %w", err)
		}
		if err := cmd.store.Append(ctx, []eventstore.Event{competing}, nil); err != nil {
			return nil, fmt.Errorf("could not append competing event: %w", err)
		}
	}
	event, err := eventstore.NewEvent("Touched", nil, touched)
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// TestExecute_PrepareRunsOnceAcrossRetries is the whole point of the phase: the
// command runs twice (one forced conflict, one success) but the I/O in Prepare
// happens once, and both attempts decide from the identical prepared value.
func TestExecute_PrepareRunsOnceAcrossRetries(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newPreparingCommand(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute with retry: %v", err)
	}

	if cmd.state.prepared != 1 {
		t.Fatalf("expected Prepare to run once, got %d", cmd.state.prepared)
	}
	if len(cmd.state.decided) != 2 {
		t.Fatalf("expected 2 attempts, got %d", len(cmd.state.decided))
	}
	if cmd.state.decided[0] != "token-1" || cmd.state.decided[1] != "token-1" {
		t.Fatalf("expected every attempt to decide from the same prepared token, got %v", cmd.state.decided)
	}
}

// TestExecute_PrepareLeavesCallerCommandPristine holds Execute's invariant across
// the new phase: Prepare writes to a copy, so the token never lands on the
// caller's command.
func TestExecute_PrepareLeavesCallerCommandPristine(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newPreparingCommand(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.token != "" {
		t.Fatalf("expected the caller's command untouched, got token %q", cmd.token)
	}
}

// TestExecute_PrepareErrorAbortsBeforeAnything proves a failed preparation is
// classifiable and costs nothing: no projections built, no decision, no append.
func TestExecute_PrepareErrorAbortsBeforeAnything(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	boom := errors.New("llm unavailable")
	cmd := newPreparingCommand(store)
	cmd.prepareErr = boom

	err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3))
	if !errors.Is(err, command.ErrPrepareFailed) {
		t.Fatalf("expected ErrPrepareFailed, got %v", err)
	}
	if !errors.Is(err, boom) {
		t.Fatalf("expected the underlying cause to stay reachable, got %v", err)
	}
	if errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("an infrastructure failure must not classify as a rejection: %v", err)
	}
	if len(cmd.state.decided) != 0 {
		t.Fatalf("expected Decide never to run, got %d attempts", len(cmd.state.decided))
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
	if cmd.state.prepared != 1 {
		t.Fatalf("expected no retry on a prepare failure, got %d preparations", cmd.state.prepared)
	}
}

// TestExecute_PrepareCanRejectOutright proves a Prepare that means "no" rather
// than "broken" reaches the same 409-shaped classification as Decide, without
// any new mechanism: DecisionError claims ErrCommandRejected through the wrap.
func TestExecute_PrepareCanRejectOutright(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newPreparingCommand(store)
	cmd.prepareErr = &command.DecisionError{Err: fmt.Errorf("%w: %q", errUsernameTaken, "alice")}

	err := command.Execute(ctx, store, cmd)
	if !errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("expected ErrCommandRejected, got %v", err)
	}
	if !errors.Is(err, errUsernameTaken) {
		t.Fatalf("expected the domain sentinel to stay reachable, got %v", err)
	}
	if !errors.Is(err, command.ErrPrepareFailed) {
		t.Fatalf("expected the phase to stay identifiable, got %v", err)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatal("expected no events appended on a prepare rejection")
	}
}

// TestExecute_WithoutPrepareIsUnchanged guards the additive promise: a command
// that does not implement Preparer takes exactly the path it always did.
func TestExecute_WithoutPrepareIsUnchanged(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newConflictOnce(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.state.attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", cmd.state.attempts)
	}
	if count := countAll(t, store); count != 2 {
		t.Fatalf("expected 2 events, got %d", count)
	}
}

// --- dispatch ordering and injection ---

// dispatchRecorder is the shared sink the registry hands to each freshly built
// command through WithInjector — the registry builds commands by reflection, so
// a pointer field is nil unless something injects it.
type dispatchRecorder struct {
	preparedFor []string
}

// signUpPreparing reads its own decoded payload in Prepare, which is exactly
// what a constructor-injected factory cannot do.
type signUpPreparing struct {
	Username string `json:"username"`

	recorder *dispatchRecorder
}

func (cmd *signUpPreparing) useRecorder(r *dispatchRecorder) { cmd.recorder = r }

func (cmd *signUpPreparing) Validate() error {
	if cmd.Username == "" {
		return errors.New("username is required")
	}
	return nil
}

func (cmd *signUpPreparing) Prepare(context.Context) error {
	cmd.recorder.preparedFor = append(cmd.recorder.preparedFor, cmd.Username)
	return nil
}

func (cmd *signUpPreparing) Projections(context.Context) ([]eventstore.Projection, error) {
	return nil, nil
}

func (cmd *signUpPreparing) Decide(context.Context) ([]eventstore.Event, error) {
	event, err := eventstore.NewEvent("UserSignedUp", cmd.Username,
		eventstore.NewTags("username", cmd.Username))
	if err != nil {
		return nil, fmt.Errorf("could not create UserSignedUp event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

func newDispatchRegistry(store eventstore.Store, recorder *dispatchRecorder) *command.Registry {
	return command.NewRegistry(store,
		command.WithCommands(&signUpPreparing{}),
		command.WithInjector(func(cmd any) error {
			if user, ok := cmd.(interface{ useRecorder(*dispatchRecorder) }); ok {
				user.useRecorder(recorder)
			}
			return nil
		}),
	)
}

// TestDispatch_PrepareSeesDecodedPayload is the reason Prepare is a command
// phase rather than a constructor hook: WithCommandFunc runs before the payload
// exists, so per-request work could not depend on the request.
func TestDispatch_PrepareSeesDecodedPayload(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	recorder := &dispatchRecorder{}

	err := newDispatchRegistry(store, recorder).Dispatch(ctx, "signUpPreparing", []byte(`{"username":"alice"}`))
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if len(recorder.preparedFor) != 1 || recorder.preparedFor[0] != "alice" {
		t.Fatalf("expected Prepare to see the decoded payload, got %v", recorder.preparedFor)
	}
}

// TestDispatch_PrepareRunsAfterValidation proves the ordering is worth something:
// a payload that was going to be rejected never reaches the expensive phase.
func TestDispatch_PrepareRunsAfterValidation(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	recorder := &dispatchRecorder{}

	err := newDispatchRegistry(store, recorder).Dispatch(ctx, "signUpPreparing", []byte(`{}`))
	if !errors.Is(err, command.ErrValidationFailed) {
		t.Fatalf("expected ErrValidationFailed, got %v", err)
	}
	if len(recorder.preparedFor) != 0 {
		t.Fatalf("expected Prepare not to run for an invalid payload, got %v", recorder.preparedFor)
	}
}

// TestDispatch_InjectorErrorAborts proves a wiring failure stops the dispatch
// before the payload is even decoded.
func TestDispatch_InjectorErrorAborts(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	boom := errors.New("no deps configured")

	registry := command.NewRegistry(store,
		command.WithCommands(&signUpPreparing{}),
		command.WithInjector(func(any) error { return boom }),
	)

	err := registry.Dispatch(ctx, "signUpPreparing", []byte(`{"username":"alice"}`))
	if !errors.Is(err, boom) {
		t.Fatalf("expected the injector error, got %v", err)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
}
