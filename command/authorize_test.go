package command_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
)

// errNotYours is an application sentinel a denial wraps, proving errors.Is
// reaches past AccessError to the application's own error types.
var errNotYours = errors.New("not your enrolment")

// authorizeState is shared by the fresh command a registry builds per dispatch
// and by the copies Execute makes per attempt — a pointer field is the
// documented way to carry bookkeeping across both.
type authorizeState struct {
	authorized int
	decided    int
	injected   bool
}

// authorizingCommand judges its caller and then appends one event. deny is what
// Authorize answers; conflict makes Decide slip a competing event into the store
// on its first run, so a test can watch the phase run again on the retry.
type authorizingCommand struct {
	store    collie.Store
	state    *authorizeState
	deny     error
	conflict bool
}

func (cmd *authorizingCommand) Projections(context.Context) ([]collie.Projection, error) {
	return []collie.Projection{
		&collie.DefaultProjection[int]{
			Query: collie.QueryItem{Tags: touched},
			Handlers: map[string]collie.HandleFunc[int]{
				"Touched": func(seen int, _ collie.SequencedEvent) (int, error) { return seen + 1, nil },
			},
		},
	}, nil
}

func (cmd *authorizingCommand) Authorize(context.Context) error {
	cmd.state.authorized++
	return cmd.deny
}

func (cmd *authorizingCommand) Decide(ctx context.Context) ([]collie.Event, error) {
	cmd.state.decided++
	if cmd.conflict && !cmd.state.injected {
		cmd.state.injected = true
		competing, err := collie.NewEvent("Touched", nil, touched)
		if err != nil {
			return nil, fmt.Errorf("could not create competing event: %w", err)
		}
		if err := cmd.store.Append(ctx, []collie.Event{competing}, nil); err != nil {
			return nil, fmt.Errorf("could not append competing event: %w", err)
		}
	}
	event, err := collie.NewEvent("Touched", nil, touched)
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []collie.Event{event}, nil
}

// newAuthorizingRegistry registers the command under a name, building a fresh
// one per dispatch that shares the test's state and its verdict.
func newAuthorizingRegistry(store collie.Store, state *authorizeState, deny error) *command.Registry {
	return command.NewRegistry(store,
		command.WithCommandFunc("authorizing", func() command.Command {
			return &authorizingCommand{store: store, state: state, deny: deny}
		}),
	)
}

// TestDispatch_DeniedAccessClassifies is the taxonomy on a refusal from
// Authorize: the message reaches the caller unchanged, errors.Is says denial and
// not rejection — a 403 and a 409 are different answers — and the sentinel the
// application wrapped stays reachable.
func TestDispatch_DeniedAccessClassifies(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &authorizeState{}
	deny := fmt.Errorf("%w: student %q", errNotYours, "s2")

	err := newAuthorizingRegistry(store, state, deny).Dispatch(ctx, "authorizing", nil)
	if !errors.Is(err, command.ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied, got %v", err)
	}
	if errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("a denial must not classify as a command rejection: %v", err)
	}
	if err.Error() != `not your enrolment: student "s2"` {
		t.Fatalf("expected the denial message unchanged, got %q", err.Error())
	}
	if !errors.Is(err, errNotYours) {
		t.Fatalf("expected the application sentinel to stay reachable, got %v", err)
	}
	var denied *command.AccessError
	if !errors.As(err, &denied) {
		t.Fatalf("expected errors.As to yield the AccessError, got %v", err)
	}
	if state.decided != 0 {
		t.Fatalf("expected Decide never to run, got %d decisions", state.decided)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
}

// TestDispatch_AuthorizedProceedsToDecide is the other half: a yes is silent,
// and the command runs exactly as it would without the phase.
func TestDispatch_AuthorizedProceedsToDecide(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &authorizeState{}

	err := newAuthorizingRegistry(store, state, nil).Dispatch(ctx, "authorizing", nil)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if state.authorized != 1 || state.decided != 1 {
		t.Fatalf("expected one authorization and one decision, got %d and %d", state.authorized, state.decided)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

// TestRegistryExecute_Authorizes pins that a constructed command is judged
// exactly as a dispatched one: the registry exempts nobody, so a transport that
// builds its commands in Go — an IMAP session, an SMTP submission — is guarded
// by the same phase as a request off the wire. A caller with no session is an
// actor the rule has to admit, not one the framework waves through.
func TestRegistryExecute_Authorizes(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &authorizeState{}
	registry := newAuthorizingRegistry(store, state, errNotYours)

	err := registry.Execute(ctx, &authorizingCommand{store: store, state: state, deny: errNotYours})
	if !errors.Is(err, command.ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied, got %v", err)
	}
	if state.authorized != 1 || state.decided != 0 {
		t.Fatalf("expected one authorization and no decision, got %d and %d", state.authorized, state.decided)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
}

// TestDispatch_WithoutAuthorizeIsUnchanged guards the additive promise: a
// command that does not implement Authorizer takes exactly the path it always
// did, with the phase on by default.
func TestDispatch_WithoutAuthorizeIsUnchanged(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	registry := command.NewRegistry(store, command.WithCommands(&signUp{}))
	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if count := usersNamed(t, store, "alice"); count != 1 {
		t.Fatalf("expected 1 signup, got %d", count)
	}
}

// TestExecute_AuthorizeRunsOnEveryAttempt is why the phase sits inside the loop
// rather than before it. A retry rebuilds the decision model, so it must ask
// again: an ownership that changed underneath the first attempt would otherwise
// be judged on a decision model that no longer exists.
//
// It runs through the bare Execute, which is also the fail-closed default —
// nothing asked for authorization here and it happened anyway.
func TestExecute_AuthorizeRunsOnEveryAttempt(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &authorizeState{}

	cmd := &authorizingCommand{store: store, state: state, conflict: true}
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute with retry: %v", err)
	}
	if state.decided != 2 {
		t.Fatalf("expected 2 attempts (1 conflict + 1 success), got %d", state.decided)
	}
	if state.authorized != state.decided {
		t.Fatalf("expected one authorization per attempt, got %d for %d attempts",
			state.authorized, state.decided)
	}
}

// --- the gate: a command that prepares and authorises ---

// gateState records both what ran and in which order. The counters answer "how
// often", and calls answers "before what", which is the whole claim the gate
// makes.
type gateState struct {
	authorized int
	prepared   int
	calls      []string
}

// gatedCommand implements [command.Preparer] and [command.Authorizer] both, so
// Execute judges it once before Prepare and once inside every attempt. deny is
// what Authorize answers; Prepare stands in for the I/O that must not happen to
// a caller who may not be here.
type gatedCommand struct {
	state *gateState
	deny  error
}

func (cmd *gatedCommand) Prepare(context.Context) error {
	cmd.state.prepared++
	cmd.state.calls = append(cmd.state.calls, "prepare")
	return nil
}

func (cmd *gatedCommand) Authorize(context.Context) error {
	cmd.state.authorized++
	cmd.state.calls = append(cmd.state.calls, "authorize")
	return cmd.deny
}

func (cmd *gatedCommand) Projections(context.Context) ([]collie.Projection, error) {
	return []collie.Projection{
		&collie.DefaultProjection[int]{
			Query: collie.QueryItem{Tags: touched},
			Handlers: map[string]collie.HandleFunc[int]{
				"Touched": func(seen int, _ collie.SequencedEvent) (int, error) { return seen + 1, nil },
			},
		},
	}, nil
}

func (cmd *gatedCommand) Decide(context.Context) ([]collie.Event, error) {
	cmd.state.calls = append(cmd.state.calls, "decide")
	event, err := collie.NewEvent("Touched", nil, touched)
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []collie.Event{event}, nil
}

// TestExecute_DeniedBeforePrepare is the guarantee the gate exists for: a caller
// who may not do this pays for a read and nothing else. Prepare stands for an
// LLM call or a payment provider, and it never runs.
func TestExecute_DeniedBeforePrepare(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &gateState{}
	deny := fmt.Errorf("%w: student %q", errNotYours, "s2")

	err := command.Execute(ctx, store, &gatedCommand{state: state, deny: deny})
	if !errors.Is(err, command.ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied, got %v", err)
	}
	if err.Error() != `not your enrolment: student "s2"` {
		t.Fatalf("expected the denial message unchanged, got %q", err.Error())
	}
	if state.prepared != 0 {
		t.Fatalf("expected Prepare never to run for a denied caller, got %d preparations", state.prepared)
	}
	if state.authorized != 1 {
		t.Fatalf("expected exactly one authorization, got %d", state.authorized)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
}

// TestExecute_GateRunsBeforePrepareAndAgainInTheLoop pins the order a permitted
// caller produces, and that the gate does not stand in for the per-attempt pass:
// the two answer different threats, so a clean single-attempt run asks twice.
func TestExecute_GateRunsBeforePrepareAndAgainInTheLoop(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &gateState{}

	err := command.Execute(ctx, store, &gatedCommand{state: state})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	want := []string{"authorize", "prepare", "authorize", "decide"}
	if !slices.Equal(state.calls, want) {
		t.Fatalf("calls = %v, want %v", state.calls, want)
	}
	if state.prepared != 1 {
		t.Fatalf("expected Prepare to run once, got %d", state.prepared)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

// TestRegistryExecute_KeepsTheGate: a constructed command that prepares is
// gated before Prepare exactly as a dispatched one — a denied caller never
// reaches the I/O, whichever door they came through.
func TestRegistryExecute_KeepsTheGate(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	state := &gateState{}

	registry := command.NewRegistry(store)
	err := registry.Execute(ctx, &gatedCommand{state: state, deny: errNotYours})
	if !errors.Is(err, command.ErrAccessDenied) {
		t.Fatalf("expected ErrAccessDenied, got %v", err)
	}
	if state.prepared != 0 {
		t.Fatalf("expected Prepare never to run, got %d", state.prepared)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected no events appended, got %d", count)
	}
}
