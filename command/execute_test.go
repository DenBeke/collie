package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
)

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

// countAll returns the number of events in the store (nil query matches all).
func countAll(t *testing.T, store eventstore.Store) int {
	t.Helper()
	count := 0
	for _, err := range store.Read(context.Background(), nil, 0) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	return count
}

func TestExecute_AppendsOnSuccess(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if err := command.Execute(ctx, store, &signUp{Username: "alice"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

func TestExecute_AbortsOnDecideError(t *testing.T) {
	ctx := context.Background()
	store := seed(t, 1, "UserSignedUp", eventstore.NewTags("username", "alice"))

	err := command.Execute(ctx, store, &signUp{Username: "alice"})
	if err == nil || !strings.Contains(err.Error(), "already taken") {
		t.Fatalf("expected 'already taken' error, got %v", err)
	}
	// Decide aborted: no new event appended (still just the seeded one).
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event (no append on abort), got %d", count)
	}
}

func TestExecute_NoOpWhenNoEvents(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if err := command.Execute(ctx, store, noopCommand{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected 0 events, got %d", count)
	}
}

func TestExecute_UnconditionalWhenNoProjections(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if err := command.Execute(ctx, store, unconditionalCommand{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

func TestExecute_RetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newConflictOnce(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute with retry: %v", err)
	}
	if cmd.state.attempts != 2 {
		t.Fatalf("expected 2 attempts (1 conflict + 1 success), got %d", cmd.state.attempts)
	}
	// The injected competing event plus the command's own event.
	if count := countAll(t, store); count != 2 {
		t.Fatalf("expected 2 events, got %d", count)
	}
}

func TestExecute_NoRetryByDefault(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newConflictOnce(store)
	err := command.Execute(ctx, store, cmd) // default: one attempt
	if !errors.Is(err, eventstore.ErrAppendConditionViolated) {
		t.Fatalf("expected ErrAppendConditionViolated, got %v", err)
	}
	if cmd.state.attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", cmd.state.attempts)
	}
}

// TestExecute_RetryDecidesFromFreshState is the regression test for
// clone-per-attempt: a command that builds its projection once and caches it
// in a field would, without the per-attempt copy, hand the already-populated
// projection to the retry's decision model — the whole history re-applies onto
// existing state (the count doubles) and Decide falsely hits capacity.
func TestExecute_RetryDecidesFromFreshState(t *testing.T) {
	ctx := context.Background()
	store := seed(t, 1, "Touched", nil)

	cmd := newCachingCommand(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute with retry: %v", err)
	}
	// The seeded event, the injected competing event, and the command's own.
	if count := countAll(t, store); count != 3 {
		t.Fatalf("expected 3 events, got %d", count)
	}
}

// TestExecute_CallerCommandStaysPristine proves Execute never mutates the
// caller's command: attempts run on shallow copies, so the projection an
// attempt stashed never appears on the original.
func TestExecute_CallerCommandStaysPristine(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := newCachingCommand(store)
	if err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if cmd.seen != nil {
		t.Fatal("expected the caller's command untouched: stashed projection must stay nil")
	}
	if cmd.state.attempts != 2 {
		t.Fatalf("expected shared pointer state to survive copies (2 attempts), got %d", cmd.state.attempts)
	}
}

// errUsernameTaken is a domain sentinel a command wraps into its Decide error,
// proving application error types survive the trip through Execute.
var errUsernameTaken = errors.New("username taken")

// rejectingCommand always rejects with a wrapped domain sentinel.
type rejectingCommand struct{}

func (rejectingCommand) Projections(context.Context) ([]eventstore.Projection, error) {
	return nil, nil
}
func (rejectingCommand) Decide(context.Context) ([]eventstore.Event, error) {
	return nil, fmt.Errorf("%w: %q", errUsernameTaken, "alice")
}

// TestExecute_DecisionErrorClassification proves the error taxonomy on domain
// rejections: errors.Is matches ErrCommandRejected, the message is the domain
// message unchanged, and application sentinels wrapped by Decide stay reachable.
func TestExecute_DecisionErrorClassification(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	err := command.Execute(ctx, store, rejectingCommand{})
	if err == nil {
		t.Fatal("expected a rejection")
	}
	if !errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("expected errors.Is(err, ErrCommandRejected), got %v", err)
	}
	if err.Error() != `username taken: "alice"` {
		t.Fatalf("expected the domain message unchanged, got %q", err.Error())
	}
	if !errors.Is(err, errUsernameTaken) {
		t.Fatalf("expected the domain sentinel to be reachable, got %v", err)
	}
	var decision *command.DecisionError
	if !errors.As(err, &decision) || decision.Err.Error() != `username taken: "alice"` {
		t.Fatalf("expected errors.As to yield the DecisionError, got %v", err)
	}
	if countAll(t, store) != 0 {
		t.Fatal("expected no events appended on rejection")
	}
}

// TestExecute_ConflictIsNotADecisionError proves the two 4xx-ish classes stay
// distinct: an exhausted optimistic retry matches ErrAppendConditionViolated
// but not ErrCommandRejected.
func TestExecute_ConflictIsNotADecisionError(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	err := command.Execute(ctx, store, newConflictOnce(store)) // one attempt: conflict surfaces
	if !errors.Is(err, eventstore.ErrAppendConditionViolated) {
		t.Fatalf("expected ErrAppendConditionViolated, got %v", err)
	}
	if errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("conflict must not classify as a command rejection: %v", err)
	}
}

// --- test commands ---

// noopCommand declares nothing and decides nothing.
type noopCommand struct{}

func (noopCommand) Projections(context.Context) ([]eventstore.Projection, error) { return nil, nil }
func (noopCommand) Decide(context.Context) ([]eventstore.Event, error)           { return nil, nil }

// unconditionalCommand declares no projections and always appends one event.
type unconditionalCommand struct{}

func (unconditionalCommand) Projections(context.Context) ([]eventstore.Projection, error) {
	return nil, nil
}
func (unconditionalCommand) Decide(context.Context) ([]eventstore.Event, error) {
	event, err := eventstore.NewEvent("Pinged", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create Pinged event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// conflictOnce forces exactly one append-condition conflict: on the first attempt
// it slips a competing event into the store after the decision model is built
// (in Decide), so Execute's guarded append fails; the retry then succeeds.
// Execute runs each attempt on a shallow copy of the command, so cross-attempt
// bookkeeping lives behind a pointer field the copies share — the documented
// way to carry state across attempts.
type conflictState struct {
	attempts int
	injected bool
}

type conflictOnce struct {
	store eventstore.Store
	state *conflictState
}

func newConflictOnce(store eventstore.Store) *conflictOnce {
	return &conflictOnce{store: store, state: &conflictState{}}
}

func (cmd *conflictOnce) Projections(context.Context) ([]eventstore.Projection, error) {
	cmd.state.attempts++
	return []eventstore.Projection{
		&eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"Touched"}},
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Touched": func(seen int, _ eventstore.SequencedEvent) (int, error) { return seen + 1, nil },
			},
		},
	}, nil
}

func (cmd *conflictOnce) Decide(ctx context.Context) ([]eventstore.Event, error) {
	if !cmd.state.injected {
		cmd.state.injected = true
		competing, err := eventstore.NewEvent("Touched", nil, nil)
		if err != nil {
			return nil, fmt.Errorf("could not create competing event: %w", err)
		}
		if err := cmd.store.Append(ctx, []eventstore.Event{competing}, nil); err != nil {
			return nil, fmt.Errorf("could not append competing event: %w", err)
		}
	}
	event, err := eventstore.NewEvent("Touched", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// cachingCommand builds its projection once and stashes it in a field, reading
// it in Decide — the pattern the per-attempt copy makes safe. Like conflictOnce
// it injects one competing event in Decide to force a retry, and it rejects
// when the count reaches capacity — which a doubled count would falsely hit.
type cachingCommand struct {
	store eventstore.Store
	state *conflictState
	seen  *eventstore.DefaultProjection[int]
}

func newCachingCommand(store eventstore.Store) *cachingCommand {
	return &cachingCommand{store: store, state: &conflictState{}}
}

func (cmd *cachingCommand) Projections(context.Context) ([]eventstore.Projection, error) {
	cmd.state.attempts++
	if cmd.seen == nil {
		cmd.seen = &eventstore.DefaultProjection[int]{
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Touched": func(seen int, _ eventstore.SequencedEvent) (int, error) { return seen + 1, nil },
			},
		}
	}
	return []eventstore.Projection{cmd.seen}, nil
}

func (cmd *cachingCommand) Decide(ctx context.Context) ([]eventstore.Event, error) {
	if cmd.seen.State >= 3 {
		return nil, fmt.Errorf("capacity reached: %d touched", cmd.seen.State)
	}
	if !cmd.state.injected {
		cmd.state.injected = true
		competing, err := eventstore.NewEvent("Touched", nil, nil)
		if err != nil {
			return nil, fmt.Errorf("could not create competing event: %w", err)
		}
		if err := cmd.store.Append(ctx, []eventstore.Event{competing}, nil); err != nil {
			return nil, fmt.Errorf("could not append competing event: %w", err)
		}
	}
	event, err := eventstore.NewEvent("Touched", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []eventstore.Event{event}, nil
}
