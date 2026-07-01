package eventstore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"eventstore"
	"eventstore/memory"
)

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

	if err := eventstore.Execute(ctx, store, &SignUp{Username: "alice"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

func TestExecute_AbortsOnDecideError(t *testing.T) {
	ctx := context.Background()
	store := seed(t, 1, "UserSignedUp", eventstore.NewTags("username", "alice"))

	err := eventstore.Execute(ctx, store, &SignUp{Username: "alice"})
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

	if err := eventstore.Execute(ctx, store, noopCommand{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 0 {
		t.Fatalf("expected 0 events, got %d", count)
	}
}

func TestExecute_UnconditionalWhenNoProjections(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	if err := eventstore.Execute(ctx, store, unconditionalCommand{}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if count := countAll(t, store); count != 1 {
		t.Fatalf("expected 1 event, got %d", count)
	}
}

func TestExecute_RetriesOnConflict(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := &conflictOnce{store: store}
	if err := eventstore.Execute(ctx, store, cmd, eventstore.WithMaxAttempts(3)); err != nil {
		t.Fatalf("execute with retry: %v", err)
	}
	if cmd.attempts != 2 {
		t.Fatalf("expected 2 attempts (1 conflict + 1 success), got %d", cmd.attempts)
	}
	// The injected competing event plus the command's own event.
	if count := countAll(t, store); count != 2 {
		t.Fatalf("expected 2 events, got %d", count)
	}
}

func TestExecute_NoRetryByDefault(t *testing.T) {
	ctx := context.Background()
	store := memory.New()

	cmd := &conflictOnce{store: store}
	err := eventstore.Execute(ctx, store, cmd) // default: one attempt
	if !errors.Is(err, eventstore.ErrAppendConditionViolated) {
		t.Fatalf("expected ErrAppendConditionViolated, got %v", err)
	}
	if cmd.attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", cmd.attempts)
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
type conflictOnce struct {
	store    eventstore.Store
	attempts int
	injected bool
}

func (cmd *conflictOnce) Projections(context.Context) ([]eventstore.Projection, error) {
	cmd.attempts++
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
	if !cmd.injected {
		cmd.injected = true
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
