package command

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/DenBeke/collie"
)

// Command is a self-handling DCB command: it declares the projections that form
// its decision model and, once those are loaded, decides which events to append.
// A concrete command is named after the operation it performs and carries its
// parameters as fields; Execute runs it.
type Command interface {
	// Projections declares the decision model for this command, built from the
	// command's own fields. It is called once per attempt; Execute runs every
	// attempt on a fresh shallow copy of the command, so it is safe to cache a
	// projection in a field here and read it in Decide — a retry starts from
	// the caller's pristine command and builds anew. The copy is shallow:
	// map, slice, and pointer fields are shared across attempts, so do not
	// mutate shared reference fields in Projections or Decide. The context
	// allows projections that load persisted state (e.g.
	// [snapshot.Projection]); return an error if that loading fails.
	// Returning no projections makes the append unconditional.
	Projections(ctx context.Context) ([]eventstore.Projection, error)
	// Decide runs after the projections are loaded. It reads their state and
	// returns the events to append, an error to abort (nothing is appended), or
	// no events for a no-op.
	Decide(ctx context.Context) ([]eventstore.Event, error)
}

// ErrCommandRejected classifies domain rejections returned by Command.Decide,
// letting transport adapters distinguish a business-rule refusal from an
// infrastructure failure via errors.Is(err, ErrCommandRejected). It is a pure
// identity token: it never appears in any error chain or message —
// DecisionError claims it through its Is method.
var ErrCommandRejected = errors.New("command rejected")

// DecisionError wraps an error returned by Command.Decide: a domain rejection,
// as opposed to an infrastructure failure. Error() returns the domain message
// unchanged, Unwrap lets errors.Is/As reach application-defined error types,
// and Is answers true for ErrCommandRejected.
type DecisionError struct {
	Err error
}

func (e *DecisionError) Error() string { return e.Err.Error() }

func (e *DecisionError) Unwrap() error { return e.Err }

// Is claims ErrCommandRejected, so errors.Is classifies any decision error
// without the sentinel polluting the message.
func (e *DecisionError) Is(target error) bool { return target == ErrCommandRejected }

// ExecuteOption configures Execute.
type ExecuteOption func(*executeConfig)

type executeConfig struct {
	maxAttempts int
	backoff     func(ctx context.Context, attempt int)
}

// WithMaxAttempts sets how many times Execute runs the command before giving up
// on an append-condition conflict. The default is 1 (no retry); retry is opt-in.
// Values below 1 are ignored.
func WithMaxAttempts(maxAttempts int) ExecuteOption {
	return func(config *executeConfig) {
		if maxAttempts >= 1 {
			config.maxAttempts = maxAttempts
		}
	}
}

// WithBackoff sets a function called between retry attempts — after a conflict
// and before the next attempt. attempt is the number of the attempt that just
// failed (starting at 1). Use it to sleep, jitter, or honor ctx cancellation.
func WithBackoff(backoff func(ctx context.Context, attempt int)) ExecuteOption {
	return func(config *executeConfig) {
		config.backoff = backoff
	}
}

// Execute runs a Command against the store: it loads the command's projections
// into a decision model, asks the command to decide, and appends the resulting
// events guarded by an append condition built from exactly those projections —
// so the optimistic-concurrency boundary always covers what the decision read.
//
// On an append-condition conflict it rebuilds the decision model and retries, up
// to the configured number of attempts (one by default — opt in via
// WithMaxAttempts). Execute never mutates the caller's command: every attempt
// runs on a fresh shallow copy of it, so state a command stashes in its fields
// (a cached projection, say) lives and dies with that attempt.
// If Decide returns an error, Execute aborts without appending
// and does not retry; the error comes back wrapped in a DecisionError (matching
// errors.Is(err, ErrCommandRejected)) with the domain message unchanged. If
// Decide returns no events, Execute is a no-op. A command that declares no
// projections appends unconditionally.
func Execute(ctx context.Context, store eventstore.Store, command Command, options ...ExecuteOption) error {
	config := executeConfig{maxAttempts: 1}
	for _, option := range options {
		option(&config)
	}

	for attempt := 1; ; attempt++ {
		cmd := commandForAttempt(command)

		projections, err := cmd.Projections(ctx)
		if err != nil {
			return fmt.Errorf("could not build projections: %w", err)
		}

		var condition *eventstore.AppendCondition
		if len(projections) > 0 {
			built, err := eventstore.BuildDecisionModel(ctx, store, projections...)
			if err != nil {
				return fmt.Errorf("could not build decision model: %w", err)
			}
			condition = built
		}

		events, err := cmd.Decide(ctx)
		if err != nil {
			// A DecisionError keeps the domain message byte-identical while marking
			// the error as a business-rule rejection (ErrCommandRejected).
			return &DecisionError{Err: err}
		}
		if len(events) == 0 {
			return nil
		}

		err = store.Append(ctx, events, condition)
		if err == nil {
			return nil
		}
		if !errors.Is(err, eventstore.ErrAppendConditionViolated) || attempt >= config.maxAttempts {
			return fmt.Errorf("could not append events: %w", err)
		}
		if config.backoff != nil {
			config.backoff(ctx, attempt)
		}
	}
}

// commandForAttempt returns the command instance one attempt runs on: a
// shallow copy of the caller's pristine command, so state stashed by a
// previous attempt never leaks into the next. Commands that are not a
// pointer to a struct are returned as-is (their methods cannot retain
// state between calls anyway).
func commandForAttempt(command Command) Command {
	value := reflect.ValueOf(command)
	if value.Kind() != reflect.Pointer || value.Elem().Kind() != reflect.Struct {
		return command
	}
	clone := reflect.New(value.Elem().Type())
	clone.Elem().Set(value.Elem())
	return clone.Interface().(Command)
}
