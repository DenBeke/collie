// Package eventstore defines the core types and interface for a Dynamic
// Consistency Boundary event store. See https://dcb.events/specification/
package eventstore

import (
	"context"
	"errors"
	"iter"
)

// AppendCondition enforces optimistic concurrency: the append fails with
// ErrAppendConditionViolated if the store contains any event matching Query with
// a sequence number strictly greater than After — i.e. someone changed relevant
// state since you last read it. After 0 checks against all events.
//
// Query uses the usual semantics, so a nil/empty Query matches EVERY event: such
// a condition fails if any event at all was appended after After (a global
// "nothing changed since" guard, rarely what you want for a scoped boundary).
type AppendCondition struct {
	Query Query
	After uint64
}

// ErrAppendConditionViolated is returned by Append when the AppendCondition is
// violated, meaning relevant state changed since the caller last read.
var ErrAppendConditionViolated = errors.New("append condition violated")

// Store reads and appends events.
type Store interface {
	// Read returns the events matching query after the given sequence number, and
	// head: the store's last sequence number when Read was called. After a complete,
	// error-free iteration, every matching event up to head has been yielded.
	Read(ctx context.Context, query Query, after uint64) (iter.Seq2[SequencedEvent, error], uint64, error)

	// Append atomically persists events. If condition is non-nil, the append
	// fails with ErrAppendConditionViolated when the store contains any event
	// matching condition.Query with a sequence number greater than condition.After.
	Append(ctx context.Context, events []Event, condition *AppendCondition) error
}
