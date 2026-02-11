// Package eventstore defines the core types and interface for a Dynamic
// Consistency Boundary event store. See https://dcb.events/specification/
package eventstore

import (
	"context"
	"errors"
	"iter"
)

// AppendCondition enforces optimistic concurrency. The append fails if the
// store contains any event matching Query after the given sequence number,
// meaning someone else changed relevant state since you last read it.
// Use After 0 to check against all events.
type AppendCondition struct {
	Query Query
	After uint64
}

// ErrAppendConditionViolated is returned by Append when the AppendCondition is
// violated, meaning relevant state changed since the caller last read.
var ErrAppendConditionViolated = errors.New("append condition violated")

// Store reads and appends events.
type Store interface {
	// Read returns an iterator over events matching the query with a sequence
	// number greater than after. Pass 0 to read from the beginning.
	Read(ctx context.Context, query Query, after uint64) iter.Seq2[SequencedEvent, error]

	// Append atomically persists events. If condition is non-nil, the append
	// fails with ErrConditionViolated when the store contains matching events.
	Append(ctx context.Context, events []Event, condition *AppendCondition) error
}
