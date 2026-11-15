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

// Keyable reports whether the condition names a finite set of boundaries: its
// Query is not empty and every item has at least one type and one tag. A store
// that locks per boundary can only take such a condition; see
// ErrUnkeyableCondition.
func (c AppendCondition) Keyable() bool {
	if len(c.Query) == 0 {
		return false
	}
	for _, item := range c.Query {
		if len(item.Types) == 0 || len(item.Tags) == 0 {
			return false
		}
	}
	return true
}

// ErrAppendConditionViolated is returned by Append when the AppendCondition is
// violated, meaning relevant state changed since the caller last read.
var ErrAppendConditionViolated = errors.New("append condition violated")

// ErrUnkeyableCondition is returned by Append when the store only takes keyable
// conditions (see AppendCondition.Keyable) and was given another one: a
// match-all, or an item without a type or without a tag. The memory and
// PostgreSQL stores refuse these by default and take them WithGlobalLock. To
// make a whole kind of event one boundary, give each of those events a constant
// tag and put it in the condition.
var ErrUnkeyableCondition = errors.New("append condition is not keyable: the query must be non-empty and every item needs at least one type and one tag")

// Store reads and appends events.
//
// Two things are left to the store, and code written against this interface
// must not assume either way:
//
//   - Append may refuse a condition that is not keyable, with
//     ErrUnkeyableCondition.
//   - Events may become readable some time after Append returns. They count
//     for every later condition at once, but Read, and so a decision model,
//     may not show them yet; a decision on the same boundary made in that
//     window fails with ErrAppendConditionViolated and succeeds on a retry.
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
