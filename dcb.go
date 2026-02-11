// Package dcb defines the core types and interface for a Dynamic Consistency
// Boundary event store. See https://dcb.events/specification/
package dcb

import (
	"context"
	"errors"
	"iter"
	"time"
)

// Event represents a domain event to be appended to the store.
// Tags are optional key-value metadata used for filtering and partitioning.
type Event struct {
	Type string
	Data []byte
	Tags map[string]string
}

// SequencedEvent is an Event that has been persisted and assigned a unique,
// monotonically increasing sequence number by the store.
type SequencedEvent struct {
	Event
	SequenceNumber uint64
	CreatedAt      time.Time
}

// QueryItem matches events whose type is in Types (OR) and whose tags contain
// every entry in Tags (AND). An empty Types matches all types. An empty Tags
// imposes no tag constraint.
type QueryItem struct {
	Types []string
	Tags  map[string]string
}

// Matches reports whether the event satisfies this query item.
func (q QueryItem) Matches(e SequencedEvent) bool {
	if len(q.Types) > 0 {
		found := false
		for _, t := range q.Types {
			if e.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range q.Tags {
		if e.Tags[k] != v {
			return false
		}
	}
	return true
}

// Query is a list of QueryItems combined with OR logic. An event matches if it
// satisfies at least one item. A nil/empty Query matches all events.
type Query []QueryItem

// Matches reports whether the event satisfies at least one item in the query.
// A nil/empty Query matches all events.
func (q Query) Matches(e SequencedEvent) bool {
	if len(q) == 0 {
		return true
	}
	for _, item := range q {
		if item.Matches(e) {
			return true
		}
	}
	return false
}

// AppendCondition enforces optimistic concurrency. The append fails if the
// store contains any event matching Query after the given sequence number,
// meaning someone else changed relevant state since you last read it.
// Use After 0 to check against all events.
type AppendCondition struct {
	Query Query
	After uint64
}

// ErrConditionViolated is returned by Append when the AppendCondition is
// violated, meaning relevant state changed since the caller last read.
var ErrConditionViolated = errors.New("append condition violated")

// EventStore reads and appends events.
type EventStore interface {
	// Read returns an iterator over events matching the query with a sequence
	// number greater than after. Pass 0 to read from the beginning.
	Read(ctx context.Context, query Query, after uint64) iter.Seq2[SequencedEvent, error]

	// Append atomically persists events. If condition is non-nil, the append
	// fails with ErrConditionViolated when the store contains matching events.
	Append(ctx context.Context, events []Event, condition *AppendCondition) error
}
