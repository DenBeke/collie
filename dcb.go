// Package dcb defines the core types and interface for a Dynamic Consistency
// Boundary event store. See https://dcb.events/specification/
package dcb

// Event represents a domain event to be appended to the store.
// Tags are optional key-value metadata used for filtering and partitioning.
type Event struct {
	Type string
	Data string
	Tags map[string]string
}

// SequencedEvent is an Event that has been persisted and assigned a unique,
// monotonically increasing sequence number by the store.
type SequencedEvent struct {
	Event
	SequenceNumber uint64
}

// QueryItem matches events whose type is in Types (OR) and whose tags contain
// every entry in Tags (AND). An empty Types matches all types. An empty Tags
// imposes no tag constraint.
type QueryItem struct {
	Types []string
	Tags  map[string]string
}

// Query is a list of QueryItems combined with OR logic. An event matches if it
// satisfies at least one item. A nil/empty Query matches all events.
type Query []QueryItem

// AppendCondition enforces optimistic concurrency. The append fails if the
// store contains any event matching Query after the given sequence number,
// meaning someone else changed relevant state since you last read it.
// Use After 0 to check against all events.
type AppendCondition struct {
	Query Query
	After uint64
}

// EventStore reads and appends events.
type EventStore interface {
	// Read returns events matching the query with a sequence number greater
	// than after. Pass 0 to read from the beginning.
	Read(query Query, after uint64) ([]SequencedEvent, error)

	// Append atomically persists events. If condition is non-nil, the append
	// fails when the store contains matching events.
	Append(events []Event, condition *AppendCondition) error
}
