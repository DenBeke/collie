package eventstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event represents a domain event to be appended to the store.
// Tags are optional key-value metadata used for filtering and partitioning.
// Metadata carries middleware-specific data (e.g. encryption keys, blob refs).
type Event struct {
	Type     string
	Data     []byte
	Tags     map[string]string
	Metadata map[string]string
}

// SequencedEvent is an Event that has been persisted and assigned a unique,
// monotonically increasing sequence number by the store.
type SequencedEvent struct {
	Event
	SequenceNumber uint64
	CreatedAt      time.Time
}

// EventData unmarshals the event's Data field into a value of type T.
func EventData[T any](e SequencedEvent) (T, error) {
	var t T
	err := json.Unmarshal(e.Data, &t)
	if err != nil {
		return t, fmt.Errorf("could not unmarshal event data: %w", err)
	}
	return t, nil
}

// EventOption configures optional fields on an Event.
type EventOption func(*Event)

// NewEvent creates an Event by JSON-marshaling data into the Data field.
func NewEvent(eventType string, data any, tags map[string]string, opts ...EventOption) (Event, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return Event{}, fmt.Errorf("could not marshal event data: %w", err)
	}
	e := Event{Type: eventType, Data: bytes, Tags: tags}
	for _, opt := range opts {
		opt(&e)
	}
	return e, nil
}
