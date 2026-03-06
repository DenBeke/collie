package eventstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Event represents a domain event to be appended to the store.
// Tags are optional key-value metadata used for filtering and partitioning.
// EncryptionKeys lists tag names whose values identify the data subject for
// GDPR crypto-shredding. Every name must exist as a key in Tags.
type Event struct {
	Type           string
	Data           []byte
	Tags           map[string]string
	EncryptionKeys []string
}

// SequencedEvent is an Event that has been persisted and assigned a unique,
// monotonically increasing sequence number by the store.
type SequencedEvent struct {
	Event
	SequenceNumber uint64
	CreatedAt      time.Time
}

// Redacted returns true if the event's data has been crypto-shredded
// and is no longer readable.
func (e SequencedEvent) Redacted() bool {
	return e.Data == nil
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

// ErrMissingEncryptionKeyTag is returned when an encryption key name does not
// exist as a tag on the event.
var ErrMissingEncryptionKeyTag = errors.New("encryption key references missing tag")

// EventOption configures optional fields on an Event.
type EventOption func(*Event)

// WithEncryptionKeys marks the event for GDPR crypto-shredding. The provided
// names must each exist as a key in the event's Tags; their values identify
// the data subject whose encryption key protects this event's Data.
func WithEncryptionKeys(keys ...string) EventOption {
	return func(e *Event) { e.EncryptionKeys = keys }
}

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
	for _, key := range e.EncryptionKeys {
		if _, ok := tags[key]; !ok {
			return Event{}, fmt.Errorf("%w: %q", ErrMissingEncryptionKeyTag, key)
		}
	}
	return e, nil
}
