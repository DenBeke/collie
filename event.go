package eventstore

import (
	"encoding/json"
	"fmt"
	"time"
)

// Tag is a single key/value label on an event, used for filtering and for
// defining consistency boundaries. An event may carry the same key more than
// once (e.g. a transfer tagged with two account ids), so tags are an ordered
// list rather than a map.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NewTag builds a single Tag.
func NewTag(key, value string) Tag {
	return Tag{Key: key, Value: value}
}

// NewTags builds a []Tag from alternating key, value arguments:
//
//	NewTags("student", "s1", "course", "c1")
//
// NewTags panics on an odd argument count (like strings.NewReplacer): a
// silently dropped trailing key would widen a consistency boundary.
func NewTags(keyValuePairs ...string) []Tag {
	if len(keyValuePairs)%2 != 0 {
		panic("eventstore.NewTags: odd argument count")
	}
	tags := make([]Tag, 0, len(keyValuePairs)/2)
	for i := 0; i+1 < len(keyValuePairs); i += 2 {
		tags = append(tags, Tag{Key: keyValuePairs[i], Value: keyValuePairs[i+1]})
	}
	return tags
}

// Event represents a domain event to be appended to the store.
// Tags are optional key/value labels used for filtering and for defining
// consistency boundaries; the same key may appear more than once.
// Actor and Cause are the envelope: who caused this event and what it followed
// from. Domain code leaves them zero — they are stamped from the context just
// before the event is persisted; see the envelope package for the whole design.
// Metadata carries middleware-specific data (e.g. encryption keys, blob refs).
type Event struct {
	Type     string
	Data     []byte
	Tags     []Tag
	Actor    string
	Cause    string
	Metadata map[string]string
}

// TagValues returns all values of tags with the given key, in order, or nil if
// the event has no such tag. A key may appear more than once (e.g. an event
// touching two accounts), so this returns every match rather than one value.
func (e Event) TagValues(key string) []string {
	var values []string
	for _, t := range e.Tags {
		if t.Key == key {
			values = append(values, t.Value)
		}
	}
	return values
}

// SequencedEvent is an Event that has been persisted and assigned a unique,
// monotonically increasing sequence number by the store.
//
// RecordedAt is the read-side half of the envelope: the moment the store
// accepted the event, assigned by the store and by nothing else. It is zero for
// events written before the envelope existed.
type SequencedEvent struct {
	Event
	SequenceNumber uint64
	RecordedAt     time.Time
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
func NewEvent(eventType string, data any, tags []Tag, opts ...EventOption) (Event, error) {
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
