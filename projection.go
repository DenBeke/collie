package eventstore

import (
	"context"
	"fmt"
)

// Projection is satisfied by *DefaultProjection[S] for any S, or any custom type
// that can apply events and report its query.
type Projection interface {
	EventQuery() QueryItem
	Apply(context.Context, SequencedEvent) error
}

// CheckpointProjection is a Projection that tracks its position in the event
// stream. When a Projection also implements this interface, BuildDecisionModel
// skips events it has already processed, and UpdateProjection uses the position
// to apply only new events. Implementations decide how state and position are
// stored (in-memory, database snapshot, per-row, etc.).
type CheckpointProjection interface {
	Projection
	After() uint64
}

// HandleFunc processes a single event and returns the updated state.
type HandleFunc[S any] func(S, SequencedEvent) (S, error)

// DefaultProjection defines how to apply matching events into a state of type S.
// Query controls which events are read from the store. Handlers maps event
// types to functions that update state; unhandled types are ignored.
type DefaultProjection[S any] struct {
	Query    QueryItem
	State    S
	Handlers map[string]HandleFunc[S]
}

// EventQuery returns the projection's QueryItem.
func (p *DefaultProjection[S]) EventQuery() QueryItem {
	return p.Query
}

// Apply applies a single event to State.
func (p *DefaultProjection[S]) Apply(_ context.Context, e SequencedEvent) error {
	if h, ok := p.Handlers[e.Type]; ok {
		s, err := h(p.State, e)
		if err != nil {
			return fmt.Errorf("could not handle %s event: %w", e.Type, err)
		}
		p.State = s
	}
	return nil
}

// UpdateProjection catches up a CheckpointProjection by applying all new events
// since its last checkpoint. Only events matching the projection's query are
// read and applied.
func UpdateProjection(ctx context.Context, store Store, p CheckpointProjection) error {
	for e, err := range store.Read(ctx, Query{p.EventQuery()}, p.After()) {
		if err != nil {
			return fmt.Errorf("could not read events: %w", err)
		}
		err = p.Apply(ctx, e)
		if err != nil {
			return fmt.Errorf("could not apply %s event: %w", e.Type, err)
		}
	}
	return nil
}
