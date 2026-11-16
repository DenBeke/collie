package collie

import (
	"context"
	"fmt"
	"maps"
	"slices"
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
//
// A catch-up is a batch: BuildDecisionModel and UpdateProjection call Apply for
// each new event and then Checkpoint exactly once. Implementations that buffer
// state in memory record it durably in Checkpoint (one write per catch-up
// instead of one per event); implementations that persist in Apply return nil.
type CheckpointProjection interface {
	Projection
	After() uint64
	// Checkpoint records progress after a complete catch-up: every matching event
	// up to upTo has been applied.
	Checkpoint(ctx context.Context, upTo uint64) error
}

// HandleFunc processes a single event and returns the updated state.
type HandleFunc[S any] func(S, SequencedEvent) (S, error)

// DefaultProjection defines how to apply matching events into a state of type S.
// Query controls which events are read from the store; when Query.Types is
// empty, the types derive from the Handlers keys — the usual case, since an
// event type without a handler cannot change the state (and so cannot change
// any decision built on it). Handlers maps event types to functions that update
// state; unhandled types are ignored.
type DefaultProjection[S any] struct {
	Query    QueryItem
	State    S
	Handlers map[string]HandleFunc[S]
}

// EventQuery returns the projection's QueryItem. When Query.Types is empty, the
// types are the sorted Handlers keys, so query and handlers cannot drift apart.
func (p *DefaultProjection[S]) EventQuery() QueryItem {
	query := p.Query
	if len(query.Types) == 0 && len(p.Handlers) > 0 {
		query.Types = slices.Sorted(maps.Keys(p.Handlers))
	}
	return query
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
// since its last checkpoint, then calling Checkpoint once to record the
// progress. Only events matching the projection's query are read and applied.
func UpdateProjection(ctx context.Context, store Store, p CheckpointProjection) error {
	events, upTo, err := store.Read(ctx, Query{p.EventQuery()}, p.After())
	if err != nil {
		return fmt.Errorf("could not read events: %w", err)
	}
	for e, err := range events {
		if err != nil {
			return fmt.Errorf("could not read events: %w", err)
		}
		err = p.Apply(ctx, e)
		if err != nil {
			return fmt.Errorf("could not apply %s event: %w", e.Type, err)
		}
		upTo = max(upTo, e.SequenceNumber)
	}
	if err := p.Checkpoint(ctx, upTo); err != nil {
		return fmt.Errorf("could not checkpoint projection: %w", err)
	}
	return nil
}
