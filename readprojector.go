package eventstore

import (
	"context"
	"fmt"
)

// CheckpointProjector is a Projector that tracks its position in the event
// stream. Implementations decide how state and position are stored (in-memory,
// database snapshot, per-row, etc.).
type CheckpointProjector interface {
	EventQuery() QueryItem
	After() int64
	Apply(context.Context, SequencedEvent) error
}

// UpdateProjection catches up a CheckpointProjector by applying all new
// events since its last checkpoint. Only events matching the projector's query
// are read and applied.
func UpdateProjection(ctx context.Context, store Store, p CheckpointProjector) error {
	for e, err := range store.Read(ctx, Query{p.EventQuery()}, uint64(p.After())) {
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
