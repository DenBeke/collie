package memory

import (
	"context"
	"fmt"

	"eventstore"
)

// Projection is an in-memory [eventstore.CheckpointProjector]. It wraps a
// [eventstore.Projection] and tracks the stream position so that
// [eventstore.UpdateProjection] only processes new events on each call.
type Projection[S any] struct {
	eventstore.Projection[S]
	position uint64
}

// NewProjection returns an in-memory [Projection] that starts at position 0.
func NewProjection[S any](p eventstore.Projection[S]) *Projection[S] {
	return &Projection[S]{Projection: p}
}

// After returns the sequence number of the last applied event.
func (p *Projection[S]) After() int64 {
	return int64(p.position)
}

// Apply applies a single event and advances the checkpoint.
func (p *Projection[S]) Apply(ctx context.Context, e eventstore.SequencedEvent) error {
	err := p.Projection.Apply(ctx, e)
	if err != nil {
		return fmt.Errorf("could not apply event: %w", err)
	}
	p.position = e.SequenceNumber
	return nil
}
