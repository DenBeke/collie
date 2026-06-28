package memory

import (
	"context"
	"fmt"

	"eventstore"
)

// CheckpointProjection is an in-memory [eventstore.CheckpointProjection]. It
// wraps a [eventstore.DefaultProjection] and tracks the stream position so that
// [eventstore.UpdateProjection] only processes new events on each call.
type CheckpointProjection[S any] struct {
	eventstore.DefaultProjection[S]
	position uint64
}

// NewCheckpointProjection returns an in-memory [CheckpointProjection] that
// starts at position 0.
func NewCheckpointProjection[S any](p eventstore.DefaultProjection[S]) *CheckpointProjection[S] {
	return &CheckpointProjection[S]{DefaultProjection: p}
}

// After returns the sequence number of the last applied event.
func (p *CheckpointProjection[S]) After() uint64 {
	return p.position
}

// Apply applies a single event and advances the checkpoint.
func (p *CheckpointProjection[S]) Apply(ctx context.Context, e eventstore.SequencedEvent) error {
	err := p.DefaultProjection.Apply(ctx, e)
	if err != nil {
		return fmt.Errorf("could not apply event: %w", err)
	}
	p.position = e.SequenceNumber
	return nil
}
