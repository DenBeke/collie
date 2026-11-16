package collie

import (
	"context"
	"fmt"
	"math"
)

// BuildDecisionModel reads events matching all projections in one pass, applies
// each event to the projections it matches, and returns an AppendCondition
// whose Query is the union of all projection queries and whose After is the
// read's head.
//
// Projections that implement CheckpointProjection are treated as stored
// decision models: events at or before their checkpoint are skipped, and the
// read starts from the lowest checkpoint across all projections.
func BuildDecisionModel(ctx context.Context, store Store, projections ...Projection) (*AppendCondition, error) {
	var query Query
	for _, projection := range projections {
		query = append(query, projection.EventQuery())
	}

	events, upTo, err := store.Read(ctx, query, readOffset(projections))
	if err != nil {
		return nil, fmt.Errorf("could not read events: %w", err)
	}
	for event, err := range events {
		if err != nil {
			return nil, fmt.Errorf("could not read events: %w", err)
		}
		upTo = max(upTo, event.SequenceNumber)
		for _, projection := range projections {
			if !projection.EventQuery().Matches(event) {
				continue
			}
			if stored, ok := projection.(CheckpointProjection); ok && event.SequenceNumber <= stored.After() {
				continue
			}
			if err := projection.Apply(ctx, event); err != nil {
				return nil, fmt.Errorf("could not apply %s event: %w", event.Type, err)
			}
		}
	}

	for _, projection := range projections {
		if stored, ok := projection.(CheckpointProjection); ok {
			if err := stored.Checkpoint(ctx, upTo); err != nil {
				return nil, fmt.Errorf("could not checkpoint projection: %w", err)
			}
		}
	}

	return &AppendCondition{
		Query: query,
		After: upTo,
	}, nil
}

// readOffset returns the sequence number to start reading from: the lowest
// checkpoint across the projections, or 0 when any of them is fresh.
func readOffset(projections []Projection) uint64 {
	minAfter := uint64(math.MaxUint64)
	for _, projection := range projections {
		stored, ok := projection.(CheckpointProjection)
		if !ok {
			return 0 // fresh projection → full replay
		}
		minAfter = min(minAfter, stored.After())
	}
	if minAfter == math.MaxUint64 {
		return 0
	}
	return minAfter
}
