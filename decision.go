package eventstore

import (
	"context"
	"fmt"
	"math"
)

// BuildDecisionModel reads events matching all projections in one pass, applies
// each event to the projections it matches, and returns an AppendCondition
// whose Query is the union of all projection queries and whose After is the
// highest sequence number seen.
//
// Projections that implement CheckpointProjection are treated as stored
// decision models: events at or before their checkpoint are skipped, and the
// read starts from the lowest checkpoint across all projections.
func BuildDecisionModel(ctx context.Context, store Store, projections ...Projection) (*AppendCondition, error) {
	var query Query
	for _, projection := range projections {
		query = append(query, projection.EventQuery())
	}

	minAfter, maxAfter := readOffset(projections)

	for event, err := range store.Read(ctx, query, minAfter) {
		if err != nil {
			return nil, fmt.Errorf("could not read events: %w", err)
		}
		if event.SequenceNumber > maxAfter {
			maxAfter = event.SequenceNumber
		}
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
			if err := stored.Checkpoint(ctx); err != nil {
				return nil, fmt.Errorf("could not checkpoint projection: %w", err)
			}
		}
	}

	return &AppendCondition{
		Query: query,
		After: maxAfter,
	}, nil
}

// readOffset returns the sequence number to start reading from. For stored
// projections this is the lowest checkpoint; fresh projections force a full
// replay from 0. maxAfter is the highest checkpoint seen — we need it because
// when no new events exist after the checkpoint, the read loop never executes,
// and without maxAfter the AppendCondition would get After=0 ("reject if ANY
// matching event exists"), which is wrong. maxAfter preserves the fact that we
// already know about events up to that point.
func readOffset(projections []Projection) (minAfter, maxAfter uint64) {
	minAfter = math.MaxUint64
	for _, projection := range projections {
		stored, ok := projection.(CheckpointProjection)
		if !ok {
			return 0, 0 // fresh projection → full replay
		}
		checkpoint := stored.After()
		if checkpoint < minAfter {
			minAfter = checkpoint
		}
		if checkpoint > maxAfter {
			maxAfter = checkpoint
		}
	}
	if minAfter == math.MaxUint64 {
		return 0, 0
	}
	return minAfter, maxAfter
}
