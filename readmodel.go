package eventstore

import (
	"context"
	"fmt"
)

// ReadModel is a long-living, incrementally updated projection used to serve
// read queries (e.g. API GET endpoints). Unlike decision model projections
// which are built on the fly per command, a ReadModel tracks its position in
// the event stream and catches up with new events on demand.
//
// TODO add way to store it in datahase or partition it.
type ReadModel[S any] struct {
	Projection[S]
	store          Store
	sequenceNumber uint64
}

// NewReadModel creates a ReadModel from a Projection.
func NewReadModel[S any](p Projection[S], store Store, sequenceNumber uint64) *ReadModel[S] {
	return &ReadModel[S]{
		Projection:     p,
		store:          store,
		sequenceNumber: sequenceNumber,
	}
}

// Update catches up the read model by applying all new events since the last
// update. Only events matching the projection's query are read and applied.
func (rm *ReadModel[S]) Update(ctx context.Context) error {
	for e, err := range rm.store.Read(ctx, Query{rm.Query}, rm.sequenceNumber) {
		if err != nil {
			return fmt.Errorf("could not read events: %w", err)
		}
		if err := rm.Apply(e); err != nil {
			return fmt.Errorf("could not apply %s event: %w", e.Type, err)
		}
		rm.sequenceNumber = e.SequenceNumber
	}
	return nil
}
