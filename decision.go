package dcb

import "context"

// Projector is satisfied by *Projection[S] for any S, or any custom type that
// can apply events and report its query.
type Projector interface {
	EventQuery() QueryItem
	Apply(SequencedEvent)
}

// BuildDecisionModel reads events matching all projections in one pass, applies
// each event to the projections it matches, and returns an AppendCondition
// whose Query is the union of all projection queries and whose After is the
// highest sequence number seen.
func BuildDecisionModel(ctx context.Context, store EventStore, projections ...Projector) (*AppendCondition, error) {
	// TODO: add "after" parameter?
	var query Query
	for _, p := range projections {
		query = append(query, p.EventQuery())
	}

	var maxSeq uint64
	for e, err := range store.Read(ctx, query, 0) {
		if err != nil {
			return nil, err
		}
		if e.SequenceNumber > maxSeq {
			maxSeq = e.SequenceNumber
		}
		for _, p := range projections {
			if p.EventQuery().Matches(e) {
				p.Apply(e)
			}
		}
	}

	return &AppendCondition{
		Query: query,
		After: maxSeq,
	}, nil
}
