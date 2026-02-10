package dcb

// Projector is satisfied by *Projection[S] for any S, or any custom type that
// can apply events and report its query.
type Projector interface {
	EventQuery() QueryItem
	Apply([]SequencedEvent)
}

// BuildDecisionModel reads events matching all projections in one pass, applies
// them to each projection, and returns an AppendCondition whose Query is the
// union of all projection queries and whose After is the highest sequence
// number seen.
func BuildDecisionModel(store EventStore, projections ...Projector) (*AppendCondition, error) {
	var query Query
	for _, p := range projections {
		query = append(query, p.EventQuery())
	}

	events, err := store.Read(query, 0)
	if err != nil {
		return nil, err
	}

	for _, p := range projections {
		p.Apply(events)
	}

	var maxSeq uint64
	for _, e := range events {
		if e.SequenceNumber > maxSeq {
			maxSeq = e.SequenceNumber
		}
	}

	return &AppendCondition{
		Query: query,
		After: maxSeq,
	}, nil
}

func matchesQueryItem(e SequencedEvent, q QueryItem) bool {
	if len(q.Types) > 0 {
		found := false
		for _, t := range q.Types {
			if e.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range q.Tags {
		if e.Tags[k] != v {
			return false
		}
	}
	return true
}
