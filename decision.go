package dcb

// Option configures a BuildDecisionModel call.
type Option func(*decisionModel)

type decisionModel struct {
	query   Query
	runners []func([]SequencedEvent)
}

// WithProjection adds a projection to the decision model. After
// BuildDecisionModel returns, *target contains the folded state.
func WithProjection[S any](target *S, p Projection[S]) Option {
	return func(dm *decisionModel) {
		dm.query = append(dm.query, p.Query)
		dm.runners = append(dm.runners, func(events []SequencedEvent) {
			*target = p.Fold(events)
		})
	}
}

// BuildDecisionModel reads events matching all registered projections in one
// pass, folds each projection's state, and returns an AppendCondition whose
// Query is the union of all projection queries and whose After is the highest
// sequence number seen.
func BuildDecisionModel(store EventStore, opts ...Option) (*AppendCondition, error) {
	var dm decisionModel
	for _, opt := range opts {
		opt(&dm)
	}

	events, err := store.Read(dm.query, 0)
	if err != nil {
		return nil, err
	}

	for _, run := range dm.runners {
		run(events)
	}

	var maxSeq uint64
	for _, e := range events {
		if e.SequenceNumber > maxSeq {
			maxSeq = e.SequenceNumber
		}
	}

	return &AppendCondition{
		Query: dm.query,
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
