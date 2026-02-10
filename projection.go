package dcb

// HandleFunc processes a single event and returns the updated state.
type HandleFunc[S any] func(S, SequencedEvent) S

// Projection defines how to fold matching events into a state of type S.
// Query controls which events are read from the store. Handlers maps event
// types to functions that update state; unhandled types are ignored.
type Projection[S any] struct {
	Query    QueryItem
	Initial  S
	Handlers map[string]HandleFunc[S]
}

// Fold applies matching events to Initial and returns the final state.
func (p Projection[S]) Fold(events []SequencedEvent) S {
	state := p.Initial
	for _, e := range events {
		if matchesQueryItem(e, p.Query) {
			if h, ok := p.Handlers[e.Type]; ok {
				state = h(state, e)
			}
		}
	}
	return state
}
