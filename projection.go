package dcb

// HandleFunc processes a single event and returns the updated state.
type HandleFunc[S any] func(S, SequencedEvent) S

// Projection defines how to apply matching events into a state of type S.
// Query controls which events are read from the store. Handlers maps event
// types to functions that update state; unhandled types are ignored.
type Projection[S any] struct {
	Query    QueryItem
	State    S
	Handlers map[string]HandleFunc[S]
}

// EventQuery returns the projection's QueryItem.
func (p *Projection[S]) EventQuery() QueryItem { return p.Query }

// Apply applies matching events to State in place.
func (p *Projection[S]) Apply(events []SequencedEvent) {
	for _, e := range events {
		if matchesQueryItem(e, p.Query) {
			if h, ok := p.Handlers[e.Type]; ok {
				p.State = h(p.State, e)
			}
		}
	}
}
