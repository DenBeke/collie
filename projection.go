package eventstore

import "fmt"

// HandleFunc processes a single event and returns the updated state.
type HandleFunc[S any] func(S, SequencedEvent) (S, error)

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

// Apply applies a single event to State.
func (p *Projection[S]) Apply(e SequencedEvent) error {
	if h, ok := p.Handlers[e.Type]; ok {
		s, err := h(p.State, e)
		if err != nil {
			return fmt.Errorf("could not handle %s event: %w", e.Type, err)
		}
		p.State = s
	}
	return nil
}
