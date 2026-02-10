package dcb

import "errors"

// MemoryStore is a minimal in-memory EventStore.
type MemoryStore struct {
	events []SequencedEvent
}

func (s *MemoryStore) Read(query Query, after uint64) ([]SequencedEvent, error) {
	var result []SequencedEvent
	for _, e := range s.events {
		if e.SequenceNumber > after && matchesQuery(e, query) {
			result = append(result, e)
		}
	}
	return result, nil
}

func (s *MemoryStore) Append(events []Event, condition *AppendCondition) error {
	if condition != nil {
		for _, e := range s.events {
			if e.SequenceNumber > condition.After && matchesQuery(e, condition.Query) {
				return errors.New("append condition violated")
			}
		}
	}
	for _, e := range events {
		s.events = append(s.events, SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
		})
	}
	return nil
}

func matchesQuery(e SequencedEvent, query Query) bool {
	if len(query) == 0 {
		return true
	}
	for _, item := range query {
		if matchesQueryItem(e, item) {
			return true
		}
	}
	return false
}
