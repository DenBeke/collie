package dcb

import (
	"context"
	"iter"
	"sync"
	"time"
)

// MemoryStore is a minimal in-memory EventStore.
type MemoryStore struct {
	mu     sync.RWMutex
	events []SequencedEvent
}

func (s *MemoryStore) Read(_ context.Context, query Query, after uint64) iter.Seq2[SequencedEvent, error] {
	return func(yield func(SequencedEvent, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, e := range s.events {
			if e.SequenceNumber > after && query.Matches(e) {
				if !yield(e, nil) {
					return
				}
			}
		}
	}
}

func (s *MemoryStore) Append(_ context.Context, events []Event, condition *AppendCondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if condition != nil {
		for _, e := range s.events {
			if e.SequenceNumber > condition.After && condition.Query.Matches(e) {
				return ErrConditionViolated
			}
		}
	}
	for _, e := range events {
		s.events = append(s.events, SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
			CreatedAt:      time.Now(),
		})
	}
	return nil
}
