// Package memory provides an in-memory event store implementation.
package memory

import (
	"context"
	"iter"
	"sync"
	"time"

	"eventstore"
)

// Store is a minimal in-memory event store.
type Store struct {
	mu     sync.RWMutex
	events []eventstore.SequencedEvent
}

func (s *Store) Read(_ context.Context, query eventstore.Query, after uint64) iter.Seq2[eventstore.SequencedEvent, error] {
	return func(yield func(eventstore.SequencedEvent, error) bool) {
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

func (s *Store) Append(_ context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if condition != nil {
		for _, e := range s.events {
			if e.SequenceNumber > condition.After && condition.Query.Matches(e) {
				return eventstore.ErrAppendConditionViolated
			}
		}
	}
	for _, e := range events {
		s.events = append(s.events, eventstore.SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
			CreatedAt:      time.Now(),
		})
	}
	return nil
}
