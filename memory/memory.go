// Package memory provides an in-memory event store implementation.
package memory

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/DenBeke/collie"
)

// Store is a minimal in-memory event store.
type Store struct {
	mu     sync.RWMutex
	events []eventstore.SequencedEvent
}

// New creates an in-memory store.
func New() *Store {
	return &Store{}
}

func (s *Store) Read(ctx context.Context, query eventstore.Query, after uint64) iter.Seq2[eventstore.SequencedEvent, error] {
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

func (s *Store) Append(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if condition != nil {
		for _, e := range s.events {
			if e.SequenceNumber > condition.After && condition.Query.Matches(e) {
				return eventstore.ErrAppendConditionViolated
			}
		}
	}
	// Acceptance is now: this store has nothing between the append and the log.
	accepted := time.Now()
	for _, e := range events {
		s.events = append(s.events, eventstore.SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
			RecordedAt:     accepted,
		})
	}
	return nil
}
