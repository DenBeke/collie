// Package memory provides an in-memory event store implementation.
package memory

import (
	"context"
	"iter"
	"sync"
	"time"

	"github.com/DenBeke/collie"
)

// Store is a minimal in-memory event store. Like the PostgreSQL store it only
// takes keyable conditions unless it is built WithGlobalLock, so a decision
// that passes here is not refused there. The zero value is ready to use.
type Store struct {
	mu         sync.RWMutex
	events     []eventstore.SequencedEvent
	globalLock bool
}

// Option configures a Store.
type Option func(*Store)

// WithGlobalLock makes Append take any condition, as the PostgreSQL store does
// WithGlobalLock. Without it a condition must be keyable, or Append returns
// eventstore.ErrUnkeyableCondition.
func WithGlobalLock() Option {
	return func(s *Store) { s.globalLock = true }
}

// New creates an in-memory store.
func New(opts ...Option) *Store {
	s := &Store{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Store) Read(ctx context.Context, query eventstore.Query, after uint64) (iter.Seq2[eventstore.SequencedEvent, error], uint64, error) {
	s.mu.RLock()
	head := uint64(len(s.events))
	s.mu.RUnlock()
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
	}, head, nil
}

func (s *Store) Append(ctx context.Context, events []eventstore.Event, condition *eventstore.AppendCondition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if condition != nil {
		if !s.globalLock && !condition.Keyable() {
			return eventstore.ErrUnkeyableCondition
		}
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
