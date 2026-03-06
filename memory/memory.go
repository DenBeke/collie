// Package memory provides an in-memory event store implementation.
package memory

import (
	"context"
	"fmt"
	"iter"
	"sync"
	"time"

	"eventstore"
	"eventstore/encryption"
)

// Store is a minimal in-memory event store.
type Store struct {
	mu        sync.RWMutex
	events    []eventstore.SequencedEvent
	encryptor *encryption.EventEncryptor
}

// Option configures a Store.
type Option func(*Store)

// WithEncryption enables GDPR crypto-shredding. Events with EncryptionKeys
// will have their Data encrypted at rest.
func WithEncryption(encryptor *encryption.EventEncryptor) Option {
	return func(s *Store) { s.encryptor = encryptor }
}

// New creates an in-memory store.
func New(opts ...Option) *Store {
	s := &Store{}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Store) Read(ctx context.Context, query eventstore.Query, after uint64) iter.Seq2[eventstore.SequencedEvent, error] {
	return func(yield func(eventstore.SequencedEvent, error) bool) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		for _, e := range s.events {
			if e.SequenceNumber > after && query.Matches(e) {
				if s.encryptor != nil {
					var err error
					e, err = s.encryptor.Open(ctx, e)
					if err != nil {
						if !yield(e, err) {
							return
						}
						continue
					}
				}
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
	for _, e := range events {
		if s.encryptor != nil {
			var err error
			e, err = s.encryptor.Seal(ctx, e)
			if err != nil {
				return fmt.Errorf("could not seal event: %w", err)
			}
		}
		s.events = append(s.events, eventstore.SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
			CreatedAt:      time.Now(),
		})
	}
	return nil
}

// Forget deletes encryption keys for the given subject tags, making
// the corresponding event data permanently unreadable.
func (s *Store) Forget(ctx context.Context, tags map[string]string) error {
	if s.encryptor == nil {
		return fmt.Errorf("no encryptor configured")
	}
	return s.encryptor.Forget(ctx, tags)
}
