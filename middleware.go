package eventstore

import (
	"context"
	"fmt"
	"iter"
)

// AppendMiddleware transforms events before they are persisted.
type AppendMiddleware func(ctx context.Context, e *Event) error

// ReadMiddleware transforms events after they are read from the store.
type ReadMiddleware func(ctx context.Context, e *SequencedEvent) error

// MiddlewareStore wraps a Store and applies middleware to events flowing
// through Append and Read.
type MiddlewareStore struct {
	inner  Store
	append []AppendMiddleware
	read   []ReadMiddleware
}

// MiddlewareOption configures a MiddlewareStore.
type MiddlewareOption func(*MiddlewareStore)

// WithAppendMiddleware registers middleware that runs on each event before
// it is persisted. Middleware runs in the order registered.
func WithAppendMiddleware(m ...AppendMiddleware) MiddlewareOption {
	return func(s *MiddlewareStore) { s.append = append(s.append, m...) }
}

// WithReadMiddleware registers middleware that runs on each event after
// it is read from the store. Middleware runs in the order registered.
func WithReadMiddleware(m ...ReadMiddleware) MiddlewareOption {
	return func(s *MiddlewareStore) { s.read = append(s.read, m...) }
}

// NewMiddlewareStore creates a Store that applies middleware around an inner Store.
func NewMiddlewareStore(store Store, opts ...MiddlewareOption) *MiddlewareStore {
	s := &MiddlewareStore{inner: store}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Append runs append middleware on each event, then delegates to the inner store.
func (s *MiddlewareStore) Append(ctx context.Context, events []Event, condition *AppendCondition) error {
	// Clone events so middleware mutations don't affect the caller's slice.
	processed := make([]Event, len(events))
	copy(processed, events)
	for i := range processed {
		for _, m := range s.append {
			if err := m(ctx, &processed[i]); err != nil {
				return fmt.Errorf("append middleware: %w", err)
			}
		}
	}
	return s.inner.Append(ctx, processed, condition)
}

func (s *MiddlewareStore) Read(ctx context.Context, query Query, after uint64) iter.Seq2[SequencedEvent, error] {
	inner := s.inner.Read(ctx, query, after)
	if len(s.read) == 0 {
		return inner
	}
	return func(yield func(SequencedEvent, error) bool) {
		for e, err := range inner {
			if err != nil {
				if !yield(e, err) {
					return
				}
				continue
			}
			for _, m := range s.read {
				if err = m(ctx, &e); err != nil {
					break
				}
			}
			if !yield(e, err) {
				return
			}
		}
	}
}
