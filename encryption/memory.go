package encryption

import (
	"context"
	"crypto/rand"
	"slices"
	"sync"

	"github.com/DenBeke/collie"
)

type memoryKey struct {
	tags []eventstore.Tag
	key  []byte
}

// MemoryKeyStore is an in-memory KeyStore for testing and development.
type MemoryKeyStore struct {
	mu   sync.RWMutex
	keys map[string]memoryKey
}

// NewMemoryKeyStore creates an empty in-memory key store.
func NewMemoryKeyStore() *MemoryKeyStore {
	return &MemoryKeyStore{keys: make(map[string]memoryKey)}
}

// GetOrCreate returns the key for the subject, generating a new one if needed.
func (s *MemoryKeyStore) GetOrCreate(_ context.Context, subject []eventstore.Tag) ([]byte, error) {
	id := TagID(subject)
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.keys[id]; ok {
		return entry.key, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	s.keys[id] = memoryKey{tags: canonicalTags(subject), key: key}
	return key, nil
}

// Get returns the key for the exact subject, or (nil, false, nil) if absent.
func (s *MemoryKeyStore) Get(_ context.Context, subject []eventstore.Tag) ([]byte, bool, error) {
	id := TagID(subject)
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.keys[id]
	if !ok {
		return nil, false, nil
	}
	return entry.key, true, nil
}

// Forget deletes all keys whose subject is a superset of the provided tags.
func (s *MemoryKeyStore) Forget(_ context.Context, tags []eventstore.Tag) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.keys {
		if containsAll(entry.tags, tags) {
			delete(s.keys, id)
		}
	}
	return nil
}

// containsAll returns true if superset contains every tag in subset.
func containsAll(superset, subset []eventstore.Tag) bool {
	for _, t := range subset {
		if !slices.Contains(superset, t) {
			return false
		}
	}
	return true
}
