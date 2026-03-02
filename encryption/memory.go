package encryption

import (
	"context"
	"crypto/rand"
	"sync"
)

type memoryKey struct {
	tags map[string]string
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

func (s *MemoryKeyStore) GetOrCreate(_ context.Context, tags map[string]string) ([]byte, error) {
	id := tagID(tags)
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.keys[id]; ok {
		return entry.key, nil
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	s.keys[id] = memoryKey{tags: copyTags(tags), key: key}
	return key, nil
}

func (s *MemoryKeyStore) Get(_ context.Context, tags map[string]string) ([]byte, bool, error) {
	id := tagID(tags)
	s.mu.RLock()
	defer s.mu.RUnlock()
	entry, ok := s.keys[id]
	if !ok {
		return nil, false, nil
	}
	return entry.key, true, nil
}

func (s *MemoryKeyStore) Forget(_ context.Context, tags map[string]string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.keys {
		if containsAll(entry.tags, tags) {
			delete(s.keys, id)
		}
	}
	return nil
}

// containsAll returns true if superset contains every key-value pair in subset.
func containsAll(superset, subset map[string]string) bool {
	for k, v := range subset {
		if superset[k] != v {
			return false
		}
	}
	return true
}

func copyTags(tags map[string]string) map[string]string {
	cp := make(map[string]string, len(tags))
	for k, v := range tags {
		cp[k] = v
	}
	return cp
}
