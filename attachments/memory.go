package attachments

import (
	"context"
	"fmt"
	"sync"

	"crypto/rand"
	"encoding/hex"
)

// MemoryBlobStore is an in-memory BlobStore for testing.
type MemoryBlobStore struct {
	mu    sync.RWMutex
	blobs map[string][]byte
}

// NewMemoryBlobStore creates an empty in-memory blob store.
func NewMemoryBlobStore() *MemoryBlobStore {
	return &MemoryBlobStore{blobs: make(map[string][]byte)}
}

func (s *MemoryBlobStore) Put(_ context.Context, data []byte) (string, error) {
	id, err := randomID()
	if err != nil {
		return "", err
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	s.mu.Lock()
	s.blobs[id] = dataCopy
	s.mu.Unlock()
	return id, nil
}

func (s *MemoryBlobStore) Get(_ context.Context, id string) ([]byte, error) {
	s.mu.RLock()
	data, ok := s.blobs[id]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("blob %q not found", id)
	}
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, nil
}

// Delete removes a blob by ID. This is not part of the BlobStore interface
// but is useful for testing missing-blob scenarios.
func (s *MemoryBlobStore) Delete(id string) {
	s.mu.Lock()
	delete(s.blobs, id)
	s.mu.Unlock()
}

func randomID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
