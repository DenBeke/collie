package memory

import (
	"context"
	"sync"

	"eventstore"
)

// Compile-time check that SnapshotStore implements eventstore.SnapshotStore.
var _ eventstore.SnapshotStore = (*SnapshotStore)(nil)

// SnapshotStore is an in-memory [eventstore.SnapshotStore]. State is kept in a
// map and lost when the process exits — useful for tests and for
// [eventstore.StoredProjection]s that only need within-process checkpointing.
type SnapshotStore struct {
	mu        sync.RWMutex
	snapshots map[string]snapshot
}

type snapshot struct {
	state    []byte
	position uint64
}

// NewSnapshotStore creates an empty in-memory snapshot store.
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{snapshots: make(map[string]snapshot)}
}

// Load returns the stored state and position for name.
func (store *SnapshotStore) Load(_ context.Context, name string) (state []byte, position uint64, found bool, err error) {
	store.mu.RLock()
	defer store.mu.RUnlock()
	entry, ok := store.snapshots[name]
	if !ok {
		return nil, 0, false, nil
	}
	return entry.state, entry.position, true, nil
}

// Save stores state and position for name, copying state so later mutations by
// the caller do not alias the stored snapshot.
func (store *SnapshotStore) Save(_ context.Context, name string, state []byte, position uint64) error {
	stateCopy := make([]byte, len(state))
	copy(stateCopy, state)
	store.mu.Lock()
	defer store.mu.Unlock()
	store.snapshots[name] = snapshot{state: stateCopy, position: position}
	return nil
}

// Delete removes the snapshot for name.
func (store *SnapshotStore) Delete(_ context.Context, name string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	delete(store.snapshots, name)
	return nil
}
