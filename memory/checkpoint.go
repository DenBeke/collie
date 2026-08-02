package memory

import (
	"context"
	"sync"

	"github.com/DenBeke/collie/reactor"
)

// Compile-time check that CheckpointStore implements reactor.CheckpointStore.
var _ reactor.CheckpointStore = (*CheckpointStore)(nil)

// CheckpointStore is an in-memory [reactor.CheckpointStore]. Positions die with
// the process, restarting every reactor at the head of the log: fine for tests,
// never where effects leave the machine.
type CheckpointStore struct {
	mu          sync.Mutex
	checkpoints map[string]uint64
}

// NewCheckpointStore creates an empty in-memory reactor checkpoint store.
func NewCheckpointStore() *CheckpointStore {
	return &CheckpointStore{checkpoints: make(map[string]uint64)}
}

// Load returns the position stored for name.
func (store *CheckpointStore) Load(_ context.Context, name string) (position uint64, found bool, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	position, found = store.checkpoints[name]
	return position, found, nil
}

// Save stores position for name.
func (store *CheckpointStore) Save(_ context.Context, name string, position uint64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.checkpoints[name] = position
	return nil
}
