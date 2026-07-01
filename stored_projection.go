package eventstore

import (
	"context"
	"encoding/json"
	"fmt"
)

// SnapshotStore persists projection snapshots (serialized state) and stream
// positions, keyed by a projection name. It is deliberately untyped — it moves
// only []byte — so a single StoredProjection can use it for any state type and
// any storage engine can implement it (postgres, in-memory, etc.) without
// generics. See the memory and postgres packages for implementations.
type SnapshotStore interface {
	// Load returns the persisted state and position for name. found is false
	// when no snapshot exists yet, in which case the projection starts from its
	// initial state.
	Load(ctx context.Context, name string) (state []byte, position uint64, found bool, err error)
	// Save persists state and position for name, overwriting any previous value.
	Save(ctx context.Context, name string, state []byte, position uint64) error
	// Delete removes the snapshot and position for name.
	Delete(ctx context.Context, name string) error
}

// StoredProjection is a CheckpointProjection whose state and position are
// persisted through a SnapshotStore. It keeps the full state in memory (like
// DefaultProjection) and writes a snapshot on every applied event, so it can
// resume from its last checkpoint instead of replaying the whole stream.
//
// The state type S is marshaled to and from JSON here — in the constructor and
// Apply — so the SnapshotStore only ever sees the resulting bytes.
type StoredProjection[S any] struct {
	DefaultProjection[S]
	store    SnapshotStore
	name     string
	position uint64
}

// Compile-time check that StoredProjection implements CheckpointProjection.
var _ CheckpointProjection = (*StoredProjection[any])(nil)

// NewStoredProjection creates a StoredProjection backed by store under name. If
// a snapshot exists, its state and position are restored; otherwise the
// projection starts from def's initial state at position 0.
func NewStoredProjection[S any](ctx context.Context, store SnapshotStore, name string, def DefaultProjection[S]) (*StoredProjection[S], error) {
	p := &StoredProjection[S]{
		DefaultProjection: def,
		store:             store,
		name:              name,
	}
	state, position, found, err := store.Load(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("could not load projection %q: %w", name, err)
	}
	if found {
		p.position = position
		if len(state) > 0 {
			if err := json.Unmarshal(state, &p.State); err != nil {
				return nil, fmt.Errorf("could not unmarshal projection %q state: %w", name, err)
			}
		}
	}
	return p, nil
}

// After returns the sequence number of the last applied event.
func (p *StoredProjection[S]) After() uint64 {
	return p.position
}

// Apply applies a single event to the in-memory state, advances the position,
// and persists the new snapshot through the store.
func (p *StoredProjection[S]) Apply(ctx context.Context, e SequencedEvent) error {
	if err := p.DefaultProjection.Apply(ctx, e); err != nil {
		return fmt.Errorf("could not apply %s event: %w", e.Type, err)
	}
	p.position = e.SequenceNumber
	state, err := json.Marshal(p.State)
	if err != nil {
		return fmt.Errorf("could not marshal projection %q state: %w", p.name, err)
	}
	if err := p.store.Save(ctx, p.name, state, p.position); err != nil {
		return fmt.Errorf("could not save projection %q: %w", p.name, err)
	}
	return nil
}

// Reset deletes the persisted snapshot and resets the in-memory state and
// position to their zero values.
func (p *StoredProjection[S]) Reset(ctx context.Context) error {
	if err := p.store.Delete(ctx, p.name); err != nil {
		return fmt.Errorf("could not reset projection %q: %w", p.name, err)
	}
	var zero S
	p.State = zero
	p.position = 0
	return nil
}
