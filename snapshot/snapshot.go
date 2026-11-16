// Package snapshot caches projection state so decisions and read models resume
// from a checkpoint instead of replaying their whole event stream. It is an
// optimization layer over the collie core, not part of the DCB
// specification: a [Projection] is an [collie.CheckpointProjection] whose
// state and position persist through a [Store], and it drops unchanged into
// collie.BuildDecisionModel and collie.UpdateProjection.
package snapshot

import "context"

// Store persists projection snapshots (serialized state) and stream positions,
// keyed by a projection name. It is deliberately untyped — it moves only
// []byte — so a single Projection can use it for any state type and any
// storage engine can implement it (postgres, in-memory, etc.) without
// generics. See the memory and postgres packages for implementations.
type Store interface {
	// Load returns the persisted state and position for name. found is false
	// when no snapshot exists yet, in which case the projection starts from its
	// initial state.
	Load(ctx context.Context, name string) (state []byte, position uint64, found bool, err error)
	// Save persists state and position for name, overwriting any previous value.
	Save(ctx context.Context, name string, state []byte, position uint64) error
	// Delete removes the snapshot and position for name.
	Delete(ctx context.Context, name string) error
}
