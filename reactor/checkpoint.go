package reactor

import "context"

// CheckpointStore persists how far each reactor got, keyed by registered name.
//
// Deliberately not the snapshot store: a wiped snapshot is rebuilt from the
// log, but a checkpoint remembers effects that fired outside it — losing one
// re-fires them, advancing one skips reactions. Treat it like the event log,
// not like a cache.
type CheckpointStore interface {
	// Load returns the position a reactor reached. found is false when the name
	// has no checkpoint yet, which is how the runner spots a reactor's first run.
	Load(ctx context.Context, name string) (position uint64, found bool, err error)
	// Save records that everything up to and including position has been handled,
	// overwriting any previous value for name.
	Save(ctx context.Context, name string, position uint64) error
}
