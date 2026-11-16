# Checkpoint and snapshot projections

## Stored decision models

For large event streams, replaying every matching event on each command becomes slow. You can pre-load projection state at a known checkpoint and implement `CheckpointProjection` so that `BuildDecisionModel` only replays events after the checkpoint:

```go
type CheckpointProjection interface {
    Projection
    After() uint64
    Checkpoint(ctx context.Context, upTo uint64) error
}
```

For example, a stored projection that loads its state from a cache or database:

```go
type storedProjection[S any] struct {
    collie.DefaultProjection[S]
    after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }

func (s *storedProjection[S]) Checkpoint(_ context.Context, upTo uint64) error {
    s.after = max(s.after, upTo)
    return nil
}
```

The caller only changes how the projection is constructed — `BuildDecisionModel`, the business rule check, and `Append` stay the same. When multiple projections are passed, the read starts from the lowest checkpoint and each projection skips events it has already processed.

## CheckpointProjection

A `CheckpointProjection` is a long-lived projection that tracks its position in the event stream. It catches up incrementally on demand via `UpdateProjection`, making it suitable for serving read queries.

It is a `Projection` that also reports and records its position:

```go
type CheckpointProjection interface {
    Projection          // EventQuery() QueryItem; Apply(context.Context, SequencedEvent) error
    After() uint64
    Checkpoint(ctx context.Context, upTo uint64) error
}
```

`Checkpoint` closes a catch-up: every matching event up to `upTo` has been applied, so the projection records `max(After(), upTo)`. It is only called after a complete, error-free catch-up. The provided implementations save an otherwise unchanged checkpoint only once `upTo` is 10,000 past it, so a busy log does not turn every catch-up into a write.

Two implementations are provided:

| Implementation | Storage | Use case |
|----------------|---------|----------|
| `snapshot.Projection[S]` | A `snapshot.Store`: `memory.NewSnapshotStore()` or `postgres.NewSnapshotStore(db)` | One folded state that resumes from its last checkpoint instead of replaying the stream |
| `postgres.KeyedProjection[V]` / `memory.KeyedProjection[V]` | One value per key | The read side: state you address by key and list, and state too large to fold per command |

## Snapshot projections


`snapshot.Projection[S]` wraps a `DefaultProjection[S]` with position tracking and persists state and position through a `snapshot.Store`. Define the projection once:

```go
projection := collie.DefaultProjection[int]{
    Query: collie.QueryItem{Types: []string{"UserSignedUp"}},
    State: 0,
    Handlers: map[string]collie.HandleFunc[int]{
        "UserSignedUp": func(count int, _ collie.SequencedEvent) (int, error) {
            return count + 1, nil
        },
    },
}
```

Then pick where the snapshot lives, in memory (lost on restart) or Postgres (call `postgres.MigrateProjections(db)` once). The constructor loads an existing snapshot if there is one:

```go
snapshots := memory.NewSnapshotStore()        // or postgres.NewSnapshotStore(db)
totalUsers, err := snapshot.NewProjection(ctx, snapshots, "total_users/v1", projection)
```

Update and read the state the same way:

```go
collie.UpdateProjection(ctx, store, totalUsers)
fmt.Println(totalUsers.State)
```

Two things the name does. Version it (`/v1`) and bump it when the handlers change, so an old snapshot is rebuilt rather than read as wrong state. And pass partition tags to scope one projection definition to one boundary; each partition then keeps its own snapshot and checkpoint:

```go
seats, err := snapshot.NewProjection(ctx, snapshots, "course_seats/v1", seatsDelta, collie.NewTag("course", courseID))
```

A snapshot projection is also what `BuildDecisionModel` uses to skip already-folded events in a decision, so the same definition serves a command and a read endpoint.
