# Pluggable projection persistence — `SnapshotStore`

The seam that lets **checkpointed/cached projections** (used to avoid replaying a long-lived
entity's whole history on every decision) be backed by *any* store — postgres today, Mongo or a
gorm-free driver tomorrow — **without rewriting a single command/handler**.

Pairs with the command framework (see `Command`/`Execute`/`Registry`): a command's `Projections()`
may return a `CheckpointProjection`, and that projection's persistence is the swappable thing here.

## The problem it solves

Checkpoint projections cache a projection's state + position so a decision reads only the *delta*
since the snapshot instead of replaying years of events. But the existing checkpoint projection is
welded to gorm/postgres — a command that uses one imports `gorm` + `postgres` + the concrete
constructor. Switch to Mongo, or drop gorm, and **every** such handler has to be rewritten. Leak.

The fix is **not** to add a second cache beside the postgres one (that duplicates its snapshot
logic), but to **extract that persistence behind an interface** so a *single* checkpoint projection
can use any backend.

## The seam — untyped on purpose

```go
// core (eventstore): persistence only — serialized state + a position, keyed by name.
type SnapshotStore interface {
    Load(ctx context.Context, name string) (state []byte, position uint64, found bool, err error)
    Save(ctx context.Context, name string, state []byte, position uint64) error
}
```

It is deliberately **untyped** (`[]byte`), which sidesteps Go's generics wall: an interface *method*
can't be generic, but this one doesn't need to be — it only moves bytes.

## One generic checkpoint projection over the store

```go
// core: typed state; persistence delegated to the SnapshotStore.
type CheckpointProjection[S any] struct {
    DefaultProjection[S]
    store    SnapshotStore
    name     string
    position uint64
}

// Generic CONSTRUCTOR (functions CAN be generic — this is the bridge):
// loads the snapshot via store.Load and unmarshals it into S.
func NewCheckpointProjection[S any](ctx context.Context, store SnapshotStore, name string, def DefaultProjection[S]) (*CheckpointProjection[S], error)

// Apply applies the event to the in-memory .State, then store.Save(name, marshal(State), position).
// After() returns position.
```

The typing lives in the generic constructor and `Apply` (which know `S` and do the
marshal/unmarshal); the `SnapshotStore` only ever sees `[]byte`. So you keep typed `.State` with
**zero generics on the interface**. `CheckpointProjection[S]` still satisfies the `Projection` /
`CheckpointProjection` interfaces, so it drops into `BuildDecisionModel` / `Execute` unchanged.

## Backends become just stores

This **replaces and de-duplicates** today's `postgres.CheckpointProjection` and
`memory.CheckpointProjection`, which currently copy the same catch-up logic and differ only in how
they persist:

```go
postgres.NewSnapshotStore(db)        // the gorm snapshot/checkpoint tables you already have, extracted
memory.NewSnapshotStore()            // in-memory map
mongo.NewSnapshotStore(collection)   // tomorrow — write this one type, nothing else changes
```

## Commands stay backend-agnostic

```go
type PrescribeDrug struct {
    PatientID string `json:"patient_id"`
    store     eventstore.SnapshotStore                  // ← interface. No gorm, no postgres import.
    allergies *eventstore.CheckpointProjection[AllergyState]
}
func (c *PrescribeDrug) UseSnapshotStore(s eventstore.SnapshotStore) { c.store = s } // injected by the Registry

func (c *PrescribeDrug) Projections(ctx context.Context) []eventstore.Projection {
    c.allergies, _ = eventstore.NewCheckpointProjection[AllergyState](
        ctx, c.store, "patient-allergies-"+c.PatientID, allergyDef())
    return []eventstore.Projection{c.allergies}
}
func (c *PrescribeDrug) Decide(context.Context) ([]eventstore.Event, error) {
    if c.allergies.State.Contains(c.Drug) { return nil, ErrAllergy } // typed state
    ...
}
```

The command depends only on **core** types (`SnapshotStore`, `CheckpointProjection`,
`NewCheckpointProjection`). The Registry injects whichever store you wired:

```go
reg := command.NewRegistry(store,
    command.WithSnapshotStore(postgres.NewSnapshotStore(db)), // ← swap this one line for Mongo
    command.WithCommands(&PrescribeDrug{}),
)
```

**Swapping a backend costs exactly: write one `SnapshotStore` impl, change one injection line. Zero
handler rewrites.** That is the entire point of the seam.

## Cost & notes

- **It's a projection-layer refactor:** move `CheckpointProjection` into core over `SnapshotStore`,
  and turn `postgres`/`memory`'s checkpoint types into `SnapshotStore` impls. Upside: it removes the
  current memory-vs-postgres duplication. `Projections()` gains a `ctx` (it may now do I/O to load).
- **Pre-warm cold snapshots.** The first decision for an entity with no snapshot still replays its
  full history (and `Save`s per event). Keep that off the request hot path with a background
  `UpdateProjection` that warms snapshots.
- **`Save` is per-`Apply`.** A decision's catch-up writes once per delta event — cheap in steady
  state, and it advances the snapshot so the *next* decision/retry catches up even less. Correct even
  if the append then loses the race (those events are committed; the snapshot should reflect them).

## Status

Design (not built). A prerequisite for backend-pluggable *cached* commands; the rest of the command
framework (`Command`/`Execute`/`Registry`) doesn't depend on it.
