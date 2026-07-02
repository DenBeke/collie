# Framework design review — findings, decisions, and change list

Outcome of a full design review of the library (core, `command`, `memory`, `postgres`,
projections, examples), with the goal of making this a clean, generic framework for quickly
building DCB event sourcing applications. This is a living document: settled decisions first,
then the change list to build from, then the detailed reasoning behind each item.

## Settled design decisions

These were discussed and are now the framework's explicit stance:

1. **The read side is eventually consistent, full stop.** `Append` returns nothing (per the DCB
   spec: `append(events, condition?): void`), and the staging append path assigns positions at
   flush time, so read-your-writes tokens are structurally impossible. On-demand catch-up
   (`UpdateProjection` before serving a query) gives freshness only when the same instance
   serves the read and the non-staging path is used. Document this; don't fight it.
2. **Commands are fire-and-forget at the framework level.** `Dispatch`/`Execute` return only
   `error` — either the command succeeded or it failed. No result values, no idempotency keys;
   retry semantics belong to the caller/application.
3. **The two-phase `Command` interface stays** (`Projections()` then `Decide()`). The
   separation of concerns and testability are valued; passing projections as a `Decide`
   parameter was considered and rejected. Instead, the retry footgun is fixed inside `Execute`
   (clone per attempt, see below).
4. **`command.Registry` is the primary entry point.** Commands arrive as name + JSON payload
   (typically over HTTP); handlers call `Dispatch`, which calls `Execute`. The registry stays
   **transport-neutral**: it exposes a typed error taxonomy and adapters do their own status
   mapping. No HTTP adapter ships with the core.
5. **Row projections are read-side views and must be excluded from commands** — enforced in the
   type system (marker method), not just documented. The decision-side answer to "tagged state
   in commands" is the partitioned decision snapshot (below), not row projections.
6. **Multi-instance deployments are supported without ownership/leader election.**
   Correctness on the write side comes from the append condition; snapshots are caches.
   View updates become safe via checkpoint guards (below), so any instance may update any view.
7. **Snapshots are caches, never sources of truth.** Racing writers can regress a snapshot's
   position; that is benign because state+position are saved atomically and the next catch-up
   re-applies the delta. Worth a README section — it's a genuinely nice property of the design.

## Change list

Ordered by priority. Items 5–8 form one coherent piece of work (the projection-model split).

### Correctness (do first)

1. **Checkpoint guard in `RowProjection.Apply`.** Two instances catching up the same view
   concurrently currently double-apply events (both load row at state v, both apply event 101,
   both write). Fix inside the existing per-event transaction: advance the checkpoint with
   `... SET position = ? WHERE projection_name = ? AND position < ?`; if zero rows were
   affected, another instance already applied this event — skip it. Also: stop assigning
   `p.position` inside the transaction closure — on transaction failure the in-memory position
   currently stays advanced and the instance skips that event until restart
   (`postgres/row_projection.go:89`).
2. **Clone-per-attempt in `Execute`.** The `Projections()` contract ("must return FRESH
   projections each call") is a silent-corruption footgun: a command that caches its projection
   in a field re-applies events onto already-populated state on retry (a counter doubles,
   nothing errors). Fix without touching the `Command` interface: keep the caller's command
   pristine and run each attempt on a fresh shallow copy (same reflection trick as
   `command.cloneFactory`). Stashed pointers keep working because `Projections` and `Decide`
   run on the same clone. Optionally add a `dcbtest` helper that forces a conflict+retry and
   fails if the second attempt's decision differs from a fresh run.
3. **Concurrency rule for `StoredProjection`.** Two concurrent `UpdateProjection` calls on the
   same instance are a literal Go data race on `State`/`position` (the `ExampleServe` GET
   handler teaches this pattern). Either add a mutex or document a strict
   one-goroutine-per-projection-instance rule — and fix the example.
4. **`NewTags` panics on odd argument count.** Silently dropping a trailing tag
   (`event.go:27`) silently widens a consistency boundary. Programmer error → panic.

### Interface shape (breaking; cheapest while the library is young)

5. **Marker method separating decision projections from views.** `RowProjection` structurally
   satisfies `CheckpointProjection`, so nothing stops it being passed to `BuildDecisionModel` —
   and then the append condition covers the projection's entire broad query: any concurrent
   event touching any row violates the condition, serializing the whole app through one
   boundary. Go's structural typing can't distinguish identical method sets, so add a marker
   (e.g. `DecisionModel()`) to decision-capable projections; `BuildDecisionModel` accepts only
   those.
6. **`PartitionedProjection` — first-class per-key decision snapshots.** ✅ Implemented
   (`partitioned_projection.go`, example in `partitioned_projection_example_test.go`; includes
   the catch-up `End` lifecycle — one snapshot write per catch-up — see the decision record
   below). Formalizes the hand-rolled `"seats-" + room` pattern (see full design below).
   Constructor:

   ```go
   NewPartitionedProjection[S](ctx, snapshots SnapshotStore, name string, partition []Tag,
       def DefaultProjection[S]) (*PartitionedProjection[S], error)
   ```

   Includes: canonical snapshot-name encoding from the tag set, per-partition checkpoints,
   a version knob for handler-logic changes, and one `Save` at the end of catch-up instead of
   per applied event (also fix this in `StoredProjection`).
7. **Multi-entity events in `RowHandler`.** `Key(e) (string, error)` assumes one entity per
   event; a transfer-between-accounts event (the reason `Tag` allows duplicate keys) can't
   update both rows. Change to `Keys(e) ([]string, error)` with `Apply` receiving the key it is
   being applied for. Interface shape can't be retrofitted cheaply — do it now.
8. **`RowProjection.List` returns key+value pairs**, not bare `[]V` — unless V redundantly
   embeds its own ID, rows are currently unidentifiable. Also resolve the split-brain filtering:
   `WithMatchTag` filters on the *last event's* tags (upserted last-writer-wins, duplicate tag
   keys dropped by `tagsToMap`) while `WithOrderBy` reads the JSON *value* — two data sources
   with different staleness. Preferred: drop event-tag storage on rows entirely and filter on
   `value` fields, which the handler fully controls.

### Registry layer

9. **Error taxonomy (transport-neutral).** `Dispatch`/`Execute` must let adapters distinguish,
   via `errors.Is`/`errors.As`:
   - `ErrUnknownCommand` — no factory registered (→ 404)
   - `ErrInvalidPayload` — JSON decode or `Validate()` failure (→ 400)
   - `DecisionError{Err}` — domain rejection from `Decide`; wraps the domain error so messages
     and `errors.As` still reach it (→ 409/422, app's choice). Today `Decide` errors pass
     through unwrapped by design — good for messages, but the transport can't tell a domain
     rejection from an infrastructure failure.
   - `ErrAppendConditionViolated` — conflict, retries exhausted (→ 409/503)
   - anything else — infrastructure (→ 500)
10. **`Validate() error` hook**, checked by `Dispatch` after decode, before `Execute`.
11. **Dispatch middleware**: `func(ctx, name, payload, next) error` for auth, logging, metrics,
    tracing. `ExecuteOption`s can't express cross-cutting dispatch concerns.

### Hygiene

12. **Un-leak gorm.** Every public postgres constructor takes `*gorm.DB` (`New`,
    `NewRowProjection`, `NewSnapshotStore`, `NewKeyStore`), making gorm part of the public API
    contract — a gorm v3 or a switch to pgx breaks every user's wiring. Take `*sql.DB` (or a
    DSN) and open gorm internally; gorm becomes a swappable implementation detail.
13. **Rewrite the README against the actual API.** It currently documents `map[string]string`
    tags, `Projection[S]`, `CheckpointedProjector`, and `memory.NewProjection` /
    `postgres.NewProjection` — none of which exist. Add explicit design sections: eventual
    consistency stance, snapshots-are-caches, decision projections vs views.
14. **SQL hardening in the row projection.** `whereClause` and `WithOrderBy` splice key names
    into SQL via `Sprintf` (`postgres/list_option.go`, `row_projection.go:170`) — the moment an
    HTTP handler wires `?sort=` to `WithOrderBy`, that's injection. Whitelist or quote.
    `MigrateIndexes` breaks on projection names containing dashes and mentions a `Count` that
    doesn't exist (add it or fix the doc).

## The projection model — why the split, and the PartitionedProjection design

### Two concepts, one interface (the original problem)

- A **decision projection** answers one question inside one consistency boundary. Narrow query
  (scoped by tags), small state, checkpoint **per boundary**.
- A **view projection** serves read queries. Broad query (all `ProfileCreated`, no tag scope),
  keyed per entity, **one global checkpoint**, list/filter API.

`RowProjection` is the second thing. Using it in a decision model is quietly catastrophic
(boundary = the whole projection), hence item 5. But the observation that "almost all DCB
examples carry a tag" is correct — decisions constantly need *per-entity* state. The
`ReserveSeat` example already hand-rolls exactly that: a `StoredProjection` named
`"seats-" + room` is a row projection where key = room, **except each "row" carries its own
checkpoint** — which is precisely what makes it safe for decisions. That pattern deserves to be
first-class: `PartitionedProjection`.

### Partition key = tag set, not a single tag

The canonical course-subscription command needs three differently-scoped pieces of state:

```go
func (cmd *SubscribeToCourse) Projections(ctx context.Context) ([]eventstore.Projection, error) {
    // Partition key {course: c1} — one snapshot per course
    capacity, _ := eventstore.NewPartitionedProjection(ctx, cmd.SnapshotStore(), "course-capacity",
        eventstore.NewTags("course", cmd.CourseID), capacityDef())

    // Partition key {student: s1} — one snapshot per student
    count, _ := eventstore.NewPartitionedProjection(ctx, cmd.SnapshotStore(), "student-subscriptions",
        eventstore.NewTags("student", cmd.StudentID), countDef())

    // Partition key {student: s1, course: c1} — one snapshot per pair
    already, _ := eventstore.NewPartitionedProjection(ctx, cmd.SnapshotStore(), "subscription",
        eventstore.NewTags("student", cmd.StudentID, "course", cmd.CourseID), existsDef())

    return []eventstore.Projection{capacity, count, already}, nil
}
```

Design points:

- **Query scoping**: `QueryItem.Tags` is already AND semantics — a multi-tag partition puts the
  whole tag set in the query. Single-tag is just the one-element case.
- **Snapshot naming**: canonical encoding of prefix + sorted tag set, e.g.
  `subscription|course=c1|student=s1`. Deterministic ordering (so `{student, course}` and
  `{course, student}` hit the same snapshot) and separator escaping (so a tag value containing
  the separator can't collide with a different tag set) are mandatory — this is exactly the
  string concatenation users would hand-roll wrong.
- **Checkpoints are per partition** — every checkpoint *value* is a position in the single
  global event stream, but each partition keeps its own. Correctness: the append condition for
  "reserve seat in R1" must pair R1's query with R1's position; a checkpoint shared across
  partitions couples unrelated boundaries. Speed: catch-up reads only events matching the
  partition's tags after its own checkpoint (tag-indexed, typically zero-to-few rows), and
  commands on different partitions never contend.
- **Versioning**: a snapshot is only valid for the handler logic that built it. Include a
  version in the snapshot name (or derive it from the projection definition) so changing
  handlers invalidates old snapshots explicitly instead of silently reading wrong state.
- **Memory profile**: a command touches one key, so it loads one partition's state (one room's
  count, one patient's allergies). The "whole projection" exists only as N independent snapshot
  rows in the store; no process ever holds them all. Memory is O(one entity) regardless of
  entity count — this is the trouble row-projections-in-commands would have caused, avoided.

### Usage guidance (goes in the docs with the feature)

- **Not every partition deserves a snapshot.** The pair-scoped `subscription` projection has a
  history of ~0–2 events; snapshotting it buys nothing and costs a Load+Save per command.
  Snapshots pay off for high-traffic boundaries (the course with 10k events). Tiny boundaries
  stay ephemeral `DefaultProjection`s; mixing both in one command works today.
- **Known limitation — read amplification when mixing cold and warm.** `readOffset` starts the
  single read at the *lowest* checkpoint across the command's projections; an ephemeral
  projection's checkpoint is 0, so mixing a warm snapshot (checkpoint 1M) with an ephemeral
  check re-reads the union query from 0. The snapshot still avoids re-*applying*, not
  re-*reading*. In practice bounded by the narrow tag queries; rarely a problem. The proper fix,
  if it ever is: bounded per-projection reads — pin the store head H, read each projection's
  delta from its own checkpoint up to H, set the condition's `After = H`. That requires `head()`
  and an `until` read option the interface deliberately lacks today. Noted, not built.

## Decision record: the catch-up `Checkpoint` lifecycle

**Problem.** `StoredProjection.Apply` used to write a snapshot per applied event. Warm
partitions don't care (delta is 0–2 events), but a cold partition replaying 10k events did 10k
useless intermediate `Save`s on the hot path, and a busy read model catching up a large delta
via `UpdateProjection` doubled its write load. Only the *end-of-batch* state matters, and only
the catch-up drivers (`BuildDecisionModel`, `UpdateProjection`) know when the batch ends.

**Options considered.**

1. *Per-event save (status quo).* Simplest; identical cost in steady state; cold replays resume
   from the crash point. Rejected for the recurring read-model catch-up cost — but it remains
   the honest fallback if the lifecycle ever feels too heavy.
2. *Flush from `Execute` / the command layer.* Wrong altitude twice: `UpdateProjection` (read
   side) drives the same projections, and `BuildDecisionModel` is public API used without
   `Execute` — each path would need its own flush or silently stop persisting. Commands
   shouldn't know persistence mechanics.
3. *Optional `Flusher` capability interface, probed by the drivers.* Idiomatic Go
   (`http.Flusher`), non-breaking — but it hides the lifecycle in an anonymous type assertion,
   and "Flush" collides with `postgres.Store.Flush` (the staging flusher).
4. *`Flush` on the `CheckpointProjection` interface.* Rejected on interface segregation:
   checkpointing and buffering are orthogonal. `RowProjection` persists per event (its row
   write + checkpoint must share a transaction), test doubles and purely in-memory checkpointed
   projections persist never — a `Flush` most implementors no-op is a lie in the name.
5. **A batch-close method on `CheckpointProjection` — chosen.** A *lifecycle hook* ("this
   catch-up batch is complete; record your progress") is universally meaningful for every
   checkpoint projection, so a no-op is a truthful answer, unlike a no-op "flush". The contract
   is explicit in the interface ("Apply per event, then close the batch exactly once"), no
   anonymous capability probe remains, and the drivers already special-case
   `CheckpointProjection` for skip logic — same named seam.

**Naming.** First `End`, renamed to **`Checkpoint`**: it names what the framework wants
("record your progress durably now"), it is inherently repeatable where `End` misreads as
terminal (`io.Closer` intuitions — but this runs once per catch-up, thousands of times over a
lifetime), it matches database vocabulary exactly (Postgres `CHECKPOINT` = write buffered dirty
state durably), and it rhymes with the interface: a `CheckpointProjection` can `Checkpoint()`,
and `After()` says where the last checkpoint stands. `Flush` was additionally ruled out because
`postgres.Store.Flush` (the staging flusher) already owns that word in this codebase.

**Consequences.** `StoredProjection` buffers in memory and persists in `Checkpoint`
(dirty-flag: no write when nothing applied). `RowProjection.Checkpoint` is a truthful no-op
today and the natural hook for batching a whole catch-up's row writes into one guarded
transaction later. Trade-offs accepted: a cold replay that crashes mid-catch-up restarts from
the last checkpoint instead of the failure point (mitigation if ever needed: a
`CheckpointEvery(K)` knob; pre-warming advice stands), and anyone driving `Apply` manually must
call `Checkpoint` — discoverable on the interface.

## Multi-instance semantics (summary)

| Concern | Status | Mechanism |
|---|---|---|
| Concurrent commands, same boundary | Safe today | Append condition enforced atomically by the store |
| Snapshot write races (`StoredProjection`/partitions) | Benign today | State+position saved atomically; regression just re-applies delta |
| Concurrent `UpdateProjection` on a shared view (cross-instance) | **Broken** → item 1 | Checkpoint guard (`position < ?`) makes event application idempotent |
| Concurrent `UpdateProjection` on the same in-memory instance | **Data race** → item 3 | Mutex or one-goroutine rule |

No leader election, no projection ownership: any instance may run any command and update any
view. Flexibility was an explicit requirement.

## Other findings kept for the record

- `Projection.EventQuery()` returns a single `QueryItem`, so one projection can't OR over
  differently-tagged items (e.g. current + legacy event shapes). Consider returning `Query`.
- No subscription/notification mechanism (polling `UpdateProjection` is the only catch-up).
  Acceptable under the eventual-consistency stance; LISTEN/NOTIFY could come later without
  interface changes.
- `StoredProjection` saves a snapshot per applied event during catch-up; a cold snapshot over a
  long history means N writes for one decision. Folded into item 6 (save once at end of
  catch-up). Pre-warming cold snapshots off the hot path (background `UpdateProjection`)
  remains good advice (see `snapshot-store.md`).
- Rebuilding a `RowProjection` runs one transaction per event; a batch/rebuild path is missing.
  Not urgent; note for when rebuilds hurt.
- Naming: `DefaultProjection` is a non-name, and the `CheckpointProjection` (interface) /
  `StoredProjection` (struct) family is confusing. Revisit when touching items 5–6 anyway.
