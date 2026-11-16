# Framework design review — findings, decisions, and change list

Outcome of a full design review of the library (core, `command`, `memory`, `postgres`,
projections, examples), with the goal of making this a clean, generic framework for quickly
building DCB event sourcing applications. This is a living document: settled decisions first,
then the change list to build from, then the detailed reasoning behind each item.

Second pass (July 2026): the earlier decisions were re-challenged with fresh eyes and held up;
the pass added the read-model view subsystem design (the "next step" work), the append-strategy
constructor mode, and a handful of new findings. Everything is consolidated here.

## Settled design decisions

These were discussed and are now the framework's explicit stance:

1. **The read side is eventually consistent, full stop.** `Append` returns nothing (per the DCB
   spec: `append(events, condition?): void`), and the staging append path assigns positions at
   flush time, so read-your-writes tokens are structurally impossible. On-demand catch-up
   (`UpdateProjection` before serving a query) gives freshness only when the same instance
   serves the read and the non-staging path is used. Document this; don't fight it.
   Reaffirmed in the second pass: `Read` serves only the `events` table and never looks at
   staging — the staging condition check is what keeps *decisions* correct in the gap.
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
   mapping. No HTTP adapter ships with the core (a ready-made adapter *package* may come later,
   beside the core).
5. **Row projections are read-side views and must be excluded from commands** — enforced in the
   type system (marker method), not just documented. The decision-side answer to "tagged state
   in commands" is the partitioned decision snapshot (below), not row projections.
6. **Multi-instance deployments are supported without ownership/leader election.**
   Correctness on the write side comes from the append condition; snapshots are caches.
   View updates become safe via checkpoint guards (below), so any instance may update any view.
7. **Snapshots are caches, never sources of truth.** Racing writers can regress a snapshot's
   position; that is benign because state+position are saved atomically and the next catch-up
   re-applies the delta. Worth a README section — it's a genuinely nice property of the design.
8. **Package boundary: `collie` is the DCB spec kernel; frameworks live beside it.** The
   core holds what the specification defines (store, events, queries, projections, decision
   model) and privileges no command style or caching strategy. `command` is the command
   framework (`Command`, `Execute` and its options, `DecisionError`/`ErrCommandRejected`,
   `Registry`). `snapshot` is the projection-caching framework (`snapshot.Store` — the seam
   from snapshot-store.md — and `snapshot.Projection` with `snapshot.NewProjection`).
   Consequences: all command-outcome errors sit in one package (only the spec-level
   `ErrAppendConditionViolated` stays core); `command.Command` stutters — accepted, with
   `context.Context` as precedent; a typical command file imports all three packages.
9. **Staging is the committed append strategy, selected at store construction.** The
   staging design (per-boundary locks + singleton flusher assigning gap-free positions) stays
   on its own merits. But it must be reachable through `collie.Store` — today
   `command.Execute` and `BuildDecisionModel` call `Append`, and `AppendStaging` is a sibling
   method the framework can't see. Fix: **constructor mode** — `postgres.New(db)` keeps the
   simple direct append (no flusher needed; right for small apps, dev, tests), and
   `postgres.New(db, WithStagingAppend())` makes `Store.Append` *be* the staging path. One
   store instance = one strategy; the unsafe mixing of the two paths (a direct append's
   condition check never looks at staging, and the global and per-boundary locks don't
   compose) becomes unrepresentable instead of a doc warning. In staging mode,
   `ErrUnkeyableCondition` is promoted from "use the other method" to a real constraint of the
   mode: every condition item needs at least one type and one tag — which real DCB queries
   always have. (Nov 2026: built the other way round — staging is the default and
   `WithGlobalLock()` the option; see item 21.)
10. **Read-model views are a generic row store.** Key → JSON value with `Get`/`List`/`Count`,
    evolving today's `RowProjection`. Clients wanting custom SQL schemas hand-roll their own
    consumers; the framework does not provide an app-owned-tables mode.
11. **Views are portable via a storage seam** (`view.Store`, parallel to `snapshot.Store`):
    memory implementation for tests, postgres implementation for production. The new view code
    is gorm-free from the start (`database/sql` in the public API).
12. **View catch-up is pluggable, one contract, two shipped drivers.** The contract is the
    existing `CheckpointProjection` lifecycle driven by `collie.UpdateProjection`. Driver A:
    on-demand pull (call `UpdateProjection` before serving a read). Driver B: a background
    runner woken by Postgres LISTEN/NOTIFY with a polling fallback (and a `Tick` waker for any
    store). See the view subsystem section.
13. **Views stay in the `CheckpointProjection` family, guarded by the marker method.** A
    standalone view type outside the projection interfaces was designed and considered (safety
    by subtraction: it *can't* be passed to `BuildDecisionModel`); decided against — views keep
    riding `UpdateProjection` as the one catch-up primitive, and decision-eligibility is
    expressed by the marker (settled decision 5 / item 5 as originally written).
14. **Poison events halt the projection, loudly.** An `Apply` error stops catch-up with an
    error naming the event type and sequence number; the checkpoint parks just before the
    poison event; reads keep serving stale-but-internally-consistent data. Silently skipping is
    corruption nobody notices; dead-lettering needs an inspect/replay surface that isn't earned
    yet. Recovery = fix the handler, rebuild under a versioned name.
15. **Rebuild/versioning is a naming convention, for views exactly as for snapshots.**
    `"user-profiles/v2"` is an empty view at position 0; blue-green = build v2 alongside v1,
    flip reads when v2's position catches v1's, then `Reset` v1. Recipe in docs; no
    orchestration API. (Nov 2026: there is still no `head()` primitive, but `Read` now returns
    the head, and checkpoints move with it even when nothing matched — see item 20.)

## Change list

Ordered by priority. Items 5–8 formed one coherent piece of work (the projection-model split);
items 1, 5, 7, 8 and 14 are now consolidated into the **view subsystem** design below.

### Correctness (do first)

1. **Checkpoint guard in `RowProjection.Apply`.** Two instances catching up the same view
   concurrently currently double-apply events (both load row at state v, both apply event 101,
   both write). Also: `p.position` is assigned inside the transaction closure — on transaction
   failure the in-memory position stays advanced and the instance skips that event until
   restart (`postgres/row_projection.go:95`).
   → **Subsumed by the view subsystem**: `view.Store.Commit` is a CAS on the checkpoint base
   (see below), and the in-memory position advances only after a successful commit. Both
   failure modes become impossible by construction; `RowProjection` is deleted.
2. **Clone-per-attempt in `Execute`.** ✅ Implemented. The `Projections()` contract ("must
   return FRESH projections each call") was a silent-corruption footgun: a command that caches
   its projection in a field re-applies events onto already-populated state on retry (a counter
   doubles, nothing errors). Fixed without touching the `Command` interface:
   `command.commandForAttempt` runs *every* attempt (including the first) on a fresh shallow
   copy of the caller's command, which is never mutated — consistent with fire-and-forget
   (settled decision 2). Stashed pointers keep working because `Projections` and `Decide` run
   on the same copy; caching a projection in a field is now the blessed pattern (docs updated).
   Non-pointer-to-struct commands run as-is (a value receiver can't retain state between calls
   anyway). Documented caveat: the copy is *shallow* — map/slice/pointer fields are shared
   across attempts, so commands must not mutate shared reference fields in
   `Projections`/`Decide` (deliberate cross-attempt state goes behind a pointer field, as
   `conflictOnce` in the tests demonstrates). The optional `dcbtest` retry-safety helper was
   skipped; regression coverage lives in `TestExecute_RetryDecidesFromFreshState` and
   `TestExecute_CallerCommandStaysPristine`.
3. **Concurrency rule for stored projections.** Two concurrent `UpdateProjection` calls on the
   same instance are a literal Go data race on `State`/`position` (the `ExampleServe` GET
   handler teaches this pattern). For the new `view.View`, an internal mutex serializes
   catch-ups on one instance (a long-lived server-shared object gets a mutex, not a documented
   rule). For `snapshot.Projection` used as a long-lived read model, the same treatment or the
   one-goroutine rule still applies — and fix the example.
4. **`NewTags` panics on odd argument count.** ✅ Implemented. Silently dropping a trailing
   tag silently widens a consistency boundary. Programmer error → panic, like
   `strings.NewReplacer` (the direct stdlib precedent — variadic pairs, panics on odd count;
   `slog`'s tolerate-and-mark `!BADKEY` stance was considered and rejected: a log line is
   disposable, a consistency boundary is not). Error returns were rejected because they detect
   at the same moment (first execution) while taxing every correct call site and being
   swallowable (`tags, _ :=`). Removing the constructor in favor of compile-safe
   `[]Tag{{k, v}}` literals was considered and rejected: the dominant call shape is a single
   pair, where the arity bug is nearly impossible and the constructor reads best.

### Interface shape (breaking; cheapest while the library is young)

5. **Marker method separating decision projections from views.** ✅ Design settled.
   `RowProjection` structurally satisfies `CheckpointProjection`, so nothing stops it being
   passed to `BuildDecisionModel` — and then the append condition covers the projection's
   entire broad query: any concurrent event touching any row violates the condition,
   serializing the whole app through one boundary. Go's structural typing can't distinguish
   identical method sets, so:

   ```go
   // collie: marks projections whose query is narrow enough for a consistency boundary.
   type DecisionProjection interface {
       Projection
       DecisionModel() // marker; no behavior
   }
   ```

   `BuildDecisionModel(ctx, store, projections ...DecisionProjection)`;
   `command.Command.Projections` returns `[]collie.DecisionProjection`.
   `DefaultProjection[S]` and `snapshot.Projection[S]` gain the no-op marker; `view.View[V]`
   implements `CheckpointProjection` but **not** the marker — passing a view to a decision is a
   compile error, while views keep riding `UpdateProjection` (settled decision 13).
6. **Per-key decision snapshots.** ✅ Implemented, after several design rounds (see the
   partition decision record below), as one variadic parameter — no dedicated type:

   ```go
   snapshot.NewProjection[S](ctx, store snapshot.Store, name string,
       def collie.DefaultProjection[S], partition ...Tag) (*snapshot.Projection[S], error)
   ```

   Partition tags are AND-ed into the query and the snapshot name is canonically derived
   (sorted + escaped → order-insensitive, collision-free) with a per-partition checkpoint;
   zero tags = the fixed-name singleton. Versioning is a naming convention
   (`"course-seats/v2"`), not API. Includes the catch-up `Checkpoint` lifecycle (one snapshot
   write per catch-up — see that decision record) and `DefaultProjection` deriving
   `Query.Types` from the `Handlers` keys when unset, which kills the
   add-a-handler-forget-the-query-type footgun. Example: `snapshot/example_test.go`
   (course subscriptions, three differently-scoped boundaries).
7. **Multi-entity events in `RowHandler`.** `Key(e) (string, error)` assumes one entity per
   event; a transfer-between-accounts event (the reason `Tag` allows duplicate keys) can't
   update both rows. → **Folded into the view subsystem**: `view.Handler.Keys(e) ([]string,
   error)`, with `Apply` receiving the key it is being applied for.
8. **`RowProjection.List` returns key+value pairs**, not bare `[]V`. Also resolve the
   split-brain filtering: `WithMatchTag` filters on the *last event's* tags (upserted
   last-writer-wins, duplicate tag keys dropped by `tagsToMap`) while `WithOrderBy` reads the
   JSON *value* — two data sources with different staleness. → **Folded into the view
   subsystem**: `List` returns `[]Entry[V]{Key, Value}`, rows store **no tags column** at all,
   and all filtering/ordering is on value fields, which the handler fully controls.

### Registry layer

9. **Error taxonomy (transport-neutral).** ✅ Implemented. Everything classifies with plain
   `errors.Is`, demonstrated in `ExampleRegistry_errorTaxonomy`:
   - `command.ErrUnknownCommand` — no factory registered (→ 404)
   - `command.ErrInvalidPayload` — JSON decode failure (→ 400)
   - `command.ErrValidationFailed` — decoded but failed `Validate()` (→ 422)
   - `command.ErrCommandRejected` — domain rejection from `Decide` (→ 409/422, app's
     choice). Mechanism: `Execute` wraps the `Decide` error in `command.DecisionError`,
     whose `Error()` returns the domain message byte-identical, whose `Unwrap` keeps
     application error types reachable via `errors.Is/As`, and whose `Is` method *claims* the
     `ErrCommandRejected` sentinel — the sentinel is a pure identity token that never appears
     in any chain or message.
   - `collie.ErrAppendConditionViolated` — conflict, retries exhausted (→ 409/503)
   - anything else — infrastructure (→ 500)
10. **Validation.** ✅ Implemented, two layers, both wrapping `ErrValidationFailed` with the
    cause chained (so e.g. `validator.ValidationErrors` stays `errors.As`-reachable for
    per-field 422 bodies):
    - `command.WithValidator(func(any) error)` — registry-wide, wired once, runs on every
      decoded command. The seam for declarative libraries (go-playground/validator etc.);
      the core stays dependency-free and never blesses a specific library.
    - `command.Validator` (`Validate() error`) — per-command, for semantic/cross-field rules
      the declarative layer can't express. Runs after the registry-wide check.
    Deliberately wire-boundary only — commands constructed in code and run through `Execute`
    directly are not validated. Validation must stay stateless: a rule that needs the event
    store is a decision and belongs in `Decide` (guarded by the append condition), not in
    validation. JSON Schema was considered and rejected as a validation source of truth (the
    Go struct is the command definition; parallel schemas drift) — generating schemas *from*
    registered commands for API docs remains a possible later feature.
11. **Dispatch middleware**: `func(ctx, name, payload, next) error` for auth, logging, metrics,
    tracing. `ExecuteOption`s can't express cross-cutting dispatch concerns.

### Hygiene

12. **Un-leak gorm.** Every public postgres constructor takes `*gorm.DB` (`New`,
    `NewRowProjection`, `NewSnapshotStore`, `NewKeyStore`), making gorm part of the public API
    contract — a gorm v3 or a switch to pgx breaks every user's wiring. Take `*sql.DB` (or a
    DSN) and open gorm internally; gorm becomes a swappable implementation detail.
    Status: the **new** view store and listener are gorm-free from day one (settled decision
    11); migrating the existing constructors (`New`, `NewSnapshotStore`, `NewKeyStore`) is
    deferred to a later sweep.
13. **Rewrite the README against the actual API.** It currently documents `map[string]string`
    tags, `Projection[S]`, `CheckpointedProjector`, and `memory.NewProjection` /
    `postgres.NewProjection` — none of which exist. Add explicit design sections: eventual
    consistency stance, snapshots-are-caches, decision projections vs views, the staging
    append mode and its operational notes (item 16).
14. **SQL hardening in the row projection.** `whereClause` and `WithOrderBy` splice key names
    into SQL via `Sprintf` (`postgres/list_option.go`, `row_projection.go:170`) — the moment an
    HTTP handler wires `?sort=` to `WithOrderBy`, that's injection. `MigrateIndexes` breaks on
    projection names containing dashes and mentions a `Count` that doesn't exist.
    → **Subsumed by the view subsystem**: the injection dies structurally (field names are bind
    parameters, never identifiers — see the seam design), `MigrateIndexes` whitelists field
    names and hashes index names, and `Count` becomes real.

### New findings (July 2026 re-challenge)

15. **The committed append path was unreachable from the framework.** Resolved by settled
    decision 9 (constructor mode). Implementation: `postgres.WithStagingAppend()`; `Append`
    dispatches; `AppendStaging` stops being public API (or stays as the documented internal).
16. **The flusher is load-bearing infrastructure in staging mode.** If it dies, appends still
    succeed (durable in staging) but nothing ever becomes visible to `Read`. Consequences to
    document and design in: supervision/restart guidance for `RunFlusher`; the flusher is the
    natural single emitter of LISTEN/NOTIFY (positions exist only after flush — see the view
    subsystem); and **same-boundary command latency couples to flush cadence** — command B on
    the boundary of just-staged command A fails its condition check (the staging row matches)
    until the flush lands, so `Execute` retry/backoff defaults should be documented relative to
    the flush interval (a non-issue at the default 5ms; a real one if operators raise it).
17. **Poison events permanently stall projections.** `UpdateProjection`: an `Apply` error
    returns before `Checkpoint`, so buffered progress is lost and the next catch-up re-reads
    from the old checkpoint and hits the same event, forever. In a decision boundary, a poison
    event bricks the boundary until a code fix ships. Policy settled (decision 14): halt
    loudly. The view design's sub-batch commits additionally preserve pre-poison progress.
18. **`postgres.Store.Read` can loop on an undecodable row.** On a `toSequencedEvent` decode
    error the cursor does not advance (`postgres/postgres.go:147–159`); if the *last* event of
    a batch fails decoding, an error-tolerant consumer refetches the same batch and re-yields
    the same error forever. Advance the cursor past undecodable rows — the yielded error
    already identifies the event.
19. **`Projection.EventQuery()` returning a single `QueryItem` blocks OR-views** (e.g. current
    + legacy event shapes in one view — promoted from the findings-for-the-record section,
    since views now live in the projection family). Recommended, tiny and mechanical: widen to
    `EventQuery() Query`; `BuildDecisionModel` changes `append(query, p.EventQuery())` to
    `append(query, p.EventQuery()...)`; `DefaultProjection` returns a one-item `Query`. Do it
    inside the view sweep or explicitly accept single-item views.

20. **`Read` returns the head (Nov 2026).** ✅ Implemented. A checkpoint used to be the last
    *matching* event, so a projection on a rare type stayed at 42 while the log was at 10⁹, and
    two things paid for the gap on every catch-up: a multi-type read whose planner walks the
    primary key through every non-matching event since 42 (measured: 3M rows and ~930 MB per
    poll on 5M events, against 0.02 ms from the head), and the append condition, which scans
    every event carrying the boundary's tags since `After` — inside the global append lock.
    Cold storage would make it structural: a checkpoint in the cold tier makes every catch-up
    and every condition check touch it. Now `Read(ctx, query, after)` returns
    `(events, head, err)`: head is the last sequence number when `Read` was called, valid once
    the events are drained completely and without error. `UpdateProjection` and
    `BuildDecisionModel` pass `max(head, last seen)` to `Checkpoint(ctx, upTo)` and use it as
    the condition's `After`; `readOffset`'s `maxAfter` workaround is gone. Considered: a
    separate `Head()` (rejected — callers must call it *before* the read and reason about
    commit order themselves), a result object (rejected — a plain return value is enough when
    the drivers own the rule), the head error deferred into the iterator (rejected — a caller
    that does not iterate, like the reactor learning where to start, would read 0 and replay
    every effect). Snapshot and Postgres keyed projections save an otherwise unchanged
    checkpoint only once `upTo` is 10,000 past it, against write amplification on a busy log.
    The guarantee rests on sequence order being commit order (one writer to `events`), which
    makes the constructor mode of item 15 more urgent.

21. **Staging is the default append mode, recorded in the database (Nov 2026).** ✅ Implemented.
    `postgres.New(db)` appends through staging and starts its own flusher (leader-elected
    across processes, standbys wait; `Close` stops it, and a stopping leader drains staging
    first); `WithoutFlusher()` leaves it out, `WithGlobalLock()` is the direct mode.
    `AppendStaging` and `Flush` are no longer public. Why staging by default: one writer to
    `events` by construction, concurrency across boundaries, and the last blocker — conditions
    without a tag or type — turned out to be fixable where it arose (a type next to a tag-only
    query; a constant tag on every event of a kind that is one boundary as a whole). Kept from
    the alternatives: `Append` returns once staged rather than waiting for the flush, since the
    flush runs every 5 ms when idle. The mode is recorded in the database (`dcb_append_mode()`);
    `Migrate` records it the first time and refuses the other mode afterwards, installing a
    refusing body for the other mode's functions, so a misconfigured process fails at boot and
    one still running in the old mode during a switch fails on its next append instead of
    becoming a second writer. `Migrate` never changes the recorded mode; `SwitchAppendMode` does,
    and going to the global lock needs an empty `staging`. Not covered: a binary from before the
    mode existed, which installs its own functions at `Migrate`. Found on the way: `RunFlusher`
    released its leadership by `conn.Close()`, which returns a pooled connection with its session
    — and the session lock — still alive, so after a flusher stopped nobody could lead again
    until that connection happened to close; the connection is now discarded.

## The view subsystem (consolidates items 1, 5, 7, 8, 14)

New top-level package `view`, parallel to `snapshot`: `snapshot` is the decision-side cache,
`view` is the read side. `postgres/row_projection.go` and `postgres/list_option.go` are deleted.

### Storage seam — untyped, closed query descriptor, no SQL crosses it

```go
package view

type Row struct{ Key string; Value []byte }
type Op struct{ Key string; Value []byte } // nil Value = delete

// Filter matches rows whose top-level JSON field equals any of Values
// (one value = equality, several = IN; text comparison, Postgres ->> semantics).
// Filters combine with AND.
type Filter struct{ Field string; Values []string }

type ListQuery struct {
    Filters []Filter
    OrderBy string // top-level JSON field; "" = order by key
    Desc    bool
    Numeric bool   // order numerically instead of lexicographically
    Offset  int
    Limit   int    // 0 = no limit
}

type Store interface {
    Position(ctx context.Context, view string) (uint64, error)
    Get(ctx context.Context, view, key string) (value []byte, found bool, err error)
    List(ctx context.Context, view string, q ListQuery) ([]Row, error)
    Count(ctx context.Context, view string, filters []Filter) (int64, error)
    // Commit atomically applies one catch-up batch: it advances the checkpoint
    // from base to position and applies ops in one transaction. If the stored
    // checkpoint no longer equals base (another instance advanced it), Commit
    // writes NOTHING and returns false.
    Commit(ctx context.Context, view string, base, position uint64, ops []Op) (bool, error)
    Reset(ctx context.Context, view string) error
}
```

- **Injection dies structurally**: the postgres implementation binds JSON field names as
  parameters (`value->>$1 = $2`, `ORDER BY value->>$1` — `->>` takes a text operand, so no
  splicing is ever needed); `ASC`/`DESC` and `::numeric` come from two-value enums. Index DDL
  is the only place a name must enter SQL as an identifier, so it lives on the postgres type,
  not the seam: `MigrateIndexes(ctx, view, fields...)` validates fields against
  `^[A-Za-z_][A-Za-z0-9_]*$` and names indexes from a hash of (view, field) — fixing the
  dashes-in-names bug too.
- **CAS on the base position, not `position < new`**: during a catch-up the handler reads
  current row values as it goes; if another instance commits mid-batch, the buffered fold was
  built on mixed row states and committing it would double-apply. `position = base` invalidates
  the batch wholesale — correctness by abandonment. (`position < new` would let it through.)
- Tables: `view_rows(view_name, key, value jsonb, PRIMARY KEY(view_name, key))` and
  `view_checkpoints(view_name PK, position)`. **No tags column** (item 8).
- Implementations: `postgres.NewViewStore(db *sql.DB)` — no gorm — and `memory.ViewStore`
  (map-of-maps behind a mutex; `List` compares text forms, documented to match `->>`).

### View type and handler

```go
package view

type Handler[V any] interface {
    // Keys returns the row keys affected by e — several for multi-entity
    // events (a transfer touches both accounts).                    (item 7)
    Keys(e collie.SequencedEvent) ([]string, error)
    // Apply transforms the row for one key. current is nil when the row does
    // not exist; return nil to delete it.
    Apply(e collie.SequencedEvent, key string, current *V) (*V, error)
}

func New[V any](store Store, name string, query collie.Query, handler Handler[V],
    opts ...Option) *View[V]
func FlushEvery(n int) Option // ops per Commit during catch-up; default 1000

type Entry[V any] struct{ Key string; Value V }

// View[V] implements collie.CheckpointProjection — but NOT DecisionProjection
// (item 5), so it cannot enter BuildDecisionModel. Plus the read API:
//   Get(ctx, key) (*V, error)                       // nil = absent
//   List(ctx, ...ListOption) ([]Entry[V], error)    // item 8: keys included
//   Count(ctx, ...ListOption) (int64, error)
//   Position(ctx) (uint64, error)
//   Reset(ctx) error
// ListOptions: Where(field, values...), OrderBy(field, desc),
//              OrderByNumeric(field, desc), Page(offset, limit).
```

Lifecycle, mapped onto the settled `Checkpoint` batch-close contract:

- `After()` returns the base loaded fresh from `Store.Position` at the start of each catch-up;
  an internal mutex serializes concurrent catch-ups on one instance (item 3).
- `Apply` skips events ≤ base, buffers ops **read-through** (first touch of a key loads the
  row; later events in the batch hit the buffer), and every `FlushEvery` ops commits a guarded
  sub-batch — bounded memory on cold rebuilds, one transaction per ~1000 events instead of per
  event (also fixes the rebuild-runs-one-tx-per-event finding).
- `Checkpoint(ctx)` does the final `Commit`. **On CAS failure** (another instance advanced the
  checkpoint): drop the buffer, reload base, continue — subsequent `Apply` calls skip covered
  events and re-read rows fresh beyond it. Self-healing, no error surfaced; under
  many-instances-pulling-on-demand, contention silently dedupes work.
- The in-memory position advances only after a successful `Commit` — the
  `row_projection.go:95` failure mode (position advanced despite a rolled-back transaction)
  becomes impossible.
- Poison event: `Apply` returns the error; committed sub-batches persist; every instance halts
  deterministically at the same event (settled decision 14).

### Catch-up drivers — one contract, two drivers

- **Pull** (unchanged core pattern): `collie.UpdateProjection(ctx, store, v)` before
  `v.Get`/`v.List`.
- **Runner**:

  ```go
  // Waker blocks until new events may be available. Wakes may be spurious;
  // the runner treats every wake as "check now".
  type Waker interface{ Wait(ctx context.Context) error }
  func Tick(interval time.Duration) Waker // polling; works with any store

  type RunOptions struct {
      // OnError is invoked when a view's catch-up fails; Run continues and
      // retries that view on the next wake. Nil (default) makes Run return.
      OnError func(view string, err error)
  }
  // Run catches up views on every wake until ctx is cancelled. It runs on the
  // caller's goroutine (house style: postgres.Store.RunFlusher).
  func Run(ctx context.Context, events collie.Store, w Waker, opts RunOptions,
      views ...collie.CheckpointProjection) error
  ```

- **Wake seam implementations**: `postgres.NewListener(dsn, ...) *Listener` owns one dedicated
  connection (LISTEN is incompatible with pooled `*sql.DB` connections, hence DSN), executes
  `LISTEN dcb_events`, and `Wait` blocks with a deadline equal to the poll-fallback interval
  (default ~30s) — lost notifications and dropped connections degrade to polling; reconnects
  with backoff inside `Wait`; `Close()` releases the connection. `memory.Store.Waker()` returns
  a 1-buffered channel poked non-blockingly on append. The subsystem itself owns **zero
  goroutines**; the caller owns one per `Run`.
- **The NOTIFY contract lives in SQL, not Go**: `pg_notify('dcb_events', '')` at the tail of
  `dcb_append` (transactional NOTIFY fires at commit, exactly when events become readable) and
  in `dcb_flush` when rows moved (the flusher is where staged events become visible —
  `dcb_append_staging` deliberately does *not* notify). Empty payload = pure wake; max-sequence
  or type hints can be added later without breaking anyone.

### Migration from `RowProjection`

Breaking, one sweep: add `view` + memory/postgres implementations + `pg_notify` (re-run
`Migrate`; `CREATE OR REPLACE` is idempotent); delete `RowProjection`, `RowHandler`, the list
options, old `MigrateIndexes`, the `projectionRow` model (checkpoint/snapshot tables stay for
`snapshot`). No data migration tool — views are derived state: construct under the new store
and replay. Port callers: `Key` → `Keys` (one-element slice), `Apply(e, *V)` →
`Apply(e, key, *V)`, `WithMatchTag(tag)` → put the field in `V` and `Where(field, ...)`.
Rewrite `ExampleServe`'s GET handler and the README section.

### Deliberately not built (recorded)

- **Selective NOTIFY** (type/tag hints in the payload): wake-everything is fine at current
  scale; the payload is reserved.
- **Per-view-goroutine runner**: views update sequentially within one `Run`; split only if one
  slow view ever starves others.
- **Jittered wake delay** against N-instance read amplification (N instances read the delta,
  one wins the CAS): fine for small N; jitter — not leader election — is the mitigation if it
  ever hurts.
- **Skip-and-log poison policy**: a per-view `OnApplyError` option is the future escape hatch;
  don't ship it now.

## The projection model — why the split, and the partitioned-snapshot design

### Two concepts, one interface (the original problem)

- A **decision projection** answers one question inside one consistency boundary. Narrow query
  (scoped by tags), small state, checkpoint **per boundary**.
- A **view projection** serves read queries. Broad query (all `ProfileCreated`, no tag scope),
  keyed per entity, **one global checkpoint**, list/filter API.

`RowProjection` is the second thing. Using it in a decision model is quietly catastrophic
(boundary = the whole projection), hence item 5. But the observation that "almost all DCB
examples carry a tag" is correct — decisions constantly need *per-entity* state. The
`ReserveSeat` example already hand-rolled exactly that: a snapshot projection named
`"seats-" + room` is a row projection where key = room, **except each "row" carries its own
checkpoint** — which is precisely what makes it safe for decisions. That pattern is now
first-class: the `partition ...Tag` parameter of `snapshot.NewProjection`.

### Partition key = tag set, not a single tag

The canonical course-subscription command needs three differently-scoped pieces of state:

```go
func (cmd *SubscribeToCourse) Projections(ctx context.Context) ([]collie.Projection, error) {
    // Partition key {course: c1} — one snapshot per course
    capacity, _ := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "course-capacity",
        capacityDef, collie.NewTag("course", cmd.CourseID))

    // Partition key {student: s1} — one snapshot per student
    count, _ := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "student-subscriptions",
        countDef, collie.NewTag("student", cmd.StudentID))

    // Partition key {student: s1, course: c1} — one snapshot per pair
    already, _ := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "subscription",
        existsDef, collie.NewTag("student", cmd.StudentID), collie.NewTag("course", cmd.CourseID))

    return []collie.Projection{capacity, count, already}, nil
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
- **Versioning**: a snapshot is only valid for the handler logic that built it. Convention,
  not API: put a version in the name (`"course-seats/v2"`) so changing handlers invalidates
  old snapshots explicitly instead of silently reading wrong state.
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
  delta from its own checkpoint up to H, set the condition's `After = H`. `Read` now returns H
  (item 20); the `until` read option is still missing. Noted, not built.

## Decision record: partitioned snapshots — from a type to a parameter

The per-key decision snapshot went through four designs; recording them so the losers stay
lost:

1. *`PartitionedProjection[S]` type + `PartitionedOption`/`WithVersion`* — worked, but was a
   whole API surface (type, options, accessor) wrapping "a stored projection whose name came
   from tags".
2. *Definition struct + `For()` method* (`def.For(ctx, store, tags)`) — killed the inline
   re-declaration and the shared-name/divergent-handlers hazard, but `For` was a preposition
   pretending to be a verb, called on something that read like a projection.
3. *Flattened `StoredProjectionDef` + constructor* — flat literal, but still a helper struct;
   the nesting had been shifted, not removed.
4. **Chosen: no new type at all.** `DefaultProjection` stays the only definition carrier
   (explicitly desired); the existing constructor gains a variadic partition:
   `snapshot.NewProjection(ctx, store, name, def, partition ...Tag)`. Tags are arguments, not
   options, because the partition determines *identity* (which snapshot you get), and options
   are for behavior. Canonical name encoding became an internal concern. Versioning is a
   naming convention (`"name/v2"`), not a knob. Two lasting extras rescued from rounds 2–3:
   `Query.Types` derives from `Handlers` keys when unset (drift footgun eliminated, and safe —
   an unhandled type cannot change state, so it cannot change a decision; narrowing only
   removes false conflicts), and the machinery moved to its own `snapshot` package per settled
   decision 8, renaming `StoredProjection` → `snapshot.Projection` and `SnapshotStore` →
   `snapshot.Store` along the way (which also resolved part of the naming-family confusion).

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

Second-pass postscript: the view subsystem realizes the anticipated hook — `view.View` buffers
ops and commits guarded sub-batches every `FlushEvery` events plus once in `Checkpoint`, so the
mid-catch-up-crash trade-off shrinks to at most one sub-batch of lost progress.

## Decision record: view type — in the projection family, not beside it

Two shapes were designed for `view.View[V]`:

1. *Standalone type with its own `Update(ctx, events)` driver, outside the `Projection`
   interfaces entirely.* Safety by subtraction — a view can't be passed to `BuildDecisionModel`
   because it doesn't satisfy the parameter type; no marker method needed; a full
   `collie.Query` (OR over items) without touching the root interface; the checkpoint can
   be re-read from the store at the start of every catch-up because `Update` owns the whole
   lifecycle.
2. **Chosen: stay a `CheckpointProjection`, guarded by the `DecisionProjection` marker
   (item 5).** Views keep riding `collie.UpdateProjection` as the single catch-up
   primitive — one lifecycle concept in the whole framework instead of two ("catch up a
   projection" vs "update a view"), and `Run` can drive any `CheckpointProjection`, snapshot
   read models included. Costs accepted: the marker interface is new API (but it was already
   item 5); OR-views need the `EventQuery() Query` widening (item 19) or stay single-item;
   fresh-base-per-catch-up must be implemented inside `After()`/`Checkpoint()` rather than
   falling naturally out of an owned `Update`.

## Decision record: append strategy — constructor mode

`Append` (global advisory lock, immediate visibility) and `AppendStaging` (per-boundary locks,
staging table, singleton flusher, gap-free positions) are both correct alone and unsafe mixed:
a direct append's condition check never looks at staging, and the global lock and per-boundary
locks share no key, so a conflict between the two paths goes unnoticed. The old shape — two
public sibling methods and a doc warning — also left the staging path unreachable from
`command.Execute`/`BuildDecisionModel`, which only see `collie.Store.Append`.

Options: (a) staging only — delete the direct path; every deployment, including tests and toy
apps, runs a flusher. (b) Keep both methods and document. (c) **Constructor mode — chosen**:
the strategy is fixed per store instance at construction (`postgres.New(db)` = direct;
`postgres.New(db, WithStagingAppend())` = staging behind `Store.Append`). The framework stays
strategy-blind, small apps keep the zero-infra path, and the unsafe mix is unrepresentable
(one instance, one strategy; two instances with different modes on one database remain a
misconfiguration to warn about in docs).

## Multi-instance semantics (summary)

| Concern | Status | Mechanism |
|---|---|---|
| Concurrent commands, same boundary | Safe today | Append condition enforced atomically by the store |
| Snapshot write races (`snapshot.Projection`/partitions) | Benign today | State+position saved atomically; regression just re-applies delta |
| Concurrent catch-up on a shared view (cross-instance) | Design settled → view subsystem | `view.Store.Commit` CAS on the checkpoint base; losing batch abandoned wholesale, loser self-heals |
| Concurrent catch-up on the same in-memory instance | Design settled → view subsystem | Mutex inside `view.View`; `snapshot.Projection` as long-lived read model still needs the same treatment (item 3) |
| Flusher liveness (staging mode) | Documented risk → item 16 | Session advisory lock elects one live flusher; supervision/restart is the operator's job |

No leader election, no projection ownership: any instance may run any command and update any
view. Flexibility was an explicit requirement.

## Other findings kept for the record

- ~~`Projection.EventQuery()` returns a single `QueryItem`~~ → promoted to item 19.
- ~~No subscription/notification mechanism~~ → designed: LISTEN/NOTIFY waker + `Tick` polling
  fallback in the view subsystem, with the NOTIFY living in `dcb_append`/`dcb_flush`.
- ~~`StoredProjection` saves a snapshot per applied event during catch-up~~ → fixed by the
  `Checkpoint` lifecycle (item 6 ✅).
- ~~Rebuilding a `RowProjection` runs one transaction per event~~ → fixed by the view design's
  `FlushEvery` sub-batch commits.
- Naming: `DefaultProjection` is a non-name, and the `CheckpointProjection` (interface) /
  `snapshot.Projection` (struct) family is confusing. A `Projector`/`Projection[S]` flip was
  considered in the second pass and **deferred** — not a passenger on the view sweep.
- `postgres.Read` cursor loop on undecodable rows → item 18.
