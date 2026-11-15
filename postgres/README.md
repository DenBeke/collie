# PostgreSQL store

```go
// PostgreSQL (pgstore is "github.com/DenBeke/collie/postgres")
db, _ := gorm.Open(postgres.Open(connStr))
pgStore := pgstore.New(db, pgstore.WithBatchSize(500))
defer pgStore.Close() // stops its flusher
pgStore.Migrate()
```

## Append modes: staging (default) and the global lock

By default the PostgreSQL store appends through **staging**: an append locks only the boundaries
its condition and events name, checks the condition against both `staging` and `events`, and lands
in the `staging` table. A single flusher moves staged appends into `events` in order, so appends to
disjoint boundaries run concurrently while sequence numbers stay gap-free and in commit order.

- **The flusher comes with the store.** `New` starts one; across processes one leads and the
  others wait as standbys, so every process can simply start one. `Close` stops it, and a leader
  that stops first moves what is still staged. `WithoutFlusher()` leaves it out (a read-only tool,
  or a test that flushes itself); `RunFlusher` runs one yourself.
- **Readable within milliseconds.** `Append` returns once the events are staged: durable, and
  already seen by every condition. `Read` sees them after the next flush — at most the flush
  interval (5 ms by default) when idle. Two decisions on the same boundary in quick succession
  can therefore see the second one's condition fail on the first one's staged row before its read
  shows it; retry with a backoff longer than the flush interval.
- **Every condition item needs a type and a tag**, or `Append` returns `ErrUnkeyableCondition`.
  To make a whole kind of event one boundary, give each of those events a constant tag
  (`directory: peppol`) and put it in the condition. The in-memory store applies the same
  rule, so a test catches it; `memory.New(memory.WithGlobalLock())` lifts it there too.

`WithGlobalLock()` is the other mode: every append serializes on one global advisory lock and is
readable as soon as `Append` returns, conditions need no tags, and there is no flusher.

**One mode per database.** `Migrate` records the mode the first time it runs and installs only that
mode's functions; a store configured for the other mode then fails `Migrate` with
`ErrAppendModeMismatch`, and its appends fail the same way, because two modes writing to one
database would each break the other's guarantees. `SwitchAppendMode` changes the recorded mode on
purpose: processes still running in the old mode get errors from then on, and switching to the
global lock needs an empty `staging` table. A binary from before the mode existed knows nothing of
it and would install its own functions at `Migrate`; keep those away from a database you switch.

The design and the benchmarks behind choosing staging (including the per-boundary and hybrid
strategies that were evaluated and rejected) are written up in
[`appendbench-conclusion.md`](../appendbench-conclusion.md), with full methodology in
[`appendbench-scaleway.md`](../appendbench-scaleway.md).

## Projection tables

Call `MigrateProjections` to create the tables used by `postgres.SnapshotStore` and `postgres.KeyedProjection[V]`:

```go
postgres.MigrateProjections(db)
```

This creates three tables: `projection_checkpoints` (shared position tracking), `projection_snapshots` (snapshot state), and `projection_entries` (the values of keyed projections), with foreign key constraints from snapshots/entries to checkpoints.

If using encryption, also migrate the key store table:

```go
postgres.MigrateKeyStore(db)
```

## Sequence order and the head

Besides the events, `Read` returns the **head**: the store's last sequence number when `Read` was called. Once the events have been iterated completely and without an error, every matching event up to the head has been seen, even when none matched. So `max(head, last event seen)` is how far a reader has checked the log, and it is what `BuildDecisionModel` and `UpdateProjection` record. A projection on a rare event type therefore keeps moving with the log instead of staying at its last matching event, and an append condition only has to check the writes that landed during the decision. After a `break` or an error, the head promises nothing. The events stay lazy, so `Read` without iterating is a cheap way to learn the head.

This relies on sequence order being commit order: the PostgreSQL store guarantees it with one writer to `events` (the single flusher in staging mode, or the global append lock), which is why a database records its append mode and refuses the other one (see above). The head and the events must also come from the same server; spreading reads over replicas with different lag would let a head from one overclaim the events of another.

## Upgrading from the `created_at` schema

`RecordedAt` is the store's only timestamp. It replaced an older application-clock `created_at` column, which `Migrate` **drops** — without backfilling, because a timestamp the application picked on its way to the database is not the moment the store accepted the event. A log migrated off that schema keeps `NULL` on its historical rows and loses the old column's data.
