# appendbench

A load tester for the postgres event store's append paths, selected with `-lock`:

- **`-lock=global`** (default) — `postgres.Store.Append`: one **global advisory lock**
  (`pg_advisory_xact_lock`, same key for every append), so every append serializes.
- **`-lock=staging`** — `postgres.Store.AppendStaging` + a singleton `RunFlusher`: appends
  land concurrently in a per-boundary-locked staging buffer and a single flusher drains them
  into `events` (consistent, concurrent, eventual read-visibility). Tune the flusher with
  `-flush-interval` and `-flush-batch`.

The two other strategies benchmarked during the exploration (per-boundary and hybrid locks)
were removed from the library when consolidating on staging; see `appendbench-conclusion.md`
for the full four-way comparison and why staging was chosen.

## The idea it measures

Under `-lock=global` **every append serializes regardless of whether tags overlap**.
So the suite separates two things that are easy to conflate:

- **Lock-bound throughput** ≈ `1 / (mean critical-section duration)` — the advisory-lock
  acquire + condition `COUNT` + inserts + commit. Adding users past a few cannot raise it;
  it just makes each `Append` wait longer behind the lock.
- **Conflict / retry rate** — driven by *overlap* (the X:Y ratio and the distribution),
  surfacing as `ErrAppendConditionViolated`. This is wasted work that eats into the
  serialized capacity.

A run reports both, plus latency percentiles, so you can see the ceiling and the waste.

## Build & run

```sh
go build -o /tmp/appendbench ./cmd/appendbench
```

Point it at a **persistent** Postgres (recommended — seed once, reuse across many runs):

```sh
/tmp/appendbench -dsn='postgres://user:pass@localhost:5432/db?sslmode=disable' \
  -seed=1000000 -seed-only            # one-time seed
/tmp/appendbench -dsn=... -users=16 -partitions=1000 -dist=zipf -duration=30s -csv=runs.csv
```

Or omit `-dsn` to spin a throwaway `postgres:17` **testcontainer** (handy for smoke tests;
it co-locates client and DB on one host, so numbers are inflated — prefer an external DSN
for real measurements). Needs Docker; with Colima:

```sh
export DOCKER_HOST=unix://${HOME}/.colima/default/docker.sock
export TESTCONTAINERS_RYUK_DISABLED=true
```

`appendbench -csv-header > runs.csv` prints the column header (no DB needed); subsequent
runs with `-csv=runs.csv` append one row each.

## Key flags

| Axis | Flags |
|---|---|
| Strategy | `-lock global\|staging`, and for staging `-flush-interval`, `-flush-batch` |
| Concurrency X | `-users` (1 = serial lock-bound baseline) |
| Partitions Y | `-partitions` |
| Overlap | `-dist uniform\|zipf`, `-zipf-s`, `-zipf-v` |
| DB size N | `-seed` (`-fast-seed` to bypass Append for large N), `-seed-only` |
| Indexing | `-indexed-tags` (B-tree on the entity tag vs GIN-only) |
| Condition | `-condition none\|head\|create` |
| Conflict handling | `-loop open\|closed`, `-max-retries`, `-retry-backoff` |
| Window | `-duration` (default) or `-ops` + `-count-mode`, `-warmup` |
| Pool | `-max-open-conns` (auto = users+4; **must be ≥ users**) |
| Latency | `-latency-ms` (Toxiproxy container, +ms per round-trip; omit `-dsn`), `-latency-jitter-ms`, `-toxiproxy-image` |
| Output | `-csv`, `-csv-header`, `-json`, `-label`, `-quiet`, `-rng-seed` |

Condition modes:

- `none` — `cond=nil`: lock + insert only, no `COUNT`, no conflicts. The pure locked-insert ceiling.
- `head` — read the entity's head, append with `After=head` (realistic OCC). With `-loop=closed`
  it retries on violation. **Note:** with the B-tree index the `COUNT` here is a ~0-row range
  scan, roughly O(1) in N — so this path is largely insensitive to total DB size.
- `create` — `After=0`: "entity must not exist." The `COUNT` scans the entity's whole history,
  so this (and `-indexed-tags=false`, the GIN `@>` path) is where DB size N moves the cost.

## Recommended sweeps

```sh
# 1) Lock ceiling vs users (low overlap): throughput should plateau near the users=1 value.
for u in 1 2 4 8 16 32 64; do
  appendbench -dsn=$DSN -condition=none -users=$u -partitions=10000 -duration=20s -csv=runs.csv -label=ceiling
done

# 2) Conflict rate vs overlap (single-shot, no retry amplification).
for p in 32 100 1000 10000; do for d in uniform zipf; do
  appendbench -dsn=$DSN -condition=head -loop=open -users=32 -partitions=$p -dist=$d -duration=20s -csv=runs.csv -label=overlap
done; done

# 3) Retry amplification: same, closed loop — watch retries_mean / retries_max.
for p in 32 100 1000; do
  appendbench -dsn=$DSN -condition=head -loop=closed -users=32 -partitions=$p -dist=zipf -duration=20s -csv=runs.csv -label=retries
done

# 4) DB-size sensitivity (COUNT cost inside the lock): use create / GIN-only.
for n in 1000 100000 1000000; do
  appendbench -dsn=$DSN -seed=$n -fast-seed -seed-only
  appendbench -dsn=$DSN -condition=create -users=16 -partitions=1000 -duration=20s -csv=runs.csv -label=size
done

# 5) Connection-latency sensitivity (ephemeral, no -dsn): each round-trip under the lock pays it.
for l in 0 1 5 10 25; do
  appendbench -latency-ms=$l -condition=head -users=8 -partitions=1000 -duration=20s -csv=runs.csv -label=latency
done
```

Anchor any throughput comparison with a real `-users=1` row: that is the true serial,
lock-bound ceiling.

## Notes

- **Seeding via `Append` is slow** (real per-transaction global lock; ~hundreds–low-thousands
  of events/s). It reaches ~1M–10M rows in tolerable time; **1B is not reachable this way**.
  Use a persistent `-dsn` so the seed is a one-time cost, or `-fast-seed` (COPY/generate_series,
  clearly logged as *bypassing* Append) to populate large N for `COUNT`-cost runs.
- **Pool sizing matters:** `-max-open-conns` must be ≥ `-users`, otherwise surplus workers block
  acquiring a client connection instead of queueing on the DB advisory lock — and you'd measure
  the wrong thing. The default (`users+4`) handles this.
- **Connection latency** (`-latency-ms`): starts an ephemeral Postgres **and** a Toxiproxy container
  on a shared Docker network and routes the connection through Toxiproxy with a downstream `latency`
  toxic, so every DB round-trip pays the delay. Because the locked `Append` transaction makes several
  round-trips (BEGIN, advisory-lock, COUNT, INSERT, COMMIT), the critical section grows by roughly
  that multiple — e.g. 10ms/round-trip turned an ~8ms append into ~80ms in local testing, so the lock
  is held far longer and throughput collapses. Recorded in the `injected_latency_ms` CSV column. This
  mode manages its own Postgres, so omit `-dsn`. Requires Docker (it pulls the Toxiproxy image).
- `internal/loadgen` (partition picker + latency histogram) has unit tests that need no Docker:
  `go test ./cmd/appendbench/internal/...`.
