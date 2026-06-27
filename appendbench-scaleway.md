# appendbench — Scaleway (dedicated hardware) results

**Environment:** Scaleway managed PostgreSQL 17.10, dedicated server (client and DB on
separate hosts), **real network RTT ≈ 11–13 ms**, **storage: Scaleway 5K block storage
(5,000 provisioned IOPS) — the SAME volume tier across every instance below**,
`max_connections=100` (later 1000). Table seeded to **10M events over 10,000 entities**.
Client: appendbench over GORM/pgx, single round-trip append via `dcb_append` /
`dcb_append_boundary` SQL functions. `successes/s` = successful appends per second.

**Why this matters:** the earlier co-located Colima VM gave the *opposite* conclusion
(boundary always wins) because resource starvation inflated the in-lock critical section,
making the global lock look like the bottleneck. On real hardware the critical section is
sub-millisecond, so the global lock is cheap — and the conclusions below flip. Reps on real
hardware are tight (~5–15%), so these are trustworthy.

## 1. Realistic DCB (`condition=head`: read-head + conditional append), 1M rows

| users | global/s | boundary/s | ratio |
|------:|---------:|-----------:|------:|
| 1  | 45  | 41  | 0.92× |
| 8  | 287 | 229 | 0.80× |
| 16 | 473 | 355 | 0.75× |
| 32 | 574 | 458 | 0.80× |
| 64 | 516 | 511 | 0.99× |

**Per-boundary is *slower* than global here.** With the read in the write path, throughput
is gated by the round-trip latency, not the lock; the lock is barely contended (cheap
index-probe check), so removing contention gains nothing while the boundary function's extra
work (parse `bigint[]`, sort, N locks) costs ~20%.

## 2. Pure lock test (`condition=none`: lock + insert, no check, no read), 1M rows

| users | global/s | boundary/s | ratio |
|------:|---------:|-----------:|------:|
| 8  | 590  | 558  | 0.95× |
| 16 | 936  | 1016 | 1.09× |
| 32 | 1215 | 1742 | 1.43× |
| 64 | 1450 | 2637 | 1.82× |

When the append rate is high enough that inserts serialize on the global lock, per-boundary
parallelizes disjoint inserts → **up to 1.8× at 64 users**. This is the write-heavy
ingestion regime where the lock *is* the bottleneck.

## 3. Overlap sweep (`condition=none`, users=64) — validates the lock model

"Overlap" = fraction of writes hitting one shared hot boundary; rest are per-worker private
boundaries. `-overlap` flag in appendbench.

| overlap | global/s | boundary/s | ratio |
|--------:|---------:|-----------:|------:|
| 0%   | 1297 | 3084 | 2.38× |
| 10%  | 1510 | 3249 | 2.15× |
| 20%  | 1412 | 3230 | 2.29× |
| 50%  | 1430 | 2399 | 1.68× |
| 100% | 1343 | 1469 | 1.09× |

- **Global is flat** (~1300–1500/s) across overlap — it always takes one lock.
- **100% overlap ⇒ boundary ≈ global (1.09×)**: all writes hit one boundary → all
  per-boundary keys are the same key → it degenerates to a global lock. (As predicted.)
- **0% overlap ⇒ 2.38×**: disjoint boundaries run in parallel. The ceiling (~3084/s, not
  ∞) is the **database's own write capacity** — "unless the DB can't handle it."

## 4. Cost decomposition (overlap=0, users=64) — the key table

| workload | global/s | boundary/s | per-boundary | includes |
|----------|---------:|-----------:|-------------:|----------|
| `none`  | 1500 | 2624 | 1.75× | lock + insert, no check |
| `check` | 1169 | 1947 | 1.67× | **+ in-lock condition check**, no read |
| `head`  | 951  | 1232 | 1.29× | + per-op read (read gates the rate) |

- **none → check:** the in-lock condition check costs ~22–25% throughput (GIN scan of the
  entity's events under the lock; append p50 28→37 ms). This is "the time to check the
  append condition in the lock."
- **check → head:** adding the per-op read drops it further — workers spend time reading
  instead of appending.

`condition=check` = head-style in-lock check but **no read** (sentinel `After` above any
sequence number, so the check scans + passes). Models offloading the decision-read to a
read replica and measuring the pure append ceiling.

## 5. Append-only with check (`condition=check`), overlap sweep, users=64

| overlap | global/s | boundary/s | ratio |
|--------:|---------:|-----------:|------:|
| 0%   | 1169 | 1947 | 1.67× |
| 20%  | 1172 | 2075 | 1.77× |
| 50%  | 1191 | 1833 | 1.54× |
| 100% | 1146 | 1205 | 1.05× |

## 6. High-concurrency scaling (overlap=0, `max_connections=1000`)

Appends/s vs concurrent users — does per-boundary keep scaling, or hit the DB wall?

| cond | lock | u64 | u128 | u256 |
|------|------|----:|-----:|-----:|
| `check` | global   | 1857 | 1858 | 1755 |
| `check` | boundary | 3214 | 3259 | 2932 |
| `none`  | global   | 2182 | 2129 | 1995 |
| `none`  | boundary | 4303 | 5750 | 5689 |

ratio (boundary/global): `none` 1.97× → 2.70× → 2.85×; `check` ~1.7× flat.

- **Global is flat vs users** — lock-bound; more writers just queue on the one lock.
- **Boundary scales to the database's write ceiling** then plateaus: ~**5,700/s** raw
  (`none`) / ~**3,200/s** with the in-lock check (`check`), reached by ~128 users. Beyond
  that the DB (WAL/CPU/IOPS) is the limit, not the lock.
- **512 users collapsed to ~135/s** — a *client-side* connection-establishment wall (512
  SSL handshakes over 11 ms RTT can't ramp within the window), not a DB limit. Past ~256
  concurrent writers you need pooling/pre-warmed connections to test further.

**appendbench bug fixed during this run:** the tool didn't `Close()` its connection pool on
exit, so rapid back-to-back high-user runs left lingering server connections that exhausted
the limit and broke subsequent runs (and even refused new psql sessions). Now closed on exit.

**On "1 billion requests":** not a runnable benchmark — at ~2,000/s that's ~6 days
(~28 h even at 10k/s). Throughput is a *rate*, measured over a short window; a billion ops
adds nothing. (1 billion *rows* seeded would be ~10 h and 100GB+, likely beyond the tier.)
The meaningful "bigger test" is concurrency scaling above, which finds the DB write wall.

## 7. Database size effect (cache pressure) — `none`/`check`, users=64, overlap=0

Tier cache ≈ 1.3 GB (`effective_cache_size`), `shared_buffers` 450 MB. The DB outgrows RAM at
~7–10M rows; past that, index probes for the in-lock check hit disk. Measured fast-seed rate
~21,500 rows/s (steady) ⇒ 1B ≈ ~13 h (overnight, feasible). DB size ≈ 175 bytes/row incl.
indexes (1B ≈ 175 GB).

Size curve (appends/s), 10k entities so per-entity history grows with row count:

| cond/lock | 2.7M (cached, 0.5GB) | 10M (1.6GB) | 13M (2.2GB) |
|-----------|---------------------:|------------:|------------:|
| none-global   | 2182 | 1466 | 1634 |
| none-boundary | 4303 | 3203 | 3422 |
| check-global  | 1857 | 1374 | 1234 |
| check-boundary| 3214 | 2350 | 2412 |

per-boundary ratio: `none` 1.97× → 2.18× → 2.09×; `check` 1.73× → 1.71× → 1.95×.

- Throughput drops **~25–30%** once the working set exceeds the 1.3 GB cache (~7–10M rows),
  then stays roughly flat (10M ≈ 13M, both fully past-cache): slower index-maintaining inserts
  + cache-missing check probes.
- **Per-boundary's advantage holds and trends up with DB size** (`none` ~2×, `check` up to
  1.95×): a bigger DB makes the in-lock check costlier → the global lock is held longer → it's
  more of a bottleneck → per-boundary parallelism matters more.
- **Implication:** for large, production-scale databases (working set > RAM), per-boundary is
  *at least as* valuable as the small-DB tests showed — likely more.

### Tier limits + a lesson (this instance)

This is a small managed tier (~1–2 GB RAM; `shared_buffers`=450MB, `work_mem`=4MB,
`maintenance_work_mem`=150MB). Findings on its limits:
- **Writes scale further than reads:** fast-seed INSERT chunks ran up to 227s and committed;
  but a `count(*)` full-scan (~3 GB cold heap) drops the connection at ~55s. So **avoid
  `count(*)`/full scans and `CREATE INDEX` on large tables here** — use `pg_class.reltuples`.
- **The seed must use few entities.** Seeding 87M rows over **1M entities** turned every insert
  into a *random* write into a 1.77 GB `(entity, seq)` B-tree (> cache) → I/O thrashing
  (33s/M → 227s/M) that destabilized the instance. Seeding over **10k entities** is sequential
  enough to stay gentle (~22–47s/M, steady). Lesson: for big seeds, either keep entity
  cardinality low, or drop secondary indexes → bulk-load → rebuild (the rebuild needs a tier
  that survives a full-scan `CREATE INDEX`).
- **Practical ceiling on this tier ≈ 13M rows** for benchmarking; 100M–1B needs a bigger
  instance (8–16 GB+ RAM). Measured fast-seed rate ~21.5k rows/s ⇒ 1B ≈ ~13 h (feasible on
  adequate hardware), not a full day.

## 8. Overlap sweep WITH latency, at 10M (`condition=check`, users=64)

Run on the restored 10M-row DB (past-cache). Shows throughput *and* append-latency vs overlap.
"Overlap" = fraction of writes to one shared hot boundary; rest are per-worker private.

| overlap | global (succ/s · p50) | boundary (succ/s · p50) | ratio |
|--------:|----------------------:|------------------------:|------:|
| 0%   | 1253 · 69 ms | 2508 · **35 ms** | **2.00×** |
| 10%  | 1252 · 71 ms | 2417 · 35 ms | 1.93× |
| 20%  | 1381 · 66 ms | 2577 · 35 ms | 1.87× |
| 50%  | 1326 · 68 ms | 2067 · 43 ms | 1.56× |
| 100% | 1267 · 70 ms | 1315 · **68 ms** | **1.04×** |

- **Global is flat in both throughput (~1250–1380/s) and latency (~66–71 ms)** across overlap —
  it always takes the one global lock, so overlap is irrelevant to it.
- **Boundary latency rises with overlap**: append p50 ~35 ms while writes are disjoint (no lock
  queueing) → 43 ms at 50% → **68 ms at 100%**, converging on global's ~70 ms. At 100% overlap
  all writes hit one boundary ⇒ all per-boundary keys collapse to one ⇒ it *is* a global lock
  (ratio 1.04×, latencies equal).
- **At 0% overlap, per-boundary = 2× throughput at *half* the latency** (35 vs 69 ms).
- Confirms the §3/§5 overlap model at 10M (past-cache) with the realistic in-lock check and the
  real ~11 ms RTT included. (Reps were tight; the restored DB benchmarks cleanly.)

Per-op latency anatomy (real ~11 ms RTT to Scaleway): `readHead` = 1 round-trip ≈ 18 ms p50;
`append` = 1 round-trip + lock + in-lock check + insert ≈ 22–35 ms (low overlap/users) rising
with contention. A full `head` command ≈ read + append ≈ 40 ms.

## 9. Instance-tier comparison — and a commit-fsync lesson

Same 10M DB, same sweep (`check`, users=64, overlap sweep), different Scaleway instances.
**Counter-intuitive result: the bigger Cost-Optimized instance was SLOWER in absolute terms.**

global · boundary (ratio):

| overlap | 1c/2GB cost-opt | 4c/8GB cost-opt | 2c/8GB **GP** |
|--------:|----------------:|----------------:|--------------:|
| 0%   | 1253 · 2508 (2.00×) | ~345 · ~1470 (**~4.3×**) | **1406 · 2567 (1.82×)** |
| 10%  | 1252 · 2417 (1.93×) | ~350 · ~1400 (~4.0×) | 1471 · 2510 (1.71×) |
| 20%  | 1381 · 2577 (1.87×) | ~345 · ~910 (~2.6×) | 1328 · 2039 (1.54×) |
| 50%  | 1326 · 2067 (1.56×) | ~350 · ~540 (~1.5×) | 1482 · 2105 (1.42×) |
| 100% | 1267 · 1315 (1.04×) | ~350 · ~340 (~1.0×) | 1451 · 1193 (0.82×) |

(4c/8GB measured twice, hours apart — stable, so not a warm-up artifact. GP = General-Purpose
instance type; the others were Cost-Optimized.)

**Resolution — it was commit fsync latency, and the 4c/8GB instance was an ANOMALY (not the tier).**
Config is identical across all three (`synchronous_commit=on`, `fsync=on`, `wal_sync_method=fdatasync`).
Deriving commit cost from global throughput (each fsync serializes under the global lock, so
global ≈ 1/fsync): **1c/2GB cost-opt ~0.8 ms, 2c/8GB GP ~0.8 ms, 4c/8GB cost-opt ~2.8 ms.** The
original *Cost-Optimized* box was fast — so "cost-opt = slow storage" is FALSE. Only the 4c/8GB
instance had ~3× slower fsync — and **all four instances ran on the SAME 5K block storage
(5,000 IOPS)**, same config. So it can't be the storage tier or the IOPS spec.

The cause is **below the managed-DB abstraction and not observable from Postgres** (no OS access;
`track_io_timing`/`track_wal_io_timing` off): most likely **host-level I/O contention (noisy
neighbor)** or a **transient degraded VM state** — note the 4c/8GB was resized *immediately after*
the 100M-entity thrash + crash + restore and may never have settled. *Not* the instance tier,
spec, or config. (Earlier draft wrongly attributed it to "storage IOPS tier" — corrected.)

Robust, instance-independent facts:
- The workload is **commit-fsync-bound** on network block storage — even GP doubles with
  `synchronous_commit=off` (global 1144 → 2353/s).
- GP 2c/8GB is the fastest measured (global 1406/s, boundary 2567/s); its honest per-boundary
  ratio is **~1.8×**.
- A slow commit *inflates* the boundary/global ratio (the serialized global path pays full fsync;
  the parallel boundary path amortizes via group commit) — so **a higher ratio can mean a worse/
  more-contended box, not a better one.** Always read ratio alongside absolute throughput.
- Commit latency on managed cloud DBs varies ~3× between instances for reasons you can't see or
  control — benchmark the actual target, repeatedly.

**Why: the workload is commit-fsync-bound, and the bigger cost-opt instance had a slower fsync.**
- `users=1` probe (one append, no contention): append p50 = **17.3 ms** ≈ 11 ms RTT + **~6 ms
  server**, almost all of it the commit fsync (inserts hit fully-cached indexes, sub-ms). The old
  box hit 1253/s global → its fsync was sub-millisecond, ~5–10× faster.
- Decisive test — `synchronous_commit` on vs off (4c/8GB, check, u64, ov0):
  global **353 → 880/s (+149%)**, boundary 1430 → 1941/s (+36%).
- **The serialized global path pays a full fsync per commit (no group commit) → cratered.** The
  parallel boundary path amortizes fsyncs via **group commit** → hit far less. So slow commits
  *inflate* the boundary/global ratio (2.0× → ~4.3×) while *lowering* absolute throughput.

**Lessons:**
1. A *higher* per-boundary ratio is NOT automatically better — here it reflects how badly slow
   commits punish the serialized path, not more raw speed.
2. This workload's ceiling is **commit I/O (WAL fsync)**, not CPU/RAM. Cores+RAM help reads
   (10M now fully cached in `shared_buffers`=1.9GB) but not commit-bound appends.
3. The levers that matter: **storage IOPS** (Cost-Optimized vs General-Purpose vs local NVMe) and
   **`synchronous_commit`** (off recovered +149%). Always check the volume's provisioned IOPS.

## 10. 32 vCPU / 128 GB GP — boundary scaling unleashed

Same 10M DB, `condition=check`. This host's commit fsync is again slow-ish (`users=1` append
16.6 ms; ~5.6 ms server-side), so global stays fsync+lock-bound and low; the cores let *boundary*
scale. `shared_buffers`=30.5 GB (10M trivially cached).

**Overlap sweep, users=64** (global · boundary · ratio):

| overlap | global (succ/s · p50) | boundary (succ/s · p50) | ratio |
|--------:|----------------------:|------------------------:|------:|
| 0%   | 1517 · 62 ms | 3243 · 26 ms | 2.14× |
| 10%  | 1540 · 59 ms | 3465 · 25 ms | 2.25× |
| 20%  | 1538 · 60 ms | 3287 · 26 ms | 2.14× |
| 50%  | 1547 · 58 ms | 2620 · 35 ms | 1.69× |
| 100% | 1576 · 58 ms | 1475 · 61 ms | 0.94× |

users=64 only shows ~2.1× — 64 I/O-bound (RTT+fsync) appends can't fill 32 cores. Scale concurrency:

**Concurrency sweep, overlap=0:**

| users | global (succ/s · p50) | boundary (succ/s · p50) | ratio |
|------:|----------------------:|------------------------:|------:|
| 64  | 1816 · 58 ms  | 3498 · 28 ms  | 1.93× |
| 128 | 1746 · 120 ms | 6153 · 30 ms  | 3.52× |
| 256 | 1526 · 255 ms | 5171 · 73 ms  | 3.39× |
| 512 | 1354 · 520 ms | **6918** · 93 ms | **5.11×** |
| 768 | 180 · — | 6424 · 133 ms | 35.6× (degenerate) |

- **Boundary scales to ~6,900/s** (vs the 2-core ceiling of 2,567/s) — the cores finally let
  disjoint appends run in parallel. The earlier ~2× was genuinely **core-capped**.
- **Global is flat-to-collapsing** (1816 → 1354 → 180): lock + fsync-bound, can't use cores; the
  lock queue's latency explodes (p50 58 → 520 ms) and falls apart at 768 users. The u768 **35.6×
  is NOT a real win** — global degenerated (lock-queue saturation, ops canceled). **Honest peak
  ≈ 5.11× at u512.**
- **New ceiling ≈ 6,900/s = the database write path** (WAL throughput, group-commit/fsync,
  hot-page index contention: single-value `type` index, monotonic PK/`created_at`), not the lock
  or cores.

**Net across all hardware:** per-boundary's real advantage for this workload is ~1.8–2× when
core-starved (2 cores), rising to a genuine **~5× at high concurrency on 32 cores**, with
boundary peaking ~6,900 appends/s while global plateaus ~1,500–1,800/s regardless of cores (it
can't parallelize). Absolute numbers and the ratio both swing with the host's commit-fsync
latency (the uncontrollable per-instance variance from §9).

## 11. Storage IOPS is NOT the bottleneck; per-boundary ceiling is mostly core/contention-bound

### 5K vs 15K block storage (same 32-core compute) — no change
Tripling provisioned IOPS (5K → 15K) changed nothing within run-to-run noise — overlap sweep and
concurrency sweep both ~identical, boundary ceiling **~6,300–6,900/s** either way. And the clincher:
with **`synchronous_commit=off` (fsync entirely removed)** the boundary ceiling stayed **~6,800/s**.
So boundary is **neither IOPS-bound nor fsync-bound**. With group commit, ~6,900 commits/s is only a
few hundred actual fsyncs/sec — far under even 5,000 IOPS, so storage was never the cap.

| path | bound by | evidence |
|------|----------|----------|
| global (~1,500/s) | commit-fsync **latency** (serialized, 1 fsync at a time) | `sync=off` *doubles* it (1321→2538) |
| boundary (~6,800/s) | **WAL insert + hot-page index contention** (not fsync/IOPS) | 5K=15K, and `sync=on`≈`sync=off` at the ceiling |

Hot pages = single-value `type` index (all `"BenchAppended"` → one leaf), monotonic PK
`sequence_number` and `created_at` (→ rightmost leaf). To raise it: drop those indexes, partition,
batch more events/append. NOT faster storage.

### Boundary peak vs cores — the contention ceiling is mostly serial
Per-boundary's *absolute* ceiling barely triples from 1 → 32 cores, because the bottleneck (WAL +
hot index pages) is itself largely serial:

| compute | boundary @ u64 | boundary peak (concurrency-swept) |
|---------|---------------:|----------------------------------:|
| 1 core / 2 GB | 2568 | **~2,800/s** (u128–256) |
| 2 cores / 8 GB | 2567 | (not swept) |
| 32 cores / 128 GB | 3498 | **~6,900/s** (u512) |

**1c/2GB concurrency sweep** (15K, overlap=0, check), global · boundary (ratio):

| users | global · boundary (ratio) |
|------:|--------------------------:|
| 64  | 1608 · 2568 (1.60×) |
| 128 | 1509 · 2812 (1.86×) |
| 256 | 1443 · 2783 (1.93×) |
| 512 | **OOM — instance crashed** (≈516 backends > 2 GB RAM) |

- 1-core boundary plateaus ~2,800/s — disjoint appends can't truly run in parallel on 1 core, so it
  only gets the I/O-overlap / group-commit benefit. 32 cores reach ~6,900 — but that's only **~2.5×**
  more despite 32× the cores, because WAL/index contention is the real (serial) cap.
- The high boundary/global **ratio** (up to ~5× on 32 cores) is partly **global collapsing** under
  its lock queue at high concurrency, not boundary scaling — read ratio with absolute throughput.
- A **2 GB box can't even attempt high concurrency**: users=512 OOM-killed it. Small instances are
  doubly limited — few cores (low boundary ceiling) *and* can't sustain the connections to fill it.
- 15K vs 5K made no difference on the small box either — confirming storage IOPS is a non-factor.

## Conclusions

**TL;DR.** Per-boundary advisory locks let appends to *disjoint* boundaries run in parallel
instead of serializing on one global lock. The win is real but **entirely workload- and
hardware-dependent** — it ranges from *negative* to **~5×**. For typical read-then-conditional
DCB it's neutral-to-negative; for write-heavy, low-overlap, read-offloaded ingestion on a
multi-core box it's a clear, large win. Global is always the flat baseline (it can't parallelize).

### When per-boundary HELPS
- **Write-heavy / high-throughput** append ingestion, **unconditional or cheap-condition**.
- **Low overlap** — disjoint entities, so the per-boundary keys differ and appends don't collide.
- **Enough cores AND concurrency** to run appends in parallel. The advantage is **core-capped**:
  ~1.8–2× on 2 cores → genuine **~5× on 32 cores** (boundary ~6,900/s vs global ~1,500/s @ u512).
- **Decision-read offloaded** to read replicas, so the append is the only thing hitting the primary.

### When it does NOT (≤1×)
- **Read in the write path** (`condition=head` with a per-op read): the read round-trip dominates;
  the lock isn't the bottleneck.
- **High overlap**: at 100% all boundary keys collapse to one ⇒ it *is* a global lock, plus the
  per-boundary function overhead ⇒ *slower* than global (0.8–1.0×).
- **Core-starved** hosts: little parallelism to capture.

### Global is the flat baseline
It serializes on one lock, so its throughput is independent of overlap and **does not scale with
cores** (~1,500–1,800/s here). At very high user counts its lock queue saturates and it collapses
(e.g. 180/s at 768 users) — which can make the ratio *look* huge (35×) without being a real win.

### The hardware reality (the biggest practical factor)
- This workload is **commit-fsync-bound** on the **5K block storage (5,000 IOPS)**: one durable
  commit costs ~1–6 ms, and even fast hosts roughly **double** with `synchronous_commit=off`.
- **Commit-fsync latency varied ~3× between instances on the SAME 5K block storage**, same spec
  and config — host-level I/O contention you can't see or control from inside managed Postgres.
  This moved absolute throughput 3–4× across four instances (*not* explained by tier/CPU/RAM/IOPS
  spec). The 5,000-IOPS ceiling also caps the sustained commit rate once group commit is batching.
- Once the global lock is removed, the new ceiling is the **database write path**: WAL throughput,
  group-commit/fsync, and **hot-page index contention** (the single-value `type` index, the
  monotonic PK and `created_at`). Levers to raise it: more cores, faster storage,
  `synchronous_commit=off`, dropping unused indexes, table partitioning.

### Read the ratio carefully (important)
Boundary/global ratio is **not a quality metric on its own**. A *slower-commit* (worse) host
*inflates* it — the serialized global path pays a full fsync per commit while the parallel
boundary path amortizes via group commit. We literally saw a **worse instance produce a higher
4.3× ratio**. **Always read the ratio alongside absolute throughput.**

### Correctness caveat
Per-boundary is safe for **single-boundary** conditions. **Multi-boundary** conditions have the
documented position-gap (a lock-free decision read can miss a lower-positioned event that commits
later, because BIGSERIAL positions commit out of order) — fixable only with commit-order
(`xid8`/`pg_snapshot_xmin`) cursors. The included test reproduces it deterministically.

### Methodology lessons
- **Benchmark on the real target hardware, repeatedly.** A co-located Colima VM (2-core,
  *x86-emulated* on ARM) and per-host commit variance each misled by 3–5×.
- **Separate the axes** — workload (none/check/head), overlap, DB size, concurrency, hardware —
  each moves the result independently; a single number hides the story.
- **For append-heavy workloads, watch commit IOPS / fsync first** — it dominated everything else.

## Reproduce

```sh
DSN='host=<host> port=<port> user=<user> password=... dbname=<db> sslmode=require'
appendbench -dsn="$DSN" -seed=1000000 -fast-seed -partitions=10000 -seed-only   # one-time
# overlap sweep (lock model):
for ov in 0 0.2 0.5 1.0; do for lock in global boundary; do
  appendbench -dsn="$DSN" -migrate=false -lock=$lock -condition=check -overlap=$ov \
    -users=64 -partitions=10000 -duration=8s -warmup=3s -csv=runs.csv -label="check-$lock-ov$ov"
done; done
```
