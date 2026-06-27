package main

import (
	"flag"
	"fmt"
	"time"
)

// Condition modes.
const (
	condNone   = "none"   // cond=nil: lock + insert only, no check, no conflicts
	condCreate = "create" // After=0: "entity must not exist"; single-shot
	condHead   = "head"   // read head, append with After=head; the realistic OCC path
	condCheck  = "check"  // head-style in-lock check but NO read (sentinel After); append-only ceiling
)

// Conflict-handling loops.
const (
	loopOpen   = "open"   // single attempt; record success/violation
	loopClosed = "closed" // retry on violation until success or cap
)

// Partition distributions.
const (
	distUniform = "uniform"
	distZipf    = "zipf"
)

// What -ops counts.
const (
	countAttempts  = "attempts"
	countSuccesses = "successes"
)

// Locking strategy under test.
const (
	lockGlobal   = "global"   // store.Append: one global advisory lock
	lockBoundary = "boundary" // store.AppendPerBoundary: per-(type,tag,value) locks
	lockHybrid   = "hybrid"   // store.AppendHybrid: per-boundary check + global write-lock (consistent)
	lockStaging  = "staging"  // store.AppendStaging: staging buffer + singleton flusher (consistent, concurrent)
)

// Config is the fully-resolved set of run parameters.
type Config struct {
	// Load shape.
	Users           int
	Partitions      int
	Dist            string
	ZipfS           float64
	ZipfV           float64
	Overlap         float64
	EventsPerAppend int
	PayloadBytes    int

	// Run window.
	Duration  time.Duration
	Ops       int
	CountMode string
	Warmup    time.Duration

	// Conflict / condition.
	Condition    string
	Loop         string
	MaxRetries   int
	RetryBackoff time.Duration

	// Schema / store.
	IndexedTags   bool
	BatchSize     int
	Migrate       bool
	Lock          string
	FlushInterval time.Duration // staging flusher idle interval
	FlushBatch    int           // staging flusher max rows per flush

	// DB / pool.
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration

	// Latency injection (Toxiproxy container).
	LatencyMs       int
	LatencyJitterMs int
	ToxiproxyImage  string

	// Seeding.
	Seed      int64
	SeedBatch int
	FastSeed  bool
	SeedOnly  bool

	// Output / reproducibility.
	CSVPath   string
	CSVHeader bool
	Label     string
	RNGSeed   uint64
	JSON      bool
	Quiet     bool
}

// registerFlags binds every flag onto fs and returns the Config they populate.
func registerFlags(fs *flag.FlagSet) *Config {
	c := &Config{}

	fs.IntVar(&c.Users, "users", 8, "concurrent simulated users (worker goroutines); 1 = serial lock-bound baseline")
	fs.IntVar(&c.Partitions, "partitions", 1000, "number of distinct entities (partitions)")
	fs.StringVar(&c.Dist, "dist", distUniform, "partition distribution: uniform|zipf")
	fs.Float64Var(&c.ZipfS, "zipf-s", 1.1, "zipf skew s (>1); higher = hotter partitions")
	fs.Float64Var(&c.ZipfV, "zipf-v", 1.0, "zipf v (>=1)")
	fs.Float64Var(&c.Overlap, "overlap", -1, "if >=0, fraction [0..1] of writes to ONE shared hot boundary (rest are per-worker private); overrides -partitions/-dist for entity choice. 0=no overlap (max parallel), 1=all share (≡ global lock)")
	fs.IntVar(&c.EventsPerAppend, "events-per-append", 1, "events inserted per Append call")
	fs.IntVar(&c.PayloadBytes, "payload-bytes", 0, "per-event Data payload size in bytes (0 = tiny default); use to stress write volume / the flusher")

	fs.DurationVar(&c.Duration, "duration", 30*time.Second, "measured window (use with -ops=0)")
	fs.IntVar(&c.Ops, "ops", 0, "total-op cap; if >0 you must also pass -duration=0")
	fs.StringVar(&c.CountMode, "count-mode", countAttempts, "what -ops counts: attempts|successes")
	fs.DurationVar(&c.Warmup, "warmup", 3*time.Second, "warmup window before measuring (samples discarded)")

	fs.StringVar(&c.Condition, "condition", condHead, "append condition: none|head|create|check (check = head-style in-lock check, no read)")
	fs.StringVar(&c.Loop, "loop", loopClosed, "conflict handling: open|closed")
	fs.IntVar(&c.MaxRetries, "max-retries", 50, "closed-loop retry cap before giving up")
	fs.DurationVar(&c.RetryBackoff, "retry-backoff", 0, "sleep between closed-loop retries")

	fs.BoolVar(&c.IndexedTags, "indexed-tags", true, "create/use a B-tree index on the entity tag (vs GIN-only)")
	fs.IntVar(&c.BatchSize, "batch-size", 1000, "store read batch size (WithBatchSize)")
	fs.BoolVar(&c.Migrate, "migrate", true, "run store.Migrate() before load")
	fs.StringVar(&c.Lock, "lock", lockGlobal, "locking strategy: global|boundary|hybrid|staging (staging = staging buffer + singleton flusher: consistent, concurrent, eventual read-visibility)")
	fs.DurationVar(&c.FlushInterval, "flush-interval", 5*time.Millisecond, "staging flusher idle interval (lock=staging)")
	fs.IntVar(&c.FlushBatch, "flush-batch", 5000, "staging flusher max rows moved per flush (lock=staging)")

	fs.StringVar(&c.DSN, "dsn", "", "Postgres DSN; empty starts a throwaway testcontainer")
	fs.IntVar(&c.MaxOpenConns, "max-open-conns", 0, "pool max open conns (0=auto=users+4; must be >= users)")
	fs.IntVar(&c.MaxIdleConns, "max-idle-conns", 0, "pool max idle conns (0=same as max-open)")
	fs.DurationVar(&c.ConnMaxLifetime, "conn-max-lifetime", 0, "pool conn max lifetime (0=unlimited)")

	fs.IntVar(&c.LatencyMs, "latency-ms", 0, "inject this many ms latency on every DB round-trip via a Toxiproxy container (0=off; manages its own ephemeral Postgres, so omit -dsn)")
	fs.IntVar(&c.LatencyJitterMs, "latency-jitter-ms", 0, "jitter (ms) added to the latency toxic")
	fs.StringVar(&c.ToxiproxyImage, "toxiproxy-image", "ghcr.io/shopify/toxiproxy:2.12.0", "Toxiproxy image used for latency injection")

	fs.Int64Var(&c.Seed, "seed", 0, "seed events up to this count before load (0=skip)")
	fs.IntVar(&c.SeedBatch, "seed-batch", 1000, "events per Append while seeding")
	fs.BoolVar(&c.FastSeed, "fast-seed", false, "seed via COPY/generate_series, BYPASSING Append")
	fs.BoolVar(&c.SeedOnly, "seed-only", false, "seed then exit (build a persistent DB once)")

	fs.StringVar(&c.CSVPath, "csv", "", "append one CSV result row to this file (header written if new)")
	fs.BoolVar(&c.CSVHeader, "csv-header", false, "print the CSV header and exit")
	fs.StringVar(&c.Label, "label", "", "free-form label recorded in the CSV row")
	fs.Uint64Var(&c.RNGSeed, "rng-seed", 1, "RNG seed for reproducible partition selection")
	fs.BoolVar(&c.JSON, "json", false, "also print the summary as JSON")
	fs.BoolVar(&c.Quiet, "quiet", false, "suppress the human-readable summary")

	return c
}

// validate checks the config, fills derived defaults in place, and returns any
// non-fatal warnings. A returned error is fatal.
func (c *Config) validate() (warnings []string, err error) {
	if c.Users < 1 {
		return nil, fmt.Errorf("-users must be >= 1")
	}
	if c.Partitions < 1 {
		return nil, fmt.Errorf("-partitions must be >= 1")
	}
	if c.EventsPerAppend < 1 {
		return nil, fmt.Errorf("-events-per-append must be >= 1")
	}
	if c.Overlap > 1 {
		return nil, fmt.Errorf("-overlap must be <= 1 (or negative to disable), got %v", c.Overlap)
	}
	if c.SeedBatch < 1 {
		return nil, fmt.Errorf("-seed-batch must be >= 1")
	}
	if c.BatchSize < 1 {
		return nil, fmt.Errorf("-batch-size must be >= 1")
	}
	if c.MaxRetries < 0 {
		return nil, fmt.Errorf("-max-retries must be >= 0")
	}
	if c.Seed < 0 {
		return nil, fmt.Errorf("-seed must be >= 0")
	}
	if c.LatencyMs < 0 {
		return nil, fmt.Errorf("-latency-ms must be >= 0")
	}
	if c.LatencyJitterMs < 0 {
		return nil, fmt.Errorf("-latency-jitter-ms must be >= 0")
	}
	if c.LatencyMs > 0 && c.DSN != "" {
		return nil, fmt.Errorf("-latency-ms manages its own ephemeral Postgres + Toxiproxy; omit -dsn")
	}

	switch c.Dist {
	case distUniform:
	case distZipf:
		if c.ZipfS <= 1 {
			return nil, fmt.Errorf("-zipf-s must be > 1 for zipf, got %v", c.ZipfS)
		}
		if c.ZipfV < 1 {
			return nil, fmt.Errorf("-zipf-v must be >= 1, got %v", c.ZipfV)
		}
	default:
		return nil, fmt.Errorf("-dist must be uniform|zipf, got %q", c.Dist)
	}

	switch c.Condition {
	case condNone, condCreate, condHead, condCheck:
	default:
		return nil, fmt.Errorf("-condition must be none|head|create|check, got %q", c.Condition)
	}
	switch c.Loop {
	case loopOpen, loopClosed:
	default:
		return nil, fmt.Errorf("-loop must be open|closed, got %q", c.Loop)
	}
	switch c.CountMode {
	case countAttempts, countSuccesses:
	default:
		return nil, fmt.Errorf("-count-mode must be attempts|successes, got %q", c.CountMode)
	}
	switch c.Lock {
	case lockGlobal, lockBoundary, lockHybrid, lockStaging:
	default:
		return nil, fmt.Errorf("-lock must be global|boundary|hybrid|staging, got %q", c.Lock)
	}

	// none/create are single-shot — no retry loop applies.
	if c.Condition == condNone && c.Loop != loopOpen {
		warnings = append(warnings, "condition=none has no conflicts; forcing loop=open")
		c.Loop = loopOpen
	}
	if c.Condition == condCreate && c.Loop != loopOpen {
		warnings = append(warnings, "condition=create is single-shot; forcing loop=open")
		c.Loop = loopOpen
	}
	if c.Condition == condCheck && c.Loop != loopOpen {
		warnings = append(warnings, "condition=check always passes (sentinel After); forcing loop=open")
		c.Loop = loopOpen
	}

	// Exactly one of duration / ops drives the run.
	if c.Ops > 0 && c.Duration > 0 {
		return warnings, fmt.Errorf("set -duration=0 to use -ops (got -duration=%s -ops=%d)", c.Duration, c.Ops)
	}
	if c.Ops <= 0 && c.Duration <= 0 {
		return warnings, fmt.Errorf("set either -duration>0 or -ops>0")
	}

	// Pool sizing: workers must queue on the DB advisory lock, not the client pool.
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = c.Users + 4
	}
	if c.MaxOpenConns < c.Users {
		return warnings, fmt.Errorf(
			"-max-open-conns (%d) must be >= -users (%d), else workers block on the pool instead of the DB lock",
			c.MaxOpenConns, c.Users)
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = c.MaxOpenConns
	}
	if c.MaxOpenConns > 200 {
		warnings = append(warnings, fmt.Sprintf("max-open-conns=%d: ensure the server's max_connections exceeds it", c.MaxOpenConns))
	}

	return warnings, nil
}
