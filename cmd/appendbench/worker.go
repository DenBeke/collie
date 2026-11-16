package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/cmd/appendbench/internal/loadgen"
	pgstore "github.com/DenBeke/collie/postgres"

	"gorm.io/gorm"
)

const (
	eventType      = "BenchAppended"
	entityTagKey   = "entity"
	entityIDPrefix = "e-"
)

// noReadAfter is a sentinel AppendCondition.After above any real sequence number,
// used by condition=check so the in-lock check always passes (and inserts)
// without a per-op read. Fits in int64 (bigint) when cast for the SQL function.
const noReadAfter uint64 = 1 << 62

type benchPayload struct {
	V int `json:"v"`
}

// benchData is a fixed small payload reused for every appended event; the event
// body is irrelevant to what we measure (the lock + COUNT + insert cost).
var benchData = func() []byte {
	b, _ := json.Marshal(benchPayload{V: 1})
	return b
}()

// eventBody returns the per-event Data for the run: the tiny benchData by default,
// or a fixed slice of cfg.PayloadBytes bytes when set (to stress write volume / WAL
// / TOAST and the flusher). Built once and shared read-only across workers.
var (
	payloadOnce sync.Once
	payloadData []byte
)

func eventBody(cfg *Config) []byte {
	payloadOnce.Do(func() {
		if cfg.PayloadBytes > 0 {
			// Random fill so TOAST can't compress it away — a true write-volume stress.
			payloadData = make([]byte, cfg.PayloadBytes)
			for i := range payloadData {
				payloadData[i] = byte(rand.Uint32())
			}
		} else {
			payloadData = benchData
		}
	})
	return payloadData
}

func entityID(id int) string { return entityIDPrefix + strconv.Itoa(id) }

func entityTagJSON(id int) string {
	b, _ := json.Marshal(collie.NewTags(entityTagKey, entityID(id)))
	return string(b)
}

var errLogged atomic.Int64

func logErr(err error) {
	if errLogged.Add(1) <= 20 {
		log.Printf("appendbench: db error: %v", err)
	}
}

func makeEvents(cfg *Config, id int) []collie.Event {
	tag := collie.NewTags(entityTagKey, entityID(id))
	events := make([]collie.Event, cfg.EventsPerAppend)
	for i := range events {
		events[i] = collie.Event{Type: eventType, Data: eventBody(cfg), Tags: tag}
	}
	return events
}

// appendWith appends through the store, whose mode New set from cfg.Lock — the
// A/B switch under test.
func appendWith(ctx context.Context, store *pgstore.Store, _ *Config, events []collie.Event, cond *collie.AppendCondition) error {
	return store.Append(ctx, events, cond)
}

// runWorker drives a single simulated user until the context is done (duration
// mode) or the op cap is reached (ops mode, via opCounter+cancel).
func runWorker(ctx context.Context, cancel context.CancelFunc, idx int, store *pgstore.Store, db *gorm.DB, cfg *Config, measuring *atomic.Bool, opCounter *atomic.Int64, st *workerStats) {
	r := rand.New(rand.NewPCG(cfg.RNGSeed, uint64(idx)))
	picker, err := loadgen.NewPicker(cfg.Dist, cfg.Partitions, cfg.ZipfS, cfg.ZipfV, r)
	if err != nil {
		logErr(err) // already validated; defensive
		return
	}
	for {
		if ctx.Err() != nil {
			return
		}
		id := picker.Pick()
		if cfg.Overlap >= 0 {
			// Overlap mode: with probability Overlap hit the shared hot boundary
			// (entity 0); otherwise this worker's private boundary (entity idx+1,
			// unique per worker). Both are within the seeded entity range.
			if r.Float64() < cfg.Overlap {
				id = 0
			} else {
				id = idx + 1
			}
		}
		rec := measuring.Load()
		attempts, success := doOp(ctx, store, db, cfg, id, rec, st)

		if cfg.Ops > 0 && rec {
			var inc int64
			if cfg.CountMode == countAttempts {
				inc = int64(attempts)
			} else if success {
				inc = 1
			}
			if inc > 0 && opCounter.Add(inc) >= int64(cfg.Ops) {
				cancel()
				return
			}
		}
	}
}

// doOp performs one logical operation and returns the number of Append attempts
// it made and whether it ultimately succeeded. Latency/retry samples are only
// recorded when rec is true (i.e. past warmup).
func doOp(ctx context.Context, store *pgstore.Store, db *gorm.DB, cfg *Config, id int, rec bool, st *workerStats) (attempts int, success bool) {
	// Include the type so the per-boundary lock key (type,tag,value) is defined;
	// harmless for the global path (just a more selective check).
	query := collie.Query{{Types: []string{eventType}, Tags: collie.NewTags(entityTagKey, entityID(id))}}

	switch cfg.Condition {
	case condNone:
		t0 := time.Now()
		err := appendWith(ctx, store, cfg, makeEvents(cfg, id), nil)
		dt := time.Since(t0)
		st.attempts++
		if err == nil {
			st.successes++
			if rec {
				st.appendOK.Record(uint64(dt))
				st.retries.record(0)
			}
			return 1, true
		}
		if ctx.Err() != nil {
			st.canceled++
			return 1, false
		}
		st.dbErrors++
		logErr(err)
		return 1, false

	case condCreate:
		cond := &collie.AppendCondition{Query: query, After: 0}
		t0 := time.Now()
		err := appendWith(ctx, store, cfg, makeEvents(cfg, id), cond)
		dt := time.Since(t0)
		st.attempts++
		switch {
		case err == nil:
			st.successes++
			if rec {
				st.appendOK.Record(uint64(dt))
				st.retries.record(0)
			}
			return 1, true
		case errors.Is(err, collie.ErrAppendConditionViolated):
			st.conflicts++
			if rec {
				st.appendViol.Record(uint64(dt))
			}
			return 1, false
		case ctx.Err() != nil:
			st.canceled++
			return 1, false
		default:
			st.dbErrors++
			logErr(err)
			return 1, false
		}

	case condCheck:
		// Realistic head-style condition check (scans the entity's events under
		// the lock) but WITHOUT the per-op read-head round-trip: a sentinel After
		// above any sequence number makes the check pass and insert. Models
		// offloading the decision read to a read replica and measuring the pure
		// append ceiling (lock + in-lock check + insert), reads excluded.
		cond := &collie.AppendCondition{Query: query, After: noReadAfter}
		t0 := time.Now()
		err := appendWith(ctx, store, cfg, makeEvents(cfg, id), cond)
		dt := time.Since(t0)
		st.attempts++
		switch {
		case err == nil:
			st.successes++
			if rec {
				st.appendOK.Record(uint64(dt))
				st.retries.record(0)
			}
			return 1, true
		case errors.Is(err, collie.ErrAppendConditionViolated):
			st.conflicts++ // not expected with the sentinel After; handled defensively
			if rec {
				st.appendViol.Record(uint64(dt))
			}
			return 1, false
		case ctx.Err() != nil:
			st.canceled++
			return 1, false
		default:
			st.dbErrors++
			logErr(err)
			return 1, false
		}

	default: // condHead
		return doHead(ctx, store, db, cfg, query, id, rec, st)
	}
}

// doHead implements the realistic optimistic-concurrency path: read the entity's
// current head, append with After=head, and (closed loop) retry on violation.
func doHead(ctx context.Context, store *pgstore.Store, db *gorm.DB, cfg *Config, query collie.Query, id int, rec bool, st *workerStats) (attempts int, success bool) {
	eid := entityID(id)
	entityJSON := entityTagJSON(id)
	retries := 0

	for {
		t0 := time.Now()
		head, err := readHead(ctx, db, cfg.IndexedTags, eid, entityJSON)
		dt := time.Since(t0)
		if err != nil {
			if ctx.Err() != nil {
				st.canceled++
				return attempts, false
			}
			st.dbErrors++
			logErr(err)
			return attempts, false
		}
		if rec {
			st.readHead.Record(uint64(dt))
		}

		cond := &collie.AppendCondition{Query: query, After: head}
		ta := time.Now()
		aerr := appendWith(ctx, store, cfg, makeEvents(cfg, id), cond)
		adt := time.Since(ta)
		st.attempts++
		attempts++

		switch {
		case aerr == nil:
			st.successes++
			st.totalRetries += uint64(retries)
			if rec {
				st.appendOK.Record(uint64(adt))
				st.retries.record(retries)
			}
			return attempts, true

		case errors.Is(aerr, collie.ErrAppendConditionViolated):
			st.conflicts++
			if rec {
				st.appendViol.Record(uint64(adt))
			}
			if cfg.Loop == loopOpen {
				if rec {
					st.retries.record(0)
				}
				return attempts, false
			}
			retries++
			if retries > cfg.MaxRetries {
				st.failed++
				return attempts, false
			}
			if cfg.RetryBackoff > 0 {
				select {
				case <-time.After(cfg.RetryBackoff):
				case <-ctx.Done():
					st.canceled++
					return attempts, false
				}
			}
			continue

		case ctx.Err() != nil:
			st.canceled++
			return attempts, false

		default:
			st.dbErrors++
			logErr(aerr)
			return attempts, false
		}
	}
}

// readHead returns MAX(sequence_number) for the entity, mirroring the store's
// applyQuery so it uses the same index as the condition COUNT (B-tree on
// tags->>'entity' when indexed, else the GIN @> path). It runs outside any
// transaction, so the value can be stale by the time Append takes the lock —
// that staleness is exactly what produces condition violations under load.
//
// NOTE: this duplicates the single-key predicate shape from
// postgres.applyQuery; keep in sync if the tag encoding there changes.
func readHead(ctx context.Context, db *gorm.DB, indexed bool, eid, entityJSON string) (uint64, error) {
	// Tags are a jsonb array now, so the GIN @> path is the only one (the per-key
	// tags->>'key' B-tree was removed). indexed/eid are unused but kept so the
	// caller and report columns don't change.
	q := db.WithContext(ctx).Table("events").Where("tags @> ?::jsonb", entityJSON)
	var head uint64
	err := q.Select("COALESCE(MAX(sequence_number), 0)").Row().Scan(&head)
	return head, err
}
