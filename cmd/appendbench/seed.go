package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"eventstore"
	pgstore "eventstore/postgres"

	"gorm.io/gorm"
)

// currentMaxSeq returns MAX(sequence_number), an O(1) proxy for "how many events
// exist" (the table is append-only, so this tracks table size closely enough for
// idempotent seeding).
func currentMaxSeq(db *gorm.DB) (int64, error) {
	var n int64
	err := db.Table("events").Select("COALESCE(MAX(sequence_number), 0)").Row().Scan(&n)
	return n, err
}

// seedUpTo brings the table up to cfg.Seed events, skipping if already there.
func seedUpTo(ctx context.Context, store *pgstore.Store, db *gorm.DB, cfg *Config) error {
	have, err := currentMaxSeq(db)
	if err != nil {
		return fmt.Errorf("read current count: %w", err)
	}
	if have >= cfg.Seed {
		log.Printf("appendbench: already seeded (~%d events >= target %d), skipping", have, cfg.Seed)
		return nil
	}
	need := cfg.Seed - have
	if cfg.FastSeed {
		log.Printf("appendbench: FAST-SEED %d events via COPY/generate_series (BYPASSING Append)", need)
		return fastSeed(ctx, db, cfg, need)
	}
	log.Printf("appendbench: seeding %d events via the Append path (batch=%d)", need, cfg.SeedBatch)
	return appendSeed(ctx, store, cfg, need)
}

// appendSeed seeds through the real Append code path (advisory lock + inserts),
// amortized over large batches with condition=nil. Entities are spread across
// all partitions so the seeded rows resemble run-time data.
func appendSeed(ctx context.Context, store *pgstore.Store, cfg *Config, need int64) error {
	start := time.Now()
	lastLog := start
	var done int64
	entity := 0
	for done < need {
		n := int64(cfg.SeedBatch)
		if n > need-done {
			n = need - done
		}
		events := make([]eventstore.Event, n)
		for i := int64(0); i < n; i++ {
			id := entity % cfg.Partitions
			entity++
			events[i] = eventstore.Event{
				Type: eventType,
				Data: benchData,
				Tags: map[string]string{entityTagKey: entityID(id)},
			}
		}
		if err := store.Append(ctx, events, nil); err != nil {
			return fmt.Errorf("seed append: %w", err)
		}
		done += n
		if time.Since(lastLog) > 2*time.Second {
			rate := float64(done) / time.Since(start).Seconds()
			log.Printf("appendbench: seeded %d/%d (%.0f events/s)", done, need, rate)
			lastLog = time.Now()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	log.Printf("appendbench: seeded %d events in %s", need, time.Since(start).Round(time.Millisecond))
	return nil
}

// fastSeed populates the table via a chunked INSERT ... SELECT generate_series.
// It BYPASSES Append entirely and exists only to reach large N for COUNT-cost
// runs; the rows carry the same {entity: e-<id>} tag shape so conditions hit.
func fastSeed(ctx context.Context, db *gorm.DB, cfg *Config, need int64) error {
	start := time.Now()
	const chunk = 1_000_000
	var done int64
	for done < need {
		n := need - done
		if n > chunk {
			n = chunk
		}
		// Cast every bound parameter: Postgres cannot infer the type of a
		// parameter used directly as a jsonb_build_object key or in a ||
		// concatenation (SQLSTATE 42P18).
		const sql = `INSERT INTO events (type, data, tags, created_at)
SELECT ?::text, NULL, jsonb_build_object(?::text, ?::text || ((g - 1) % ?::bigint)::text), now()
FROM generate_series(1, ?::bigint) AS g`
		if err := db.WithContext(ctx).Exec(sql, eventType, entityTagKey, entityIDPrefix, cfg.Partitions, n).Error; err != nil {
			return fmt.Errorf("fast-seed insert: %w", err)
		}
		done += n
		log.Printf("appendbench: fast-seeded %d/%d", done, need)
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	log.Printf("appendbench: fast-seeded %d events in %s", need, time.Since(start).Round(time.Millisecond))
	return nil
}
