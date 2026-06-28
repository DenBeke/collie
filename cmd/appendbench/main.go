// Command appendbench load-tests postgres.Store.Append under its single global
// advisory lock. Because that lock is global, every append serializes regardless
// of whether tags overlap: overlap drives the conflict/retry rate, not lock
// contention, and throughput is bounded by 1/(mean critical-section duration).
// This tool sweeps users, partitions, distribution, DB size, indexing, condition
// mode, conflict handling, and injected connection latency, printing a summary
// and an optional CSV row.
//
// See the package README/plan for the recommended sweeps.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	pgstore "eventstore/postgres"

	"gorm.io/gorm"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("appendbench: %v", err)
	}
}

func run() error {
	fs := flag.NewFlagSet("appendbench", flag.ExitOnError)
	cfg := registerFlags(fs)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "appendbench — load-test postgres.Store.Append under its global advisory lock.\n\n")
		fmt.Fprintf(os.Stderr, "Usage: appendbench [flags]\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  appendbench -csv-header > runs.csv\n")
		fmt.Fprintf(os.Stderr, "  appendbench -dsn='postgres://u:p@localhost:5432/db?sslmode=disable' -seed=100000 -seed-only\n")
		fmt.Fprintf(os.Stderr, "  appendbench -dsn=... -users=16 -partitions=1000 -dist=zipf -duration=30s -csv=runs.csv -label=run1\n")
	}
	_ = fs.Parse(os.Args[1:])

	if cfg.CSVHeader {
		return printCSVHeader(os.Stdout)
	}

	warnings, err := cfg.validate()
	for _, w := range warnings {
		log.Printf("appendbench: warning: %s", w)
	}
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	db, target, cleanup, err := openDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()
	// Close the connection pool on exit so the server frees the backends
	// immediately; otherwise rapid back-to-back runs leave lingering connections
	// that exhaust the server's connection limit. Runs before cleanup (LIFO).
	defer func() {
		if sqlDB, e := db.DB(); e == nil {
			_ = sqlDB.Close()
		}
	}()

	opts := []pgstore.Option{pgstore.WithBatchSize(cfg.BatchSize)}
	store := pgstore.New(db, opts...)
	if cfg.Migrate {
		if err := store.Migrate(); err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}

	if cfg.Seed > 0 {
		if err := seedUpTo(ctx, store, db, cfg); err != nil {
			return err
		}
		if cfg.SeedOnly {
			log.Printf("appendbench: seed-only complete")
			return nil
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	// For the staging strategy, run the singleton flusher concurrently with the
	// load so appends sitting in the staging buffer get moved into events.
	flushCtx, stopFlusher := context.WithCancel(ctx)
	defer stopFlusher()
	var flusherDone chan struct{}
	if cfg.Lock == lockStaging {
		flusherDone = make(chan struct{})
		go func() {
			defer close(flusherDone)
			err := store.RunFlusher(flushCtx, pgstore.FlushOptions{Interval: cfg.FlushInterval, BatchSize: cfg.FlushBatch})
			if err != nil && flushCtx.Err() == nil {
				log.Printf("appendbench: flusher error: %v", err)
			}
		}()
	}

	meta, agg := runLoad(ctx, store, db, cfg, target)

	if cfg.Lock == lockStaging {
		if depth, err := stagingDepth(db); err == nil {
			log.Printf("appendbench: staging backlog after measured window: %d rows (≈0 means the flusher kept up)", depth)
		}
		stopFlusher()
		<-flusherDone
	}

	return computeAndReport(cfg, agg, meta)
}

// stagingDepth returns the current number of un-flushed rows in the staging table.
func stagingDepth(db *gorm.DB) (int64, error) {
	var n int64
	err := db.Table("staging").Select("COUNT(*)").Row().Scan(&n)
	return n, err
}

// runLoad launches the worker pool, manages the warmup/measurement window, and
// returns the merged stats plus run metadata.
func runLoad(parent context.Context, store *pgstore.Store, db *gorm.DB, cfg *Config, target string) (runMeta, aggregate) {
	var runCtx context.Context
	var cancel context.CancelFunc
	if cfg.Duration > 0 {
		runCtx, cancel = context.WithTimeout(parent, cfg.Warmup+cfg.Duration)
	} else {
		runCtx, cancel = context.WithCancel(parent)
	}
	defer cancel()

	measuring := &atomic.Bool{}
	opCounter := &atomic.Int64{}
	measureStartCh := make(chan time.Time, 1)

	flip := func() {
		measuring.Store(true)
		select {
		case measureStartCh <- time.Now():
		default:
		}
	}
	if cfg.Warmup > 0 {
		t := time.AfterFunc(cfg.Warmup, flip)
		defer t.Stop()
	} else {
		flip()
	}

	stats := make([]*workerStats, cfg.Users)
	var wg sync.WaitGroup
	for i := 0; i < cfg.Users; i++ {
		st := newWorkerStats(cfg.MaxRetries)
		stats[i] = st
		wg.Add(1)
		go func(idx int, st *workerStats) {
			defer wg.Done()
			runWorker(runCtx, cancel, idx, store, db, cfg, measuring, opCounter, st)
		}(i, st)
	}
	wg.Wait()

	measureEnd := time.Now()
	measureStart := measureEnd
	select {
	case measureStart = <-measureStartCh:
	default:
	}
	measuredSeconds := measureEnd.Sub(measureStart).Seconds()
	if measuredSeconds < 0 {
		measuredSeconds = 0
	}

	agg := mergeStats(cfg.MaxRetries, stats)
	meta := runMeta{
		timestamp:       time.Now().Format(time.RFC3339),
		goVersion:       runtime.Version(),
		gomaxprocs:      runtime.GOMAXPROCS(0),
		target:          target,
		measuredSeconds: measuredSeconds,
	}
	return meta, agg
}
