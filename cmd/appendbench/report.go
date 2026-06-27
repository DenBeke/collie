package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
)

// runMeta carries per-run facts gathered outside the worker pool.
type runMeta struct {
	timestamp       string
	goVersion       string
	gomaxprocs      int
	target          string
	measuredSeconds float64
}

// derived holds the computed metrics (latencies already converted to µs).
type derived struct {
	successesPerSec float64
	attemptsPerSec  float64
	conflictRate    float64
	failRate        float64
	retriesMean     float64
	retriesP99      int
	retriesMax      int

	okP50, okP90, okP99, okP999, okMax, okMean float64
	violP50, violP99                           float64
	rhP50, rhP99                               float64

	serialCeiling float64
}

func us(ns uint64) float64 { return float64(ns) / 1000.0 }

func computeDerived(agg aggregate, measuredSeconds float64) derived {
	var d derived
	if measuredSeconds > 0 {
		d.successesPerSec = float64(agg.successes) / measuredSeconds
		d.attemptsPerSec = float64(agg.attempts) / measuredSeconds
	}
	if agg.attempts > 0 {
		d.conflictRate = float64(agg.conflicts) / float64(agg.attempts)
	}
	if logical := agg.successes + agg.failed; logical > 0 {
		d.failRate = float64(agg.failed) / float64(logical)
	}
	d.retriesMean = agg.retries.mean()
	d.retriesP99 = agg.retries.quantile(0.99)
	d.retriesMax = agg.retries.max()

	d.okP50 = us(agg.appendOK.Quantile(0.50))
	d.okP90 = us(agg.appendOK.Quantile(0.90))
	d.okP99 = us(agg.appendOK.Quantile(0.99))
	d.okP999 = us(agg.appendOK.Quantile(0.999))
	d.okMax = us(agg.appendOK.Max())
	d.okMean = agg.appendOK.Mean() / 1000.0
	d.violP50 = us(agg.appendViol.Quantile(0.50))
	d.violP99 = us(agg.appendViol.Quantile(0.99))
	d.rhP50 = us(agg.readHead.Quantile(0.50))
	d.rhP99 = us(agg.readHead.Quantile(0.99))

	if mean := agg.appendOK.Mean(); mean > 0 {
		d.serialCeiling = 1e9 / mean
	}
	return d
}

func usStr(microseconds float64) string {
	if microseconds >= 1000 {
		return fmt.Sprintf("%.2fms", microseconds/1000)
	}
	return fmt.Sprintf("%.0fµs", microseconds)
}

func printSummary(w io.Writer, cfg *Config, agg aggregate, meta runMeta, d derived) {
	fmt.Fprintf(w, "\nappendbench  label=%q  go=%s  GOMAXPROCS=%d\n", cfg.Label, meta.goVersion, meta.gomaxprocs)
	fmt.Fprintf(w, "inputs:  users=%d partitions=%d dist=%s", cfg.Users, cfg.Partitions, cfg.Dist)
	if cfg.Dist == distZipf {
		fmt.Fprintf(w, "(s=%.2f,v=%.2f)", cfg.ZipfS, cfg.ZipfV)
	}
	fmt.Fprintf(w, " epa=%d condition=%s loop=%s lock=%s\n", cfg.EventsPerAppend, cfg.Condition, cfg.Loop, cfg.Lock)
	fmt.Fprintf(w, "         indexed=%v seedN=%d maxOpenConns=%d latencyMs=%d", cfg.IndexedTags, cfg.Seed, cfg.MaxOpenConns, cfg.LatencyMs)
	if cfg.Duration > 0 {
		fmt.Fprintf(w, " duration=%s warmup=%s\n", cfg.Duration, cfg.Warmup)
	} else {
		fmt.Fprintf(w, " ops=%d(%s) warmup=%s\n", cfg.Ops, cfg.CountMode, cfg.Warmup)
	}
	fmt.Fprintf(w, "db:      %s\n", meta.target)
	fmt.Fprintf(w, "window:  measured=%.2fs\n", meta.measuredSeconds)
	fmt.Fprintf(w, "ops:     attempts=%d successes=%d conflicts=%d failed=%d dbErrors=%d canceled=%d\n",
		agg.attempts, agg.successes, agg.conflicts, agg.failed, agg.dbErrors, agg.canceled)
	fmt.Fprintf(w, "rates:   successes/s=%.1f attempts/s=%.1f conflict_rate=%.2f%% fail_rate=%.3f%%\n",
		d.successesPerSec, d.attemptsPerSec, d.conflictRate*100, d.failRate*100)
	fmt.Fprintf(w, "retries: mean=%.3f p99=%d max=%d\n", d.retriesMean, d.retriesP99, d.retriesMax)
	fmt.Fprintf(w, "append latency (success):   p50=%s p90=%s p99=%s p99.9=%s max=%s mean=%s\n",
		usStr(d.okP50), usStr(d.okP90), usStr(d.okP99), usStr(d.okP999), usStr(d.okMax), usStr(d.okMean))
	if agg.appendViol.Count() > 0 {
		fmt.Fprintf(w, "append latency (violation): p50=%s p99=%s\n", usStr(d.violP50), usStr(d.violP99))
	}
	if agg.readHead.Count() > 0 {
		fmt.Fprintf(w, "readHead latency:           p50=%s p99=%s\n", usStr(d.rhP50), usStr(d.rhP99))
	}
	fmt.Fprintf(w, "derived: serial_ceiling≈%.0f ops/s (1e9/append_mean)  [anchor with a -users=1 run]\n", d.serialCeiling)
}

func csvColumns() []string {
	return []string{
		"timestamp", "label", "go_version", "gomaxprocs",
		"users", "partitions", "dist", "zipf_s", "zipf_v", "overlap", "events_per_append",
		"condition", "loop", "lock", "max_retries", "retry_backoff_ms", "indexed_tags", "seed_n", "batch_size", "max_open_conns", "injected_latency_ms",
		"duration_s", "warmup_s", "ops_cap", "count_mode", "rng_seed", "measured_seconds",
		"attempts", "successes", "conflicts", "failed", "db_errors", "canceled",
		"successes_per_s", "attempts_per_s", "conflict_rate", "fail_rate", "retries_mean", "retries_p99", "retries_max",
		"append_success_p50_us", "append_success_p90_us", "append_success_p99_us", "append_success_p999_us", "append_success_max_us", "append_success_mean_us",
		"append_violation_p50_us", "append_violation_p99_us", "readhead_p50_us", "readhead_p99_us",
		"serial_ceiling_ops_s",
	}
}

func csvRow(cfg *Config, agg aggregate, meta runMeta, d derived) []string {
	g := func(x float64) string { return strconv.FormatFloat(x, 'f', -1, 64) }
	f := func(x float64) string { return strconv.FormatFloat(x, 'f', 3, 64) }
	u := func(x uint64) string { return strconv.FormatUint(x, 10) }
	return []string{
		meta.timestamp, cfg.Label, meta.goVersion, strconv.Itoa(meta.gomaxprocs),
		strconv.Itoa(cfg.Users), strconv.Itoa(cfg.Partitions), cfg.Dist, g(cfg.ZipfS), g(cfg.ZipfV), g(cfg.Overlap), strconv.Itoa(cfg.EventsPerAppend),
		cfg.Condition, cfg.Loop, cfg.Lock, strconv.Itoa(cfg.MaxRetries), strconv.FormatInt(cfg.RetryBackoff.Milliseconds(), 10), strconv.FormatBool(cfg.IndexedTags), strconv.FormatInt(cfg.Seed, 10), strconv.Itoa(cfg.BatchSize), strconv.Itoa(cfg.MaxOpenConns), strconv.Itoa(cfg.LatencyMs),
		g(cfg.Duration.Seconds()), g(cfg.Warmup.Seconds()), strconv.Itoa(cfg.Ops), cfg.CountMode, strconv.FormatUint(cfg.RNGSeed, 10), f(meta.measuredSeconds),
		u(agg.attempts), u(agg.successes), u(agg.conflicts), u(agg.failed), u(agg.dbErrors), u(agg.canceled),
		f(d.successesPerSec), f(d.attemptsPerSec), g(d.conflictRate), g(d.failRate), g(d.retriesMean), strconv.Itoa(d.retriesP99), strconv.Itoa(d.retriesMax),
		f(d.okP50), f(d.okP90), f(d.okP99), f(d.okP999), f(d.okMax), f(d.okMean),
		f(d.violP50), f(d.violP99), f(d.rhP50), f(d.rhP99),
		f(d.serialCeiling),
	}
}

func printCSVHeader(w io.Writer) error {
	cw := csv.NewWriter(w)
	if err := cw.Write(csvColumns()); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

func appendCSV(path string, row []string) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	cw := csv.NewWriter(f)
	if info.Size() == 0 {
		if err := cw.Write(csvColumns()); err != nil {
			return err
		}
	}
	if err := cw.Write(row); err != nil {
		return err
	}
	cw.Flush()
	return cw.Error()
}

func printJSON(w io.Writer, cfg *Config, agg aggregate, meta runMeta, d derived) error {
	m := map[string]any{
		"timestamp": meta.timestamp, "label": cfg.Label, "go_version": meta.goVersion, "gomaxprocs": meta.gomaxprocs, "target": meta.target,
		"users": cfg.Users, "partitions": cfg.Partitions, "dist": cfg.Dist, "zipf_s": cfg.ZipfS, "zipf_v": cfg.ZipfV, "events_per_append": cfg.EventsPerAppend,
		"condition": cfg.Condition, "loop": cfg.Loop, "lock": cfg.Lock, "indexed_tags": cfg.IndexedTags, "seed_n": cfg.Seed, "max_open_conns": cfg.MaxOpenConns, "injected_latency_ms": cfg.LatencyMs,
		"measured_seconds": meta.measuredSeconds,
		"attempts":         agg.attempts, "successes": agg.successes, "conflicts": agg.conflicts, "failed": agg.failed, "db_errors": agg.dbErrors, "canceled": agg.canceled,
		"successes_per_s": d.successesPerSec, "attempts_per_s": d.attemptsPerSec, "conflict_rate": d.conflictRate, "fail_rate": d.failRate,
		"retries_mean": d.retriesMean, "retries_p99": d.retriesP99, "retries_max": d.retriesMax,
		"append_success_p50_us": d.okP50, "append_success_p90_us": d.okP90, "append_success_p99_us": d.okP99, "append_success_p999_us": d.okP999, "append_success_max_us": d.okMax, "append_success_mean_us": d.okMean,
		"append_violation_p50_us": d.violP50, "append_violation_p99_us": d.violP99, "readhead_p50_us": d.rhP50, "readhead_p99_us": d.rhP99,
		"serial_ceiling_ops_s": d.serialCeiling,
	}
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(m)
}

func computeAndReport(cfg *Config, agg aggregate, meta runMeta) error {
	d := computeDerived(agg, meta.measuredSeconds)
	if !cfg.Quiet {
		printSummary(os.Stdout, cfg, agg, meta, d)
	}
	if cfg.JSON {
		if err := printJSON(os.Stdout, cfg, agg, meta, d); err != nil {
			return err
		}
	}
	if cfg.CSVPath != "" {
		if err := appendCSV(cfg.CSVPath, csvRow(cfg, agg, meta, d)); err != nil {
			return fmt.Errorf("write csv: %w", err)
		}
	}
	return nil
}
