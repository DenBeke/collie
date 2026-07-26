package main

import (
	"math"

	"github.com/DenBeke/collie/cmd/appendbench/internal/loadgen"
)

// workerStats is accumulated by a single worker with no shared atomics on the
// hot path, then merged once at the end.
type workerStats struct {
	attempts     uint64
	successes    uint64
	conflicts    uint64
	failed       uint64
	dbErrors     uint64
	canceled     uint64
	totalRetries uint64

	appendOK   *loadgen.Histogram // latency of successful Append calls
	appendViol *loadgen.Histogram // latency of Append calls that violated the condition
	readHead   *loadgen.Histogram // latency of the head-read (closed-loop head mode)
	retries    *retryTally        // retry count of successful ops
}

func newWorkerStats(maxRetries int) *workerStats {
	return &workerStats{
		appendOK:   loadgen.NewHistogram(),
		appendViol: loadgen.NewHistogram(),
		readHead:   loadgen.NewHistogram(),
		retries:    newRetryTally(maxRetries),
	}
}

// aggregate is the merged result across all workers.
type aggregate struct {
	attempts     uint64
	successes    uint64
	conflicts    uint64
	failed       uint64
	dbErrors     uint64
	canceled     uint64
	totalRetries uint64

	appendOK   *loadgen.Histogram
	appendViol *loadgen.Histogram
	readHead   *loadgen.Histogram
	retries    *retryTally
}

func mergeStats(maxRetries int, all []*workerStats) aggregate {
	agg := aggregate{
		appendOK:   loadgen.NewHistogram(),
		appendViol: loadgen.NewHistogram(),
		readHead:   loadgen.NewHistogram(),
		retries:    newRetryTally(maxRetries),
	}
	for _, st := range all {
		agg.attempts += st.attempts
		agg.successes += st.successes
		agg.conflicts += st.conflicts
		agg.failed += st.failed
		agg.dbErrors += st.dbErrors
		agg.canceled += st.canceled
		agg.totalRetries += st.totalRetries
		agg.appendOK.Merge(st.appendOK)
		agg.appendViol.Merge(st.appendViol)
		agg.readHead.Merge(st.readHead)
		agg.retries.merge(st.retries)
	}
	return agg
}

// retryTally is an exact discrete distribution over small retry counts
// [0, maxRetries+1]. Used for retries mean/p99/max of successful ops.
type retryTally struct{ counts []uint64 }

func newRetryTally(maxRetries int) *retryTally {
	if maxRetries < 0 {
		maxRetries = 0
	}
	return &retryTally{counts: make([]uint64, maxRetries+2)}
}

func (t *retryTally) record(n int) {
	if n < 0 {
		n = 0
	}
	if n >= len(t.counts) {
		n = len(t.counts) - 1
	}
	t.counts[n]++
}

func (t *retryTally) merge(o *retryTally) {
	for i, c := range o.counts {
		if i >= len(t.counts) {
			t.counts[len(t.counts)-1] += c
			continue
		}
		t.counts[i] += c
	}
}

func (t *retryTally) total() uint64 {
	var s uint64
	for _, c := range t.counts {
		s += c
	}
	return s
}

func (t *retryTally) mean() float64 {
	var n, s uint64
	for i, c := range t.counts {
		n += c
		s += uint64(i) * c
	}
	if n == 0 {
		return 0
	}
	return float64(s) / float64(n)
}

func (t *retryTally) max() int {
	for i := len(t.counts) - 1; i >= 0; i-- {
		if t.counts[i] > 0 {
			return i
		}
	}
	return 0
}

func (t *retryTally) quantile(q float64) int {
	total := t.total()
	if total == 0 {
		return 0
	}
	target := uint64(math.Ceil(q * float64(total)))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range t.counts {
		cum += c
		if cum >= target {
			return i
		}
	}
	return len(t.counts) - 1
}
