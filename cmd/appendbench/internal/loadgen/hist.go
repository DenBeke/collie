// Package loadgen holds the DB-free, unit-testable building blocks of the
// appendbench load tester: a partition picker and a latency histogram.
package loadgen

import "math"

// The histogram uses logarithmically spaced buckets so it keeps bounded memory
// regardless of how many samples are recorded, with a fixed ~2% relative error
// that is well below typical database run-to-run noise. Recording is
// allocation-free and histograms merge by summing bucket counts, which lets each
// worker keep its own and merge once at the end.
const (
	histGrowth   = 1.02  // ~2% per bucket
	histMaxNanos = 700e9 // ceiling: 700s, far above any realistic Append latency
)

var histLogGrowth = math.Log(histGrowth)

func histNumBuckets() int { return int(math.Log(histMaxNanos)/histLogGrowth) + 2 }

// Histogram is a log-bucketed latency histogram over nanosecond values. It keeps
// exact min/max/count/sum alongside the buckets so those are reported precisely.
type Histogram struct {
	counts []uint64
	min    uint64
	max    uint64
	sum    uint64
	count  uint64
}

// NewHistogram returns an empty histogram.
func NewHistogram() *Histogram {
	return &Histogram{counts: make([]uint64, histNumBuckets())}
}

func bucketIndex(v uint64) int {
	if v <= 1 {
		return 0
	}
	idx := int(math.Log(float64(v)) / histLogGrowth)
	if idx < 0 {
		return 0
	}
	return idx
}

// Record adds a single nanosecond sample. Allocation-free.
func (h *Histogram) Record(v uint64) {
	idx := bucketIndex(v)
	if idx >= len(h.counts) {
		idx = len(h.counts) - 1
	}
	h.counts[idx]++
	if h.count == 0 || v < h.min {
		h.min = v
	}
	if h.count == 0 || v > h.max {
		h.max = v
	}
	h.count++
	h.sum += v
}

// Merge folds another histogram into this one (bucket-wise sum + exact stats).
func (h *Histogram) Merge(o *Histogram) {
	if o.count == 0 {
		return
	}
	for i, c := range o.counts {
		h.counts[i] += c
	}
	if h.count == 0 || o.min < h.min {
		h.min = o.min
	}
	if h.count == 0 || o.max > h.max {
		h.max = o.max
	}
	h.count += o.count
	h.sum += o.sum
}

// Count returns the number of recorded samples.
func (h *Histogram) Count() uint64 { return h.count }

// Min returns the smallest recorded sample (exact), or 0 if empty.
func (h *Histogram) Min() uint64 { return h.min }

// Max returns the largest recorded sample (exact), or 0 if empty.
func (h *Histogram) Max() uint64 { return h.max }

// Mean returns the exact arithmetic mean of recorded samples, or 0 if empty.
func (h *Histogram) Mean() float64 {
	if h.count == 0 {
		return 0
	}
	return float64(h.sum) / float64(h.count)
}

// Quantile returns the approximate q-quantile (0..1) in nanoseconds. Endpoints
// return the exact min/max; interior values are the geometric midpoint of the
// containing bucket, clamped to the observed [min,max] range.
func (h *Histogram) Quantile(q float64) uint64 {
	if h.count == 0 {
		return 0
	}
	if q <= 0 {
		return h.min
	}
	if q >= 1 {
		return h.max
	}
	target := uint64(math.Ceil(q * float64(h.count)))
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i, c := range h.counts {
		cum += c
		if cum >= target {
			v := uint64(math.Exp(float64(i)*histLogGrowth) * math.Sqrt(histGrowth))
			if v < h.min {
				v = h.min
			}
			if v > h.max {
				v = h.max
			}
			return v
		}
	}
	return h.max
}
