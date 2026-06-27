package loadgen

import (
	"math"
	"testing"
)

func TestHistogramQuantiles(t *testing.T) {
	h := NewHistogram()
	const n = 100000
	for i := 1; i <= n; i++ {
		h.Record(uint64(i))
	}
	if h.Count() != n {
		t.Fatalf("count = %d, want %d", h.Count(), n)
	}
	if h.Min() != 1 {
		t.Fatalf("min = %d, want 1", h.Min())
	}
	if h.Max() != n {
		t.Fatalf("max = %d, want %d", h.Max(), n)
	}

	check := func(q, want float64) {
		t.Helper()
		got := float64(h.Quantile(q))
		if rel := math.Abs(got-want) / want; rel > 0.05 {
			t.Errorf("Quantile(%.3f) = %.0f, want ~%.0f (rel err %.2f%% > 5%%)", q, got, want, rel*100)
		}
	}
	check(0.50, 50000)
	check(0.90, 90000)
	check(0.99, 99000)

	if m := h.Mean(); math.Abs(m-50000.5)/50000.5 > 0.01 {
		t.Errorf("mean = %.1f, want ~50000.5", m)
	}
}

func TestHistogramMerge(t *testing.T) {
	a := NewHistogram()
	b := NewHistogram()
	for i := 1; i <= 1000; i++ {
		a.Record(uint64(i))
	}
	for i := 1001; i <= 2000; i++ {
		b.Record(uint64(i))
	}
	a.Merge(b)
	if a.Count() != 2000 {
		t.Fatalf("count = %d, want 2000", a.Count())
	}
	if a.Min() != 1 || a.Max() != 2000 {
		t.Fatalf("min/max = %d/%d, want 1/2000", a.Min(), a.Max())
	}
	got := float64(a.Quantile(0.5))
	if math.Abs(got-1000)/1000 > 0.05 {
		t.Errorf("p50 = %.0f, want ~1000", got)
	}
}

func TestHistogramEmpty(t *testing.T) {
	h := NewHistogram()
	if h.Count() != 0 || h.Mean() != 0 || h.Quantile(0.5) != 0 || h.Max() != 0 {
		t.Fatalf("empty histogram returned non-zero values")
	}
}
