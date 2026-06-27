package loadgen

import (
	"fmt"
	"math/rand/v2"
)

// Picker selects a partition index in [0, n). Implementations are NOT
// concurrency-safe: construct one per worker over that worker's own *rand.Rand.
type Picker interface {
	Pick() int
}

type uniformPicker struct {
	r *rand.Rand
	n int
}

func (p *uniformPicker) Pick() int { return p.r.IntN(p.n) }

type zipfPicker struct{ z *rand.Zipf }

func (p *zipfPicker) Pick() int { return int(p.z.Uint64()) }

// NewPicker builds a per-worker partition picker bound to r.
//
//   - "uniform": every partition equally likely.
//   - "zipf": a hot-spot distribution where low indices are far more likely;
//     requires s > 1 and v >= 1 (the math/rand/v2 Zipf constraints).
func NewPicker(dist string, n int, s, v float64, r *rand.Rand) (Picker, error) {
	if n < 1 {
		return nil, fmt.Errorf("partitions must be >= 1, got %d", n)
	}
	switch dist {
	case "uniform":
		return &uniformPicker{r: r, n: n}, nil
	case "zipf":
		if s <= 1 {
			return nil, fmt.Errorf("zipf requires s > 1, got %v", s)
		}
		if v < 1 {
			return nil, fmt.Errorf("zipf requires v >= 1, got %v", v)
		}
		if n == 1 {
			// imax would be 0; only one partition exists anyway.
			return &uniformPicker{r: r, n: 1}, nil
		}
		z := rand.NewZipf(r, s, v, uint64(n-1))
		if z == nil {
			return nil, fmt.Errorf("invalid zipf parameters (s=%v v=%v n=%d)", s, v, n)
		}
		return &zipfPicker{z: z}, nil
	default:
		return nil, fmt.Errorf("unknown distribution %q (want uniform|zipf)", dist)
	}
}
