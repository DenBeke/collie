package loadgen

import (
	"math/rand/v2"
	"testing"
)

func TestUniformPicker(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	p, err := NewPicker("uniform", 10, 0, 0, r)
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int, 10)
	for range 100000 {
		v := p.Pick()
		if v < 0 || v >= 10 {
			t.Fatalf("pick out of range: %d", v)
		}
		counts[v]++
	}
	for i, c := range counts {
		if c == 0 {
			t.Errorf("bucket %d never selected", i)
		}
	}
}

func TestZipfPickerIsSkewed(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	p, err := NewPicker("zipf", 100, 1.2, 1.0, r)
	if err != nil {
		t.Fatal(err)
	}
	counts := make([]int, 100)
	for range 200000 {
		v := p.Pick()
		if v < 0 || v >= 100 {
			t.Fatalf("pick out of range: %d", v)
		}
		counts[v]++
	}
	if counts[0] <= counts[99] {
		t.Errorf("expected hot-spot skew: counts[0]=%d should exceed counts[99]=%d", counts[0], counts[99])
	}
}

func TestPickerErrors(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	if _, err := NewPicker("zipf", 10, 1.0, 1.0, r); err == nil {
		t.Error("expected error for zipf s <= 1")
	}
	if _, err := NewPicker("bogus", 10, 0, 0, r); err == nil {
		t.Error("expected error for unknown distribution")
	}
	if _, err := NewPicker("uniform", 0, 0, 0, r); err == nil {
		t.Error("expected error for n < 1")
	}
}
