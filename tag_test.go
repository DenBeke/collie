package eventstore_test

import (
	"context"
	"slices"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
)

func TestTagsConstructor(t *testing.T) {
	got := eventstore.NewTags("a", "1", "b", "2")
	want := []eventstore.Tag{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	if len(got) != len(want) {
		t.Fatalf("len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("tag %d = %+v, want %+v", i, got[i], want[i])
		}
	}
	// An odd argument count is a programmer error: silently dropping the
	// trailing key would widen a consistency boundary, so it panics.
	defer func() {
		if recover() == nil {
			t.Errorf("odd args: expected panic")
		}
	}()
	eventstore.NewTags("a", "1", "dangling")
}

func TestTagValues(t *testing.T) {
	e := eventstore.Event{Tags: eventstore.NewTags("account", "A", "account", "B", "user", "u1")}
	if got := e.TagValues("account"); !slices.Equal(got, []string{"A", "B"}) {
		t.Errorf("TagValues(account) = %v; want [A B]", got)
	}
	if got := e.TagValues("user"); !slices.Equal(got, []string{"u1"}) {
		t.Errorf("TagValues(user) = %v; want [u1]", got)
	}
	if got := e.TagValues("missing"); got != nil {
		t.Errorf("TagValues(missing) = %v; want nil", got)
	}
}

// TestQueryItemMatchesMultiValue locks the in-memory matching semantics for an
// event that carries the same key twice (a transfer touching two accounts).
func TestQueryItemMatchesMultiValue(t *testing.T) {
	transfer := eventstore.SequencedEvent{
		Event:          eventstore.Event{Type: "Transferred", Tags: eventstore.NewTags("account", "A", "account", "B")},
		SequenceNumber: 1,
	}
	cases := []struct {
		name  string
		item  eventstore.QueryItem
		match bool
	}{
		{"first value", eventstore.QueryItem{Tags: eventstore.NewTags("account", "A")}, true},
		{"second value", eventstore.QueryItem{Tags: eventstore.NewTags("account", "B")}, true},
		{"absent value", eventstore.QueryItem{Tags: eventstore.NewTags("account", "C")}, false},
		{"both values (AND)", eventstore.QueryItem{Tags: eventstore.NewTags("account", "A", "account", "B")}, true},
		{"one present one absent", eventstore.QueryItem{Tags: eventstore.NewTags("account", "A", "account", "C")}, false},
		{"type match + tag", eventstore.QueryItem{Types: []string{"Transferred"}, Tags: eventstore.NewTags("account", "A")}, true},
		{"type mismatch", eventstore.QueryItem{Types: []string{"Other"}, Tags: eventstore.NewTags("account", "A")}, false},
	}
	for _, tc := range cases {
		if got := tc.item.Matches(transfer); got != tc.match {
			t.Errorf("%s: Matches = %v, want %v", tc.name, got, tc.match)
		}
	}
}

// TestMemoryMultiValueTagsRoundTrip checks multi-value tags survive an
// append/read round-trip through the memory store and that "any event touching
// X" queries work.
func TestMemoryMultiValueTagsRoundTrip(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	transfer, err := eventstore.NewEvent("Transferred", nil, eventstore.NewTags("account", "A", "account", "B"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{transfer}, nil); err != nil {
		t.Fatal(err)
	}

	count := 0
	stream, _, err := store.Read(ctx, eventstore.Query{{Tags: eventstore.NewTags("account", "A")}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatal(err)
		}
		count++
		if len(e.Tags) != 2 {
			t.Errorf("round-trip tags = %v, want 2", e.Tags)
		}
	}
	if count != 1 {
		t.Errorf("account A: got %d events, want 1", count)
	}

	stream, _, err = store.Read(ctx, eventstore.Query{{Tags: eventstore.NewTags("account", "C")}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, err := range stream {
		if err != nil {
			t.Fatal(err)
		}
		t.Errorf("account C should match no events")
	}
}
