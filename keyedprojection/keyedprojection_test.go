package keyedprojection_test

import (
	"context"
	"slices"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/keyedprojection"
	"github.com/DenBeke/collie/memory"
)

// job is a folded state that deliberately does NOT contain its own id — which
// is the ordinary case, and the reason Entry carries the key.
type job struct {
	Title  string `json:"title"`
	Status string `json:"status"`
	Rank   int    `json:"rank"`
}

type jobHandler struct{}

func (jobHandler) Key(e collie.SequencedEvent) (string, error) {
	return e.TagValues("jobId")[0], nil
}

func (jobHandler) Apply(e collie.SequencedEvent, cur *job) (*job, error) {
	if e.Type == "JobDeleted" {
		return nil, nil // the model's null handler: drop the row
	}
	next := job{}
	if cur != nil {
		next = *cur
	}
	data, err := collie.EventData[job](e)
	if err != nil {
		return nil, err
	}
	if data.Title != "" {
		next.Title = data.Title
	}
	if data.Status != "" {
		next.Status = data.Status
	}
	if data.Rank != 0 {
		next.Rank = data.Rank
	}
	return &next, nil
}

func seedJobs(t *testing.T) collie.Store {
	t.Helper()
	store := memory.New()
	add := func(typ, id string, d job, extra ...collie.Tag) {
		t.Helper()
		tags := append([]collie.Tag{collie.NewTag("jobId", id)}, extra...)
		e, err := collie.NewEvent(typ, d, tags)
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		if err := store.Append(context.Background(), []collie.Event{e}, nil); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	add("JobCreated", "j1", job{Title: "Backend", Status: "open", Rank: 2})
	add("JobCreated", "j2", job{Title: "Frontend", Status: "open", Rank: 1})
	add("JobCreated", "j3", job{Title: "Designer", Status: "closed", Rank: 3})
	// A later event scoped to the agency only: the row must stay findable by
	// jobId, which is what makes tag accumulation load-bearing.
	add("JobDetailsChanged", "j1", job{Title: "Backend engineer"},
		collie.NewTag("agencyId", "a1"))
	return store
}

func caughtUp(t *testing.T, store collie.Store, p collie.CheckpointProjection) {
	t.Helper()
	if err := collie.UpdateProjection(context.Background(), store, p); err != nil {
		t.Fatalf("catch up: %v", err)
	}
}

// newMemoryJobs returns the projection through the interface, so every test
// below is written against collie.KeyedProjection and not against the
// memory implementation.
func newMemoryJobs(t *testing.T, store collie.Store) keyedprojection.Projection[job] {
	t.Helper()
	p := memory.NewKeyedProjection[job](
		collie.QueryItem{Types: []string{"JobCreated", "JobDetailsChanged", "JobDeleted"}},
		jobHandler{},
	)
	caughtUp(t, store, p)
	return p
}

// TestKeyedProjectionListReturnsKeys is the whole point of the interface: the
// folded state has no id of its own, so without Entry.Key a result is
// unaddressable.
func TestKeyedProjectionListReturnsKeys(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)

	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	want := []string{"j1", "j2", "j3"} // key order when no Order is given
	for i, e := range got {
		if e.Key != want[i] {
			t.Errorf("entry %d key = %q, want %q", i, e.Key, want[i])
		}
		if e.Value.Title == "" {
			t.Errorf("entry %d has no value", i)
		}
	}
}

func TestKeyedProjectionGet(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)
	ctx := context.Background()

	e, found, err := jobs.Get(ctx, "j1")
	if err != nil || !found {
		t.Fatalf("Get j1: found=%v err=%v", found, err)
	}
	if e.Key != "j1" || e.Value.Title != "Backend engineer" {
		t.Errorf("Get j1 = %+v, want the folded title", e)
	}
	if _, found, err := jobs.Get(ctx, "nope"); err != nil || found {
		t.Errorf("Get of an unknown key: found=%v err=%v, want false/nil", found, err)
	}
}

// TestKeyedProjectionFilterOnValueField covers the gap that made the read side
// unusable: filtering on a property of the folded state, not just on tags.
func TestKeyedProjectionFilterOnValueField(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)

	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{
		Where: []keyedprojection.FieldEquals{{Field: "status", Value: "open"}},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("status=open returned %d, want 2", len(got))
	}
	for _, e := range got {
		if e.Value.Status != "open" {
			t.Errorf("%s leaked through the filter with status %q", e.Key, e.Value.Status)
		}
	}
}

// TestKeyedProjectionTagsAccumulate: a row stays findable by a tag an earlier
// event carried, even after a later event scoped to something else.
func TestKeyedProjectionTagsAccumulate(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)

	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{
		Tags: []collie.Tag{collie.NewTag("jobId", "j1")},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].Key != "j1" {
		t.Fatalf("jobId=j1 returned %+v; the row dropped out of its own tag filter", got)
	}
}

func TestKeyedProjectionOrderAndPage(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)
	ctx := context.Background()

	asc, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{{Field: "rank"}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(asc) != 3 || asc[0].Key != "j2" || asc[2].Key != "j3" {
		t.Fatalf("rank ascending = %v", keysOf(asc))
	}

	desc, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{{Field: "rank", Desc: true}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if desc[0].Key != "j3" {
		t.Fatalf("rank descending = %v", keysOf(desc))
	}

	page, err := jobs.List(ctx, keyedprojection.ListQuery{Limit: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page) != 2 || page[0].Key != "j1" {
		t.Fatalf("first page = %v", keysOf(page))
	}
	page, err = jobs.List(ctx, keyedprojection.ListQuery{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page) != 1 || page[0].Key != "j3" {
		t.Fatalf("second page = %v", keysOf(page))
	}
}

// TestKeyedProjectionRefusesUnknownSort: a backend that cannot honour an
// ordering must say so, not return an arbitrary one.
func TestKeyedProjectionRefusesUnknownSort(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)
	if _, err := jobs.List(context.Background(), keyedprojection.ListQuery{
		Order: []keyedprojection.SortKey{{Field: "nosuchfield"}},
	}); err == nil {
		t.Error("sorting on an unknown field should be refused")
	}
}

// TestKeyedProjectionNullHandlerDropsTheRow mirrors the model's rule that a
// handler returning null deletes the row.
func TestKeyedProjectionNullHandlerDropsTheRow(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)
	ctx := context.Background()

	e, _ := collie.NewEvent("JobDeleted", job{}, collie.NewTags("jobId", "j1"))
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	caughtUp(t, store, jobs)

	if _, found, _ := jobs.Get(ctx, "j1"); found {
		t.Error("j1 still present after JobDeleted")
	}
	got, _ := jobs.List(ctx, keyedprojection.ListQuery{})
	if len(got) != 2 {
		t.Errorf("list has %d entries after a delete, want 2", len(got))
	}
}

// TestKeyedProjectionApplyIsIdempotent: re-applying a seen event is a no-op, so
// a second catch-up cannot double-count.
func TestKeyedProjectionApplyIsIdempotent(t *testing.T) {
	store := seedJobs(t)
	jobs := newMemoryJobs(t, store)
	ctx := context.Background()

	before, _, _ := jobs.Get(ctx, "j1")
	caughtUp(t, store, jobs) // no new events
	after, _, _ := jobs.Get(ctx, "j1")

	if before.Value != after.Value {
		t.Errorf("re-catching up changed the state: %+v -> %+v", before.Value, after.Value)
	}
}

func keysOf[V any](es []keyedprojection.Entry[V]) []string {
	out := make([]string, 0, len(es))
	for _, e := range es {
		out = append(out, e.Key)
	}
	return out
}

// TestKeyedProjectionOrder pins the order SortKey describes: numbers by value,
// so 9 before 10, and text byte by byte, so "Banana" before "apple". The
// Postgres test of the same name holds that implementation to the same lists.
func TestKeyedProjectionOrder(t *testing.T) {
	store := memory.New()
	for id, d := range map[string]job{
		"j1": {Title: "Banana", Rank: 10},
		"j2": {Title: "cherry", Rank: 9},
		"j3": {Title: "apple", Rank: 100},
	} {
		e, err := collie.NewEvent("JobCreated", d, collie.NewTags("jobId", id))
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		if err := store.Append(context.Background(), []collie.Event{e}, nil); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	jobs := newMemoryJobs(t, store)

	for _, test := range []struct {
		order keyedprojection.SortKey
		want  []string
	}{
		{keyedprojection.SortKey{Field: "rank"}, []string{"j2", "j1", "j3"}},
		{keyedprojection.SortKey{Field: "rank", Desc: true}, []string{"j3", "j1", "j2"}},
		{keyedprojection.SortKey{Field: "title"}, []string{"j1", "j3", "j2"}},
	} {
		got, err := jobs.List(context.Background(), keyedprojection.ListQuery{Order: []keyedprojection.SortKey{test.order}})
		if err != nil {
			t.Fatalf("List by %+v: %v", test.order, err)
		}
		if keys := keysOf(got); !slices.Equal(keys, test.want) {
			t.Errorf("order by %+v = %v, want %v", test.order, keys, test.want)
		}
	}
}
