package postgres_test

import (
	"context"
	"slices"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/keyedprojection"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/postgres"
)

// The Postgres half of the KeyedProjection contract. The memory half lives in
// the root package's keyed_test.go; both are held to the same behaviour, which
// is the point of the interface.

type kpJob struct {
	Title  string `json:"title"`
	Status string `json:"status"`
	Rank   int    `json:"rank"`
}

type kpHandler struct{}

func (kpHandler) Key(e collie.SequencedEvent) (string, error) {
	return e.TagValues("jobId")[0], nil
}

func (kpHandler) Apply(e collie.SequencedEvent, cur *kpJob) (*kpJob, error) {
	if e.Type == "JobDeleted" {
		return nil, nil
	}
	next := kpJob{}
	if cur != nil {
		next = *cur
	}
	d, err := collie.EventData[kpJob](e)
	if err != nil {
		return nil, err
	}
	if d.Title != "" {
		next.Title = d.Title
	}
	if d.Status != "" {
		next.Status = d.Status
	}
	if d.Rank != 0 {
		next.Rank = d.Rank
	}
	return &next, nil
}

func kpSeed(t *testing.T) collie.Store {
	t.Helper()
	store := memory.New()
	add := func(typ, id string, d kpJob, extra ...collie.Tag) {
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
	add("JobCreated", "j1", kpJob{Title: "Backend", Status: "open", Rank: 2})
	add("JobCreated", "j2", kpJob{Title: "Frontend", Status: "open", Rank: 1})
	add("JobCreated", "j3", kpJob{Title: "Designer", Status: "closed", Rank: 3})
	add("JobDetailsChanged", "j1", kpJob{Title: "Backend engineer"},
		collie.NewTag("agencyId", "a1"))
	return store
}

func kpProjection(t *testing.T, name string) (keyedprojection.Projection[kpJob], collie.Store) {
	t.Helper()
	db := setupDB(t)
	if err := postgres.MigrateProjections(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := kpSeed(t)
	p, err := postgres.NewKeyedProjection[kpJob](db, name,
		collie.QueryItem{Types: []string{"JobCreated", "JobDetailsChanged", "JobDeleted"}},
		kpHandler{})
	if err != nil {
		t.Fatalf("new keyed projection: %v", err)
	}
	if err := collie.UpdateProjection(context.Background(), store, p); err != nil {
		t.Fatalf("catch up: %v", err)
	}
	return p, store
}

func TestPgKeyedListReturnsKeys(t *testing.T) {
	jobs, _ := kpProjection(t, "Jobs")
	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d entries, want 3", len(got))
	}
	for i, want := range []string{"j1", "j2", "j3"} {
		if got[i].Key != want {
			t.Errorf("entry %d key = %q, want %q", i, got[i].Key, want)
		}
	}
	if got[0].Value.Title != "Backend engineer" {
		t.Errorf("j1 title = %q, want the folded value", got[0].Value.Title)
	}
}

// TestPgKeyedFilterOnValueField is the capability RowProjection never had.
func TestPgKeyedFilterOnValueField(t *testing.T) {
	jobs, _ := kpProjection(t, "JobsFilter")
	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{
		Where: []keyedprojection.FieldEquals{{Field: "status", Value: "open"}},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("status=open returned %d, want 2", len(got))
	}
}

// TestPgKeyedTagsAccumulate: j1's last event carries only agencyId, so a row
// whose tags were overwritten rather than merged would vanish from this filter.
func TestPgKeyedTagsAccumulate(t *testing.T) {
	jobs, _ := kpProjection(t, "JobsTags")
	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{
		Tags: []collie.Tag{collie.NewTag("jobId", "j1")},
	})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 || got[0].Key != "j1" {
		t.Fatalf("jobId=j1 returned %d rows; the row dropped out of its own tag filter", len(got))
	}
}

func TestPgKeyedOrderAndPage(t *testing.T) {
	jobs, _ := kpProjection(t, "JobsOrder")
	ctx := context.Background()

	asc, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{{Field: "rank"}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if asc[0].Key != "j2" || asc[2].Key != "j3" {
		t.Fatalf("rank ascending = %s,%s,%s", asc[0].Key, asc[1].Key, asc[2].Key)
	}
	desc, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{{Field: "rank", Desc: true}}})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if desc[0].Key != "j3" {
		t.Fatalf("rank descending starts at %s", desc[0].Key)
	}
	page, err := jobs.List(ctx, keyedprojection.ListQuery{Limit: 2, Offset: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page) != 1 || page[0].Key != "j3" {
		t.Fatalf("second page = %d rows", len(page))
	}
}

// TestPgKeyedRejectsInjection: a field name that is not a bare identifier never
// reaches the query, whether it is used for filtering or ordering.
func TestPgKeyedRejectsInjection(t *testing.T) {
	jobs, _ := kpProjection(t, "JobsInject")
	ctx := context.Background()
	bad := "title' || (SELECT 1)--"

	if _, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{{Field: bad}}}); err == nil {
		t.Error("an ordering on a non-identifier must be refused")
	}
	if _, err := jobs.List(ctx, keyedprojection.ListQuery{
		Where: []keyedprojection.FieldEquals{{Field: bad, Value: "x"}},
	}); err == nil {
		t.Error("a filter on a non-identifier must be refused")
	}
}

func TestPgKeyedNullHandlerDropsTheRow(t *testing.T) {
	jobs, store := kpProjection(t, "JobsDelete")
	ctx := context.Background()

	e, _ := collie.NewEvent("JobDeleted", kpJob{}, collie.NewTags("jobId", "j1"))
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := collie.UpdateProjection(ctx, store, jobs); err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if _, found, _ := jobs.Get(ctx, "j1"); found {
		t.Error("j1 still stored after JobDeleted")
	}
}

// TestPgKeyedApplyIsIdempotent guards the checkpoint check inside Apply: a
// second instance replaying the same events must not double-apply them.
func TestPgKeyedApplyIsIdempotent(t *testing.T) {
	db := setupDB(t)
	if err := postgres.MigrateProjections(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	store := kpSeed(t)
	ctx := context.Background()

	query := collie.QueryItem{Types: []string{"JobCreated", "JobDetailsChanged", "JobDeleted"}}
	first, err := postgres.NewKeyedProjection[kpJob](db, "JobsIdem", query, kpHandler{})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := collie.UpdateProjection(ctx, store, first); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	// A second instance over the same table, starting from zero as if it had
	// never seen the stream. Every event is already applied.
	second, err := postgres.NewKeyedProjection[kpJob](db, "JobsIdem", query, kpHandler{})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if err := second.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := collie.UpdateProjection(ctx, store, second); err != nil {
		t.Fatalf("catch up: %v", err)
	}

	got, err := second.List(ctx, keyedprojection.ListQuery{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("after a rebuild there are %d rows, want 3", len(got))
	}
}

func TestPgKeyedMigrateIndexes(t *testing.T) {
	jobs, _ := kpProjection(t, "JobsIdx")
	kp, ok := jobs.(*postgres.KeyedProjection[kpJob])
	if !ok {
		t.Fatalf("unexpected type %T", jobs)
	}
	if err := kp.MigrateFieldIndexes("status", "rank"); err != nil {
		t.Fatalf("MigrateFieldIndexes: %v", err)
	}
	if err := kp.MigrateTagIndexes("jobId"); err != nil {
		t.Fatalf("MigrateTagIndexes: %v", err)
	}
	// Idempotent.
	if err := kp.MigrateFieldIndexes("status"); err != nil {
		t.Fatalf("re-running MigrateFieldIndexes: %v", err)
	}
}

func TestKeyedProjectionRecordsHeadPastTheGap(t *testing.T) {
	_, db := migratedStore(t)
	ctx := context.Background()
	open := func() *postgres.KeyedProjection[kpJob] {
		t.Helper()
		p, err := postgres.NewKeyedProjection(db, "jobs", collie.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
		if err != nil {
			t.Fatal(err)
		}
		return p
	}

	a, stale := open(), open()
	if err := a.Checkpoint(ctx, 9_999); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 0 {
		t.Fatalf("below the gap: stored=%d, want 0", got)
	}

	if err := a.Checkpoint(ctx, 20_000); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 20_000 {
		t.Fatalf("past the gap: stored=%d, want 20000", got)
	}

	// An instance that is behind must not move the stored position back.
	if err := stale.Checkpoint(ctx, 15_000); err != nil {
		t.Fatal(err)
	}
	if got := open().After(); got != 20_000 {
		t.Fatalf("after a stale checkpoint: stored=%d, want 20000", got)
	}
}

func TestKeyedProjectionAdvancesOverASparseLog(t *testing.T) {
	store, db := migratedStore(t)
	ctx := context.Background()
	pgAppend(t, store, "Other", 10_001)

	p, err := postgres.NewKeyedProjection(db, "jobs", collie.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if err := collie.UpdateProjection(ctx, store, p); err != nil {
		t.Fatal(err)
	}
	if p.After() != 10_001 {
		t.Fatalf("after=%d, want the head (10001)", p.After())
	}

	reopened, err := postgres.NewKeyedProjection(db, "jobs", collie.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if reopened.After() != 10_001 {
		t.Fatalf("reopened after=%d, want 10001", reopened.After())
	}
}

// TestPgKeyedOrder holds Postgres to the lists the memory implementation gives
// in TestKeyedProjectionOrder: numbers by value and text byte by byte, whatever
// the collation of the database.
func TestPgKeyedOrder(t *testing.T) {
	db := setupDB(t)
	if err := postgres.MigrateProjections(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()
	store := memory.New()
	for id, d := range map[string]kpJob{
		"j1": {Title: "Banana", Rank: 10},
		"j2": {Title: "cherry", Rank: 9},
		"j3": {Title: "apple", Rank: 100},
	} {
		e, err := collie.NewEvent("JobCreated", d, collie.NewTags("jobId", id))
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	jobs, err := postgres.NewKeyedProjection[kpJob](db, "JobsSorted", collie.QueryItem{Types: []string{"JobCreated"}}, kpHandler{})
	if err != nil {
		t.Fatalf("new keyed projection: %v", err)
	}
	if err := collie.UpdateProjection(ctx, store, jobs); err != nil {
		t.Fatalf("catch up: %v", err)
	}
	// The order must not depend on the index being there.
	if err := jobs.MigrateFieldIndexes("rank", "title"); err != nil {
		t.Fatalf("MigrateFieldIndexes: %v", err)
	}

	for _, test := range []struct {
		order keyedprojection.SortKey
		want  []string
	}{
		{keyedprojection.SortKey{Field: "rank"}, []string{"j2", "j1", "j3"}},
		{keyedprojection.SortKey{Field: "rank", Desc: true}, []string{"j3", "j1", "j2"}},
		{keyedprojection.SortKey{Field: "title"}, []string{"j1", "j3", "j2"}},
	} {
		got, err := jobs.List(ctx, keyedprojection.ListQuery{Order: []keyedprojection.SortKey{test.order}})
		if err != nil {
			t.Fatalf("List by %+v: %v", test.order, err)
		}
		keys := make([]string, len(got))
		for i, entry := range got {
			keys[i] = entry.Key
		}
		if !slices.Equal(keys, test.want) {
			t.Errorf("order by %+v = %v, want %v", test.order, keys, test.want)
		}
	}
}

// TestMigrateProjectionsKeepsTheRowsOfARenamedTable: a database from before
// projection_rows became projection_entries keeps what its keyed projections
// had folded.
func TestMigrateProjectionsKeepsTheRowsOfARenamedTable(t *testing.T) {
	db := setupDB(t)
	if err := postgres.MigrateProjections(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	jobs, err := postgres.NewKeyedProjection[kpJob](db, "JobsRenamed",
		collie.QueryItem{Types: []string{"JobCreated", "JobDetailsChanged", "JobDeleted"}}, kpHandler{})
	if err != nil {
		t.Fatalf("new keyed projection: %v", err)
	}
	if err := collie.UpdateProjection(context.Background(), kpSeed(t), jobs); err != nil {
		t.Fatalf("catch up: %v", err)
	}
	if err := db.Exec(`ALTER TABLE projection_entries RENAME TO projection_rows`).Error; err != nil {
		t.Fatalf("could not put the old table name back: %v", err)
	}

	for range 2 { // the second run finds nothing left to rename
		if err := postgres.MigrateProjections(db); err != nil {
			t.Fatalf("migrate: %v", err)
		}
	}
	got, err := jobs.List(context.Background(), keyedprojection.ListQuery{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("after the rename there are %d rows, want 3", len(got))
	}
}
