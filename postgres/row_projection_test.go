package postgres_test

import (
	"context"
	"testing"

	"eventstore"
	pgstore "eventstore/postgres"
)

type testProfile struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Order  int    `json:"order"`
}

type testProfileHandler struct{}

func (h testProfileHandler) Key(e eventstore.SequencedEvent) (string, error) {
	return e.Tags["profile"], nil
}

func (h testProfileHandler) Apply(e eventstore.SequencedEvent, current *testProfile) (*testProfile, error) {
	switch e.Type {
	case "ProfileCreated":
		data, err := eventstore.EventData[testProfile](e)
		if err != nil {
			return nil, err
		}
		return &data, nil
	case "ProfileDeleted":
		return nil, nil
	}
	return current, nil
}

// seedProfiles creates a RowProjection and populates it with test data via events.
func seedProfiles(t *testing.T) (*pgstore.RowProjection[testProfile], eventstore.Store) {
	t.Helper()
	db := setupDB(t)
	store := pgstore.New(db)
	if err := store.Migrate(); err != nil {
		t.Fatalf("could not migrate store: %v", err)
	}
	if err := pgstore.MigrateProjections(db); err != nil {
		t.Fatalf("could not migrate projections: %v", err)
	}

	proj, err := pgstore.NewRowProjection(db, "profiles", eventstore.QueryItem{
		Types: []string{"ProfileCreated", "ProfileDeleted"},
	}, testProfileHandler{})
	if err != nil {
		t.Fatalf("could not create projection: %v", err)
	}

	ctx := context.Background()

	profiles := []struct {
		id     string
		name   string
		tenant string
		status string
		order  int
	}{
		{"p1", "Alice", "t1", "active", 1},
		{"p2", "Bob", "t1", "active", 2},
		{"p3", "Charlie", "t2", "active", 1},
		{"p4", "Diana", "t1", "inactive", 3},
		{"p5", "Eve", "t2", "inactive", 2},
	}

	for _, p := range profiles {
		data := testProfile{
			Name:   p.name,
			Status: p.status,
			Order:  p.order,
		}
		e, err := eventstore.NewEvent("ProfileCreated", data, map[string]string{
			"profile": p.id,
			"tenant":  p.tenant,
			"status":  p.status,
		})
		if err != nil {
			t.Fatalf("could not create event: %v", err)
		}
		if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
			t.Fatalf("could not append event: %v", err)
		}
	}

	// Catch up the projection.
	if err := eventstore.UpdateProjection(ctx, store, proj); err != nil {
		t.Fatalf("could not update projection: %v", err)
	}

	return proj, store
}

func TestRowProjectionGet(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	val, err := proj.Get(ctx, "p1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val == nil {
		t.Fatal("expected non-nil value")
	}
	if val.Name != "Alice" {
		t.Errorf("expected Alice, got %s", val.Name)
	}

	val, err = proj.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for nonexistent key, got %+v", val)
	}
}

func TestRowProjectionListAll(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows, got %d", len(rows))
	}
}

func TestRowProjectionListWithMatchTag(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx, pgstore.WithMatchTag("tenant", "t1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows for tenant t1, got %d", len(rows))
	}
}

func TestRowProjectionListWithMultipleMatchTags(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx,
		pgstore.WithMatchTag("tenant", "t1"),
		pgstore.WithMatchTag("status", "active"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 active rows for tenant t1, got %d", len(rows))
	}
}

func TestRowProjectionListWithMatchTagMultipleValues(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx, pgstore.WithMatchTag("profile", "p1", "p3", "p5"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r.Name] = true
	}
	for _, expected := range []string{"Alice", "Charlie", "Eve"} {
		if !names[expected] {
			t.Errorf("expected %s in results", expected)
		}
	}
}

func TestRowProjectionListWithMatchTagEmpty(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	// Empty values should return all (the filter is skipped).
	rows, err := proj.List(ctx, pgstore.WithMatchTag("profile"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 5 {
		t.Fatalf("expected 5 rows with empty WithMatchTag, got %d", len(rows))
	}
}

func TestRowProjectionListWithOrderBy(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx,
		pgstore.WithMatchTag("tenant", "t1"),
		pgstore.WithOrderBy("name", false),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].Name != "Alice" || rows[1].Name != "Bob" || rows[2].Name != "Diana" {
		t.Errorf("unexpected order: %s, %s, %s", rows[0].Name, rows[1].Name, rows[2].Name)
	}

	// Descending.
	rows, err = proj.List(ctx,
		pgstore.WithMatchTag("tenant", "t1"),
		pgstore.WithOrderBy("name", true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rows[0].Name != "Diana" || rows[1].Name != "Bob" || rows[2].Name != "Alice" {
		t.Errorf("unexpected desc order: %s, %s, %s", rows[0].Name, rows[1].Name, rows[2].Name)
	}
}

func TestRowProjectionListWithPagination(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	// Get page 1 (2 items), sorted by name asc.
	page1, err := proj.List(ctx,
		pgstore.WithOrderBy("name", false),
		pgstore.WithPagination(0, 2),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2 rows in page 1, got %d", len(page1))
	}
	if page1[0].Name != "Alice" || page1[1].Name != "Bob" {
		t.Errorf("unexpected page 1: %s, %s", page1[0].Name, page1[1].Name)
	}

	// Get page 2.
	page2, err := proj.List(ctx,
		pgstore.WithOrderBy("name", false),
		pgstore.WithPagination(2, 2),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2 rows in page 2, got %d", len(page2))
	}
	if page2[0].Name != "Charlie" || page2[1].Name != "Diana" {
		t.Errorf("unexpected page 2: %s, %s", page2[0].Name, page2[1].Name)
	}

	// Get page 3 (partial).
	page3, err := proj.List(ctx,
		pgstore.WithOrderBy("name", false),
		pgstore.WithPagination(4, 2),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page3) != 1 {
		t.Fatalf("expected 1 row in page 3, got %d", len(page3))
	}
	if page3[0].Name != "Eve" {
		t.Errorf("unexpected page 3: %s", page3[0].Name)
	}
}

func TestRowProjectionEmptyResults(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	rows, err := proj.List(ctx, pgstore.WithMatchTag("tenant", "nonexistent"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 rows, got %d", len(rows))
	}
}

func TestRowProjectionDeletedRowsExcluded(t *testing.T) {
	proj, store := seedProfiles(t)
	ctx := context.Background()

	// Delete profile p2 (Bob).
	e, _ := eventstore.NewEvent("ProfileDeleted", nil, map[string]string{
		"profile": "p2",
		"tenant":  "t1",
	})
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatalf("could not append: %v", err)
	}
	if err := eventstore.UpdateProjection(ctx, store, proj); err != nil {
		t.Fatalf("could not update projection: %v", err)
	}

	// Bob should be gone from listings.
	rows, err := proj.List(ctx, pgstore.WithMatchTag("tenant", "t1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows after deletion, got %d", len(rows))
	}

	// Get should return nil for deleted key.
	val, err := proj.Get(ctx, "p2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for deleted row, got %+v", val)
	}
}

func TestRowProjectionMigrateIndexes(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	// Create indexes — should not error.
	if err := proj.MigrateIndexes("tenant", "status"); err != nil {
		t.Fatalf("MigrateIndexes failed: %v", err)
	}

	// Queries should still work correctly with indexes.
	rows, err := proj.List(ctx,
		pgstore.WithMatchTag("tenant", "t1"),
		pgstore.WithMatchTag("status", "active"),
	)
	if err != nil {
		t.Fatalf("unexpected error after index creation: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Idempotent — calling again should not error.
	if err := proj.MigrateIndexes("tenant", "status"); err != nil {
		t.Fatalf("second MigrateIndexes call failed: %v", err)
	}
}

func TestRowProjectionMatchTagWithMultipleValues(t *testing.T) {
	proj, _ := seedProfiles(t)
	ctx := context.Background()

	// Combine single-value and multi-value WithMatchTag.
	rows, err := proj.List(ctx,
		pgstore.WithMatchTag("tenant", "t1"),
		pgstore.WithMatchTag("profile", "p1", "p4"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	names := map[string]bool{}
	for _, r := range rows {
		names[r.Name] = true
	}
	if !names["Alice"] || !names["Diana"] {
		t.Errorf("expected Alice and Diana, got %v", names)
	}
}
