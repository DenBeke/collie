package postgres_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/DenBeke/collie"
	pgstore "github.com/DenBeke/collie/postgres"
)

// TestReadPlanUsesEventTagsIndex seeds enough events that the planner prefers the
// event_tags index, then EXPLAINs a single-tag read to confirm it does an
// index-ordered scan on event_tags (not a seq scan). Run with -v to see the plan.
func TestReadPlanUsesEventTagsIndex(t *testing.T) {
	db := setupDB(t)
	store := pgstore.New(db, pgstore.WithGlobalLock())
	if err := store.Migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	ctx := context.Background()

	const entities, perEntity = 50, 200 // 10k events; one entity ~2% selective
	for e := 0; e < entities; e++ {
		batch := make([]eventstore.Event, perEntity)
		for i := range batch {
			ev, _ := eventstore.NewEvent("Bench", nil, eventstore.NewTags("entity", fmt.Sprintf("e-%d", e)))
			batch[i] = ev
		}
		if err := store.Append(ctx, batch, nil); err != nil {
			t.Fatalf("seed append: %v", err)
		}
	}
	db.Exec("ANALYZE events")
	db.Exec("ANALYZE event_tags")

	// Mirrors buildReadSQL for a single-item single-tag query.
	const q = `WITH cand AS (
  SELECT t.sequence_number FROM event_tags t
  WHERE t.key = 'entity' AND t.value = 'e-7' AND t.sequence_number > 0
  ORDER BY t.sequence_number LIMIT 1000
)
SELECT e.sequence_number, e.type, e.data, e.tags, e.metadata, e.actor, e.cause, e.recorded_at
FROM events e JOIN (SELECT sequence_number FROM cand ORDER BY sequence_number LIMIT 1000) c
  ON c.sequence_number = e.sequence_number
ORDER BY e.sequence_number`

	var plan []string
	if err := db.Raw("EXPLAIN (ANALYZE, BUFFERS) " + q).Scan(&plan).Error; err != nil {
		t.Fatalf("explain: %v", err)
	}
	full := strings.Join(plan, "\n")
	t.Logf("single-tag read plan:\n%s", full)

	if !strings.Contains(full, "event_tags") {
		t.Errorf("plan does not reference event_tags:\n%s", full)
	}
	if strings.Contains(full, "Seq Scan on event_tags") {
		t.Errorf("plan seq-scans event_tags (expected an index scan):\n%s", full)
	}
}
