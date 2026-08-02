package postgres_test

import (
	"context"
	"testing"

	pgstore "github.com/DenBeke/collie/postgres"
)

// TestCheckpointStore proves the durable half of the reactor contract: a
// checkpoint written by one process is what the next one resumes from, and an
// unknown name reports "never ran" rather than position 0 — the difference
// between starting at the head of the log and replaying it.
func TestCheckpointStore(t *testing.T) {
	db := setupDB(t)
	checkpoints := pgstore.NewCheckpointStore(db)
	err := checkpoints.Migrate()
	if err != nil {
		t.Fatalf("migrate reactor checkpoints: %v", err)
	}
	ctx := context.Background()

	position, found, err := checkpoints.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load an unknown name: %v", err)
	}
	if found || position != 0 {
		t.Fatalf("load = (%d, %t), want (0, false) for a reactor that never ran", position, found)
	}

	err = checkpoints.Save(ctx, "welcome-mailer/v1", 42)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	position, found, err = checkpoints.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found || position != 42 {
		t.Fatalf("load = (%d, %t), want (42, true)", position, found)
	}

	// Every event handled saves again under the same name, so the upsert is the
	// common path, not the exception.
	err = checkpoints.Save(ctx, "welcome-mailer/v1", 43)
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	position, _, err = checkpoints.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load after overwrite: %v", err)
	}
	if position != 43 {
		t.Fatalf("position = %d, want the second save to have won", position)
	}

	_, found, err = checkpoints.Load(ctx, "orientation-subscriber/v1")
	if err != nil {
		t.Fatalf("load another name: %v", err)
	}
	if found {
		t.Fatal("a save under one name created a checkpoint under another")
	}

	// Migrate is run on every boot, so it must be idempotent.
	err = checkpoints.Migrate()
	if err != nil {
		t.Fatalf("migrate again: %v", err)
	}
	position, _, err = checkpoints.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load after a second migrate: %v", err)
	}
	if position != 43 {
		t.Fatalf("position = %d after a second migrate, want 43", position)
	}
}
