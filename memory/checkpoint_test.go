package memory_test

import (
	"context"
	"testing"

	"github.com/DenBeke/collie/memory"
)

// TestCheckpointStore covers the whole contract a runner leans on: an unknown
// name is "never ran" rather than position 0, a saved position comes back, and a
// later save wins — a checkpoint only ever moves.
func TestCheckpointStore(t *testing.T) {
	ctx := context.Background()
	store := memory.NewCheckpointStore()

	position, found, err := store.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load an unknown name: %v", err)
	}
	if found {
		t.Fatalf("found a checkpoint for a reactor that never ran, at %d", position)
	}
	if position != 0 {
		t.Fatalf("position = %d, want 0 when nothing is stored", position)
	}

	err = store.Save(ctx, "welcome-mailer/v1", 7)
	if err != nil {
		t.Fatalf("save: %v", err)
	}
	position, found, err = store.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if !found || position != 7 {
		t.Fatalf("load = (%d, %t), want (7, true)", position, found)
	}

	err = store.Save(ctx, "welcome-mailer/v1", 9)
	if err != nil {
		t.Fatalf("overwrite: %v", err)
	}
	position, _, err = store.Load(ctx, "welcome-mailer/v1")
	if err != nil {
		t.Fatalf("load after overwrite: %v", err)
	}
	if position != 9 {
		t.Fatalf("position = %d, want the second save to have won", position)
	}

	// Names are independent: one reactor's progress says nothing about another's.
	_, found, err = store.Load(ctx, "orientation-subscriber/v1")
	if err != nil {
		t.Fatalf("load another name: %v", err)
	}
	if found {
		t.Fatal("a save under one name created a checkpoint under another")
	}
}
