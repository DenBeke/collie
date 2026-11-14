package memory_test

import (
	"context"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
)

func appendOther(t *testing.T, store *memory.Store) error {
	t.Helper()
	event, err := eventstore.NewEvent("Other", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store.Append(context.Background(), []eventstore.Event{event}, nil)
}

func TestRead_HeadIsTakenAtCall(t *testing.T) {
	store := memory.New()
	for range 2 {
		if err := appendOther(t, store); err != nil {
			t.Fatal(err)
		}
	}

	events, head, err := store.Read(context.Background(), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("head=%d, want 2", head)
	}

	// Appended after Read, before iterating: yielded, and above the head.
	if err := appendOther(t, store); err != nil {
		t.Fatal(err)
	}
	var last uint64
	for e, err := range events {
		if err != nil {
			t.Fatal(err)
		}
		last = e.SequenceNumber
	}
	if last != 3 {
		t.Fatalf("last=%d, want 3", last)
	}
}
