package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
)

func appendOther(t *testing.T, store *memory.Store) error {
	t.Helper()
	event, err := collie.NewEvent("Other", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	return store.Append(context.Background(), []collie.Event{event}, nil)
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

// Without a tag the condition is not keyable. The default store refuses it, as
// the PostgreSQL store does by default; WithGlobalLock takes it, as there.
func TestAppend_UnkeyableCondition(t *testing.T) {
	condition := &collie.AppendCondition{Query: collie.Query{{Types: []string{"Other"}}}}

	err := appendOtherIf(t, memory.New(), condition)
	if !errors.Is(err, collie.ErrUnkeyableCondition) {
		t.Fatalf("default store: got %v, want ErrUnkeyableCondition", err)
	}

	global := memory.New(memory.WithGlobalLock())
	if err := appendOtherIf(t, global, condition); err != nil {
		t.Fatalf("WithGlobalLock: %v", err)
	}
	if err := appendOtherIf(t, global, condition); !errors.Is(err, collie.ErrAppendConditionViolated) {
		t.Fatalf("WithGlobalLock, second append: got %v, want ErrAppendConditionViolated", err)
	}
}

func appendOtherIf(t *testing.T, store *memory.Store, condition *collie.AppendCondition) error {
	t.Helper()
	event, err := collie.NewEvent("Other", nil, collie.NewTags("id", "1"))
	if err != nil {
		t.Fatal(err)
	}
	return store.Append(context.Background(), []collie.Event{event}, condition)
}
