package collie_test

import (
	"context"
	"testing"

	"github.com/DenBeke/collie"
)

func TestMiddlewareStore_PassesHeadAndError(t *testing.T) {
	store := seed(t, 4, "Other", nil)
	noop := func(context.Context, *collie.SequencedEvent) error { return nil }

	wrapped := collie.NewMiddlewareStore(store, collie.WithReadMiddleware(noop))
	if _, head, err := wrapped.Read(context.Background(), nil, 0); err != nil || head != 4 {
		t.Fatalf("head=%d err=%v, want 4 and nil", head, err)
	}

	broken := collie.NewMiddlewareStore(headlessStore{}, collie.WithReadMiddleware(noop))
	if _, _, err := broken.Read(context.Background(), nil, 0); err == nil {
		t.Fatal("expected the head error to pass through")
	}
}
