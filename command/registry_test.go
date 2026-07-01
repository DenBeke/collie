package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"eventstore"
	"eventstore/command"
	"eventstore/memory"
)

// signUp is a test command mirroring the library example.
type signUp struct {
	Username string `json:"username"`
	exists   *eventstore.DefaultProjection[bool]
}

func (cmd *signUp) Projections(context.Context) ([]eventstore.Projection, error) {
	cmd.exists = &eventstore.DefaultProjection[bool]{
		Query: eventstore.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  eventstore.NewTags("username", cmd.Username),
		},
		Handlers: map[string]eventstore.HandleFunc[bool]{
			"UserSignedUp": func(bool, eventstore.SequencedEvent) (bool, error) { return true, nil },
		},
	}
	return []eventstore.Projection{cmd.exists}, nil
}

func (cmd *signUp) Decide(context.Context) ([]eventstore.Event, error) {
	if cmd.exists.State {
		return nil, fmt.Errorf("username %q is already taken", cmd.Username)
	}
	event, err := eventstore.NewEvent("UserSignedUp", cmd.Username, eventstore.NewTags("username", cmd.Username))
	if err != nil {
		return nil, fmt.Errorf("could not create UserSignedUp event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

func usersNamed(t *testing.T, store eventstore.Store, username string) int {
	t.Helper()
	query := eventstore.Query{{Types: []string{"UserSignedUp"}, Tags: eventstore.NewTags("username", username)}}
	count := 0
	for _, err := range store.Read(context.Background(), query, 0) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	return count
}

func TestDispatch_RoutesByTypeName(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	registry := command.NewRegistry(store, command.WithCommands(&signUp{}))

	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	if got := usersNamed(t, store, "alice"); got != 1 {
		t.Fatalf("expected 1 alice signup, got %d", got)
	}
}

func TestDispatch_UnknownCommand(t *testing.T) {
	registry := command.NewRegistry(memory.New())
	err := registry.Dispatch(context.Background(), "Nope", nil)
	if err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("expected unknown command error, got %v", err)
	}
}

func TestDispatch_FreshPerCall(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	registry := command.NewRegistry(store, command.WithCommands(&signUp{}))

	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("dispatch alice: %v", err)
	}
	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"bob"}`)); err != nil {
		t.Fatalf("dispatch bob: %v", err)
	}
	// Each dispatch decoded its own payload into a fresh command — no leakage.
	if got := usersNamed(t, store, "alice"); got != 1 {
		t.Fatalf("expected 1 alice, got %d", got)
	}
	if got := usersNamed(t, store, "bob"); got != 1 {
		t.Fatalf("expected 1 bob, got %d", got)
	}
}

func TestDispatch_InjectsSnapshotStore(t *testing.T) {
	ctx := context.Background()
	registry := command.NewRegistry(memory.New(),
		command.WithSnapshotStore(memory.NewSnapshotStore()),
		command.WithCommands(&needsSnapshotStore{}),
	)
	if err := registry.Dispatch(ctx, "needsSnapshotStore", nil); err != nil {
		t.Fatalf("dispatch with injected store: %v", err)
	}
}

func TestDispatch_NoInjectionWhenStoreMissing(t *testing.T) {
	registry := command.NewRegistry(memory.New(), command.WithCommands(&needsSnapshotStore{}))
	err := registry.Dispatch(context.Background(), "needsSnapshotStore", nil)
	if err == nil || !strings.Contains(err.Error(), "not injected") {
		t.Fatalf("expected 'not injected' error, got %v", err)
	}
}

// needsSnapshotStore errors unless the registry injected a snapshot store.
type needsSnapshotStore struct {
	snapshotStore eventstore.SnapshotStore
}

func (cmd *needsSnapshotStore) UseSnapshotStore(snapshotStore eventstore.SnapshotStore) {
	cmd.snapshotStore = snapshotStore
}

func (cmd *needsSnapshotStore) Projections(context.Context) ([]eventstore.Projection, error) {
	if cmd.snapshotStore == nil {
		return nil, errors.New("snapshot store not injected")
	}
	return nil, nil
}

func (cmd *needsSnapshotStore) Decide(context.Context) ([]eventstore.Event, error) {
	event, err := eventstore.NewEvent("Ok", nil, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create Ok event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// touchCounter is a cached command: it builds a StoredProjection from the injected
// snapshot store (received via the embedded command.SnapshotStoreAccess) and
// appends one Touched event per dispatch.
type touchCounter struct {
	command.SnapshotStoreAccess
	ID string `json:"id"`

	touches *eventstore.StoredProjection[int]
}

func (cmd *touchCounter) Projections(ctx context.Context) ([]eventstore.Projection, error) {
	projection, err := eventstore.NewStoredProjection(ctx, cmd.SnapshotStore(), "touch-"+cmd.ID,
		eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{Types: []string{"Touched"}, Tags: eventstore.NewTags("id", cmd.ID)},
			Handlers: map[string]eventstore.HandleFunc[int]{
				"Touched": func(count int, _ eventstore.SequencedEvent) (int, error) { return count + 1, nil },
			},
		})
	if err != nil {
		return nil, fmt.Errorf("could not load touch projection: %w", err)
	}
	cmd.touches = projection
	return []eventstore.Projection{cmd.touches}, nil
}

func (cmd *touchCounter) Decide(context.Context) ([]eventstore.Event, error) {
	event, err := eventstore.NewEvent("Touched", nil, eventstore.NewTags("id", cmd.ID))
	if err != nil {
		return nil, fmt.Errorf("could not create Touched event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// TestDispatch_CachedCommandEndToEnd exercises the whole stack: the Registry
// injects the snapshot store, the command builds a StoredProjection from it, and
// Execute catches it up and appends — across repeated dispatches.
func TestDispatch_CachedCommandEndToEnd(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	snapshotStore := memory.NewSnapshotStore()
	registry := command.NewRegistry(store,
		command.WithSnapshotStore(snapshotStore),
		command.WithCommands(&touchCounter{}),
	)

	for dispatch := 0; dispatch < 3; dispatch++ {
		if err := registry.Dispatch(ctx, "touchCounter", []byte(`{"id":"x"}`)); err != nil {
			t.Fatalf("dispatch %d: %v", dispatch, err)
		}
	}

	// Three dispatches → three Touched events for id=x.
	query := eventstore.Query{{Types: []string{"Touched"}, Tags: eventstore.NewTags("id", "x")}}
	count := 0
	for _, err := range store.Read(ctx, query, 0) {
		if err != nil {
			t.Fatal(err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("expected 3 Touched events, got %d", count)
	}

	// The cached snapshot was persisted and advanced as dispatches caught it up.
	_, position, found, err := snapshotStore.Load(ctx, "touch-x")
	if err != nil {
		t.Fatal(err)
	}
	if !found || position == 0 {
		t.Fatalf("expected a persisted snapshot with non-zero position, found=%v position=%d", found, position)
	}
}
