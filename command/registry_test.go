package command_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/snapshot"
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
	if !errors.Is(err, command.ErrUnknownCommand) {
		t.Fatalf("expected ErrUnknownCommand, got %v", err)
	}
	if !strings.Contains(err.Error(), `"Nope"`) {
		t.Fatalf("expected the command name in the message, got %v", err)
	}
}

func TestDispatch_InvalidPayload(t *testing.T) {
	registry := command.NewRegistry(memory.New(), command.WithCommands(&signUp{}))
	err := registry.Dispatch(context.Background(), "signUp", []byte(`{not json`))
	if !errors.Is(err, command.ErrInvalidPayload) {
		t.Fatalf("expected ErrInvalidPayload, got %v", err)
	}
}

// validatedSignUp requires a username, via the Validator hook.
type validatedSignUp struct {
	signUp
}

func (cmd *validatedSignUp) Validate() error {
	if cmd.Username == "" {
		return errors.New("username is required")
	}
	return nil
}

func TestDispatch_ValidationFailed(t *testing.T) {
	store := memory.New()
	registry := command.NewRegistry(store, command.WithCommands(&validatedSignUp{}))

	err := registry.Dispatch(context.Background(), "validatedSignUp", []byte(`{}`))
	if !errors.Is(err, command.ErrValidationFailed) {
		t.Fatalf("expected ErrValidationFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), "username is required") {
		t.Fatalf("expected the validation detail in the message, got %v", err)
	}
	if got := usersNamed(t, store, ""); got != 0 {
		t.Fatalf("expected no events after failed validation, got %d", got)
	}

	// A valid payload passes validation and executes.
	if err := registry.Dispatch(context.Background(), "validatedSignUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("expected valid dispatch to succeed, got %v", err)
	}
}

// errTooShort is what the fake registry-wide validator returns, standing in for
// e.g. validator.ValidationErrors; the test proves it stays errors.Is-reachable.
var errTooShort = errors.New("username must be at least 3 characters")

func TestDispatch_RegistryWideValidator(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	registry := command.NewRegistry(store,
		command.WithValidator(func(cmd any) error {
			if signUp, ok := cmd.(*signUp); ok && len(signUp.Username) < 3 {
				return errTooShort
			}
			return nil
		}),
		command.WithCommands(&signUp{}),
	)

	err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"al"}`))
	if !errors.Is(err, command.ErrValidationFailed) {
		t.Fatalf("expected ErrValidationFailed, got %v", err)
	}
	if !errors.Is(err, errTooShort) {
		t.Fatalf("expected the validator's error in the chain, got %v", err)
	}
	if got := usersNamed(t, store, "al"); got != 0 {
		t.Fatalf("expected no events after failed validation, got %d", got)
	}

	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("expected valid dispatch to succeed, got %v", err)
	}
}

// TestDispatch_RegistryValidatorRunsBeforeCommandValidate pins the layering:
// the registry-wide (shape) check runs first; the command's own Validate
// (semantics) only runs once the shape check passes.
func TestDispatch_RegistryValidatorRunsBeforeCommandValidate(t *testing.T) {
	ctx := context.Background()
	registryRejects := errors.New("shape check failed")
	registry := command.NewRegistry(memory.New(),
		command.WithValidator(func(any) error { return registryRejects }),
		command.WithCommands(&validatedSignUp{}),
	)

	// Both layers would reject the empty username; the registry-wide error wins.
	err := registry.Dispatch(ctx, "validatedSignUp", []byte(`{}`))
	if !errors.Is(err, registryRejects) {
		t.Fatalf("expected the registry-wide validator to run first, got %v", err)
	}
	if strings.Contains(err.Error(), "username is required") {
		t.Fatalf("command Validate must not have run, got %v", err)
	}
}

// TestDispatch_DomainRejectionClassifies proves a Decide rejection surfaces from
// Dispatch matching command.ErrCommandRejected with the message unchanged.
func TestDispatch_DomainRejectionClassifies(t *testing.T) {
	ctx := context.Background()
	store := memory.New()
	registry := command.NewRegistry(store, command.WithCommands(&signUp{}))

	if err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`)); err != nil {
		t.Fatalf("first dispatch: %v", err)
	}
	err := registry.Dispatch(ctx, "signUp", []byte(`{"username":"alice"}`))
	if !errors.Is(err, command.ErrCommandRejected) {
		t.Fatalf("expected ErrCommandRejected, got %v", err)
	}
	if err.Error() != `username "alice" is already taken` {
		t.Fatalf("expected the domain message unchanged, got %q", err.Error())
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
	snapshotStore snapshot.Store
}

func (cmd *needsSnapshotStore) UseSnapshotStore(snapshotStore snapshot.Store) {
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

	touches *snapshot.Projection[int]
}

func (cmd *touchCounter) Projections(ctx context.Context) ([]eventstore.Projection, error) {
	projection, err := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "touch-"+cmd.ID,
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
