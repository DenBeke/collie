package command_test

import (
	"context"
	"errors"
	"fmt"

	"eventstore"
	"eventstore/command"
	"eventstore/memory"
)

// roomCapacity caps how many seats a room can hold.
const roomCapacity = 3

// ReserveSeat books a seat in a room. The seats already taken are a
// StoredProjection cached per room through the registry's snapshot store, so each
// booking reads the running count from its checkpoint instead of replaying every
// reservation ever made — while the append condition still guards the decision
// against a concurrent booking slipping in.
//
// It embeds command.SnapshotStoreAccess to receive the injected store; that is all
// it takes to become a cached command — no UseSnapshotStore boilerplate.
type ReserveSeat struct {
	command.SnapshotStoreAccess
	Room string `json:"room"`

	seatsTaken *eventstore.StoredProjection[int]
}

// Validate rejects a reservation without a room. Dispatch calls it after
// decoding the payload, before executing.
func (cmd *ReserveSeat) Validate() error {
	if cmd.Room == "" {
		return errors.New("room is required")
	}
	return nil
}

func (cmd *ReserveSeat) Projections(ctx context.Context) ([]eventstore.Projection, error) {
	projection, err := eventstore.NewStoredProjection(ctx, cmd.SnapshotStore(), "seats-"+cmd.Room,
		eventstore.DefaultProjection[int]{
			Query: eventstore.QueryItem{
				Types: []string{"SeatReserved"},
				Tags:  eventstore.NewTags("room", cmd.Room),
			},
			Handlers: map[string]eventstore.HandleFunc[int]{
				"SeatReserved": func(count int, _ eventstore.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		})
	if err != nil {
		return nil, fmt.Errorf("could not load seats projection: %w", err)
	}
	cmd.seatsTaken = projection
	return []eventstore.Projection{cmd.seatsTaken}, nil
}

func (cmd *ReserveSeat) Decide(context.Context) ([]eventstore.Event, error) {
	if cmd.seatsTaken.State >= roomCapacity {
		return nil, fmt.Errorf("room %q is full", cmd.Room)
	}
	event, err := eventstore.NewEvent("SeatReserved", nil, eventstore.NewTags("room", cmd.Room))
	if err != nil {
		return nil, fmt.Errorf("could not create SeatReserved event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// ExampleRegistry_storedProjection registers a command that decides from a cached
// StoredProjection and dispatches it repeatedly: the snapshot advances with each
// booking, so the fourth booking is rejected without replaying the room's history.
func ExampleRegistry_storedProjection() {
	ctx := context.Background()
	store := memory.New()
	snapshotStore := memory.NewSnapshotStore()

	registry := command.NewRegistry(store,
		command.WithSnapshotStore(snapshotStore),
		command.WithCommands(&ReserveSeat{}),
	)

	// Four bookings into a room that holds three.
	for booking := 1; booking <= 4; booking++ {
		err := registry.Dispatch(ctx, "ReserveSeat", []byte(`{"room":"R1"}`))
		fmt.Printf("booking %d: %v\n", booking, err)
	}

	// Output:
	// booking 1: <nil>
	// booking 2: <nil>
	// booking 3: <nil>
	// booking 4: room "R1" is full
}

// ExampleExecute runs a command directly, without a registry: the projections
// load, Decide enforces the rule, and the append is guarded by a condition
// covering exactly what the decision read.
func ExampleExecute() {
	ctx := context.Background()
	store := memory.New()

	err := command.Execute(ctx, store, &signUpExample{Username: "alice"}, command.WithMaxAttempts(3))
	fmt.Println("first signup:", err)

	err = command.Execute(ctx, store, &signUpExample{Username: "alice"}, command.WithMaxAttempts(3))
	fmt.Println("second signup:", err)

	// Output:
	// first signup: <nil>
	// second signup: username "alice" is already taken
}

// signUpExample registers a user, failing if the username is already taken.
type signUpExample struct {
	Username string `json:"username"`
	exists   *eventstore.DefaultProjection[bool]
}

func (cmd *signUpExample) Projections(context.Context) ([]eventstore.Projection, error) {
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

func (cmd *signUpExample) Decide(context.Context) ([]eventstore.Event, error) {
	if cmd.exists.State {
		return nil, fmt.Errorf("username %q is already taken", cmd.Username)
	}
	event, err := eventstore.NewEvent("UserSignedUp", cmd.Username, eventstore.NewTags("username", cmd.Username))
	if err != nil {
		return nil, fmt.Errorf("could not create UserSignedUp event: %w", err)
	}
	return []eventstore.Event{event}, nil
}

// ExampleRegistry_errorTaxonomy shows how a transport adapter (HTTP, queue,
// CLI) classifies Dispatch errors with errors.Is — no string matching. The
// mapping stays the adapter's choice; the parenthesized statuses are what a
// typical REST handler would pick.
func ExampleRegistry_errorTaxonomy() {
	ctx := context.Background()
	registry := command.NewRegistry(memory.New(),
		command.WithSnapshotStore(memory.NewSnapshotStore()),
		command.WithCommands(&ReserveSeat{}),
	)

	classify := func(err error) string {
		switch {
		case err == nil:
			return "ok"
		case errors.Is(err, command.ErrUnknownCommand):
			return "unknown command (404)"
		case errors.Is(err, command.ErrInvalidPayload):
			return "invalid payload (400)"
		case errors.Is(err, command.ErrValidationFailed):
			return "validation failed (422)"
		case errors.Is(err, command.ErrCommandRejected):
			return "rejected (409): " + err.Error() // the domain message, unchanged
		case errors.Is(err, eventstore.ErrAppendConditionViolated):
			return "conflict (409, retry later)" // optimistic retries exhausted
		default:
			return "internal (500)"
		}
	}

	for _, dispatch := range []struct{ name, payload string }{
		{"CancelSeat", `{}`},            // never registered
		{"ReserveSeat", `{oops`},        // malformed JSON
		{"ReserveSeat", `{}`},           // decodes, but no room
		{"ReserveSeat", `{"room":"R1"}`},
		{"ReserveSeat", `{"room":"R1"}`},
		{"ReserveSeat", `{"room":"R1"}`},
		{"ReserveSeat", `{"room":"R1"}`}, // room holds three
	} {
		fmt.Println(classify(registry.Dispatch(ctx, dispatch.name, []byte(dispatch.payload))))
	}

	// Output:
	// unknown command (404)
	// invalid payload (400)
	// validation failed (422)
	// ok
	// ok
	// ok
	// rejected (409): room "R1" is full
}
