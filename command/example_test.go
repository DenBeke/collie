package command_test

import (
	"context"
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
