package command_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/command"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/snapshot"
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

	seatsTaken *snapshot.Projection[int]
}

// Validate rejects a reservation without a room. Dispatch calls it after
// decoding the payload, before executing.
func (cmd *ReserveSeat) Validate() error {
	if cmd.Room == "" {
		return errors.New("room is required")
	}
	return nil
}

func (cmd *ReserveSeat) Projections(ctx context.Context) ([]collie.Projection, error) {
	projection, err := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "seats-"+cmd.Room,
		collie.DefaultProjection[int]{
			Query: collie.QueryItem{
				Types: []string{"SeatReserved"},
				Tags:  collie.NewTags("room", cmd.Room),
			},
			Handlers: map[string]collie.HandleFunc[int]{
				"SeatReserved": func(count int, _ collie.SequencedEvent) (int, error) {
					return count + 1, nil
				},
			},
		})
	if err != nil {
		return nil, fmt.Errorf("could not load seats projection: %w", err)
	}
	cmd.seatsTaken = projection
	return []collie.Projection{cmd.seatsTaken}, nil
}

func (cmd *ReserveSeat) Decide(context.Context) ([]collie.Event, error) {
	if cmd.seatsTaken.State >= roomCapacity {
		return nil, fmt.Errorf("room %q is full", cmd.Room)
	}
	event, err := collie.NewEvent("SeatReserved", nil, collie.NewTags("room", cmd.Room))
	if err != nil {
		return nil, fmt.Errorf("could not create SeatReserved event: %w", err)
	}
	return []collie.Event{event}, nil
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
	exists   *collie.DefaultProjection[bool]
}

func (cmd *signUpExample) Projections(context.Context) ([]collie.Projection, error) {
	cmd.exists = &collie.DefaultProjection[bool]{
		Query: collie.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  collie.NewTags("username", cmd.Username),
		},
		Handlers: map[string]collie.HandleFunc[bool]{
			"UserSignedUp": func(bool, collie.SequencedEvent) (bool, error) { return true, nil },
		},
	}
	return []collie.Projection{cmd.exists}, nil
}

func (cmd *signUpExample) Decide(context.Context) ([]collie.Event, error) {
	if cmd.exists.State {
		return nil, fmt.Errorf("username %q is already taken", cmd.Username)
	}
	event, err := collie.NewEvent("UserSignedUp", cmd.Username, collie.NewTags("username", cmd.Username))
	if err != nil {
		return nil, fmt.Errorf("could not create UserSignedUp event: %w", err)
	}
	return []collie.Event{event}, nil
}

// quoteShipping prices an order through an external API. That call must not sit
// in Decide: Execute re-runs Decide on every optimistic retry, so the API would
// be hit once per attempt and each attempt could decide from a different price.
// In Prepare it runs once, before the retry loop, and every attempt decides from
// the same quote.
type quoteShipping struct {
	Order string `json:"order"`

	store    collie.Store
	pricing  func(context.Context) (int, error)
	attempts *attemptCounter

	price int // filled by Prepare, read by Decide
}

type attemptCounter struct {
	n        int
	injected bool
}

func (cmd *quoteShipping) Prepare(ctx context.Context) error {
	price, err := cmd.pricing(ctx)
	if err != nil {
		return fmt.Errorf("could not quote shipping: %w", err)
	}
	cmd.price = price
	return nil
}

func (cmd *quoteShipping) Projections(context.Context) ([]collie.Projection, error) {
	cmd.attempts.n++
	return []collie.Projection{
		&collie.DefaultProjection[int]{
			Query: collie.QueryItem{Tags: collie.NewTags("order", cmd.Order)},
			Handlers: map[string]collie.HandleFunc[int]{
				"ShippingQuoted": func(n int, _ collie.SequencedEvent) (int, error) { return n + 1, nil },
			},
		},
	}, nil
}

func (cmd *quoteShipping) Decide(ctx context.Context) ([]collie.Event, error) {
	// Slip a competing event in on the first attempt, so the guarded append
	// conflicts once and the retry path actually runs.
	if !cmd.attempts.injected {
		cmd.attempts.injected = true
		competing, err := collie.NewEvent("ShippingQuoted", 0, collie.NewTags("order", cmd.Order))
		if err != nil {
			return nil, fmt.Errorf("could not create competing event: %w", err)
		}
		if err := cmd.store.Append(ctx, []collie.Event{competing}, nil); err != nil {
			return nil, fmt.Errorf("could not append competing event: %w", err)
		}
	}
	event, err := collie.NewEvent("ShippingQuoted", cmd.price, collie.NewTags("order", cmd.Order))
	if err != nil {
		return nil, fmt.Errorf("could not create ShippingQuoted event: %w", err)
	}
	return []collie.Event{event}, nil
}

// ExampleExecute_prepare shows the phase split that makes a command with
// external dependencies safe to retry: the command runs twice, the external
// call happens once.
func ExampleExecute_prepare() {
	ctx := context.Background()
	store := memory.New()

	calls := 0
	pricing := func(context.Context) (int, error) {
		calls++
		return 1250, nil
	}

	cmd := &quoteShipping{Order: "o1", store: store, pricing: pricing, attempts: &attemptCounter{}}
	err := command.Execute(ctx, store, cmd, command.WithMaxAttempts(3))

	fmt.Println("error:", err)
	fmt.Println("attempts:", cmd.attempts.n)
	fmt.Println("pricing calls:", calls)

	// Output:
	// error: <nil>
	// attempts: 2
	// pricing calls: 1
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
		case errors.Is(err, command.ErrPrepareFailed):
			return "dependency failed (502)" // nothing appended, safe to retry
		case errors.Is(err, collie.ErrAppendConditionViolated):
			return "conflict (409, retry later)" // optimistic retries exhausted
		default:
			return "internal (500)"
		}
	}

	for _, dispatch := range []struct{ name, payload string }{
		{"CancelSeat", `{}`},     // never registered
		{"ReserveSeat", `{oops`}, // malformed JSON
		{"ReserveSeat", `{}`},    // decodes, but no room
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
