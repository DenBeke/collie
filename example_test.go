//nolint:all
package eventstore_test

import (
	"context"
	"fmt"
	"net/http"

	"eventstore"
	"eventstore/memory"

	"github.com/labstack/echo/v4"
)

// usernameExistsProjection returns true if the given username is already taken.
func usernameExistsProjection(username string) eventstore.DefaultProjection[bool] {
	return eventstore.DefaultProjection[bool]{
		Query: eventstore.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  eventstore.NewTags("username", username),
		},
		State: false,
		Handlers: map[string]eventstore.HandleFunc[bool]{
			"UserSignedUp": func(_ bool, _ eventstore.SequencedEvent) (bool, error) {
				return true, nil
			},
		},
	}
}

// signUp registers a new user, failing if the username is already taken.
func signUp(ctx context.Context, store eventstore.Store, username string) error {
	exists := usernameExistsProjection(username)
	condition, err := eventstore.BuildDecisionModel(ctx, store, &exists)
	if err != nil {
		return fmt.Errorf("could not build decision model: %w", err)
	}
	if exists.State {
		return fmt.Errorf("username %q is already taken", username)
	}
	event, err := eventstore.NewEvent("UserSignedUp", username, eventstore.NewTags("username", username))
	if err != nil {
		return fmt.Errorf("could not create event: %w", err)
	}
	return store.Append(ctx, []eventstore.Event{event}, condition)
}

func ExampleBuildDecisionModel() {
	ctx := context.Background()
	store := &memory.Store{}

	err := signUp(ctx, store, "alice")
	fmt.Println("first signup:", err)

	err = signUp(ctx, store, "alice")
	fmt.Println("second signup:", err)

	// Output:
	// first signup: <nil>
	// second signup: username "alice" is already taken
}

// totalUsersProjection is a read model that counts all signed-up users.
// Unlike decision model projections, it is long-lived and incrementally updated.
func totalUsersProjection() *eventstore.StoredProjection[int] {
	proj, _ := eventstore.NewStoredProjection(context.Background(), memory.NewSnapshotStore(), "total-users", eventstore.DefaultProjection[int]{
		Query: eventstore.QueryItem{
			Types: []string{"UserSignedUp"},
		},
		State: 0,
		Handlers: map[string]eventstore.HandleFunc[int]{
			"UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
				return count + 1, nil
			},
		},
	})
	return proj
}

func ExampleUpdateProjection() {
	ctx := context.Background()
	store := &memory.Store{}
	totalUsers := totalUsersProjection()

	// Initial state: no users yet.
	eventstore.UpdateProjection(ctx, store, totalUsers)
	fmt.Println("total users:", totalUsers.State)

	// Sign up some users.
	signUp(ctx, store, "alice")
	signUp(ctx, store, "bob")

	// Catch up the read model.
	eventstore.UpdateProjection(ctx, store, totalUsers)
	fmt.Println("total users:", totalUsers.State)

	// Sign up another user.
	signUp(ctx, store, "charlie")

	// Incrementally update — only processes new events since last update.
	eventstore.UpdateProjection(ctx, store, totalUsers)
	fmt.Println("total users:", totalUsers.State)

	// Output:
	// total users: 0
	// total users: 2
	// total users: 3
}

// ExampleServe shows how to wire both sides into an Echo HTTP server.
// POST /signup uses a decision model projection to enforce uniqueness.
// GET /total-users uses a long-lived read model that catches up incrementally.
func ExampleServe() {
	store := &memory.Store{}
	totalUsers := totalUsersProjection()

	e := echo.New()

	e.POST("/signup", func(c echo.Context) error {
		var body struct {
			Username string `json:"username"`
		}
		if err := c.Bind(&body); err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}
		if err := signUp(c.Request().Context(), store, body.Username); err != nil {
			return c.String(http.StatusConflict, err.Error())
		}
		return c.NoContent(http.StatusNoContent)
	})

	e.GET("/total-users", func(c echo.Context) error {
		// Catch up with new events since the last query. This on-demand strategy
		// keeps the read model decoupled from the write side — no need to notify
		// read models after each command. The incremental update only processes
		// events after the last known position, so catch-up stays fast.
		if err := eventstore.UpdateProjection(c.Request().Context(), store, totalUsers); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, map[string]int{"total_users": totalUsers.State})
	})

	e.Start(":8080")
}

// The same uniqueness check expressed as a self-handling command — with the
// optimistic-concurrency retry the bare signUp helper lacks — lives in the
// command package: see [eventstore/command.Execute] and its examples.
