//nolint:all
package collie_test

import (
	"context"
	"fmt"
	"net/http"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
	"github.com/DenBeke/collie/snapshot"

	"github.com/labstack/echo/v5"
)

// usernameExistsProjection returns true if the given username is already taken.
func usernameExistsProjection(username string) collie.DefaultProjection[bool] {
	return collie.DefaultProjection[bool]{
		Query: collie.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  collie.NewTags("username", username),
		},
		State: false,
		Handlers: map[string]collie.HandleFunc[bool]{
			"UserSignedUp": func(_ bool, _ collie.SequencedEvent) (bool, error) {
				return true, nil
			},
		},
	}
}

// signUp registers a new user, failing if the username is already taken.
func signUp(ctx context.Context, store collie.Store, username string) error {
	exists := usernameExistsProjection(username)
	condition, err := collie.BuildDecisionModel(ctx, store, &exists)
	if err != nil {
		return fmt.Errorf("could not build decision model: %w", err)
	}
	if exists.State {
		return fmt.Errorf("username %q is already taken", username)
	}
	event, err := collie.NewEvent("UserSignedUp", username, collie.NewTags("username", username))
	if err != nil {
		return fmt.Errorf("could not create event: %w", err)
	}
	return store.Append(ctx, []collie.Event{event}, condition)
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
func totalUsersProjection() *snapshot.Projection[int] {
	proj, _ := snapshot.NewProjection(context.Background(), memory.NewSnapshotStore(), "total-users", collie.DefaultProjection[int]{
		Query: collie.QueryItem{
			Types: []string{"UserSignedUp"},
		},
		State: 0,
		Handlers: map[string]collie.HandleFunc[int]{
			"UserSignedUp": func(count int, _ collie.SequencedEvent) (int, error) {
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
	collie.UpdateProjection(ctx, store, totalUsers)
	fmt.Println("total users:", totalUsers.State)

	// Sign up some users.
	signUp(ctx, store, "alice")
	signUp(ctx, store, "bob")

	// Catch up the read model.
	collie.UpdateProjection(ctx, store, totalUsers)
	fmt.Println("total users:", totalUsers.State)

	// Sign up another user.
	signUp(ctx, store, "charlie")

	// Incrementally update — only processes new events since last update.
	collie.UpdateProjection(ctx, store, totalUsers)
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

	e.POST("/signup", func(c *echo.Context) error {
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

	e.GET("/total-users", func(c *echo.Context) error {
		// Catch up with new events since the last query. This on-demand strategy
		// keeps the read model decoupled from the write side — no need to notify
		// read models after each command. The incremental update only processes
		// events after the last known position, so catch-up stays fast.
		if err := collie.UpdateProjection(c.Request().Context(), store, totalUsers); err != nil {
			return err
		}
		return c.JSON(http.StatusOK, map[string]int{"total_users": totalUsers.State})
	})

	e.Start(":8080")
}

// The same uniqueness check expressed as a self-handling command — with the
// optimistic-concurrency retry the bare signUp helper lacks — lives in the
// command package: see [collie/command.Execute] and its examples.

// MessageSent is a fact about two people at once, so it belongs to two
// boundaries. Declaring the tags on the fields means the struct cannot be
// built without them, and the key may repeat because tags are a list.
type MessageSent struct {
	From string `json:"from" dcb:"tag:user"`
	To   string `json:"to"   dcb:"tag:user"`
	Body string `json:"body"`
}

func ExampleNewEventFrom() {
	event, err := collie.NewEventFrom("MessageSent", MessageSent{From: "alice", To: "bob", Body: "hi"})
	if err != nil {
		panic(err)
	}
	for _, tag := range event.Tags {
		fmt.Printf("%s=%s\n", tag.Key, tag.Value)
	}
	fmt.Println(string(event.Data))
	// Output:
	// user=alice
	// user=bob
	// {"from":"alice","to":"bob","body":"hi"}
}
