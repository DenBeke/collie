package dcb_test

import (
	"context"
	"fmt"

	"dcb"
)

// usernameExistsProjection returns true if the given username is already taken.
func usernameExistsProjection(username string) dcb.Projection[bool] {
	return dcb.Projection[bool]{
		Query: dcb.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  map[string]string{"username": username},
		},
		State: false,
		Handlers: map[string]dcb.HandleFunc[bool]{
			"UserSignedUp": func(_ bool, _ dcb.SequencedEvent) bool { return true },
		},
	}
}

// signUp registers a new user, failing if the username is already taken.
func signUp(ctx context.Context, store dcb.EventStore, username string) error {
	exists := usernameExistsProjection(username)
	condition, err := dcb.BuildDecisionModel(ctx, store, &exists)
	if err != nil {
		return err
	}
	if exists.State {
		return fmt.Errorf("username %q is already taken", username)
	}
	return store.Append(ctx, []dcb.Event{{
		Type: "UserSignedUp",
		Data: []byte(username),
		Tags: map[string]string{"username": username},
	}}, condition)
}

func ExampleBuildDecisionModel() {
	ctx := context.Background()
	store := &dcb.MemoryStore{}

	err := signUp(ctx, store, "alice")
	fmt.Println("first signup:", err)

	err = signUp(ctx, store, "alice")
	fmt.Println("second signup:", err)

	// Output:
	// first signup: <nil>
	// second signup: username "alice" is already taken
}
