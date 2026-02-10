package dcb_test

import (
	"errors"
	"fmt"

	"dcb"
)

// memStore is a minimal in-memory EventStore for examples and tests.
type memStore struct {
	events []dcb.SequencedEvent
}

func (s *memStore) Read(query dcb.Query, after uint64) ([]dcb.SequencedEvent, error) {
	var result []dcb.SequencedEvent
	for _, e := range s.events {
		if e.SequenceNumber > after && matchesQuery(e, query) {
			result = append(result, e)
		}
	}
	return result, nil
}

func (s *memStore) Append(events []dcb.Event, condition *dcb.AppendCondition) error {
	if condition != nil {
		for _, e := range s.events {
			if e.SequenceNumber > condition.After && matchesQuery(e, condition.Query) {
				return errors.New("append condition violated")
			}
		}
	}
	for _, e := range events {
		s.events = append(s.events, dcb.SequencedEvent{
			Event:          e,
			SequenceNumber: uint64(len(s.events) + 1),
		})
	}
	return nil
}

func matchesQuery(e dcb.SequencedEvent, query dcb.Query) bool {
	if len(query) == 0 {
		return true
	}
	for _, item := range query {
		if matchesItem(e, item) {
			return true
		}
	}
	return false
}

func matchesItem(e dcb.SequencedEvent, item dcb.QueryItem) bool {
	if len(item.Types) > 0 {
		found := false
		for _, t := range item.Types {
			if e.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range item.Tags {
		if e.Tags[k] != v {
			return false
		}
	}
	return true
}

// usernameExistsProjection returns true if the given username is already taken.
func usernameExistsProjection(username string) dcb.Projection[bool] {
	return dcb.Projection[bool]{
		Query: dcb.QueryItem{
			Types: []string{"UserSignedUp"},
			Tags:  map[string]string{"username": username},
		},
		Initial: false,
		Handlers: map[string]dcb.HandleFunc[bool]{
			"UserSignedUp": func(_ bool, _ dcb.SequencedEvent) bool { return true },
		},
	}
}

// signUp registers a new user, failing if the username is already taken.
func signUp(store dcb.EventStore, username string) error {
	var taken bool
	condition, err := dcb.BuildDecisionModel(store,
		dcb.WithProjection(&taken, usernameExistsProjection(username)),
	)
	if err != nil {
		return err
	}
	if taken {
		return fmt.Errorf("username %q is already taken", username)
	}
	return store.Append([]dcb.Event{{
		Type: "UserSignedUp",
		Data: username,
		Tags: map[string]string{"username": username},
	}}, condition)
}

func ExampleBuildDecisionModel() {
	store := &memStore{}

	err := signUp(store, "alice")
	fmt.Println("first signup:", err)

	err = signUp(store, "alice")
	fmt.Println("second signup:", err)

	// Output:
	// first signup: <nil>
	// second signup: username "alice" is already taken
}
