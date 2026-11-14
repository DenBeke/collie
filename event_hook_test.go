package eventstore_test

import (
	"errors"
	"testing"

	"github.com/DenBeke/collie"
)

// hooked is the only payload the test hooks act on: registration is global to
// the package's test binary, so a hook must stay inert for every other test.
type hooked struct {
	ID     string `json:"id" dcb:"tag:id"`
	Reject bool   `json:"reject"`
}

var errRejected = errors.New("rejected by hook")

func init() {
	eventstore.RegisterHook(func(e *eventstore.Event, payload any) error {
		h, ok := payload.(hooked)
		if !ok {
			return nil
		}
		if h.Reject {
			return errRejected
		}
		if e.Metadata == nil {
			e.Metadata = map[string]string{}
		}
		// Record what the hook could see: tags and option-written metadata.
		e.Metadata["first"] = "tags=" + e.TagValues("id")[0] + " option=" + e.Metadata["option"]
		return nil
	})
	eventstore.RegisterHook(func(e *eventstore.Event, payload any) error {
		if _, ok := payload.(hooked); ok {
			e.Metadata["second"] = "after " + e.Metadata["first"]
		}
		return nil
	})
}

func withOption(e *eventstore.Event) {
	e.Metadata = map[string]string{"option": "set"}
}

func TestHooksRunAfterOptionsInOrder(t *testing.T) {
	fromStruct, err := eventstore.NewEventFrom("Hooked", hooked{ID: "x"}, withOption)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := eventstore.NewEvent("Hooked", hooked{ID: "x"}, eventstore.NewTags("id", "x"), withOption)
	if err != nil {
		t.Fatal(err)
	}
	for name, e := range map[string]eventstore.Event{"NewEventFrom": fromStruct, "NewEvent": plain} {
		if got := e.Metadata["first"]; got != "tags=x option=set" {
			t.Errorf("%s: first hook saw %q", name, got)
		}
		if got := e.Metadata["second"]; got != "after tags=x option=set" {
			t.Errorf("%s: second hook saw %q", name, got)
		}
	}
}

func TestHookErrorAbortsConstruction(t *testing.T) {
	_, err := eventstore.NewEventFrom("Hooked", hooked{ID: "x", Reject: true})
	if !errors.Is(err, errRejected) {
		t.Errorf("err = %v, want errRejected", err)
	}
}

func TestHooksIgnoreOtherPayloads(t *testing.T) {
	e, err := eventstore.NewEvent("Plain", "alice", nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Metadata != nil {
		t.Errorf("metadata = %v, want nil", e.Metadata)
	}
}
