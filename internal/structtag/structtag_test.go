package structtag_test

import (
	"reflect"
	"slices"
	"testing"

	"github.com/DenBeke/collie/internal/structtag"
)

type base struct {
	Tenant string
}

type payload struct {
	base
	Name    string `json:"name"`
	Opt     string `json:"opt,omitempty"`
	Hidden  string `json:"-"`
	Dash    string `json:"-,"` //nolint:staticcheck // a field named "-" is what this fixture is for
	Untaged string
	secret  string //nolint:unused // an unexported field is what this fixture is for
}

type viaPointer struct {
	*base
	Name string
}

// names lists the yielded fields, marking a field with no value behind it.
func names(p any) []string {
	var got []string
	for f, v := range structtag.Fields(p) {
		if !v.IsValid() {
			got = append(got, f.Name+"=<absent>")
			continue
		}
		got = append(got, f.Name)
	}
	return got
}

func TestFields(t *testing.T) {
	p := payload{base: base{Tenant: "t"}, Name: "n"}
	want := []string{"base", "Tenant", "Name", "Opt", "Hidden", "Dash", "Untaged", "secret"}
	if got := names(p); !slices.Equal(got, want) {
		t.Errorf("struct: %v, want %v", got, want)
	}
	if got := names(&p); !slices.Equal(got, want) {
		t.Errorf("pointer: %v, want %v", got, want)
	}
	// A nil embedded pointer is a field itself; what it promotes has no value.
	if got, want := names(viaPointer{Name: "n"}), []string{"base", "Tenant=<absent>", "Name"}; !slices.Equal(got, want) {
		t.Errorf("nil embedded pointer: %v, want %v", got, want)
	}
	var nilPayload *payload
	for _, p := range []any{nil, nilPayload, "alice", 42, []string{"x"}} {
		if got := names(p); got != nil {
			t.Errorf("%T: %v, want nothing", p, got)
		}
	}
}

func TestJSONName(t *testing.T) {
	cases := map[string]struct {
		name string
		ok   bool
	}{
		"Name":    {"name", true},
		"Opt":     {"opt", true},
		"Hidden":  {"", false},
		"Dash":    {"-", true},
		"Untaged": {"Untaged", true},
	}
	rt := reflect.TypeFor[payload]()
	for field, want := range cases {
		f, _ := rt.FieldByName(field)
		name, ok := structtag.JSONName(f)
		if name != want.name || ok != want.ok {
			t.Errorf("%s: (%q, %v), want (%q, %v)", field, name, ok, want.name, want.ok)
		}
	}
}
