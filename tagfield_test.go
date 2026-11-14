package eventstore_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/memory"
)

type userID string

type messageSent struct {
	From userID `json:"from" dcb:"tag:user"`
	To   userID `json:"to"   dcb:"tag:user"`
	Body string `json:"body"`
}

type identity struct {
	TenantID string `json:"tenantId" dcb:"tag:tenant"`
}

type documentShared struct {
	identity
	DocumentID string   `json:"documentId" dcb:"tag:document"`
	Readers    []string `json:"readers"    dcb:"tag:user"`
}

type ownerChanged struct {
	OwnerID string `json:"ownerId" dcb:"tag:owner,tag:user"`
}

type groupNotified struct {
	Members []string `json:"members" dcb:"tag:member,tag:recipient"`
}

type optionalCourse struct {
	StudentID string   `json:"studentId" dcb:"tag:student"`
	CourseID  string   `json:"courseId"  dcb:"tag:course,omitempty"`
	Cohorts   []string `json:"cohorts"   dcb:"tag:cohort,omitempty"`
}

type viaPointer struct {
	*identity
	Name string `json:"name"`
}

func tagsEqual(t *testing.T, got, want []eventstore.Tag) {
	t.Helper()
	if !slices.Equal(got, want) {
		t.Errorf("tags = %v, want %v", got, want)
	}
}

func TestNewEventFromEmitsTags(t *testing.T) {
	cases := []struct {
		name    string
		payload any
		want    []eventstore.Tag
	}{
		{"two fields same key, named string type, field order", messageSent{From: "a", To: "b", Body: "hi"},
			eventstore.NewTags("user", "a", "user", "b")},
		{"pointer payload", &messageSent{From: "a", To: "b"},
			eventstore.NewTags("user", "a", "user", "b")},
		{"embedded struct promoted, slice in element order", documentShared{identity: identity{TenantID: "t"}, DocumentID: "d", Readers: []string{"r2", "r1"}},
			eventstore.NewTags("tenant", "t", "document", "d", "user", "r2", "user", "r1")},
		{"one field two keys in written order", ownerChanged{OwnerID: "o"},
			eventstore.NewTags("owner", "o", "user", "o")},
		{"slice with two keys is element-major", groupNotified{Members: []string{"m1", "m2"}},
			eventstore.NewTags("member", "m1", "recipient", "m1", "member", "m2", "recipient", "m2")},
		{"omitempty scalar and slice emit nothing", optionalCourse{StudentID: "s"},
			eventstore.NewTags("student", "s")},
		{"omitempty fields present", optionalCourse{StudentID: "s", CourseID: "c", Cohorts: []string{"x"}},
			eventstore.NewTags("student", "s", "course", "c", "cohort", "x")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e, err := eventstore.NewEventFrom("Happened", tc.payload)
			if err != nil {
				t.Fatal(err)
			}
			if e.Type != "Happened" {
				t.Errorf("type = %q", e.Type)
			}
			tagsEqual(t, e.Tags, tc.want)
		})
	}
}

// TestNewEventFromDataMatchesNewEvent: the tag field is a copy, not a move. Data
// is byte-identical to what NewEvent produces for the same struct, and the
// options run as they do there.
func TestNewEventFromDataMatchesNewEvent(t *testing.T) {
	msg := messageSent{From: "a", To: "b", Body: "hi"}
	mark := func(e *eventstore.Event) { e.Metadata = map[string]string{"seen": "yes"} }

	from, err := eventstore.NewEventFrom("MessageSent", msg, mark)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := eventstore.NewEvent("MessageSent", msg, nil, mark)
	if err != nil {
		t.Fatal(err)
	}
	if string(from.Data) != string(plain.Data) {
		t.Errorf("Data = %s, want %s", from.Data, plain.Data)
	}
	if from.Metadata["seen"] != "yes" {
		t.Errorf("option not applied: %v", from.Metadata)
	}
	var back messageSent
	if err := json.Unmarshal(from.Data, &back); err != nil || back != msg {
		t.Errorf("Data does not round-trip: %+v, %v", back, err)
	}
}

func TestNewEventFromEmptyValues(t *testing.T) {
	cases := []struct {
		name    string
		payload any
	}{
		{"empty scalar", messageSent{From: "a"}},
		{"nil slice", documentShared{identity: identity{TenantID: "t"}, DocumentID: "d"}},
		{"empty slice", documentShared{identity: identity{TenantID: "t"}, DocumentID: "d", Readers: []string{}}},
		{"empty slice element", documentShared{identity: identity{TenantID: "t"}, DocumentID: "d", Readers: []string{"r1", ""}}},
		{"empty element even with omitempty", optionalCourse{StudentID: "s", Cohorts: []string{""}}},
		{"nil embedded pointer", viaPointer{Name: "n"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eventstore.NewEventFrom("Happened", tc.payload)
			if !errors.Is(err, eventstore.ErrEmptyTagValue) {
				t.Errorf("err = %v, want ErrEmptyTagValue", err)
			}
		})
	}
	// The embedded pointer is fine once it points somewhere.
	e, err := eventstore.NewEventFrom("Happened", viaPointer{identity: &identity{TenantID: "t"}})
	if err != nil {
		t.Fatal(err)
	}
	tagsEqual(t, e.Tags, eventstore.NewTags("tenant", "t"))
}

type (
	untagged     struct{ Name string }
	pointerField struct {
		ID *string `dcb:"tag:id"`
	}
	intField struct {
		ID int `dcb:"tag:id"`
	}
	unexportedTag struct {
		id string `dcb:"tag:id"`
	}
	hiddenFromData struct {
		ID string `json:"-" dcb:"tag:id"`
	}
	wrongToken struct {
		ID string `dcb:"tags:id"`
	}
	emptyKey struct {
		ID string `dcb:"tag:"`
	}
	keyless struct {
		ID string `dcb:"omitempty"`
	}
	repeatedKey struct {
		ID string `dcb:"tag:id,tag:id"`
	}
	unknownOption struct {
		ID string `dcb:"tag:id,required"`
	}
	taggedEmbed struct {
		identity `dcb:"tag:tenant"`
	}
)

func TestNewEventFromRejectsInvalidStructs(t *testing.T) {
	var nilPayload *messageSent
	cases := []struct {
		name    string
		payload any
	}{
		{"nil", nil},
		{"nil pointer", nilPayload},
		{"non-struct", "alice"},
		{"no dcb fields", untagged{Name: "n"}},
		{"pointer field", pointerField{}},
		{"int field", intField{}},
		{"unexported field", unexportedTag{id: "x"}},
		{`json:"-"`, hiddenFromData{ID: "x"}},
		{"wrong token", wrongToken{ID: "x"}},
		{"empty key", emptyKey{ID: "x"}},
		{"no key", keyless{ID: "x"}},
		{"repeated key", repeatedKey{ID: "x"}},
		{"unknown option", unknownOption{ID: "x"}},
		{"tag on embedded struct", taggedEmbed{identity{TenantID: "t"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := eventstore.NewEventFrom("Happened", tc.payload)
			if !errors.Is(err, eventstore.ErrInvalidTagStruct) {
				t.Errorf("err = %v, want ErrInvalidTagStruct", err)
			}
		})
	}
}

func TestNewEventFromRoundTrip(t *testing.T) {
	store := memory.New()
	ctx := context.Background()

	sent, err := eventstore.NewEventFrom("MessageSent", messageSent{From: "a", To: "b", Body: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{sent}, nil); err != nil {
		t.Fatal(err)
	}

	count := 0
	stream, _, err := store.Read(ctx, eventstore.Query{{Tags: eventstore.NewTags("user", "b")}}, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatal(err)
		}
		count++
		tagsEqual(t, e.Tags, eventstore.NewTags("user", "a", "user", "b"))
		msg, err := eventstore.EventData[messageSent](e)
		if err != nil {
			t.Fatal(err)
		}
		if msg.From != "a" || msg.To != "b" || msg.Body != "hi" {
			t.Errorf("payload = %+v", msg)
		}
	}
	if count != 1 {
		t.Errorf("user b: got %d events, want 1", count)
	}
}
