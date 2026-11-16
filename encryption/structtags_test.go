package encryption_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/encryption"
	"github.com/DenBeke/collie/memory"
)

func init() { encryption.RegisterStructTags() }

type contact struct {
	Name  string `json:"name"`
	Phone string `json:"phone"`
}

type supplierRegistered struct {
	SupplierID string  `json:"supplierId" dcb:"tag:supplier"`
	Contact    contact `json:"contact"    dcb-encrypt:"tag:supplier"`
}

type twoFields struct {
	UserID string `json:"userId" dcb:"tag:user"`
	Name   string `json:"name"   dcb-encrypt:"tag:user"`
	Iban   string `json:"iban"   dcb-encrypt:"tag:user"`
}

func TestStructTagsDeclareWhatOptionsDeclare(t *testing.T) {
	data := twoFields{UserID: "u1", Name: "Ada", Iban: "BE00"}
	declared, err := collie.NewEventFrom("UserRegistered", data)
	if err != nil {
		t.Fatal(err)
	}
	byOptions, err := collie.NewEvent("UserRegistered", data, collie.NewTags("user", "u1"),
		encryption.WithEncryptionKeys("user"), encryption.WithEncryptedFields("name", "iban"))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{encryption.MetadataKey, encryption.FieldsMetadataKey} {
		if declared.Metadata[key] != byOptions.Metadata[key] {
			t.Errorf("%s = %q, options give %q", key, declared.Metadata[key], byOptions.Metadata[key])
		}
	}
	// Options and struct agreeing is fine; the same event built with both.
	if _, err := collie.NewEventFrom("UserRegistered", data,
		encryption.WithEncryptionKeys("user"), encryption.WithEncryptedFields("name", "iban")); err != nil {
		t.Errorf("agreeing option: %v", err)
	}
}

func TestStructTagsNestedBlockRoundTripAndForget(t *testing.T) {
	raw := memory.New()
	enc := newEncryptor()
	store := collie.NewMiddlewareStore(raw,
		collie.WithAppendMiddleware(enc.OnAppend),
		collie.WithReadMiddleware(enc.OnRead),
	)
	ctx := context.Background()

	event, err := collie.NewEventFrom("SupplierRegistered", supplierRegistered{
		SupplierID: "s1", Contact: contact{Name: "Acme", Phone: "123"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{event}, nil); err != nil {
		t.Fatal(err)
	}

	got := collectEvents(t, store, nil)[0]
	data, err := collie.EventData[supplierRegistered](got)
	if err != nil {
		t.Fatal(err)
	}
	if data.Contact.Phone != "123" {
		t.Errorf("decrypted payload = %+v", data)
	}

	// At rest the whole block is one opaque string: not even the key names.
	atRest := collectEvents(t, raw, nil)[0]
	if s := string(atRest.Data); strings.Contains(s, "phone") || strings.Contains(s, "Acme") {
		t.Errorf("stored data leaks the block: %s", s)
	}

	if err := enc.Forget(ctx, collie.NewTags("supplier", "s1")); err != nil {
		t.Fatal(err)
	}
	redacted := collectEvents(t, store, nil)[0]
	if !encryption.IsRedacted(redacted) {
		t.Fatal("expected redaction after Forget")
	}
	data, err = collie.EventData[supplierRegistered](redacted)
	if err != nil {
		t.Fatal(err)
	}
	if data.Contact != (contact{}) || data.SupplierID != "s1" {
		t.Errorf("redacted payload = %+v, want empty block and plaintext id", data)
	}
}

type (
	badValue struct {
		Name string `json:"name" dcb-encrypt:"user"`
	}
	noSuchTag struct {
		Name string `json:"name" dcb-encrypt:"tag:user"`
	}
	encryptedTag struct {
		UserID string `json:"userId" dcb:"tag:user" dcb-encrypt:"tag:user"`
	}
	hiddenField struct {
		UserID string `json:"userId" dcb:"tag:user"`
		Name   string `json:"-" dcb-encrypt:"tag:user"`
	}
	embeddedBlock struct {
		UserID  string `json:"userId" dcb:"tag:user"`
		contact `dcb-encrypt:"tag:user"`
	}
	unexportedField struct {
		UserID string `json:"userId" dcb:"tag:user"`
		name   string `dcb-encrypt:"tag:user"`
	}
	absentField struct {
		UserID string `json:"userId" dcb:"tag:user"`
		*contactBlock
	}
	contactBlock struct {
		Contact contact `json:"contact" dcb-encrypt:"tag:user"`
	}
	twoSubjects struct {
		UserID   string `json:"userId" dcb:"tag:user"`
		TenantID string `json:"tenantId" dcb:"tag:tenant"`
		Name     string `json:"name" dcb-encrypt:"tag:user"`
		Secret   string `json:"secret" dcb-encrypt:"tag:tenant"`
	}
)

func TestStructTagsRejectInvalidDeclarations(t *testing.T) {
	cases := []struct {
		name  string
		event func() (collie.Event, error)
	}{
		{"value without tag: prefix", func() (collie.Event, error) {
			return collie.NewEvent("X", badValue{Name: "n"}, collie.NewTags("user", "u"))
		}},
		{"key is not a tag on the event", func() (collie.Event, error) {
			return collie.NewEvent("X", noSuchTag{Name: "n"}, nil)
		}},
		{"tag field encrypted", func() (collie.Event, error) {
			return collie.NewEventFrom("X", encryptedTag{UserID: "u"})
		}},
		{`json:"-" field`, func() (collie.Event, error) {
			return collie.NewEventFrom("X", hiddenField{UserID: "u", Name: "n"})
		}},
		{"embedded field declared", func() (collie.Event, error) {
			return collie.NewEventFrom("X", embeddedBlock{UserID: "u", contact: contact{Name: "n"}})
		}},
		{"unexported field declared", func() (collie.Event, error) {
			return collie.NewEventFrom("X", unexportedField{UserID: "u", name: "n"})
		}},
		{"field behind nil embedded pointer", func() (collie.Event, error) {
			return collie.NewEventFrom("X", absentField{UserID: "u"})
		}},
		{"two subjects", func() (collie.Event, error) {
			return collie.NewEventFrom("X", twoSubjects{UserID: "u", TenantID: "t", Name: "n", Secret: "s"})
		}},
		{"option disagrees on subject", func() (collie.Event, error) {
			return collie.NewEvent("X", twoFields{UserID: "u", Name: "n", Iban: "i"},
				collie.NewTags("user", "u", "tenant", "t"), encryption.WithEncryptionKeys("tenant"))
		}},
		{"option disagrees on fields", func() (collie.Event, error) {
			return collie.NewEventFrom("X", twoFields{UserID: "u", Name: "n", Iban: "i"},
				encryption.WithEncryptedFields("name"))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.event()
			if !errors.Is(err, encryption.ErrInvalidStructTags) {
				t.Errorf("err = %v, want ErrInvalidStructTags", err)
			}
		})
	}
}

func TestStructTagsIgnoreUndeclaredPayloads(t *testing.T) {
	e, err := collie.NewEvent("Plain", "alice", collie.NewTags("user", "u"))
	if err != nil {
		t.Fatal(err)
	}
	if e.Metadata != nil {
		t.Errorf("metadata = %v, want none", e.Metadata)
	}
}
