package encryption

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/internal/structtag"
)

// TagKey is the struct-tag key this package reads. A payload field tagged
//
//	Name string `json:"name" dcb-encrypt:"tag:user"`
//
// is encrypted under the key of the data subject the event's "user" tag names,
// exactly as WithEncryptionKeys("user") plus WithEncryptedFields("name") would
// declare. The declaration lives on the field, so no constructor can forget it.
// Group several personal fields in a nested struct and tag that one field: the
// block is then one ciphertext and redacts to one null.
const TagKey = "dcb-encrypt"

// ErrInvalidStructTags wraps every error the struct-tag hook finds at
// construction: a malformed value, a key that is not a tag on the event, a
// declaration on an embedded, unexported or absent field, a field that is
// itself a tag or is left out of Data, fields that disagree on the subject, or
// a declaration that contradicts an option on the same event.
var ErrInvalidStructTags = errors.New("invalid dcb-encrypt struct tags")

// RegisterStructTags makes every event built from now on honour the TagKey
// declarations on its payload. Call once at boot, before any event is built.
func RegisterStructTags() {
	eventstore.RegisterHook(resolveStructTags)
}

// resolveStructTags is the hook: it turns TagKey declarations into the
// metadata OnAppend reads, filling what is absent and verifying what an option
// already set.
func resolveStructTags(e *eventstore.Event, payload any) error {
	var subject string
	var fields []string
	for f, v := range structtag.Fields(payload) {
		raw, ok := f.Tag.Lookup(TagKey)
		if !ok {
			continue
		}
		fail := func(format string, args ...any) error {
			return fmt.Errorf("%w: %T.%s: %s", ErrInvalidStructTags, payload, f.Name, fmt.Sprintf(format, args...))
		}
		key, isTag := strings.CutPrefix(raw, "tag:")
		name, inData := structtag.JSONName(f)
		_, isDCBTag := f.Tag.Lookup("dcb")
		switch {
		case !isTag || key == "":
			return fail("want %s:\"tag:<key>\", got %q", TagKey, raw)
		case f.Anonymous:
			return fail("declaration on an embedded field, whose fields JSON flattens into the parent; tag the embedded fields instead")
		case !f.IsExported():
			return fail("declaration on an unexported field, which JSON never encodes")
		case !v.IsValid():
			return fail("field sits behind a nil embedded pointer and has no value")
		case isDCBTag:
			return fail("a tag field cannot be encrypted: tag values are plaintext")
		case !inData:
			return fail("field is json:\"-\" and so has nothing in Data to encrypt")
		case !slices.ContainsFunc(e.Tags, func(t eventstore.Tag) bool { return t.Key == key }):
			return fail("the event carries no %q tag", key)
		case subject != "" && key != subject:
			return fail("fields disagree on the subject: %q and %q", subject, key)
		}
		subject = key
		fields = append(fields, name)
	}
	if len(fields) == 0 {
		return nil
	}
	if err := declare(e, MetadataKey, subject); err != nil {
		return err
	}
	return declare(e, FieldsMetadataKey, strings.Join(fields, ","))
}

// declare writes one metadata entry unless an option already wrote it, in
// which case the two must agree.
func declare(e *eventstore.Event, key, value string) error {
	if e.Metadata == nil {
		e.Metadata = make(map[string]string)
	}
	if existing, ok := e.Metadata[key]; ok && existing != value {
		return fmt.Errorf("%w: struct declares %s=%q but an option set %q", ErrInvalidStructTags, key, value, existing)
	}
	e.Metadata[key] = value
	return nil
}
