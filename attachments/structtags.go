package attachments

import (
	"errors"
	"fmt"
	"strings"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/internal/structtag"
)

// TagKey is the struct-tag key this package reads. A payload field tagged
//
//	Content []byte `json:"content" dcb-attachment:"blob"`
//
// is offloaded to the blob store, exactly as WithAttachments("content") would
// declare. The declaration lives on the field, so no constructor can forget it.
const TagKey = "dcb-attachment"

// ErrInvalidStructTags wraps every error the struct-tag hook finds at
// construction: a value other than "blob", a declaration on an embedded,
// unexported or absent field, a field that is itself a tag or is left out of
// Data, or a declaration that contradicts an option on the event.
var ErrInvalidStructTags = errors.New("invalid dcb-attachment struct tags")

// RegisterStructTags makes every event built from now on honour the TagKey
// declarations on its payload. Call once at boot, before any event is built.
func RegisterStructTags() {
	eventstore.RegisterHook(resolveStructTags)
}

// resolveStructTags is the hook: it turns TagKey declarations into the
// metadata OnAppend reads, filling what is absent and verifying what an option
// already set.
func resolveStructTags(e *eventstore.Event, payload any) error {
	var fields []string
	for f, v := range structtag.Fields(payload) {
		raw, ok := f.Tag.Lookup(TagKey)
		if !ok {
			continue
		}
		fail := func(format string, args ...any) error {
			return fmt.Errorf("%w: %T.%s: %s", ErrInvalidStructTags, payload, f.Name, fmt.Sprintf(format, args...))
		}
		name, inData := structtag.JSONName(f)
		_, isDCBTag := f.Tag.Lookup("dcb")
		switch {
		case raw != "blob":
			return fail("want %s:\"blob\", got %q", TagKey, raw)
		case f.Anonymous:
			return fail("declaration on an embedded field, whose fields JSON flattens into the parent; tag the embedded fields instead")
		case !f.IsExported():
			return fail("declaration on an unexported field, which JSON never encodes")
		case !v.IsValid():
			return fail("field sits behind a nil embedded pointer and has no value")
		case isDCBTag:
			return fail("a tag field cannot be an attachment")
		case !inData:
			return fail("field is json:\"-\" and so has nothing in Data to offload")
		}
		fields = append(fields, name)
	}
	if len(fields) == 0 {
		return nil
	}
	value := strings.Join(fields, ",")
	if e.Metadata == nil {
		e.Metadata = make(map[string]string)
	}
	if existing, ok := e.Metadata[FieldsMetadataKey]; ok && existing != value {
		return fmt.Errorf("%w: struct declares %s=%q but an option set %q", ErrInvalidStructTags, FieldsMetadataKey, value, existing)
	}
	e.Metadata[FieldsMetadataKey] = value
	return nil
}
