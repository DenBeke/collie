package collie

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/DenBeke/collie/internal/structtag"
)

// ErrInvalidTagStruct wraps every error found in a payload handed to
// NewEventFrom: a malformed `dcb` tag, a field of the wrong kind, a tag on an
// unexported or embedded field, `json:"-"` on a tag field, or a payload with no
// tag fields at all.
var ErrInvalidTagStruct = errors.New("invalid tag struct")

// ErrEmptyTagValue is returned by NewEventFrom when a tag field is "" (or an
// empty slice, or a slice holding an empty string, or sits behind a nil
// embedded pointer) and the field is not marked omitempty. A tag with an empty
// value would be a consistency boundary over nothing, which is almost always a
// value that was never set.
var ErrEmptyTagValue = errors.New("empty tag value")

// tagField is one struct field that emits tags.
type tagField struct {
	reflect.StructField
	keys      []string // one or more tag keys, in the order written
	omitEmpty bool     // an empty field emits nothing instead of failing
}

// NewEventFrom builds an Event like NewEvent, taking the tags from the
// payload's struct fields instead of a []Tag argument. A field tagged
//
//	dcb:"tag:<key>[,tag:<key>...][,omitempty]"
//
// emits one Tag per value under each key, in field declaration order. The
// field is still an ordinary payload field: it is JSON-encoded into Data as
// usual and the tag is a copy of it, so a reader can take the id from either.
//
//	type MessageSent struct {
//	    From string `json:"from" dcb:"tag:user"`
//	    To   string `json:"to"   dcb:"tag:user"`
//	    Body string `json:"body"`
//	}
//
// yields the tags user=<from>, user=<to>. A field of string kind (including a
// named string type) emits one tag per key; a slice of string kind emits one
// tag per element per key. Fields promoted from embedded structs count. An
// empty value is ErrEmptyTagValue unless the field is marked omitempty; an
// empty string inside a slice is always an error.
//
// The payload must be a struct or a non-nil pointer to one, with at least one
// tag field; use NewEvent for payloads that carry their tags separately.
// Anything else is ErrInvalidTagStruct.
func NewEventFrom(eventType string, payload any, opts ...EventOption) (Event, error) {
	var tags []Tag
	tagged := false
	for f, v := range structtag.Fields(payload) {
		raw, ok := f.Tag.Lookup("dcb")
		if !ok {
			continue
		}
		tagged = true
		field, err := parseTagField(f, raw)
		if err != nil {
			return Event{}, fmt.Errorf("%w: %T.%s: %w", ErrInvalidTagStruct, payload, f.Name, err)
		}
		values, err := fieldValues(field, v)
		if err != nil {
			return Event{}, err
		}
		if len(values) == 0 && !field.omitEmpty {
			return Event{}, fmt.Errorf("%w: field %s", ErrEmptyTagValue, f.Name)
		}
		for _, value := range values {
			for _, key := range field.keys {
				tags = append(tags, Tag{Key: key, Value: value})
			}
		}
	}
	if !tagged {
		return Event{}, fmt.Errorf("%w: %T has no dcb tag fields (use NewEvent for untagged payloads)", ErrInvalidTagStruct, payload)
	}
	return NewEvent(eventType, payload, tags, opts...)
}

// parseTagField validates one `dcb`-tagged field and parses its tag.
func parseTagField(f reflect.StructField, raw string) (tagField, error) {
	field := tagField{StructField: f}
	switch {
	case f.Anonymous:
		return field, errors.New("dcb tag on an embedded field; tag its fields instead")
	case !f.IsExported():
		return field, errors.New("dcb tag on an unexported field")
	case f.Tag.Get("json") == "-":
		return field, errors.New(`dcb tag field cannot be json:"-"; tag fields are part of the payload`)
	case f.Type.Kind() == reflect.String:
	case f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.String:
	default:
		return field, fmt.Errorf("field type %s, want string or []string", f.Type)
	}
	for token := range strings.SplitSeq(raw, ",") {
		key, isTag := strings.CutPrefix(token, "tag:")
		switch {
		case token == "omitempty":
			field.omitEmpty = true
		case !isTag:
			return field, fmt.Errorf("unknown option %q in dcb:%q", token, raw)
		case key == "":
			return field, fmt.Errorf("empty tag key in dcb:%q", raw)
		case slices.Contains(field.keys, key):
			return field, fmt.Errorf("tag key %q repeated in dcb:%q", key, raw)
		default:
			field.keys = append(field.keys, key)
		}
	}
	if len(field.keys) == 0 {
		return field, fmt.Errorf("no tag key in dcb:%q", raw)
	}
	return field, nil
}

// fieldValues reads the non-empty values of one tag field. A field with no
// value behind it (nil embedded pointer), an empty string and an empty slice
// all read as no values; an empty string inside a slice is an error, since
// omitempty cannot make a positional hole in a list meaningful.
func fieldValues(f tagField, v reflect.Value) ([]string, error) {
	if !v.IsValid() {
		return nil, nil
	}
	if v.Kind() != reflect.Slice {
		if s := v.String(); s != "" {
			return []string{s}, nil
		}
		return nil, nil
	}
	values := make([]string, 0, v.Len())
	for i := range v.Len() {
		s := v.Index(i).String()
		if s == "" {
			return nil, fmt.Errorf("%w: field %s element %d", ErrEmptyTagValue, f.Name, i)
		}
		values = append(values, s)
	}
	return values, nil
}
