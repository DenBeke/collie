package eventstore

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

// ErrInvalidTagStruct wraps every error found in a payload type handed to
// NewEventFrom: a malformed `dcb` tag, a field of the wrong kind, a tag on an
// unexported or embedded field, `json:"-"` on a tag field, or a struct with no
// tag fields at all.
var ErrInvalidTagStruct = errors.New("invalid tag struct")

// ErrEmptyTagValue is returned by NewEventFrom when a tag field is "" (or an
// empty slice, or a slice holding an empty string) and the field is not marked
// omitempty. A tag with an empty value would be a consistency boundary over
// nothing, which is almost always a value that was never set.
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
	rv := reflect.Indirect(reflect.ValueOf(payload))
	if rv.Kind() != reflect.Struct {
		return Event{}, fmt.Errorf("%w: payload %T is not a struct", ErrInvalidTagStruct, payload)
	}
	fields, err := tagFields(rv.Type())
	if err != nil {
		return Event{}, err
	}
	tags, err := tagsFrom(fields, rv)
	if err != nil {
		return Event{}, err
	}
	return NewEvent(eventType, payload, tags, opts...)
}

// tagFields walks the visible fields of rt (promoted fields included) and
// collects the ones carrying a `dcb` tag.
func tagFields(rt reflect.Type) ([]tagField, error) {
	var fields []tagField
	for _, f := range reflect.VisibleFields(rt) {
		raw, ok := f.Tag.Lookup("dcb")
		if !ok {
			continue
		}
		field, err := parseTagField(f, raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %s.%s: %w", ErrInvalidTagStruct, rt, f.Name, err)
		}
		fields = append(fields, field)
	}
	if len(fields) == 0 {
		return nil, fmt.Errorf("%w: %s has no dcb tag fields (use NewEvent for untagged payloads)", ErrInvalidTagStruct, rt)
	}
	return fields, nil
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

// tagsFrom emits the tags of one struct value, in field declaration order.
func tagsFrom(fields []tagField, rv reflect.Value) ([]Tag, error) {
	var tags []Tag
	for _, f := range fields {
		values, err := fieldValues(f, rv)
		if err != nil {
			return nil, err
		}
		if len(values) == 0 && !f.omitEmpty {
			return nil, fmt.Errorf("%w: field %s", ErrEmptyTagValue, f.Name)
		}
		for _, value := range values {
			for _, key := range f.keys {
				tags = append(tags, Tag{Key: key, Value: value})
			}
		}
	}
	return tags, nil
}

// fieldValues reads the non-empty values of one tag field. A field behind a nil
// embedded pointer, an empty string and an empty slice all read as no values;
// an empty string inside a slice is an error, since omitempty cannot make a
// positional hole in a list meaningful.
func fieldValues(f tagField, rv reflect.Value) ([]string, error) {
	fv, err := rv.FieldByIndexErr(f.Index)
	if err != nil {
		return nil, nil
	}
	if fv.Kind() != reflect.Slice {
		if s := fv.String(); s != "" {
			return []string{s}, nil
		}
		return nil, nil
	}
	values := make([]string, 0, fv.Len())
	for i := range fv.Len() {
		s := fv.Index(i).String()
		if s == "" {
			return nil, fmt.Errorf("%w: field %s element %d", ErrEmptyTagValue, f.Name, i)
		}
		values = append(values, s)
	}
	return values, nil
}
