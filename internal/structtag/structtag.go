// Package structtag is what the root package and the plugins share for reading
// struct tags off an event payload: the walk over a payload's fields and the
// JSON key a field is encoded under. It decides nothing about what is a
// mistake; each caller rejects what it rejects.
package structtag

import (
	"iter"
	"reflect"
	"strings"
)

// Fields yields every visible field of a struct, or of a non-nil pointer to
// one, with its value: exported and unexported, embedded structs themselves and
// the fields promoted from them. A field promoted through a nil embedded
// pointer is yielded with an invalid Value, so a caller can report a
// declaration that has no value behind it. Any other payload yields nothing.
func Fields(payload any) iter.Seq2[reflect.StructField, reflect.Value] {
	return func(yield func(reflect.StructField, reflect.Value) bool) {
		rv := reflect.Indirect(reflect.ValueOf(payload))
		if rv.Kind() != reflect.Struct {
			return
		}
		for _, f := range reflect.VisibleFields(rv.Type()) {
			fv, err := rv.FieldByIndexErr(f.Index)
			if err != nil {
				fv = reflect.Value{}
			}
			if !yield(f, fv) {
				return
			}
		}
	}
}

// JSONName returns the top-level JSON key encoding/json uses for a field: the
// name from its json tag, else the Go name. The second result is false for a
// field json leaves out (`json:"-"`).
func JSONName(f reflect.StructField) (string, bool) {
	tag, ok := f.Tag.Lookup("json")
	if !ok {
		return f.Name, true
	}
	if tag == "-" {
		return "", false
	}
	if name, _, _ := strings.Cut(tag, ","); name != "" {
		return name, true
	}
	return f.Name, true
}
