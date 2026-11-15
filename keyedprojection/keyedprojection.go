// Package keyedprojection provides projections whose state is one value per
// key, and which can therefore be read from.
//
// It is the third member of the projection family, and the only one you can
// point at a single value of:
//
//	eventstore.Projection            a fold. One state, for as long as its owner needs it.
//	eventstore.CheckpointProjection  ...that can resume, so it need not replay from zero.
//	keyedprojection.Projection[V]    ...whose state is many values, one per key derived
//	                                 from each event — so Get and List mean something.
//
// It lives in its own package rather than beside the other two because the read
// vocabulary it needs — ListQuery, FieldEquals, SortKey — would otherwise sit
// in the root package next to eventstore.Query, which selects events from the
// log rather than rows from a projection. Two unrelated query vocabularies in
// one package is worse than one import.
package keyedprojection

import (
	"context"

	eventstore "github.com/DenBeke/collie"
)

// Entry is one keyed value, with the key it is filed under.
//
// The key is returned because a value routinely does not contain it: a job's
// folded state has a title and a status and no jobId, so [] of the value alone
// is a result the caller cannot address, link to, or fetch the detail of.
type Entry[V any] struct {
	Key   string
	Value V
}

// FieldEquals matches rows whose named field holds Value.
//
// Field names a property of the folded state, not a tag. Predicates are values
// rather than fragments of SQL so that a backend can honour them however it
// likes, and so that nothing a caller supplies is ever concatenated into a
// query.
type FieldEquals struct {
	Field string
	Value any
}

// SortKey orders results by one field of the folded state, named as it is in
// the value's JSON.
//
// The order is the same in every backend: numbers first, by value; then
// everything else by its text, byte by byte, so "Zoe" sorts before "adam"; and
// last the rows whose field is null. Desc reverses all of it.
type SortKey struct {
	Field string
	Desc  bool
}

// ListQuery describes a page of a keyed projection.
//
// Semantics, not storage: a backend that cannot honour an ordering must say so
// with an error rather than quietly return an arbitrary order.
type ListQuery struct {
	// Where is ANDed. An empty Where matches every key.
	Where []FieldEquals
	// Tags filters on the tags carried by the events folded into a row.
	Tags []eventstore.Tag
	// Order is applied in sequence. Backends append the key as a final
	// tiebreaker so a page boundary is stable.
	Order []SortKey
	// Limit bounds the page. Zero means the backend's default.
	Limit int
	// Offset skips rows. Cursor-based paging is not modelled yet; an offset
	// that is honest about its weakness beats a cursor that silently skips.
	Offset int
}

// Projection is a projection whose state is one value per key.
//
// Implementations fold through Apply exactly like any other projection — the
// interface embeds CheckpointProjection, so UpdateProjection drives it
// unchanged — and additionally serve what they have folded.
type Projection[V any] interface {
	eventstore.CheckpointProjection

	// Get loads one value by key. found is false when there is no such key.
	Get(ctx context.Context, key string) (Entry[V], bool, error)

	// List returns the entries matching q, key-ordered when q.Order is empty.
	List(ctx context.Context, q ListQuery) ([]Entry[V], error)
}

// Handler routes events into keyed values.
//
// Key says which value an event belongs to. Apply folds it in: a nil current
// value is a key that does not exist yet, and returning nil deletes it, after
// which the next event for that key starts from nothing again.
type Handler[V any] interface {
	Key(eventstore.SequencedEvent) (string, error)
	Apply(eventstore.SequencedEvent, *V) (*V, error)
}
