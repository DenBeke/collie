package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	eventstore "github.com/DenBeke/collie"
	"github.com/DenBeke/collie/keyedprojection"
)

// KeyedProjection is an in-memory eventstore.KeyedProjection.
//
// It exists so the read side can be exercised — in tests, in a workshop demo,
// in anything that should not need a database — with the same folds and the
// same semantics the Postgres one has. Where the two must differ, they differ
// loudly: an ordering this one cannot express returns an error rather than a
// silently arbitrary order.
type KeyedProjection[V any] struct {
	query   eventstore.QueryItem
	handler keyedprojection.Handler[V]

	mu       sync.RWMutex
	rows     map[string]*V
	tags     map[string]map[string]string
	position uint64
}

var _ keyedprojection.Projection[any] = (*KeyedProjection[any])(nil)

// NewKeyedProjection builds an empty keyed projection.
func NewKeyedProjection[V any](query eventstore.QueryItem, handler keyedprojection.Handler[V]) *KeyedProjection[V] {
	return &KeyedProjection[V]{
		query:   query,
		handler: handler,
		rows:    map[string]*V{},
		tags:    map[string]map[string]string{},
	}
}

func (p *KeyedProjection[V]) EventQuery() eventstore.QueryItem { return p.query }

func (p *KeyedProjection[V]) After() uint64 {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.position
}

// Checkpoint is a no-op: state and position move together under the lock.
func (p *KeyedProjection[V]) Checkpoint(context.Context) error { return nil }

func (p *KeyedProjection[V]) Apply(_ context.Context, e eventstore.SequencedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if e.SequenceNumber <= p.position {
		return nil
	}
	key, err := p.handler.Key(e)
	if err != nil {
		return fmt.Errorf("could not extract key from %s event: %w", e.Type, err)
	}
	next, err := p.handler.Apply(e, p.rows[key])
	if err != nil {
		return fmt.Errorf("could not apply %s event to row %q: %w", e.Type, key, err)
	}
	if next == nil {
		delete(p.rows, key)
		delete(p.tags, key)
	} else {
		p.rows[key] = next
		if p.tags[key] == nil {
			p.tags[key] = map[string]string{}
		}
		for _, t := range e.Tags {
			if _, seen := p.tags[key][t.Key]; !seen {
				p.tags[key][t.Key] = t.Value
			}
		}
	}
	p.position = e.SequenceNumber
	return nil
}

func (p *KeyedProjection[V]) Get(_ context.Context, key string) (keyedprojection.Entry[V], bool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.rows[key]
	if !ok {
		return keyedprojection.Entry[V]{}, false, nil
	}
	return keyedprojection.Entry[V]{Key: key, Value: *v}, true, nil
}

func (p *KeyedProjection[V]) List(_ context.Context, q keyedprojection.ListQuery) ([]keyedprojection.Entry[V], error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	keys := make([]string, 0, len(p.rows))
	for k := range p.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var out []keyedprojection.Entry[V]
	for _, key := range keys {
		fields, err := fieldsOf(p.rows[key])
		if err != nil {
			return nil, fmt.Errorf("row %q: %w", key, err)
		}
		if !matches(fields, p.tags[key], q) {
			continue
		}
		out = append(out, keyedprojection.Entry[V]{Key: key, Value: *p.rows[key]})
	}

	if err := sortEntries(out, q.Order); err != nil {
		return nil, err
	}
	if q.Offset >= len(out) {
		return nil, nil
	}
	out = out[q.Offset:]
	limit := q.Limit
	if limit <= 0 || limit > DefaultPageSize {
		limit = DefaultPageSize
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// DefaultPageSize matches the Postgres implementation, so a caller sees the
// same paging whichever it is wired to.
const DefaultPageSize = 100

func matches(fields map[string]any, tags map[string]string, q keyedprojection.ListQuery) bool {
	for _, w := range q.Where {
		v, ok := fields[w.Field]
		if !ok || fmt.Sprint(v) != fmt.Sprint(w.Value) {
			return false
		}
	}
	for _, t := range q.Tags {
		if tags[t.Key] != t.Value {
			return false
		}
	}
	return true
}

func sortEntries[V any](out []keyedprojection.Entry[V], order []keyedprojection.SortKey) error {
	if len(order) == 0 {
		return nil // already key-ordered
	}
	// Decode once rather than per comparison.
	decoded := make([]map[string]any, len(out))
	for i := range out {
		f, err := fieldsOf(&out[i].Value)
		if err != nil {
			return err
		}
		decoded[i] = f
	}
	idx := make([]int, len(out))
	for i := range idx {
		idx[i] = i
	}
	var sortErr error
	sort.SliceStable(idx, func(a, b int) bool {
		for _, s := range order {
			va, oka := decoded[idx[a]][s.Field]
			vb, okb := decoded[idx[b]][s.Field]
			if !oka || !okb {
				sortErr = fmt.Errorf("cannot sort on %q: not a field of the folded state", s.Field)
				return false
			}
			c := compare(va, vb)
			if c == 0 {
				continue
			}
			if s.Desc {
				return c > 0
			}
			return c < 0
		}
		// Key is the final tiebreaker, matching the Postgres ordering.
		return out[idx[a]].Key < out[idx[b]].Key
	})
	if sortErr != nil {
		return sortErr
	}
	sorted := make([]keyedprojection.Entry[V], len(out))
	for i, j := range idx {
		sorted[i] = out[j]
	}
	copy(out, sorted)
	return nil
}

func compare(a, b any) int {
	if fa, ok := a.(float64); ok {
		if fb, ok := b.(float64); ok {
			switch {
			case fa < fb:
				return -1
			case fa > fb:
				return 1
			default:
				return 0
			}
		}
	}
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// fieldsOf renders a folded value as the field map filters and ordering work
// against.
//
// It goes through JSON so that a field means exactly what it means in the
// Postgres implementation, where the value is jsonb and `value->>'status'`
// reads the json name. Matching on Go field names instead would make the same
// query mean different things depending on which store you wired up.
func fieldsOf[V any](v *V) (map[string]any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("could not encode value: %w", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		// A scalar state has no fields to filter on, which is not an error —
		// it simply matches nothing.
		return map[string]any{}, nil
	}
	return m, nil
}
