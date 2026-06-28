package eventstore

import "slices"

// Query is a list of QueryItems combined with OR logic. An event matches if it
// satisfies at least one item. A nil/empty Query matches all events.
type Query []QueryItem

// Matches reports whether the event satisfies at least one item in the query.
// A nil/empty Query matches all events.
func (q Query) Matches(e SequencedEvent) bool {
	if len(q) == 0 {
		return true
	}
	for _, item := range q {
		if item.Matches(e) {
			return true
		}
	}
	return false
}

// QueryItem matches events whose type is in Types (OR) and which carry every tag
// in Tags (AND). An empty Types matches all types. An empty Tags imposes no tag
// constraint. Listing the same key twice in Tags requires the event to carry
// both values.
type QueryItem struct {
	Types []string
	Tags  []Tag
}

// Matches reports whether the event satisfies this query item.
func (q QueryItem) Matches(e SequencedEvent) bool {
	if len(q.Types) > 0 && !slices.Contains(q.Types, e.Type) {
		return false
	}
	for _, t := range q.Tags {
		if !slices.Contains(e.Tags, t) {
			return false
		}
	}
	return true
}
