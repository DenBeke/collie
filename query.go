package eventstore

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

// QueryItem matches events whose type is in Types (OR) and whose tags contain
// every entry in Tags (AND). An empty Types matches all types. An empty Tags
// imposes no tag constraint.
type QueryItem struct {
	Types []string
	Tags  map[string]string
}

// Matches reports whether the event satisfies this query item.
func (q QueryItem) Matches(e SequencedEvent) bool {
	if len(q.Types) > 0 {
		found := false
		for _, t := range q.Types {
			if e.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	for k, v := range q.Tags {
		if e.Tags[k] != v {
			return false
		}
	}
	return true
}
