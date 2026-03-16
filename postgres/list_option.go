package postgres

import "fmt"

// ListOption configures a [RowProjection.List] query.
type ListOption func(*listQuery)

type listQuery struct {
	matchTags  []matchTag
	orderBy    *orderBy
	pagination *pagination
}

type matchTag struct {
	key    string
	values []string
}

type orderBy struct {
	key  string
	desc bool
}

type pagination struct {
	offset int
	limit  int
}

// WithMatchTag filters rows by a tag value. A single value requires an exact
// match; multiple values match any (IN). Multiple WithMatchTag options are
// combined with AND logic.
func WithMatchTag(key string, values ...string) ListOption {
	return func(q *listQuery) {
		q.matchTags = append(q.matchTags, matchTag{key: key, values: values})
	}
}

// WithOrderBy sets the sort order on a JSONB value field.
func WithOrderBy(jsonKey string, desc bool) ListOption {
	return func(q *listQuery) {
		q.orderBy = &orderBy{key: jsonKey, desc: desc}
	}
}

// WithPagination sets the offset and limit for the query.
func WithPagination(offset, limit int) ListOption {
	return func(q *listQuery) {
		q.pagination = &pagination{offset: offset, limit: limit}
	}
}

func buildListQuery(opts []ListOption) listQuery {
	var q listQuery
	for _, opt := range opts {
		opt(&q)
	}
	return q
}

// whereClause builds the WHERE fragment and returns the SQL and arguments.
func (q *listQuery) whereClause(projectionName string) (string, []any) {
	sql := "projection_name = ?"
	args := []any{projectionName}

	for _, m := range q.matchTags {
		switch len(m.values) {
		case 0:
			continue
		case 1:
			sql += fmt.Sprintf(" AND tags->>'%s' = ?", m.key)
			args = append(args, m.values[0])
		default:
			sql += fmt.Sprintf(" AND tags->>'%s' IN ?", m.key)
			args = append(args, m.values)
		}
	}

	return sql, args
}
