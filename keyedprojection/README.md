# Keyed projections

A keyed projection folds every event of its types and routes each one to a value
by key, so unlike the other two you can address a single value and list across
them. It is what a read endpoint reads.

`keyedprojection.Projection[V]` is the interface; `postgres.KeyedProjection[V]`
and `memory.KeyedProjection[V]` implement it, so the same handler and the same
queries work with or without a database.

Implement `keyedprojection.Handler[V]`:

```go
type UserProfile struct {
    Name   string `json:"name"`
    Email  string `json:"email"`
    Status string `json:"status"`
}

type userProfileHandler struct{}

func (userProfileHandler) Key(e collie.SequencedEvent) (string, error) {
    ids := e.TagValues("userId")
    if len(ids) == 0 {
        return "", fmt.Errorf("%s carries no userId tag", e.Type)
    }
    return ids[0], nil
}

func (userProfileHandler) Apply(e collie.SequencedEvent, current *UserProfile) (*UserProfile, error) {
    switch e.Type {
    case "UserSignedUp":
        d, err := collie.EventData[UserProfile](e)
        if err != nil {
            return nil, err
        }
        return &d, nil
    case "UserEmailChanged":
        d, err := collie.EventData[UserProfile](e)
        if err != nil {
            return nil, err
        }
        current.Email = d.Email
        return current, nil
    case "UserDeleted":
        return nil, nil // nil deletes the value
    }
    return current, nil
}
```

Then create it and read from it:

```go
profiles, err := postgres.NewKeyedProjection[UserProfile](db, "user_profiles",
    collie.QueryItem{Types: []string{"UserSignedUp", "UserEmailChanged", "UserDeleted"}},
    userProfileHandler{},
)

// Catch up before serving. Read models have no subscriptions: a read is what
// pulls the projection forward — only reactors watch the log on their
// own schedule. Safe to call concurrently — Apply skips an event it has
// already applied, so racing catch-ups converge instead of double-counting.
collie.UpdateProjection(ctx, store, profiles)

// One value by key.
entry, found, err := profiles.Get(ctx, "user-123")
fmt.Println(entry.Key, entry.Value.Name)

// A page. Entries carry their key, because folded state usually does not
// contain its own id.
page, err := profiles.List(ctx, keyedprojection.ListQuery{
    Where: []keyedprojection.FieldEquals{{Field: "status", Value: "active"}},
    Tags:  []collie.Tag{collie.NewTag("tenant", "acme")},
    Order: []keyedprojection.SortKey{{Field: "name"}},
    Limit: 20,
})
```

`Where` filters on fields of the folded value; `Tags` filters on the tags of the
events folded into it, accumulated across all of them. Field names are values,
never fragments of SQL, and anything that is not a plain identifier is refused —
so a `?sort=` parameter can be passed through safely. `Order` sorts numbers by
value and text byte by byte, the same in both implementations.

For anything you filter or sort on regularly, add the indexes:

```go
profiles.MigrateFieldIndexes("status", "name")
profiles.MigrateTagIndexes("tenant")
```
