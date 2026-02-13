# eventstore

A minimal Go library for event sourcing with [Dynamic Consistency Boundaries](https://dcb.events/specification/). It provides an event store interface, typed projections, and optimistic concurrency control — everything needed to enforce business rules on the write side and serve read queries on the read side.

## Package structure

```
eventstore/
  eventstore.go        — Store interface, AppendCondition
  event.go             — Event, SequencedEvent, NewEvent, EventData
  query.go             — Query, QueryItem
  projection.go        — Projection[S], HandleFunc, Projector interface
  decision.go          — BuildDecisionModel
  readprojector.go     — CheckpointProjector interface, UpdateProjection
  memory/
    memory.go          — In-memory Store
    projection.go      — In-memory CheckpointProjector (memory.Projection[S])
  postgres/
    postgres.go        — PostgreSQL Store
    projection_models.go — GORM models, MigrateProjections
    projection.go      — Snapshot CheckpointProjector (postgres.Projection[S])
    row_projection.go  — Row-based CheckpointProjector (postgres.RowProjection[V])
```

## Core concepts

### Events

An event is a fact that happened. It has a type, a JSON-serializable data payload, and optional tags for filtering:

```go
event, _ := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
    "username": "alice",
})
```

Once appended to the store, events become `SequencedEvent`s with a monotonically increasing sequence number and a timestamp.

### Tags and queries

Tags are key-value metadata attached to events. A `QueryItem` filters events by type and/or tags:

```go
query := eventstore.QueryItem{
    Types: []string{"UserSignedUp"},               // match these event types (OR)
    Tags:  map[string]string{"username": "alice"},  // require these tags (AND)
}
```

Multiple `QueryItem`s combine into a `Query` with OR logic.

### Projections

A `Projection[S]` reduces matching events into a typed state. It declares which events to read (via a query) and how to handle each event type:

```go
exists := eventstore.Projection[bool]{
    Query: eventstore.QueryItem{
        Types: []string{"UserSignedUp"},
        Tags:  map[string]string{"username": "alice"},
    },
    State: false,
    Handlers: map[string]eventstore.HandleFunc[bool]{
        "UserSignedUp": func(_ bool, _ eventstore.SequencedEvent) (bool, error) {
            return true, nil
        },
    },
}
```

### Decision model and AppendCondition

`BuildDecisionModel` reads events for one or more projections in a single pass, populates their state, and returns an `AppendCondition`. The append condition captures *which events were read* and *up to what sequence number* — if someone else appends a conflicting event between the read and your write, the append fails with `ErrAppendConditionViolated`. This is optimistic concurrency control:

```go
condition, _ := eventstore.BuildDecisionModel(ctx, store, &exists)

if exists.State {
    // business rule: username already taken
}

// The condition ensures no new UserSignedUp event for "alice"
// was appended since we checked.
store.Append(ctx, []eventstore.Event{event}, condition)
```

Decision model projections are short-lived — created per command, checked, then discarded.

### CheckpointProjector

A `CheckpointProjector` is a long-lived projection that tracks its position in the event stream. It catches up incrementally on demand via `UpdateProjection`, making it suitable for serving read queries.

The interface has three methods:

```go
type CheckpointProjector interface {
    EventQuery() QueryItem
    After() int64
    Apply(context.Context, SequencedEvent) error
}
```

Three implementations are provided:

| Implementation | Storage | Use case |
|----------------|---------|----------|
| `memory.Projection[S]` | In-memory | Testing, ephemeral state |
| `postgres.Projection[S]` | Snapshot to DB | Full state persisted, survives restarts |
| `postgres.RowProjection[V]` | Row per entity in DB | Large projections where full state doesn't fit in memory |

#### In-memory projection

Wraps a `Projection[S]` with position tracking. State is lost on restart.

```go
totalUsers := memory.NewProjection(eventstore.Projection[int]{
    Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
    State: 0,
    Handlers: map[string]eventstore.HandleFunc[int]{
        "UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
            return count + 1, nil
        },
    },
})

eventstore.UpdateProjection(ctx, store, totalUsers)
fmt.Println(totalUsers.State)
```

#### Postgres snapshot projection

Wraps a `Projection[S]` and persists the full state and position to the database on every applied event. Loads existing state on construction, surviving restarts.

```go
totalUsers, err := postgres.NewProjection(db, "total_users", eventstore.Projection[int]{
    Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
    State: 0,
    Handlers: map[string]eventstore.HandleFunc[int]{
        "UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
            return count + 1, nil
        },
    },
})

eventstore.UpdateProjection(ctx, store, totalUsers)
fmt.Println(totalUsers.State)
```

#### Postgres row projection

Stores one row per entity. Each event loads the affected row, applies a handler, and writes it back in a single transaction. Does not keep the full projection in memory.

Implement the `RowHandler[V]` interface:

```go
type UserProfile struct {
    Name  string
    Email string
}

type userProfileHandler struct{}

func (h userProfileHandler) Key(e eventstore.SequencedEvent) (string, error) {
    return e.Tags["user_id"], nil
}

func (h userProfileHandler) Apply(e eventstore.SequencedEvent, current *UserProfile) (*UserProfile, error) {
    switch e.Type {
    case "UserSignedUp":
        var data struct{ Name, Email string }
        json.Unmarshal(e.Data, &data)
        return &UserProfile{Name: data.Name, Email: data.Email}, nil
    case "UserEmailChanged":
        var data struct{ Email string }
        json.Unmarshal(e.Data, &data)
        current.Email = data.Email
        return current, nil
    case "UserDeleted":
        return nil, nil // nil deletes the row
    }
    return current, nil
}
```

Then create the projection:

```go
profiles, err := postgres.NewRowProjection(db, "user_profiles",
    eventstore.QueryItem{Types: []string{"UserSignedUp", "UserEmailChanged", "UserDeleted"}},
    userProfileHandler{},
)

eventstore.UpdateProjection(ctx, store, profiles)

// Retrieve a single row.
profile, err := profiles.Get(ctx, "user-123")
```

## Example: enforcing uniqueness on signup

```go
func signUp(ctx context.Context, store eventstore.Store, username string) error {
    // 1. Build the decision model — checks if username is taken.
    exists := eventstore.Projection[bool]{
        Query: eventstore.QueryItem{
            Types: []string{"UserSignedUp"},
            Tags:  map[string]string{"username": username},
        },
        State: false,
        Handlers: map[string]eventstore.HandleFunc[bool]{
            "UserSignedUp": func(_ bool, _ eventstore.SequencedEvent) (bool, error) {
                return true, nil
            },
        },
    }
    condition, err := eventstore.BuildDecisionModel(ctx, store, &exists)
    if err != nil {
        return err
    }

    // 2. Enforce the business rule.
    if exists.State {
        return fmt.Errorf("username %q is already taken", username)
    }

    // 3. Append the event with the condition — fails if someone else
    //    signed up with the same username since we checked.
    event, err := eventstore.NewEvent("UserSignedUp", username, map[string]string{
        "username": username,
    })
    if err != nil {
        return err
    }
    return store.Append(ctx, []eventstore.Event{event}, condition)
}
```

## Example: serving a read query

```go
store := &memory.Store{}
totalUsers := memory.NewProjection(/* ... */)

e := echo.New()

e.GET("/total-users", func(c echo.Context) error {
    err := eventstore.UpdateProjection(c.Request().Context(), store, totalUsers)
    if err != nil {
        return err
    }
    return c.JSON(200, map[string]int{"total_users": totalUsers.State})
})
```

## Store implementations

| Package | Description |
|---------|-------------|
| `eventstore/memory` | In-memory store for testing and development |
| `eventstore/postgres` | PostgreSQL store with advisory locking, batched reads, and B-tree/GIN tag indexing |

```go
// In-memory
var store eventstore.Store = &memory.Store{}

// PostgreSQL
db, _ := gorm.Open(postgres.Open(connStr))
pgStore := pgstore.New(db, pgstore.WithIndexedTags("tenant_id"))
pgStore.Migrate()
```

The `Store` interface has two methods: `Read` and `Append`. Implement it to back the event store with any storage engine.

### PostgreSQL projection tables

Call `MigrateProjections` to create the tables used by `postgres.Projection[S]` and `postgres.RowProjection[V]`:

```go
postgres.MigrateProjections(db)
```

This creates three tables: `projection_checkpoints` (shared position tracking), `projection_snapshots` (snapshot state), and `projection_rows` (per-entity rows), with foreign key constraints from snapshots/rows to checkpoints.
