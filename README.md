# eventstore

A minimal Go library for event sourcing with [Dynamic Consistency Boundaries](https://dcb.events/specification/). It provides an event store interface, typed projections, and optimistic concurrency control — everything needed to enforce business rules on the write side and serve read queries on the read side.

## Package structure

```
eventstore/
  eventstore.go    — Store interface, AppendCondition, core query types
  event.go         — Event, SequencedEvent, NewEvent, EventData helpers
  projection.go    — Projection[S], HandleFunc
  decision.go      — BuildDecisionModel
  readmodel.go     — CheckpointProjector interface, UpdateProjection
  memory/
    memory.go      — In-memory Store implementation
    projection.go  — In-memory CheckpointProjector implementation
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
    Types: []string{"UserSignedUp"},          // match these event types (OR)
    Tags:  map[string]string{"username": "alice"}, // require these tags (AND)
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

### Read model

A `CheckpointProjector` is a long-lived projection that tracks its position in the event stream. It catches up incrementally on demand via `UpdateProjection`, making it suitable for serving GET endpoints.

The `memory.Projection[S]` wraps an `eventstore.Projection[S]` with position tracking:

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

// Each call only processes events since the last update.
eventstore.UpdateProjection(ctx, store, totalUsers)
fmt.Println(totalUsers.State) // 42
```

The read model is decoupled from the write side — no need to notify it after each command. It catches up when queried.

Implement the `CheckpointProjector` interface to use a different storage strategy (e.g. database snapshots, per-row storage).

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
    // Catch up with new events since the last query.
    if err := eventstore.UpdateProjection(c.Request().Context(), store, totalUsers); err != nil {
        return err
    }
    return c.JSON(200, map[string]int{"total_users": totalUsers.State})
})
```

## Store implementations

| Package | Description |
|---------|-------------|
| `eventstore/memory` | In-memory store for testing and development |
| `eventstore/postgres` | PostgreSQL store (planned) |

```go
var store eventstore.Store = &memory.Store{}
```

The `Store` interface has two methods: `Read` and `Append`. Implement it to back the event store with any storage engine.
