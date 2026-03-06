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
  encryption/
    encryption.go      — EventEncryptor, ErrRedacted
    keystore.go        — KeyStore interface
    memory.go          — In-memory KeyStore for testing
  memory/
    memory.go          — In-memory Store
    projection.go      — In-memory CheckpointProjector (memory.Projection[S])
  postgres/
    postgres.go        — PostgreSQL Store
    keystore.go        — PostgresKeyStore, MigrateKeyStore
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
| `eventstore/encryption` | GDPR crypto-shredding: per-subject encryption with key deletion |

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

If using encryption, also migrate the key store table:

```go
postgres.MigrateKeyStore(db)
```

## GDPR / Crypto-shredding

Event sourcing stores facts forever, but GDPR requires the ability to forget personal data. This library solves this with **crypto-shredding**: event payloads containing personal data are encrypted with a per-subject key. "Forgetting" a subject means deleting their key — the events remain in the store but the data becomes permanently unreadable.

### Setting up encryption

Pass an `EventEncryptor` when creating a store:

```go
keys := encryption.NewMemoryKeyStore()
store := memory.New(memory.WithEncryption(encryption.NewEventEncryptor(keys)))
```

For PostgreSQL, use the built-in `PostgresKeyStore` to persist keys in the database:

```go
pgstore.MigrateKeyStore(db)
keys := pgstore.NewPostgresKeyStore(db)
pgStore := pgstore.New(db, pgstore.WithEncryptor(encryption.NewEventEncryptor(keys)))
```

### Marking events as personal data

Use the `WithEncryptionKeys` option to name which tag(s) identify the data subject:

```go
event, _ := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
    "userId": "user-123",
}, eventstore.WithEncryptionKeys("userId"))
```

Every key name must exist in the event's tags — `NewEvent` returns an error otherwise. Events created **without** `WithEncryptionKeys` pass through unencrypted.

For multi-subject events, list all subject tag names:

```go
eventstore.NewEvent("FriendRequest", requestData, map[string]string{
    "senderId":   "user-1",
    "receiverId": "user-2",
}, eventstore.WithEncryptionKeys("senderId", "receiverId"))
```

All events with the same subject tags share the same encryption key, regardless of what other tags they carry. An event with tags `{userId: "user-123", orderId: "order-456"}` and `WithEncryptionKeys("userId")` uses the same key as one with just `{userId: "user-123"}` and `WithEncryptionKeys("userId")`.

### Forgetting a subject

Call `Forget` with the subject's tags:

```go
store.Forget(ctx, map[string]string{"userId": "user-123"})
```

This deletes the encryption key. All events for that subject are now permanently unreadable. The forget uses **superset matching** — `Forget({"userId": "user-1"})` also deletes keys for compound subjects like `{"senderId": "user-1", "receiverId": "user-2"}`.

### Handling redacted events in projections

After forgetting, events still flow through the store with their type, tags, and sequence number preserved, but the data is redacted. Use `e.Redacted()` to check:

```go
Handlers: map[string]eventstore.HandleFunc[MyState]{
    "UserSignedUp": func(state MyState, e eventstore.SequencedEvent) (MyState, error) {
        if e.Redacted() {
            return state, nil // crypto-shredded, skip
        }
        data, err := eventstore.EventData[UserData](e)
        if err != nil {
            return state, err
        }
        // normal handling
        state.Users = append(state.Users, data)
        return state, nil
    },
}
```

Projections that only check event existence (like the `usernameExistsProjection` example) work without changes since they don't read `Data`.

The `encryption.ErrRedacted` error is provided as a convenience for application code that needs to signal redacted data to callers.

### Recommended forget workflow

The library's `Forget` only deletes encryption keys — it does not emit events. Use domain events to coordinate:

```go
func deleteUser(ctx context.Context, store *memory.Store, userId string) error {
    // 1. Append a business event so projections can clean up.
    event, _ := eventstore.NewEvent("UserDeleted", userId, map[string]string{
        "userId": userId,
    })
    store.Append(ctx, []eventstore.Event{event}, nil)

    // 2. Shred the encryption keys.
    return store.Forget(ctx, map[string]string{"userId": userId})
}
```

Projections listen to `UserDeleted` to actively remove user data from their state. On replay, old events are redacted (`e.Redacted()` returns true) and handled gracefully.

### Custom KeyStore

A PostgreSQL-backed key store is provided out of the box via `postgres.NewPostgresKeyStore(db)`. For other backends (AWS KMS, HashiCorp Vault, etc.), implement the `encryption.KeyStore` interface:

```go
type KeyStore interface {
    GetOrCreate(ctx context.Context, tags map[string]string) ([]byte, error)
    Get(ctx context.Context, tags map[string]string) ([]byte, bool, error)
    Forget(ctx context.Context, tags map[string]string) error
}
```

`GetOrCreate` must be safe for concurrent calls with the same tags. `Get` returns `(nil, false, nil)` when the key has been forgotten. `Forget` deletes all keys whose tags are a superset of the provided tags.
