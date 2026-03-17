# eventstore

A minimal Go library for event sourcing with [Dynamic Consistency Boundaries](https://dcb.events/specification/). It provides an event store interface, typed projections, and optimistic concurrency control — everything needed to enforce business rules on the write side and serve read queries on the read side.


## Installation

```
go get eventstore
```

## Core concepts

### Store

The central interface has two methods — append events and read them back:

```go
type Store interface {
    Read(ctx context.Context, query Query, after uint64) iter.Seq2[SequencedEvent, error]
    Append(ctx context.Context, events []Event, condition *AppendCondition) error
}
```

Implement this interface to back the event store with any storage engine. In-memory and PostgreSQL implementations are provided.

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

### Stored decision models

For large event streams, replaying every matching event on each command becomes slow. You can pre-load projection state at a known checkpoint and implement `CheckpointedProjector` so that `BuildDecisionModel` only replays events after the checkpoint:

```go
type CheckpointedProjector interface {
    Projector
    After() uint64
}
```

For example, a stored projection that loads its state from a cache or database:

```go
type storedProjection[S any] struct {
    eventstore.Projection[S]
    after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }
```

The caller only changes how the projection is constructed — `BuildDecisionModel`, the business rule check, and `Append` stay the same. When multiple projections are passed, the read starts from the lowest checkpoint and each projection skips events it has already processed.

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

#### In-memory and postgres snapshot projections

Both wrap a `Projection[S]` with position tracking. Define the projection once:

```go
projection := eventstore.Projection[int]{
    Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
    State: 0,
    Handlers: map[string]eventstore.HandleFunc[int]{
        "UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
            return count + 1, nil
        },
    },
}
```

Then choose in-memory (state lost on restart) or postgres (state persisted to DB):

```go
// In-memory
totalUsers := memory.NewProjection(projection)

// Postgres — loads existing state on construction
totalUsers, err := postgres.NewProjection(db, "total_users", projection)
```

Update and read the state the same way:

```go
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

// List rows filtered by event tags.
active, err := profiles.List(ctx, postgres.WithMatchTag("status", "active"))

// Multiple values match any (IN logic).
subset, err := profiles.List(ctx, postgres.WithMatchTag("userId", "user-1", "user-2"))

// Combine filters (AND), add ordering and pagination.
page, err := profiles.List(ctx,
    postgres.WithMatchTag("tenant", "acme"),
    postgres.WithOrderBy("name", false),
    postgres.WithPagination(0, 20),
)
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
store := memory.New()
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

## Store backends

| Package | Description |
|---------|-------------|
| `eventstore/memory` | In-memory store for testing and development |
| `eventstore/postgres` | PostgreSQL store with advisory locking, batched reads, and B-tree/GIN tag indexing |

| Package | Description |
|---------|-------------|
| `eventstore/encryption` | Middleware: GDPR crypto-shredding via per-subject encryption |
| `eventstore/attachments` | Middleware: offload large event fields to blob storage |

```go
// In-memory
var store eventstore.Store = memory.New()

// PostgreSQL
db, _ := gorm.Open(postgres.Open(connStr))
pgStore := pgstore.New(db, pgstore.WithIndexedTags("tenant_id"))
pgStore.Migrate()
```

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

## Middleware

A `MiddlewareStore` wraps any `Store` and applies middleware to events flowing through `Append` and `Read`. Middleware is a pure event transform — it never needs access to store internals.

```
Append:  Event → [AppendMiddleware₁ → AppendMiddleware₂] → Store.Append
Read:    Store.Read → [ReadMiddleware₁ → ReadMiddleware₂] → Consumer
```

```go
keys := encryption.NewMemoryKeyStore()
encryptor := encryption.NewEventEncryptor(keys)
store := eventstore.NewMiddlewareStore(memory.New(),
    eventstore.WithAppendMiddleware(encryptor.OnAppend),  // runs before persist
    eventstore.WithReadMiddleware(encryptor.OnRead),       // runs after read
)
```

Middleware types:

```go
type AppendMiddleware func(ctx context.Context, e *Event) error
type ReadMiddleware func(ctx context.Context, e *SequencedEvent) error
```

Multiple middleware run in registration order. Append middleware can validate, transform, or encrypt event data. Read middleware can decrypt, decompress, or enrich events.

### Metadata

Events carry a `Metadata map[string]string` field for middleware-specific data. Metadata is persisted alongside events but is not used for querying. Middleware reads and writes metadata to coordinate — for example, the encryption middleware stores encryption key names in `metadata["encryptionKeys"]`.

## GDPR / Crypto-shredding

Event sourcing stores facts forever, but GDPR requires the ability to forget personal data. This library solves this with **crypto-shredding**: event payloads containing personal data are encrypted with a per-subject key. "Forgetting" a subject means deleting their key — the events remain in the store but the data becomes permanently unreadable.

Encryption is implemented as middleware, keeping store implementations clean.

### Setting up encryption

Create an `EventEncryptor` and compose it with a store via `MiddlewareStore`:

```go
keys := encryption.NewMemoryKeyStore()
encryptor := encryption.NewEventEncryptor(keys)
store := eventstore.NewMiddlewareStore(memory.New(),
    eventstore.WithAppendMiddleware(encryptor.OnAppend),
    eventstore.WithReadMiddleware(encryptor.OnRead),
)
```

For PostgreSQL, use the built-in `KeyStore` to persist keys in the database:

```go
pgstore.MigrateKeyStore(db)
keys := pgstore.NewKeyStore(db)
encryptor := encryption.NewEventEncryptor(keys)
store := eventstore.NewMiddlewareStore(pgstore.New(db),
    eventstore.WithAppendMiddleware(encryptor.OnAppend),
    eventstore.WithReadMiddleware(encryptor.OnRead),
)
```

### Marking events as personal data

Use the `WithEncryptionKeys` option to name which tag(s) identify the data subject:

```go
event, _ := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
    "userId": "user-123",
}, encryption.WithEncryptionKeys("userId"))
```

Every key name must exist in the event's tags — the encryption middleware returns an error on append otherwise. Events created **without** `WithEncryptionKeys` pass through unencrypted.

For multi-subject events, list all subject tag names:

```go
eventstore.NewEvent("FriendRequest", requestData, map[string]string{
    "senderId":   "user-1",
    "receiverId": "user-2",
}, encryption.WithEncryptionKeys("senderId", "receiverId"))
```

All events with the same subject tags share the same encryption key, regardless of what other tags they carry.

### Field-level encryption

By default, `WithEncryptionKeys` encrypts the entire event payload. If only some fields contain personal data, use `WithEncryptedFields` to encrypt just those fields — non-sensitive fields remain readable even after the subject is forgotten:

```go
event, _ := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
    "userId": "user-123",
},
    encryption.WithEncryptionKeys("userId"),
    encryption.WithEncryptedFields("name", "email"),
)
```

On read, encrypted fields are transparently decrypted. After a `Forget`, encrypted fields become `null` while the rest of the payload stays intact — `IsRedacted(e)` returns true but `e.Data` is not nil. This works with nested objects too: the middleware encrypts the raw JSON value of each field, preserving its structure on round-trip.

### Forgetting a subject

Call `Forget` directly on the encryptor:

```go
encryptor.Forget(ctx, map[string]string{"userId": "user-123"})
```

This deletes the encryption key. All events for that subject are now permanently unreadable. The forget uses **superset matching** — `Forget({"userId": "user-1"})` also deletes keys for compound subjects like `{"senderId": "user-1", "receiverId": "user-2"}`.

### Handling redacted events in projections

After forgetting, events still flow through the store with their type, tags, and sequence number preserved, but the data is redacted. For full-blob encryption `Data` is nil; for field-level encryption `Data` is preserved with encrypted fields set to `null`. Use `encryption.IsRedacted(e)` to check:

```go
Handlers: map[string]eventstore.HandleFunc[MyState]{
    "UserSignedUp": func(state MyState, e eventstore.SequencedEvent) (MyState, error) {
        if encryption.IsRedacted(e) {
            return state, nil // crypto-shredded, skip
        }
        data, err := eventstore.EventData[UserData](e)
        if err != nil {
            return state, err
        }
        state.Users = append(state.Users, data)
        return state, nil
    },
}
```

Projections that only check event existence work without changes since they don't read `Data`.

### Recommended forget workflow

`Forget` only deletes encryption keys — it does not emit events. Use domain events to coordinate:

```go
func deleteUser(ctx context.Context, store eventstore.Store, encryptor *encryption.EventEncryptor, userId string) error {
    event, _ := eventstore.NewEvent("UserDeleted", userId, map[string]string{
        "userId": userId,
    })
    store.Append(ctx, []eventstore.Event{event}, nil)

    return encryptor.Forget(ctx, map[string]string{"userId": userId})
}
```

Projections listen to `UserDeleted` to actively remove user data from their state. On replay, old events are redacted (`encryption.IsRedacted(e)` returns true) and handled gracefully.

### Custom KeyStore

A PostgreSQL-backed key store is provided out of the box via `postgres.NewKeyStore(db)`. For other backends (AWS KMS, HashiCorp Vault, etc.), implement the `encryption.KeyStore` interface:

```go
type KeyStore interface {
    GetOrCreate(ctx context.Context, tags map[string]string) ([]byte, error)
    Get(ctx context.Context, tags map[string]string) ([]byte, bool, error)
    Forget(ctx context.Context, tags map[string]string) error
}
```

`GetOrCreate` must be safe for concurrent calls with the same tags. `Get` returns `(nil, false, nil)` when the key has been forgotten. `Forget` deletes all keys whose tags are a superset of the provided tags.

## Attachments / Blob offloading

Large event fields (files, images, binary payloads) can be offloaded to a blob store instead of being stored inline. The attachment middleware transparently extracts marked fields on append and restores them on read — consumers see the original struct unchanged.

### Setting up attachments

```go
blobStore := attachments.NewMemoryBlobStore() // or your own BlobStore implementation
attachmentMiddleware := attachments.NewAttachmentMiddleware(blobStore)
store := eventstore.NewMiddlewareStore(inner,
    eventstore.WithAppendMiddleware(attachmentMiddleware.OnAppend),
    eventstore.WithReadMiddleware(attachmentMiddleware.OnRead),
)
```

### Marking fields for offloading

Use `WithAttachments` to name which JSON fields should be stored in the blob store:

```go
type DocumentUploaded struct {
    FileName string `json:"fileName"`
    Content  []byte `json:"content"` // large, offloaded
}

event, _ := eventstore.NewEvent("DocumentUploaded", doc, tags,
    attachments.WithAttachments("content"),
)
```

Multiple fields can be offloaded at once:

```go
eventstore.NewEvent("PhotoUploaded", photo, tags,
    attachments.WithAttachments("content", "thumbnail"),
)
```

### Reading events with attachments

On read, the middleware restores the original data from the blob store. Consumers use `EventData[T]` as usual. If a blob cannot be retrieved, the field is set to `null` and the event is flagged — check with `attachments.IsMissing(event)`.

```go
data, _ := eventstore.EventData[DocumentUploaded](event)
fmt.Println(len(data.Content)) // original size
```

### Direct blob access

Blob IDs are preserved in event metadata for direct access (e.g. serving download URLs without loading the full blob through the event store):

```go
ids := attachments.BlobIDs(event) // map[string]string{"content": "blob-abc-123"}
url := fmt.Sprintf("/blobs/%s", ids["content"])
```

### Lazy loading

If you don't want blobs fetched on every read (e.g. for projections that only need metadata), skip registering the read middleware:

```go
store := eventstore.NewMiddlewareStore(inner,
    eventstore.WithAppendMiddleware(attachmentMiddleware.OnAppend),
    // no OnRead — blob IDs stay in metadata for manual access
)
```

### Custom BlobStore

Implement the `attachments.BlobStore` interface for any storage backend (S3, GCS, local filesystem, etc.):

```go
type BlobStore interface {
    Put(ctx context.Context, data []byte) (string, error) // returns blob ID
    Get(ctx context.Context, id string) ([]byte, error)
}
```

An in-memory implementation is provided for testing via `attachments.NewMemoryBlobStore()`.
