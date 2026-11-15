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
    Read(ctx context.Context, query Query, after uint64) (iter.Seq2[SequencedEvent, error], uint64, error)
    Append(ctx context.Context, events []Event, condition *AppendCondition) error
}
```

Implement this interface to back the event store with any storage engine. In-memory and PostgreSQL implementations are provided.

Besides the events, `Read` returns the **head**: the store's last sequence number when `Read` was called. Once the events have been iterated completely and without an error, every matching event up to the head has been seen, even when none matched. So `max(head, last event seen)` is how far a reader has checked the log, and it is what `BuildDecisionModel` and `UpdateProjection` record. A projection on a rare event type therefore keeps moving with the log instead of staying at its last matching event, and an append condition only has to check the writes that landed during the decision. After a `break` or an error, the head promises nothing. The events stay lazy, so `Read` without iterating is a cheap way to learn the head.

This relies on sequence order being commit order: the PostgreSQL store guarantees it with one writer to `events` (the single flusher in staging mode, or the global append lock), which is why a database records its append mode and refuses the other one (see below). The head and the events must also come from the same server; spreading reads over replicas with different lag would let a head from one overclaim the events of another.

### Events

An event is a fact that happened. It has a type, a JSON payload, and tags: the consistency boundaries it belongs to. An event is declared as a struct, and the struct says everything about it. Its `json` tags name the payload keys, its `dcb` tags name the boundaries:

```go
type UserSignedUp struct {
    Username string `json:"username" dcb:"tag:username"`
}

event, err := eventstore.NewEventFrom("UserSignedUp", UserSignedUp{Username: "alice"})
// event.Tags == [username=alice]
```

The type name stays an explicit argument on purpose: deriving it from the Go type name would make a rename a silent migration of the log. For a payload that is not a struct, `NewEvent` takes the tags next to it:

```go
event, err := eventstore.NewEvent("UserSignedUp", "alice", eventstore.NewTags("username", "alice"))
```

Once appended to the store, events become `SequencedEvent`s with a monotonically increasing sequence number and a timestamp. Read the payload back with `eventstore.EventData[UserSignedUp](e)`.

### Tags from struct fields

A `dcb:"tag:<key>"` field emits one tag per value, so an event cannot be built without the boundaries it belongs to. The same key may repeat, because tags are an ordered list rather than a map:

```go
// A chat message is a fact about two people at once.
type MessageSent struct {
    From string `json:"from" dcb:"tag:user"`
    To   string `json:"to"   dcb:"tag:user"`
    Body string `json:"body"`
}

event, _ := eventstore.NewEventFrom("MessageSent", MessageSent{From: "alice", To: "bob", Body: "hi"})
// event.Tags == [user=alice user=bob]
```

Rules:

- The grammar is `dcb:"tag:<key>[,tag:<key>...][,omitempty]"`. One field can emit under several keys (`dcb:"tag:owner,tag:user"`), in the order written.
- A tag field is still an ordinary payload field: it is JSON-encoded into `Data` as usual and the tag is a copy of it. Readers take the id from either. `json:"-"` on a tag field is rejected.
- The field must be of string kind (named string types work) or a slice of string kind. A slice emits one tag per element, in order. Fields promoted from embedded structs count.
- An empty value is an error unless the field is marked `omitempty`. An empty string inside a slice is always an error.
- A struct with no `dcb` fields, or anything that is not a struct, is `ErrInvalidTagStruct`. Use `NewEvent` for untagged payloads.

#### Plugins and hooks

The root package reads one struct-tag key, `dcb`. Every other concern owns its own key, prefixed `dcb-` (`dcb-encrypt`, `dcb-attachment`, your own), the way `json` and `gorm` each read theirs. A plugin turns its declarations into the event `Metadata` its middleware already understands through a **hook**:

```go
// Registered once at boot, before any event is built.
encryption.RegisterStructTags()
attachments.RegisterStructTags()
```

A `Hook` is `func(e *Event, payload any) error`, registered with `eventstore.RegisterHook`. It runs on every event built by `NewEvent` or `NewEventFrom`, after the tags and the options, and sees the typed payload. The rule a hook follows: fill what is absent, verify what is present, never overwrite. A struct tag and an option that agree are fine; ones that disagree fail construction. Registration is not synchronised with construction, so register at boot.

### Tags and queries

Tags are key-value metadata attached to events. A `QueryItem` filters events by type and/or tags:

```go
query := eventstore.QueryItem{
    Types: []string{"UserSignedUp"},                   // match these event types (OR)
    Tags:  eventstore.NewTags("username", "alice"),    // require these tags (AND)
}
```

Multiple `QueryItem`s combine into a `Query` with OR logic.

### Projections

A `DefaultProjection[S]` reduces matching events into a typed state. It declares which events to read (via a query) and how to handle each event type:

```go
exists := eventstore.DefaultProjection[bool]{
    Query: eventstore.QueryItem{
        Types: []string{"UserSignedUp"},
        Tags:  eventstore.NewTags("username", "alice"),
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

For large event streams, replaying every matching event on each command becomes slow. You can pre-load projection state at a known checkpoint and implement `CheckpointProjection` so that `BuildDecisionModel` only replays events after the checkpoint:

```go
type CheckpointProjection interface {
    Projection
    After() uint64
    Checkpoint(ctx context.Context, upTo uint64) error
}
```

For example, a stored projection that loads its state from a cache or database:

```go
type storedProjection[S any] struct {
    eventstore.DefaultProjection[S]
    after uint64
}

func (s *storedProjection[S]) After() uint64 { return s.after }

func (s *storedProjection[S]) Checkpoint(_ context.Context, upTo uint64) error {
    s.after = max(s.after, upTo)
    return nil
}
```

The caller only changes how the projection is constructed — `BuildDecisionModel`, the business rule check, and `Append` stay the same. When multiple projections are passed, the read starts from the lowest checkpoint and each projection skips events it has already processed.

### CheckpointProjection

A `CheckpointProjection` is a long-lived projection that tracks its position in the event stream. It catches up incrementally on demand via `UpdateProjection`, making it suitable for serving read queries.

It is a `Projection` that also reports and records its position:

```go
type CheckpointProjection interface {
    Projection          // EventQuery() QueryItem; Apply(context.Context, SequencedEvent) error
    After() uint64
    Checkpoint(ctx context.Context, upTo uint64) error
}
```

`Checkpoint` closes a catch-up: every matching event up to `upTo` has been applied, so the projection records `max(After(), upTo)`. It is only called after a complete, error-free catch-up. The provided implementations save an otherwise unchanged checkpoint only once `upTo` is 10,000 past it, so a busy log does not turn every catch-up into a write.

Two implementations are provided:

| Implementation | Storage | Use case |
|----------------|---------|----------|
| `snapshot.Projection[S]` | A `snapshot.Store`: `memory.NewSnapshotStore()` or `postgres.NewSnapshotStore(db)` | One folded state that resumes from its last checkpoint instead of replaying the stream |
| `postgres.KeyedProjection[V]` / `memory.KeyedProjection[V]` | One value per key | The read side: state you address by key and list, and state too large to fold per command |

#### Snapshot projections

`snapshot.Projection[S]` wraps a `DefaultProjection[S]` with position tracking and persists state and position through a `snapshot.Store`. Define the projection once:

```go
projection := eventstore.DefaultProjection[int]{
    Query: eventstore.QueryItem{Types: []string{"UserSignedUp"}},
    State: 0,
    Handlers: map[string]eventstore.HandleFunc[int]{
        "UserSignedUp": func(count int, _ eventstore.SequencedEvent) (int, error) {
            return count + 1, nil
        },
    },
}
```

Then pick where the snapshot lives, in memory (lost on restart) or Postgres (call `postgres.MigrateProjections(db)` once). The constructor loads an existing snapshot if there is one:

```go
snapshots := memory.NewSnapshotStore()        // or postgres.NewSnapshotStore(db)
totalUsers, err := snapshot.NewProjection(ctx, snapshots, "total_users/v1", projection)
```

Update and read the state the same way:

```go
eventstore.UpdateProjection(ctx, store, totalUsers)
fmt.Println(totalUsers.State)
```

Two things the name does. Version it (`/v1`) and bump it when the handlers change, so an old snapshot is rebuilt rather than read as wrong state. And pass partition tags to scope one projection definition to one boundary; each partition then keeps its own snapshot and checkpoint:

```go
seats, err := snapshot.NewProjection(ctx, snapshots, "course_seats/v1", seatsDelta, eventstore.NewTag("course", courseID))
```

A snapshot projection is also what `BuildDecisionModel` uses to skip already-folded events in a decision, so the same definition serves a command and a read endpoint.

#### Keyed projections — the read side

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

func (userProfileHandler) Key(e eventstore.SequencedEvent) (string, error) {
    ids := e.TagValues("userId")
    if len(ids) == 0 {
        return "", fmt.Errorf("%s carries no userId tag", e.Type)
    }
    return ids[0], nil
}

func (userProfileHandler) Apply(e eventstore.SequencedEvent, current *UserProfile) (*UserProfile, error) {
    switch e.Type {
    case "UserSignedUp":
        d, err := eventstore.EventData[UserProfile](e)
        if err != nil {
            return nil, err
        }
        return &d, nil
    case "UserEmailChanged":
        d, err := eventstore.EventData[UserProfile](e)
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
    eventstore.QueryItem{Types: []string{"UserSignedUp", "UserEmailChanged", "UserDeleted"}},
    userProfileHandler{},
)

// Catch up before serving. Read models have no subscriptions: a read is what
// pulls the projection forward — only reactors (below) watch the log on their
// own schedule. Safe to call concurrently — Apply skips an event it has
// already applied, so racing catch-ups converge instead of double-counting.
eventstore.UpdateProjection(ctx, store, profiles)

// One value by key.
entry, found, err := profiles.Get(ctx, "user-123")
fmt.Println(entry.Key, entry.Value.Name)

// A page. Entries carry their key, because folded state usually does not
// contain its own id.
page, err := profiles.List(ctx, keyedprojection.ListQuery{
    Where: []keyedprojection.FieldEquals{{Field: "status", Value: "active"}},
    Tags:  []eventstore.Tag{eventstore.NewTag("tenant", "acme")},
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

## Example: enforcing uniqueness on signup

```go
// UserSignedUp belongs to one boundary: the username being claimed.
type UserSignedUp struct {
    Username string `json:"username" dcb:"tag:username"`
}

func signUp(ctx context.Context, store eventstore.Store, username string) error {
    // 1. Build the decision model — checks if username is taken.
    exists := eventstore.DefaultProjection[bool]{
        Query: eventstore.QueryItem{
            Types: []string{"UserSignedUp"},
            Tags:  eventstore.NewTags("username", username),
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
    event, err := eventstore.NewEventFrom("UserSignedUp", UserSignedUp{Username: username})
    if err != nil {
        return err
    }
    return store.Append(ctx, []eventstore.Event{event}, condition)
}
```

## Example: serving a read query

```go
store := memory.New()
totalUsers, _ := snapshot.NewProjection(ctx, memory.NewSnapshotStore(), "total_users/v1", projection)

e := echo.New()

e.GET("/total-users", func(c echo.Context) error {
    err := eventstore.UpdateProjection(c.Request().Context(), store, totalUsers)
    if err != nil {
        return err
    }
    return c.JSON(200, map[string]int{"total_users": totalUsers.State})
})
```

## Reactors

`eventstore/reactor` is the reacting edge: handlers that watch the log and answer a fact with a side effect, with the commands that should follow from it, or with both.

```go
runner := reactor.NewRunner(store, checkpoints, registry, reactor.WithInterval(time.Second))

runner.Register("welcome-mailer/v1", reactor.ReactorFunc(
    func(ctx context.Context, event eventstore.SequencedEvent) ([]command.Command, error) {
        if event.Type != "UserSignedUp" {
            return nil, nil
        }
        return nil, mailer.Welcome(ctx, event)
    }))

go runner.Run(ctx) // ticks until ctx is cancelled
```

A reactor never appends. It returns `command.Command`s, so every reaction still loads a decision model, asserts the rules, and is guarded by an append condition — the executor is usually the `command.Registry` itself, via `Registry.Execute`.

Each reactor has its own checkpoint, persisted in a `reactor.CheckpointStore` under the name it was registered with, so a restart resumes and one stuck handler holds up only itself. The registered name *is* that checkpoint's identity. A new reactor starts from the head of the log, because replaying history means re-firing effects; `reactor.FromBeginning()` opts into a deliberate replay.

That store is not the snapshot store, and the separation is the point. A projection snapshot is a cache: drop it and the next pass rebuilds the same state from the log. A reactor checkpoint has no such source — the effects it remembers firing live outside the log — so losing it means every mail goes out a second time. Use `postgres.NewCheckpointStore(db)` (call its `Migrate` at boot) or `memory.NewCheckpointStore()`.

Delivery is at least once — the effect and the checkpoint cannot be made atomic — so a handler must tolerate the same event twice. Returning commands gets that for free: a redelivered event re-runs a command the rules already answered, the refusal classifies as `command.ErrCommandRejected`, and the runner absorbs it. Refusals are a normal outcome for a reactor, which always acts on slightly stale state; only infrastructure failures hold the checkpoint for a retry.

## State-dependent external work

A command's `Prepare` phase is where I/O belongs, and it may not read event state: only projections are covered by the append condition, so a call made from something the log never guarded was decided against nothing. That leaves the call whose input *is* state — mint an id for this subject if nobody has claimed it, charge this card if the order is still unpaid — outside the command. Two idioms cover it, and which one applies is decided by the call, not by the domain.

A cheap, idempotent call goes in a caller loop. Read the projection, make the call, and hand the command both the result and the premise it rested on; a rule refuses when that premise no longer holds, guarded by the append condition like any other decision, and the caller re-derives its question from the fresh state and goes round again. Nothing is lost but a call that was cheap by assumption, and no outcome is ever recorded against a premise that had already moved. An identity resolver is the worked example: it folds the identity boundary, mints an id for a subject it found unclaimed, and passes both into the command — and when a second login won the race, the rule refuses and the caller reads the winner's claim instead of retrying its own.

A call too expensive or too once-only to make twice is reserved, then reacted to. Split it in three. A first command appends the premise as a fact on the tightest boundary that expresses it — the reservation — and its rule is what makes a second one impossible. A reactor answers that fact by performing the call, which is where at-least-once delivery lives, so the call needs a dedup of its own: the provider's idempotency key, or a rule refusing the second recording. A second command records the outcome as another fact. The call is then performed effectively once, nothing is paid before the reservation held, and every failure mode is a fact on the log rather than a state inside a process.

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

// PostgreSQL (pgstore is "github.com/DenBeke/collie/postgres")
db, _ := gorm.Open(postgres.Open(connStr))
pgStore := pgstore.New(db, pgstore.WithBatchSize(500))
defer pgStore.Close() // stops its flusher
pgStore.Migrate()
```

### Append modes: staging (default) and the global lock

By default the PostgreSQL store appends through **staging**: an append locks only the boundaries
its condition and events name, checks the condition against both `staging` and `events`, and lands
in the `staging` table. A single flusher moves staged appends into `events` in order, so appends to
disjoint boundaries run concurrently while sequence numbers stay gap-free and in commit order.

- **The flusher comes with the store.** `New` starts one; across processes one leads and the
  others wait as standbys, so every process can simply start one. `Close` stops it, and a leader
  that stops first moves what is still staged. `WithoutFlusher()` leaves it out (a read-only tool,
  or a test that flushes itself); `RunFlusher` runs one yourself.
- **Readable within milliseconds.** `Append` returns once the events are staged: durable, and
  already seen by every condition. `Read` sees them after the next flush — at most the flush
  interval (5 ms by default) when idle. Two decisions on the same boundary in quick succession
  can therefore see the second one's condition fail on the first one's staged row before its read
  shows it; retry with a backoff longer than the flush interval.
- **Every condition item needs a type and a tag**, or `Append` returns `ErrUnkeyableCondition`.
  To make a whole kind of event one boundary, give each of those events a constant tag
  (`directory: peppol`) and put it in the condition. The in-memory store applies the same
  rule, so a test catches it; `memory.New(memory.WithGlobalLock())` lifts it there too.

`WithGlobalLock()` is the other mode: every append serializes on one global advisory lock and is
readable as soon as `Append` returns, conditions need no tags, and there is no flusher.

**One mode per database.** `Migrate` records the mode the first time it runs and installs only that
mode's functions; a store configured for the other mode then fails `Migrate` with
`ErrAppendModeMismatch`, and its appends fail the same way, because two modes writing to one
database would each break the other's guarantees. `SwitchAppendMode` changes the recorded mode on
purpose: processes still running in the old mode get errors from then on, and switching to the
global lock needs an empty `staging` table. A binary from before the mode existed knows nothing of
it and would install its own functions at `Migrate`; keep those away from a database you switch.

The design and the benchmarks behind choosing staging (including the per-boundary and hybrid
strategies that were evaluated and rejected) are written up in
[`appendbench-conclusion.md`](appendbench-conclusion.md), with full methodology in
[`appendbench-scaleway.md`](appendbench-scaleway.md).

### PostgreSQL projection tables

Call `MigrateProjections` to create the tables used by `postgres.SnapshotStore` and `postgres.KeyedProjection[V]`:

```go
postgres.MigrateProjections(db)
```

This creates three tables: `projection_checkpoints` (shared position tracking), `projection_snapshots` (snapshot state), and `projection_entries` (the values of keyed projections), with foreign key constraints from snapshots/entries to checkpoints.

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

### Envelope

Beside that free map, an event carries three typed fields — who caused it, what it followed from, and when the store took it:

| Field | On | Stamped by |
|---|---|---|
| `Actor` | `Event` | the envelope middleware, from the context |
| `Cause` | `Event` | the envelope middleware, from the context |
| `RecordedAt` | `SequencedEvent` | the store, at acceptance |

Domain code sets none of them. It puts them on the context, and the middleware does the rest:

```go
store := eventstore.NewMiddlewareStore(inner,
    // Before encryption: stamp first, then encrypt.
    eventstore.WithAppendMiddleware(envelope.New().OnAppend, encryptor.OnAppend),
    eventstore.WithReadMiddleware(encryptor.OnRead),
)

ctx = eventstore.ContextWithActor(ctx, "student:s1")
ctx = eventstore.ContextWithCause(ctx, "request:8f3a")
```

Both are opaque `"kind:id"` strings whose grammar the application owns. The framework reserves the two prefixes it produces itself: `reactor:<registered-name>` is the actor of a command a reactor dispatched, and `event:<sequence>` is the cause it hands that command.

Strict by default — an append with no actor on the context fails with `envelope.ErrNoActor`, so a forgotten actor breaks the first request in development instead of flowing silently into an immutable log. Opting out is explicit, and greppable:

```go
envelope.New(envelope.AllowAnonymous())
```

A cause is always optional. `RecordedAt` is zero, and the Postgres columns are `NULL`, for events written before the envelope existed — `Migrate` adds `recorded_at` without a default and only then gives it one, so upgrading a log does not stamp its history with the moment of the migration.

`RecordedAt` is the store's only timestamp. It replaced an older application-clock `created_at` column, which `Migrate` **drops** — without backfilling, because a timestamp the application picked on its way to the database is not the moment the store accepted the event. A log migrated off that schema keeps `NULL` on its historical rows and loses the old column's data.

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

Declare it on the field. A `dcb-encrypt:"tag:<key>"` tag says "encrypt this field under the key of the data subject that tag names":

```go
type UserRegistered struct {
    UserID string `json:"userId" dcb:"tag:user"`
    Name   string `json:"name"   dcb-encrypt:"tag:user"`
}

event, _ := eventstore.NewEventFrom("UserRegistered", UserRegistered{UserID: "user-123", Name: "Ada"})
```

This needs `encryption.RegisterStructTags()` once at boot (see "Plugins and hooks"). The hook checks at construction that `user` is a tag on the event, that the field is in `Data` and is not itself a tag, and that every encrypted field in the struct names the same subject. Group several personal fields in a nested struct and tag that one field: the block is stored as one ciphertext, so not even its key names are visible at rest, and it redacts to one `null`:

```go
type SupplierRegistered struct {
    SupplierID string  `json:"supplierId" dcb:"tag:supplier"`
    Contact    Contact `json:"contact"    dcb-encrypt:"tag:supplier"`
}
```

For events built without a struct, the options declare the same thing. `WithEncryptionKeys` names the tag(s) identifying the subject and, on its own, encrypts the whole payload; `WithEncryptedFields` limits it to the listed top-level JSON fields:

```go
event, _ := eventstore.NewEvent("UserSignedUp", userData, eventstore.NewTags("userId", "user-123"),
    encryption.WithEncryptionKeys("userId"),
    encryption.WithEncryptedFields("name", "email"),
)
```

Every key name must exist in the event's tags — the encryption middleware returns an error on append otherwise. Events that declare nothing pass through unencrypted. For multi-subject events, list all subject tag names in `WithEncryptionKeys`; all events with the same subject tags share the same encryption key, regardless of what other tags they carry.

### Field-level encryption

On read, encrypted fields are transparently decrypted. After a `Forget`, encrypted fields become `null` while the rest of the payload stays intact — `IsRedacted(e)` returns true but `e.Data` is not nil. This works with nested objects too: the middleware encrypts the raw JSON value of each field, preserving its structure on round-trip.

### Forgetting a subject

Call `Forget` directly on the encryptor:

```go
encryptor.Forget(ctx, eventstore.NewTags("user", "user-123"))
```

This deletes the encryption key. All events for that subject are now permanently unreadable. The forget uses **superset matching**: forgetting `user=user-1` also deletes keys for compound subjects such as `user=user-1, tenant=acme`, and a message shared between two users is shredded when either of them is forgotten.

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
type UserDeleted struct {
    UserID string `json:"userId" dcb:"tag:user"`
}

func deleteUser(ctx context.Context, store eventstore.Store, encryptor *encryption.EventEncryptor, userID string) error {
    event, _ := eventstore.NewEventFrom("UserDeleted", UserDeleted{UserID: userID})
    store.Append(ctx, []eventstore.Event{event}, nil)

    return encryptor.Forget(ctx, eventstore.NewTags("user", userID))
}
```

Projections listen to `UserDeleted` to actively remove user data from their state. On replay, old events are redacted (`encryption.IsRedacted(e)` returns true) and handled gracefully.

### Custom KeyStore

A PostgreSQL-backed key store is provided out of the box via `postgres.NewKeyStore(db)`. For other backends (AWS KMS, HashiCorp Vault, etc.), implement the `encryption.KeyStore` interface:

```go
type KeyStore interface {
    GetOrCreate(ctx context.Context, subject []eventstore.Tag) ([]byte, error)
    Get(ctx context.Context, subject []eventstore.Tag) ([]byte, bool, error)
    Forget(ctx context.Context, tags []eventstore.Tag) error
}
```

`GetOrCreate` must be safe for concurrent calls with the same subject. `Get` returns `(nil, false, nil)` when the key has been forgotten. `Forget` deletes all keys whose subject is a superset of the provided tags.

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

Declare it on the field with `dcb-attachment:"blob"`:

```go
type DocumentUploaded struct {
    DocumentID string `json:"documentId" dcb:"tag:document"`
    FileName   string `json:"fileName"`
    Content    []byte `json:"content"    dcb-attachment:"blob"` // large, offloaded
}

event, _ := eventstore.NewEventFrom("DocumentUploaded", doc)
```

This needs `attachments.RegisterStructTags()` once at boot (see "Plugins and hooks"). Several fields can carry the tag; a nested struct is offloaded as one blob.

For events built without a struct, `WithAttachments` names the top-level JSON fields instead:

```go
event, _ := eventstore.NewEvent("PhotoUploaded", photo, tags,
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
    Delete(ctx context.Context, id string) error          // idempotent
}
```

An in-memory implementation is provided for testing via `attachments.NewMemoryBlobStore()`.

`Delete` is the attachments analogue of `EventEncryptor.Forget`: it lets a GDPR-erasure handler crypto-shred the encrypted fields *and* physically delete the blob attachments for the same subject in one operation. It is idempotent — deleting an unknown id returns nil — so callers can erase without first checking which blobs exist.
