# Envelope

Beside the free `Metadata` map, an event carries three typed fields — who caused it, what it followed from, and when the store took it:

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
