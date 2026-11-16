# GDPR / Crypto-shredding

Event sourcing stores facts forever, but GDPR requires the ability to forget personal data. This library solves this with **crypto-shredding**: event payloads containing personal data are encrypted with a per-subject key. "Forgetting" a subject means deleting their key — the events remain in the store but the data becomes permanently unreadable.

Encryption is implemented as middleware, keeping store implementations clean.

## Setting up encryption

Create an `EventEncryptor` and compose it with a store via `MiddlewareStore`:

```go
keys := encryption.NewMemoryKeyStore()
encryptor := encryption.NewEventEncryptor(keys)
store := collie.NewMiddlewareStore(memory.New(),
    collie.WithAppendMiddleware(encryptor.OnAppend),
    collie.WithReadMiddleware(encryptor.OnRead),
)
```

For PostgreSQL, use the built-in `KeyStore` to persist keys in the database:

```go
pgstore.MigrateKeyStore(db)
keys := pgstore.NewKeyStore(db)
encryptor := encryption.NewEventEncryptor(keys)
store := collie.NewMiddlewareStore(pgstore.New(db),
    collie.WithAppendMiddleware(encryptor.OnAppend),
    collie.WithReadMiddleware(encryptor.OnRead),
)
```

## Marking events as personal data

Declare it on the field. A `dcb-encrypt:"tag:<key>"` tag says "encrypt this field under the key of the data subject that tag names":

```go
type UserRegistered struct {
    UserID string `json:"userId" dcb:"tag:user"`
    Name   string `json:"name"   dcb-encrypt:"tag:user"`
}

event, _ := collie.NewEventFrom("UserRegistered", UserRegistered{UserID: "user-123", Name: "Ada"})
```

`NewEventEncryptor` switches these declarations on, so create the encryptor at boot, before any event is built (see "Struct tags and hooks" below). The hook checks at construction that `user` is a tag on the event, that the field is in `Data` and is not itself a tag, and that every encrypted field in the struct names the same subject. Group several personal fields in a nested struct and tag that one field: the block is stored as one ciphertext, so not even its key names are visible at rest, and it redacts to one `null`:

```go
type SupplierRegistered struct {
    SupplierID string  `json:"supplierId" dcb:"tag:supplier"`
    Contact    Contact `json:"contact"    dcb-encrypt:"tag:supplier"`
}
```

For events built without a struct, the options declare the same thing. `WithEncryptionKeys` names the tag(s) identifying the subject and, on its own, encrypts the whole payload; `WithEncryptedFields` limits it to the listed top-level JSON fields:

```go
event, _ := collie.NewEvent("UserSignedUp", userData, collie.NewTags("userId", "user-123"),
    encryption.WithEncryptionKeys("userId"),
    encryption.WithEncryptedFields("name", "email"),
)
```

Every key name must exist in the event's tags — the encryption middleware returns an error on append otherwise. Events that declare nothing pass through unencrypted. For multi-subject events, list all subject tag names in `WithEncryptionKeys`; all events with the same subject tags share the same encryption key, regardless of what other tags they carry.

## Field-level encryption

On read, encrypted fields are transparently decrypted. After a `Forget`, encrypted fields become `null` while the rest of the payload stays intact — `IsRedacted(e)` returns true but `e.Data` is not nil. This works with nested objects too: the middleware encrypts the raw JSON value of each field, preserving its structure on round-trip.

## Forgetting a subject

Call `Forget` directly on the encryptor:

```go
encryptor.Forget(ctx, collie.NewTags("user", "user-123"))
```

This deletes the encryption key. All events for that subject are now permanently unreadable. The forget uses **superset matching**: forgetting `user=user-1` also deletes keys for compound subjects such as `user=user-1, tenant=acme`, and a message shared between two users is shredded when either of them is forgotten.

## Handling redacted events in projections

After forgetting, events still flow through the store with their type, tags, and sequence number preserved, but the data is redacted. For full-blob encryption `Data` is nil; for field-level encryption `Data` is preserved with encrypted fields set to `null`. Use `encryption.IsRedacted(e)` to check:

```go
Handlers: map[string]collie.HandleFunc[MyState]{
    "UserSignedUp": func(state MyState, e collie.SequencedEvent) (MyState, error) {
        if encryption.IsRedacted(e) {
            return state, nil // crypto-shredded, skip
        }
        data, err := collie.EventData[UserData](e)
        if err != nil {
            return state, err
        }
        state.Users = append(state.Users, data)
        return state, nil
    },
}
```

Projections that only check event existence work without changes since they don't read `Data`.

## Recommended forget workflow

`Forget` only deletes encryption keys — it does not emit events. Use domain events to coordinate:

```go
type UserDeleted struct {
    UserID string `json:"userId" dcb:"tag:user"`
}

func deleteUser(ctx context.Context, store collie.Store, encryptor *encryption.EventEncryptor, userID string) error {
    event, _ := collie.NewEventFrom("UserDeleted", UserDeleted{UserID: userID})
    store.Append(ctx, []collie.Event{event}, nil)

    return encryptor.Forget(ctx, collie.NewTags("user", userID))
}
```

Projections listen to `UserDeleted` to actively remove user data from their state. On replay, old events are redacted (`encryption.IsRedacted(e)` returns true) and handled gracefully.

## Custom KeyStore

A PostgreSQL-backed key store is provided out of the box via `postgres.NewKeyStore(db)`. For other backends (AWS KMS, HashiCorp Vault, etc.), implement the `encryption.KeyStore` interface:

```go
type KeyStore interface {
    GetOrCreate(ctx context.Context, subject []collie.Tag) ([]byte, error)
    Get(ctx context.Context, subject []collie.Tag) ([]byte, bool, error)
    Forget(ctx context.Context, tags []collie.Tag) error
}
```

`GetOrCreate` must be safe for concurrent calls with the same subject. `Get` returns `(nil, false, nil)` when the key has been forgotten. `Forget` deletes all keys whose subject is a superset of the provided tags.

## Struct tags and hooks

The root package reads one struct-tag key, `dcb`. Every other concern owns its own key, prefixed `dcb-` (`dcb-encrypt`, `dcb-attachment`, your own), the way `json` and `gorm` each read theirs. A plugin turns its declarations into the event `Metadata` its middleware already understands through a **hook**:

```go
// Registered once at boot, before any event is built.
encryption.NewEventEncryptor(keys)         // registers the dcb-encrypt hook itself
attachments.NewAttachmentMiddleware(blobs) // registers the dcb-attachment hook itself
```

A `Hook` is `func(e *Event, payload any) error`, registered with `collie.RegisterHook`. It runs on every event built by `NewEvent` or `NewEventFrom`, after the tags and the options, and sees the typed payload. The rule a hook follows: fill what is absent, verify what is present, never overwrite. A struct tag and an option that agree are fine; ones that disagree fail construction. Registration is not synchronised with construction, so register at boot.
