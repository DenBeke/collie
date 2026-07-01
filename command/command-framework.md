# Command framework

A thin, optional layer for running DCB commands. It owns the mechanical part of every
write — load the decision model, decide which events to append, append them guarded by
an optimistic-concurrency condition, and (optionally) retry on conflict — so the only
code you write per command is your domain logic. Built on the existing primitives
(`BuildDecisionModel`, `Store.Append`); it adds no new store behavior.

## Where it lives

| Layer | Package | Provides |
|---|---|---|
| **Run one** | `eventstore` | `Command`, `Execute`, `ExecuteOption` |
| **Run many** | `eventstore/command` (this package) | `Registry`, `Dispatch`, injection helpers |

`Command` and `Execute` live in the core `eventstore` package so they can be used on
their own; the `Registry` is an opt-in convenience on top.

## Command + Execute (`eventstore`)

```go
type Command interface {
	// Projections declares the decision model, built from the command's own
	// fields. It is called once per attempt and must return FRESH projections
	// each call (their state is mutated in place while loading). It returns an
	// error if a projection fails to load; returning no projections makes the
	// append unconditional.
	Projections(ctx context.Context) ([]Projection, error)
	// Decide reads the loaded projections' state and returns the events to
	// append, an error to abort (nothing is appended), or no events for a no-op.
	Decide(ctx context.Context) ([]Event, error)
}
```

A concrete command is named after the operation and carries its parameters as **exported
fields** (the request payload); **unexported fields** hold the loaded projections:

```go
type SignUp struct {
	Username string `json:"username"`
	exists   *eventstore.DefaultProjection[bool]
}

func (cmd *SignUp) Projections(_ context.Context) ([]eventstore.Projection, error) {
	projection := usernameExistsProjection(cmd.Username)
	cmd.exists = &projection
	return []eventstore.Projection{cmd.exists}, nil
}

func (cmd *SignUp) Decide(_ context.Context) ([]eventstore.Event, error) {
	if cmd.exists.State {
		return nil, fmt.Errorf("username %q is already taken", cmd.Username)
	}
	event, err := eventstore.NewEvent("UserSignedUp", cmd.Username, eventstore.NewTags("username", cmd.Username))
	if err != nil {
		return nil, fmt.Errorf("could not create UserSignedUp event: %w", err)
	}
	return []eventstore.Event{event}, nil
}
```

`Execute` is written once and runs any command:

```go
func Execute(ctx context.Context, store Store, command Command, options ...ExecuteOption) error
```

Per attempt it: **loads** (`command.Projections` → `BuildDecisionModel`) → **decides**
(`command.Decide`) → **appends** (`store.Append`, guarded by the condition built from
exactly those projections) → **retries** on `ErrAppendConditionViolated`. A `Decide`
error aborts without appending and without retrying; no events is a no-op; no projections
means an unconditional append.

Because the `AppendCondition` is built from exactly the projections you declared, the
guard always covers what the decision read — **correct by construction**.

**Retry is off by default** (one attempt). Opt in:

```go
func WithMaxAttempts(maxAttempts int) ExecuteOption               // > 1 enables retry
func WithBackoff(backoff func(ctx context.Context, attempt int)) ExecuteOption

err := eventstore.Execute(ctx, store, &SignUp{Username: "alice"}, eventstore.WithMaxAttempts(3))
```

**Error conventions:** infrastructure failures are wrapped with context
(`fmt.Errorf("...: %w", err)`); the command's own `Decide` error is passed through
unwrapped so callers see the domain message directly. `ErrAppendConditionViolated` stays
reachable via `errors.Is` even after retries are exhausted.

## Registry — run many by name (this package)

Register command **values**, run them by name. Transport-neutral: call `Dispatch` from an
HTTP handler, a queue consumer, a CLI, or a test.

```go
func NewRegistry(store eventstore.Store, options ...Option) *Registry

func WithCommands(commands ...eventstore.Command) Option                        // names = Go type names
func WithCommand(name string, command eventstore.Command) Option                // explicit wire name
func WithCommandFunc(name string, newCommand func() eventstore.Command) Option  // factory (bespoke deps)
func WithSnapshotStore(snapshotStore eventstore.SnapshotStore) Option
func WithExecuteOptions(options ...eventstore.ExecuteOption) Option

func (registry *Registry) Dispatch(ctx context.Context, name string, payload []byte) error
```

```go
registry := command.NewRegistry(store,
	command.WithCommands(&SignUp{}, &ReserveSeat{}),
	command.WithSnapshotStore(postgres.NewSnapshotStore(db)),
	command.WithExecuteOptions(eventstore.WithMaxAttempts(5)),
)
err := registry.Dispatch(ctx, "SignUp", []byte(`{"username":"alice"}`))
```

`Dispatch` looks the command up by name, builds a **fresh copy** (`reflect.New`), injects
the snapshot store if applicable, decodes the JSON payload into it, then runs `Execute`.
Register pointer values (`&SignUp{}`) — commands have no constructor; `NewRegistry` is the
only `New*` here.

## Cached commands (stored projections)

For long-lived entities, replaying the whole history on every decision is the cost to
avoid. A command whose `Projections` returns an `eventstore.StoredProjection` reads from a
persisted snapshot (a cheap load + delta catch-up) instead. That persistence is a pluggable
`eventstore.SnapshotStore` (`postgres.NewSnapshotStore(db)`, `memory.NewSnapshotStore()`,
or your own), and the registry injects it into the command.

Embed **`command.SnapshotStoreAccess`** (by value) to receive the store — no
`UseSnapshotStore` boilerplate:

```go
type ReserveSeat struct {
	command.SnapshotStoreAccess
	Room string `json:"room"`

	seatsTaken *eventstore.StoredProjection[int]
}

func (cmd *ReserveSeat) Projections(ctx context.Context) ([]eventstore.Projection, error) {
	projection, err := eventstore.NewStoredProjection(ctx, cmd.SnapshotStore(), "seats-"+cmd.Room, seatsDef())
	if err != nil {
		return nil, fmt.Errorf("could not load seats projection: %w", err)
	}
	cmd.seatsTaken = projection
	return []eventstore.Projection{cmd.seatsTaken}, nil
}
```

Injection is a small interface the registry type-asserts for:

```go
type SnapshotStoreUser interface {
	UseSnapshotStore(eventstore.SnapshotStore)
}
```

`SnapshotStoreAccess` satisfies it (its promoted pointer method lands in the command's
method set when embedded by value). A command may instead implement `UseSnapshotStore` by
hand, or use `WithCommandFunc` when it needs constructor-injected dependencies beyond the
shared snapshot store.

Swapping the backend costs one `SnapshotStore` implementation plus one `WithSnapshotStore`
line — no command changes. (See the `SnapshotStore` / `StoredProjection` design notes for
the persistence seam.)

## Naming glossary
- **Command** — one operation (`Projections` + `Decide`); concrete types named after the op.
- **Execute** — run one command (load → decide → guarded append → optional retry).
- **Registry** / **NewRegistry** — a set of named commands; like `http.ServeMux` for commands.
- **Dispatch** — decode a payload into the named command and `Execute` it.
- **SnapshotStore** — pluggable persistence for stored projections.
- **StoredProjection** — a checkpointed projection whose state persists via a `SnapshotStore`.
- **SnapshotStoreUser** / **SnapshotStoreAccess** — the injection interface and its embeddable helper.
