// Package command routes named DCB commands to [Execute]. It is an
// optional layer over the core Command/Execute primitives: register your
// commands once, then dispatch them by name from an HTTP handler, a queue
// consumer, a CLI, or a test. It owns no transport.
package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"

	"eventstore"
)

// Dispatch errors, classifiable with errors.Is so a transport adapter (HTTP,
// queue, CLI) can map outcomes without string matching. Domain rejections from
// Decide match [ErrCommandRejected], and an exhausted optimistic retry matches
// [eventstore.ErrAppendConditionViolated]; anything else is an infrastructure
// failure.
var (
	// ErrUnknownCommand: no command is registered under the dispatched name.
	ErrUnknownCommand = errors.New("unknown command")
	// ErrInvalidPayload: the payload did not decode into the command.
	ErrInvalidPayload = errors.New("invalid command payload")
	// ErrValidationFailed: the decoded command failed its Validate check.
	ErrValidationFailed = errors.New("command validation failed")
)

// Validator is implemented by commands that check their decoded payload.
// Dispatch calls Validate after decoding, dependency injection, and the
// registry-wide validator (see [WithValidator]); a non-nil error aborts the
// dispatch wrapped in ErrValidationFailed. Validation is a wire-boundary
// concern: commands constructed in code and run through [Execute] directly are
// not validated. It must stay stateless — a rule that needs the
// event store is a decision and belongs in Decide, guarded by the append
// condition.
type Validator interface {
	Validate() error
}

// SnapshotStoreUser is implemented by commands that need the registry's shared
// snapshot store injected (to build [eventstore.StoredProjection]s). The Registry
// calls UseSnapshotStore on each freshly constructed command that implements it,
// before decoding the payload and executing.
type SnapshotStoreUser interface {
	UseSnapshotStore(eventstore.SnapshotStore)
}

// SnapshotStoreAccess is an embeddable helper that satisfies [SnapshotStoreUser].
// Embed it (by value) in a command that needs the registry's snapshot store and it
// supplies both the injection setter and a SnapshotStore accessor, so you don't
// write UseSnapshotStore by hand:
//
//	type ReserveSeat struct {
//	    command.SnapshotStoreAccess
//	    Room string `json:"room"`
//	}
//
//	func (cmd *ReserveSeat) Projections(ctx context.Context) ([]eventstore.Projection, error) {
//	    p, err := eventstore.NewStoredProjection(ctx, cmd.SnapshotStore(), "seats-"+cmd.Room, def)
//	    // ...
//	}
type SnapshotStoreAccess struct {
	snapshotStore eventstore.SnapshotStore
}

var _ SnapshotStoreUser = (*SnapshotStoreAccess)(nil)

// UseSnapshotStore receives the store from the registry.
func (access *SnapshotStoreAccess) UseSnapshotStore(snapshotStore eventstore.SnapshotStore) {
	access.snapshotStore = snapshotStore
}

// SnapshotStore returns the injected store (nil until the registry injects it).
func (access *SnapshotStoreAccess) SnapshotStore() eventstore.SnapshotStore {
	return access.snapshotStore
}

// Registry holds a set of named commands and runs them by name via
// [Execute]. Construct it with [NewRegistry] and register commands
// with the With... options.
type Registry struct {
	store          eventstore.Store
	snapshotStore  eventstore.SnapshotStore
	factories      map[string]func() Command
	executeOptions []ExecuteOption
	validator      func(command any) error
}

// Option configures a Registry.
type Option func(*Registry)

// NewRegistry creates a Registry backed by store.
func NewRegistry(store eventstore.Store, options ...Option) *Registry {
	registry := &Registry{
		store:     store,
		factories: make(map[string]func() Command),
	}
	for _, option := range options {
		option(registry)
	}
	return registry
}

// WithCommands registers each command under its Go type name (e.g. *SignUp ->
// "SignUp"). Register pointer values, e.g. &SignUp{}; Dispatch makes a fresh copy
// per call.
func WithCommands(commands ...Command) Option {
	return func(registry *Registry) {
		for _, command := range commands {
			registry.factories[commandName(command)] = cloneFactory(command)
		}
	}
}

// WithCommand registers a command under an explicit name, decoupling the wire
// name from the Go type name. Register a pointer value, e.g. &SignUp{}.
func WithCommand(name string, command Command) Option {
	return func(registry *Registry) {
		registry.factories[name] = cloneFactory(command)
	}
}

// WithCommandFunc registers a command built by newCommand under name. Use this
// when a command needs constructor-injected dependencies beyond the shared
// snapshot store; newCommand must return a fresh command on each call.
func WithCommandFunc(name string, newCommand func() Command) Option {
	return func(registry *Registry) {
		registry.factories[name] = newCommand
	}
}

// WithSnapshotStore sets the snapshot store injected into commands that implement
// [SnapshotStoreUser].
func WithSnapshotStore(snapshotStore eventstore.SnapshotStore) Option {
	return func(registry *Registry) {
		registry.snapshotStore = snapshotStore
	}
}

// WithExecuteOptions sets [ExecuteOption]s applied to every dispatch
// (e.g. a retry policy).
func WithExecuteOptions(options ...ExecuteOption) Option {
	return func(registry *Registry) {
		registry.executeOptions = append(registry.executeOptions, options...)
	}
}

// WithValidator sets a registry-wide validator that Dispatch runs on every
// decoded command, before the command's own [Validator] check. Use it to wire a
// struct-validation library exactly once:
//
//	validate := validator.New() // go-playground/validator
//	command.NewRegistry(store,
//	    command.WithValidator(func(cmd any) error { return validate.Struct(cmd) }),
//	    ...)
//
// A non-nil error aborts the dispatch wrapped in [ErrValidationFailed], with
// the validator's error kept in the chain for errors.As (e.g. to render
// per-field responses from validator.ValidationErrors).
func WithValidator(validate func(command any) error) Option {
	return func(registry *Registry) {
		registry.validator = validate
	}
}

// Dispatch constructs a fresh copy of the command registered under name, injects
// the snapshot store if applicable, decodes payload into it (when non-empty),
// validates it — first with the registry-wide validator (see [WithValidator]),
// then with the command's own [Validator] check — and runs it via
// [Execute]. Errors are classifiable with errors.Is: an unknown name
// matches [ErrUnknownCommand], a payload that fails to decode matches
// [ErrInvalidPayload], a failed validation matches [ErrValidationFailed], and
// Execute's errors pass through as documented there.
func (registry *Registry) Dispatch(ctx context.Context, name string, payload []byte) error {
	newCommand, ok := registry.factories[name]
	if !ok {
		return fmt.Errorf("command %q: %w", name, ErrUnknownCommand)
	}
	command := newCommand()
	if storeUser, ok := command.(SnapshotStoreUser); ok && registry.snapshotStore != nil {
		storeUser.UseSnapshotStore(registry.snapshotStore)
	}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, command); err != nil {
			return fmt.Errorf("command %q: %w: %w", name, ErrInvalidPayload, err)
		}
	}
	if registry.validator != nil {
		if err := registry.validator(command); err != nil {
			return fmt.Errorf("command %q: %w: %w", name, ErrValidationFailed, err)
		}
	}
	if validator, ok := command.(Validator); ok {
		if err := validator.Validate(); err != nil {
			return fmt.Errorf("command %q: %w: %w", name, ErrValidationFailed, err)
		}
	}
	return Execute(ctx, registry.store, command, registry.executeOptions...)
}

// commandName returns the type name of a (pointer to) command, e.g. *SignUp -> "SignUp".
func commandName(command Command) string {
	commandType := reflect.TypeOf(command)
	if commandType.Kind() == reflect.Pointer {
		commandType = commandType.Elem()
	}
	return commandType.Name()
}

// cloneFactory returns a function that produces a fresh zero-valued copy of the
// same concrete type as sample, which must be a pointer to a struct.
func cloneFactory(sample Command) func() Command {
	structType := reflect.TypeOf(sample).Elem()
	return func() Command {
		return reflect.New(structType).Interface().(Command)
	}
}
