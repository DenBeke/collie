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

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/snapshot"
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
// dispatch wrapped in ErrValidationFailed. Validation is a registry concern:
// [Registry.Dispatch] and [Registry.Execute] both run it, the bare [Execute]
// function does not. It must stay stateless — a rule that needs the
// event store is a decision and belongs in Decide, guarded by the append
// condition.
type Validator interface {
	Validate() error
}

// SnapshotStoreUser is implemented by commands that need the registry's shared
// snapshot store injected (to build [snapshot.Projection]s). The Registry
// calls UseSnapshotStore on each freshly constructed command that implements it,
// before decoding the payload and executing.
type SnapshotStoreUser interface {
	UseSnapshotStore(snapshot.Store)
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
//	    p, err := snapshot.NewProjection(ctx, cmd.SnapshotStore(), "seats-"+cmd.Room, def)
//	    // ...
//	}
type SnapshotStoreAccess struct {
	snapshotStore snapshot.Store
}

var _ SnapshotStoreUser = (*SnapshotStoreAccess)(nil)

// UseSnapshotStore receives the store from the registry.
func (access *SnapshotStoreAccess) UseSnapshotStore(snapshotStore snapshot.Store) {
	access.snapshotStore = snapshotStore
}

// SnapshotStore returns the injected store (nil until the registry injects it).
func (access *SnapshotStoreAccess) SnapshotStore() snapshot.Store {
	return access.snapshotStore
}

// Registry holds a set of named commands and runs them by name via
// [Execute]. Construct it with [NewRegistry] and register commands
// with the With... options.
type Registry struct {
	store          eventstore.Store
	snapshotStore  snapshot.Store
	factories      map[string]func() Command
	executeOptions []ExecuteOption
	validator      func(command any) error
	injector       func(command any) error
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
func WithSnapshotStore(snapshotStore snapshot.Store) Option {
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

// WithInjector sets a function run on every freshly constructed command, before
// its payload is decoded. It is the general form of [WithSnapshotStore]: the
// registry knows nothing about your dependencies, so you match on an interface
// you own.
//
//	type DepsUser interface{ UseDeps(app.Deps) }
//
//	command.NewRegistry(store,
//	    command.WithInjector(func(cmd any) error {
//	        if user, ok := cmd.(DepsUser); ok {
//	            user.UseDeps(deps)
//	        }
//	        return nil
//	    }),
//	    ...)
//
// A non-nil error aborts the dispatch. Use it for wiring only — it runs before
// the payload exists, so it cannot see the request. Per-request work belongs in
// [Preparer.Prepare].
func WithInjector(inject func(command any) error) Option {
	return func(registry *Registry) {
		registry.injector = inject
	}
}

// Dispatch constructs a fresh copy of the command registered under name, injects
// the snapshot store if applicable, runs the registry-wide injector (see
// [WithInjector]), decodes payload into it (when non-empty), validates it —
// first with the registry-wide validator (see [WithValidator]), then with the
// command's own [Validator] check — and runs it via [Execute].
//
// So a command that implements [Preparer] has its dependencies, its payload and
// its validation all settled by the time Prepare runs.
//
// Errors are classifiable with errors.Is: an unknown name matches
// [ErrUnknownCommand], a payload that fails to decode matches
// [ErrInvalidPayload], a failed validation matches [ErrValidationFailed], and
// Execute's errors pass through as documented there.
func (registry *Registry) Dispatch(ctx context.Context, name string, payload []byte) error {
	newCommand, ok := registry.factories[name]
	if !ok {
		return fmt.Errorf("command %q: %w", name, ErrUnknownCommand)
	}
	command := newCommand()
	err := registry.inject(name, command)
	if err != nil {
		return err
	}
	if len(payload) > 0 {
		err = json.Unmarshal(payload, command)
		if err != nil {
			return fmt.Errorf("command %q: %w: %w", name, ErrInvalidPayload, err)
		}
	}
	return registry.validateAndExecute(ctx, name, command)
}

// Execute runs an already-constructed command through the registry: everything
// [Registry.Dispatch] does except the two wire-boundary steps, name lookup and
// JSON decoding. It exists for callers whose commands come from Go rather than
// from a request — a reactor reacting to an event, a scheduled job, a test —
// so they still get the registry's dependency injection, validation and execute
// options instead of reaching past them to the bare [Execute].
//
// The command need not be registered: the registry is a name table for the wire,
// and this call has no name to look up. Errors classify as they do for Dispatch,
// with the command's Go type name standing in for the dispatched name.
func (registry *Registry) Execute(ctx context.Context, cmd Command) error {
	name := commandName(cmd)
	err := registry.inject(name, cmd)
	if err != nil {
		return err
	}
	return registry.validateAndExecute(ctx, name, cmd)
}

// inject hands a command its dependencies: the shared snapshot store first, then
// the registry-wide injector. It runs before any payload is decoded, so nothing
// here can see the request — per-request work belongs in [Preparer.Prepare].
func (registry *Registry) inject(name string, command Command) error {
	if storeUser, ok := command.(SnapshotStoreUser); ok && registry.snapshotStore != nil {
		storeUser.UseSnapshotStore(registry.snapshotStore)
	}
	if registry.injector == nil {
		return nil
	}
	err := registry.injector(command)
	if err != nil {
		return fmt.Errorf("command %q: could not inject dependencies: %w", name, err)
	}
	return nil
}

// validateAndExecute runs both validation layers — the registry-wide shape check
// first, the command's own semantics second — and hands the survivor to
// [Execute]. Both refusals wrap ErrValidationFailed, so a transport maps them to
// one status without caring which layer spoke.
func (registry *Registry) validateAndExecute(ctx context.Context, name string, command Command) error {
	if registry.validator != nil {
		err := registry.validator(command)
		if err != nil {
			return fmt.Errorf("command %q: %w: %w", name, ErrValidationFailed, err)
		}
	}
	if validator, ok := command.(Validator); ok {
		err := validator.Validate()
		if err != nil {
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
