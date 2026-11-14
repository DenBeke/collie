package eventstore

// Hook runs on every event built by NewEvent or NewEventFrom, after Type, Data,
// Tags and the options are set. It sees the typed payload the event was built
// from, which is how a plugin resolves its own struct tags (its own `dcb-<name>`
// key) into the Metadata its middleware already reads. The root package knows
// no struct-tag key but `dcb`.
//
// A hook fills what is absent, verifies what is present and never overwrites,
// so an option and a struct tag declaring the same thing agree, and declaring
// different things fails here rather than silently preferring one. Returning an
// error aborts construction.
type Hook func(e *Event, payload any) error

// hooks run in registration order.
var hooks []Hook

// RegisterHook adds a hook for every event built from now on. Register at boot,
// before any event is built: registration is not synchronised with
// construction, by choice.
func RegisterHook(h Hook) {
	hooks = append(hooks, h)
}
