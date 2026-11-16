package collie

import "context"

// actorKey and causeKey are the context keys for the envelope fields. They are
// unexported types, so no other package can write or overwrite these values.
type (
	actorKey struct{}
	causeKey struct{}
)

// ContextWithActor returns a context carrying actor, an opaque "kind:id" string naming
// whoever is responsible for what gets appended under it. The envelope
// middleware reads it; see package envelope for the grammar and who sets it.
func ContextWithActor(ctx context.Context, actor string) context.Context {
	return context.WithValue(ctx, actorKey{}, actor)
}

// ActorFromContext returns the actor carried by ctx, or the empty string if none was set.
func ActorFromContext(ctx context.Context) string {
	actor, _ := ctx.Value(actorKey{}).(string)
	return actor
}

// ContextWithCause returns a context carrying cause, an opaque "kind:id" string naming
// what the appends under it follow from — a request, an event, a job.
func ContextWithCause(ctx context.Context, cause string) context.Context {
	return context.WithValue(ctx, causeKey{}, cause)
}

// CauseFromContext returns the cause carried by ctx, or the empty string if none was set.
func CauseFromContext(ctx context.Context) string {
	cause, _ := ctx.Value(causeKey{}).(string)
	return cause
}
