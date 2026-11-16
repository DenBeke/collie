# Reactors

The `reactor` package is the reacting edge: handlers that watch the log and answer a fact with a side effect, with the commands that should follow from it, or with both.

```go
runner := reactor.NewRunner(store, checkpoints, registry, reactor.WithInterval(time.Second))

runner.Register("welcome-mailer/v1", reactor.ReactorFunc(
    func(ctx context.Context, event collie.SequencedEvent) ([]command.Command, error) {
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
