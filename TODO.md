# Framework backlog

Items discovered while building the dcb-application reference app (Aug 2026).

## 1. Encryption: treat decryption failure as redaction (bug, do first)

`EventEncryptor.OnRead` handles a *missing* key as redaction (`Metadata["redacted"]`,
fields → JSON null), but a *failing decryption* as a hard error. The two states are
one shred apart: after `Forget`, the next append for the same subject makes
`KeyStore.GetOrCreate` mint a fresh key, and every pre-shred event then decrypts
against the wrong key — `cipher: message authentication failed` — permanently
breaking every read of that subject's boundary (folds, projections, reactors).

Fix: in `OnRead`, an AES-GCM authentication failure on a subject that has a key
should redact exactly like a missing key does. The old ciphertext is unreadable
by construction — that is what a shred *means* — so redaction is the truthful
answer, not an error.

Until then, dcb-application works around it at the application level: an erasure
is terminal for encrypted fields (`projections.Profile.Forgotten` +
`UpdateUserProfile` recording nothing for a forgotten person). Once this fix
lands, that workaround becomes a choice rather than a necessity.

## 2. Tag ergonomics: `RequiredTag` helper, then tag-hydrated struct fields

Reading "who is this event about" from tags costs a 4-line `TagValues` +
missing-tag error branch in every fold (see dcb-application
`reactors.studentID`, `projections/identity.go`). Two-step plan:

**Step 1 (cheap):** `func (e SequencedEvent) RequiredTag(key string) (string, error)`
— one-line access with the loud missing-tag error built in.

**Step 2 (the real feature):** a struct field tag that hydrates from the event's
tags at decode, and — the bigger win — derives tags from the struct at append,
making the "type and tags can never disagree" guarantee structural instead of
constructor-conventional:

```go
type IdentityClaimed struct {
    UserID  string `dcb:"tag:user"`   // filled from the tag; never serialized into Data
    Issuer  string `json:"issuer"`
    Subject string `json:"subject"`
}
```

Design constraints to solve:
- Repeated tag keys: tags are a list and one key may appear twice (the
  two-account transfer). Scalar field + repeated key must be an ERROR; a
  `[]string` field is the multi-value form. Silently taking the first value
  would recreate the Keyed.TagKeys first-value-wins trap.
- The field must never round-trip into `Data`. `NewEvent` owns `json.Marshal`,
  so the framework can enforce the exclusion rather than trusting `json:"-"`.
- Per-type reflection metadata cache — `EventData` sits on hot fold paths.
- Missing tag at decode = loud error (same contract as today's hand-written helpers).
- Nice emergent property, keep it: tag-hydrated fields survive a crypto-shred
  (tags are the durable half), so identity fields stay readable while encrypted
  PII redacts — the split falls out of the type system.
