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

## 2. Tag ergonomics: struct field tags (partly done, Oct 2026)

Done:

- `NewEventFrom(type, payload, opts...)`: tags derived from `dcb:"tag:<key>"`
  fields of the payload struct (see README, "Tags from struct fields"). The tag
  field is a *copy*: it stays in `Data` and is read back from there, so
  `EventData` needed no change and folds read the id from the struct.
- Construction hooks (`eventstore.RegisterHook`) and the `dcb-<plugin>`
  struct-tag convention: `dcb-encrypt:"tag:<key>"` and
  `dcb-attachment:"blob"` are resolved into the existing Metadata by a hook
  each plugin registers once at boot, so a policy declared on a field cannot
  be forgotten per event. Known and deliberately ignored: a field that is both
  encrypted and offloaded is only shredded if the encryptor middleware runs
  before the attachment middleware; nothing checks that order at boot.
- Step 1 (`RequiredTag`) was dropped as redundant: with the id in the payload,
  every fold that knows its event type decodes the struct instead. Note that
  events appended *before* the struct tags carry the id only in their tags, so
  a fold over such a log must still read `TagValues`.

Still open, each a deliberate non-decision rather than an oversight:

- **Tag-only fields hydrated from tags.** A field marked `json:"-"` could be
  left out of `Data` and filled from the event's tags in `EventData`. That is
  what makes "identity survives a full-blob crypto-shred" fall out of the type
  system (tags are the durable half). Constraints if we do it: a hydrated key
  must have exactly one emitting field in the type (a scalar plus a repeated
  key is an ERROR, `[]string` is the multi-value form); `EventData` must skip
  `json.Unmarshal` on nil Data so a redacted event still hydrates. Today
  `json:"-"` on a tag field is rejected so this cannot be half-enabled.
- **Derived tags.** The identity domain tags a sha256 over two fields. With
  "every tag is a field" the hash is a real field the caller must compute; an
  empty one fails the append, so forgetting it is loud but not impossible.
  Alternatives: an `ExtraTags() []Tag` hook on the struct, or a `dcb` option
  that names a derivation. Decide once a second derived tag shows up.
- **Type name.** Stays an explicit argument on purpose: deriving it from the Go
  type name makes a rename a silent migration of the log.
