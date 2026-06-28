// Package encryption provides GDPR crypto-shredding for event stores.
// Events tagged with a "gdpr" tag have their Data field encrypted with
// a per-subject AES-256-GCM key. "Forgetting" a subject deletes the key,
// making the data permanently unreadable while preserving event metadata.
package encryption

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"eventstore"
)

// KeyStore manages per-subject encryption keys for crypto-shredding. A subject
// is identified by a set of tags ([]eventstore.Tag) extracted from an event; the
// same key may appear with multiple values (e.g. a message between two users).
type KeyStore interface {
	// GetOrCreate returns the encryption key for the exact subject tag set,
	// creating a new random key if none exists.
	GetOrCreate(ctx context.Context, subject []eventstore.Tag) ([]byte, error)

	// Get returns the encryption key for the exact subject tag set.
	// Returns false if the key does not exist (e.g. it was forgotten).
	Get(ctx context.Context, subject []eventstore.Tag) ([]byte, bool, error)

	// Forget deletes all keys whose subject is a superset of the provided tags.
	// For example, Forget({"userId": "u1"}) deletes keys for {"userId": "u1"} and
	// {"userId": "u1", "orderId": "o1"} — and a shared {"user": "u1", "user": "u2"}
	// message is shredded when EITHER participant is forgotten.
	Forget(ctx context.Context, tags []eventstore.Tag) error
}

// TagID returns a deterministic identifier for a subject tag set, independent of
// tag order and duplicates (the set is deduped and sorted before marshaling).
func TagID(tags []eventstore.Tag) string {
	b, _ := json.Marshal(canonicalTags(tags))
	return string(b)
}

// canonicalTags returns a deduped, sorted copy of tags so logically equal subject
// sets map to the same TagID regardless of order.
func canonicalTags(tags []eventstore.Tag) []eventstore.Tag {
	canon := slices.Clone(tags)
	slices.SortFunc(canon, func(a, b eventstore.Tag) int {
		if c := strings.Compare(a.Key, b.Key); c != 0 {
			return c
		}
		return strings.Compare(a.Value, b.Value)
	})
	return slices.Compact(canon)
}
