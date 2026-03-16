// Package encryption provides GDPR crypto-shredding for event stores.
// Events tagged with a "gdpr" tag have their Data field encrypted with
// a per-subject AES-256-GCM key. "Forgetting" a subject deletes the key,
// making the data permanently unreadable while preserving event metadata.
package encryption

import (
	"context"
	"encoding/json"
)

// KeyStore manages per-subject encryption keys for crypto-shredding.
// Keys are identified by tag sets extracted from events.
type KeyStore interface {
	// GetOrCreate returns the encryption key for the exact tag set,
	// creating a new random key if none exists.
	GetOrCreate(ctx context.Context, tags map[string]string) ([]byte, error)

	// Get returns the encryption key for the exact tag set.
	// Returns false if the key does not exist (e.g. it was forgotten).
	Get(ctx context.Context, tags map[string]string) ([]byte, bool, error)

	// Forget deletes all keys whose tag sets are a superset of the
	// provided tags. For example, Forget({"userId": "u1"}) deletes
	// keys for {"userId": "u1"} and {"userId": "u1", "orderId": "o1"}.
	Forget(ctx context.Context, tags map[string]string) error
}

// TagID returns a deterministic string identifier for a tag set.
// json.Marshal sorts map keys, so the output is stable.
func TagID(tags map[string]string) string {
	b, _ := json.Marshal(tags)
	return string(b)
}
