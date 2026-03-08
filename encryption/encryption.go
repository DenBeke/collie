package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"strings"

	"eventstore"
)

// MetadataKey is the metadata key used to store encryption key names.
const MetadataKey = "encryptionKeys"

// ErrRedacted is a convenience error for consumers to return when encountering
// redacted events (events whose Data is nil after crypto-shredding).
var ErrRedacted = errors.New("event data redacted")

// ErrMissingTag is returned when an encryption key name does not exist as a
// tag on the event.
var ErrMissingTag = errors.New("encryption key references missing tag")

// WithEncryptionKeys marks the event for GDPR crypto-shredding. The provided
// names must each exist as a key in the event's Tags; their values identify
// the data subject whose encryption key protects this event's Data.
// Validation is performed by the OnAppend middleware.
func WithEncryptionKeys(keys ...string) eventstore.EventOption {
	return func(e *eventstore.Event) {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata[MetadataKey] = strings.Join(keys, ",")
	}
}

// IsRedacted returns true if the event was crypto-shredded by the encryption
// middleware (the subject's key was forgotten).
func IsRedacted(e eventstore.SequencedEvent) bool {
	return e.Metadata["redacted"] == "true"
}

// EventEncryptor transparently encrypts and decrypts event data for GDPR
// compliance. Use OnAppend and OnRead as middleware with MiddlewareStore.
type EventEncryptor struct {
	keys KeyStore
}

// NewEventEncryptor creates an EventEncryptor backed by the given KeyStore.
func NewEventEncryptor(keys KeyStore) *EventEncryptor {
	return &EventEncryptor{keys: keys}
}

// OnAppend is an AppendMiddleware that encrypts event Data if the event has
// encryption keys set in its Metadata.
func (enc *EventEncryptor) OnAppend(ctx context.Context, e *eventstore.Event) error {
	keys := encryptionKeys(e.Metadata)
	if len(keys) == 0 {
		return nil
	}
	subject, err := subjectTags(e.Tags, keys)
	if err != nil {
		return err
	}
	key, err := enc.keys.GetOrCreate(ctx, subject)
	if err != nil {
		return fmt.Errorf("could not get encryption key: %w", err)
	}
	ciphertext, err := encrypt(key, e.Data)
	if err != nil {
		return fmt.Errorf("could not encrypt event data: %w", err)
	}
	e.Data = ciphertext
	return nil
}

// OnRead is a ReadMiddleware that decrypts event Data if the event has
// encryption keys set in its Metadata. If the subject's key has been
// forgotten, Data is set to nil (redacted).
func (enc *EventEncryptor) OnRead(ctx context.Context, e *eventstore.SequencedEvent) error {
	keys := encryptionKeys(e.Metadata)
	if len(keys) == 0 {
		return nil
	}
	subject, err := subjectTags(e.Tags, keys)
	if err != nil {
		return err
	}
	key, ok, err := enc.keys.Get(ctx, subject)
	if err != nil {
		return fmt.Errorf("could not get decryption key: %w", err)
	}
	if !ok {
		e.Data = nil
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata["redacted"] = "true"
		return nil
	}
	plaintext, err := decrypt(key, e.Data)
	if err != nil {
		return fmt.Errorf("could not decrypt event data: %w", err)
	}
	e.Data = plaintext
	return nil
}

// Forget deletes all keys whose tags are a superset of the provided tags.
func (enc *EventEncryptor) Forget(ctx context.Context, tags map[string]string) error {
	return enc.keys.Forget(ctx, tags)
}

// encryptionKeys reads the encryption key names from event metadata.
func encryptionKeys(metadata map[string]string) []string {
	v, ok := metadata[MetadataKey]
	if !ok || v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// subjectTags extracts the subject-identifying tags from an event's tag set
// using the provided encryption key names. Returns an error if any key name
// does not exist in tags.
func subjectTags(tags map[string]string, keys []string) (map[string]string, error) {
	subject := make(map[string]string, len(keys))
	for _, name := range keys {
		v, ok := tags[name]
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrMissingTag, name)
		}
		subject[name] = v
	}
	return subject, nil
}

// encrypt encrypts plaintext with AES-256-GCM. The nonce is prepended
// to the ciphertext. key must be 32 bytes.
func encrypt(key, plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decrypt decrypts ciphertext produced by encrypt.
func decrypt(key, ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	return gcm.Open(nil, nonce, ciphertext, nil)
}
