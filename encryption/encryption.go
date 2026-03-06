package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"

	"eventstore"
)

// ErrRedacted is a convenience error for consumers to return when encountering
// redacted events (events whose Data is nil after crypto-shredding).
var ErrRedacted = errors.New("event data redacted")

// EventEncryptor transparently encrypts and decrypts event data for GDPR
// compliance. It is used internally by Store implementations.
type EventEncryptor struct {
	keys KeyStore
}

// NewEventEncryptor creates an EventEncryptor backed by the given KeyStore.
func NewEventEncryptor(keys KeyStore) *EventEncryptor {
	return &EventEncryptor{keys: keys}
}

// Seal encrypts the event's Data if it has EncryptionKeys set. Events without
// encryption keys are returned unchanged.
func (enc *EventEncryptor) Seal(ctx context.Context, e eventstore.Event) (eventstore.Event, error) {
	subject := subjectTags(e.Tags, e.EncryptionKeys)
	if subject == nil {
		return e, nil
	}
	key, err := enc.keys.GetOrCreate(ctx, subject)
	if err != nil {
		return e, fmt.Errorf("could not get encryption key: %w", err)
	}
	ciphertext, err := encrypt(key, e.Data)
	if err != nil {
		return e, fmt.Errorf("could not encrypt event data: %w", err)
	}
	return eventstore.Event{
		Type:           e.Type,
		Data:           ciphertext,
		Tags:           e.Tags,
		EncryptionKeys: e.EncryptionKeys,
	}, nil
}

// Open decrypts the event's Data if it has EncryptionKeys set. If the subject's
// key has been forgotten, the event is returned with Data set to nil.
// Events without encryption keys are returned unchanged.
func (enc *EventEncryptor) Open(ctx context.Context, e eventstore.SequencedEvent) (eventstore.SequencedEvent, error) {
	subject := subjectTags(e.Tags, e.EncryptionKeys)
	if subject == nil {
		return e, nil
	}
	key, ok, err := enc.keys.Get(ctx, subject)
	if err != nil {
		return e, fmt.Errorf("could not get decryption key: %w", err)
	}
	if !ok {
		e.Data = nil
		return e, nil
	}
	plaintext, err := decrypt(key, e.Data)
	if err != nil {
		return e, fmt.Errorf("could not decrypt event data: %w", err)
	}
	e.Data = plaintext
	return e, nil
}

// Forget deletes all keys whose tags are a superset of the provided tags.
func (enc *EventEncryptor) Forget(ctx context.Context, tags map[string]string) error {
	return enc.keys.Forget(ctx, tags)
}

// subjectTags extracts the subject-identifying tags from an event's tag set
// using the provided encryption key names. Returns nil if keys is empty.
func subjectTags(tags map[string]string, keys []string) map[string]string {
	if len(keys) == 0 {
		return nil
	}
	subject := make(map[string]string, len(keys))
	for _, name := range keys {
		if v, ok := tags[name]; ok {
			subject[name] = v
		}
	}
	if len(subject) == 0 {
		return nil
	}
	return subject
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
