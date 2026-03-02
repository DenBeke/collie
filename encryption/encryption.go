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

// GDPRTag is the tag key that marks an event as containing personal data.
// Its value is a comma-separated list of tag names that identify the data subject.
// For example, gdpr:"userId" means the encryption key is derived from the userId tag.
const GDPRTag = "gdpr"

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

// Seal encrypts the event's Data if it has a gdpr tag. Events without
// the tag are returned unchanged.
func (enc *EventEncryptor) Seal(ctx context.Context, e eventstore.Event) (eventstore.Event, error) {
	subject := subjectTags(e.Tags)
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
		Type: e.Type,
		Data: ciphertext,
		Tags: e.Tags,
	}, nil
}

// Open decrypts the event's Data if it has a gdpr tag. If the subject's key
// has been forgotten, the event is returned with Data set to nil.
// Events without the tag are returned unchanged.
func (enc *EventEncryptor) Open(ctx context.Context, e eventstore.SequencedEvent) (eventstore.SequencedEvent, error) {
	subject := subjectTags(e.Tags)
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

// subjectTags extracts the subject-identifying tags from an event's tag set.
// It reads the gdpr tag value as a comma-separated list of tag names and
// returns a map of those tags and their values. Returns nil if no gdpr tag.
func subjectTags(tags map[string]string) map[string]string {
	gdprValue, ok := tags[GDPRTag]
	if !ok {
		return nil
	}
	subject := make(map[string]string)
	for _, name := range strings.Split(gdprValue, ",") {
		name = strings.TrimSpace(name)
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
