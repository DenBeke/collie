package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/DenBeke/collie"
)

// MetadataKey is the metadata key used to store encryption key names.
const MetadataKey = "encryptionKeys"

// FieldsMetadataKey is the metadata key used to list JSON field names that
// should be encrypted individually instead of encrypting the whole payload.
const FieldsMetadataKey = "encryptedFields"

// ErrRedacted is a convenience error for consumers to return when encountering
// redacted events. For full-blob encryption Data is nil; for field-level
// encryption Data is preserved but encrypted fields are set to JSON null.
var ErrRedacted = errors.New("event data redacted")

// ErrMissingTag is returned when an encryption key name does not exist as a
// tag on the event.
var ErrMissingTag = errors.New("encryption key references missing tag")

// ErrMissingField is returned when an encrypted field name does not exist as
// a top-level key in the JSON-encoded event data.
var ErrMissingField = errors.New("encrypted field not found in event data")

// WithEncryptionKeys marks the event for GDPR crypto-shredding. The provided
// names must each exist as a key in the event's Tags; their values identify
// the data subject whose encryption key protects this event's Data.
// Validation is performed by the OnAppend middleware.
func WithEncryptionKeys(keys ...string) collie.EventOption {
	return func(e *collie.Event) {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata[MetadataKey] = strings.Join(keys, ",")
	}
}

// WithEncryptedFields limits encryption to specific top-level JSON fields
// instead of encrypting the entire payload. Must be combined with
// WithEncryptionKeys. Each name must correspond to a top-level key in the
// JSON-encoded event data.
func WithEncryptedFields(fields ...string) collie.EventOption {
	return func(e *collie.Event) {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata[FieldsMetadataKey] = strings.Join(fields, ",")
	}
}

// IsRedacted returns true if the event was crypto-shredded by the encryption
// middleware (the subject's key was forgotten).
func IsRedacted(e collie.SequencedEvent) bool {
	return e.Metadata["redacted"] == "true"
}

// EventEncryptor transparently encrypts and decrypts event data for GDPR
// compliance. Use OnAppend and OnRead as middleware with MiddlewareStore.
type EventEncryptor struct {
	keys KeyStore
}

// NewEventEncryptor creates an EventEncryptor backed by the given KeyStore. It
// also registers the struct-tag hook (see RegisterStructTags), so that a
// dcb-encrypt declaration is never ignored once an encryptor exists. Create it
// at boot, before any event is built.
func NewEventEncryptor(keys KeyStore) *EventEncryptor {
	RegisterStructTags()
	return &EventEncryptor{keys: keys}
}

// OnAppend is an AppendMiddleware that encrypts event Data if the event has
// encryption keys set in its Metadata. When encrypted fields are specified,
// only those JSON fields are encrypted; otherwise the entire payload is
// encrypted.
func (enc *EventEncryptor) OnAppend(ctx context.Context, e *collie.Event) error {
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

	fields := encryptedFields(e.Metadata)
	if len(fields) > 0 {
		return encryptFields(key, e, fields)
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
// forgotten, Data is set to nil for full-blob encryption or fields are set
// to null for field-level encryption.
func (enc *EventEncryptor) OnRead(ctx context.Context, e *collie.SequencedEvent) error {
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

	fields := encryptedFields(e.Metadata)

	if !ok {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata["redacted"] = "true"
		if len(fields) > 0 {
			return redactFields(e, fields)
		}
		e.Data = nil
		return nil
	}

	if len(fields) > 0 {
		return decryptFields(key, e, fields)
	}

	plaintext, err := decrypt(key, e.Data)
	if err != nil {
		return fmt.Errorf("could not decrypt event data: %w", err)
	}
	e.Data = plaintext
	return nil
}

// Forget deletes all keys whose subject is a superset of the provided tags.
func (enc *EventEncryptor) Forget(ctx context.Context, tags []collie.Tag) error {
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
// using the provided encryption key names. A name may carry multiple values
// (e.g. a message between two users), all of which become part of the subject.
// Returns ErrMissingTag if a name has no value on the event.
func subjectTags(tags []collie.Tag, keys []string) ([]collie.Tag, error) {
	var subject []collie.Tag
	for _, name := range keys {
		before := len(subject)
		for _, t := range tags {
			if t.Key == name {
				subject = append(subject, t)
			}
		}
		if len(subject) == before {
			return nil, fmt.Errorf("%w: %q", ErrMissingTag, name)
		}
	}
	return subject, nil
}

// encryptedFields reads the field names to encrypt from event metadata.
func encryptedFields(metadata map[string]string) []string {
	v, ok := metadata[FieldsMetadataKey]
	if !ok || v == "" {
		return nil
	}
	return strings.Split(v, ",")
}

// encryptFields encrypts specific JSON fields in the event data.
func encryptFields(key []byte, e *collie.Event, fields []string) error {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("could not parse event data for field encryption: %w", err)
	}
	for _, field := range fields {
		raw, exists := data[field]
		if !exists {
			return fmt.Errorf("%w: %q", ErrMissingField, field)
		}
		ciphertext, err := encrypt(key, []byte(raw))
		if err != nil {
			return fmt.Errorf("could not encrypt field %q: %w", field, err)
		}
		encoded, err := json.Marshal(base64.StdEncoding.EncodeToString(ciphertext))
		if err != nil {
			return fmt.Errorf("could not encode encrypted field %q: %w", field, err)
		}
		data[field] = encoded
	}
	newData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("could not re-marshal event data: %w", err)
	}
	e.Data = newData
	return nil
}

// decryptFields decrypts specific JSON fields in the event data.
func decryptFields(key []byte, e *collie.SequencedEvent, fields []string) error {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("could not parse event data for field decryption: %w", err)
	}
	for _, field := range fields {
		raw, exists := data[field]
		if !exists {
			continue
		}
		var encoded string
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return fmt.Errorf("could not decode encrypted field %q: %w", field, err)
		}
		ciphertext, err := base64.StdEncoding.DecodeString(encoded)
		if err != nil {
			return fmt.Errorf("could not base64-decode field %q: %w", field, err)
		}
		plaintext, err := decrypt(key, ciphertext)
		if err != nil {
			return fmt.Errorf("could not decrypt field %q: %w", field, err)
		}
		data[field] = json.RawMessage(plaintext)
	}
	newData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("could not re-marshal event data: %w", err)
	}
	e.Data = newData
	return nil
}

// redactFields sets encrypted fields to JSON null when the key is forgotten.
func redactFields(e *collie.SequencedEvent, fields []string) error {
	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("could not parse event data for redaction: %w", err)
	}
	for _, field := range fields {
		data[field] = json.RawMessage("null")
	}
	newData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("could not re-marshal event data: %w", err)
	}
	e.Data = newData
	return nil
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
