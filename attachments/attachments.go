// Package attachments offloads large event fields to an external blob store.
package attachments

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"eventstore"
)

// FieldsMetadataKey is the metadata key set by WithAttachments to list field
// names that should be offloaded. Consumed by OnAppend.
const FieldsMetadataKey = "attachmentFields"

// MappingMetadataKey is the metadata key set by OnAppend to store the
// field→blobID mapping as JSON. Read by OnRead and available to consumers.
const MappingMetadataKey = "attachments"

// MissingMetadataKey is set to "true" by OnRead when one or more blobs
// could not be retrieved from the blob store.
const MissingMetadataKey = "attachmentsMissing"

// BlobStore is the interface for storing and retrieving binary data.
type BlobStore interface {
	Put(ctx context.Context, data []byte) (string, error)
	Get(ctx context.Context, id string) ([]byte, error)
	// Delete removes a blob by ID. Idempotent: deleting an unknown ID is not
	// an error. The contract is "after Delete returns nil, the blob is not in
	// the store" — useful for GDPR erasure where the caller doesn't always
	// know which blobs were ever stored.
	Delete(ctx context.Context, id string) error
}

// WithAttachments marks the listed JSON field names for offloading to a blob
// store. Each name must correspond to a top-level key in the JSON-encoded
// event data.
func WithAttachments(fields ...string) eventstore.EventOption {
	return func(e *eventstore.Event) {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata[FieldsMetadataKey] = strings.Join(fields, ",")
	}
}

// IsMissing returns true if one or more attachment blobs could not be
// retrieved on read.
func IsMissing(e eventstore.SequencedEvent) bool {
	return e.Metadata[MissingMetadataKey] == "true"
}

// BlobIDs returns the field→blobID mapping from the event's metadata.
// Returns nil if the event has no attachments.
func BlobIDs(e eventstore.SequencedEvent) map[string]string {
	v, ok := e.Metadata[MappingMetadataKey]
	if !ok || v == "" {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(v), &m); err != nil {
		return nil
	}
	return m
}

// AttachmentMiddleware transparently offloads event data fields to a blob
// store on append and restores them on read.
type AttachmentMiddleware struct {
	blobs BlobStore
}

// NewAttachmentMiddleware creates middleware backed by the given BlobStore.
func NewAttachmentMiddleware(blobs BlobStore) *AttachmentMiddleware {
	return &AttachmentMiddleware{blobs: blobs}
}

// OnAppend extracts attachment fields from event data, stores them in the
// blob store, and replaces them with blob IDs in metadata.
func (m *AttachmentMiddleware) OnAppend(ctx context.Context, e *eventstore.Event) error {
	raw, ok := e.Metadata[FieldsMetadataKey]
	if !ok || raw == "" {
		return nil
	}
	fields := strings.Split(raw, ",")

	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("attachments: could not parse event data: %w", err)
	}

	mapping := make(map[string]string, len(fields))
	for _, field := range fields {
		value, exists := data[field]
		if !exists {
			return fmt.Errorf("attachments: field %q not found in event data", field)
		}
		id, err := m.blobs.Put(ctx, []byte(value))
		if err != nil {
			return fmt.Errorf("attachments: could not store blob for field %q: %w", field, err)
		}
		mapping[field] = id
		delete(data, field)
	}

	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("attachments: could not marshal mapping: %w", err)
	}

	newData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("attachments: could not re-marshal event data: %w", err)
	}

	e.Data = newData
	delete(e.Metadata, FieldsMetadataKey)
	e.Metadata[MappingMetadataKey] = string(mappingJSON)
	return nil
}

// OnRead restores attachment fields from the blob store into event data.
// If a blob is missing, the field is set to JSON null and metadata is
// marked with attachmentsMissing.
func (m *AttachmentMiddleware) OnRead(ctx context.Context, e *eventstore.SequencedEvent) error {
	raw, ok := e.Metadata[MappingMetadataKey]
	if !ok || raw == "" {
		return nil
	}

	var mapping map[string]string
	if err := json.Unmarshal([]byte(raw), &mapping); err != nil {
		return fmt.Errorf("attachments: could not parse mapping: %w", err)
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(e.Data, &data); err != nil {
		return fmt.Errorf("attachments: could not parse event data: %w", err)
	}

	missing := false
	for field, id := range mapping {
		value, err := m.blobs.Get(ctx, id)
		if err != nil {
			data[field] = json.RawMessage("null")
			missing = true
			continue
		}
		data[field] = json.RawMessage(value)
	}

	newData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("attachments: could not re-marshal event data: %w", err)
	}

	e.Data = newData
	if missing {
		if e.Metadata == nil {
			e.Metadata = make(map[string]string)
		}
		e.Metadata[MissingMetadataKey] = "true"
	}
	return nil
}
