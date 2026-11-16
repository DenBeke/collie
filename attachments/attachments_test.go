package attachments_test

import (
	"context"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/attachments"
	"github.com/DenBeke/collie/memory"
)

func newStore() (*collie.MiddlewareStore, *attachments.MemoryBlobStore) {
	blobs := attachments.NewMemoryBlobStore()
	att := attachments.NewAttachmentMiddleware(blobs)
	store := collie.NewMiddlewareStore(memory.New(),
		collie.WithAppendMiddleware(att.OnAppend),
		collie.WithReadMiddleware(att.OnRead),
	)
	return store, blobs
}

func collectEvents(t *testing.T, store collie.Store) []collie.SequencedEvent {
	t.Helper()
	ctx := context.Background()
	var events []collie.SequencedEvent
	stream, _, err := store.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for e, err := range stream {
		if err != nil {
			t.Fatalf("unexpected read error: %v", err)
		}
		events = append(events, e)
	}
	return events
}

func TestPassthroughWithoutAttachments(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	e, err := collie.NewEvent("OrderPlaced", map[string]string{"orderId": "123"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := collie.EventData[map[string]string](events[0])
	if err != nil {
		t.Fatal(err)
	}
	if data["orderId"] != "123" {
		t.Fatalf("expected orderId=123, got %s", data["orderId"])
	}
}

func TestAttachmentRoundTrip(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName string `json:"fileName"`
		Content  []byte `json:"content"`
	}
	doc := Doc{FileName: "report.pdf", Content: []byte("big-binary-data")}
	e, err := collie.NewEvent("DocumentUploaded", doc, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := collie.EventData[Doc](events[0])
	if err != nil {
		t.Fatal(err)
	}
	if data.FileName != "report.pdf" {
		t.Fatalf("expected report.pdf, got %s", data.FileName)
	}
	if string(data.Content) != "big-binary-data" {
		t.Fatalf("expected big-binary-data, got %s", string(data.Content))
	}
}

func TestAttachmentNotStoredInline(t *testing.T) {
	blobs := attachments.NewMemoryBlobStore()
	att := attachments.NewAttachmentMiddleware(blobs)
	raw := memory.New()
	store := collie.NewMiddlewareStore(raw,
		collie.WithAppendMiddleware(att.OnAppend),
		collie.WithReadMiddleware(att.OnRead),
	)
	ctx := context.Background()

	type Doc struct {
		FileName string `json:"fileName"`
		Content  string `json:"content"`
	}
	e, err := collie.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: "secret-content"}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Read through raw store — content should not be present inline
	var rawEvents []collie.SequencedEvent
	stream, _, err := raw.Read(ctx, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for ev, err := range stream {
		if err != nil {
			t.Fatal(err)
		}
		rawEvents = append(rawEvents, ev)
	}
	rawData := string(rawEvents[0].Data)
	if contains(rawData, "secret-content") {
		t.Fatal("attachment content should not be stored inline in event data")
	}
	if !contains(rawData, "fileName") {
		t.Fatal("non-attachment fields should remain in event data")
	}
}

func TestMissingBlobOnRead(t *testing.T) {
	store, blobs := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName string  `json:"fileName"`
		Content  *string `json:"content"`
	}
	content := "will-be-deleted"
	e, err := collie.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: &content}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Get the blob ID, then delete it
	events := collectEvents(t, store)
	ids := attachments.BlobIDs(events[0])
	blobID := ids["content"]
	if err := blobs.Delete(ctx, blobID); err != nil {
		t.Fatal(err)
	}

	// Re-read — should get null content and missing flag
	events = collectEvents(t, store)
	if !attachments.IsMissing(events[0]) {
		t.Fatal("expected event to be marked as attachmentsMissing")
	}
	data, err := collie.EventData[Doc](events[0])
	if err != nil {
		t.Fatal(err)
	}
	if data.Content != nil {
		t.Fatal("expected content to be nil when blob is missing")
	}
	if data.FileName != "f.txt" {
		t.Fatalf("expected fileName preserved, got %s", data.FileName)
	}
}

func TestBlobDelete(t *testing.T) {
	store, blobs := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName string  `json:"fileName"`
		Content  *string `json:"content"`
	}
	content := "to-be-erased"
	e, err := collie.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: &content}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	blobID := attachments.BlobIDs(events[0])["content"]
	if blobID == "" {
		t.Fatal("expected blob ID in metadata")
	}

	// Delete via the BlobStore interface — proves the method is on the interface.
	var iface attachments.BlobStore = blobs
	if err := iface.Delete(ctx, blobID); err != nil {
		t.Fatalf("first delete: %v", err)
	}

	// Re-reading reports the blob as missing.
	events = collectEvents(t, store)
	if !attachments.IsMissing(events[0]) {
		t.Fatal("expected event to be marked as attachmentsMissing after Delete")
	}

	// Idempotency: deleting the same id again is not an error.
	if err := iface.Delete(ctx, blobID); err != nil {
		t.Fatalf("second delete should be a no-op, got error: %v", err)
	}

	// Idempotency: deleting an unknown id is not an error.
	if err := iface.Delete(ctx, "never-stored"); err != nil {
		t.Fatalf("delete of unknown id should be a no-op, got error: %v", err)
	}
}

func TestBlobIDsAccessible(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName string `json:"fileName"`
		Content  string `json:"content"`
	}
	e, err := collie.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: "data"}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	ids := attachments.BlobIDs(events[0])
	if ids == nil {
		t.Fatal("expected blob IDs in metadata")
	}
	if ids["content"] == "" {
		t.Fatal("expected content blob ID")
	}
}

func TestMultipleAttachmentFields(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName  string `json:"fileName"`
		Content   []byte `json:"content"`
		Thumbnail []byte `json:"thumbnail"`
	}
	doc := Doc{FileName: "photo.jpg", Content: []byte("image-bytes"), Thumbnail: []byte("thumb-bytes")}
	e, err := collie.NewEvent("PhotoUploaded", doc, nil,
		attachments.WithAttachments("content", "thumbnail"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	data, err := collie.EventData[Doc](events[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(data.Content) != "image-bytes" {
		t.Fatalf("expected image-bytes, got %s", string(data.Content))
	}
	if string(data.Thumbnail) != "thumb-bytes" {
		t.Fatalf("expected thumb-bytes, got %s", string(data.Thumbnail))
	}

	ids := attachments.BlobIDs(events[0])
	if len(ids) != 2 {
		t.Fatalf("expected 2 blob IDs, got %d", len(ids))
	}
}

func TestMissingFieldOnAppendErrors(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	e, err := collie.NewEvent("Test", map[string]string{"a": "b"}, nil,
		attachments.WithAttachments("nonExistent"),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Append(ctx, []collie.Event{e}, nil)
	if err == nil {
		t.Fatal("expected error for missing field, got nil")
	}
}

func TestMixedAttachmentAndPlainEvents(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	plain, _ := collie.NewEvent("SystemBooted", "v1.0", nil)
	withAtt, _ := collie.NewEvent("DocUploaded", map[string]string{"fileName": "f.txt", "content": "data"}, nil,
		attachments.WithAttachments("content"),
	)
	if err := store.Append(ctx, []collie.Event{plain, withAtt}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Plain event should be untouched
	data, err := collie.EventData[string](events[0])
	if err != nil || data != "v1.0" {
		t.Fatalf("plain event: data=%s, err=%v", data, err)
	}
	if attachments.IsMissing(events[0]) {
		t.Fatal("plain event should not be marked as missing")
	}

	// Attachment event should be restored
	attData, err := collie.EventData[map[string]string](events[1])
	if err != nil {
		t.Fatal(err)
	}
	if attData["content"] != "data" {
		t.Fatalf("expected content=data, got %s", attData["content"])
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
