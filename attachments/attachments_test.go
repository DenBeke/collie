package attachments_test

import (
	"context"
	"testing"

	"eventstore"
	"eventstore/attachments"
	"eventstore/memory"
)

func newStore() (*eventstore.MiddlewareStore, *attachments.MemoryBlobStore) {
	blobs := attachments.NewMemoryBlobStore()
	att := attachments.NewAttachmentMiddleware(blobs)
	store := eventstore.NewMiddlewareStore(memory.New(),
		eventstore.WithAppendMiddleware(att.OnAppend),
		eventstore.WithReadMiddleware(att.OnRead),
	)
	return store, blobs
}

func collectEvents(t *testing.T, store eventstore.Store, query eventstore.Query) []eventstore.SequencedEvent {
	t.Helper()
	ctx := context.Background()
	var events []eventstore.SequencedEvent
	for e, err := range store.Read(ctx, query, 0) {
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

	e, err := eventstore.NewEvent("OrderPlaced", map[string]string{"orderId": "123"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := eventstore.EventData[map[string]string](events[0])
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
	e, err := eventstore.NewEvent("DocumentUploaded", doc, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	data, err := eventstore.EventData[Doc](events[0])
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
	store := eventstore.NewMiddlewareStore(raw,
		eventstore.WithAppendMiddleware(att.OnAppend),
		eventstore.WithReadMiddleware(att.OnRead),
	)
	ctx := context.Background()

	type Doc struct {
		FileName string `json:"fileName"`
		Content  string `json:"content"`
	}
	e, err := eventstore.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: "secret-content"}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Read through raw store — content should not be present inline
	var rawEvents []eventstore.SequencedEvent
	for ev, err := range raw.Read(ctx, nil, 0) {
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
	e, err := eventstore.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: &content}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Get the blob ID, then delete it
	events := collectEvents(t, store, nil)
	ids := attachments.BlobIDs(events[0])
	blobID := ids["content"]
	blobs.Delete(blobID)

	// Re-read — should get null content and missing flag
	events = collectEvents(t, store, nil)
	if !attachments.IsMissing(events[0]) {
		t.Fatal("expected event to be marked as attachmentsMissing")
	}
	data, err := eventstore.EventData[Doc](events[0])
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

func TestBlobIDsAccessible(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type Doc struct {
		FileName string `json:"fileName"`
		Content  string `json:"content"`
	}
	e, err := eventstore.NewEvent("DocumentUploaded", Doc{FileName: "f.txt", Content: "data"}, nil,
		attachments.WithAttachments("content"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
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
	e, err := eventstore.NewEvent("PhotoUploaded", doc, nil,
		attachments.WithAttachments("content", "thumbnail"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, err := eventstore.EventData[Doc](events[0])
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

	e, err := eventstore.NewEvent("Test", map[string]string{"a": "b"}, nil,
		attachments.WithAttachments("nonExistent"),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Append(ctx, []eventstore.Event{e}, nil)
	if err == nil {
		t.Fatal("expected error for missing field, got nil")
	}
}

func TestMixedAttachmentAndPlainEvents(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	plain, _ := eventstore.NewEvent("SystemBooted", "v1.0", nil)
	withAtt, _ := eventstore.NewEvent("DocUploaded", map[string]string{"fileName": "f.txt", "content": "data"}, nil,
		attachments.WithAttachments("content"),
	)
	if err := store.Append(ctx, []eventstore.Event{plain, withAtt}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Plain event should be untouched
	data, err := eventstore.EventData[string](events[0])
	if err != nil || data != "v1.0" {
		t.Fatalf("plain event: data=%s, err=%v", data, err)
	}
	if attachments.IsMissing(events[0]) {
		t.Fatal("plain event should not be marked as missing")
	}

	// Attachment event should be restored
	attData, err := eventstore.EventData[map[string]string](events[1])
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
