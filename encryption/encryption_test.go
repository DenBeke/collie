package encryption_test

import (
	"context"
	"strings"
	"testing"

	"eventstore"
	"eventstore/encryption"
	"eventstore/memory"
)

func newEncryptor() *encryption.EventEncryptor {
	return encryption.NewEventEncryptor(encryption.NewMemoryKeyStore())
}

func newStore() (*eventstore.MiddlewareStore, *encryption.EventEncryptor) {
	enc := newEncryptor()
	store := eventstore.NewMiddlewareStore(memory.New(),
		eventstore.WithAppendMiddleware(enc.OnAppend),
		eventstore.WithReadMiddleware(enc.OnRead),
	)
	return store, enc
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

func TestPassthroughWithoutEncryptionKeys(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	e, err := eventstore.NewEvent("SystemBooted", "v1.0", nil)
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
	data, err := eventstore.EventData[string](events[0])
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if data != "v1.0" {
		t.Fatalf("expected v1.0, got %s", data)
	}
}

func TestEncryptedRoundTrip(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
	}
	userData := user{Name: "Alice", Email: "alice@example.com"}

	e, err := eventstore.NewEvent("UserSignedUp", userData, map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
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
	data, err := eventstore.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if data.Name != "Alice" || data.Email != "alice@example.com" {
		t.Fatalf("unexpected data: %+v", data)
	}
}

func TestEncryptedAtRest(t *testing.T) {
	// Create a store with encryption, and a raw store without it to verify
	// the data is actually encrypted at rest.
	enc := newEncryptor()
	raw := memory.New()
	encryptedStore := eventstore.NewMiddlewareStore(raw,
		eventstore.WithAppendMiddleware(enc.OnAppend),
		eventstore.WithReadMiddleware(enc.OnRead),
	)

	ctx := context.Background()

	e, err := eventstore.NewEvent("UserSignedUp", "secret-data", map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
	if err != nil {
		t.Fatal(err)
	}

	if err := encryptedStore.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Read through encrypted store — should get plaintext
	events := collectEvents(t, encryptedStore, nil)
	data, err := eventstore.EventData[string](events[0])
	if err != nil {
		t.Fatal(err)
	}
	if data != "secret-data" {
		t.Fatalf("expected secret-data, got %s", data)
	}

	// Read through raw store — should get ciphertext (not plaintext)
	rawEvents := collectEvents(t, raw, nil)
	if string(rawEvents[0].Data) == `"secret-data"` {
		t.Fatal("data should be encrypted at rest, but found plaintext in raw store")
	}
}

func TestForgetRedactsData(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	e, err := eventstore.NewEvent("UserSignedUp", "alice-data", map[string]string{
		"userId": "user-123",
	}, encryption.WithEncryptionKeys("userId"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Forget the user — called directly on the middleware
	if err := enc.Forget(ctx, map[string]string{"userId": "user-123"}); err != nil {
		t.Fatal(err)
	}

	// Read — event should still exist but with nil Data
	events := collectEvents(t, store, nil)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}

	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected event to be redacted after forget")
	}
	if events[0].Type != "UserSignedUp" {
		t.Fatalf("expected type preserved, got %s", events[0].Type)
	}
	if events[0].Tags["userId"] != "user-123" {
		t.Fatalf("expected tags preserved, got %v", events[0].Tags)
	}
	if events[0].SequenceNumber != 1 {
		t.Fatalf("expected sequence number preserved, got %d", events[0].SequenceNumber)
	}
}

func TestForgetDoesNotAffectOtherSubjects(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	e2, _ := eventstore.NewEvent("UserSignedUp", "bob", map[string]string{
		"userId": "user-2",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e1, e2}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected user-1 to be redacted")
	}

	data, err := eventstore.EventData[string](events[1])
	if err != nil {
		t.Fatalf("unexpected error reading user-2: %v", err)
	}
	if data != "bob" {
		t.Fatalf("expected bob, got %s", data)
	}
}

func TestSameSubjectSameKey(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	e2, _ := eventstore.NewEvent("OrderPlaced", "order-data", map[string]string{
		"userId":  "user-1",
		"orderId": "order-99",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e1, e2}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events = collectEvents(t, store, nil)
	for i, e := range events {
		if !encryption.IsRedacted(e) {
			t.Fatalf("event %d: expected to be redacted after forget", i)
		}
	}
}

func TestSupersetForgetWithMultiKeySubject(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	e, _ := eventstore.NewEvent("FriendRequest", "request-data", map[string]string{
		"senderId":   "user-1",
		"receiverId": "user-2",
	}, encryption.WithEncryptionKeys("senderId", "receiverId"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, _ := eventstore.EventData[string](events[0])
	if data != "request-data" {
		t.Fatalf("expected request-data, got %s", data)
	}

	if err := enc.Forget(ctx, map[string]string{"senderId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events = collectEvents(t, store, nil)
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected event to be redacted after superset forget")
	}
}

func TestAppendConditionWorksThroughEncryption(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	e1, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e1}, nil); err != nil {
		t.Fatal(err)
	}

	condition := &eventstore.AppendCondition{
		Query: eventstore.Query{
			eventstore.QueryItem{Types: []string{"UserSignedUp"}, Tags: map[string]string{"userId": "user-1"}},
		},
		After: 0,
	}

	e2, _ := eventstore.NewEvent("UserSignedUp", "alice-again", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	err := store.Append(ctx, []eventstore.Event{e2}, condition)
	if err != eventstore.ErrAppendConditionViolated {
		t.Fatalf("expected ErrAppendConditionViolated, got %v", err)
	}
}

func TestMixedEncryptedAndPlainEvents(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	plain, _ := eventstore.NewEvent("SystemBooted", "v1.0", nil)
	encrypted, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{plain, encrypted}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	data, err := eventstore.EventData[string](events[0])
	if err != nil || data != "v1.0" {
		t.Fatalf("plain event should be readable: data=%s, err=%v", data, err)
	}

	if !encryption.IsRedacted(events[1]) {
		t.Fatal("expected encrypted event to be redacted after forget")
	}
}

func TestNoMiddlewarePassthrough(t *testing.T) {
	// Store without middleware should work as plain store
	store := memory.New()
	ctx := context.Background()

	e, _ := eventstore.NewEvent("TestEvent", "data", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, err := eventstore.EventData[string](events[0])
	if err != nil || data != "data" {
		t.Fatalf("expected plaintext passthrough: data=%s, err=%v", data, err)
	}
}

func TestEncryptionKeysMustReferenceExistingTags(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	e, _ := eventstore.NewEvent("UserSignedUp", "alice", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("nonExistentTag"))

	err := store.Append(ctx, []eventstore.Event{e}, nil)
	if err == nil {
		t.Fatal("expected error for missing tag, got nil")
	}
}

// --- Field-level encryption tests ---

func TestFieldLevelWithoutEncryptionKeysIsNoOp(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type user struct {
		Name string `json:"name"`
	}

	// WithEncryptedFields alone (no WithEncryptionKeys) should pass through
	e, _ := eventstore.NewEvent("UserSignedUp", user{Name: "Alice"}, nil,
		encryption.WithEncryptedFields("name"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, err := eventstore.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.Name != "Alice" {
		t.Fatalf("expected Alice, got %s", data.Name)
	}
}

func TestFieldLevelRoundTrip(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	u := user{Name: "Alice", Email: "alice@example.com", Role: "admin"}

	e, err := eventstore.NewEvent("UserSignedUp", u, map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("name", "email"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, err := eventstore.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}
	if data.Name != "Alice" || data.Email != "alice@example.com" || data.Role != "admin" {
		t.Fatalf("unexpected data: %+v", data)
	}
}

func TestFieldLevelEncryptedAtRest(t *testing.T) {
	enc := newEncryptor()
	raw := memory.New()
	store := eventstore.NewMiddlewareStore(raw,
		eventstore.WithAppendMiddleware(enc.OnAppend),
		eventstore.WithReadMiddleware(enc.OnRead),
	)
	ctx := context.Background()

	type user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Role  string `json:"role"`
	}
	u := user{Name: "Alice", Email: "alice@example.com", Role: "admin"}

	e, _ := eventstore.NewEvent("UserSignedUp", u, map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("email"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	// Raw store should have "role" in plaintext but "email" should be opaque
	rawEvents := collectEvents(t, raw, nil)
	rawData := string(rawEvents[0].Data)
	if !strings.Contains(rawData, `"role":"admin"`) {
		t.Fatalf("expected non-encrypted field 'role' in plaintext, got: %s", rawData)
	}
	if strings.Contains(rawData, "alice@example.com") {
		t.Fatal("encrypted field 'email' should not appear as plaintext in raw store")
	}
}

func TestFieldLevelForgetRedactsOnlyEncryptedFields(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	type user struct {
		Name  string  `json:"name"`
		Email *string `json:"email"`
		Role  string  `json:"role"`
	}
	email := "alice@example.com"
	u := user{Name: "Alice", Email: &email, Role: "admin"}

	e, _ := eventstore.NewEvent("UserSignedUp", u, map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("name", "email"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected event to be marked as redacted")
	}
	// Data should NOT be nil — non-encrypted fields are preserved
	if events[0].Data == nil {
		t.Fatal("expected Data to be non-nil for field-level redaction")
	}

	data, err := eventstore.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.Role != "admin" {
		t.Fatalf("expected non-encrypted field preserved, got role=%s", data.Role)
	}
	if data.Name != "" {
		t.Fatalf("expected redacted field to be zero value, got name=%s", data.Name)
	}
	if data.Email != nil {
		t.Fatalf("expected redacted field to be nil, got email=%v", data.Email)
	}
}

func TestFieldLevelNestedStructRoundTrip(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}
	type user struct {
		Name    string  `json:"name"`
		Address address `json:"address"`
		Role    string  `json:"role"`
	}
	u := user{
		Name:    "Alice",
		Address: address{Street: "123 Main St", City: "Springfield"},
		Role:    "admin",
	}

	e, _ := eventstore.NewEvent("UserSignedUp", u, map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("address"))
	if err := store.Append(ctx, []eventstore.Event{e}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	data, err := eventstore.EventData[user](events[0])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.Address.Street != "123 Main St" || data.Address.City != "Springfield" {
		t.Fatalf("expected nested struct preserved, got: %+v", data.Address)
	}
}

func TestFieldLevelMissingFieldError(t *testing.T) {
	store, _ := newStore()
	ctx := context.Background()

	type user struct {
		Name string `json:"name"`
	}

	e, _ := eventstore.NewEvent("UserSignedUp", user{Name: "Alice"}, map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("nonExistent"))

	err := store.Append(ctx, []eventstore.Event{e}, nil)
	if err == nil {
		t.Fatal("expected error for missing field, got nil")
	}
}

func TestMixedFullBlobAndFieldLevelEvents(t *testing.T) {
	store, enc := newStore()
	ctx := context.Background()

	type user struct {
		Name  string `json:"name"`
		Email string `json:"email"`
		Role  string `json:"role"`
	}

	// Full-blob encryption
	fullBlob, _ := eventstore.NewEvent("UserSignedUp", "full-secret", map[string]string{
		"userId": "user-1",
	}, encryption.WithEncryptionKeys("userId"))

	// Field-level encryption
	fieldLevel, _ := eventstore.NewEvent("ProfileUpdated", user{Name: "Alice", Email: "a@b.c", Role: "admin"}, map[string]string{
		"userId": "user-2",
	}, encryption.WithEncryptionKeys("userId"), encryption.WithEncryptedFields("email"))

	if err := store.Append(ctx, []eventstore.Event{fullBlob, fieldLevel}, nil); err != nil {
		t.Fatal(err)
	}

	// Both readable before forget
	events := collectEvents(t, store, nil)
	if len(events) != 2 {
		t.Fatalf("expected 2 events, got %d", len(events))
	}

	// Forget user-1 (full-blob)
	if err := enc.Forget(ctx, map[string]string{"userId": "user-1"}); err != nil {
		t.Fatal(err)
	}

	events = collectEvents(t, store, nil)

	// Full-blob: Data should be nil
	if !encryption.IsRedacted(events[0]) {
		t.Fatal("expected full-blob event to be redacted")
	}
	if events[0].Data != nil {
		t.Fatal("expected full-blob Data to be nil")
	}

	// Field-level: should still be fully readable (different subject)
	data, err := eventstore.EventData[user](events[1])
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if data.Email != "a@b.c" {
		t.Fatalf("expected field-level event unaffected, got email=%s", data.Email)
	}
}

