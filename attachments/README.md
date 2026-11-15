# Attachments / Blob offloading

Large event fields (files, images, binary payloads) can be offloaded to a blob store instead of being stored inline. The attachment middleware transparently extracts marked fields on append and restores them on read — consumers see the original struct unchanged.

## Setting up attachments

```go
blobStore := attachments.NewMemoryBlobStore() // or your own BlobStore implementation
attachmentMiddleware := attachments.NewAttachmentMiddleware(blobStore)
store := eventstore.NewMiddlewareStore(inner,
    eventstore.WithAppendMiddleware(attachmentMiddleware.OnAppend),
    eventstore.WithReadMiddleware(attachmentMiddleware.OnRead),
)
```

## Marking fields for offloading

Declare it on the field with `dcb-attachment:"blob"`:

```go
type DocumentUploaded struct {
    DocumentID string `json:"documentId" dcb:"tag:document"`
    FileName   string `json:"fileName"`
    Content    []byte `json:"content"    dcb-attachment:"blob"` // large, offloaded
}

event, _ := eventstore.NewEventFrom("DocumentUploaded", doc)
```

This needs `attachments.RegisterStructTags()` once at boot (see [Struct tags and hooks](../encryption/README.md#struct-tags-and-hooks)). Several fields can carry the tag; a nested struct is offloaded as one blob.

For events built without a struct, `WithAttachments` names the top-level JSON fields instead:

```go
event, _ := eventstore.NewEvent("PhotoUploaded", photo, tags,
    attachments.WithAttachments("content", "thumbnail"),
)
```

## Reading events with attachments

On read, the middleware restores the original data from the blob store. Consumers use `EventData[T]` as usual. If a blob cannot be retrieved, the field is set to `null` and the event is flagged — check with `attachments.IsMissing(event)`.

```go
data, _ := eventstore.EventData[DocumentUploaded](event)
fmt.Println(len(data.Content)) // original size
```

## Direct blob access

Blob IDs are preserved in event metadata for direct access (e.g. serving download URLs without loading the full blob through the event store):

```go
ids := attachments.BlobIDs(event) // map[string]string{"content": "blob-abc-123"}
url := fmt.Sprintf("/blobs/%s", ids["content"])
```

## Lazy loading

If you don't want blobs fetched on every read (e.g. for projections that only need metadata), skip registering the read middleware:

```go
store := eventstore.NewMiddlewareStore(inner,
    eventstore.WithAppendMiddleware(attachmentMiddleware.OnAppend),
    // no OnRead — blob IDs stay in metadata for manual access
)
```

## Custom BlobStore

Implement the `attachments.BlobStore` interface for any storage backend (S3, GCS, local filesystem, etc.):

```go
type BlobStore interface {
    Put(ctx context.Context, data []byte) (string, error) // returns blob ID
    Get(ctx context.Context, id string) ([]byte, error)
    Delete(ctx context.Context, id string) error          // idempotent
}
```

An in-memory implementation is provided for testing via `attachments.NewMemoryBlobStore()`.

`Delete` is the attachments analogue of `EventEncryptor.Forget`: it lets a GDPR-erasure handler crypto-shred the encrypted fields *and* physically delete the blob attachments for the same subject in one operation. It is idempotent — deleting an unknown id returns nil — so callers can erase without first checking which blobs exist.
