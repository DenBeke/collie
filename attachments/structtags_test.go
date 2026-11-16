package attachments_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/DenBeke/collie"
	"github.com/DenBeke/collie/attachments"
	"github.com/DenBeke/collie/memory"
)

func init() { attachments.RegisterStructTags() }

type documentUploaded struct {
	DocumentID string `json:"documentId" dcb:"tag:document"`
	FileName   string `json:"fileName"`
	Content    []byte `json:"content" dcb-attachment:"blob"`
}

type scan struct {
	Front []byte `json:"front"`
	Back  []byte `json:"back"`
}

type idScanned struct {
	UserID string `json:"userId" dcb:"tag:user"`
	Scan   scan   `json:"scan" dcb-attachment:"blob"`
}

func TestStructTagsDeclareWhatOptionsDeclare(t *testing.T) {
	data := documentUploaded{DocumentID: "d1", FileName: "a.pdf", Content: []byte("pdf")}
	declared, err := collie.NewEventFrom("DocumentUploaded", data)
	if err != nil {
		t.Fatal(err)
	}
	byOption, err := collie.NewEvent("DocumentUploaded", data, nil, attachments.WithAttachments("content"))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := declared.Metadata[attachments.FieldsMetadataKey], byOption.Metadata[attachments.FieldsMetadataKey]; got != want {
		t.Errorf("%s = %q, option gives %q", attachments.FieldsMetadataKey, got, want)
	}
	if _, err := collie.NewEventFrom("DocumentUploaded", data, attachments.WithAttachments("content")); err != nil {
		t.Errorf("agreeing option: %v", err)
	}
}

func TestStructTagsRoundTrip(t *testing.T) {
	raw := memory.New()
	blobs := attachments.NewMemoryBlobStore()
	att := attachments.NewAttachmentMiddleware(blobs)
	store := collie.NewMiddlewareStore(raw,
		collie.WithAppendMiddleware(att.OnAppend),
		collie.WithReadMiddleware(att.OnRead),
	)
	ctx := context.Background()

	doc, err := collie.NewEventFrom("DocumentUploaded", documentUploaded{DocumentID: "d1", FileName: "a.pdf", Content: []byte("pdf-bytes")})
	if err != nil {
		t.Fatal(err)
	}
	id, err := collie.NewEventFrom("IDScanned", idScanned{UserID: "u1", Scan: scan{Front: []byte("f"), Back: []byte("b")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Append(ctx, []collie.Event{doc, id}, nil); err != nil {
		t.Fatal(err)
	}

	events := collectEvents(t, store, nil)
	gotDoc, err := collie.EventData[documentUploaded](events[0])
	if err != nil || !bytes.Equal(gotDoc.Content, []byte("pdf-bytes")) {
		t.Errorf("document = %+v, %v", gotDoc, err)
	}
	gotScan, err := collie.EventData[idScanned](events[1])
	if err != nil || string(gotScan.Scan.Back) != "b" {
		t.Errorf("scan = %+v, %v", gotScan, err)
	}
	ids := attachments.BlobIDs(events[1])
	if len(ids) != 1 || ids["scan"] == "" {
		t.Fatalf("blob ids = %v, want one for scan", ids)
	}
	if blob, err := blobs.Get(ctx, ids["scan"]); err != nil || !bytes.Contains(blob, []byte("front")) {
		t.Errorf("blob = %s, %v", blob, err)
	}

	// Inline, the offloaded field is gone from Data.
	atRest := collectEvents(t, raw, nil)[0]
	if bytes.Contains(atRest.Data, []byte("content")) {
		t.Errorf("stored data still holds the attachment: %s", atRest.Data)
	}
}

type (
	badValue struct {
		Content []byte `json:"content" dcb-attachment:"true"`
	}
	taggedBlob struct {
		ID string `json:"id" dcb:"tag:id" dcb-attachment:"blob"`
	}
	hiddenBlob struct {
		Content []byte `json:"-" dcb-attachment:"blob"`
	}
	embeddedBlob struct {
		scan `dcb-attachment:"blob"`
	}
	unexportedBlob struct {
		content []byte `dcb-attachment:"blob"`
	}
	absentBlob struct {
		*scanBlock
	}
	scanBlock struct {
		Scan scan `json:"scan" dcb-attachment:"blob"`
	}
)

func TestStructTagsRejectInvalidDeclarations(t *testing.T) {
	cases := []struct {
		name    string
		payload any
		opts    []collie.EventOption
	}{
		{"value other than blob", badValue{}, nil},
		{"tag field as blob", taggedBlob{ID: "x"}, nil},
		{`json:"-" field`, hiddenBlob{}, nil},
		{"embedded field declared", embeddedBlob{scan: scan{Front: []byte("f")}}, nil},
		{"unexported field declared", unexportedBlob{content: []byte("c")}, nil},
		{"field behind nil embedded pointer", absentBlob{}, nil},
		{"option disagrees", documentUploaded{DocumentID: "d", Content: []byte("c")}, []collie.EventOption{attachments.WithAttachments("fileName")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := collie.NewEvent("X", tc.payload, nil, tc.opts...)
			if !errors.Is(err, attachments.ErrInvalidStructTags) {
				t.Errorf("err = %v, want ErrInvalidStructTags", err)
			}
		})
	}
}
