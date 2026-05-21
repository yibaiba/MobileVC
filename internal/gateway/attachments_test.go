package gateway

import (
	"context"
	"encoding/base64"
	"os"
	"testing"

	"mobilevc/internal/protocol"
)

func TestPersistImageAttachmentsWritesOwnerOnlyFile(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	paths, err := persistImageAttachments(context.Background(), "session/test", []protocol.ImageAttachment{
		{
			Name:     "screen.png",
			MIMEType: "image/png",
			Data:     base64.StdEncoding.EncodeToString([]byte("png-bytes")),
		},
	})
	if err != nil {
		t.Fatalf("persist image attachments: %v", err)
	}
	if len(paths) != 1 {
		t.Fatalf("paths count: got %d want 1", len(paths))
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatalf("read persisted image: %v", err)
	}
	if string(raw) != "png-bytes" {
		t.Fatalf("persisted bytes: got %q", string(raw))
	}
	info, err := os.Stat(paths[0])
	if err != nil {
		t.Fatalf("stat persisted image: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("file mode: got %o want 600", info.Mode().Perm())
	}
}

func TestPersistImageAttachmentsRejectsUnsupportedMIME(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	_, err := persistImageAttachments(context.Background(), "session-1", []protocol.ImageAttachment{
		{
			Name:     "note.txt",
			MIMEType: "text/plain",
			Data:     base64.StdEncoding.EncodeToString([]byte("nope")),
		},
	})
	if err == nil {
		t.Fatal("expected unsupported mime type to fail")
	}
}

func TestAppendAttachmentPathPrompt(t *testing.T) {
	got := appendAttachmentPathPrompt("请分析\n", []string{"/tmp/a.png", "/tmp/b.jpg"})
	want := "请分析\n\nAttached local image files:\n- /tmp/a.png\n- /tmp/b.jpg\n"
	if got != want {
		t.Fatalf("prompt:\ngot  %q\nwant %q", got, want)
	}
}
