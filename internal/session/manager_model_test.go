package session

import "testing"

func TestDetectRuntimeModelDoesNotInventCodexModel(t *testing.T) {
	if got := detectRuntimeModel("codex", "codex"); got != "" {
		t.Fatalf("expected empty codex model without explicit flag, got %q", got)
	}
	if got := detectRuntimeModel("codex -m gpt-5.5", "codex"); got != "gpt-5.5" {
		t.Fatalf("expected explicit codex model, got %q", got)
	}
}
