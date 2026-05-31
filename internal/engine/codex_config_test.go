package engine

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCodexConfigDefaultsReadsCodexHome(t *testing.T) {
	codexHome := t.TempDir()
	t.Setenv("CODEX_HOME", codexHome)

	config := `model_provider = "custom"
model = "gpt-5.5"
model_reasoning_effort = "xhigh"

[features]
model = "ignored"
`
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte(config), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	defaults, err := loadCodexConfigDefaults()
	if err != nil {
		t.Fatalf("load defaults: %v", err)
	}
	if defaults.model != "gpt-5.5" {
		t.Fatalf("expected model default, got %q", defaults.model)
	}
	if defaults.reasoningEffort != "xhigh" {
		t.Fatalf("expected reasoning effort default, got %q", defaults.reasoningEffort)
	}
}

func TestLoadCodexConfigDefaultsAllowsMissingConfig(t *testing.T) {
	t.Setenv("CODEX_HOME", t.TempDir())

	defaults, err := loadCodexConfigDefaults()
	if err != nil {
		t.Fatalf("load missing defaults: %v", err)
	}
	if defaults != (codexConfigDefaults{}) {
		t.Fatalf("expected empty defaults for missing config, got %#v", defaults)
	}
}

func TestParseCodexConfigAssignmentIgnoresCommentsInQuotes(t *testing.T) {
	key, value, ok := parseCodexConfigAssignment(`model = "gpt-5.5#preview" # real comment`)
	if !ok {
		t.Fatal("expected assignment")
	}
	if key != "model" || value != "gpt-5.5#preview" {
		t.Fatalf("unexpected assignment: key=%q value=%q", key, value)
	}
}

func TestNormalizeCodexSandboxMode(t *testing.T) {
	for _, mode := range []string{"", "workspace-write", "read-only", "danger-full-access"} {
		got, err := normalizeCodexSandboxMode(mode)
		if err != nil {
			t.Fatalf("normalize sandbox %q: %v", mode, err)
		}
		if mode == "" {
			if got != "workspace-write" {
				t.Fatalf("expected empty sandbox to default to workspace-write, got %q", got)
			}
			continue
		}
		if got != mode {
			t.Fatalf("expected sandbox %q, got %q", mode, got)
		}
	}
}

func TestNormalizeCodexSandboxModeRejectsInvalidValue(t *testing.T) {
	if _, err := normalizeCodexSandboxMode("full"); err == nil {
		t.Fatal("expected invalid sandbox mode to fail")
	}
}
