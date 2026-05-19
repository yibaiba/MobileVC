package fileaccess

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPolicyResolveAllowsWorkspaceFile(t *testing.T) {
	workspace := t.TempDir()
	filePath := filepath.Join(workspace, "logs", "session.txt")
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, nil)

	resolved, err := policy.Resolve(filePath)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if resolved != realPath(t, filePath) {
		t.Fatalf("resolved path: got %q, want %q", resolved, realPath(t, filePath))
	}
}

func TestPolicyResolveRejectsParentEscape(t *testing.T) {
	parent := t.TempDir()
	workspace := filepath.Join(parent, "workspace")
	if err := os.Mkdir(workspace, 0o755); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(parent, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, nil)

	if _, err := policy.Resolve("../secret.txt"); err == nil {
		t.Fatal("expected parent escape to be rejected")
	}
}

func TestPolicyResolveRejectsAbsoluteOutsideRoot(t *testing.T) {
	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, nil)

	if _, err := policy.Resolve(outside); err == nil {
		t.Fatal("expected absolute outside path to be rejected")
	}
}

func TestPolicyResolveRejectsSymlinkEscape(t *testing.T) {
	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	linkPath := filepath.Join(workspace, "secret-link")
	if err := os.Symlink(outside, linkPath); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, nil)

	if _, err := policy.Resolve(linkPath); err == nil {
		t.Fatal("expected symlink escape to be rejected")
	}
}

func TestPolicyResolveAllowsTrustedAdditionalRoot(t *testing.T) {
	workspace := t.TempDir()
	trusted := t.TempDir()
	filePath := filepath.Join(trusted, "allowed.txt")
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, []string{trusted})

	resolved, err := policy.Resolve(filePath)
	if err != nil {
		t.Fatalf("Resolve failed: %v", err)
	}
	if resolved != realPath(t, filePath) {
		t.Fatalf("resolved path: got %q, want %q", resolved, realPath(t, filePath))
	}
}

func TestPolicyWithClientTrustedRootsCombinesEnvAndClientRoots(t *testing.T) {
	workspace := t.TempDir()
	envRoot := t.TempDir()
	clientRoot := t.TempDir()
	envFile := filepath.Join(envRoot, "env.txt")
	clientFile := filepath.Join(clientRoot, "client.txt")
	if err := os.WriteFile(envFile, []byte("env"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(clientFile, []byte("client"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, []string{envRoot})
	nextPolicy, err := policy.WithClientTrustedRoots([]string{clientRoot})
	if err != nil {
		t.Fatalf("WithClientTrustedRoots failed: %v", err)
	}

	if _, err := nextPolicy.Resolve(envFile); err != nil {
		t.Fatalf("env root should remain trusted: %v", err)
	}
	if _, err := nextPolicy.Resolve(clientFile); err != nil {
		t.Fatalf("client root should be trusted: %v", err)
	}
}

func TestPolicyWithClientTrustedRootsRejectsInvalidRoot(t *testing.T) {
	workspace := t.TempDir()
	policy := newTestPolicy(t, workspace, nil)

	if _, err := policy.WithClientTrustedRoots([]string{
		filepath.Join(t.TempDir(), "missing"),
	}); err == nil {
		t.Fatal("expected invalid client root to be rejected")
	}
}

func TestPolicyWithClientTrustedRootsReplacesPreviousClientRoots(t *testing.T) {
	workspace := t.TempDir()
	envRoot := t.TempDir()
	firstClientRoot := t.TempDir()
	secondClientRoot := t.TempDir()
	envFile := writeTestFile(t, envRoot, "env.txt")
	firstFile := writeTestFile(t, firstClientRoot, "first.txt")
	secondFile := writeTestFile(t, secondClientRoot, "second.txt")
	policy := newTestPolicy(t, workspace, []string{envRoot})
	policyWithFirst, err := policy.WithClientTrustedRoots([]string{firstClientRoot})
	if err != nil {
		t.Fatalf("WithClientTrustedRoots first failed: %v", err)
	}
	policyWithSecond, err := policyWithFirst.WithClientTrustedRoots([]string{secondClientRoot})
	if err != nil {
		t.Fatalf("WithClientTrustedRoots second failed: %v", err)
	}

	if _, err := policyWithSecond.Resolve(envFile); err != nil {
		t.Fatalf("env root should remain trusted: %v", err)
	}
	if _, err := policyWithSecond.Resolve(secondFile); err != nil {
		t.Fatalf("second client root should be trusted: %v", err)
	}
	if _, err := policyWithSecond.Resolve(firstFile); err == nil {
		t.Fatal("expected previous client root to be removed")
	}
}

func TestPolicyResolveUsesWorkspaceForEmptyAndRelativePaths(t *testing.T) {
	workspace := t.TempDir()
	filePath := filepath.Join(workspace, "nested.txt")
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newTestPolicy(t, workspace, nil)

	resolvedRoot, err := policy.Resolve("")
	if err != nil {
		t.Fatalf("Resolve empty failed: %v", err)
	}
	if resolvedRoot != realPath(t, workspace) {
		t.Fatalf("empty path resolved to %q, want %q", resolvedRoot, realPath(t, workspace))
	}
	resolvedFile, err := policy.Resolve("nested.txt")
	if err != nil {
		t.Fatalf("Resolve relative failed: %v", err)
	}
	if resolvedFile != realPath(t, filePath) {
		t.Fatalf("relative path resolved to %q, want %q", resolvedFile, realPath(t, filePath))
	}
}

func newTestPolicy(t *testing.T, workspace string, trusted []string) Policy {
	t.Helper()
	policy, err := NewPolicy(workspace, trusted)
	if err != nil {
		t.Fatalf("NewPolicy failed: %v", err)
	}
	return policy
}

func realPath(t *testing.T, path string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	return resolved
}

func writeTestFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
