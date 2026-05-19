package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"mobilevc/internal/fileaccess"
)

func TestServeDownloadAllowsWorkspaceFile(t *testing.T) {
	workspace := t.TempDir()
	filePath := filepath.Join(workspace, "session.log")
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newDownloadTestPolicy(t, workspace, nil)
	resp := requestDownload(t, policy, filePath)

	if resp.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d body=%q", resp.Code, http.StatusOK, resp.Body.String())
	}
	if resp.Body.String() != "ok" {
		t.Fatalf("body: got %q", resp.Body.String())
	}
}

func TestServeDownloadRejectsOutsidePath(t *testing.T) {
	workspace := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.txt")
	if err := os.WriteFile(outside, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newDownloadTestPolicy(t, workspace, nil)
	resp := requestDownload(t, policy, outside)

	if resp.Code != http.StatusForbidden {
		t.Fatalf("status: got %d, want %d body=%q", resp.Code, http.StatusForbidden, resp.Body.String())
	}
}

func TestServeDownloadAllowsTrustedAdditionalRoot(t *testing.T) {
	workspace := t.TempDir()
	trusted := t.TempDir()
	filePath := filepath.Join(trusted, "shared.txt")
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	policy := newDownloadTestPolicy(t, workspace, []string{trusted})
	resp := requestDownload(t, policy, filePath)

	if resp.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d body=%q", resp.Code, http.StatusOK, resp.Body.String())
	}
}

func newDownloadTestPolicy(t *testing.T, workspace string, trusted []string) fileaccess.Policy {
	t.Helper()
	policy, err := fileaccess.NewPolicy(workspace, trusted)
	if err != nil {
		t.Fatalf("NewPolicy failed: %v", err)
	}
	return policy
}

func requestDownload(t *testing.T, policy fileaccess.Policy, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/download?token=test&path="+path, nil)
	resp := httptest.NewRecorder()
	serveDownload(resp, req, "test", policy)
	return resp
}
