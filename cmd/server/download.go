package main

import (
	"errors"
	"fmt"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"mobilevc/internal/fileaccess"
)

func serveDownload(w http.ResponseWriter, r *http.Request, authToken string, policy fileaccess.Policy) {
	if r.URL.Query().Get("token") != authToken {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	target := strings.TrimSpace(r.URL.Query().Get("path"))
	if target == "" {
		http.Error(w, "path is required", http.StatusBadRequest)
		return
	}
	absPath, err := policy.Resolve(target)
	if err != nil {
		writeResolveError(w, err)
		return
	}
	if err := validateDownloadTarget(absPath); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filepath.Base(absPath)))
	w.Header().Set("Content-Type", downloadContentType(absPath))
	http.ServeFile(w, r, absPath)
}

func writeResolveError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, fileaccess.ErrOutsideRoot) {
		status = http.StatusForbidden
	}
	http.Error(w, err.Error(), status)
}

func validateDownloadTarget(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("file not found")
	}
	if info.IsDir() {
		return fmt.Errorf("path is a directory")
	}
	return nil
}

func downloadContentType(path string) string {
	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}
