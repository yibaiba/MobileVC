package gateway

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"mobilevc/internal/protocol"
)

const (
	maxImageAttachmentBytes = 4 * 1024 * 1024
	maxImageAttachments     = 4
)

var allowedImageMIMETypes = map[string]string{
	"image/jpeg": ".jpg",
	"image/png":  ".png",
	"image/webp": ".webp",
	"image/gif":  ".gif",
}

func persistImageAttachments(ctx context.Context, sessionID string, attachments []protocol.ImageAttachment) ([]string, error) {
	if len(attachments) == 0 {
		return nil, nil
	}
	if len(attachments) > maxImageAttachments {
		return nil, fmt.Errorf("最多一次发送 %d 张图片", maxImageAttachments)
	}
	baseDir, err := attachmentBaseDir(sessionID)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(baseDir, 0o700); err != nil {
		return nil, fmt.Errorf("create attachment dir: %w", err)
	}
	paths := make([]string, 0, len(attachments))
	for index, attachment := range attachments {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		path, err := persistImageAttachment(baseDir, index, attachment)
		if err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, nil
}

func persistImageAttachment(baseDir string, index int, attachment protocol.ImageAttachment) (string, error) {
	extension, err := imageAttachmentExtension(attachment)
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(attachment.Data))
	if err != nil {
		raw, err = base64.RawStdEncoding.DecodeString(strings.TrimSpace(attachment.Data))
	}
	if err != nil {
		return "", fmt.Errorf("decode image attachment %d: %w", index+1, err)
	}
	if len(raw) == 0 || len(raw) > maxImageAttachmentBytes {
		return "", fmt.Errorf("图片 %d 大小必须在 1B 到 %dB 之间", index+1, maxImageAttachmentBytes)
	}
	name := fmt.Sprintf("%s-%02d-%s%s", time.Now().UTC().Format("20060102T150405Z"), index+1, uuid.NewString(), extension)
	path := filepath.Join(baseDir, name)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return "", fmt.Errorf("write image attachment: %w", err)
	}
	return path, nil
}

func imageAttachmentExtension(attachment protocol.ImageAttachment) (string, error) {
	mimeType := strings.ToLower(strings.TrimSpace(attachment.MIMEType))
	extension, ok := allowedImageMIMETypes[mimeType]
	if !ok {
		return "", fmt.Errorf("不支持的图片类型：%s", fallback(mimeType, "<empty>"))
	}
	if nameExt := strings.ToLower(filepath.Ext(attachment.Name)); nameExt != "" && extensionAllowedForMIME(mimeType, nameExt) {
		return nameExt, nil
	}
	return extension, nil
}

func extensionAllowedForMIME(mimeType string, extension string) bool {
	switch mimeType {
	case "image/jpeg":
		return extension == ".jpg" || extension == ".jpeg"
	default:
		return allowedImageMIMETypes[mimeType] == extension
	}
}

func attachmentBaseDir(sessionID string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("resolve user home for attachments: %w", err)
	}
	return filepath.Join(home, ".mobilevc", "attachments", safeAttachmentSessionID(sessionID)), nil
}

func safeAttachmentSessionID(sessionID string) string {
	trimmed := strings.TrimSpace(sessionID)
	if trimmed == "" {
		return "unknown"
	}
	replacer := strings.NewReplacer("/", "_", "\\", "_", ":", "_", "..", "_")
	return replacer.Replace(trimmed)
}

func appendAttachmentPathPrompt(input string, imagePaths []string) string {
	if len(imagePaths) == 0 {
		return input
	}
	lines := []string{strings.TrimRight(input, "\n"), "", "Attached local image files:"}
	for _, path := range imagePaths {
		lines = append(lines, "- "+path)
	}
	return strings.TrimLeft(strings.Join(lines, "\n"), "\n") + "\n"
}
