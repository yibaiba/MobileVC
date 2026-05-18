package config

import (
	"os"
	"testing"
)

func TestLoadTTSValidation(t *testing.T) {
	oldEnv := os.Environ()
	_ = oldEnv
	keys := []string{
		"AUTH_TOKEN",
		"TTS_ENABLED",
		"TTS_PROVIDER",
		"TTS_PYTHON_SERVICE_URL",
		"TTS_REQUEST_TIMEOUT_SECONDS",
		"TTS_MAX_TEXT_LENGTH",
		"TTS_DEFAULT_FORMAT",
		"PUBLIC_EXPOSURE_MODE",
		"ALLOWED_ORIGINS",
	}
	for _, key := range keys {
		prev, ok := os.LookupEnv(key)
		if ok {
			defer os.Setenv(key, prev)
		} else {
			defer os.Unsetenv(key)
		}
	}

	t.Run("disabled tts passes", func(t *testing.T) {
		os.Setenv("AUTH_TOKEN", "test")
		os.Setenv("TTS_ENABLED", "false")
		if _, err := Load(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("enabled tts with invalid provider fails", func(t *testing.T) {
		os.Setenv("AUTH_TOKEN", "test")
		os.Setenv("TTS_ENABLED", "true")
		os.Setenv("TTS_PROVIDER", "other")
		os.Setenv("TTS_PYTHON_SERVICE_URL", "http://127.0.0.1:9966")
		os.Setenv("TTS_REQUEST_TIMEOUT_SECONDS", "30")
		os.Setenv("TTS_MAX_TEXT_LENGTH", "200")
		os.Setenv("TTS_DEFAULT_FORMAT", "wav")
		if _, err := Load(); err == nil {
			t.Fatal("expected validation error")
		}
	})

	t.Run("enabled tts with invalid format fails", func(t *testing.T) {
		os.Setenv("AUTH_TOKEN", "test")
		os.Setenv("TTS_ENABLED", "true")
		os.Setenv("TTS_PROVIDER", "chattts-http")
		os.Setenv("TTS_PYTHON_SERVICE_URL", "http://127.0.0.1:9966")
		os.Setenv("TTS_REQUEST_TIMEOUT_SECONDS", "30")
		os.Setenv("TTS_MAX_TEXT_LENGTH", "200")
		os.Setenv("TTS_DEFAULT_FORMAT", "mp3")
		if _, err := Load(); err == nil {
			t.Fatal("expected validation error")
		}
	})
}

func TestLoadPublicExposureModeRequiresAllowedOrigins(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN":           "test",
		"PUBLIC_EXPOSURE_MODE": "true",
		"ALLOWED_ORIGINS":      "",
	})

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
	if err.Error() != "ALLOWED_ORIGINS is required when PUBLIC_EXPOSURE_MODE is true" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadPublicExposureModeAllowedOrigins(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN":           "test",
		"PUBLIC_EXPOSURE_MODE": "true",
		"ALLOWED_ORIGINS":      "https://example.test:443, http://127.0.0.1:80",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if !cfg.Security.PublicExposureMode {
		t.Fatal("PublicExposureMode should be true")
	}
	want := []string{"https://example.test", "http://127.0.0.1"}
	if len(cfg.Security.AllowedOrigins) != len(want) {
		t.Fatalf("AllowedOrigins: got %#v", cfg.Security.AllowedOrigins)
	}
	for i, origin := range want {
		if cfg.Security.AllowedOrigins[i] != origin {
			t.Fatalf("AllowedOrigins[%d]: got %q, want %q", i, cfg.Security.AllowedOrigins[i], origin)
		}
	}
}

func TestLoadRejectsInvalidAllowedOrigin(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN":           "test",
		"PUBLIC_EXPOSURE_MODE": "true",
		"ALLOWED_ORIGINS":      "https://example.test/path",
	})

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func withEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for key, value := range values {
		prev, ok := os.LookupEnv(key)
		if ok {
			t.Cleanup(func() { os.Setenv(key, prev) })
		} else {
			t.Cleanup(func() { os.Unsetenv(key) })
		}
		os.Setenv(key, value)
	}
}
