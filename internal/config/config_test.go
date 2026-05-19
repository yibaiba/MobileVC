package config

import (
	"os"
	"testing"
)

func TestLoadTTSValidation(t *testing.T) {
	preserveConfigEnv(t)

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

func preserveConfigEnv(t *testing.T) {
	t.Helper()
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
		"RELAY_MODE",
		"RELAY_URL",
		"RELAY_PAIRING_TTL",
		"RELAY_AGENT_GRACE_PERIOD",
		"RELAY_PAIRING_EVENT_PATH",
	}
	for _, key := range keys {
		preserveEnv(t, key)
	}
}

func preserveEnv(t *testing.T, key string) {
	t.Helper()
	prev, ok := os.LookupEnv(key)
	if ok {
		t.Cleanup(func() { os.Setenv(key, prev) })
		return
	}
	t.Cleanup(func() { os.Unsetenv(key) })
}

func TestLoadTrustedFileRoots(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN":                 "test",
		"RUNTIME_TRUSTED_FILE_ROOTS": "/tmp/shared" + string(os.PathListSeparator) + "/var/log",
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if len(cfg.Runtime.TrustedFileRoots) != 2 {
		t.Fatalf("TrustedFileRoots: got %#v", cfg.Runtime.TrustedFileRoots)
	}
	if cfg.Runtime.TrustedFileRoots[0] != "/tmp/shared" {
		t.Fatalf("first trusted root: got %q", cfg.Runtime.TrustedFileRoots[0])
	}
	if cfg.Runtime.TrustedFileRoots[1] != "/var/log" {
		t.Fatalf("second trusted root: got %q", cfg.Runtime.TrustedFileRoots[1])
	}
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

func TestLoadRelayModeValidatesURL(t *testing.T) {
	valid := []string{
		"wss://relay.example.com",
		"ws://127.0.0.1:9000",
		"ws://localhost:9000",
		"ws://192.168.1.10:9000",
	}
	for _, relayURL := range valid {
		t.Run(relayURL, func(t *testing.T) {
			withEnv(t, map[string]string{
				"AUTH_TOKEN":               "test",
				"RELAY_MODE":               "true",
				"RELAY_URL":                relayURL,
				"RELAY_PAIRING_EVENT_PATH": "/tmp/mobilevc-pairing.json",
			})
			cfg, err := Load()
			if err != nil {
				t.Fatalf("Load failed: %v", err)
			}
			if !cfg.Relay.Enabled || cfg.Relay.URL != relayURL {
				t.Fatalf("Relay config mismatch: %#v", cfg.Relay)
			}
		})
	}
}

func TestLoadRelayModeRejectsUnsafeURL(t *testing.T) {
	invalid := []string{
		"",
		"http://relay.example.com",
		"https://relay.example.com",
		"ws://relay.example.com",
		"wss://relay.example.com/path",
	}
	for _, relayURL := range invalid {
		t.Run(relayURL, func(t *testing.T) {
			withEnv(t, map[string]string{
				"AUTH_TOKEN":               "test",
				"RELAY_MODE":               "true",
				"RELAY_URL":                relayURL,
				"RELAY_PAIRING_EVENT_PATH": "/tmp/mobilevc-pairing.json",
			})
			if _, err := Load(); err == nil {
				t.Fatal("expected relay validation error")
			}
		})
	}
}

func TestLoadRelayModeRequiresPairingEventPath(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN": "test",
		"RELAY_MODE": "true",
		"RELAY_URL":  "wss://relay.example.com",
	})

	if _, err := Load(); err == nil {
		t.Fatal("expected relay pairing event path validation error")
	}
}

func TestLoadRelayModeRejectsInvalidDuration(t *testing.T) {
	withEnv(t, map[string]string{
		"AUTH_TOKEN":               "test",
		"RELAY_MODE":               "true",
		"RELAY_URL":                "wss://relay.example.com",
		"RELAY_PAIRING_TTL":        "not-a-duration",
		"RELAY_PAIRING_EVENT_PATH": "/tmp/mobilevc-pairing.json",
	})

	if _, err := Load(); err == nil {
		t.Fatal("expected relay duration validation error")
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
