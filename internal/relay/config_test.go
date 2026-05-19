package relay

import (
	"os"
	"testing"
)

func TestLoadConfigFromEnvRejectsInvalidDuration(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_PAIRING_TTL": "invalid",
	})

	if _, err := LoadConfigFromEnv(); err == nil {
		t.Fatal("expected invalid relay duration to fail")
	}
}

func TestLoadConfigFromEnvRejectsInvalidLimit(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_MAX_CONNS_PER_IP": "invalid",
	})

	if _, err := LoadConfigFromEnv(); err == nil {
		t.Fatal("expected invalid relay connection limit to fail")
	}
}

func TestLoadConfigFromEnvRejectsInvalidByteLimit(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_MAX_CONTROL_FRAME_BYTES": "invalid",
	})

	if _, err := LoadConfigFromEnv(); err == nil {
		t.Fatal("expected invalid relay byte limit to fail")
	}
}

func withRelayEnv(t *testing.T, values map[string]string) {
	t.Helper()
	for key, value := range values {
		prev, ok := os.LookupEnv(key)
		t.Setenv(key, value)
		if ok {
			t.Cleanup(func() { t.Setenv(key, prev) })
		}
	}
}
