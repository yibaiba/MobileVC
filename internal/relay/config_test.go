package relay

import "testing"

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

func TestLoadConfigFromEnvReadsPayloadByteLimit(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_MAX_PAYLOAD_BYTES": "12KiB",
	})

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.MaxPayloadBytes != 12*1024 {
		t.Fatalf("payload bytes: got %d", cfg.MaxPayloadBytes)
	}
}

func TestLoadConfigFromEnvReadsDefaultRouteAllowlists(t *testing.T) {
	withRelayEnv(t, map[string]string{})

	cfg, err := LoadConfigFromEnv()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if !routeAllowed(cfg.HTTPAllowedRoutes, "GET", "/healthz") {
		t.Fatal("default HTTP allowlist should include GET /healthz")
	}
	if !routeAllowed(cfg.WSAllowedRoutes, "GET", "/ws") {
		t.Fatal("default WS allowlist should include GET /ws")
	}
	if routeAllowed(cfg.HTTPAllowedRoutes, "POST", "/healthz") {
		t.Fatal("default HTTP allowlist should reject wrong method")
	}
}

func TestLoadConfigFromEnvRejectsInvalidRouteAllowlist(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_HTTP_ALLOWLIST": "GET:healthz",
	})

	if _, err := LoadConfigFromEnv(); err == nil {
		t.Fatal("expected invalid route allowlist to fail")
	}
}

func TestValidateRejectsEmptyRouteAllowlist(t *testing.T) {
	cfg := baseTestRelayConfig()
	cfg.HTTPAllowedRoutes = nil
	cfg.WSAllowedRoutes = nil

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected empty route allowlist validation error")
	}
}

func TestLoadConfigAppliesOverrides(t *testing.T) {
	withRelayEnv(t, map[string]string{
		"RELAY_ADDR":       ":9000",
		"RELAY_PUBLIC_URL": "ws://127.0.0.1:9000",
	})

	cfg, err := LoadConfig(Overrides{
		Addr:              ":9443",
		PublicURL:         "wss://relay.example.test",
		MaxAgentConns:     7,
		MaxClientConns:    8,
		MaxConnsPerIP:     3,
		ForwardQueueSize:  9,
		HTTPAllowlist:     "GET:/healthz",
		WSAllowlist:       "GET:/ws",
		TrustedProxyCIDRs: "127.0.0.1/32",
	})
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg.Addr != ":9443" ||
		cfg.PublicURL != "wss://relay.example.test" ||
		cfg.MaxAgentConns != 7 ||
		cfg.MaxClientConns != 8 ||
		cfg.MaxConnsPerIP != 3 ||
		cfg.ForwardQueueSize != 9 ||
		cfg.TrustedProxyCIDRs != "127.0.0.1/32" {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if !routeAllowed(cfg.HTTPAllowedRoutes, "GET", "/healthz") ||
		routeAllowed(cfg.HTTPAllowedRoutes, "GET", "/version") {
		t.Fatalf("unexpected HTTP allowlist: %#v", cfg.HTTPAllowedRoutes)
	}
}

func withRelayEnv(t *testing.T, values map[string]string) {
	t.Helper()
	keys := []string{
		"RELAY_ADDR",
		"RELAY_PUBLIC_URL",
		"RELAY_PAIRING_TTL",
		"RELAY_MAX_CONNS_PER_IP",
		"RELAY_MAX_CONTROL_FRAME_BYTES",
		"RELAY_MAX_PAYLOAD_BYTES",
		"RELAY_HTTP_ALLOWLIST",
		"RELAY_WS_ALLOWLIST",
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	for key, value := range values {
		t.Setenv(key, value)
	}
}
