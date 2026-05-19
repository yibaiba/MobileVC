package relay

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAddr                    = ":9000"
	defaultPublicURL               = "ws://127.0.0.1:9000"
	defaultPairingTTL              = 5 * time.Minute
	defaultAgentGracePeriod        = 60 * time.Second
	defaultHandshakeTimeout        = 10 * time.Second
	defaultAgentRegisterTimeout    = 10 * time.Second
	defaultMaxPairingFailuresPerIP = 5
	defaultMaxSessions             = 1000
	defaultMaxAgentConns           = 1000
	defaultMaxClientConns          = 2000
	defaultMaxConnsPerIP           = 20
	defaultPingInterval            = 30 * time.Second
	defaultPongTimeout             = 10 * time.Second
	defaultMaxControlFrameBytes    = 16 * 1024
	defaultForwardQueueSize        = 64
	MaxPayloadBytes                = 1024 * 1024
)

type Config struct {
	Addr                    string
	PublicURL               string
	PairingTTL              time.Duration
	AgentGracePeriod        time.Duration
	PairingHandshakeTimeout time.Duration
	AgentRegisterTimeout    time.Duration
	MaxPairingFailuresPerIP int
	MaxSessions             int
	MaxAgentConns           int
	MaxClientConns          int
	MaxConnsPerIP           int
	PingInterval            time.Duration
	PongTimeout             time.Duration
	MaxControlFrameBytes    int64
	ForwardQueueSize        int
	TrustedProxyCIDRs       string
}

func LoadConfigFromEnv() (Config, error) {
	cfg, err := loadConfigFromEnv()
	if err != nil {
		return Config{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadConfigFromEnv() (Config, error) {
	pairingTTL, err := getEnvDuration("RELAY_PAIRING_TTL", defaultPairingTTL)
	if err != nil {
		return Config{}, err
	}
	grace, err := getEnvDuration("RELAY_AGENT_GRACE_PERIOD", defaultAgentGracePeriod)
	if err != nil {
		return Config{}, err
	}
	limits, err := loadLimitConfigFromEnv()
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Addr:              getEnv("RELAY_ADDR", defaultAddr),
		PublicURL:         getEnv("RELAY_PUBLIC_URL", defaultPublicURL),
		PairingTTL:        pairingTTL,
		AgentGracePeriod:  grace,
		TrustedProxyCIDRs: strings.TrimSpace(os.Getenv("RELAY_TRUSTED_PROXY_CIDRS")),
	}
	applyLimitConfig(&cfg, limits)
	return cfg, nil
}

type limitConfig struct {
	PairingHandshakeTimeout time.Duration
	AgentRegisterTimeout    time.Duration
	MaxPairingFailuresPerIP int
	MaxSessions             int
	MaxAgentConns           int
	MaxClientConns          int
	MaxConnsPerIP           int
	PingInterval            time.Duration
	PongTimeout             time.Duration
	MaxControlFrameBytes    int64
	ForwardQueueSize        int
}

func loadLimitConfigFromEnv() (limitConfig, error) {
	cfg := limitConfig{}
	var err error
	cfg.PairingHandshakeTimeout, err = getEnvDuration("RELAY_PAIRING_HANDSHAKE_TIMEOUT", defaultHandshakeTimeout)
	if err != nil {
		return limitConfig{}, err
	}
	return loadRemainingLimitConfig(cfg)
}

func loadRemainingLimitConfig(cfg limitConfig) (limitConfig, error) {
	loaders := []func(*limitConfig) error{
		loadRegisterTimeout,
		loadConnectionLimits,
		loadPingSettings,
		loadFrameLimits,
	}
	for _, load := range loaders {
		if err := load(&cfg); err != nil {
			return limitConfig{}, err
		}
	}
	return cfg, nil
}

func applyLimitConfig(cfg *Config, limits limitConfig) {
	cfg.PairingHandshakeTimeout = limits.PairingHandshakeTimeout
	cfg.AgentRegisterTimeout = limits.AgentRegisterTimeout
	cfg.MaxPairingFailuresPerIP = limits.MaxPairingFailuresPerIP
	cfg.MaxSessions = limits.MaxSessions
	cfg.MaxAgentConns = limits.MaxAgentConns
	cfg.MaxClientConns = limits.MaxClientConns
	cfg.MaxConnsPerIP = limits.MaxConnsPerIP
	cfg.PingInterval = limits.PingInterval
	cfg.PongTimeout = limits.PongTimeout
	cfg.MaxControlFrameBytes = limits.MaxControlFrameBytes
	cfg.ForwardQueueSize = limits.ForwardQueueSize
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Addr) == "" {
		return fmt.Errorf("RELAY_ADDR is required")
	}
	if err := ValidateRelayURL(c.PublicURL); err != nil {
		return fmt.Errorf("RELAY_PUBLIC_URL: %w", err)
	}
	if c.PairingTTL <= 0 || c.AgentGracePeriod <= 0 {
		return fmt.Errorf("relay ttl and grace period must be positive")
	}
	if c.ForwardQueueSize <= 0 || c.MaxControlFrameBytes <= 0 {
		return fmt.Errorf("relay frame limits must be positive")
	}
	if c.MaxSessions <= 0 || c.MaxAgentConns <= 0 || c.MaxClientConns <= 0 || c.MaxConnsPerIP <= 0 {
		return fmt.Errorf("relay connection limits must be positive")
	}
	if c.PingInterval <= 0 || c.PongTimeout <= 0 {
		return fmt.Errorf("relay ping settings must be positive")
	}
	return nil
}

func getEnv(key string, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnvDuration(key string, fallback time.Duration) (time.Duration, error) {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			return 0, fmt.Errorf("%s must be a valid duration: %w", key, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func getEnvInt(key string, fallback int) (int, error) {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("%s must be a valid integer: %w", key, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func getEnvBytes(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSuffix(value, "B"))
	if err == nil {
		return parsed, nil
	}
	if strings.HasSuffix(strings.ToLower(value), "kib") {
		return parseByteUnit(value, "KiB", 1024)
	}
	return 0, fmt.Errorf("%s must be bytes or KiB", key)
}

func parseByteUnit(value string, suffix string, unit int) (int, error) {
	number := strings.TrimSpace(strings.TrimSuffix(value, suffix))
	parsed, err := strconv.Atoi(number)
	if err != nil {
		return 0, err
	}
	return parsed * unit, nil
}
