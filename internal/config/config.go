package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

type RuntimeConfig struct {
	DefaultCommand         string
	DefaultMode            string
	Debug                  bool
	WorkspaceRoot          string
	TrustedFileRoots       []string
	EnhancedProjection     bool
	EnableStepProjection   bool
	EnableDiffProjection   bool
	EnablePromptProjection bool
}

type TTSConfig struct {
	Enabled               bool
	Provider              string
	PythonServiceURL      string
	RequestTimeoutSeconds int
	MaxTextLength         int
	DefaultFormat         string
}

type SecurityConfig struct {
	PublicExposureMode bool
	AllowedOrigins     []string
}

type RelayConfig struct {
	Enabled          bool
	URL              string
	PairingTTL       time.Duration
	AgentGracePeriod time.Duration
	PairingEventPath string
}

type Config struct {
	Port      string
	AuthToken string
	Runtime   RuntimeConfig
	TTS       TTSConfig
	Security  SecurityConfig
	Relay     RelayConfig
}

type Summary struct {
	Port                   string
	AuthTokenConfigured    bool
	DefaultCommand         string
	DefaultMode            string
	Debug                  bool
	WorkspaceRoot          string
	TrustedFileRoots       []string
	EnhancedProjection     bool
	EnableStepProjection   bool
	EnableDiffProjection   bool
	EnablePromptProjection bool
	TTSEnabled             bool
	TTSProvider            string
	TTSPythonServiceURL    string
	TTSRequestTimeout      int
	TTSMaxTextLength       int
	TTSDefaultFormat       string
	PublicExposureMode     bool
	AllowedOrigins         []string
	RelayMode              bool
	RelayURL               string
}

func Load() (Config, error) {
	relayCfg, err := loadRelayConfig()
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		Port:      getEnv("PORT", "8001"),
		AuthToken: os.Getenv("AUTH_TOKEN"),
		Runtime:   loadRuntimeConfig(),
		TTS:       loadTTSConfig(),
		Security:  loadSecurityConfig(),
		Relay:     relayCfg,
	}

	if cfg.AuthToken == "" {
		return Config{}, fmt.Errorf("AUTH_TOKEN is required")
	}
	if err := cfg.validateTTS(); err != nil {
		return Config{}, err
	}
	if err := cfg.validateSecurity(); err != nil {
		return Config{}, err
	}
	if err := cfg.validateRelay(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) Summary() Summary {
	return Summary{
		Port:                   c.Port,
		AuthTokenConfigured:    strings.TrimSpace(c.AuthToken) != "",
		DefaultCommand:         c.Runtime.DefaultCommand,
		DefaultMode:            c.Runtime.DefaultMode,
		Debug:                  c.Runtime.Debug,
		WorkspaceRoot:          c.Runtime.WorkspaceRoot,
		TrustedFileRoots:       append([]string(nil), c.Runtime.TrustedFileRoots...),
		EnhancedProjection:     c.Runtime.EnhancedProjection,
		EnableStepProjection:   c.Runtime.EnableStepProjection,
		EnableDiffProjection:   c.Runtime.EnableDiffProjection,
		EnablePromptProjection: c.Runtime.EnablePromptProjection,
		TTSEnabled:             c.TTS.Enabled,
		TTSProvider:            c.TTS.Provider,
		TTSPythonServiceURL:    c.TTS.PythonServiceURL,
		TTSRequestTimeout:      c.TTS.RequestTimeoutSeconds,
		TTSMaxTextLength:       c.TTS.MaxTextLength,
		TTSDefaultFormat:       c.TTS.DefaultFormat,
		PublicExposureMode:     c.Security.PublicExposureMode,
		AllowedOrigins:         append([]string(nil), c.Security.AllowedOrigins...),
		RelayMode:              c.Relay.Enabled,
		RelayURL:               c.Relay.URL,
	}
}
