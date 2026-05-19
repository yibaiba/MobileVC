package config

import (
	"fmt"
	"strings"
)

func (c Config) validateSecurity() error {
	if c.Security.PublicExposureMode && len(c.Security.AllowedOrigins) == 0 {
		return fmt.Errorf("ALLOWED_ORIGINS is required when PUBLIC_EXPOSURE_MODE is true")
	}
	for _, origin := range c.Security.AllowedOrigins {
		if _, err := NormalizeOrigin(origin); err != nil {
			return err
		}
	}
	return nil
}

func (c Config) validateRelay() error {
	if !c.Relay.Enabled {
		return nil
	}
	if strings.TrimSpace(c.Relay.URL) == "" {
		return fmt.Errorf("RELAY_URL is required when RELAY_MODE is true")
	}
	if err := ValidateRelayURL(c.Relay.URL); err != nil {
		return err
	}
	if strings.TrimSpace(c.Relay.PairingEventPath) == "" {
		return fmt.Errorf("RELAY_PAIRING_EVENT_PATH is required when RELAY_MODE is true")
	}
	if c.Relay.PairingTTL <= 0 || c.Relay.AgentGracePeriod <= 0 {
		return fmt.Errorf("relay durations must be positive")
	}
	return nil
}

func (c Config) validateTTS() error {
	if !c.TTS.Enabled {
		return nil
	}
	if c.TTS.Provider != "chattts-http" {
		return fmt.Errorf("TTS_PROVIDER must be chattts-http")
	}
	if strings.TrimSpace(c.TTS.PythonServiceURL) == "" {
		return fmt.Errorf("TTS_PYTHON_SERVICE_URL is required when TTS is enabled")
	}
	if c.TTS.RequestTimeoutSeconds <= 0 {
		return fmt.Errorf("TTS_REQUEST_TIMEOUT_SECONDS must be greater than 0")
	}
	if c.TTS.MaxTextLength <= 0 {
		return fmt.Errorf("TTS_MAX_TEXT_LENGTH must be greater than 0")
	}
	if strings.ToLower(strings.TrimSpace(c.TTS.DefaultFormat)) != "wav" {
		return fmt.Errorf("TTS_DEFAULT_FORMAT must be wav")
	}
	return nil
}
