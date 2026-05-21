package relayclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"mobilevc/internal/logx"
	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

type Config struct {
	RelayURL           string
	PairingTTL         time.Duration
	AgentGracePeriod   time.Duration
	PairingEventPath   string
	ReconnectBackoff   ReconnectBackoff
	Capabilities       e2ee.CapabilitySet
	NodeFingerprintHex string
	NodeIdentity       *e2ee.NodeIdentity
	DeviceTrust        *e2ee.DeviceTrustStore
}

type ReconnectBackoff struct {
	Initial time.Duration
	Max     time.Duration
}

type PairingReadyEvent struct {
	Type               string             `json:"type"`
	RelayURL           string             `json:"relayUrl"`
	SessionID          string             `json:"sessionId"`
	PairingSecret      string             `json:"pairingSecret"`
	ExpiresAt          int64              `json:"expiresAt"`
	Capabilities       e2ee.CapabilitySet `json:"capabilities"`
	NodeFingerprintHex string             `json:"nodeFingerprintHex"`
}

type LocalPairingEmitter func(string, PairingReadyEvent) error

const relayControlTimeout = 10 * time.Second

type agentRegisterRequest struct {
	SessionID       string
	PairSecret      string
	ReconnectSecret string
	ExpiresAt       time.Time
	Capabilities    e2ee.CapabilitySet
}

type agentReconnectRequest struct {
	SessionID       string
	ReconnectSecret string
}

func Run(ctx context.Context, cfg Config, handler Handler, emit LocalPairingEmitter) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	pairingSecret, err := relay.NewSecret()
	if err != nil {
		return err
	}
	reconnectSecret, err := relay.NewSecret()
	if err != nil {
		return err
	}
	sessionID := "rs_" + uuid.NewString()
	expiresAt := time.Now().Add(cfg.PairingTTL)
	req := agentRegisterRequest{
		SessionID:       sessionID,
		PairSecret:      pairingSecret,
		ReconnectSecret: reconnectSecret,
		ExpiresAt:       expiresAt,
		Capabilities:    relayClientCapabilities(cfg),
	}
	conn, err := connectAndRegister(ctx, cfg, req)
	if err != nil {
		return err
	}
	if err := emit(cfg.PairingEventPath, PairingReadyEvent{
		Type:               "mobilevc.relay.pairing_ready",
		RelayURL:           cfg.RelayURL,
		SessionID:          sessionID,
		PairingSecret:      pairingSecret,
		ExpiresAt:          expiresAt.Unix(),
		Capabilities:       req.Capabilities,
		NodeFingerprintHex: strings.TrimSpace(cfg.NodeFingerprintHex),
	}); err != nil {
		_ = conn.Close()
		return err
	}
	defer removePairingEventFile(cfg.PairingEventPath)
	return serveWithReconnect(ctx, cfg, handler, conn, sessionID, pairingSecret, reconnectSecret)
}

func relayClientCapabilities(cfg Config) e2ee.CapabilitySet {
	if cfg.Capabilities.RelayProtocolVersion != 0 {
		return cfg.Capabilities
	}
	return e2ee.PlaintextTestCapabilities()
}

func validateConfig(cfg Config) error {
	if err := relay.ValidateRelayURL(cfg.RelayURL); err != nil {
		return err
	}
	if cfg.PairingTTL <= 0 || cfg.AgentGracePeriod <= 0 {
		return fmt.Errorf("relay pairing ttl and grace period must be positive")
	}
	if strings.TrimSpace(cfg.PairingEventPath) == "" {
		return fmt.Errorf("relay pairing event path is required")
	}
	if !isFingerprintHex(cfg.NodeFingerprintHex) {
		return fmt.Errorf("relay node fingerprint is required")
	}
	if err := e2ee.ValidateProductionCapabilities(relayClientCapabilities(cfg)); err == nil && cfg.NodeIdentity == nil {
		return fmt.Errorf("relay node identity is required for e2ee mode")
	}
	return nil
}

func isFingerprintHex(value string) bool {
	normalized := strings.TrimSpace(value)
	if len(normalized) != 64 {
		return false
	}
	for _, char := range normalized {
		if (char >= '0' && char <= '9') ||
			(char >= 'a' && char <= 'f') ||
			(char >= 'A' && char <= 'F') {
			continue
		}
		return false
	}
	return true
}

func connectAndRegister(ctx context.Context, cfg Config, req agentRegisterRequest) (*websocket.Conn, error) {
	conn, err := dialAgent(ctx, cfg.RelayURL)
	if err != nil {
		return nil, err
	}
	if err := registerAgent(conn, req); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func dialAgent(ctx context.Context, rawURL string) (*websocket.Conn, error) {
	endpoint, err := relayEndpoint(rawURL, "/relay/agent")
	if err != nil {
		return nil, err
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("connect relay agent websocket: %w", err)
	}
	return conn, nil
}

func relayEndpoint(rawURL string, path string) (string, error) {
	parsed, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return "", err
	}
	parsed.Path = path
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func registerAgent(conn *websocket.Conn, req agentRegisterRequest) error {
	frame := relay.AgentRegisterFrame{
		Type:                     relay.TypeAgentRegister,
		Version:                  relay.Version,
		SessionID:                req.SessionID,
		PairingSecretHash:        relay.SecretHash(req.PairSecret),
		AgentReconnectSecretHash: relay.SecretHash(req.ReconnectSecret),
		PairingExpiresAt:         req.ExpiresAt.Unix(),
		Capabilities:             &req.Capabilities,
	}
	if err := writeControlJSON(conn, frame); err != nil {
		return fmt.Errorf("send relay agent registration: %w", err)
	}
	var registered relay.AgentRegisteredFrame
	if err := readControlJSON(conn, &registered); err != nil {
		return fmt.Errorf("read relay agent registration response: %w", err)
	}
	if registered.Type != relay.TypeAgentRegistered || registered.SessionID != req.SessionID {
		return fmt.Errorf("relay agent registration rejected")
	}
	return nil
}

func reconnectAgent(conn *websocket.Conn, req agentReconnectRequest) error {
	frame := relay.AgentReconnectFrame{
		Type:                 relay.TypeAgentReconnect,
		Version:              relay.Version,
		SessionID:            req.SessionID,
		AgentReconnectSecret: req.ReconnectSecret,
	}
	if err := writeControlJSON(conn, frame); err != nil {
		return fmt.Errorf("send relay agent reconnect: %w", err)
	}
	var registered relay.AgentRegisteredFrame
	if err := readControlJSON(conn, &registered); err != nil {
		return fmt.Errorf("read relay agent reconnect response: %w", err)
	}
	if registered.Type != relay.TypeAgentRegistered || registered.SessionID != req.SessionID {
		return fmt.Errorf("relay agent reconnect rejected")
	}
	return nil
}

func writeControlJSON(conn *websocket.Conn, frame any) error {
	if err := conn.SetWriteDeadline(time.Now().Add(relayControlTimeout)); err != nil {
		return err
	}
	if err := conn.WriteJSON(frame); err != nil {
		return err
	}
	return conn.SetWriteDeadline(time.Time{})
}

func readControlJSON(conn *websocket.Conn, frame any) error {
	if err := conn.SetReadDeadline(time.Now().Add(relayControlTimeout)); err != nil {
		return err
	}
	if err := conn.ReadJSON(frame); err != nil {
		return err
	}
	return conn.SetReadDeadline(time.Time{})
}

func EmitPairingFile(path string, event PairingReadyEvent) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal pairing event: %w", err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		return fmt.Errorf("write pairing event file: %w", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		return fmt.Errorf("secure pairing event file: %w", err)
	}
	return nil
}

func removePairingEventFile(path string) {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		logx.Warn("relay", "remove pairing event file failed: %v", err)
	}
}
