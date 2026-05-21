package relay

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"

	"mobilevc/internal/logx"
)

func (s *Server) forwardLoop(peer *peerConn, sessionID string, direction string) {
	for {
		var env ForwardEnvelope
		if err := peer.ReadJSON(&env); err != nil {
			return
		}
		if err := s.forwardEnvelope(peer, sessionID, direction, env); err != nil {
			if errors.Is(err, errPayloadTooLarge) {
				continue
			}
			return
		}
	}
}

func (s *Server) forwardEnvelope(peer *peerConn, sessionID string, direction string, env ForwardEnvelope) error {
	var err error
	env, err = s.normalizeForward(peer, sessionID, direction, env)
	if err != nil {
		writeError(peer, CodeProtocolError)
		return err
	}
	if err := s.validateForward(peer, sessionID, direction, env); err != nil {
		writeError(peer, forwardValidationErrorCode(err))
		return err
	}
	if size, err := s.validatePayloadSize(env.Payload); err != nil {
		if errors.Is(err, errInvalidPayloadEncoding) {
			logx.Warn("relay", "invalid payload encoding: sessionID=%s clientID=%s direction=%s remote=%s err=%v", sessionID, env.ClientID, direction, peer.remote, err)
			writeError(peer, CodeProtocolError)
			return err
		}
		logx.Warn("relay", "payload too large: sessionID=%s clientID=%s direction=%s decodedBytes=%d maxBytes=%d remote=%s", sessionID, env.ClientID, direction, size, s.cfg.MaxPayloadBytes, peer.remote)
		writeErrorFrame(peer, payloadTooLargeFrame(size, s.cfg.MaxPayloadBytes))
		return err
	}
	target := s.targetConn(sessionID, direction)
	if target == nil {
		writeError(peer, CodeTargetUnavailable)
		return errors.New("target unavailable")
	}
	if err := target.Enqueue(env); err != nil {
		writeError(peer, CodeQueueFull)
		return err
	}
	return nil
}

func (s *Server) normalizeForward(peer *peerConn, sessionID string, direction string, env ForwardEnvelope) (ForwardEnvelope, error) {
	if peer.role != roleAgent || direction != DirectionAgentToClient || strings.TrimSpace(env.ClientID) != "" {
		return env, nil
	}
	clientID, err := s.activeClientID(peer, sessionID)
	if err != nil {
		return env, err
	}
	env.ClientID = clientID
	return env, nil
}

func (s *Server) activeClientID(peer *peerConn, sessionID string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.agent != peer || state.client == nil {
		return "", errors.New("missing active relay client")
	}
	return state.clientID, nil
}

func (s *Server) validateForward(peer *peerConn, sessionID string, direction string, env ForwardEnvelope) error {
	if err := validateForwardMetadata(env, sessionID, direction); err != nil {
		return err
	}
	if err := s.forwardSecurityPolicy().Validate(env); err != nil {
		return err
	}
	if !s.forwardClientIDMatches(peer, sessionID, env.ClientID) {
		return errors.New("invalid forward client id")
	}
	return nil
}

func (s *Server) forwardSecurityPolicy() ForwardSecurityPolicy {
	return ForwardSecurityPolicy{
		RequireE2EE:       s.cfg.RequireE2EE,
		PlaintextTestMode: s.cfg.PlaintextTestMode,
	}
}

func forwardValidationErrorCode(err error) string {
	switch {
	case errors.Is(err, ErrE2EERequired):
		return CodeE2EERequired
	case errors.Is(err, ErrE2EEUnsupported):
		return CodeE2EEUnsupported
	default:
		return CodeProtocolError
	}
}

func validateForwardMetadata(env ForwardEnvelope, sessionID string, direction string) error {
	if env.Type != TypeRelayForward || env.Version != Version {
		return errors.New("invalid forward frame")
	}
	if env.SessionID != sessionID || env.Direction != direction {
		return errors.New("invalid forward routing")
	}
	if env.ContentType != ContentTypeMobileVC {
		return errors.New("invalid forward content")
	}
	if env.PayloadEncoding != PayloadBase64URL || strings.TrimSpace(env.MessageID) == "" {
		return errors.New("invalid forward payload metadata")
	}
	return nil
}

func (s *Server) forwardClientIDMatches(peer *peerConn, sessionID string, clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || strings.TrimSpace(clientID) == "" {
		return false
	}
	if peer.role == roleClient {
		return state.client == peer && state.clientID == clientID
	}
	return state.agent == peer && state.clientID == clientID
}

var (
	errInvalidPayloadEncoding = errors.New("invalid payload encoding")
	errPayloadTooLarge        = errors.New("payload too large")
)

func (s *Server) validatePayloadSize(payload string) (int, error) {
	decoded, err := DecodePayloadBase64URL(payload)
	if err != nil {
		return 0, errInvalidPayloadEncoding
	}
	if len(decoded) > s.cfg.MaxPayloadBytes {
		return len(decoded), errPayloadTooLarge
	}
	return len(decoded), nil
}

// DecodePayloadBase64URL accepts both padded and unpadded base64url payloads.
func DecodePayloadBase64URL(payload string) ([]byte, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err == nil {
		return decoded, nil
	}
	return base64.URLEncoding.DecodeString(payload)
}

func (s *Server) targetConn(sessionID string, direction string) *peerConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil {
		return nil
	}
	if direction == DirectionClientToAgent {
		return state.agent
	}
	return state.client
}

func (s *Server) markAgentDisconnected(sessionID string, peer *peerConn) {
	s.mu.Lock()
	state := s.sessions[sessionID]
	if state == nil || state.agent != peer {
		s.mu.Unlock()
		return
	}
	state.agent = nil
	state.agentDisconnectedAt = time.Now()
	disconnectedAt := state.agentDisconnectedAt
	s.mu.Unlock()
	go s.expireDisconnectedAgent(sessionID, disconnectedAt)
}

func (s *Server) expireDisconnectedAgent(sessionID string, disconnectedAt time.Time) {
	time.Sleep(s.cfg.AgentGracePeriod)
	client := s.removeExpiredDisconnectedAgent(sessionID, disconnectedAt)
	if client != nil {
		_ = client.Close()
	}
}

func (s *Server) removeExpiredDisconnectedAgent(sessionID string, disconnectedAt time.Time) *peerConn {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.agent != nil || !state.agentDisconnectedAt.Equal(disconnectedAt) {
		return nil
	}
	delete(s.sessions, sessionID)
	return state.client
}

func (s *Server) markClientDisconnected(sessionID string, peer *peerConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.client != peer {
		return
	}
	state.client = nil
}

func writeError(peer *peerConn, code string) {
	_ = peer.WriteJSON(NewErrorFrame(code))
}

func writeErrorFrame(peer *peerConn, frame ErrorFrame) {
	_ = peer.WriteJSON(frame)
}

func payloadTooLargeFrame(decodedBytes int, maxBytes int) ErrorFrame {
	frame := NewErrorFrame(CodePayloadTooLarge)
	frame.DecodedBytes = decodedBytes
	frame.MaxBytes = maxBytes
	return frame
}
