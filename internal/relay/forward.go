package relay

import (
	"encoding/base64"
	"errors"
	"strings"
	"time"
)

func (s *Server) forwardLoop(peer *peerConn, sessionID string, direction string) {
	for {
		var env ForwardEnvelope
		if err := peer.ReadJSON(&env); err != nil {
			return
		}
		if err := s.forwardEnvelope(peer, sessionID, direction, env); err != nil {
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
		writeError(peer, CodeProtocolError)
		return err
	}
	if err := validatePayloadSize(env.Payload); err != nil {
		writeError(peer, CodePayloadTooLarge)
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
	if !s.forwardClientIDMatches(peer, sessionID, env.ClientID) {
		return errors.New("invalid forward client id")
	}
	return nil
}

func validateForwardMetadata(env ForwardEnvelope, sessionID string, direction string) error {
	if env.Type != TypeRelayForward || env.Version != Version {
		return errors.New("invalid forward frame")
	}
	if env.SessionID != sessionID || env.Direction != direction {
		return errors.New("invalid forward routing")
	}
	if env.ContentType != ContentTypeMobileVC || env.Encryption != EncryptionNone {
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

func validatePayloadSize(payload string) error {
	decoded, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return err
	}
	if len(decoded) > MaxPayloadBytes {
		return errors.New("payload too large")
	}
	return nil
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
