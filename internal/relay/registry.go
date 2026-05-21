package relay

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

func (s *Server) registerAgent(peer *peerConn, raw []byte) (string, error) {
	state, err := newSessionState(peer, raw)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.sessions[state.id] != nil {
		s.mu.Unlock()
		return "", errors.New("session already exists")
	}
	if len(s.sessions) >= s.cfg.MaxSessions {
		s.mu.Unlock()
		return "", errors.New("session capacity reached")
	}
	s.sessions[state.id] = state
	s.mu.Unlock()
	return state.id, writeRegistered(peer, state.id)
}

func newSessionState(peer *peerConn, raw []byte) (*sessionState, error) {
	var frame AgentRegisterFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, err
	}
	state := &sessionState{
		id:                   strings.TrimSpace(frame.SessionID),
		pairingHash:          strings.TrimSpace(frame.PairingSecretHash),
		agentReconnectHash:   strings.TrimSpace(frame.AgentReconnectSecretHash),
		pairingExpiresAt:     time.Unix(frame.PairingExpiresAt, 0),
		agent:                peer,
		pairFailuresByRemote: map[string]int{},
		devices:              map[string]*deviceState{},
	}
	if state.id == "" || state.pairingHash == "" || state.agentReconnectHash == "" {
		return nil, errors.New("missing agent registration fields")
	}
	return state, nil
}

func (s *Server) reconnectAgent(peer *peerConn, raw []byte) (string, error) {
	var frame AgentReconnectFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return "", err
	}
	s.mu.Lock()
	state := s.sessions[strings.TrimSpace(frame.SessionID)]
	if !s.canReconnectAgent(state, frame.AgentReconnectSecret) {
		s.mu.Unlock()
		return "", errors.New("agent reconnect rejected")
	}
	disconnectedAt := state.agentDisconnectedAt
	clientID := ""
	if state.client != nil {
		clientID = state.clientID
	}
	state.agent = peer
	state.agentDisconnectedAt = time.Time{}
	sessionID := state.id
	s.mu.Unlock()
	if err := writeRegistered(peer, sessionID); err != nil {
		s.rollbackAgentReconnect(sessionID, peer, disconnectedAt)
		return "", err
	}
	if strings.TrimSpace(clientID) == "" {
		return sessionID, nil
	}
	if err := notifyClientAttached(peer, sessionID, clientID); err != nil {
		s.rollbackAgentReconnect(sessionID, peer, disconnectedAt)
		return "", err
	}
	return sessionID, nil
}

func (s *Server) canReconnectAgent(state *sessionState, secret string) bool {
	if state == nil || state.agent != nil || state.agentDisconnectedAt.IsZero() {
		return false
	}
	if time.Since(state.agentDisconnectedAt) > s.cfg.AgentGracePeriod {
		delete(s.sessions, state.id)
		return false
	}
	return SecretHashMatches(state.agentReconnectHash, secret)
}

func (s *Server) pairClient(peer *peerConn, raw []byte, remote string) (string, string, error) {
	var frame ClientPairFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return "", "", err
	}
	s.mu.Lock()
	state := s.sessions[strings.TrimSpace(frame.SessionID)]
	if !s.canPair(state, remote) || !SecretHashMatches(state.pairingHash, frame.PairingSecret) {
		s.recordPairingFailure(state, remote)
		s.mu.Unlock()
		return "", "", errors.New("pairing rejected")
	}
	clientID := "rc_" + uuid.NewString()
	clientReconnectSecret, err := NewSecret()
	if err != nil {
		s.mu.Unlock()
		return "", "", err
	}
	sessionID := state.id
	agent := state.agent
	pairingHash := state.pairingHash
	state.client = peer
	state.clientID = clientID
	state.clientReconnectHash = SecretHash(clientReconnectSecret)
	state.pairingHash = ""
	state.pairingConsumed = true
	s.rememberDeviceLocked(state, clientID, frame.DeviceName, state.clientReconnectHash)
	s.mu.Unlock()
	if err := writePaired(peer, sessionID, clientID, clientReconnectSecret); err != nil {
		s.rollbackPairing(sessionID, peer, pairingHash)
		return "", "", err
	}
	if err := notifyClientAttached(agent, sessionID, clientID); err != nil {
		s.rollbackPairing(sessionID, peer, pairingHash)
		return "", "", err
	}
	return sessionID, clientID, nil
}

func (s *Server) reconnectClient(peer *peerConn, raw []byte) (string, string, error) {
	var frame ClientReconnectFrame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return "", "", err
	}
	s.mu.Lock()
	state := s.sessions[strings.TrimSpace(frame.SessionID)]
	device := s.reconnectableDevice(state, frame.ClientID, frame.ClientReconnectSecret)
	if device == nil {
		s.mu.Unlock()
		return "", "", errors.New("client reconnect rejected")
	}
	previousClient := state.client
	state.client = peer
	state.clientID = device.ClientID
	device.LastSeenAt = time.Now().UTC()
	sessionID := state.id
	clientID := device.ClientID
	agent := state.agent
	s.mu.Unlock()
	if previousClient != nil && previousClient != peer {
		_ = previousClient.Close()
	}
	if err := writePaired(peer, sessionID, clientID, ""); err != nil {
		s.rollbackClientReconnect(sessionID, peer)
		return "", "", err
	}
	if err := notifyClientAttached(agent, sessionID, clientID); err != nil {
		s.rollbackClientReconnect(sessionID, peer)
		return "", "", err
	}
	return sessionID, clientID, nil
}

func (s *Server) canPair(state *sessionState, remote string) bool {
	if state == nil || state.agent == nil || state.client != nil || state.pairingConsumed {
		return false
	}
	if time.Now().After(state.pairingExpiresAt) {
		return false
	}
	return state.pairFailuresByRemote[remote] < s.cfg.MaxPairingFailuresPerIP
}

func (s *Server) canReconnectClient(state *sessionState, clientID string, secret string) bool {
	return s.reconnectableDevice(state, clientID, secret) != nil
}

func (s *Server) reconnectableDevice(state *sessionState, clientID string, secret string) *deviceState {
	if state == nil || state.agent == nil {
		return nil
	}
	normalizedID := strings.TrimSpace(clientID)
	if normalizedID == "" {
		return nil
	}
	device := state.devices[normalizedID]
	if device == nil || device.Revoked {
		return nil
	}
	if !SecretHashMatches(device.ReconnectHash, secret) {
		return nil
	}
	return device
}

func (s *Server) recordPairingFailure(state *sessionState, remote string) {
	if state != nil {
		state.pairFailuresByRemote[remote]++
	}
}

func (s *Server) rollbackPairing(sessionID string, peer *peerConn, pairingHash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.client != peer {
		return
	}
	delete(state.devices, state.clientID)
	state.client = nil
	state.clientID = ""
	state.clientReconnectHash = ""
	state.pairingHash = pairingHash
	state.pairingConsumed = false
}

func (s *Server) rollbackClientReconnect(sessionID string, peer *peerConn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.client != peer {
		return
	}
	state.client = nil
}

func (s *Server) rollbackAgentReconnect(sessionID string, peer *peerConn, disconnectedAt time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.sessions[sessionID]
	if state == nil || state.agent != peer {
		return
	}
	state.agent = nil
	state.agentDisconnectedAt = disconnectedAt
}

func writeRegistered(peer *peerConn, sessionID string) error {
	return peer.WriteJSON(AgentRegisteredFrame{
		Type: TypeAgentRegistered, Version: Version, SessionID: sessionID,
	})
}

func writePaired(peer *peerConn, sessionID string, clientID string, clientReconnectSecret string) error {
	return peer.WriteJSON(ClientPairedFrame{
		Type:                  TypeClientPaired,
		Version:               Version,
		SessionID:             sessionID,
		ClientID:              clientID,
		ClientReconnectSecret: clientReconnectSecret,
	})
}

func notifyClientAttached(peer *peerConn, sessionID string, clientID string) error {
	return peer.Enqueue(ClientAttachedFrame{
		Type: TypeClientAttached, Version: Version, SessionID: sessionID, ClientID: clientID,
	})
}
