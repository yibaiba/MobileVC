package relayclient

import (
	"fmt"
	"sync"

	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

type agentE2EEHandshakeHandler struct {
	mu            sync.Mutex
	sessionID     string
	pairingSecret string
	capabilities  e2ee.CapabilitySet
	nodeIdentity  *e2ee.NodeIdentity
	pending       map[string]pendingPairingHandshake
	completed     map[string]completedPairingHandshake
}

type pendingPairingHandshake struct {
	input         e2ee.HandshakeInput
	nodeEphemeral *e2ee.EphemeralKeyPair
}

type completedPairingHandshake struct {
	clientID string
	keys     *e2ee.TrafficKeys
}

func newAgentE2EEHandshakeHandler(sessionID string, pairingSecret string, capabilities e2ee.CapabilitySet, nodeIdentity *e2ee.NodeIdentity) *agentE2EEHandshakeHandler {
	if nodeIdentity == nil {
		return nil
	}
	if err := e2ee.ValidateProductionCapabilities(capabilities); err != nil {
		return nil
	}
	return &agentE2EEHandshakeHandler{
		sessionID: sessionID, pairingSecret: pairingSecret,
		capabilities: capabilities, nodeIdentity: nodeIdentity,
		pending:   map[string]pendingPairingHandshake{},
		completed: map[string]completedPairingHandshake{},
	}
}

func (h *agentE2EEHandshakeHandler) handleClientHello(frame relay.E2EEClientHelloFrame) (relay.E2EEAgentHelloFrame, error) {
	if h == nil {
		return relay.E2EEAgentHelloFrame{}, fmt.Errorf("relay e2ee handshake is not configured")
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	material, err := e2ee.ValidateClientHelloFrame(frame)
	if err != nil {
		return relay.E2EEAgentHelloFrame{}, err
	}
	if frame.Kind != e2ee.HandshakeKindPairing {
		return relay.E2EEAgentHelloFrame{}, fmt.Errorf("%w: reconnect handshake is not wired", e2ee.ErrHandshakeFailed)
	}
	if frame.SessionID != h.sessionID {
		return relay.E2EEAgentHelloFrame{}, fmt.Errorf("%w: invalid session", e2ee.ErrHandshakeFailed)
	}
	nodeEphemeral, err := e2ee.NewEphemeralKeyPair()
	if err != nil {
		return relay.E2EEAgentHelloFrame{}, err
	}
	input := material.Capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     frame.Kind,
		SessionID:                frame.SessionID,
		ClientID:                 frame.ClientID,
		HandshakeID:              frame.HandshakeID,
		ClientEphemeralPublicKey: material.ClientEphemeralPublicKey,
		NodeEphemeralPublicKey:   nodeEphemeral.PublicKey,
		NodeIdentityPublicKey:    h.nodeIdentity.PublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		return relay.E2EEAgentHelloFrame{}, err
	}
	signature, err := h.nodeIdentity.SignTranscript(transcript)
	if err != nil {
		return relay.E2EEAgentHelloFrame{}, err
	}
	h.pending[frame.HandshakeID] = pendingPairingHandshake{
		input: input, nodeEphemeral: nodeEphemeral,
	}
	return relay.E2EEAgentHelloFrame{
		Type: relay.TypeAgentE2EEHello, Version: relay.Version,
		SessionID: frame.SessionID, ClientID: frame.ClientID, HandshakeID: frame.HandshakeID,
		Capabilities:           &h.capabilities,
		NodeEphemeralPublicKey: e2ee.EncodeFrameBytes(nodeEphemeral.PublicKey),
		NodeIdentityPublicKey:  e2ee.EncodeFrameBytes(h.nodeIdentity.PublicKey),
		NodeSignature:          e2ee.EncodeFrameBytes(signature),
	}, nil
}

func (h *agentE2EEHandshakeHandler) handleClientProof(frame relay.E2EEClientProofFrame) (relay.E2EEAgentResultFrame, error) {
	if h == nil {
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), fmt.Errorf("relay e2ee handshake is not configured")
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	material, err := e2ee.ValidateClientProofFrame(frame)
	if err != nil {
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), err
	}
	pending, ok := h.pending[frame.HandshakeID]
	if !ok || frame.Kind != e2ee.HandshakeKindPairing || !sameHandshakeRoute(frame, pending.input) {
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), fmt.Errorf("%w: missing pending pairing handshake", e2ee.ErrHandshakeFailed)
	}
	transcript, err := e2ee.HandshakeTranscript(pending.input)
	if err != nil {
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), err
	}
	if !e2ee.VerifyPairingProof(h.pairingSecret, transcript, material.PairingProof) {
		delete(h.pending, frame.HandshakeID)
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), e2ee.ErrHandshakeFailed
	}
	keys, err := e2ee.DeriveHandshakeTrafficKeys(
		pending.nodeEphemeral.PrivateScalar,
		pending.input.ClientEphemeralPublicKey,
		pending.input,
	)
	if err != nil {
		delete(h.pending, frame.HandshakeID)
		return e2eeAgentResult(frame, false, relay.CodeE2EEHandshakeFailed), err
	}
	delete(h.pending, frame.HandshakeID)
	h.completed[frame.HandshakeID] = completedPairingHandshake{
		clientID: pending.input.ClientID,
		keys:     keys,
	}
	return e2eeAgentResult(frame, true, ""), nil
}

func (h *agentE2EEHandshakeHandler) trafficKeys(handshakeID string) (*e2ee.TrafficKeys, bool) {
	if h == nil {
		return nil, false
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	completed, ok := h.completed[handshakeID]
	return completed.keys, ok
}

func (h *agentE2EEHandshakeHandler) completedCodec(handshakeID string) (*e2ee.MobileVCStreamCodec, bool, error) {
	if h == nil {
		return nil, false, nil
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	completed, ok := h.completed[handshakeID]
	if !ok {
		return nil, false, nil
	}
	codec, err := e2ee.NewAgentMobileVCStreamCodec(
		h.sessionID,
		completed.clientID,
		handshakeID,
		completed.keys,
	)
	if err != nil {
		return nil, false, err
	}
	return codec, true, nil
}

func sameHandshakeRoute(frame relay.E2EEClientProofFrame, input e2ee.HandshakeInput) bool {
	return frame.SessionID == input.SessionID &&
		frame.ClientID == input.ClientID &&
		frame.HandshakeID == input.HandshakeID
}

func e2eeAgentResult(frame relay.E2EEClientProofFrame, ok bool, code string) relay.E2EEAgentResultFrame {
	return relay.E2EEAgentResultFrame{
		Type: relay.TypeAgentE2EEResult, Version: relay.Version,
		SessionID: frame.SessionID, ClientID: frame.ClientID, HandshakeID: frame.HandshakeID,
		OK: ok, ErrorCode: code,
	}
}
