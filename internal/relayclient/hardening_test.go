package relayclient

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

func TestGatewayConnConsumesClientAttachedBeforeForward(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")

	go func() {
		_ = serverConn.WriteJSON(relay.ClientAttachedFrame{
			Type: relay.TypeClientAttached, Version: relay.Version,
			SessionID: "rs_gateway", ClientID: "rc_attached",
		})
		_ = serverConn.WriteJSON(testRelayForward("rc_attached"))
	}()

	var payload map[string]any
	if err := gateway.ReadJSON(&payload); err != nil {
		t.Fatalf("read gateway payload: %v", err)
	}
	if gateway.RemoteAddr() != "relay:rs_gateway/rc_attached" {
		t.Fatalf("remote addr did not include client id: %s", gateway.RemoteAddr())
	}
}

func TestGatewayConnAcceptsPaddedBase64URLPayload(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")
	t.Cleanup(func() { _ = gateway.Close() })

	env := testRelayForward("rc_attached")
	env.Payload = base64.URLEncoding.EncodeToString([]byte(`{"action":"ping"}`))
	go func() {
		_ = serverConn.WriteJSON(relay.ClientAttachedFrame{
			Type: relay.TypeClientAttached, Version: relay.Version,
			SessionID: "rs_gateway", ClientID: "rc_attached",
		})
		_ = serverConn.WriteJSON(env)
	}()

	var payload map[string]any
	if err := gateway.ReadJSON(&payload); err != nil {
		t.Fatalf("read padded gateway payload: %v", err)
	}
	if payload["action"] != "ping" {
		t.Fatalf("unexpected padded payload: %#v", payload)
	}
}

func TestGatewayConnWritesCurrentClientID(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")

	go func() {
		_ = serverConn.WriteJSON(relay.ClientAttachedFrame{
			Type: relay.TypeClientAttached, Version: relay.Version,
			SessionID: "rs_gateway", ClientID: "rc_attached",
		})
		_ = serverConn.WriteJSON(testRelayForward("rc_attached"))
	}()
	var payload map[string]any
	if err := gateway.ReadJSON(&payload); err != nil {
		t.Fatalf("read attached gateway payload: %v", err)
	}
	if err := gateway.WriteJSON(map[string]string{"event": "ready"}); err != nil {
		t.Fatalf("write gateway payload: %v", err)
	}
	var env relay.ForwardEnvelope
	if err := serverConn.ReadJSON(&env); err != nil {
		t.Fatalf("read written forward: %v", err)
	}
	if env.ClientID != "rc_attached" {
		t.Fatalf("written client id: got %q", env.ClientID)
	}
}

func TestGatewayConnRespondsToRelayPing(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(relay.ControlFrame{Type: relay.TypeRelayPing, Version: relay.Version}); err != nil {
		t.Fatalf("write relay ping: %v", err)
	}
	var pong relay.ControlFrame
	if err := serverConn.ReadJSON(&pong); err != nil {
		t.Fatalf("read relay pong: %v", err)
	}
	if pong.Type != relay.TypeRelayPong || pong.Version != relay.Version {
		t.Fatalf("unexpected pong frame: %#v", pong)
	}
}

func TestGatewayConnRejectsWrongSessionRelayPing(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(map[string]any{
		"type":      relay.TypeRelayPing,
		"version":   relay.Version,
		"sessionId": "rs_other",
	}); err != nil {
		t.Fatalf("write relay ping: %v", err)
	}
	var payload map[string]any
	err := gateway.ReadJSON(&payload)
	if err == nil || !strings.Contains(err.Error(), "relay ping routing") {
		t.Fatalf("expected relay ping routing error, got %v", err)
	}
}

func TestGatewayConnRejectsE2EEControlFramesUntilHandshakeIsWired(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := newGatewayConn(clientConn, "rs_gateway")
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_test",
	}); err != nil {
		t.Fatalf("write e2ee control frame: %v", err)
	}
	var payload map[string]any
	err := gateway.ReadJSON(&payload)
	if err == nil || !strings.Contains(err.Error(), "e2ee handshake") {
		t.Fatalf("expected explicit e2ee control rejection, got %v", err)
	}
}

func TestGatewayConnHandlesPairingE2EEHandshake(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	nodeIdentity, err := e2ee.GenerateNodeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	capabilities := e2ee.ProductionCapabilities()
	handshake := newAgentE2EEHandshakeHandler(
		"rs_gateway",
		"pair-secret-128-bit-minimum",
		capabilities,
		nodeIdentity,
	)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	clientEphemeral, err := e2ee.NewEphemeralKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	clientHello := relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_pairing",
		Kind: e2ee.HandshakeKindPairing, Capabilities: &capabilities,
		ClientEphemeralPublicKey: e2ee.EncodeFrameBytes(clientEphemeral.PublicKey),
	}
	if err := serverConn.WriteJSON(clientHello); err != nil {
		t.Fatalf("write client hello: %v", err)
	}
	var agentHello relay.E2EEAgentHelloFrame
	if err := serverConn.ReadJSON(&agentHello); err != nil {
		t.Fatalf("read agent hello: %v", err)
	}
	agentMaterial, err := e2ee.ValidateAgentHelloFrame(agentHello)
	if err != nil {
		t.Fatalf("validate agent hello: %v", err)
	}
	if fmt.Sprintf("%x", e2ee.Fingerprint(agentMaterial.NodeIdentityPublicKey)) != testNodeFingerprintHexFromIdentity(nodeIdentity) {
		t.Fatal("agent hello node identity fingerprint mismatch")
	}
	input := capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     e2ee.HandshakeKindPairing,
		SessionID:                "rs_gateway",
		ClientID:                 "rc_attached",
		HandshakeID:              "hs_pairing",
		ClientEphemeralPublicKey: clientEphemeral.PublicKey,
		NodeEphemeralPublicKey:   agentMaterial.NodeEphemeralPublicKey,
		NodeIdentityPublicKey:    agentMaterial.NodeIdentityPublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := e2ee.VerifyNodeSignature(
		agentMaterial.NodeIdentityPublicKey,
		transcript,
		agentMaterial.NodeSignature,
	)
	if err != nil || !verified {
		t.Fatalf("node signature verification failed: verified=%v err=%v", verified, err)
	}
	proof := relay.E2EEClientProofFrame{
		Type: relay.TypeClientE2EEProof, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_pairing",
		Kind:         e2ee.HandshakeKindPairing,
		PairingProof: e2ee.EncodeFrameBytes(e2ee.PairingProof("pair-secret-128-bit-minimum", transcript)),
	}
	if err := serverConn.WriteJSON(proof); err != nil {
		t.Fatalf("write client proof: %v", err)
	}
	var result relay.E2EEAgentResultFrame
	if err := serverConn.ReadJSON(&result); err != nil {
		t.Fatalf("read agent result: %v", err)
	}
	if err := e2ee.ValidateAgentResultFrame(result); err != nil {
		t.Fatalf("validate agent result: %v", err)
	}
	if !result.OK {
		t.Fatalf("unexpected failed result: %#v", result)
	}
	agentKeys, ok := handshake.trafficKeys("hs_pairing")
	if !ok {
		t.Fatal("missing completed agent traffic keys")
	}
	clientKeys, err := e2ee.DeriveHandshakeTrafficKeys(
		clientEphemeral.PrivateScalar,
		agentMaterial.NodeEphemeralPublicKey,
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(agentKeys.ClientToAgent) != string(clientKeys.ClientToAgent) ||
		string(agentKeys.AgentToClient) != string(clientKeys.AgentToClient) {
		t.Fatal("agent and client traffic keys differ")
	}
}

func TestGatewayConnEncryptsRelayForwardAfterPairingE2EEHandshake(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	handshake, clientEphemeral := testPairingHandshakeHandler(t)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(relay.ClientAttachedFrame{
		Type: relay.TypeClientAttached, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached",
	}); err != nil {
		t.Fatalf("write attached: %v", err)
	}
	clientKeys := driveGatewayE2EEHandshake(t, serverConn, clientEphemeral)
	clientCodec, err := e2ee.NewClientMobileVCStreamCodec("rs_gateway", "rc_attached", "hs_pairing", clientKeys)
	if err != nil {
		t.Fatal(err)
	}
	inbound, err := clientCodec.EncodeJSON("msg_in", map[string]string{"action": "ping"})
	if err != nil {
		t.Fatal(err)
	}
	if err := serverConn.WriteJSON(relay.ForwardEnvelope(inbound)); err != nil {
		t.Fatalf("write encrypted inbound: %v", err)
	}
	var decoded map[string]any
	if err := gateway.ReadJSON(&decoded); err != nil {
		t.Fatalf("read encrypted inbound: %v", err)
	}
	if decoded["action"] != "ping" {
		t.Fatalf("unexpected decrypted inbound: %#v", decoded)
	}

	if err := gateway.WriteJSON(map[string]string{"event": "ready"}); err != nil {
		t.Fatalf("write encrypted outbound: %v", err)
	}
	var outbound relay.ForwardEnvelope
	if err := serverConn.ReadJSON(&outbound); err != nil {
		t.Fatalf("read encrypted outbound: %v", err)
	}
	if outbound.Encryption != relay.EncryptionE2EEV1 || strings.Contains(outbound.Payload, "ready") {
		t.Fatalf("outbound was not encrypted: %#v", outbound)
	}
	var plaintext map[string]any
	if err := clientCodec.DecodeJSON(e2ee.RelayForwardFrame(outbound), &plaintext); err != nil {
		t.Fatalf("decrypt outbound: %v", err)
	}
	if plaintext["event"] != "ready" {
		t.Fatalf("unexpected decrypted outbound: %#v", plaintext)
	}
}

func TestGatewayConnWaitsForE2EEBeforeWritingProductionForward(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	handshake, clientEphemeral := testPairingHandshakeHandler(t)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(relay.ClientAttachedFrame{
		Type: relay.TypeClientAttached, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached",
	}); err != nil {
		t.Fatalf("write attached: %v", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- gateway.WriteJSON(map[string]string{"event": "ready"})
	}()

	select {
	case err := <-writeDone:
		t.Fatalf("write completed before e2ee activation: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	clientKeys := driveGatewayE2EEHandshake(t, serverConn, clientEphemeral)
	if err := <-writeDone; err != nil {
		t.Fatalf("write after e2ee activation: %v", err)
	}
	var outbound relay.ForwardEnvelope
	if err := serverConn.ReadJSON(&outbound); err != nil {
		t.Fatalf("read encrypted outbound: %v", err)
	}
	if outbound.Encryption != relay.EncryptionE2EEV1 || strings.Contains(outbound.Payload, "ready") {
		t.Fatalf("outbound was not encrypted: %#v", outbound)
	}
	clientCodec, err := e2ee.NewClientMobileVCStreamCodec("rs_gateway", "rc_attached", "hs_pairing", clientKeys)
	if err != nil {
		t.Fatal(err)
	}
	var plaintext map[string]any
	if err := clientCodec.DecodeJSON(e2ee.RelayForwardFrame(outbound), &plaintext); err != nil {
		t.Fatalf("decrypt outbound: %v", err)
	}
	if plaintext["event"] != "ready" {
		t.Fatalf("unexpected decrypted outbound: %#v", plaintext)
	}
}

func TestGatewayConnRejectsPlaintextForwardBeforeProductionE2EE(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	handshake, _ := testPairingHandshakeHandler(t)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	if err := serverConn.WriteJSON(relay.ClientAttachedFrame{
		Type: relay.TypeClientAttached, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached",
	}); err != nil {
		t.Fatalf("write attached: %v", err)
	}
	if err := serverConn.WriteJSON(testRelayForward("rc_attached")); err != nil {
		t.Fatalf("write plaintext forward: %v", err)
	}
	var payload map[string]any
	err := gateway.ReadJSON(&payload)
	if err == nil || !strings.Contains(err.Error(), relay.CodeE2EERequired) {
		t.Fatalf("expected plaintext e2ee rejection, got payload=%#v err=%v", payload, err)
	}
}

func TestGatewayConnRejectsMismatchedPairingE2EEProofRoute(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	handshake, clientEphemeral := testPairingHandshakeHandler(t)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	capabilities := e2ee.ProductionCapabilities()
	clientHello := relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_pairing",
		Kind: e2ee.HandshakeKindPairing, Capabilities: &capabilities,
		ClientEphemeralPublicKey: e2ee.EncodeFrameBytes(clientEphemeral.PublicKey),
	}
	if err := serverConn.WriteJSON(clientHello); err != nil {
		t.Fatalf("write client hello: %v", err)
	}
	var agentHello relay.E2EEAgentHelloFrame
	if err := serverConn.ReadJSON(&agentHello); err != nil {
		t.Fatalf("read agent hello: %v", err)
	}
	agentMaterial, err := e2ee.ValidateAgentHelloFrame(agentHello)
	if err != nil {
		t.Fatalf("validate agent hello: %v", err)
	}
	input := capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     e2ee.HandshakeKindPairing,
		SessionID:                "rs_gateway",
		ClientID:                 "rc_attached",
		HandshakeID:              "hs_pairing",
		ClientEphemeralPublicKey: clientEphemeral.PublicKey,
		NodeEphemeralPublicKey:   agentMaterial.NodeEphemeralPublicKey,
		NodeIdentityPublicKey:    agentMaterial.NodeIdentityPublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		t.Fatal(err)
	}
	proof := relay.E2EEClientProofFrame{
		Type: relay.TypeClientE2EEProof, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_other", HandshakeID: "hs_pairing",
		Kind:         e2ee.HandshakeKindPairing,
		PairingProof: e2ee.EncodeFrameBytes(e2ee.PairingProof("pair-secret-128-bit-minimum", transcript)),
	}
	if err := serverConn.WriteJSON(proof); err != nil {
		t.Fatalf("write client proof: %v", err)
	}
	var result relay.E2EEAgentResultFrame
	if err := serverConn.ReadJSON(&result); err != nil {
		t.Fatalf("read agent result: %v", err)
	}
	if result.OK || result.ErrorCode != relay.CodeE2EEHandshakeFailed {
		t.Fatalf("expected handshake failure result, got %#v", result)
	}
}

func TestGatewayConnHandlesReconnectE2EEHandshake(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	handshake, clientEphemeral, deviceIdentity, credential := testReconnectHandshakeHandler(t)
	gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
	t.Cleanup(func() { _ = gateway.Close() })

	capabilities := e2ee.ProductionCapabilities()
	clientHello := relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_reconnect",
		Kind: e2ee.HandshakeKindReconnect, Capabilities: &capabilities,
		ClientEphemeralPublicKey: e2ee.EncodeFrameBytes(clientEphemeral.PublicKey),
		DeviceID:                 "dev_pixel",
		DeviceIdentityPublicKey:  e2ee.EncodeFrameBytes(deviceIdentity.PublicKey),
	}
	if err := serverConn.WriteJSON(clientHello); err != nil {
		t.Fatalf("write reconnect hello: %v", err)
	}
	var agentHello relay.E2EEAgentHelloFrame
	if err := serverConn.ReadJSON(&agentHello); err != nil {
		t.Fatalf("read reconnect agent hello: %v", err)
	}
	agentMaterial, err := e2ee.ValidateAgentHelloFrame(agentHello)
	if err != nil {
		t.Fatalf("validate reconnect agent hello: %v", err)
	}
	input := capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     e2ee.HandshakeKindReconnect,
		SessionID:                "rs_gateway",
		ClientID:                 "rc_attached",
		HandshakeID:              "hs_reconnect",
		ClientEphemeralPublicKey: clientEphemeral.PublicKey,
		NodeEphemeralPublicKey:   agentMaterial.NodeEphemeralPublicKey,
		NodeIdentityPublicKey:    agentMaterial.NodeIdentityPublicKey,
		DeviceIdentityPublicKey:  deviceIdentity.PublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		t.Fatal(err)
	}
	deviceSignature, err := deviceIdentity.SignTranscript(transcript)
	if err != nil {
		t.Fatal(err)
	}
	proof := relay.E2EEClientProofFrame{
		Type: relay.TypeClientE2EEProof, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_reconnect",
		Kind:            e2ee.HandshakeKindReconnect,
		DeviceProof:     e2ee.EncodeFrameBytes(e2ee.DeviceProof(credential, transcript)),
		DeviceSignature: e2ee.EncodeFrameBytes(deviceSignature),
	}
	if err := serverConn.WriteJSON(proof); err != nil {
		t.Fatalf("write reconnect proof: %v", err)
	}
	var result relay.E2EEAgentResultFrame
	if err := serverConn.ReadJSON(&result); err != nil {
		t.Fatalf("read reconnect result: %v", err)
	}
	if !result.OK {
		t.Fatalf("unexpected reconnect failure: %#v", result)
	}
	agentKeys, ok := handshake.trafficKeys("hs_reconnect")
	if !ok {
		t.Fatal("missing reconnect traffic keys")
	}
	clientKeys, err := e2ee.DeriveHandshakeTrafficKeys(
		clientEphemeral.PrivateScalar,
		agentMaterial.NodeEphemeralPublicKey,
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	if string(agentKeys.ClientToAgent) != string(clientKeys.ClientToAgent) ||
		string(agentKeys.AgentToClient) != string(clientKeys.AgentToClient) {
		t.Fatal("reconnect traffic keys differ")
	}
}

func TestGatewayConnRejectsReconnectE2EEProofFailures(t *testing.T) {
	tests := []struct {
		name         string
		deviceID     string
		mutateProof  func(*relay.E2EEClientProofFrame)
		revokeDevice bool
		wantCode     string
	}{
		{name: "unknown device", deviceID: "dev_unknown", wantCode: relay.CodeDeviceUnknown},
		{name: "wrong credential", mutateProof: func(frame *relay.E2EEClientProofFrame) {
			frame.DeviceProof = e2ee.EncodeFrameBytes([]byte("wrong-proof"))
		}, wantCode: relay.CodeDeviceUnknown},
		{name: "bad signature", mutateProof: func(frame *relay.E2EEClientProofFrame) {
			frame.DeviceSignature = e2ee.EncodeFrameBytes([]byte("bad-signature"))
		}, wantCode: relay.CodeDeviceUnknown},
		{name: "revoked device", revokeDevice: true, wantCode: relay.CodeDeviceRevoked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverConn, clientConn := newRelayClientTestConns(t)
			defer serverConn.Close()
			handshake, clientEphemeral, deviceIdentity, credential := testReconnectHandshakeHandler(t)
			if tt.revokeDevice {
				if _, err := handshake.deviceTrust.RevokeDevice("dev_pixel", time.Now().UTC()); err != nil {
					t.Fatal(err)
				}
			}
			gateway := newGatewayConnWithE2EE(clientConn, "rs_gateway", handshake)
			t.Cleanup(func() { _ = gateway.Close() })

			deviceID := "dev_pixel"
			if tt.deviceID != "" {
				deviceID = tt.deviceID
			}
			proof := driveReconnectHello(t, serverConn, clientEphemeral, deviceIdentity, deviceID, credential)
			if tt.mutateProof != nil {
				tt.mutateProof(proof)
			}
			if err := serverConn.WriteJSON(proof); err != nil {
				t.Fatalf("write reconnect proof: %v", err)
			}
			var result relay.E2EEAgentResultFrame
			if err := serverConn.ReadJSON(&result); err != nil {
				t.Fatalf("read reconnect result: %v", err)
			}
			if result.OK || result.ErrorCode != tt.wantCode {
				t.Fatalf("unexpected reconnect result: %#v", result)
			}
		})
	}
}

func TestGatewayConnCloseUnblocksFullReadQueueSend(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	gateway := &gatewayConn{
		conn: clientConn, readCh: make(chan readResult, relayReadQueueSize),
		closeCh: make(chan struct{}),
	}

	for i := 0; i < relayReadQueueSize; i++ {
		gateway.readCh <- readResult{}
	}
	if err := gateway.Close(); err != nil {
		t.Fatalf("close gateway: %v", err)
	}
	done := make(chan struct{})
	go func() {
		gateway.sendReadResult(readResult{})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sendReadResult stayed blocked after gateway close")
	}
}

func TestRegisterAgentUsesControlReadDeadline(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	t.Cleanup(func() { clientConn.Close() })

	done := make(chan error, 1)
	go func() {
		done <- registerAgent(clientConn, agentRegisterRequest{
			SessionID:       "rs_timeout",
			PairSecret:      "pair-secret",
			ReconnectSecret: "reconnect-secret",
			ExpiresAt:       time.Now().Add(time.Minute),
		})
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected registration read timeout")
		}
	case <-time.After(relayControlTimeout + time.Second):
		t.Fatal("registration did not honor control read deadline")
	}
}

func TestRegisterAgentClearsWriteDeadlineAfterControlWrite(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	t.Cleanup(func() { clientConn.Close() })

	done := make(chan error, 1)
	go func() {
		done <- registerAgent(clientConn, agentRegisterRequest{
			SessionID:       "rs_deadline",
			PairSecret:      "pair-secret",
			ReconnectSecret: "reconnect-secret",
			ExpiresAt:       time.Now().Add(time.Minute),
		})
	}()

	var registered relay.AgentRegisterFrame
	if err := serverConn.ReadJSON(&registered); err != nil {
		t.Fatalf("read agent register: %v", err)
	}
	if err := serverConn.WriteJSON(relay.AgentRegisteredFrame{
		Type: relay.TypeAgentRegistered, Version: relay.Version,
		SessionID: "rs_deadline",
	}); err != nil {
		t.Fatalf("write registration response: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("register agent: %v", err)
	}
	time.Sleep(relayControlTimeout + 100*time.Millisecond)
	if err := clientConn.WriteJSON(map[string]string{"type": "after_deadline"}); err != nil {
		t.Fatalf("write after cleared deadline: %v", err)
	}
	var next map[string]string
	if err := serverConn.ReadJSON(&next); err != nil {
		t.Fatalf("read after cleared deadline: %v", err)
	}
	if next["type"] != "after_deadline" {
		t.Fatalf("unexpected post-deadline frame: %#v", next)
	}
}

func TestRegisterAgentSendsExplicitCapabilities(t *testing.T) {
	serverConn, clientConn := newRelayClientTestConns(t)
	defer serverConn.Close()
	t.Cleanup(func() { clientConn.Close() })

	done := make(chan error, 1)
	go func() {
		done <- registerAgent(clientConn, agentRegisterRequest{
			SessionID:       "rs_capability",
			PairSecret:      "pair-secret",
			ReconnectSecret: "reconnect-secret",
			ExpiresAt:       time.Now().Add(time.Minute),
			Capabilities:    e2ee.PlaintextTestCapabilities(),
		})
	}()

	var registered relay.AgentRegisterFrame
	if err := serverConn.ReadJSON(&registered); err != nil {
		t.Fatalf("read agent register: %v", err)
	}
	if registered.Capabilities == nil {
		t.Fatal("missing agent capabilities")
	}
	if err := e2ee.ValidatePlaintextTestCapabilities(*registered.Capabilities); err != nil {
		t.Fatalf("invalid plaintext capabilities: %v", err)
	}
	if err := serverConn.WriteJSON(relay.AgentRegisteredFrame{
		Type: relay.TypeAgentRegistered, Version: relay.Version,
		SessionID: "rs_capability",
	}); err != nil {
		t.Fatalf("write registration response: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("register agent: %v", err)
	}
}

func TestPairingEventIncludesCapabilities(t *testing.T) {
	event := PairingReadyEvent{
		Type:               "mobilevc.relay.pairing_ready",
		RelayURL:           "wss://relay.example.test",
		SessionID:          "rs_capability",
		PairingSecret:      "pair-secret",
		ExpiresAt:          time.Now().Add(time.Minute).Unix(),
		Capabilities:       e2ee.PlaintextTestCapabilities(),
		NodeFingerprintHex: testNodeFingerprintHex,
	}

	raw, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal pairing event: %v", err)
	}
	var decoded PairingReadyEvent
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode pairing event: %v", err)
	}
	if err := e2ee.ValidatePlaintextTestCapabilities(decoded.Capabilities); err != nil {
		t.Fatalf("decoded capabilities: %v", err)
	}
	if decoded.NodeFingerprintHex != testNodeFingerprintHex {
		t.Fatalf("node fingerprint: got %q", decoded.NodeFingerprintHex)
	}
}

func TestValidateConfigRequiresNodeFingerprint(t *testing.T) {
	err := validateConfig(Config{
		RelayURL:         "wss://relay.example.test",
		PairingTTL:       time.Minute,
		AgentGracePeriod: time.Minute,
		PairingEventPath: "/tmp/mobilevc-relay-pairing.json",
	})
	if err == nil || !strings.Contains(err.Error(), "node fingerprint") {
		t.Fatalf("expected node fingerprint config error, got %v", err)
	}
}

const testNodeFingerprintHex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func testNodeFingerprintHexFromIdentity(identity *e2ee.NodeIdentity) string {
	return fmt.Sprintf("%x", identity.Fingerprint)
}

func testPairingHandshakeHandler(t *testing.T) (*agentE2EEHandshakeHandler, *e2ee.EphemeralKeyPair) {
	t.Helper()
	nodeIdentity, err := e2ee.GenerateNodeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	clientEphemeral, err := e2ee.NewEphemeralKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	return newAgentE2EEHandshakeHandler(
		"rs_gateway",
		"pair-secret-128-bit-minimum",
		e2ee.ProductionCapabilities(),
		nodeIdentity,
	), clientEphemeral
}

func testReconnectHandshakeHandler(t *testing.T) (*agentE2EEHandshakeHandler, *e2ee.EphemeralKeyPair, *e2ee.NodeIdentity, string) {
	t.Helper()
	nodeIdentity, err := e2ee.GenerateNodeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	deviceIdentity, err := e2ee.GenerateNodeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	clientEphemeral, err := e2ee.NewEphemeralKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	trustStore, err := e2ee.LoadOrCreateDeviceTrustStore(t.TempDir() + "/" + e2ee.DeviceTrustFileName)
	if err != nil {
		t.Fatal(err)
	}
	credential := "device-credential-128-bit-minimum"
	if _, err := trustStore.RegisterDevice(e2ee.DeviceRegistration{
		ID: "dev_pixel", DisplayName: "Pixel", PublicKey: deviceIdentity.PublicKey,
		DeviceCredential: credential,
	}); err != nil {
		t.Fatal(err)
	}
	return newAgentE2EEHandshakeHandlerWithDeviceTrust(
		"rs_gateway",
		"pair-secret-128-bit-minimum",
		e2ee.ProductionCapabilities(),
		nodeIdentity,
		trustStore,
	), clientEphemeral, deviceIdentity, credential
}

func driveReconnectHello(t *testing.T, serverConn *websocket.Conn, clientEphemeral *e2ee.EphemeralKeyPair, deviceIdentity *e2ee.NodeIdentity, deviceID string, credential string) *relay.E2EEClientProofFrame {
	t.Helper()
	capabilities := e2ee.ProductionCapabilities()
	clientHello := relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_reconnect",
		Kind: e2ee.HandshakeKindReconnect, Capabilities: &capabilities,
		ClientEphemeralPublicKey: e2ee.EncodeFrameBytes(clientEphemeral.PublicKey),
		DeviceID:                 deviceID,
		DeviceIdentityPublicKey:  e2ee.EncodeFrameBytes(deviceIdentity.PublicKey),
	}
	if err := serverConn.WriteJSON(clientHello); err != nil {
		t.Fatalf("write reconnect hello: %v", err)
	}
	var agentHello relay.E2EEAgentHelloFrame
	if err := serverConn.ReadJSON(&agentHello); err != nil {
		t.Fatalf("read reconnect agent hello: %v", err)
	}
	agentMaterial, err := e2ee.ValidateAgentHelloFrame(agentHello)
	if err != nil {
		t.Fatalf("validate reconnect agent hello: %v", err)
	}
	input := capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     e2ee.HandshakeKindReconnect,
		SessionID:                "rs_gateway",
		ClientID:                 "rc_attached",
		HandshakeID:              "hs_reconnect",
		ClientEphemeralPublicKey: clientEphemeral.PublicKey,
		NodeEphemeralPublicKey:   agentMaterial.NodeEphemeralPublicKey,
		NodeIdentityPublicKey:    agentMaterial.NodeIdentityPublicKey,
		DeviceIdentityPublicKey:  deviceIdentity.PublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		t.Fatal(err)
	}
	deviceSignature, err := deviceIdentity.SignTranscript(transcript)
	if err != nil {
		t.Fatal(err)
	}
	return &relay.E2EEClientProofFrame{
		Type: relay.TypeClientE2EEProof, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_reconnect",
		Kind:            e2ee.HandshakeKindReconnect,
		DeviceProof:     e2ee.EncodeFrameBytes(e2ee.DeviceProof(credential, transcript)),
		DeviceSignature: e2ee.EncodeFrameBytes(deviceSignature),
	}
}

func driveGatewayE2EEHandshake(t *testing.T, serverConn *websocket.Conn, clientEphemeral *e2ee.EphemeralKeyPair) *e2ee.TrafficKeys {
	t.Helper()
	capabilities := e2ee.ProductionCapabilities()
	clientHello := relay.E2EEClientHelloFrame{
		Type: relay.TypeClientE2EEHello, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_pairing",
		Kind: e2ee.HandshakeKindPairing, Capabilities: &capabilities,
		ClientEphemeralPublicKey: e2ee.EncodeFrameBytes(clientEphemeral.PublicKey),
	}
	if err := serverConn.WriteJSON(clientHello); err != nil {
		t.Fatalf("write client hello: %v", err)
	}
	var agentHello relay.E2EEAgentHelloFrame
	if err := serverConn.ReadJSON(&agentHello); err != nil {
		t.Fatalf("read agent hello: %v", err)
	}
	agentMaterial, err := e2ee.ValidateAgentHelloFrame(agentHello)
	if err != nil {
		t.Fatalf("validate agent hello: %v", err)
	}
	input := capabilities.ApplyToHandshake(e2ee.HandshakeInput{
		Kind:                     e2ee.HandshakeKindPairing,
		SessionID:                "rs_gateway",
		ClientID:                 "rc_attached",
		HandshakeID:              "hs_pairing",
		ClientEphemeralPublicKey: clientEphemeral.PublicKey,
		NodeEphemeralPublicKey:   agentMaterial.NodeEphemeralPublicKey,
		NodeIdentityPublicKey:    agentMaterial.NodeIdentityPublicKey,
	})
	transcript, err := e2ee.HandshakeTranscript(input)
	if err != nil {
		t.Fatal(err)
	}
	proof := relay.E2EEClientProofFrame{
		Type: relay.TypeClientE2EEProof, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: "rc_attached", HandshakeID: "hs_pairing",
		Kind:         e2ee.HandshakeKindPairing,
		PairingProof: e2ee.EncodeFrameBytes(e2ee.PairingProof("pair-secret-128-bit-minimum", transcript)),
	}
	if err := serverConn.WriteJSON(proof); err != nil {
		t.Fatalf("write client proof: %v", err)
	}
	var result relay.E2EEAgentResultFrame
	if err := serverConn.ReadJSON(&result); err != nil {
		t.Fatalf("read agent result: %v", err)
	}
	if !result.OK {
		t.Fatalf("unexpected handshake failure: %#v", result)
	}
	keys, err := e2ee.DeriveHandshakeTrafficKeys(
		clientEphemeral.PrivateScalar,
		agentMaterial.NodeEphemeralPublicKey,
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func newRelayClientTestConns(t *testing.T) (*websocket.Conn, *websocket.Conn) {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade relayclient test: %v", err)
			return
		}
		accepted <- conn
	}))
	t.Cleanup(server.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial relayclient test: %v", err)
	}
	select {
	case serverConn := <-accepted:
		return serverConn, client
	case <-time.After(time.Second):
		t.Fatal("relayclient websocket was not accepted")
	}
	return nil, nil
}

func testRelayForward(clientID string) relay.ForwardEnvelope {
	return relay.ForwardEnvelope{
		Type: relay.TypeRelayForward, Version: relay.Version,
		SessionID: "rs_gateway", ClientID: clientID,
		Direction:   relay.DirectionClientToAgent,
		ContentType: relay.ContentTypeMobileVC, Encryption: relay.EncryptionNone,
		MessageID: "msg_test", PayloadEncoding: relay.PayloadBase64URL,
		Payload: "eyJhY3Rpb24iOiJwaW5nIn0",
	}
}
