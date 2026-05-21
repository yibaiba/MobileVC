package relayclient

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mobilevc/internal/relay"
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
