package relayclient

import (
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
