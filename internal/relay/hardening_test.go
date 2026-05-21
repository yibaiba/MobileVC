package relay

import (
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const testShortAgentGrace = 50 * time.Millisecond

func TestRelayExpiresDisconnectedAgentSessionAfterGrace(t *testing.T) {
	server := newLimitedTestRelayServer(t, Config{AgentGracePeriod: testShortAgentGrace})
	defer server.Close()

	sessionID := "rs_expire_agent"
	first := dialRelay(t, server.URL, "/relay/agent")
	registerAgent(t, first, sessionID, "pair-secret-one", "reconnect-secret-one", time.Now().Add(time.Minute))
	_ = first.Close()
	time.Sleep(testShortAgentGrace * 2)

	second := dialRelay(t, server.URL, "/relay/agent")
	defer second.Close()
	registerAgent(t, second, sessionID, "pair-secret-two", "reconnect-secret-two", time.Now().Add(time.Minute))
}

func TestRelayClosesClientWhenDisconnectedAgentExpires(t *testing.T) {
	server := newLimitedTestRelayServer(t, Config{AgentGracePeriod: testShortAgentGrace})
	defer server.Close()

	sessionID := "rs_close_client"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))
	client := dialRelay(t, server.URL, "/relay/client")
	defer client.Close()
	pairClient(t, client, sessionID, secret)

	_ = agent.Close()
	_ = client.SetReadDeadline(time.Now().Add(testShortAgentGrace * 4))
	if err := client.ReadJSON(&ErrorFrame{}); err == nil {
		t.Fatal("expected relay client connection to close")
	}
}

func TestRelayNotifiesAgentWhenClientAttaches(t *testing.T) {
	server := newTestRelayServer(t)
	defer server.Close()

	sessionID := "rs_attached"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))
	client := dialRelay(t, server.URL, "/relay/client")
	defer client.Close()
	clientID := pairClient(t, client, sessionID, secret)

	var attached ClientAttachedFrame
	if err := agent.ReadJSON(&attached); err != nil {
		t.Fatalf("read client attached: %v", err)
	}
	if attached.SessionID != sessionID || attached.ClientID != clientID {
		t.Fatalf("unexpected client attached frame: %#v", attached)
	}
}

func TestRelayNotifiesReconnectedAgentWhenClientIsAttached(t *testing.T) {
	server := newTestRelayServer(t)
	defer server.Close()

	sessionID := "rs_reconnect_attached"
	secret := "pair-secret-128-bit-minimum"
	reconnectSecret := "reconnect-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	registerAgent(t, agent, sessionID, secret, reconnectSecret, time.Now().Add(time.Minute))
	client := dialRelay(t, server.URL, "/relay/client")
	defer client.Close()
	clientID := pairClient(t, client, sessionID, secret)
	readAttached(t, agent, clientID)
	_ = agent.Close()

	reconnected := reconnectAgentEventually(t, server.URL, sessionID, reconnectSecret)
	defer reconnected.Close()
	readAttached(t, reconnected, clientID)
}

func TestRelayAllowsFirstAgentForwardWithEmptyClientID(t *testing.T) {
	server := newTestRelayServer(t)
	defer server.Close()

	sessionID := "rs_empty_client"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))
	client := dialRelay(t, server.URL, "/relay/client")
	defer client.Close()
	clientID := pairClient(t, client, sessionID, secret)
	readAttached(t, agent, clientID)

	env := testEnvelope(sessionID, "", DirectionAgentToClient, []byte(`{"event":"initial"}`))
	if err := agent.WriteJSON(env); err != nil {
		t.Fatalf("write empty-client agent forward: %v", err)
	}
	var got ForwardEnvelope
	if err := client.ReadJSON(&got); err != nil {
		t.Fatalf("read normalized agent forward: %v", err)
	}
	if got.ClientID != clientID || got.Payload != env.Payload {
		t.Fatalf("unexpected normalized forward: %#v", got)
	}
}

func readAttached(t *testing.T, conn *websocket.Conn, clientID string) {
	t.Helper()
	var attached ClientAttachedFrame
	if err := conn.ReadJSON(&attached); err != nil {
		t.Fatalf("read client attached: %v", err)
	}
	if attached.ClientID != clientID {
		t.Fatalf("attached client id: got %q want %q", attached.ClientID, clientID)
	}
}

func reconnectAgentEventually(t *testing.T, baseURL string, sessionID string, secret string) *websocket.Conn {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn := dialRelay(t, baseURL, "/relay/agent")
		if tryReconnectAgent(t, conn, sessionID, secret) {
			return conn
		}
		_ = conn.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("agent reconnect did not succeed before timeout")
	return nil
}

func tryReconnectAgent(t *testing.T, conn *websocket.Conn, sessionID string, secret string) bool {
	t.Helper()
	if err := conn.WriteJSON(AgentReconnectFrame{
		Type: TypeAgentReconnect, Version: Version,
		SessionID: sessionID, AgentReconnectSecret: secret,
	}); err != nil {
		t.Fatalf("write reconnect frame: %v", err)
	}
	var registered AgentRegisteredFrame
	if err := conn.ReadJSON(&registered); err != nil {
		t.Fatalf("read reconnect response: %v", err)
	}
	return registered.Type == TypeAgentRegistered && registered.SessionID == sessionID
}
