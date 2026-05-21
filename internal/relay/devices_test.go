package relay

import (
	"testing"
	"time"
)

func TestRelayPairingCreatesDevice(t *testing.T) {
	relayServer, server := newInspectableRelayServer(t, Config{})
	defer server.Close()

	sessionID := "rs_device_pair"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))

	client := dialRelay(t, server.URL, "/relay/client")
	defer client.Close()
	clientID := pairClient(t, client, sessionID, secret)

	devices := relayServer.Devices(sessionID)
	if len(devices) != 1 {
		t.Fatalf("device count: got %d want 1", len(devices))
	}
	if devices[0].ClientID != clientID || !devices[0].Connected || devices[0].Revoked {
		t.Fatalf("unexpected device info: %#v", devices[0])
	}
}

func TestRelayClientReconnectUpdatesDeviceLastSeen(t *testing.T) {
	relayServer, server := newInspectableRelayServer(t, Config{})
	defer server.Close()

	sessionID := "rs_device_reconnect"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))

	client := dialRelay(t, server.URL, "/relay/client")
	clientID, reconnectSecret := pairClientWithReconnectSecret(t, client, sessionID, secret)
	readAttached(t, agent, clientID)
	before := onlyDevice(t, relayServer.Devices(sessionID)).LastSeenAt
	_ = client.Close()
	time.Sleep(time.Millisecond)

	reconnected := dialRelay(t, server.URL, "/relay/client")
	defer reconnected.Close()
	reconnectClient(t, reconnected, sessionID, clientID, reconnectSecret)
	after := onlyDevice(t, relayServer.Devices(sessionID))

	if after.ClientID != clientID || !after.Connected || !after.LastSeenAt.After(before) {
		t.Fatalf("unexpected reconnected device: before=%s after=%#v", before, after)
	}
}

func TestRelayRevokedDeviceCannotReconnect(t *testing.T) {
	relayServer, server := newInspectableRelayServer(t, Config{})
	defer server.Close()

	sessionID := "rs_device_revoke"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))

	client := dialRelay(t, server.URL, "/relay/client")
	clientID, reconnectSecret := pairClientWithReconnectSecret(t, client, sessionID, secret)
	readAttached(t, agent, clientID)

	if !relayServer.RevokeDevice(sessionID, clientID) {
		t.Fatal("expected device revoke to succeed")
	}
	devices := relayServer.Devices(sessionID)
	if got := onlyDevice(t, devices); !got.Revoked || got.Connected {
		t.Fatalf("unexpected revoked device: %#v", got)
	}

	reconnected := dialRelay(t, server.URL, "/relay/client")
	defer reconnected.Close()
	writeReconnectClient(t, reconnected, sessionID, clientID, reconnectSecret)
	readPairingRejected(t, reconnected)
}

func TestRelayRotateSessionCredentialsRevokesDevices(t *testing.T) {
	relayServer, server := newInspectableRelayServer(t, Config{})
	defer server.Close()

	sessionID := "rs_device_rotate"
	secret := "pair-secret-128-bit-minimum"
	agent := dialRelay(t, server.URL, "/relay/agent")
	defer agent.Close()
	registerAgent(t, agent, sessionID, secret, "reconnect-secret", time.Now().Add(time.Minute))

	client := dialRelay(t, server.URL, "/relay/client")
	clientID, reconnectSecret := pairClientWithReconnectSecret(t, client, sessionID, secret)
	readAttached(t, agent, clientID)

	if !relayServer.RotateSessionCredentials(sessionID) {
		t.Fatal("expected credential rotation to succeed")
	}
	devices := relayServer.Devices(sessionID)
	if got := onlyDevice(t, devices); !got.Revoked || got.Connected {
		t.Fatalf("unexpected rotated device: %#v", got)
	}

	reconnected := dialRelay(t, server.URL, "/relay/client")
	defer reconnected.Close()
	writeReconnectClient(t, reconnected, sessionID, clientID, reconnectSecret)
	readPairingRejected(t, reconnected)
}

func reconnectClient(t *testing.T, conn interface {
	WriteJSON(any) error
	ReadJSON(any) error
}, sessionID string, clientID string, secret string) {
	t.Helper()
	writeReconnectClient(t, conn, sessionID, clientID, secret)
	var paired ClientPairedFrame
	if err := conn.ReadJSON(&paired); err != nil {
		t.Fatalf("read client reconnect response: %v", err)
	}
	if paired.Type != TypeClientPaired || paired.ClientID != clientID {
		t.Fatalf("unexpected client reconnect response: %#v", paired)
	}
}

func writeReconnectClient(t *testing.T, conn interface{ WriteJSON(any) error }, sessionID string, clientID string, secret string) {
	t.Helper()
	if err := conn.WriteJSON(ClientReconnectFrame{
		Type:                  TypeClientReconnect,
		Version:               Version,
		SessionID:             sessionID,
		ClientID:              clientID,
		ClientReconnectSecret: secret,
	}); err != nil {
		t.Fatalf("write client reconnect: %v", err)
	}
}

func readPairingRejected(t *testing.T, conn interface{ ReadJSON(any) error }) {
	t.Helper()
	var errFrame ErrorFrame
	if err := conn.ReadJSON(&errFrame); err != nil {
		t.Fatalf("read pairing rejected: %v", err)
	}
	if errFrame.Code != CodePairingRejected {
		t.Fatalf("unexpected error frame: %#v", errFrame)
	}
}

func onlyDevice(t *testing.T, devices []DeviceInfo) DeviceInfo {
	t.Helper()
	if len(devices) != 1 {
		t.Fatalf("device count: got %d want 1", len(devices))
	}
	return devices[0]
}
