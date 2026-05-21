package gateway

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"mobilevc/internal/protocol"
	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

func TestRegisterRelayDeviceRequiresE2EE(t *testing.T) {
	h := NewHandler("test", nil)
	store, err := e2ee.LoadOrCreateDeviceTrustStore(filepath.Join(t.TempDir(), e2ee.DeviceTrustFileName))
	if err != nil {
		t.Fatal(err)
	}
	h.DeviceTrust = store

	_, err = h.registerRelayDevice(staticRelayClientConn{}, testRelayDeviceRegisterRequest(t))
	if err == nil || !strings.Contains(err.Error(), relay.CodeE2EERequired) {
		t.Fatalf("expected e2ee required error, got %v", err)
	}
}

func TestRegisterRelayDevicePersistsTrustedDeviceWithoutPlaintextCredential(t *testing.T) {
	trustPath := filepath.Join(t.TempDir(), e2ee.DeviceTrustFileName)
	store, err := e2ee.LoadOrCreateDeviceTrustStore(trustPath)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler("test", nil)
	h.DeviceTrust = store
	req := testRelayDeviceRegisterRequest(t)

	result, err := h.registerRelayDevice(staticRelayClientConn{
		info: RelayE2EEInfo{
			Enabled:     true,
			SessionID:   "rs_gateway",
			ClientID:    "rc_attached",
			HandshakeID: "hs_pairing",
		},
	}, req)
	if err != nil {
		t.Fatalf("register relay device: %v", err)
	}
	if result.DeviceID != req.DeviceID || result.Status != "registered" || result.SessionID != "rs_gateway" {
		t.Fatalf("unexpected registration result: %#v", result)
	}
	devices, err := store.ListDevices()
	if err != nil {
		t.Fatal(err)
	}
	if len(devices) != 1 || devices[0].ID != req.DeviceID || devices[0].CredentialHash == req.DeviceCredential {
		t.Fatalf("unexpected trusted device: %#v", devices)
	}
	if strings.Contains(readTestFile(t, trustPath), req.DeviceCredential) {
		t.Fatal("device trust store contains plaintext credential")
	}
}

func testRelayDeviceRegisterRequest(t *testing.T) protocol.RelayDeviceRegisterRequestEvent {
	t.Helper()
	identity, err := e2ee.GenerateNodeIdentity()
	if err != nil {
		t.Fatal(err)
	}
	return protocol.RelayDeviceRegisterRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "relay_device_register"},
		DeviceID:    "dev_test",
		DisplayName: "Pixel",
		DeviceIdentityPublicKey: base64.RawURLEncoding.EncodeToString(
			identity.PublicKey,
		),
		DeviceCredential: "device-credential-128-bit-minimum",
	}
}

type staticRelayClientConn struct {
	info RelayE2EEInfo
}

func (c staticRelayClientConn) ReadJSON(any) error  { return nil }
func (c staticRelayClientConn) WriteJSON(any) error { return nil }
func (c staticRelayClientConn) Close() error        { return nil }
func (c staticRelayClientConn) RemoteAddr() string  { return "relay:test" }
func (c staticRelayClientConn) Origin() string      { return "relay" }
func (c staticRelayClientConn) RelayE2EEInfo() RelayE2EEInfo {
	return c.info
}

func readTestFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
