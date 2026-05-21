package gateway

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"mobilevc/internal/protocol"
	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

func (h *Handler) registerRelayDevice(client ClientConn, req protocol.RelayDeviceRegisterRequestEvent) (protocol.RelayDeviceRegisterResultEvent, error) {
	info, ok := relayE2EEInfo(client)
	if !ok || !info.Enabled {
		return protocol.RelayDeviceRegisterResultEvent{}, fmt.Errorf("%s: relay device registration requires e2ee", relay.CodeE2EERequired)
	}
	if h.DeviceTrust == nil {
		return protocol.RelayDeviceRegisterResultEvent{}, errors.New("relay device trust store is not configured")
	}
	if strings.TrimSpace(req.DeviceID) == "" ||
		strings.TrimSpace(req.DisplayName) == "" ||
		strings.TrimSpace(req.DeviceCredential) == "" {
		return protocol.RelayDeviceRegisterResultEvent{}, fmt.Errorf("%s: missing relay device registration fields", relay.CodeDeviceUnknown)
	}
	publicKey, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(req.DeviceIdentityPublicKey))
	if err != nil {
		return protocol.RelayDeviceRegisterResultEvent{}, fmt.Errorf("%s: invalid relay device identity encoding", relay.CodeE2EEHandshakeFailed)
	}
	device, err := h.DeviceTrust.RegisterDevice(e2ee.DeviceRegistration{
		ID:               req.DeviceID,
		DisplayName:      req.DisplayName,
		PublicKey:        publicKey,
		DeviceCredential: req.DeviceCredential,
		Now:              time.Now().UTC(),
	})
	if err != nil {
		return protocol.RelayDeviceRegisterResultEvent{}, err
	}
	return protocol.NewRelayDeviceRegisterResultEvent(
		info.SessionID,
		device.ID,
		fmt.Sprintf("%x", device.Fingerprint),
		"registered",
	), nil
}

func relayE2EEInfo(client ClientConn) (RelayE2EEInfo, bool) {
	relayClient, ok := client.(relayE2EEClientConn)
	if !ok {
		return RelayE2EEInfo{}, false
	}
	info := relayClient.RelayE2EEInfo()
	return info, info.SessionID != "" && info.ClientID != "" && info.HandshakeID != ""
}

func relayDeviceRegisterErrorCode(err error) string {
	message := err.Error()
	for _, code := range []string{
		relay.CodeE2EERequired,
		relay.CodeE2EEHandshakeFailed,
		relay.CodeDeviceRevoked,
		relay.CodeDeviceUnknown,
	} {
		if strings.Contains(message, code) {
			return code
		}
	}
	return relay.CodeE2EEHandshakeFailed
}
