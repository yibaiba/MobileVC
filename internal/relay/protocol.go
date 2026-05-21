package relay

const (
	Version = 1

	TypeAgentRegister   = "agent.register"
	TypeAgentRegistered = "agent.registered"
	TypeAgentReconnect  = "agent.reconnect"
	TypeClientPair      = "client.pair"
	TypeClientReconnect = "client.reconnect"
	TypeClientPaired    = "client.paired"
	TypeClientAttached  = "client.attached"
	TypeRelayForward    = "relay.forward"
	TypeRelayError      = "relay.error"
	TypeRelayPing       = "relay.ping"
	TypeRelayPong       = "relay.pong"

	DirectionClientToAgent = "client_to_agent"
	DirectionAgentToClient = "agent_to_client"
	ContentTypeMobileVC    = "mobilevc.ws.v1"
	EncryptionNone         = "none"
	EncryptionE2EEV1       = "p256-ecdsa+p256-ecdh+hkdf-sha256+aes-256-gcm"
	PayloadBase64URL       = "base64url"
)

const (
	CodePairingRejected      = "pairing_rejected"
	CodeUnauthorized         = "unauthorized"
	CodeCapacityReached      = "capacity_reached"
	CodeTimeout              = "timeout"
	CodeFrameTooLarge        = "frame_too_large"
	CodePayloadTooLarge      = "payload_too_large"
	CodeProtocolError        = "protocol_error"
	CodeTargetUnavailable    = "target_unavailable"
	CodeQueueFull            = "queue_full"
	CodeAgentDisconnected    = "agent_disconnected"
	CodeControllerDisconnect = "controller_disconnected"
	CodeE2EERequired         = "e2ee_required"
	CodeE2EEUnsupported      = "e2ee_unsupported_version"
	CodeE2EEHandshakeFailed  = "e2ee_handshake_failed"
	CodeE2EEDecryptFailed    = "e2ee_decrypt_failed"
	CodeE2EEReplayDetected   = "e2ee_replay_detected"
	CodeDeviceRevoked        = "device_revoked"
	CodeDeviceUnknown        = "device_unknown"
	CodeStreamCancelled      = "stream_cancelled"
	CodeStreamWindowExceeded = "stream_window_exceeded"
	CodeDownloadDenied       = "download_denied"
	CodeDownloadFailed       = "download_failed"
)

type ControlFrame struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
}

type AgentRegisterFrame struct {
	Type                     string `json:"type"`
	Version                  int    `json:"version"`
	SessionID                string `json:"sessionId"`
	PairingSecretHash        string `json:"pairingSecretHash"`
	AgentReconnectSecretHash string `json:"agentReconnectSecretHash"`
	PairingExpiresAt         int64  `json:"pairingExpiresAt"`
}

type AgentRegisteredFrame struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	SessionID string `json:"sessionId"`
}

type AgentReconnectFrame struct {
	Type                 string `json:"type"`
	Version              int    `json:"version"`
	SessionID            string `json:"sessionId"`
	AgentReconnectSecret string `json:"agentReconnectSecret"`
}

type ClientPairFrame struct {
	Type          string `json:"type"`
	Version       int    `json:"version"`
	SessionID     string `json:"sessionId"`
	PairingSecret string `json:"pairingSecret"`
	DeviceName    string `json:"deviceName,omitempty"`
}

type ClientReconnectFrame struct {
	Type                  string `json:"type"`
	Version               int    `json:"version"`
	SessionID             string `json:"sessionId"`
	ClientID              string `json:"clientId"`
	ClientReconnectSecret string `json:"clientReconnectSecret"`
}

type ClientPairedFrame struct {
	Type                  string `json:"type"`
	Version               int    `json:"version"`
	SessionID             string `json:"sessionId"`
	ClientID              string `json:"clientId"`
	ClientReconnectSecret string `json:"clientReconnectSecret,omitempty"`
}

type ClientAttachedFrame struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	SessionID string `json:"sessionId"`
	ClientID  string `json:"clientId"`
}

type ForwardEnvelope struct {
	Type            string `json:"type"`
	Version         int    `json:"version"`
	SessionID       string `json:"sessionId"`
	ClientID        string `json:"clientId"`
	Direction       string `json:"direction"`
	MessageID       string `json:"messageId"`
	ContentType     string `json:"contentType"`
	Encryption      string `json:"encryption"`
	PayloadEncoding string `json:"payloadEncoding"`
	Payload         string `json:"payload"`
	StreamID        uint64 `json:"streamId,omitempty"`
	Counter         uint64 `json:"counter,omitempty"`
	HandshakeID     string `json:"handshakeId,omitempty"`
}

type ErrorFrame struct {
	Type         string `json:"type"`
	Version      int    `json:"version"`
	Code         string `json:"code"`
	Message      string `json:"message"`
	DecodedBytes int    `json:"decodedBytes,omitempty"`
	MaxBytes     int    `json:"maxBytes,omitempty"`
}

func NewErrorFrame(code string) ErrorFrame {
	return ErrorFrame{
		Type:    TypeRelayError,
		Version: Version,
		Code:    code,
		Message: defaultErrorMessage(code),
	}
}

func defaultErrorMessage(code string) string {
	switch code {
	case CodePairingRejected:
		return "pairing rejected"
	case CodeUnauthorized:
		return "unauthorized"
	case CodeCapacityReached:
		return "capacity reached"
	case CodeTimeout:
		return "timeout"
	case CodeFrameTooLarge:
		return "frame too large"
	case CodePayloadTooLarge:
		return "payload too large"
	case CodeTargetUnavailable:
		return "target unavailable"
	case CodeQueueFull:
		return "queue full"
	case CodeAgentDisconnected:
		return "agent disconnected"
	case CodeControllerDisconnect:
		return "controller disconnected"
	case CodeE2EERequired:
		return "e2ee required"
	case CodeE2EEUnsupported:
		return "e2ee unsupported version"
	case CodeE2EEHandshakeFailed:
		return "e2ee handshake failed"
	case CodeE2EEDecryptFailed:
		return "e2ee decrypt failed"
	case CodeE2EEReplayDetected:
		return "e2ee replay detected"
	case CodeDeviceRevoked:
		return "device revoked"
	case CodeDeviceUnknown:
		return "device unknown"
	case CodeStreamCancelled:
		return "stream cancelled"
	case CodeStreamWindowExceeded:
		return "stream window exceeded"
	case CodeDownloadDenied:
		return "download denied"
	case CodeDownloadFailed:
		return "download failed"
	default:
		return "protocol error"
	}
}
