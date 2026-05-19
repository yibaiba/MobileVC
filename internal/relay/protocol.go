package relay

const (
	Version = 1

	TypeAgentRegister   = "agent.register"
	TypeAgentRegistered = "agent.registered"
	TypeAgentReconnect  = "agent.reconnect"
	TypeClientPair      = "client.pair"
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
}

type ClientPairedFrame struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	SessionID string `json:"sessionId"`
	ClientID  string `json:"clientId"`
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
}

type ErrorFrame struct {
	Type    string `json:"type"`
	Version int    `json:"version"`
	Code    string `json:"code"`
	Message string `json:"message"`
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
	default:
		return "protocol error"
	}
}
