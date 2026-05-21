package relayclient

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

type gatewayConn struct {
	conn       *websocket.Conn
	sessionID  string
	clientID   string
	mu         sync.Mutex
	attachCh   chan struct{}
	attachOnce sync.Once
	readCh     chan readResult
	readDone   chan struct{}
	readErr    error
	closeCh    chan struct{}
	closeOnce  sync.Once
	e2ee       *agentE2EEHandshakeHandler
	stream     *e2ee.MobileVCStreamCodec
}

type readResult struct {
	env relay.ForwardEnvelope
	err error
}

const relayReadQueueSize = 8

func newGatewayConn(conn *websocket.Conn, sessionID string) *gatewayConn {
	return newGatewayConnWithE2EE(conn, sessionID, nil)
}

func newGatewayConnWithE2EE(conn *websocket.Conn, sessionID string, e2eeHandler *agentE2EEHandshakeHandler) *gatewayConn {
	gateway := &gatewayConn{
		conn: conn, sessionID: sessionID,
		attachCh: make(chan struct{}), readCh: make(chan readResult, relayReadQueueSize),
		readDone: make(chan struct{}), closeCh: make(chan struct{}), e2ee: e2eeHandler,
	}
	go gateway.readLoop()
	return gateway
}

func (c *gatewayConn) ReadJSON(v any) error {
	for {
		result, ok := <-c.readCh
		if !ok {
			return c.readError()
		}
		if result.err != nil {
			return result.err
		}
		if err := c.decodeForward(result.env, v); err != nil {
			return err
		}
		return nil
	}
}

func (c *gatewayConn) readLoop() {
	defer close(c.readCh)
	defer close(c.readDone)
	for {
		var raw map[string]any
		if err := c.conn.ReadJSON(&raw); err != nil {
			c.setReadError(err)
			c.sendReadResult(readResult{err: err})
			return
		}
		env, err := c.dispatchRawFrame(raw)
		if err != nil {
			c.setReadError(err)
			c.sendReadResult(readResult{err: err})
			return
		}
		if env == nil {
			continue
		}
		c.sendReadResult(readResult{env: *env})
	}
}

func (c *gatewayConn) dispatchRawFrame(raw map[string]any) (*relay.ForwardEnvelope, error) {
	frameType, _ := raw["type"].(string)
	switch frameType {
	case relay.TypeClientAttached:
		var frame relay.ClientAttachedFrame
		if err := decodeRawFrame(raw, &frame); err != nil {
			return nil, err
		}
		if frame.SessionID == c.sessionID {
			c.setClientID(frame.ClientID)
		}
		return nil, nil
	case relay.TypeRelayPing:
		var frame relayPingFrame
		if err := decodeRawFrame(raw, &frame); err != nil {
			return nil, err
		}
		if strings.TrimSpace(frame.SessionID) != "" && frame.SessionID != c.sessionID {
			return nil, fmt.Errorf("invalid relay ping routing")
		}
		if err := c.writeControl(relay.ControlFrame{Type: relay.TypeRelayPong, Version: relay.Version}); err != nil {
			return nil, err
		}
		return nil, nil
	case relay.TypeClientE2EEHello:
		return nil, c.handleClientE2EEHello(raw)
	case relay.TypeClientE2EEProof:
		return nil, c.handleClientE2EEProof(raw)
	case relay.TypeAgentE2EEHello, relay.TypeAgentE2EEResult:
		return nil, fmt.Errorf("unexpected agent e2ee control frame on local relay agent")
	}
	var env relay.ForwardEnvelope
	if err := decodeRawFrame(raw, &env); err != nil {
		return nil, err
	}
	return &env, nil
}

func decodeRawFrame(raw map[string]any, out any) error {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return err
	}
	return json.Unmarshal(encoded, out)
}

type relayPingFrame struct {
	Type      string `json:"type"`
	Version   int    `json:"version"`
	SessionID string `json:"sessionId,omitempty"`
}

func (c *gatewayConn) handleClientE2EEHello(raw map[string]any) error {
	var frame relay.E2EEClientHelloFrame
	if err := decodeRawFrame(raw, &frame); err != nil {
		return err
	}
	if c.e2ee == nil {
		return fmt.Errorf("relay e2ee handshake is not connected to the local agent yet")
	}
	response, err := c.e2ee.handleClientHello(frame)
	if err != nil {
		return err
	}
	return c.writeControl(response)
}

func (c *gatewayConn) handleClientE2EEProof(raw map[string]any) error {
	var frame relay.E2EEClientProofFrame
	if err := decodeRawFrame(raw, &frame); err != nil {
		return err
	}
	if c.e2ee == nil {
		return fmt.Errorf("relay e2ee handshake is not connected to the local agent yet")
	}
	response, err := c.e2ee.handleClientProof(frame)
	if writeErr := c.writeControl(response); writeErr != nil {
		return writeErr
	}
	if response.OK {
		if err := c.activateE2EEStream(frame.HandshakeID); err != nil {
			return err
		}
	}
	return err
}

func (c *gatewayConn) activateE2EEStream(handshakeID string) error {
	if c.e2ee == nil {
		return fmt.Errorf("relay e2ee handshake is not configured")
	}
	codec, ok, err := c.e2ee.completedCodec(handshakeID)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("relay e2ee traffic keys missing for completed handshake")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stream = codec
	return nil
}

func (c *gatewayConn) sendReadResult(result readResult) {
	select {
	case c.readCh <- result:
	case <-c.closeCh:
	}
}

func (c *gatewayConn) decodeForward(env relay.ForwardEnvelope, v any) error {
	if env.Type != relay.TypeRelayForward {
		return fmt.Errorf("unexpected relay frame: %s", env.Type)
	}
	if env.Direction != relay.DirectionClientToAgent || env.SessionID != c.sessionID {
		return fmt.Errorf("invalid relay forward routing")
	}
	c.setClientID(env.ClientID)
	if env.Encryption == relay.EncryptionE2EEV1 {
		return c.decodeEncryptedForward(env, v)
	}
	if c.hasE2EEStream() {
		return fmt.Errorf("%s: plaintext relay forward after e2ee activation", relay.CodeE2EERequired)
	}
	if env.Encryption != relay.EncryptionNone {
		return fmt.Errorf("%s: unsupported relay forward encryption", relay.CodeE2EEUnsupported)
	}
	payload, err := relay.DecodePayloadBase64URL(env.Payload)
	if err != nil {
		return fmt.Errorf("decode relay payload: %w", err)
	}
	return json.Unmarshal(payload, v)
}

func (c *gatewayConn) decodeEncryptedForward(env relay.ForwardEnvelope, v any) error {
	codec := c.e2eeStream()
	if codec == nil {
		return fmt.Errorf("%s: encrypted relay forward before e2ee activation", relay.CodeE2EEHandshakeFailed)
	}
	frame := e2ee.RelayForwardFrame(env)
	if err := codec.DecodeJSON(frame, v); err != nil {
		if strings.Contains(err.Error(), "replay") {
			return fmt.Errorf("%s: %w", relay.CodeE2EEReplayDetected, err)
		}
		return fmt.Errorf("%s: %w", relay.CodeE2EEDecryptFailed, err)
	}
	return nil
}

func (c *gatewayConn) WriteJSON(v any) error {
	payload, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if err := c.waitAttached(); err != nil {
		return err
	}
	return c.writeForward(payload)
}

func (c *gatewayConn) setClientID(clientID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.clientID = strings.TrimSpace(clientID)
	if c.clientID != "" {
		c.attachOnce.Do(func() { close(c.attachCh) })
	}
}

func (c *gatewayConn) waitAttached() error {
	select {
	case <-c.attachCh:
		return nil
	case <-c.readDone:
		return c.readError()
	case <-c.closeCh:
		return c.readError()
	}
}

func (c *gatewayConn) setReadError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.readErr = err
}

func (c *gatewayConn) readError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr != nil {
		return c.readErr
	}
	return fmt.Errorf("relay connection closed before client attached")
}

func (c *gatewayConn) writeForward(payload []byte) error {
	env, err := c.forwardEnvelope(payload)
	if err != nil {
		return err
	}
	return c.writeControl(env)
}

func (c *gatewayConn) writeControl(frame any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(frame)
}

func (c *gatewayConn) forwardEnvelope(payload []byte) (relay.ForwardEnvelope, error) {
	if codec := c.e2eeStream(); codec != nil {
		frame, err := codec.Encode("msg_"+uuid.NewString(), payload)
		if err != nil {
			return relay.ForwardEnvelope{}, fmt.Errorf("%s: %w", relay.CodeE2EEDecryptFailed, err)
		}
		return relay.ForwardEnvelope(frame), nil
	}
	return relay.ForwardEnvelope{
		Type:            relay.TypeRelayForward,
		Version:         relay.Version,
		SessionID:       c.sessionID,
		ClientID:        c.clientID,
		Direction:       relay.DirectionAgentToClient,
		MessageID:       "msg_" + uuid.NewString(),
		ContentType:     relay.ContentTypeMobileVC,
		Encryption:      relay.EncryptionNone,
		PayloadEncoding: relay.PayloadBase64URL,
		Payload:         base64.RawURLEncoding.EncodeToString(payload),
	}, nil
}

func (c *gatewayConn) e2eeStream() *e2ee.MobileVCStreamCodec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream
}

func (c *gatewayConn) hasE2EEStream() bool {
	return c.e2eeStream() != nil
}

func (c *gatewayConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	return c.conn.Close()
}

func (c *gatewayConn) RemoteAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.clientID == "" {
		return "relay:" + c.sessionID
	}
	return "relay:" + c.sessionID + "/" + c.clientID
}

func (c *gatewayConn) Origin() string {
	return "relay"
}
