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
}

type readResult struct {
	env relay.ForwardEnvelope
	err error
}

const relayReadQueueSize = 8

func newGatewayConn(conn *websocket.Conn, sessionID string) *gatewayConn {
	gateway := &gatewayConn{
		conn: conn, sessionID: sessionID,
		attachCh: make(chan struct{}), readCh: make(chan readResult, relayReadQueueSize),
		readDone: make(chan struct{}), closeCh: make(chan struct{}),
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
		var env relay.ForwardEnvelope
		if err := c.conn.ReadJSON(&env); err != nil {
			c.setReadError(err)
			c.sendReadResult(readResult{err: err})
			return
		}
		if c.acceptClientAttached(env) {
			continue
		}
		if c.acceptRelayPing(env) {
			continue
		}
		c.sendReadResult(readResult{env: env})
	}
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
	payload, err := relay.DecodePayloadBase64URL(env.Payload)
	if err != nil {
		return fmt.Errorf("decode relay payload: %w", err)
	}
	return json.Unmarshal(payload, v)
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

func (c *gatewayConn) acceptClientAttached(env relay.ForwardEnvelope) bool {
	if env.Type != relay.TypeClientAttached || env.SessionID != c.sessionID {
		return false
	}
	c.setClientID(env.ClientID)
	return true
}

func (c *gatewayConn) acceptRelayPing(env relay.ForwardEnvelope) bool {
	if env.Type != relay.TypeRelayPing {
		return false
	}
	if strings.TrimSpace(env.SessionID) != "" && env.SessionID != c.sessionID {
		return false
	}
	if err := c.writeControl(relay.ControlFrame{Type: relay.TypeRelayPong, Version: relay.Version}); err != nil {
		c.setReadError(err)
		c.sendReadResult(readResult{err: err})
	}
	return true
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
	return c.writeControl(c.forwardEnvelope(payload))
}

func (c *gatewayConn) writeControl(frame any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(frame)
}

func (c *gatewayConn) forwardEnvelope(payload []byte) relay.ForwardEnvelope {
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
	}
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
