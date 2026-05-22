package relayclient

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"mobilevc/internal/gateway"
	"mobilevc/internal/relay"
	"mobilevc/internal/relay/e2ee"
)

var errRelayTunnelFrameConsumed = fmt.Errorf("relay encrypted tunnel frame consumed")

type gatewayConn struct {
	conn          *websocket.Conn
	sessionID     string
	downloadRoots []string
	clientID      string
	mu            sync.Mutex
	attachCh      chan struct{}
	attachOnce    sync.Once
	readCh        chan readResult
	readDone      chan struct{}
	readErr       error
	closeCh       chan struct{}
	closeOnce     sync.Once
	e2eeReadyCh   chan struct{}
	e2eeReadyOnce sync.Once
	e2ee          *agentE2EEHandshakeHandler
	stream        *e2ee.MobileVCStreamCodec
	streamHS      string
	deviceID      string
	tunnelSend    *e2ee.TunnelCounterState
	downloadsMu   sync.Mutex
	downloads     map[uint64]*fileDownloadStream
}

type readResult struct {
	env relay.ForwardEnvelope
	err error
}

const relayReadQueueSize = 8

func newGatewayConn(conn *websocket.Conn, sessionID string) *gatewayConn {
	return newGatewayConnWithE2EE(conn, sessionID, nil)
}

func newGatewayConnWithE2EE(conn *websocket.Conn, sessionID string, e2eeHandler *agentE2EEHandshakeHandler, downloadRoots ...[]string) *gatewayConn {
	roots := []string(nil)
	if len(downloadRoots) > 0 {
		roots = downloadRoots[0]
	}
	gateway := &gatewayConn{
		conn: conn, sessionID: sessionID, downloadRoots: append([]string(nil), roots...),
		attachCh: make(chan struct{}), readCh: make(chan readResult, relayReadQueueSize),
		readDone: make(chan struct{}), closeCh: make(chan struct{}), e2ee: e2eeHandler,
		tunnelSend: e2ee.NewTunnelCounterState(),
		downloads:  map[uint64]*fileDownloadStream{},
	}
	if e2eeHandler != nil {
		gateway.e2eeReadyCh = make(chan struct{})
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
			if errors.Is(err, errRelayTunnelFrameConsumed) {
				continue
			}
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
	c.stream = codec
	c.streamHS = handshakeID
	c.mu.Unlock()
	c.e2eeReadyOnce.Do(func() {
		if c.e2eeReadyCh != nil {
			close(c.e2eeReadyCh)
		}
	})
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
	if env.Encryption != relay.EncryptionNone {
		return fmt.Errorf("%s: unsupported relay forward encryption", relay.CodeE2EEUnsupported)
	}
	if c.requiresE2EE() {
		return fmt.Errorf("%s: plaintext relay forward before e2ee activation", relay.CodeE2EERequired)
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
	if frame.StreamID != e2ee.MobileVCStreamID {
		return c.handleEncryptedTunnelForward(codec, frame)
	}
	if err := codec.DecodeJSON(frame, v); err != nil {
		if strings.Contains(err.Error(), "replay") {
			return fmt.Errorf("%s: %w", relay.CodeE2EEReplayDetected, err)
		}
		return fmt.Errorf("%s: %w", relay.CodeE2EEDecryptFailed, err)
	}
	return nil
}

func (c *gatewayConn) handleEncryptedTunnelForward(codec *e2ee.MobileVCStreamCodec, frame e2ee.RelayForwardFrame) error {
	tunnelFrame, err := codec.DecodeTunnelFrame(frame)
	if err != nil {
		if strings.Contains(err.Error(), "replay") {
			return fmt.Errorf("%s: %w", relay.CodeE2EEReplayDetected, err)
		}
		return fmt.Errorf("%s: %w", relay.CodeE2EEDecryptFailed, err)
	}
	if tunnelFrame.Type == e2ee.TunnelFrameStreamOpen && tunnelFrame.StreamType == e2ee.TunnelStreamFileDownload {
		go c.serveEncryptedFileDownload(tunnelFrame)
		return errRelayTunnelFrameConsumed
	}
	if c.routeFileDownloadControl(tunnelFrame) {
		return errRelayTunnelFrameConsumed
	}
	if tunnelFrame.StreamID != 0 {
		_ = c.writeEncryptedFileDownloadError(tunnelFrame.StreamID, relay.CodeStreamCancelled, "unsupported encrypted tunnel frame")
		return errRelayTunnelFrameConsumed
	}
	return fmt.Errorf("%s: unsupported encrypted tunnel frame", relay.CodeProtocolError)
}

type fileDownloadStream struct {
	controlCh chan e2ee.TunnelFrame
	done      chan struct{}
}

const fileDownloadControlQueueSize = 8

func (c *gatewayConn) registerFileDownload(streamID uint64) (*fileDownloadStream, bool) {
	c.downloadsMu.Lock()
	defer c.downloadsMu.Unlock()

	if c.downloads[streamID] != nil {
		return nil, false
	}
	stream := &fileDownloadStream{
		controlCh: make(chan e2ee.TunnelFrame, fileDownloadControlQueueSize),
		done:      make(chan struct{}),
	}
	c.downloads[streamID] = stream
	return stream, true
}

func (c *gatewayConn) unregisterFileDownload(streamID uint64, stream *fileDownloadStream) {
	c.downloadsMu.Lock()
	defer c.downloadsMu.Unlock()

	if c.downloads[streamID] == stream {
		delete(c.downloads, streamID)
		close(stream.done)
	}
}

func (c *gatewayConn) routeFileDownloadControl(frame e2ee.TunnelFrame) bool {
	switch frame.Type {
	case e2ee.TunnelFrameStreamAck, e2ee.TunnelFrameStreamReset:
	default:
		return false
	}
	c.downloadsMu.Lock()
	stream := c.downloads[frame.StreamID]
	c.downloadsMu.Unlock()
	if stream == nil {
		return true
	}
	select {
	case stream.controlCh <- frame:
	default:
		_ = c.writeEncryptedFileDownloadError(frame.StreamID, relay.CodeStreamWindowExceeded, "file download control queue is full")
	}
	return true
}

func (c *gatewayConn) serveEncryptedFileDownload(openFrame e2ee.TunnelFrame) {
	stream, ok := c.registerFileDownload(openFrame.StreamID)
	if !ok {
		_ = c.writeEncryptedFileDownloadError(openFrame.StreamID, relay.CodeStreamWindowExceeded, "file download stream already exists")
		return
	}
	defer c.unregisterFileDownload(openFrame.StreamID, stream)

	if err := c.sendEncryptedFile(openFrame, stream); err != nil {
		code := relay.CodeDownloadFailed
		if strings.Contains(err.Error(), relay.CodeDownloadDenied) || strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "directory") {
			code = relay.CodeDownloadDenied
		}
		_ = c.writeEncryptedFileDownloadError(openFrame.StreamID, code, err.Error())
	}
}

func (c *gatewayConn) sendEncryptedFile(openFrame e2ee.TunnelFrame, stream *fileDownloadStream) error {
	path, file, info, err := openDownloadTarget(openFrame.Metadata["path"], c.downloadRoots)
	if err != nil {
		return err
	}
	defer file.Close()

	codec := c.e2eeStream()
	if codec == nil {
		return fmt.Errorf("%s: relay e2ee stream is not ready", relay.CodeE2EEHandshakeFailed)
	}
	size := info.Size()
	metadata := e2ee.FileDownloadMetadata{
		Path: path, FileName: filepath.Base(path), ContentType: relayDownloadContentType(path), Size: &size,
	}
	responseOpen, err := e2ee.NewFileDownloadOpenFrame(openFrame.StreamID, metadata, openFrame.Window)
	if err != nil {
		return err
	}
	if err := c.writeEncryptedTunnelFrame(codec, responseOpen); err != nil {
		return err
	}

	chunker, err := e2ee.NewFileDownloadChunker(file, e2ee.FileDownloadDefaultChunkSize)
	if err != nil {
		return err
	}
	window, err := e2ee.NewFileDownloadSendWindow(openFrame.Window)
	if err != nil {
		return err
	}
	for {
		chunk, err := chunker.Next()
		if err == io.EOF {
			return c.writeEncryptedFileDownloadClose(codec, openFrame.StreamID)
		}
		if err != nil {
			return fmt.Errorf("%s: read file chunk: %w", relay.CodeDownloadFailed, err)
		}
		if err := c.writeEncryptedFileDownloadChunk(codec, stream, window, openFrame.StreamID, chunk); err != nil {
			return err
		}
	}
}

func openDownloadTarget(rawPath string, rawRoots []string) (string, *os.File, os.FileInfo, error) {
	target := strings.TrimSpace(rawPath)
	if target == "" {
		return "", nil, nil, fmt.Errorf("%s: path is required", relay.CodeDownloadDenied)
	}
	roots, err := validateDownloadRoots(rawRoots)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%s: invalid download root: %w", relay.CodeDownloadDenied, err)
	}
	if len(roots) == 0 {
		return "", nil, nil, fmt.Errorf("%s: download root is not configured", relay.CodeDownloadDenied)
	}
	absPath, err := resolveDownloadPath(target)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%s: invalid path: %w", relay.CodeDownloadDenied, err)
	}
	if !downloadPathAllowed(absPath, roots) {
		return "", nil, nil, fmt.Errorf("%s: path is outside allowed download roots", relay.CodeDownloadDenied)
	}
	info, err := os.Stat(absPath)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%s: file not found", relay.CodeDownloadDenied)
	}
	if info.IsDir() {
		return "", nil, nil, fmt.Errorf("%s: path is a directory", relay.CodeDownloadDenied)
	}
	file, err := os.Open(absPath)
	if err != nil {
		return "", nil, nil, fmt.Errorf("%s: open file: %w", relay.CodeDownloadFailed, err)
	}
	return absPath, file, info, nil
}

func validateDownloadRoots(rawRoots []string) ([]string, error) {
	roots := make([]string, 0, len(rawRoots))
	seen := map[string]struct{}{}
	for _, raw := range rawRoots {
		root, err := resolveDownloadRoot(raw)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[root]; ok {
			continue
		}
		seen[root] = struct{}{}
		roots = append(roots, root)
	}
	return roots, nil
}

func resolveDownloadRoot(raw string) (string, error) {
	root, err := resolveDownloadPath(raw)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(root)
	if err != nil {
		return "", err
	}
	if !info.IsDir() {
		return "", fmt.Errorf("download root is not a directory: %s", root)
	}
	return root, nil
}

func resolveDownloadPath(raw string) (string, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return "", fmt.Errorf("path is required")
	}
	absPath, err := filepath.Abs(filepath.Clean(target))
	if err != nil {
		return "", err
	}
	evaluated, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return absPath, nil
		}
		return "", err
	}
	return evaluated, nil
}

func downloadPathAllowed(path string, roots []string) bool {
	for _, root := range roots {
		if path == root {
			return true
		}
		rel, err := filepath.Rel(root, path)
		if err == nil && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && rel != ".." {
			return true
		}
	}
	return false
}

func relayDownloadContentType(path string) string {
	contentType := mime.TypeByExtension(filepath.Ext(path))
	if contentType != "" {
		return contentType
	}
	return "application/octet-stream"
}

func (c *gatewayConn) writeEncryptedFileDownloadChunk(codec *e2ee.MobileVCStreamCodec, stream *fileDownloadStream, window *e2ee.FileDownloadSendWindow, streamID uint64, chunk []byte) error {
	seq, err := c.nextTunnelSeq(streamID)
	if err != nil {
		return err
	}
	frame, err := e2ee.NewFileDownloadDataFrame(streamID, seq, chunk, e2ee.FileDownloadDefaultChunkSize)
	if err != nil {
		return err
	}
	for {
		err = window.ObserveSend(frame)
		if err == nil {
			break
		}
		if !strings.Contains(err.Error(), e2ee.FileDownloadErrorWindowExceeded) {
			return err
		}
		if err := c.waitFileDownloadAck(stream, window); err != nil {
			return err
		}
	}
	return c.writeEncryptedTunnelFrame(codec, frame)
}

func (c *gatewayConn) waitFileDownloadAck(stream *fileDownloadStream, window *e2ee.FileDownloadSendWindow) error {
	select {
	case frame := <-stream.controlCh:
		if frame.Type == e2ee.TunnelFrameStreamReset {
			return fmt.Errorf("%s: file download cancelled", relay.CodeStreamCancelled)
		}
		return window.ObserveAck(frame)
	case <-stream.done:
		return fmt.Errorf("%s: file download closed", relay.CodeStreamCancelled)
	case <-c.closeCh:
		return c.readError()
	}
}

func (c *gatewayConn) writeEncryptedFileDownloadClose(codec *e2ee.MobileVCStreamCodec, streamID uint64) error {
	seq, err := c.nextTunnelSeq(streamID)
	if err != nil {
		return err
	}
	frame, err := e2ee.NewFileDownloadCloseFrame(streamID, seq)
	if err != nil {
		return err
	}
	return c.writeEncryptedTunnelFrame(codec, frame)
}

func (c *gatewayConn) writeEncryptedFileDownloadError(streamID uint64, code string, message string) error {
	codec := c.e2eeStream()
	if codec == nil {
		return fmt.Errorf("%s: relay e2ee stream is not ready", relay.CodeE2EEHandshakeFailed)
	}
	frame, err := e2ee.NewFileDownloadErrorFrame(streamID, code, map[string]string{"message": message})
	if err != nil {
		return err
	}
	return c.writeEncryptedTunnelFrame(codec, frame)
}

func (c *gatewayConn) nextTunnelSeq(streamID uint64) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.tunnelSend == nil {
		c.tunnelSend = e2ee.NewTunnelCounterState()
	}
	return c.tunnelSend.NextSeq(streamID)
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
	if c.readErr != nil {
		return
	}
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

func (c *gatewayConn) closeReason() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.readErr
}

func (c *gatewayConn) setCloseReason(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.readErr == nil {
		c.readErr = err
	}
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
	if c.requiresE2EE() {
		if err := c.waitE2EEReady(); err != nil {
			return relay.ForwardEnvelope{}, err
		}
		return c.encryptedForwardEnvelope(payload)
	}
	clientID := c.currentClientID()
	return relay.ForwardEnvelope{
		Type:            relay.TypeRelayForward,
		Version:         relay.Version,
		SessionID:       c.sessionID,
		ClientID:        clientID,
		Direction:       relay.DirectionAgentToClient,
		MessageID:       "msg_" + uuid.NewString(),
		ContentType:     relay.ContentTypeMobileVC,
		Encryption:      relay.EncryptionNone,
		PayloadEncoding: relay.PayloadBase64URL,
		Payload:         base64.RawURLEncoding.EncodeToString(payload),
	}, nil
}

func (c *gatewayConn) encryptedForwardEnvelope(payload []byte) (relay.ForwardEnvelope, error) {
	codec := c.e2eeStream()
	if codec == nil {
		return relay.ForwardEnvelope{}, fmt.Errorf("%s: relay e2ee stream is not ready", relay.CodeE2EEHandshakeFailed)
	}
	frame, err := codec.Encode("msg_"+uuid.NewString(), payload)
	if err != nil {
		return relay.ForwardEnvelope{}, fmt.Errorf("%s: %w", relay.CodeE2EEDecryptFailed, err)
	}
	return relay.ForwardEnvelope(frame), nil
}

func (c *gatewayConn) writeEncryptedTunnelFrame(codec *e2ee.MobileVCStreamCodec, frame e2ee.TunnelFrame) error {
	env, err := codec.EncodeTunnelFrame("msg_"+uuid.NewString(), frame)
	if err != nil {
		return err
	}
	return c.writeControl(relay.ForwardEnvelope(env))
}

func (c *gatewayConn) e2eeStream() *e2ee.MobileVCStreamCodec {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stream
}

func (c *gatewayConn) requiresE2EE() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.e2ee != nil
}

func (c *gatewayConn) waitE2EEReady() error {
	if c.e2eeReadyCh == nil {
		return nil
	}
	select {
	case <-c.e2eeReadyCh:
		return nil
	case <-c.readDone:
		return c.readError()
	case <-c.closeCh:
		return c.readError()
	}
}

func (c *gatewayConn) currentClientID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientID
}

func (c *gatewayConn) Close() error {
	c.closeOnce.Do(func() { close(c.closeCh) })
	return c.conn.Close()
}

func (c *gatewayConn) RotateRelaySession() error {
	c.setCloseReason(errRelaySessionRotated)
	return c.Close()
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

func (c *gatewayConn) RelayE2EEInfo() gateway.RelayE2EEInfo {
	c.mu.Lock()
	defer c.mu.Unlock()
	deviceID := c.deviceID
	if c.e2ee != nil && c.streamHS != "" {
		if completedDeviceID := c.e2ee.completedDeviceID(c.streamHS); completedDeviceID != "" {
			deviceID = completedDeviceID
		}
	}
	return gateway.RelayE2EEInfo{
		Enabled:     c.stream != nil,
		SessionID:   c.sessionID,
		ClientID:    c.clientID,
		HandshakeID: c.streamHS,
		DeviceID:    deviceID,
	}
}

func (c *gatewayConn) SetRelayE2EEDeviceID(deviceID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deviceID = strings.TrimSpace(deviceID)
}
