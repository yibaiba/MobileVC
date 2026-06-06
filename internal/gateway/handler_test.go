package gateway

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"mobilevc/internal/data"
	"mobilevc/internal/data/claudesync"
	"mobilevc/internal/data/codexsync"
	"mobilevc/internal/engine"
	"mobilevc/internal/protocol"
	"mobilevc/internal/push"
	"mobilevc/internal/session"

	_ "modernc.org/sqlite"
)

type stubRunner struct {
	mu                         sync.Mutex
	events                     []any
	writeCh                    chan []byte
	writeErr                   error
	holdOpen                   bool
	interactive                bool
	started                    chan struct{}
	sink                       engine.EventSink
	lastPermissionMode         string
	permissionModes            []string
	onStart                    func()
	hasPendingPermission       bool
	permissionResponseErr      error
	permissionResponseWriteCh  chan string
	claudeSessionID            string
	processRef                 engine.ProcessRef
	lastReq                    engine.ExecRequest
	currentPermissionRequestID string
	closedCh                   chan struct{}
	contextUsage               protocol.ContextWindowUsage
	contextUsageOK             bool
	contextUsageErr            error
	compactStarted             chan struct{}
	compactRelease             chan struct{}
	compactErr                 error
}

type failingWriteClientConn struct {
	writeErr  error
	closedCh  chan struct{}
	closeOnce sync.Once
}

func newFailingWriteClientConn(writeErr error) *failingWriteClientConn {
	return &failingWriteClientConn{
		writeErr: writeErr,
		closedCh: make(chan struct{}),
	}
}

func (c *failingWriteClientConn) ReadJSON(any) error {
	<-c.closedCh
	return errors.New("client closed")
}

func (c *failingWriteClientConn) WriteJSON(any) error {
	return c.writeErr
}

func (c *failingWriteClientConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closedCh)
	})
	return nil
}

func (c *failingWriteClientConn) RemoteAddr() string {
	return "test-failing-write"
}

func (c *failingWriteClientConn) Origin() string {
	return "test"
}

func (c *failingWriteClientConn) closed() bool {
	select {
	case <-c.closedCh:
		return true
	default:
		return false
	}
}

func newStubRunner(events ...any) *stubRunner {
	return &stubRunner{
		events:                    events,
		writeCh:                   make(chan []byte, 8),
		started:                   make(chan struct{}),
		permissionResponseWriteCh: make(chan string, 8),
		closedCh:                  make(chan struct{}, 1),
	}
}

func newBlockingCompactStubRunner() *stubRunner {
	stub := newInteractiveHoldingStubRunner()
	stub.compactStarted = make(chan struct{})
	stub.compactRelease = make(chan struct{})
	return stub
}

func newHoldingStubRunner(events ...any) *stubRunner {
	stub := newStubRunner(events...)
	stub.holdOpen = true
	stub.interactive = true
	stub.hasPendingPermission = true
	return stub
}

func newInteractiveHoldingStubRunner(events ...any) *stubRunner {
	stub := newHoldingStubRunner(events...)
	stub.hasPendingPermission = false
	return stub
}

func newNonInteractiveHoldingStubRunner(events ...any) *stubRunner {
	stub := newStubRunner(events...)
	stub.holdOpen = true
	stub.interactive = false
	stub.hasPendingPermission = true
	return stub
}

func TestGatewayWriterPrioritizesClientActionAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writeCh := make(chan any, gatewayWriteQueueSize)
	priorityWriteCh := make(chan any, gatewayPriorityWriteQueueSize)
	normalEvent := protocol.NewLogEvent("session-1", "queued output", "stdout")
	ackEvent := protocol.NewClientActionAckEvent("session-1", "ai_turn", "action-1", "accepted", false)

	writeCh <- normalEvent
	enqueueGatewayWrite(ctx, writeCh, priorityWriteCh, ackEvent)

	next, ok := nextGatewayWriterEvent(ctx, writeCh, priorityWriteCh)
	if !ok {
		t.Fatal("writer queue returned closed")
	}
	if _, ok := next.(protocol.ClientActionAckEvent); !ok {
		t.Fatalf("expected client action ack to be written first, got %#v", next)
	}
}

func TestGatewayWriterPrioritizesPong(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writeCh := make(chan any, gatewayWriteQueueSize)
	priorityWriteCh := make(chan any, gatewayPriorityWriteQueueSize)
	normalEvent := protocol.NewLogEvent("session-1", "queued output", "stdout")
	pongEvent := map[string]any{
		"type":      "pong",
		"sessionId": "session-1",
		"pingId":    "ping-1",
	}

	writeCh <- normalEvent
	enqueueGatewayWrite(ctx, writeCh, priorityWriteCh, pongEvent)

	next, ok := nextGatewayWriterEvent(ctx, writeCh, priorityWriteCh)
	if !ok {
		t.Fatal("writer queue returned closed")
	}
	pong, ok := next.(map[string]any)
	if !ok || pong["type"] != "pong" || pong["pingId"] != "ping-1" {
		t.Fatalf("expected pong to be written first, got %#v", next)
	}
}

func TestGatewayWriterPrioritizesHeartbeatSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writeCh := make(chan any, gatewayWriteQueueSize)
	priorityWriteCh := make(chan any, gatewayPriorityWriteQueueSize)
	normalEvent := protocol.NewLogEvent("session-1", "queued output", "stdout")
	snapshot := protocol.NewTaskSnapshotEvent(
		"session-1",
		"IDLE",
		"Task idle (heartbeat)",
		false,
		false,
		"",
		"",
		"",
		0,
		time.Time{},
		protocol.RuntimeMeta{},
	)

	writeCh <- normalEvent
	enqueueGatewayWrite(ctx, writeCh, priorityWriteCh, snapshot)

	next, ok := nextGatewayWriterEvent(ctx, writeCh, priorityWriteCh)
	if !ok {
		t.Fatal("writer queue returned closed")
	}
	got, ok := next.(protocol.TaskSnapshotEvent)
	if !ok || got.Message != "Task idle (heartbeat)" {
		t.Fatalf("expected heartbeat snapshot to be written first, got %#v", next)
	}
}

func TestServeClientConnClosesWhenWriterFails(t *testing.T) {
	h := NewHandler("token", nil)
	client := newFailingWriteClientConn(errors.New("write failed"))
	done := make(chan struct{})

	go func() {
		h.ServeClientConn(context.Background(), client)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("ServeClientConn did not return after writer failure")
	}
	if !client.closed() {
		t.Fatal("writer failure did not close client connection")
	}
}

func (s *stubRunner) Run(ctx context.Context, req engine.ExecRequest, sink engine.EventSink) error {
	s.mu.Lock()
	s.lastReq = req
	s.sink = sink
	events := append([]any(nil), s.events...)
	s.mu.Unlock()
	select {
	case <-s.started:
	default:
		close(s.started)
	}
	if s.onStart != nil {
		s.onStart()
	}
	for _, event := range events {
		sink(event)
	}
	if !s.holdOpen {
		return nil
	}
	<-ctx.Done()
	return nil
}

func (s *stubRunner) Write(ctx context.Context, data []byte) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.writeCh <- append([]byte(nil), data...):
		return nil
	}
}

func (s *stubRunner) Close() error {
	select {
	case s.closedCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *stubRunner) SetPermissionMode(mode string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastPermissionMode = mode
	s.permissionModes = append(s.permissionModes, mode)
}

func (s *stubRunner) CanAcceptInteractiveInput() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.interactive
}

func (s *stubRunner) HasPendingPermissionRequest() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hasPendingPermission
}

func (s *stubRunner) CurrentPermissionRequestID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.TrimSpace(s.currentPermissionRequestID)
}

func (s *stubRunner) WritePermissionResponse(ctx context.Context, decision string) error {
	if s.permissionResponseErr != nil {
		return s.permissionResponseErr
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.permissionResponseWriteCh <- decision:
		s.mu.Lock()
		s.hasPendingPermission = false
		s.currentPermissionRequestID = ""
		s.mu.Unlock()
		return nil
	}
}

func (s *stubRunner) WaitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start")
	}
}

func (s *stubRunner) Emit(event any) {
	s.mu.Lock()
	s.events = append(s.events, event)
	sink := s.sink
	s.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (s *stubRunner) WaitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closedCh:
	case <-time.After(5 * time.Second):
		t.Fatal("runner was not closed")
	}
}

func (s *stubRunner) ClaudeSessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.claudeSessionID
}

func (s *stubRunner) ProcessRef() engine.ProcessRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.processRef
}

func (s *stubRunner) ContextWindowUsage(ctx context.Context) (protocol.ContextWindowUsage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.contextUsageErr != nil {
		return protocol.ContextWindowUsage{}, false, s.contextUsageErr
	}
	return s.contextUsage, s.contextUsageOK, nil
}

func (s *stubRunner) Compact(ctx context.Context) error {
	s.mu.Lock()
	started := s.compactStarted
	release := s.compactRelease
	err := s.compactErr
	s.mu.Unlock()
	if started == nil || release == nil {
		return engine.ErrInputNotSupported
	}
	select {
	case <-started:
	default:
		close(started)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-release:
		return err
	}
}

func (s *stubRunner) WaitCompactStarted(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	started := s.compactStarted
	s.mu.Unlock()
	if started == nil {
		t.Fatal("runner is not configured to block compact")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("compact did not start")
	}
}

func (s *stubRunner) ReleaseCompact() {
	s.mu.Lock()
	release := s.compactRelease
	s.mu.Unlock()
	if release == nil {
		return
	}
	select {
	case <-release:
	default:
		close(release)
	}
}

type writeOnlyStubRunner struct {
	base *stubRunner
}

func (s *writeOnlyStubRunner) Run(ctx context.Context, req engine.ExecRequest, sink engine.EventSink) error {
	return s.base.Run(ctx, req, sink)
}

func (s *writeOnlyStubRunner) Write(ctx context.Context, data []byte) error {
	return s.base.Write(ctx, data)
}

func (s *writeOnlyStubRunner) Close() error {
	return s.base.Close()
}

func (s *writeOnlyStubRunner) SetPermissionMode(mode string) {
	s.base.SetPermissionMode(mode)
}

func (s *writeOnlyStubRunner) CanAcceptInteractiveInput() bool {
	return s.base.CanAcceptInteractiveInput()
}

func (s *writeOnlyStubRunner) WaitStarted(t *testing.T) {
	s.base.WaitStarted(t)
}

func newTestHandler() *Handler {
	return NewHandler("test", nil)
}

type localTestServer struct {
	URL      string
	server   *http.Server
	listener net.Listener
	done     chan struct{}
	wg       sync.WaitGroup
}

type countingStore struct {
	inner      data.Store
	mu         sync.Mutex
	getCalls   int
	listCalls  int
	upsertErr  error
	upsertSeen []string
}

type blockingSkillSyncStore struct {
	data.Store
	mu                 sync.Mutex
	syncingSaveStarted chan struct{}
	syncingSaveRelease chan struct{}
	syncingSaveCount   int
}

type blockingFinalSkillSyncStore struct {
	data.Store
	mu               sync.Mutex
	finalSaveStarted chan struct{}
	finalSaveRelease chan struct{}
	finalSaveCount   int
}

type blockingFinalMemorySyncStore struct {
	data.Store
	mu               sync.Mutex
	finalSaveStarted chan struct{}
	finalSaveRelease chan struct{}
	finalSaveCount   int
}

type blockingProjectionSaveStore struct {
	data.Store
	mu          sync.Mutex
	saveStarted chan struct{}
	saveRelease chan struct{}
	saveDone    chan struct{}
	saveCount   int
}

type failOnceProjectionSaveStore struct {
	data.Store
	mu        sync.Mutex
	saveCount int
}

type projectionSaveOnlyStore struct {
	data.Store
}

func newBlockingSkillSyncStore(store data.Store) *blockingSkillSyncStore {
	return &blockingSkillSyncStore{
		Store:              store,
		syncingSaveStarted: make(chan struct{}),
		syncingSaveRelease: make(chan struct{}),
	}
}

func newBlockingProjectionSaveStore(store data.Store) *blockingProjectionSaveStore {
	return &blockingProjectionSaveStore{
		Store:       store,
		saveStarted: make(chan struct{}),
		saveRelease: make(chan struct{}),
		saveDone:    make(chan struct{}),
	}
}

func newFailOnceProjectionSaveStore(store data.Store) *failOnceProjectionSaveStore {
	return &failOnceProjectionSaveStore{Store: store}
}

func newBlockingFinalSkillSyncStore(store data.Store) *blockingFinalSkillSyncStore {
	return &blockingFinalSkillSyncStore{
		Store:            store,
		finalSaveStarted: make(chan struct{}),
		finalSaveRelease: make(chan struct{}),
	}
}

func newBlockingFinalMemorySyncStore(store data.Store) *blockingFinalMemorySyncStore {
	return &blockingFinalMemorySyncStore{
		Store:            store,
		finalSaveStarted: make(chan struct{}),
		finalSaveRelease: make(chan struct{}),
	}
}

func (s *blockingSkillSyncStore) SaveSkillCatalogSnapshot(ctx context.Context, snapshot data.SkillCatalogSnapshot) error {
	if snapshot.Meta.SyncState != data.CatalogSyncStateSyncing {
		return s.Store.SaveSkillCatalogSnapshot(ctx, snapshot)
	}
	s.mu.Lock()
	s.syncingSaveCount++
	count := s.syncingSaveCount
	s.mu.Unlock()
	if count == 1 {
		close(s.syncingSaveStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.syncingSaveRelease:
		}
	}
	return s.Store.SaveSkillCatalogSnapshot(ctx, snapshot)
}

func (s *blockingFinalSkillSyncStore) SaveSkillCatalogSnapshot(ctx context.Context, snapshot data.SkillCatalogSnapshot) error {
	if snapshot.Meta.SyncState != data.CatalogSyncStateSynced {
		return s.Store.SaveSkillCatalogSnapshot(ctx, snapshot)
	}
	s.mu.Lock()
	s.finalSaveCount++
	count := s.finalSaveCount
	s.mu.Unlock()
	if count == 1 {
		close(s.finalSaveStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.finalSaveRelease:
		}
	}
	return s.Store.SaveSkillCatalogSnapshot(ctx, snapshot)
}

func (s *blockingFinalMemorySyncStore) SaveMemoryCatalogSnapshot(ctx context.Context, snapshot data.MemoryCatalogSnapshot) error {
	if snapshot.Meta.SyncState != data.CatalogSyncStateSynced {
		return s.Store.SaveMemoryCatalogSnapshot(ctx, snapshot)
	}
	s.mu.Lock()
	s.finalSaveCount++
	count := s.finalSaveCount
	s.mu.Unlock()
	if count == 1 {
		close(s.finalSaveStarted)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.finalSaveRelease:
		}
	}
	return s.Store.SaveMemoryCatalogSnapshot(ctx, snapshot)
}

func (s *blockingProjectionSaveStore) SaveProjection(ctx context.Context, sessionID string, projection data.ProjectionSnapshot) (data.SessionSummary, error) {
	return s.SaveProjectionWithOptions(ctx, sessionID, projection)
}

func (s *blockingProjectionSaveStore) SaveProjectionWithOptions(ctx context.Context, sessionID string, projection data.ProjectionSnapshot, opts ...data.ProjectionSaveOption) (data.SessionSummary, error) {
	s.mu.Lock()
	s.saveCount++
	count := s.saveCount
	s.mu.Unlock()
	if count == 1 {
		close(s.saveStarted)
		defer close(s.saveDone)
		select {
		case <-ctx.Done():
			return data.SessionSummary{}, ctx.Err()
		case <-s.saveRelease:
		}
	}
	return saveProjectionWithOptions(s.Store, ctx, sessionID, projection, opts...)
}

func (s *failOnceProjectionSaveStore) SaveProjection(ctx context.Context, sessionID string, projection data.ProjectionSnapshot) (data.SessionSummary, error) {
	return s.SaveProjectionWithOptions(ctx, sessionID, projection)
}

func (s *failOnceProjectionSaveStore) SaveProjectionWithOptions(ctx context.Context, sessionID string, projection data.ProjectionSnapshot, opts ...data.ProjectionSaveOption) (data.SessionSummary, error) {
	s.mu.Lock()
	s.saveCount++
	count := s.saveCount
	s.mu.Unlock()
	if count == 1 {
		return data.SessionSummary{}, fmt.Errorf("injected projection save failure")
	}
	return saveProjectionWithOptions(s.Store, ctx, sessionID, projection, opts...)
}

func (s *projectionSaveOnlyStore) SaveProjection(ctx context.Context, sessionID string, projection data.ProjectionSnapshot) (data.SessionSummary, error) {
	return s.Store.SaveProjection(ctx, sessionID, projection)
}

func (s *blockingSkillSyncStore) WaitSyncingSaveStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.syncingSaveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("skill sync did not reach syncing save")
	}
}

func (s *blockingFinalSkillSyncStore) WaitFinalSaveStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.finalSaveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("skill sync did not reach final save")
	}
}

func (s *blockingFinalMemorySyncStore) WaitFinalSaveStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.finalSaveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("memory sync did not reach final save")
	}
}

func (s *blockingProjectionSaveStore) WaitSaveStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.saveStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("projection save did not start")
	}
}

func (s *blockingSkillSyncStore) ReleaseSyncingSave() {
	select {
	case <-s.syncingSaveRelease:
	default:
		close(s.syncingSaveRelease)
	}
}

func (s *blockingProjectionSaveStore) ReleaseSave() {
	select {
	case <-s.saveRelease:
	default:
		close(s.saveRelease)
	}
}

func (s *blockingProjectionSaveStore) WaitSaveDone(t *testing.T) {
	t.Helper()
	select {
	case <-s.saveDone:
	case <-time.After(5 * time.Second):
		t.Fatal("projection save did not finish")
	}
}

func (s *failOnceProjectionSaveStore) SaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.saveCount
}

func (s *blockingFinalSkillSyncStore) ReleaseFinalSave() {
	select {
	case <-s.finalSaveRelease:
	default:
		close(s.finalSaveRelease)
	}
}

func (s *blockingFinalMemorySyncStore) ReleaseFinalSave() {
	select {
	case <-s.finalSaveRelease:
	default:
		close(s.finalSaveRelease)
	}
}

func (s *blockingSkillSyncStore) SyncingSaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.syncingSaveCount
}

func (s *blockingFinalSkillSyncStore) FinalSaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalSaveCount
}

func (s *blockingFinalMemorySyncStore) FinalSaveCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.finalSaveCount
}

func (s *countingStore) CreateSession(ctx context.Context, title string) (data.SessionSummary, error) {
	return s.inner.CreateSession(ctx, title)
}

func (s *countingStore) UpsertSession(ctx context.Context, record data.SessionRecord) (data.SessionSummary, error) {
	s.mu.Lock()
	s.upsertSeen = append(s.upsertSeen, record.Summary.ID)
	err := s.upsertErr
	s.mu.Unlock()
	if err != nil {
		return data.SessionSummary{}, err
	}
	return s.inner.UpsertSession(ctx, record)
}

func (s *countingStore) SaveProjection(ctx context.Context, sessionID string, projection data.ProjectionSnapshot) (data.SessionSummary, error) {
	return s.inner.SaveProjection(ctx, sessionID, projection)
}

func (s *countingStore) SaveProjectionWithOptions(ctx context.Context, sessionID string, projection data.ProjectionSnapshot, opts ...data.ProjectionSaveOption) (data.SessionSummary, error) {
	return saveProjectionWithOptions(s.inner, ctx, sessionID, projection, opts...)
}

func (s *countingStore) GetSession(ctx context.Context, sessionID string) (data.SessionRecord, error) {
	s.mu.Lock()
	s.getCalls++
	s.mu.Unlock()
	return s.inner.GetSession(ctx, sessionID)
}

func (s *countingStore) ListSessions(ctx context.Context) ([]data.SessionSummary, error) {
	s.mu.Lock()
	s.listCalls++
	s.mu.Unlock()
	return s.inner.ListSessions(ctx)
}

func (s *countingStore) DeleteSession(ctx context.Context, sessionID string) error {
	return s.inner.DeleteSession(ctx, sessionID)
}

func (s *countingStore) MarkClientAction(ctx context.Context, sessionID string, record data.ClientActionRecord, ttl time.Duration, limit int) (bool, error) {
	return s.inner.MarkClientAction(ctx, sessionID, record, ttl, limit)
}

func (s *countingStore) SavePushToken(ctx context.Context, sessionID, token, platform string) error {
	return s.inner.SavePushToken(ctx, sessionID, token, platform)
}

func (s *countingStore) GetPushToken(ctx context.Context, sessionID string) (string, string, error) {
	return s.inner.GetPushToken(ctx, sessionID)
}

func (s *countingStore) ListSkillCatalog(ctx context.Context) ([]data.SkillDefinition, error) {
	return s.inner.ListSkillCatalog(ctx)
}

func (s *countingStore) SaveSkillCatalog(ctx context.Context, items []data.SkillDefinition) error {
	return s.inner.SaveSkillCatalog(ctx, items)
}

func (s *countingStore) GetSkillCatalogSnapshot(ctx context.Context) (data.SkillCatalogSnapshot, error) {
	return s.inner.GetSkillCatalogSnapshot(ctx)
}

func (s *countingStore) SaveSkillCatalogSnapshot(ctx context.Context, snapshot data.SkillCatalogSnapshot) error {
	return s.inner.SaveSkillCatalogSnapshot(ctx, snapshot)
}

func (s *countingStore) ListMemoryCatalog(ctx context.Context) ([]data.MemoryItem, error) {
	return s.inner.ListMemoryCatalog(ctx)
}

func (s *countingStore) SaveMemoryCatalog(ctx context.Context, items []data.MemoryItem) error {
	return s.inner.SaveMemoryCatalog(ctx, items)
}

func (s *countingStore) GetMemoryCatalogSnapshot(ctx context.Context) (data.MemoryCatalogSnapshot, error) {
	return s.inner.GetMemoryCatalogSnapshot(ctx)
}

func (s *countingStore) SaveMemoryCatalogSnapshot(ctx context.Context, snapshot data.MemoryCatalogSnapshot) error {
	return s.inner.SaveMemoryCatalogSnapshot(ctx, snapshot)
}

func (s *countingStore) GetPermissionRuleSnapshot(ctx context.Context) (data.PermissionRuleSnapshot, error) {
	return s.inner.GetPermissionRuleSnapshot(ctx)
}

func (s *countingStore) SavePermissionRuleSnapshot(ctx context.Context, snapshot data.PermissionRuleSnapshot) error {
	return s.inner.SavePermissionRuleSnapshot(ctx, snapshot)
}

func (s *countingStore) getCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.getCalls
}

func (s *countingStore) listCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listCalls
}

func (s *localTestServer) Close() {
	if s == nil {
		return
	}
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = s.server.Shutdown(ctx)
		cancel()
	}
	_ = s.listener.Close()
	handlerDone := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(handlerDone)
	}()
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
	}
	if s.done != nil {
		select {
		case <-s.done:
		case <-time.After(2 * time.Second):
		}
	}
}

func newLocalHTTPServer(t *testing.T, handler http.Handler) *localTestServer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen test server: %v", err)
	}
	done := make(chan struct{})
	testServer := &localTestServer{
		URL:      "http://" + listener.Addr().String(),
		listener: listener,
		done:     done,
	}
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		testServer.wg.Add(1)
		defer testServer.wg.Done()
		handler.ServeHTTP(w, r)
	})
	server := &http.Server{Handler: wrappedHandler}
	testServer.server = server
	go func() {
		defer close(done)
		_ = server.Serve(listener)
	}()
	t.Cleanup(testServer.Close)
	return testServer
}

func newTestConn(t *testing.T, h *Handler) *websocket.Conn {
	t.Helper()
	server := newLocalHTTPServer(t, h)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Close()
		if h != nil {
			h.Close()
		}
	})
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	return conn
}

func dialTestConn(t *testing.T, h *Handler, origin string) (*websocket.Conn, *http.Response) {
	t.Helper()
	server := newLocalHTTPServer(t, h)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	headers := http.Header{}
	if origin != "" {
		headers.Set("Origin", origin)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, headers)
	if err != nil {
		return nil, resp
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Close()
		if h != nil {
			h.Close()
		}
	})
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	return conn, resp
}

func TestHandlerAllowsBrowserOrigin(t *testing.T) {
	h := newTestHandler()

	conn, resp := dialTestConn(t, h, "https://example.test")
	if conn == nil {
		t.Fatalf("expected websocket connection, status=%s", responseStatus(resp))
	}
}

func responseStatus(resp *http.Response) string {
	if resp == nil {
		return "<nil>"
	}
	return resp.Status
}

func readEventMap(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	var event map[string]any
	if err := conn.ReadJSON(&event); err != nil {
		t.Fatalf("read event: %v", err)
	}
	return event
}

func readInitialEvents(t *testing.T, conn *websocket.Conn) (map[string]any, map[string]any) {
	t.Helper()
	first := readEventMap(t, conn)
	second := readEventMap(t, conn)
	return first, second
}

func requireEventType(t *testing.T, event map[string]any, want string) {
	t.Helper()
	if event["type"] != want {
		t.Fatalf("expected %s event, got %#v", want, event)
	}
}

func requireAgentState(t *testing.T, event map[string]any, wantState string, wantAwait bool) {
	t.Helper()
	requireEventType(t, event, protocol.EventTypeAgentState)
	if event["state"] != wantState {
		t.Fatalf("expected agent state %q, got %#v", wantState, event)
	}
	await, _ := event["awaitInput"].(bool)
	if await != wantAwait {
		t.Fatalf("expected awaitInput=%v, got %#v", wantAwait, event)
	}
}

func TestHandlerFileAccessReadsAbsolutePath(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "shared.txt")
	if err := os.WriteFile(filePath, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.FSReadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "fs_read"},
		Path:        filePath,
	}); err != nil {
		t.Fatal(err)
	}
	readEvent := readUntilType(t, conn, protocol.EventTypeFSReadResult)
	if readEvent["content"] != "ok" {
		t.Fatalf("content: got %#v", readEvent["content"])
	}
}

func TestHandlerFileAccessWritesTextFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "note.md")
	if err := os.WriteFile(filePath, []byte("# Old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.FSWriteRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "fs_write"},
		Path:        filePath,
		Content:     "# New\n\nBody",
	}); err != nil {
		t.Fatal(err)
	}
	writeEvent := readUntilType(t, conn, protocol.EventTypeFSWriteResult)
	if writeEvent["content"] != "# New\n\nBody" {
		t.Fatalf("content: got %#v", writeEvent["content"])
	}
	if writeEvent["isText"] != true {
		t.Fatalf("isText: got %#v", writeEvent["isText"])
	}
	raw, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "# New\n\nBody" {
		t.Fatalf("file content: got %#v", string(raw))
	}
}

func TestHandlerFileAccessRejectsOversizedTextFile(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "large.log")
	largeContent := bytes.Repeat([]byte("x"), maxInlineFileReadBytes+1)
	if err := os.WriteFile(filePath, largeContent, 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.FSReadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "fs_read"},
		Path:        filePath,
	}); err != nil {
		t.Fatal(err)
	}
	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if msg := fmt.Sprint(errorEvent["msg"]); !strings.Contains(msg, "file exceeds inline read limit") {
		t.Fatalf("message: got %#v", errorEvent["msg"])
	}
}

func TestHandlerFileAccessReadsImageAsBase64(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "screen.png")
	raw := []byte{0x89, 0x50, 0x4E, 0x47}
	if err := os.WriteFile(filePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.FSReadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "fs_read"},
		Path:        filePath,
	}); err != nil {
		t.Fatal(err)
	}
	readEvent := readUntilType(t, conn, protocol.EventTypeFSReadResult)
	if readEvent["content"] != base64.StdEncoding.EncodeToString(raw) {
		t.Fatalf("content: got %#v", readEvent["content"])
	}
	if readEvent["isText"] != false {
		t.Fatalf("isText: got %#v", readEvent["isText"])
	}
}

func TestHandlerFileAccessRejectsOversizedImage(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "large.png")
	largeContent := append([]byte{0x89, 0x50, 0x4E, 0x47}, bytes.Repeat([]byte("x"), maxInlineFileReadBytes+1)...)
	if err := os.WriteFile(filePath, largeContent, 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.FSReadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "fs_read"},
		Path:        filePath,
	}); err != nil {
		t.Fatal(err)
	}
	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if msg := fmt.Sprint(errorEvent["msg"]); !strings.Contains(msg, "file exceeds inline read limit") {
		t.Fatalf("message: got %#v", errorEvent["msg"])
	}
}

func TestHandlerMediaPreviewReadsImageAsBase64(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "preview.png")
	raw := []byte{0x89, 0x50, 0x4E, 0x47}
	if err := os.WriteFile(filePath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.MediaPreviewRequestEvent{
		ClientEvent:  protocol.ClientEvent{Action: "media_preview"},
		AttachmentID: "att-1",
		Path:         filePath,
	}); err != nil {
		t.Fatal(err)
	}
	result := readUntilType(t, conn, protocol.EventTypeMediaPreviewResult)
	if result["attachmentId"] != "att-1" {
		t.Fatalf("attachmentId: got %#v", result["attachmentId"])
	}
	if result["status"] != "ok" {
		t.Fatalf("status: got %#v", result["status"])
	}
	if result["content"] != base64.StdEncoding.EncodeToString(raw) {
		t.Fatalf("content: got %#v", result["content"])
	}
}

func TestHandlerMediaPreviewRejectsOversizedImage(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "large.png")
	largeContent := append([]byte{0x89, 0x50, 0x4E, 0x47}, bytes.Repeat([]byte("x"), maxImageAttachmentBytes+1)...)
	if err := os.WriteFile(filePath, largeContent, 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.MediaPreviewRequestEvent{
		ClientEvent:  protocol.ClientEvent{Action: "media_preview"},
		AttachmentID: "att-large",
		Path:         filePath,
	}); err != nil {
		t.Fatal(err)
	}
	result := readUntilType(t, conn, protocol.EventTypeMediaPreviewResult)
	if result["status"] != "unsupported" {
		t.Fatalf("status: got %#v", result["status"])
	}
	if msg := fmt.Sprint(result["message"]); !strings.Contains(msg, "image exceeds") {
		t.Fatalf("message: got %#v", result["message"])
	}
	if content := fmt.Sprint(result["content"]); content != "" && content != "<nil>" {
		t.Fatalf("content should be empty for oversized image, got %#v", result["content"])
	}
}

func TestHandlerMediaPreviewRejectsNonImage(t *testing.T) {
	dir := t.TempDir()
	filePath := filepath.Join(dir, "notes.txt")
	if err := os.WriteFile(filePath, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	h := newTestHandler()
	conn := newTestConn(t, h)

	if err := conn.WriteJSON(protocol.MediaPreviewRequestEvent{
		ClientEvent:  protocol.ClientEvent{Action: "media_preview"},
		AttachmentID: "att-text",
		Path:         filePath,
	}); err != nil {
		t.Fatal(err)
	}
	result := readUntilType(t, conn, protocol.EventTypeMediaPreviewResult)
	if result["status"] != "unsupported" {
		t.Fatalf("status: got %#v", result["status"])
	}
	if result["message"] != "file is not an image" {
		t.Fatalf("message: got %#v", result["message"])
	}
}

type switchableStubRunner struct {
	mu       sync.Mutex
	writeCh  chan []byte
	sink     engine.EventSink
	req      engine.ExecRequest
	started  chan struct{}
	closed   chan struct{}
	closeErr error
}

func newSwitchableStubRunner() *switchableStubRunner {
	return &switchableStubRunner{
		writeCh: make(chan []byte, 8),
		started: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (s *switchableStubRunner) Run(ctx context.Context, req engine.ExecRequest, sink engine.EventSink) error {
	s.mu.Lock()
	s.req = req
	s.sink = sink
	s.mu.Unlock()
	close(s.started)
	<-ctx.Done()
	return s.closeErr
}

func (s *switchableStubRunner) Write(ctx context.Context, data []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.writeCh <- append([]byte(nil), data...):
		return nil
	}
}

func (s *switchableStubRunner) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

func (s *switchableStubRunner) CanAcceptInteractiveInput() bool {
	return true
}

func (s *switchableStubRunner) Emit(event any) {
	s.mu.Lock()
	sink := s.sink
	s.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (s *switchableStubRunner) WaitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-s.started:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not start")
	}
}

func (s *switchableStubRunner) WaitClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("runner was not closed")
	}
}

func (s *switchableStubRunner) AssertNotClosed(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-s.closed:
		t.Fatal("runner was closed")
	case <-time.After(timeout):
	}
}

func readInitialSessionID(t *testing.T, conn *websocket.Conn) string {
	t.Helper()
	first, second := readInitialEvents(t, conn)
	requireEventType(t, first, protocol.EventTypeSessionState)
	requireAgentState(t, second, "IDLE", false)
	sessionID, _ := first["sessionId"].(string)
	if sessionID == "" {
		t.Fatalf("expected initial session id, got %#v", first)
	}
	return sessionID
}

func readUntilType(t *testing.T, conn *websocket.Conn, want string) map[string]any {
	t.Helper()
	for i := 0; i < 20; i++ {
		event := readEventMap(t, conn)
		if event["type"] == want {
			return event
		}
	}
	t.Fatalf("did not receive %s event", want)
	return nil
}

func readUntilClientActionAck(t *testing.T, conn *websocket.Conn, action, clientActionID string) map[string]any {
	t.Helper()
	for i := 0; i < 40; i++ {
		event := readEventMap(t, conn)
		if event["type"] != protocol.EventTypeClientActionAck {
			continue
		}
		if event["action"] == action && event["clientActionId"] == clientActionID {
			return event
		}
	}
	t.Fatalf("did not receive client_action_ack action=%s clientActionID=%s", action, clientActionID)
	return nil
}

func readUntilPongID(t *testing.T, conn *websocket.Conn, pingID string) map[string]any {
	t.Helper()
	for i := 0; i < 40; i++ {
		event := readEventMap(t, conn)
		if event["type"] == "pong" && event["pingId"] == pingID {
			return event
		}
	}
	t.Fatalf("did not receive pong pingID=%s", pingID)
	return nil
}

func waitRuntimeSessionDetached(t *testing.T, h *Handler, sessionID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if h == nil || h.runtimeSessions == nil || !h.runtimeSessions.HasActiveConnection(sessionID) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("runtime session %q still has an active connection", sessionID)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func readUntilReviewFileStatus(t *testing.T, conn *websocket.Conn, fileID, status string) map[string]any {
	t.Helper()
	for i := 0; i < 20; i++ {
		event := readUntilType(t, conn, protocol.EventTypeReviewState)
		file := reviewStateFile(event, fileID)
		if file != nil && file["reviewStatus"] == status {
			return event
		}
	}
	t.Fatalf("did not receive review_state for file %q with status %q", fileID, status)
	return nil
}

func reviewStateFile(event map[string]any, fileID string) map[string]any {
	groups, ok := event["groups"].([]any)
	if !ok {
		return nil
	}
	for _, group := range groups {
		groupMap, ok := group.(map[string]any)
		if !ok {
			continue
		}
		files, ok := groupMap["files"].([]any)
		if !ok {
			continue
		}
		for _, file := range files {
			fileMap, ok := file.(map[string]any)
			if ok && (fileMap["id"] == fileID || fileMap["path"] == fileID) {
				return fileMap
			}
		}
	}
	return nil
}

func waitForPersistedReviewFile(t *testing.T, h *Handler, sessionID, fileID string) data.ProjectionSnapshot {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last data.ProjectionSnapshot
	for time.Now().Before(deadline) {
		record, err := h.SessionStore.GetSession(context.Background(), sessionID)
		if err != nil {
			t.Fatalf("get session: %v", err)
		}
		last = record.Projection
		for _, diff := range record.Projection.Diffs {
			if diff.ContextID == fileID || diff.Path == fileID {
				return record.Projection
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("did not persist review file %q, last projection: %#v", fileID, last)
	return last
}

func waitForPersistedPermissionMode(t *testing.T, store data.Store, sessionID, mode string) data.SessionRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last data.SessionRecord
	for time.Now().Before(deadline) {
		record, err := store.GetSession(context.Background(), sessionID)
		if err != nil {
			t.Fatalf("get session projection: %v", err)
		}
		last = record
		if record.Projection.Runtime.PermissionMode == mode {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected persisted permission mode %q, got %#v", mode, last.Projection.Runtime)
	return last
}

func waitForPersistedSessionText(t *testing.T, store data.Store, sessionID, want string) data.SessionRecord {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last data.SessionRecord
	for time.Now().Before(deadline) {
		record, err := store.GetSession(context.Background(), sessionID)
		if err != nil {
			t.Fatalf("get session projection: %v", err)
		}
		last = record
		if containsText(sessionLogTexts(record), want) {
			return record
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected persisted text %q, got %#v", want, sessionLogTexts(last))
	return last
}

func assertNoEventType(t *testing.T, conn *websocket.Conn, want string, timeout time.Duration) {
	t.Helper()
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	defer func() {
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	}()
	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				return
			}
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == want {
			t.Fatalf("unexpected %s event: %#v", want, event)
		}
	}
}

func closeConnAndCleanupRuntime(t *testing.T, conn *websocket.Conn, h *Handler, runners ...*stubRunner) {
	t.Helper()
	if conn != nil {
		closeTestConnGracefully(t, conn)
	}
	if h != nil && h.runtimeSessions != nil {
		h.runtimeSessions.CleanupAll()
	}
	for _, runner := range runners {
		if runner != nil {
			runner.WaitClosed(t)
		}
	}
}

func closeTestConnGracefully(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	if err := conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
		deadline,
	); err != nil {
		t.Fatalf("write websocket close frame: %v", err)
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatalf("set websocket close read deadline: %v", err)
	}
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

func readUntilSessionHistory(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	return readUntilType(t, conn, protocol.EventTypeSessionHistory)
}

func readUntilSessionHistoryPage(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	return readUntilType(t, conn, protocol.EventTypeSessionHistoryPage)
}

func readUntilSessionCreated(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	return readUntilType(t, conn, protocol.EventTypeSessionCreated)
}

func sessionLogTexts(record data.SessionRecord) []string {
	out := make([]string, 0, len(record.Projection.LogEntries))
	for _, entry := range record.Projection.LogEntries {
		switch entry.Kind {
		case "markdown", "system", "user":
			if strings.TrimSpace(entry.Message) != "" {
				out = append(out, entry.Message)
			}
		case "terminal":
			if strings.TrimSpace(entry.Text) != "" {
				out = append(out, entry.Text)
			}
		case "error":
			if entry.Context != nil && strings.TrimSpace(entry.Context.Message) != "" {
				out = append(out, entry.Context.Message)
			}
		case "step":
			if entry.Context != nil && strings.TrimSpace(entry.Context.Message) != "" {
				out = append(out, entry.Context.Message)
			}
		case "diff":
			if entry.Context != nil && strings.TrimSpace(entry.Context.Title) != "" {
				out = append(out, entry.Context.Title)
			}
		}
	}
	return out
}

func containsText(items []string, want string) bool {
	for _, item := range items {
		if strings.Contains(item, want) {
			return true
		}
	}
	return false
}

func historyEventContainsText(event map[string]any, want string) bool {
	entries, ok := event["logEntries"].([]any)
	if !ok {
		return false
	}
	return eventEntriesContainText(entries, want)
}

func deltaEventContainsText(event map[string]any, want string) bool {
	entries, ok := event["appendLogEntries"].([]any)
	if !ok {
		return false
	}
	return eventEntriesContainText(entries, want)
}

func eventEntriesContainText(entries []any, want string) bool {
	for _, raw := range entries {
		entry, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if strings.Contains(fmt.Sprint(entry["message"], entry["text"]), want) {
			return true
		}
	}
	return false
}

func containsSessionSummaryID(items []data.SessionSummary, want string) bool {
	for _, item := range items {
		if item.ID == want {
			return true
		}
	}
	return false
}

func createHistorySessionForHandlerTest(t *testing.T, h *Handler, conn *websocket.Conn, title string) string {
	t.Helper()
	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: title}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	created := readUntilSessionCreated(t, conn)
	summary, ok := created["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", created)
	}
	sessionID, _ := summary["id"].(string)
	if sessionID == "" {
		t.Fatalf("expected created session id, got %#v", created)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	return sessionID
}

func writeTestFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

type nativeCodexThreadFixture struct {
	ID           string
	CWD          string
	Title        string
	Source       string
	ThreadSource string
}

func seedNativeCodexSessionFixture(t *testing.T, homeDir, cwd string) string {
	t.Helper()
	threadID := "019d3c6b-c538-7420-8028-345f7dd70d63"
	seedNativeCodexThreadsFixture(t, homeDir, []nativeCodexThreadFixture{{
		ID:    threadID,
		CWD:   cwd,
		Title: "Desktop Codex Session",
	}})
	return threadID
}

func seedNativeCodexThreadsFixture(t *testing.T, homeDir string, fixtures []nativeCodexThreadFixture) {
	t.Helper()
	if len(fixtures) == 0 {
		t.Fatal("expected at least one native codex fixture")
	}
	codexDir := filepath.Join(homeDir, ".codex")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	db, err := sql.Open("sqlite", filepath.Join(codexDir, "state_5.sqlite"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec("create table if not exists threads (id text primary key, cwd text, title text, model text, source text, model_provider text, thread_source text, created_at integer, updated_at integer, first_user_message text, rollout_path text, archived integer default 0);"); err != nil {
		t.Fatalf("create native codex threads table: %v", err)
	}
	if _, err := db.Exec("delete from threads;"); err != nil {
		t.Fatalf("clear native codex threads table: %v", err)
	}

	createdAt := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC).Unix()
	updatedAt := time.Date(2026, 3, 30, 11, 30, 0, 0, time.UTC).Unix()
	for i, fixture := range fixtures {
		threadID := strings.TrimSpace(fixture.ID)
		if threadID == "" {
			t.Fatalf("native codex fixture %d missing id", i)
		}
		cwd := strings.TrimSpace(fixture.CWD)
		if cwd == "" {
			t.Fatalf("native codex fixture %s missing cwd", threadID)
		}
		title := firstNonEmptyString(strings.TrimSpace(fixture.Title), "Desktop Codex Session")
		source := firstNonEmptyString(strings.TrimSpace(fixture.Source), "cli")
		rolloutPath := filepath.Join(codexDir, "sessions", "2026", "03", "30", "rollout-2026-03-30T11-30-00-"+threadID+".jsonl")
		if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o755); err != nil {
			t.Fatalf("mkdir rollout dir: %v", err)
		}
		if _, err := db.Exec(
			`insert into threads (id, cwd, title, model, source, model_provider, thread_source, created_at, updated_at, first_user_message, rollout_path, archived) values (?, ?, ?, 'gpt-5-codex', ?, 'openai', ?, ?, ?, ?, ?, 0)`,
			threadID,
			cwd,
			title,
			source,
			strings.TrimSpace(fixture.ThreadSource),
			createdAt+int64(i),
			updatedAt+int64(i),
			"Fix the README wording",
			rolloutPath,
		); err != nil {
			t.Fatalf("insert native codex fixture: %v", err)
		}
		writeNativeCodexRolloutFixture(t, rolloutPath, createdAt, updatedAt)
	}
	writeNativeCodexHistoryFixture(t, filepath.Join(codexDir, "history.jsonl"), fixtures, createdAt, updatedAt)
}

func writeNativeCodexHistoryFixture(t *testing.T, path string, fixtures []nativeCodexThreadFixture, createdAt, updatedAt int64) {
	t.Helper()
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create history fixture: %v", err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, fixture := range fixtures {
		threadID := strings.TrimSpace(fixture.ID)
		for _, line := range []map[string]any{
			{"session_id": threadID, "ts": createdAt, "text": "Fix the README wording"},
			{"session_id": threadID, "ts": updatedAt, "text": "Also align the mobile labels"},
		} {
			if err := encoder.Encode(line); err != nil {
				t.Fatalf("write history fixture: %v", err)
			}
		}
	}
}

func writeNativeCodexRolloutFixture(t *testing.T, path string, createdAt, updatedAt int64) {
	t.Helper()
	rolloutFile, err := os.Create(path)
	if err != nil {
		t.Fatalf("create rollout fixture: %v", err)
	}
	defer rolloutFile.Close()
	rolloutEncoder := json.NewEncoder(rolloutFile)
	rolloutLines := []map[string]any{
		{
			"timestamp": time.Unix(createdAt, 0).UTC().Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":    "task_started",
				"turn_id": "turn-1",
			},
		},
		{
			"timestamp": time.Unix(createdAt, 0).UTC().Add(time.Second).Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":    "user_message",
				"message": "Fix the README wording",
			},
		},
		{
			"timestamp": time.Unix(updatedAt, 0).UTC().Add(-time.Second).Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":    "agent_message",
				"message": "README wording updated. Mobile labels aligned too.",
				"phase":   "final_answer",
			},
		},
		{
			"timestamp": time.Unix(updatedAt, 0).UTC().Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":               "task_complete",
				"turn_id":            "turn-1",
				"last_agent_message": "README wording updated. Mobile labels aligned too.",
			},
		},
	}
	for _, line := range rolloutLines {
		if err := rolloutEncoder.Encode(line); err != nil {
			t.Fatalf("write rollout fixture: %v", err)
		}
	}
}

func TestApplyEventToProjectionPersistsAgentAndPromptState(t *testing.T) {
	snapshot := data.ProjectionSnapshot{}
	agent := protocol.NewAgentStateEvent("s1", string(session.ControllerStateThinking), "思考中", false, "claude", "", "")
	agent.RuntimeMeta = protocol.RuntimeMeta{Command: "claude", Engine: "claude", CWD: "/tmp", PermissionMode: "default", ClaudeLifecycle: "starting"}
	updated, ok := session.ApplyEventToProjection(snapshot, agent)
	if !ok {
		t.Fatal("expected agent state event to be persisted")
	}
	if updated.Controller.State != session.ControllerStateThinking {
		t.Fatalf("unexpected controller state: %v", updated.Controller.State)
	}
	if updated.Runtime.Command != "claude" {
		t.Fatalf("unexpected runtime command: %q", updated.Runtime.Command)
	}

	prompt := protocol.NewPromptRequestEvent("s1", "等待输入", []string{"y", "n"})
	prompt.RuntimeMeta = protocol.RuntimeMeta{Command: "claude", ResumeSessionID: "resume-1"}
	updated, ok = session.ApplyEventToProjection(updated, prompt)
	if !ok {
		t.Fatal("expected prompt request event to be persisted")
	}
	if updated.Controller.State != session.ControllerStateWaitInput {
		t.Fatalf("unexpected controller wait state: %v", updated.Controller.State)
	}
	if updated.Runtime.ClaudeLifecycle != "waiting_input" {
		t.Fatalf("unexpected runtime lifecycle: %q", updated.Runtime.ClaudeLifecycle)
	}
	if updated.Runtime.ResumeSessionID != "resume-1" {
		t.Fatalf("unexpected resume session id: %q", updated.Runtime.ResumeSessionID)
	}
}

func TestApplyEventToProjectionKeepsBootstrapLogsOutOfMarkdownHistory(t *testing.T) {
	snapshot := data.ProjectionSnapshot{}
	logEvent := protocol.NewLogEvent("s1", "Using codex medium mode", "stdout")
	logEvent.RuntimeMeta = protocol.RuntimeMeta{
		Command: "codex",
		Engine:  "codex",
	}
	marked, ok := session.MarkSystemBootstrapEvent(logEvent).(protocol.LogEvent)
	if !ok {
		t.Fatal("expected marked bootstrap log event")
	}
	updated, applied := session.ApplyEventToProjection(snapshot, marked)
	if !applied {
		t.Fatal("expected bootstrap log to be persisted")
	}
	if len(updated.LogEntries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(updated.LogEntries))
	}
	entry := updated.LogEntries[0]
	if entry.Kind != "terminal" {
		t.Fatalf("expected bootstrap log to stay terminal, got %q", entry.Kind)
	}
	if entry.Context == nil || entry.Context.Source != "system/bootstrap" {
		t.Fatalf("expected bootstrap source metadata, got %#v", entry.Context)
	}
}

func TestAssistantReplyNoticeParityWithProjectionPersistence(t *testing.T) {
	sessionID := "session-1"
	logEvent := protocol.NewLogEvent(sessionID, "我先帮你梳理下这个问题的根因，然后把现有判定链路和最稳妥的修复方案完整写给你，避免前后端再次分叉。", "stdout")
	logEvent.RuntimeMeta = protocol.RuntimeMeta{
		Command: "claude",
		Engine:  "claude",
	}

	notice := detachedResumeNoticeEvent(sessionID, logEvent)
	if notice == nil {
		t.Fatal("expected assistant reply notice for visible assistant log")
	}
	if notice.NoticeType != "assistant_reply" {
		t.Fatalf("expected assistant_reply notice, got %#v", notice)
	}

	updated, applied := session.ApplyEventToProjection(data.ProjectionSnapshot{}, logEvent)
	if !applied {
		t.Fatal("expected log event to be applied to projection")
	}
	if len(updated.LogEntries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(updated.LogEntries))
	}
	entry := updated.LogEntries[0]
	if entry.Kind != "markdown" {
		t.Fatalf("expected visible assistant reply to persist as markdown, got %#v", entry)
	}
	if !strings.Contains(entry.Message, "避免前后端再次分叉") {
		t.Fatalf("expected assistant reply body to persist, got %#v", entry)
	}
}

func TestAIStatusUsesBackendToolDetail(t *testing.T) {
	sessionID := "session-1"
	event := protocol.NewAgentStateEvent(sessionID, string(session.ControllerStateRunningTool), "Edit", false, "claude", "", "edit")
	event.RuntimeMeta = protocol.RuntimeMeta{
		Command:    "claude",
		Engine:     "claude",
		TargetPath: "/workspace/lib/main.dart",
	}

	status, ok := session.AIStatusEventForBackendEvent(sessionID, nil, data.ProjectionSnapshot{}, event)
	if !ok {
		t.Fatal("expected ai_status event")
	}
	if !status.Visible {
		t.Fatalf("expected ai_status visible, got %#v", status)
	}
	if status.Label != "正在修改 · main.dart" {
		t.Fatalf("expected concrete tool label, got %q", status.Label)
	}
	if status.Phase != "running_tool" {
		t.Fatalf("expected running_tool phase, got %q", status.Phase)
	}
}

func TestAIStatusSnapshotDoesNotHideProjectedBusyState(t *testing.T) {
	sessionID := "session-1"
	projection := session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Controller: session.ControllerSnapshot{
			SessionID:       sessionID,
			State:           session.ControllerStateRunningTool,
			CurrentCommand:  "claude",
			LastTool:        "edit",
			ActiveMeta:      protocol.RuntimeMeta{Command: "claude", TargetPath: "/workspace/lib/main.dart"},
			ClaudeLifecycle: "active",
		},
	})
	snapshot := protocol.NewTaskSnapshotEvent(
		sessionID,
		string(session.ControllerStateIdle),
		"stale idle",
		false,
		false,
		"claude",
		"",
		"",
		0,
		time.Time{},
		protocol.RuntimeMeta{Command: "claude"},
	)

	status, ok := session.AIStatusEventForBackendEvent(sessionID, nil, projection, snapshot)
	if !ok {
		t.Fatal("expected ai_status event")
	}
	if !status.Visible {
		t.Fatalf("expected projected busy state to remain visible, got %#v", status)
	}
	if status.Label != "正在修改 · main.dart" {
		t.Fatalf("expected projected tool label, got %q", status.Label)
	}
}

func TestApplyAICommandPreferencesReplacesStaleCodexModel(t *testing.T) {
	got := applyAICommandPreferences(
		"codex -m gpt-5-codex --config model_reasoning_effort=medium",
		"codex",
		"gpt-5.5",
		"high",
	)
	want := "codex -m gpt-5.5 --config model_reasoning_effort=high"
	if got != want {
		t.Fatalf("unexpected codex command: got %q want %q", got, want)
	}
}

func TestApplyAICommandPreferencesDefaultModelRemovesStaleModelFlag(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		engine  string
		want    string
	}{
		{
			name:    "claude",
			command: "claude --model sonnet",
			engine:  "claude",
			want:    "claude",
		},
		{
			name:    "codex",
			command: "codex -m gpt-5-codex --config model_reasoning_effort=medium",
			engine:  "codex",
			want:    "codex --config model_reasoning_effort=medium",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := applyAICommandPreferences(tc.command, tc.engine, "default", "")
			if got != tc.want {
				t.Fatalf("unexpected command: got %q want %q", got, tc.want)
			}
		})
	}
}

func TestAIStatusAssistantReplySettlesAndProjectionIdles(t *testing.T) {
	sessionID := "session-1"
	logEvent := protocol.NewLogEvent(sessionID, "我已经处理好了，可以继续。", "stdout")
	logEvent.RuntimeMeta = protocol.RuntimeMeta{Command: "claude", Engine: "claude"}
	projection := session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Controller: session.ControllerSnapshot{
			SessionID: sessionID,
			State:     session.ControllerStateRunningTool,
		},
	})

	updated, applied := session.ApplyEventToProjection(projection, logEvent)
	if !applied {
		t.Fatal("expected assistant log to apply")
	}
	if updated.Controller.State != session.ControllerStateIdle {
		t.Fatalf("expected assistant reply to settle controller state, got %q", updated.Controller.State)
	}
	status, ok := session.AIStatusEventForBackendEvent(sessionID, nil, updated, logEvent)
	if !ok {
		t.Fatal("expected ai_status event")
	}
	if status.Visible {
		t.Fatalf("expected settled assistant reply to hide ai_status, got %#v", status)
	}
	if status.Phase != "settled" {
		t.Fatalf("expected settled phase, got %q", status.Phase)
	}
}

func TestTerminalNoiseDoesNotTriggerAssistantReplyNoticeOrMarkdownPersistence(t *testing.T) {
	sessionID := "session-1"
	logEvent := protocol.NewLogEvent(sessionID, "2026-04-13 10:20:30 [INFO] build completed", "stdout")
	logEvent.RuntimeMeta = protocol.RuntimeMeta{
		Command: "claude",
		Engine:  "claude",
	}

	if notice := detachedResumeNoticeEvent(sessionID, logEvent); notice != nil {
		t.Fatalf("expected terminal-like noise not to trigger assistant notice, got %#v", notice)
	}

	updated, applied := session.ApplyEventToProjection(data.ProjectionSnapshot{}, logEvent)
	if !applied {
		t.Fatal("expected log event to be applied to projection")
	}
	if len(updated.LogEntries) != 1 {
		t.Fatalf("expected one log entry, got %d", len(updated.LogEntries))
	}
	if updated.LogEntries[0].Kind != "terminal" {
		t.Fatalf("expected terminal-like noise to stay terminal, got %#v", updated.LogEntries[0])
	}
}

func TestSessionResumeReplaysPendingEvents(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "resume-session")
	runtimeSession := h.runtimeSessions.Ensure(sessionID)
	if runtimeSession == nil {
		t.Fatal("expected runtime session")
	}
	runtimeSession.appendPending(protocol.NewPromptRequestEvent(sessionID, "继续输入", nil))
	runtimeSession.appendPending(protocol.NewSessionResumeNoticeEvent(sessionID, "assistant_reply", "info", "MobileVC", "后台期间有新的回复"))

	if err := conn.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "session_resume"},
		SessionID:           sessionID,
		LastSeenEventCursor: 0,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}

	_ = readUntilType(t, conn, protocol.EventTypeSessionHistory)
	promptEvent := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if got, _ := promptEvent["eventCursor"].(float64); got != 1 {
		t.Fatalf("expected prompt replay cursor 1, got %#v", promptEvent)
	}
	noticeEvent := readUntilType(t, conn, protocol.EventTypeSessionResumeNotice)
	if got, _ := noticeEvent["eventCursor"].(float64); got != 2 {
		t.Fatalf("expected notice replay cursor 2, got %#v", noticeEvent)
	}
	resultEvent := readUntilType(t, conn, protocol.EventTypeSessionResumeResult)
	if resultEvent["latestCursor"] != float64(2) {
		t.Fatalf("expected latest cursor 2, got %#v", resultEvent)
	}
	if resultEvent["replayedCount"] != float64(2) {
		t.Fatalf("expected replayedCount 2, got %#v", resultEvent)
	}
	if resultEvent["runtimeAlive"] != false {
		t.Fatalf("expected runtimeAlive false, got %#v", resultEvent)
	}
}

func TestSessionResumePreservesRequestedCodexPermissions(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "codex-resume-sandbox")
	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	record.Projection = session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Runtime: data.SessionRuntime{
			Command:         "codex",
			Engine:          "codex",
			CWD:             "/tmp/project",
			ResumeSessionID: "thread-sandbox",
		},
	})
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionID, record.Projection); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_resume"},
		RuntimeMeta: protocol.RuntimeMeta{
			Engine:           "codex",
			CodexSandboxMode: "danger-full-access",
			PermissionMode:   "config",
		},
		SessionID: sessionID,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}

	_ = readUntilSessionHistory(t, conn)
	runtimeEntry := h.runtimeSessions.Ensure(sessionID)
	if runtimeEntry == nil || runtimeEntry.service == nil {
		t.Fatal("expected runtime service")
	}
	if got := runtimeEntry.service.RuntimeSnapshot().ActiveMeta.CodexSandboxMode; got != "danger-full-access" {
		t.Fatalf("expected synced codex sandbox mode, got %q", got)
	}
	if got := runtimeEntry.service.RuntimeSnapshot().ActiveMeta.PermissionMode; got != "config" {
		t.Fatalf("expected synced codex permission mode, got %q", got)
	}
	updated, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if got := updated.Projection.Runtime.CodexSandboxMode; got != "danger-full-access" {
		t.Fatalf("expected persisted projection codex sandbox mode, got %#v", updated.Projection.Runtime)
	}
	if got := updated.Summary.Runtime.CodexSandboxMode; got != "danger-full-access" {
		t.Fatalf("expected persisted summary codex sandbox mode, got %#v", updated.Summary.Runtime)
	}
	if got := updated.Projection.Runtime.PermissionMode; got != "config" {
		t.Fatalf("expected persisted codex permission mode, got %#v", updated.Projection.Runtime)
	}
	if got := updated.Summary.Runtime.PermissionMode; got != "config" {
		t.Fatalf("expected persisted summary codex permission mode, got %#v", updated.Summary.Runtime)
	}
}

func TestSessionResumePreservesStoredCodexPermissionsFromCommand(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "codex-resume-command")
	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	record.Projection = session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Runtime: data.SessionRuntime{
			Command:          "/usr/local/bin/codex resume thread-command",
			CWD:              "/tmp/project",
			PermissionMode:   "auto",
			CodexSandboxMode: "workspace-write",
			ResumeSessionID:  "thread-command",
		},
	})
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionID, record.Projection); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_resume"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}

	_ = readUntilSessionHistory(t, conn)
	runtimeEntry := h.runtimeSessions.Ensure(sessionID)
	if runtimeEntry == nil || runtimeEntry.service == nil {
		t.Fatal("expected runtime service")
	}
	if got := runtimeEntry.service.RuntimeSnapshot().ActiveMeta.CodexSandboxMode; got != "workspace-write" {
		t.Fatalf("expected stored codex sandbox mode, got %q", got)
	}
	if got := runtimeEntry.service.RuntimeSnapshot().ActiveMeta.PermissionMode; got != "auto" {
		t.Fatalf("expected stored codex permission mode, got %q", got)
	}
	updated, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if got := updated.Projection.Runtime.CodexSandboxMode; got != "workspace-write" {
		t.Fatalf("expected persisted codex sandbox mode, got %#v", updated.Projection.Runtime)
	}
	if got := updated.Projection.Runtime.PermissionMode; got != "auto" {
		t.Fatalf("expected persisted codex permission mode, got %#v", updated.Projection.Runtime)
	}
}

func TestHandlerSessionResumeReturnsBoundedHistoryWindow(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	now := time.Date(2026, 5, 31, 17, 0, 0, 0, time.UTC)
	entries := make([]data.SnapshotLogEntry, sessionResumeHistoryLimit+2)
	for i := range entries {
		entries[i] = data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   fmt.Sprintf("entry-%03d", i+1),
			Timestamp: now.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
	}
	record := data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "resume-window-session",
			Title:     "Resume Window",
			CreatedAt: now,
			UpdatedAt: now,
		},
		Projection: data.ProjectionSnapshot{LogEntries: entries},
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_resume"},
		SessionID:   record.Summary.ID,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}

	history := readUntilSessionHistory(t, conn)
	if got := intFromJSONNumber(history["logEntryStart"]); got != 2 {
		t.Fatalf("history start: got %d", got)
	}
	if got := intFromJSONNumber(history["logEntryTotal"]); got != sessionResumeHistoryLimit+2 {
		t.Fatalf("history total: got %d", got)
	}
	if history["hasMoreBefore"] != true {
		t.Fatalf("expected hasMoreBefore=true, got %#v", history)
	}
	gotEntries := history["logEntries"].([]any)
	if len(gotEntries) != sessionResumeHistoryLimit {
		t.Fatalf("expected %d entries, got %d", sessionResumeHistoryLimit, len(gotEntries))
	}
	if gotEntries[0].(map[string]any)["message"] != "entry-003" {
		t.Fatalf("unexpected first bounded entry: %#v", gotEntries[0])
	}
}

func TestHandlerSkillCatalogLifecycle(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".claude", "skills", "image-generation", "SKILL.md"),
		"---\nname: image-generation\ndescription: Generate images from text.\n---\n# Image Generation\n\nUse the image model.\n",
	)
	writeTestFile(t,
		filepath.Join(homeDir, ".agents", "skills", "shared-agent-skill", "SKILL.md"),
		"---\nname: shared-agent-skill\ndescription: Shared agent skill.\n---\n# Shared Agent Skill\n\nUse the shared agent skill.\n",
	)
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.SkillLauncher = nil
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "skill-session")

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "skill_catalog_get"}); err != nil {
		t.Fatalf("write skill_catalog_get request: %v", err)
	}
	getEvent := readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	items, ok := getEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected skill catalog items, got %#v", getEvent)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty persisted skill catalog initially, got %#v", items)
	}

	if err := conn.WriteJSON(protocol.SkillCatalogRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "skill_catalog_upsert"},
		Skill: protocol.SkillDefinition{
			Name:        "local-review-extra",
			Description: "local skill",
			Prompt:      "please review",
			ResultView:  "review-card",
			TargetType:  "diff",
		},
	}); err != nil {
		t.Fatalf("write skill_catalog_upsert request: %v", err)
	}
	upsertEvent := readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	upsertItems, ok := upsertEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected skill catalog items, got %#v", upsertEvent)
	}
	foundLocal := false
	for _, raw := range upsertItems {
		item, _ := raw.(map[string]any)
		if item["name"] == "local-review-extra" {
			foundLocal = true
			if item["source"] != string(data.SkillSourceLocal) {
				t.Fatalf("expected local source, got %#v", item)
			}
		}
	}
	if !foundLocal {
		t.Fatalf("expected local skill in catalog, got %#v", upsertItems)
	}

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "skill_sync_pull"}); err != nil {
		t.Fatalf("write skill_sync_pull request: %v", err)
	}
	syncEvent := readUntilType(t, conn, protocol.EventTypeSkillSyncResult)
	if syncEvent["msg"] != "skill 同步完成" {
		t.Fatalf("unexpected skill sync event: %#v", syncEvent)
	}
	syncResult := readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	if syncResult["domain"] != "skill" || syncResult["success"] != true {
		t.Fatalf("unexpected catalog sync result: %#v", syncResult)
	}
	syncedCatalog := readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	syncedItems, ok := syncedCatalog["items"].([]any)
	if !ok {
		t.Fatalf("expected synced skill catalog items, got %#v", syncedCatalog)
	}
	meta, ok := syncedCatalog["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected skill catalog meta, got %#v", syncedCatalog)
	}
	if meta["syncState"] != string(data.CatalogSyncStateSynced) || meta["sourceOfTruth"] != string(data.CatalogSourceTruthClaude) {
		t.Fatalf("unexpected skill catalog meta: %#v", meta)
	}
	foundExternal := false
	foundAgentSkill := false
	for _, raw := range syncedItems {
		item, _ := raw.(map[string]any)
		if item["name"] == "image-generation" {
			foundExternal = true
			if item["source"] != string(data.SkillSourceExternal) {
				t.Fatalf("expected external source, got %#v", item)
			}
			if item["syncState"] != string(data.CatalogSyncStateSynced) {
				t.Fatalf("expected synced item state, got %#v", item)
			}
			if item["editable"] != true {
				t.Fatalf("expected synced external skill editable, got %#v", item)
			}
		}
		if item["name"] == "shared-agent-skill" {
			foundAgentSkill = true
			if item["source"] != string(data.SkillSourceExternal) || item["sourceOfTruth"] != string(data.CatalogSourceTruthClaude) {
				t.Fatalf("expected shared agent skill to sync as claude external skill, got %#v", item)
			}
		}
	}
	if !foundExternal {
		t.Fatalf("expected external synced skill, got %#v", syncedItems)
	}
	if !foundAgentSkill {
		t.Fatalf("expected shared agent skill, got %#v", syncedItems)
	}
}

func TestHandlerSkillSyncPullAckAndPingWhileSyncBlocks(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".claude", "skills", "blocked-sync-skill", "SKILL.md"),
		"---\nname: blocked-sync-skill\ndescription: Blocking sync fixture.\n---\n# Blocking Sync Skill\n\nUse this fixture.\n",
	)
	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingSkillSyncStore(fileStore)
	h := newTestHandler()
	h.SessionStore = blockingStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "skill-sync-ack-session")

	if err := conn.WriteJSON(protocol.ClientEvent{
		Action:         "skill_sync_pull",
		ClientActionID: "skill-sync-blocking",
	}); err != nil {
		t.Fatalf("write skill_sync_pull request: %v", err)
	}
	ack := readUntilClientActionAck(t, conn, "skill_sync_pull", "skill-sync-blocking")
	if ack["duplicate"] == true {
		t.Fatalf("expected first skill sync ack to be accepted, got %#v", ack)
	}
	blockingStore.WaitSyncingSaveStarted(t)

	if err := conn.WriteJSON(map[string]any{"action": "ping", "pingId": "during-skill-sync"}); err != nil {
		t.Fatalf("write ping request: %v", err)
	}
	pong := readUntilPongID(t, conn, "during-skill-sync")
	if pong["sessionId"] != sessionID {
		t.Fatalf("expected pong for loaded session %q, got %#v", sessionID, pong)
	}

	blockingStore.ReleaseSyncingSave()
	_ = readUntilType(t, conn, protocol.EventTypeSkillSyncResult)
	result := readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	if result["domain"] != "skill" || result["success"] != true {
		t.Fatalf("unexpected skill sync result: %#v", result)
	}
}

func TestHandlerSkillSyncPullDuplicateClientActionDoesNotStartSecondSync(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".claude", "skills", "duplicate-sync-skill", "SKILL.md"),
		"---\nname: duplicate-sync-skill\ndescription: Duplicate sync fixture.\n---\n# Duplicate Sync Skill\n\nUse this fixture.\n",
	)
	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingSkillSyncStore(fileStore)
	h := newTestHandler()
	h.SessionStore = blockingStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "skill-sync-duplicate-session")

	request := protocol.ClientEvent{
		Action:         "skill_sync_pull",
		ClientActionID: "skill-sync-repeat",
	}
	if err := conn.WriteJSON(request); err != nil {
		t.Fatalf("write first skill_sync_pull request: %v", err)
	}
	firstAck := readUntilClientActionAck(t, conn, "skill_sync_pull", "skill-sync-repeat")
	if firstAck["duplicate"] == true {
		t.Fatalf("expected first sync ack to be accepted, got %#v", firstAck)
	}
	blockingStore.WaitSyncingSaveStarted(t)

	if err := conn.WriteJSON(request); err != nil {
		t.Fatalf("write duplicate skill_sync_pull request: %v", err)
	}
	duplicateAck := readUntilClientActionAck(t, conn, "skill_sync_pull", "skill-sync-repeat")
	if duplicateAck["duplicate"] != true {
		t.Fatalf("expected duplicate sync ack, got %#v", duplicateAck)
	}
	if got := blockingStore.SyncingSaveCount(); got != 1 {
		t.Fatalf("expected one skill sync to start, got %d", got)
	}

	blockingStore.ReleaseSyncingSave()
	_ = readUntilType(t, conn, protocol.EventTypeSkillSyncResult)
	result := readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	if result["domain"] != "skill" || result["success"] != true {
		t.Fatalf("unexpected skill sync result: %#v", result)
	}
	if got := blockingStore.SyncingSaveCount(); got != 1 {
		t.Fatalf("expected duplicate request not to start another sync, got %d", got)
	}
}

func TestHandlerSkillSyncPullFinalStateSurvivesDisconnect(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".claude", "skills", "disconnect-sync-skill", "SKILL.md"),
		"---\nname: disconnect-sync-skill\ndescription: Disconnect sync fixture.\n---\n# Disconnect Sync Skill\n\nUse this fixture.\n",
	)
	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingFinalSkillSyncStore(fileStore)
	h := newTestHandler()
	h.SessionStore = blockingStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "skill-sync-disconnect-session")

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "skill_sync_pull"}); err != nil {
		t.Fatalf("write skill_sync_pull request: %v", err)
	}
	blockingStore.WaitFinalSaveStarted(t)
	if err := conn.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}
	waitRuntimeSessionDetached(t, h, sessionID)
	blockingStore.ReleaseFinalSave()

	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := fileStore.GetSkillCatalogSnapshot(context.Background())
		if err != nil {
			t.Fatalf("get skill catalog snapshot: %v", err)
		}
		if snapshot.Meta.SyncState == data.CatalogSyncStateSynced {
			if got := blockingStore.FinalSaveCount(); got != 1 {
				t.Fatalf("expected one final skill sync save, got %d", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected disconnected skill sync to finish, got meta %#v", snapshot.Meta)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHandlerSkillCatalogLifecycleUsesCodexSkillsForCodexSession(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".codex", "skills", "mobilevc-release", "SKILL.md"),
		"---\nname: mobilevc-release\ndescription: Publish MobileVC.\n---\n# MobileVC Release\n\nDo release work.\n",
	)
	writeTestFile(t,
		filepath.Join(homeDir, ".agents", "skills", "shared-agent-skill", "SKILL.md"),
		"---\nname: shared-agent-skill\ndescription: Shared agent skill.\n---\n# Shared Agent Skill\n\nUse the shared agent skill.\n",
	)
	writeTestFile(t,
		filepath.Join(homeDir, ".agents", "skills", "mobilevc-release", "SKILL.md"),
		"---\nname: mobilevc-release\ndescription: Shared duplicate.\n---\n# Shared Duplicate\n\nThis should not replace the codex-specific skill.\n",
	)
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "codex-skill-session")
	if _, err := tempStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Runtime:             data.SessionRuntime{Engine: "codex", Command: "codex", Source: "mobilevc"},
	}); err != nil {
		t.Fatalf("save codex projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "skill_sync_pull"}); err != nil {
		t.Fatalf("write skill_sync_pull request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSkillSyncResult)
	_ = readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	syncedCatalog := readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	meta, ok := syncedCatalog["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected skill catalog meta, got %#v", syncedCatalog)
	}
	if meta["sourceOfTruth"] != string(data.CatalogSourceTruthCodex) {
		t.Fatalf("expected codex source of truth, got %#v", meta)
	}
	items, ok := syncedCatalog["items"].([]any)
	if !ok {
		t.Fatalf("expected synced skill catalog items, got %#v", syncedCatalog)
	}
	foundCodexSkill := false
	foundAgentSkill := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["name"] == "mobilevc-release" {
			foundCodexSkill = true
			if item["sourceOfTruth"] != string(data.CatalogSourceTruthCodex) {
				t.Fatalf("expected codex skill item, got %#v", item)
			}
			if item["description"] != "Publish MobileVC." {
				t.Fatalf("expected codex skill to win over shared duplicate, got %#v", item)
			}
		}
		if item["name"] == "shared-agent-skill" {
			foundAgentSkill = true
			if item["sourceOfTruth"] != string(data.CatalogSourceTruthCodex) {
				t.Fatalf("expected shared agent skill to sync under codex source of truth, got %#v", item)
			}
		}
		if item["name"] == "image-generation" {
			t.Fatalf("did not expect claude skill in codex catalog: %#v", item)
		}
	}
	if !foundCodexSkill {
		t.Fatalf("expected codex synced skill, got %#v", items)
	}
	if !foundAgentSkill {
		t.Fatalf("expected shared agent skill, got %#v", items)
	}
}

func TestLoadSkillDefinitionsIncludesSingularAgentSkillsDir(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".agent", "skills", "singular-agent-skill", "SKILL.md"),
		"---\nname: singular-agent-skill\ndescription: Singular agent skill dir.\n---\n# Singular Agent Skill\n\nUse the singular agent skill.\n",
	)

	items, err := loadCodexSkillDefinitions(time.Now().UTC())
	if err != nil {
		t.Fatalf("load codex skill definitions: %v", err)
	}
	for _, item := range items {
		if item.Name == "singular-agent-skill" {
			if item.SourceOfTruth != data.CatalogSourceTruthCodex {
				t.Fatalf("expected codex source of truth for singular agent skill, got %#v", item)
			}
			return
		}
	}
	t.Fatalf("expected singular agent skill, got %#v", items)
}

func TestHandlerCatalogAuthoringSkillAutoUpsertsAndEmitsCatalog(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerStub := newSwitchableStubRunner()
	h.NewPtyRunner = func() engine.Runner { return runnerStub }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "authoring-skill-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "author a skill",
		Mode:        "pty",
		RuntimeMeta: protocol.RuntimeMeta{Source: "catalog-authoring", TargetType: "skill", ResultView: "skill-catalog"},
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	runnerStub.Emit(protocol.CatalogAuthoringResultEvent{
		Event:  protocol.NewBaseEvent(protocol.EventTypeCatalogAuthoringResult, runnerStub.req.SessionID),
		Domain: "skill",
		Skill: &protocol.SkillDefinition{
			Name:        "authoring-skill",
			Description: "generated",
			Prompt:      "do it",
			TargetType:  "diff",
			ResultView:  "review-card",
		},
	})

	catalogEvent := readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	items, ok := catalogEvent["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one skill item, got %#v", catalogEvent)
	}
	item, _ := items[0].(map[string]any)
	if item["name"] != "authoring-skill" {
		t.Fatalf("unexpected skill payload: %#v", item)
	}
}

func TestHandlerCatalogAuthoringMemoryAutoUpsertsAndEmitsCatalog(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerStub := newSwitchableStubRunner()
	h.NewPtyRunner = func() engine.Runner { return runnerStub }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "authoring-memory-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "author a memory",
		Mode:        "pty",
		RuntimeMeta: protocol.RuntimeMeta{Source: "catalog-authoring", TargetType: "memory", ResultView: "memory-catalog"},
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	runnerStub.Emit(protocol.CatalogAuthoringResultEvent{
		Event:  protocol.NewBaseEvent(protocol.EventTypeCatalogAuthoringResult, runnerStub.req.SessionID),
		Domain: "memory",
		Memory: &protocol.MemoryItem{ID: "mem-author", Title: "Author", Content: "generated memory"},
	})

	listEvent := readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	items, ok := listEvent["items"].([]any)
	if !ok || len(items) != 1 {
		t.Fatalf("expected one memory item, got %#v", listEvent)
	}
	item, _ := items[0].(map[string]any)
	if item["id"] != "mem-author" {
		t.Fatalf("unexpected memory payload: %#v", item)
	}
}

func TestHandlerCatalogAuthoringInvalidPayloadEmitsError(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerStub := newSwitchableStubRunner()
	h.NewPtyRunner = func() engine.Runner { return runnerStub }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "authoring-invalid-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "author invalid",
		Mode:        "pty",
		RuntimeMeta: protocol.RuntimeMeta{Source: "catalog-authoring", TargetType: "skill", ResultView: "skill-catalog"},
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	runnerStub.Emit(protocol.CatalogAuthoringResultEvent{
		Event:  protocol.NewBaseEvent(protocol.EventTypeCatalogAuthoringResult, runnerStub.req.SessionID),
		Domain: "skill",
		Skill:  &protocol.SkillDefinition{},
	})

	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if _, ok := errorEvent["msg"].(string); !ok {
		t.Fatalf("expected error event, got %#v", errorEvent)
	}
}

func TestHandlerMemoryListAndUpsert(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "memory-session")

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "memory_list"}); err != nil {
		t.Fatalf("write memory_list request: %v", err)
	}
	listEvent := readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected memory items array, got %#v", listEvent)
	}
	if len(items) != 0 {
		t.Fatalf("expected empty memory catalog, got %#v", items)
	}

	if err := conn.WriteJSON(protocol.MemoryRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "memory_upsert"},
		Item:        protocol.MemoryItem{ID: "m-test", Title: "Test Memory", Content: "remember this"},
	}); err != nil {
		t.Fatalf("write memory_upsert request: %v", err)
	}
	upsertEvent := readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	upsertItems, ok := upsertEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected memory items array after upsert, got %#v", upsertEvent)
	}
	if len(upsertItems) != 1 {
		t.Fatalf("expected one memory item after upsert, got %#v", upsertItems)
	}
	item, _ := upsertItems[0].(map[string]any)
	if item["id"] != "m-test" || item["content"] != "remember this" {
		t.Fatalf("unexpected memory item payload: %#v", item)
	}
	persisted, err := tempStore.ListMemoryCatalog(context.Background())
	if err != nil {
		t.Fatalf("list persisted memory catalog: %v", err)
	}
	if len(persisted) != 1 || persisted[0].ID != "m-test" {
		t.Fatalf("unexpected persisted memory items: %#v", persisted)
	}
}

func TestHandlerMemorySyncPullEmitsCatalogLifecycle(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "memory-sync-session")
	projectRoot := filepath.Join(homeDir, "workspace", "demo-project")
	projectChild := filepath.Join(projectRoot, "mobile_vc")
	if err := os.MkdirAll(projectChild, 0o755); err != nil {
		t.Fatalf("mkdir project child: %v", err)
	}
	memoryDir := filepath.Join(
		homeDir,
		".claude",
		"projects",
		encodeClaudeProjectPath(projectRoot),
		"memory",
	)
	writeTestFile(t,
		filepath.Join(memoryDir, "testing_notes.md"),
		"# Testing Notes\n\nRemember the real Claude project memory synced.\n",
	)
	if _, err := tempStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Runtime:             data.SessionRuntime{CWD: projectChild},
	}); err != nil {
		t.Fatalf("save projection: %v", err)
	}
	record, err := tempStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get saved session: %v", err)
	}
	if got := normalizeSessionCWD(record.Projection.Runtime.CWD); got != normalizeSessionCWD(projectChild) {
		t.Fatalf("expected saved projection cwd %q, got %q", normalizeSessionCWD(projectChild), got)
	}
	if got := resolveCatalogSyncCWD(tempStore, context.Background(), sessionID, ""); got != normalizeSessionCWD(projectChild) {
		t.Fatalf("expected sync cwd %q, got %q", normalizeSessionCWD(projectChild), got)
	}
	if got, err := findClaudeProjectMemoryDir(projectChild); err != nil {
		t.Fatalf("find claude project memory dir: %v", err)
	} else if normalizeSessionCWD(got) != normalizeSessionCWD(memoryDir) {
		t.Fatalf("expected memory dir %q, got %q", normalizeSessionCWD(memoryDir), normalizeSessionCWD(got))
	}
	externalMemories, err := loadClaudeProjectMemories(projectChild, time.Now().UTC())
	if err != nil {
		t.Fatalf("load claude project memories: %v", err)
	}
	if len(externalMemories) != 1 || externalMemories[0].ID != "testing_notes" {
		t.Fatalf("expected direct external memory lookup to find testing_notes, got %#v", externalMemories)
	}

	if err := conn.WriteJSON(protocol.MemoryRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "memory_upsert"},
		Item:        protocol.MemoryItem{ID: "m-test", Title: "Test Memory", Content: "remember this"},
	}); err != nil {
		t.Fatalf("write memory_upsert request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)

	if err := conn.WriteJSON(protocol.MemoryRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "memory_sync_pull"},
		CWD:         projectChild,
	}); err != nil {
		t.Fatalf("write memory_sync_pull request: %v", err)
	}
	statusEvent := readUntilType(t, conn, protocol.EventTypeCatalogSyncStatus)
	if statusEvent["domain"] != "memory" {
		t.Fatalf("unexpected memory sync status: %#v", statusEvent)
	}
	if statusEvent["sessionId"] != sessionID {
		t.Fatalf("expected memory sync status session %q, got %#v", sessionID, statusEvent)
	}
	resultEvent := readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	if resultEvent["domain"] != "memory" || resultEvent["success"] != true {
		t.Fatalf("unexpected memory sync result: %#v", resultEvent)
	}
	if resultEvent["sessionId"] != sessionID {
		t.Fatalf("expected memory sync result session %q, got %#v", sessionID, resultEvent)
	}
	listEvent := readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	if listEvent["sessionId"] != sessionID {
		t.Fatalf("expected memory list session %q, got %#v", sessionID, listEvent)
	}
	meta, ok := listEvent["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory meta, got %#v", listEvent)
	}
	if meta["syncState"] != string(data.CatalogSyncStateSynced) || meta["sourceOfTruth"] != string(data.CatalogSourceTruthClaude) {
		t.Fatalf("unexpected memory meta after sync: %#v", meta)
	}
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected memory items after sync, got %#v", listEvent)
	}
	if len(items) != 2 {
		t.Fatalf("expected local and external memory items after sync, got %#v", items)
	}
	foundLocal := false
	foundExternal := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == "m-test" {
			foundLocal = true
			if item["syncState"] != string(data.CatalogSyncStateDraft) {
				t.Fatalf("unexpected local memory payload: %#v", item)
			}
		}
		if item["id"] == "testing_notes" {
			foundExternal = true
			if item["source"] != "claude-project-memory" || item["syncState"] != string(data.CatalogSyncStateSynced) {
				t.Fatalf("unexpected external memory payload: %#v", item)
			}
			if item["editable"] != true {
				t.Fatalf("expected synced external memory editable, got %#v", item)
			}
		}
	}
	if !foundLocal || !foundExternal {
		t.Fatalf("expected both local and external memories, got %#v", items)
	}
}

func TestHandlerMemorySyncPullFinalStateSurvivesDisconnect(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	projectRoot := filepath.Join(homeDir, "workspace", "disconnect-project")
	projectChild := filepath.Join(projectRoot, "mobile_vc")
	if err := os.MkdirAll(projectChild, 0o755); err != nil {
		t.Fatalf("mkdir project child: %v", err)
	}
	memoryDir := filepath.Join(
		homeDir,
		".claude",
		"projects",
		encodeClaudeProjectPath(projectRoot),
		"memory",
	)
	writeTestFile(t,
		filepath.Join(memoryDir, "disconnect_notes.md"),
		"# Disconnect Notes\n\nRemember that memory sync finishes after disconnect.\n",
	)

	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingFinalMemorySyncStore(fileStore)
	h := newTestHandler()
	h.SessionStore = blockingStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "memory-sync-disconnect-session")
	if _, err := fileStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Runtime:             data.SessionRuntime{CWD: projectChild},
	}); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.MemoryRequestEvent{ClientEvent: protocol.ClientEvent{Action: "memory_sync_pull"}}); err != nil {
		t.Fatalf("write memory_sync_pull request: %v", err)
	}
	blockingStore.WaitFinalSaveStarted(t)
	if err := conn.Close(); err != nil {
		t.Fatalf("close websocket: %v", err)
	}
	waitRuntimeSessionDetached(t, h, sessionID)
	blockingStore.ReleaseFinalSave()

	deadline := time.Now().Add(5 * time.Second)
	for {
		snapshot, err := fileStore.GetMemoryCatalogSnapshot(context.Background())
		if err != nil {
			t.Fatalf("get memory catalog snapshot: %v", err)
		}
		if snapshot.Meta.SyncState == data.CatalogSyncStateSynced {
			if got := blockingStore.FinalSaveCount(); got != 1 {
				t.Fatalf("expected one final memory sync save, got %d", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected disconnected memory sync to finish, got meta %#v", snapshot.Meta)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHandlerMemorySyncPullUsesCodexMemoryForCodexSession(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(homeDir, ".codex", "memories", "mobilevc.md"),
		"# MobileVC\n\nRemember Codex-specific MobileVC notes.\n",
	)
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "codex-memory-session")
	if _, err := tempStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Runtime:             data.SessionRuntime{Engine: "codex", Command: "codex", Source: "mobilevc"},
	}); err != nil {
		t.Fatalf("save codex projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.MemoryRequestEvent{ClientEvent: protocol.ClientEvent{Action: "memory_sync_pull"}}); err != nil {
		t.Fatalf("write memory_sync_pull request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeCatalogSyncStatus)
	_ = readUntilType(t, conn, protocol.EventTypeCatalogSyncResult)
	listEvent := readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	meta, ok := listEvent["meta"].(map[string]any)
	if !ok {
		t.Fatalf("expected memory meta, got %#v", listEvent)
	}
	if meta["sourceOfTruth"] != string(data.CatalogSourceTruthCodex) {
		t.Fatalf("expected codex memory source of truth, got %#v", meta)
	}
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected memory items, got %#v", listEvent)
	}
	foundCodexMemory := false
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == "mobilevc" {
			foundCodexMemory = true
			if item["source"] != "codex-memory" {
				t.Fatalf("expected codex memory source, got %#v", item)
			}
			if item["editable"] != true {
				t.Fatalf("expected codex memory editable, got %#v", item)
			}
		}
		if item["id"] == "testing_notes" {
			t.Fatalf("did not expect claude project memory in codex session: %#v", item)
		}
	}
	if !foundCodexMemory {
		t.Fatalf("expected codex memory item, got %#v", items)
	}
}

func TestFindClaudeProjectMemoryDirDoesNotFallbackToHomeLevelMemory(t *testing.T) {
	homeDir := t.TempDir()
	t.Setenv("HOME", homeDir)
	writeTestFile(t,
		filepath.Join(
			homeDir,
			".claude",
			"projects",
			encodeClaudeProjectPath(homeDir),
			"memory",
			"mobilevc.md",
		),
		"# Home Memory\n\nThis should not be treated as the current project memory.\n",
	)
	cwd := filepath.Join(homeDir, ".codex")
	if err := os.MkdirAll(cwd, 0o755); err != nil {
		t.Fatalf("mkdir cwd: %v", err)
	}

	got, err := findClaudeProjectMemoryDir(cwd)
	if err != nil {
		t.Fatalf("findClaudeProjectMemoryDir: %v", err)
	}
	if got != "" {
		t.Fatalf("expected no project memory match for home-level fallback, got %q", got)
	}
}

func TestHandlerPermissionDecisionWithoutActiveClaudeRunnerDoesNotResumeLoop(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerStub := newStubRunner()
	h.NewPtyRunner = func() engine.Runner { return runnerStub }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "permission-session")

	if _, err := tempStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Runtime: data.SessionRuntime{
			Command:         "bash",
			ResumeSessionID: "resume-123",
			PermissionMode:  "default",
			CWD:             "/workspace",
		},
	}); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:     protocol.ClientEvent{Action: "permission_decision"},
		Decision:        "approve",
		PermissionMode:  "default",
		ResumeSessionID: "resume-123",
		PromptMessage:   "Allow write?",
		FallbackCommand: "bash",
		FallbackCWD:     "/workspace",
	}); err != nil {
		t.Fatalf("write permission_decision request: %v", err)
	}

	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	msg, _ := errorEvent["msg"].(string)
	if msg != "当前没有可交互的 Claude 会话，无法继续处理该权限请求" {
		t.Fatalf("unexpected error event: %#v", errorEvent)
	}
	select {
	case data := <-runnerStub.writeCh:
		t.Fatalf("expected no permission input replay, got %q", string(data))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerInitialConnectionDoesNotTreatConnectionIDAsSession(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	first, second := readInitialEvents(t, conn)
	requireEventType(t, first, protocol.EventTypeSessionState)
	requireAgentState(t, second, "IDLE", false)
	if sessionID, _ := first["sessionId"].(string); sessionID != "" {
		t.Fatalf("expected empty initial session id, got %#v", first)
	}
	for i := 0; i < 3; i++ {
		event := readEventMap(t, conn)
		if event["type"] == protocol.EventTypeError {
			msg, _ := event["msg"].(string)
			if strings.Contains(msg, "session not found: conn-") {
				t.Fatalf("unexpected connection-id session lookup error: %#v", event)
			}
		}
	}
}

func TestHandlerSessionContextGetWithoutSelectedSessionReturnsEmptyResult(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	if err := conn.WriteJSON(protocol.ClientEvent{Action: "session_context_get"}); err != nil {
		t.Fatalf("write session_context_get request: %v", err)
	}
	event := readUntilType(t, conn, protocol.EventTypeSessionContextResult)
	ctx, ok := event["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionContext payload, got %#v", event)
	}
	if len(ctx) != 0 {
		t.Fatalf("expected empty session context, got %#v", ctx)
	}
}

func TestHandlerSessionContextUpdateWithoutSelectedSessionReturnsError(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_context_update"}, EnabledSkillNames: []string{"review"}}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	event := readUntilType(t, conn, protocol.EventTypeError)
	msg, _ := event["msg"].(string)
	if !strings.Contains(msg, "请先创建或加载会话") {
		t.Fatalf("expected explicit no-session error, got %#v", event)
	}
}

func TestHandlerExecWithoutSelectedSessionReturnsError(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	msg, _ := event["msg"].(string)
	if !strings.Contains(msg, "请先创建或加载会话") {
		t.Fatalf("expected explicit no-session error, got %#v", event)
	}
	items, err := tempStore.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no projection/session writes, got %#v", items)
	}
}

func TestHandlerInputWithoutSelectedSessionReturnsError(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "hello\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	msg, _ := event["msg"].(string)
	if !strings.Contains(msg, "请先创建或加载会话") {
		t.Fatalf("expected explicit no-session error, got %#v", event)
	}
	items, err := tempStore.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("list sessions: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("expected no projection/session writes, got %#v", items)
	}
}

func TestHandlerDeleteCurrentSessionFallsBackToEmptyState(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSkillCatalogResult)
	_ = readUntilType(t, conn, protocol.EventTypeMemoryListResult)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "only-session")
	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_delete"}, SessionID: sessionID}); err != nil {
		t.Fatalf("write session_delete request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	event := readUntilType(t, conn, protocol.EventTypeSessionState)
	if sessionValue, _ := event["sessionId"].(string); sessionValue != "" {
		t.Fatalf("expected empty session after delete fallback, got %#v", event)
	}
	if state, _ := event["state"].(string); state != string(session.StateActive) {
		t.Fatalf("expected active empty state, got %#v", event)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request after delete: %v", err)
	}
	guardEvent := readUntilType(t, conn, protocol.EventTypeError)
	msg, _ := guardEvent["msg"].(string)
	if !strings.Contains(msg, "请先创建或加载会话") {
		t.Fatalf("expected explicit no-session error after delete fallback, got %#v", guardEvent)
	}
}

func TestHandlerSessionContextGetUpdateAndRestore(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "context-session")

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "session_context_get"}); err != nil {
		t.Fatalf("write session_context_get request: %v", err)
	}
	initialEvent := readUntilType(t, conn, protocol.EventTypeSessionContextResult)
	initialContext, ok := initialEvent["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionContext payload, got %#v", initialEvent)
	}
	if len(initialContext) != 0 {
		t.Fatalf("expected empty initial sessionContext, got %#v", initialContext)
	}

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:       protocol.ClientEvent{Action: "session_context_update"},
		EnabledSkillNames: []string{"review", "analyze"},
		EnabledMemoryIDs:  []string{"m-test", "m-2"},
	}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	updatedEvent := readUntilType(t, conn, protocol.EventTypeSessionContextResult)
	updatedContext, ok := updatedEvent["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected updated sessionContext payload, got %#v", updatedEvent)
	}
	skillNames, ok := updatedContext["enabledSkillNames"].([]any)
	if !ok || len(skillNames) != 2 {
		t.Fatalf("expected enabledSkillNames, got %#v", updatedContext)
	}
	memoryIDs, ok := updatedContext["enabledMemoryIds"].([]any)
	if !ok || len(memoryIDs) != 2 {
		t.Fatalf("expected enabledMemoryIds, got %#v", updatedContext)
	}

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if len(record.Projection.SessionContext.EnabledSkillNames) != 2 || len(record.Projection.SessionContext.EnabledMemoryIDs) != 2 {
		t.Fatalf("unexpected persisted session context: %#v", record.Projection.SessionContext)
	}
	if record.Projection.SkillCatalogMeta.SyncState != data.CatalogSyncStateIdle || record.Projection.MemoryCatalogMeta.SyncState != data.CatalogSyncStateIdle {
		t.Fatalf("session context update should preserve catalog sync state defaults: %#v %#v", record.Projection.SkillCatalogMeta, record.Projection.MemoryCatalogMeta)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_load"}, SessionID: sessionID}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	historyContext, ok := history["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionContext in session history, got %#v", history)
	}
	historySkills, ok := historyContext["enabledSkillNames"].([]any)
	if !ok || len(historySkills) != 2 {
		t.Fatalf("expected restored enabledSkillNames, got %#v", historyContext)
	}
	historyMemory, ok := historyContext["enabledMemoryIds"].([]any)
	if !ok || len(historyMemory) != 2 {
		t.Fatalf("expected restored enabledMemoryIds, got %#v", historyContext)
	}
}

func TestHandlerSessionContextUpdateAndRestore(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "context-session")

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "session_context_get"}); err != nil {
		t.Fatalf("write session_context_get request: %v", err)
	}
	initialEvent := readUntilType(t, conn, protocol.EventTypeSessionContextResult)
	initialContext, ok := initialEvent["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionContext payload, got %#v", initialEvent)
	}
	if len(initialContext) != 0 {
		t.Fatalf("expected empty initial sessionContext, got %#v", initialContext)
	}

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:       protocol.ClientEvent{Action: "session_context_update"},
		EnabledSkillNames: []string{"review", "analyze"},
		EnabledMemoryIDs:  []string{"m-test", "m-2"},
	}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	updatedEvent := readUntilType(t, conn, protocol.EventTypeSessionContextResult)
	updatedContext, ok := updatedEvent["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected updated sessionContext payload, got %#v", updatedEvent)
	}
	skillNames, ok := updatedContext["enabledSkillNames"].([]any)
	if !ok || len(skillNames) != 2 {
		t.Fatalf("expected enabledSkillNames, got %#v", updatedContext)
	}
	memoryIDs, ok := updatedContext["enabledMemoryIds"].([]any)
	if !ok || len(memoryIDs) != 2 {
		t.Fatalf("expected enabledMemoryIds, got %#v", updatedContext)
	}

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if len(record.Projection.SessionContext.EnabledSkillNames) != 2 || len(record.Projection.SessionContext.EnabledMemoryIDs) != 2 {
		t.Fatalf("unexpected persisted session context: %#v", record.Projection.SessionContext)
	}
	if record.Projection.SkillCatalogMeta.SyncState != data.CatalogSyncStateIdle || record.Projection.MemoryCatalogMeta.SyncState != data.CatalogSyncStateIdle {
		t.Fatalf("session context update should preserve catalog sync state defaults: %#v %#v", record.Projection.SkillCatalogMeta, record.Projection.MemoryCatalogMeta)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_load"}, SessionID: sessionID}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	historyContext, ok := history["sessionContext"].(map[string]any)
	if !ok {
		t.Fatalf("expected sessionContext in session history, got %#v", history)
	}
	historySkills, ok := historyContext["enabledSkillNames"].([]any)
	if !ok || len(historySkills) != 2 {
		t.Fatalf("expected restored enabledSkillNames, got %#v", historyContext)
	}
	historyMemory, ok := historyContext["enabledMemoryIds"].([]any)
	if !ok || len(historyMemory) != 2 {
		t.Fatalf("expected restored enabledMemoryIds, got %#v", historyContext)
	}
	reviewState := readUntilType(t, conn, protocol.EventTypeReviewState)
	if reviewState["type"] != protocol.EventTypeReviewState {
		t.Fatalf("expected review_state after session_load, got %#v", reviewState)
	}
}

func TestHandlerSessionLoadRestoresAgentStateFromProjection(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "restore-agent-session")

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	record.Projection = session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		CurrentStep: &data.SnapshotContext{
			Message: "继续处理中",
		},
		Controller: session.ControllerSnapshot{
			SessionID:       sessionID,
			State:           session.ControllerStateThinking,
			CurrentCommand:  "codex",
			LastStep:        "继续处理中",
			ResumeSession:   "thread-restore",
			ClaudeLifecycle: "active",
			ActiveMeta: protocol.RuntimeMeta{
				Command:         "codex",
				Engine:          "codex",
				CWD:             "/tmp/project",
				ResumeSessionID: "thread-restore",
				ClaudeLifecycle: "active",
			},
		},
		Runtime: data.SessionRuntime{
			ResumeSessionID: "thread-restore",
			Command:         "codex",
			Engine:          "codex",
			CWD:             "/tmp/project",
			ClaudeLifecycle: "active",
			Source:          "mobilevc",
		},
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
	})
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionID, record.Projection); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}

	history := readUntilSessionHistory(t, conn)
	if history["sessionId"] != sessionID {
		t.Fatalf("expected session history for %q, got %#v", sessionID, history)
	}
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	agentState := readUntilType(t, conn, protocol.EventTypeAgentState)
	requireAgentState(t, agentState, "WAIT_INPUT", true)
	if agentState["resumeSessionId"] != "thread-restore" {
		t.Fatalf("expected restored resumeSessionId, got %#v", agentState)
	}
	if agentState["claudeLifecycle"] != "waiting_input" {
		t.Fatalf("expected restored claudeLifecycle, got %#v", agentState)
	}
}

func TestHandlerSessionLoadDoesNotReplayRunningStateWithoutResume(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "restore-idle-session")

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	record.Projection = session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		CurrentStep: &data.SnapshotContext{
			Message: "旧运行态",
		},
		Controller: session.ControllerSnapshot{
			SessionID:      sessionID,
			State:          session.ControllerStateRunningTool,
			CurrentCommand: "bash -lc long-task",
			LastStep:       "旧运行态",
			ActiveMeta: protocol.RuntimeMeta{
				Command: "bash -lc long-task",
				CWD:     "/tmp/project",
			},
		},
		Runtime: data.SessionRuntime{
			Command: "bash -lc long-task",
			CWD:     "/tmp/project",
			Source:  "mobilevc",
		},
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
	})
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionID, record.Projection); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}

	_ = readUntilSessionHistory(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	event := readUntilType(t, conn, protocol.EventTypeSessionState)
	if event["state"] != string(session.StateActive) {
		t.Fatalf("expected active session state after history load, got %#v", event)
	}
	_ = conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var extra map[string]any
	if err := conn.ReadJSON(&extra); err == nil && extra["type"] == protocol.EventTypeAgentState {
		t.Fatalf("expected no restored running agent state without resume, got %#v", extra)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
}

func TestHandlerSessionResumeReattachesRunningRuntimeAfterDisconnect(t *testing.T) {
	runnerStub := newSwitchableStubRunner()
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return runnerStub }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "reattach-runtime")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.Close(); err != nil {
		t.Fatalf("close first connection: %v", err)
	}

	select {
	case <-runnerStub.closed:
		t.Fatal("expected running session to survive websocket disconnect")
	case <-time.After(150 * time.Millisecond):
	}

	runnerStub.Emit(protocol.ApplyRuntimeMeta(
		protocol.NewLogEvent("ignored", "background output before resume", "stdout"),
		protocol.RuntimeMeta{Engine: "claude"},
	))
	waitForPersistedSessionText(t, h.SessionStore, sessionID, "background output before resume")

	conn2 := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn2)
	if err := conn2.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "session_resume"},
		SessionID:           sessionID,
		LastSeenEventCursor: 0,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}

	history := readUntilSessionHistory(t, conn2)
	if history["sessionId"] != sessionID {
		t.Fatalf("expected session history for %s, got %#v", sessionID, history)
	}
	if history["runtimeAlive"] != true {
		t.Fatalf("expected runtimeAlive=true in resume history, got %#v", history)
	}
	if !historyEventContainsText(history, "background output before resume") {
		t.Fatalf("expected background output in resume history, got %#v", history)
	}
	notice := readUntilType(t, conn2, protocol.EventTypeSessionResumeNotice)
	if notice["msg"] != "background output before resume" {
		t.Fatalf("expected resume notice for background output, got %#v", notice)
	}
	result := readUntilType(t, conn2, protocol.EventTypeSessionResumeResult)
	if result["runtimeAlive"] != true {
		t.Fatalf("expected runtimeAlive=true in resume result, got %#v", result)
	}
	if result["reattaching"] != true {
		t.Fatalf("expected resume result to mark reattaching, got %#v", result)
	}

	runnerStub.Emit(protocol.NewLogEvent("ignored", "live output after reconnect", "stdout"))
	liveLogEvent := readUntilType(t, conn2, protocol.EventTypeLog)
	if liveLogEvent["msg"] != "live output after reconnect" {
		t.Fatalf("expected live log after reconnect, got %#v", liveLogEvent)
	}

	if entry := h.runtimeSessions.Ensure(sessionID); entry != nil {
		entry.service.Cleanup()
	}
}

func TestHandlerSessionDeltaReadsLiveProjectionAfterDisconnectWhilePersistenceIsBlocked(t *testing.T) {
	runnerStub := newSwitchableStubRunner()
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingProjectionSaveStore(tempStore)
	h.SessionStore = blockingStore
	h.NewPtyRunner = func() engine.Runner { return runnerStub }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "live-projection-delta")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.Close(); err != nil {
		t.Fatalf("close first connection: %v", err)
	}
	waitRuntimeSessionDetached(t, h, sessionID)

	runnerStub.Emit(protocol.ApplyRuntimeMeta(
		protocol.NewLogEvent(sessionID, "live output before disk save", "stdout"),
		protocol.RuntimeMeta{Engine: "claude"},
	))
	blockingStore.WaitSaveStarted(t)

	conn2 := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn2)
	if err := conn2.WriteJSON(protocol.SessionResumeRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_resume"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_resume request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn2)
	_ = readUntilType(t, conn2, protocol.EventTypeSessionResumeResult)

	if err := conn2.WriteJSON(protocol.SessionDeltaRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delta_get"},
		SessionID:   sessionID,
		Known: protocol.SessionDeltaKnown{
			LogEntryCount: 0,
		},
		Reason: "live_projection_regression",
	}); err != nil {
		t.Fatalf("write session_delta_get request: %v", err)
	}
	delta := readUntilType(t, conn2, protocol.EventTypeSessionDelta)
	if !deltaEventContainsText(delta, "live output before disk save") {
		t.Fatalf("expected live projection output in delta before save release, got %#v", delta)
	}

	blockingStore.ReleaseSave()
	blockingStore.WaitSaveDone(t)
	waitForPersistedSessionText(t, h.SessionStore, sessionID, "live output before disk save")

	if entry := h.runtimeSessions.Ensure(sessionID); entry != nil {
		entry.service.Cleanup()
	}
}

func TestRuntimeBufferedSinkDoesNotRunDetachedProcessorInline(t *testing.T) {
	runtimeSession := newRuntimeSession(session.NewService("session-buffered", session.Dependencies{}))
	processorStarted := make(chan struct{})
	releaseProcessor := make(chan struct{})
	processorDone := make(chan struct{})
	sink := runtimeSession.EnsureBufferedSinkWithProcessor(func(any) {
		close(processorStarted)
		<-releaseProcessor
		close(processorDone)
	})

	done := make(chan struct{})
	go func() {
		sink(protocol.NewLogEvent("session-buffered", "background output", "stdout"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("detached runtime sink blocked on processor")
	}
	select {
	case <-processorStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("detached runtime processor did not receive event")
	}
	close(releaseProcessor)
	select {
	case <-processorDone:
	case <-time.After(2 * time.Second):
		t.Fatal("detached runtime processor did not finish")
	}
	runtimeSession.shutdownSink()
}

func TestHandlerPublishesSessionUpdatedAcrossConnections(t *testing.T) {
	runnerStub := newSwitchableStubRunner()
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return runnerStub }

	connA := newTestConn(t, h)
	_ = connA.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, _ = readInitialEvents(t, connA)
	sessionID := createHistorySessionForHandlerTest(t, h, connA, "freshness-source")

	connB := newTestConn(t, h)
	_ = connB.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, _ = readInitialEvents(t, connB)
	otherSessionID := createHistorySessionForHandlerTest(t, h, connB, "freshness-observer")
	if otherSessionID == sessionID {
		t.Fatal("expected distinct observer session")
	}

	if err := connA.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerStub.WaitStarted(t)
	requireAgentState(t, readUntilType(t, connA, protocol.EventTypeAgentState), "THINKING", false)

	runnerStub.Emit(protocol.ApplyRuntimeMeta(
		protocol.NewLogEvent(sessionID, "freshness hint output", "stdout"),
		protocol.RuntimeMeta{Engine: "claude"},
	))
	updated := readUntilType(t, connB, protocol.EventTypeSessionUpdated)
	if updated["sessionId"] != sessionID {
		t.Fatalf("expected cross-connection session_updated for %q, got %#v", sessionID, updated)
	}
	if generation, _ := updated["generation"].(float64); generation <= 0 {
		t.Fatalf("expected positive generation, got %#v", updated)
	}

	if entry := h.runtimeSessions.Ensure(sessionID); entry != nil {
		entry.service.Cleanup()
	}
}

func TestHandlerProjectionPersistRetriesLatestSnapshotAfterFailure(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	store := newFailOnceProjectionSaveStore(tempStore)
	h.SessionStore = store
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "retry-projection")

	sink := h.runtimeEventSinkForSession(sessionID, func(any) {})
	sink(protocol.ApplyRuntimeMeta(
		protocol.NewLogEvent(sessionID, "retry eventually persists latest", "stdout"),
		protocol.RuntimeMeta{Engine: "claude"},
	))

	waitForPersistedSessionText(t, h.SessionStore, sessionID, "retry eventually persists latest")
	if saves := store.SaveCount(); saves < 2 {
		t.Fatalf("expected projection persistence retry, saves=%d", saves)
	}
}

func TestHandlerProjectionPersistRetryDoesNotDuplicateClaudeJSONL(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "ClaudeProject")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	store := newFailOnceProjectionSaveStore(tempStore)
	h.SessionStore = store
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "retry-jsonl")

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	record.Summary.Runtime.CWD = projectDir
	record.Summary.ClaudeSessionUUID = "retry-jsonl-claude"
	record.Projection.Runtime = data.SessionRuntime{
		ResumeSessionID: record.Summary.ClaudeSessionUUID,
		Command:         "claude --resume " + record.Summary.ClaudeSessionUUID,
		Engine:          "claude",
		CWD:             projectDir,
		Source:          "mobilevc",
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert mobilevc claude session: %v", err)
	}
	h.projectionWriter.updateMeta(h.SessionStore, record)

	sink := h.runtimeEventSinkForSession(sessionID, func(any) {})
	logEvent := protocol.NewLogEvent(sessionID, "retry jsonl assistant once", "stdout")
	logEvent.RuntimeMeta = protocol.RuntimeMeta{
		Engine: "claude",
		Source: "claude/assistant",
	}
	sink(logEvent)

	persisted := waitForPersistedSessionText(t, h.SessionStore, sessionID, "retry jsonl assistant once")
	if persisted.Summary.JSONLSyncEntryCount != 1 {
		t.Fatalf("expected jsonl sync count to persist after retry, got %d", persisted.Summary.JSONLSyncEntryCount)
	}
	projectsDir, err := claudesync.ClaudeProjectsDir()
	if err != nil {
		t.Fatalf("resolve claude projects dir: %v", err)
	}
	filePath := filepath.Join(projectsDir, claudesync.EncodeCWDToProjectDir(projectDir), record.Summary.ClaudeSessionUUID+".jsonl")
	content, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("read claude jsonl: %v", err)
	}
	if count := strings.Count(string(content), "retry jsonl assistant once"); count != 1 {
		t.Fatalf("expected retry to write claude jsonl text once, count=%d content=%s", count, string(content))
	}
	if saves := store.SaveCount(); saves < 2 {
		t.Fatalf("expected projection persistence retry, saves=%d", saves)
	}
}

func TestSaveProjectionWithOptionsRequiresOptionStoreWhenOptionsPresent(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	summary, err := tempStore.CreateSession(context.Background(), "projection-options")
	if err != nil {
		t.Fatalf("create session: %v", err)
	}
	wrapped := &projectionSaveOnlyStore{Store: tempStore}
	if _, err := saveProjectionWithOptions(wrapped, context.Background(), summary.ID, data.ProjectionSnapshot{
		LogEntries: []data.SnapshotLogEntry{{Kind: "markdown", Message: "plain save"}},
	}); err != nil {
		t.Fatalf("expected plain save without options to remain supported: %v", err)
	}
	_, err = saveProjectionWithOptions(wrapped, context.Background(), summary.ID, data.ProjectionSnapshot{
		LogEntries: []data.SnapshotLogEntry{{Kind: "markdown", Message: "option save"}},
	}, data.WithJSONLSyncEntryCount(1))
	if err == nil || !strings.Contains(err.Error(), "ProjectionOptionStore") {
		t.Fatalf("expected explicit ProjectionOptionStore error, got %v", err)
	}
}

func TestHandlerReviewStateGetReturnsProjectionGroups(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "review-state-session")

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	record.Projection = session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Diffs: []session.DiffContext{{
			ContextID:     "diff-1",
			Title:         "handler diff",
			Path:          "internal/ws/handler.go",
			Diff:          "@@ -1 +1 @@",
			Lang:          "go",
			PendingReview: true,
			ExecutionID:   "exec-1",
			GroupID:       "group-1",
			GroupTitle:    "修改组 1",
			ReviewStatus:  "pending",
		}},
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
	})
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionID, record.Projection); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.ClientEvent{Action: "review_state_get"}); err != nil {
		t.Fatalf("write review_state_get request: %v", err)
	}
	event := readUntilType(t, conn, protocol.EventTypeReviewState)
	groups, ok := event["groups"].([]any)
	if !ok || len(groups) != 1 {
		t.Fatalf("expected one review group, got %#v", event)
	}
	activeGroup, ok := event["activeGroup"].(map[string]any)
	if !ok {
		t.Fatalf("expected activeGroup payload, got %#v", event)
	}
	if activeGroup["id"] != "group-1" {
		t.Fatalf("expected activeGroup id group-1, got %#v", activeGroup)
	}
}

func TestHandlerExecFlow(t *testing.T) {
	execRunner := newStubRunner(
		protocol.NewLogEvent("ignored", "hello from runner", "stdout"),
		protocol.NewSessionStateEvent("ignored", "closed", "command finished"),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewExecRunner = func() engine.Runner { return execRunner }

	conn := newTestConn(t, h)
	first, second := readInitialEvents(t, conn)
	requireEventType(t, first, protocol.EventTypeSessionState)
	requireAgentState(t, second, "IDLE", false)
	_ = createHistorySessionForHandlerTest(t, h, conn, "exec-flow")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'ignored'",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	if event := readUntilType(t, conn, protocol.EventTypeLog); event["msg"] != "hello from runner" || event["stream"] != "stdout" {
		t.Fatalf("expected stdout log event, got %#v", event)
	}
	if event := readUntilType(t, conn, protocol.EventTypeSessionState); event["state"] != "closed" {
		t.Fatalf("expected closed session event, got %#v", event)
	}
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "IDLE", false)
}

func TestHandlerExecPreservesCodexSandboxMode(t *testing.T) {
	ptyRunner := newSwitchableStubRunner()

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "exec-codex-sandbox")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "codex",
		Mode:           "pty",
		PermissionMode: "bypassPermissions",
		RuntimeMeta: protocol.RuntimeMeta{
			Engine:           "codex",
			CodexSandboxMode: "danger-full-access",
		},
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if got := ptyRunner.req.RuntimeMeta.CodexSandboxMode; got != "danger-full-access" {
		t.Fatalf("codex sandbox mode: %q", got)
	}
	if got := ptyRunner.req.PermissionMode; got != "bypassPermissions" {
		t.Fatalf("permission mode: %q", got)
	}
}

func TestHandlerRuntimeProcessListReturnsActiveTree(t *testing.T) {
	execRunner := newHoldingStubRunner()
	execRunner.processRef = engine.ProcessRef{
		RootPID:     os.Getpid(),
		ExecutionID: "exec-live-1",
		Command:     "codex exec",
		CWD:         "/tmp/mobilevc",
		Source:      "codex",
	}

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewExecRunner = func() engine.Runner { return execRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "runtime-process-list")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'keep running'",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	execRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.RuntimeProcessListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_process_list"},
	}); err != nil {
		t.Fatalf("write runtime_process_list request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeRuntimeProcessList)
	if got := int(event["rootPid"].(float64)); got != os.Getpid() {
		t.Fatalf("expected root pid %d, got %#v", os.Getpid(), event["rootPid"])
	}
	items, ok := event["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected runtime process items, got %#v", event)
	}
	root, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first runtime process item, got %#v", items[0])
	}
	if got := int(root["pid"].(float64)); got != os.Getpid() {
		t.Fatalf("expected root item pid %d, got %#v", os.Getpid(), root["pid"])
	}
	if root["executionId"] != "exec-live-1" {
		t.Fatalf("expected executionId exec-live-1, got %#v", root["executionId"])
	}
	if root["cwd"] != "/tmp/mobilevc" {
		t.Fatalf("expected cwd /tmp/mobilevc, got %#v", root["cwd"])
	}
	if root["source"] != "codex" {
		t.Fatalf("expected source codex, got %#v", root["source"])
	}
	if root["root"] != true {
		t.Fatalf("expected root marker, got %#v", root["root"])
	}
	if root["logAvailable"] != true {
		t.Fatalf("expected logAvailable=true, got %#v", root["logAvailable"])
	}
}

func TestHandlerRuntimeProcessLogReturnsCapturedExecutionOutput(t *testing.T) {
	execRunner := newHoldingStubRunner()
	execRunner.processRef = engine.ProcessRef{
		RootPID:     os.Getpid(),
		ExecutionID: "exec-live-2",
		Command:     "codex exec",
		CWD:         "/tmp/mobilevc",
		Source:      "codex",
	}

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewExecRunner = func() engine.Runner { return execRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "runtime-process-log")

	snapshot := session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		TerminalExecutions: []data.TerminalExecution{{
			ExecutionID: "exec-live-2",
			Command:     "codex exec",
			CWD:         "/tmp/mobilevc",
			Stdout:      "captured stdout",
			Stderr:      "captured stderr",
		}},
		RawTerminalByStream: map[string]string{
			"stdout": "fallback stdout",
			"stderr": "fallback stderr",
		},
	})
	if _, err := tempStore.SaveProjection(context.Background(), sessionID, snapshot); err != nil {
		t.Fatalf("save projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'keep running'",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	execRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.RuntimeProcessLogRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_process_log_get"},
		PID:         os.Getpid(),
	}); err != nil {
		t.Fatalf("write runtime_process_log_get request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeRuntimeProcessLog)
	if got := int(event["pid"].(float64)); got != os.Getpid() {
		t.Fatalf("expected pid %d, got %#v", os.Getpid(), event["pid"])
	}
	if event["executionId"] != "exec-live-2" {
		t.Fatalf("expected executionId exec-live-2, got %#v", event["executionId"])
	}
	if event["stdout"] != "captured stdout" {
		t.Fatalf("expected captured stdout, got %#v", event["stdout"])
	}
	if event["stderr"] != "captured stderr" {
		t.Fatalf("expected captured stderr, got %#v", event["stderr"])
	}
}

func TestHandlerRuntimeProcessListAndLogUseGeneratedExecutionIDForPty(t *testing.T) {
	ptyRunner := newHoldingStubRunner()
	ptyRunner.processRef = engine.ProcessRef{
		RootPID: os.Getpid(),
		Command: "bash -lc 'sleep 10'",
		CWD:     "/tmp/mobilevc",
		Source:  "pty",
	}

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "runtime-process-pty")

	ptyRunner.mu.Lock()
	ptyRunner.events = []any{
		protocol.NewLogEvent(sessionID, "pty stdout", "stdout"),
	}
	ptyRunner.mu.Unlock()

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "bash -lc 'sleep 10'",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write pty exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	ptyRunner.WaitStarted(t)

	ptyRunner.mu.Lock()
	executionID := strings.TrimSpace(ptyRunner.lastReq.RuntimeMeta.ExecutionID)
	ptyRunner.mu.Unlock()
	if executionID == "" {
		t.Fatal("expected generated execution id on pty request")
	}

	if err := conn.WriteJSON(protocol.RuntimeProcessListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_process_list"},
	}); err != nil {
		t.Fatalf("write runtime_process_list request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeRuntimeProcessList)
	items, ok := listEvent["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected runtime process items, got %#v", listEvent)
	}
	root, ok := items[0].(map[string]any)
	if !ok {
		t.Fatalf("expected first runtime process item, got %#v", items[0])
	}
	if root["executionId"] != executionID {
		t.Fatalf("expected generated executionId %q, got %#v", executionID, root["executionId"])
	}
	if root["logAvailable"] != true {
		t.Fatalf("expected logAvailable=true, got %#v", root["logAvailable"])
	}

	if err := conn.WriteJSON(protocol.RuntimeProcessLogRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_process_log_get"},
		PID:         os.Getpid(),
	}); err != nil {
		t.Fatalf("write runtime_process_log_get request: %v", err)
	}

	logEvent := readUntilType(t, conn, protocol.EventTypeRuntimeProcessLog)
	if logEvent["executionId"] != executionID {
		t.Fatalf("expected generated executionId %q on runtime log, got %#v", executionID, logEvent["executionId"])
	}
	if logEvent["stdout"] != "pty stdout" {
		t.Fatalf("expected pty stdout, got %#v", logEvent["stdout"])
	}
}

func TestProjectionBuildsReviewGroupsFromDiffs(t *testing.T) {
	snapshot := data.ProjectionSnapshot{RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""}}
	event := protocol.ApplyRuntimeMeta(
		protocol.NewFileDiffEvent("session-1", "internal/ws/handler.go", "handler diff", "diff --git a/internal/ws/handler.go b/internal/ws/handler.go", "go"),
		protocol.RuntimeMeta{ContextID: "diff-1", ExecutionID: "exec-1", GroupID: "group-1", GroupTitle: "修改组 1"},
	)
	snapshot, changed := session.ApplyEventToProjection(snapshot, event)
	if !changed {
		t.Fatal("expected file_diff to change projection")
	}
	if len(snapshot.ReviewGroups) != 1 {
		t.Fatalf("expected one review group, got %#v", snapshot.ReviewGroups)
	}
	group := snapshot.ReviewGroups[0]
	if group.ID != "group-1" {
		t.Fatalf("expected group id group-1, got %#v", group)
	}
	if group.PendingCount != 1 || !group.PendingReview {
		t.Fatalf("expected pending review group, got %#v", group)
	}
	if snapshot.ActiveReviewGroup == nil || snapshot.ActiveReviewGroup.ID != "group-1" {
		t.Fatalf("expected active review group to be restored, got %#v", snapshot.ActiveReviewGroup)
	}
}

func TestProjectionAutoAcceptsReviewGroupsInAutoPermissionMode(t *testing.T) {
	snapshot := data.ProjectionSnapshot{RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""}}
	event := protocol.ApplyRuntimeMeta(
		protocol.NewFileDiffEvent("session-1", "internal/ws/handler.go", "handler diff", "diff --git a/internal/ws/handler.go b/internal/ws/handler.go", "go"),
		protocol.RuntimeMeta{ContextID: "diff-auto-1", ExecutionID: "exec-auto-1", GroupID: "group-auto-1", PermissionMode: "auto"},
	)
	snapshot, changed := session.ApplyEventToProjection(snapshot, event)
	if !changed {
		t.Fatal("expected file_diff to change projection")
	}
	if len(snapshot.ReviewGroups) != 1 {
		t.Fatalf("expected one review group, got %#v", snapshot.ReviewGroups)
	}
	group := snapshot.ReviewGroups[0]
	if group.PendingReview || group.PendingCount != 0 || group.ReviewStatus != "accepted" {
		t.Fatalf("expected auto-accepted review group, got %#v", group)
	}
	if len(group.Files) != 1 || group.Files[0].PendingReview || group.Files[0].ReviewStatus != "accepted" {
		t.Fatalf("expected auto-accepted review file, got %#v", group.Files)
	}
	if snapshot.CurrentDiff == nil || snapshot.CurrentDiff.PendingReview || snapshot.CurrentDiff.ReviewStatus != "accepted" {
		t.Fatalf("expected current diff to be auto-accepted, got %#v", snapshot.CurrentDiff)
	}
}

func TestApplyReviewDecisionToProjectionUpdatesReviewState(t *testing.T) {
	snapshot := session.NormalizeProjectionSnapshot(data.ProjectionSnapshot{
		Diffs: []session.DiffContext{{
			ContextID:     "diff-1",
			Title:         "handler diff",
			Path:          "internal/ws/handler.go",
			Diff:          "@@ -1 +1 @@",
			Lang:          "go",
			PendingReview: true,
			ExecutionID:   "exec-1",
			GroupID:       "group-1",
			GroupTitle:    "修改组 1",
			ReviewStatus:  "pending",
		}},
	})
	snapshot = session.ApplyReviewDecisionToProjection(snapshot, protocol.ReviewDecisionRequestEvent{
		Decision:    "accept",
		ContextID:   "diff-1",
		TargetPath:  "internal/ws/handler.go",
		ExecutionID: "exec-1",
		GroupID:     "group-1",
	}, "accept", session.DiffContext{})
	if len(snapshot.ReviewGroups) != 1 {
		t.Fatalf("expected one review group, got %#v", snapshot.ReviewGroups)
	}
	group := snapshot.ReviewGroups[0]
	if group.ReviewStatus != "accepted" {
		t.Fatalf("expected accepted group, got %#v", group)
	}
	if group.PendingReview || group.PendingCount != 0 {
		t.Fatalf("expected no pending reviews after accept, got %#v", group)
	}
	if len(group.Files) != 1 || group.Files[0].ReviewStatus != "accepted" {
		t.Fatalf("expected accepted review file, got %#v", group.Files)
	}
	if snapshot.CurrentDiff == nil || snapshot.CurrentDiff.ReviewStatus != "accepted" || snapshot.CurrentDiff.PendingReview {
		t.Fatalf("expected current diff to be marked accepted, got %#v", snapshot.CurrentDiff)
	}
}

func TestWithRuntimeSnapshotPrefersLiveLifecycleOverStaleStarting(t *testing.T) {
	snapshot := session.WithRuntimeSnapshot(data.ProjectionSnapshot{
		Controller: session.ControllerSnapshot{
			SessionID:       "s1",
			CurrentCommand:  "claude",
			ResumeSession:   "resume-1",
			ClaudeLifecycle: "starting",
			ActiveMeta:      protocol.RuntimeMeta{Command: "claude", ResumeSessionID: "resume-1", ClaudeLifecycle: "starting"},
		},
		Runtime: data.SessionRuntime{ResumeSessionID: "resume-1", Command: "claude", ClaudeLifecycle: "starting"},
	}, nil)
	if snapshot.Runtime.ClaudeLifecycle != "resumable" {
		t.Fatalf("expected resumable lifecycle, got %#v", snapshot.Runtime)
	}
}

func TestWithRuntimeSnapshotPreservesCodexResumeMetadataAfterCommandFinishes(t *testing.T) {
	stub := newStubRunner()
	service := session.NewService("session-1", session.Dependencies{
		NewPtyRunner: func() engine.Runner { return stub },
	})
	err := service.Execute(context.Background(), "session-1", session.ExecuteRequest{
		Command: "codex",
		Mode:    engine.ModePTY,
		RuntimeMeta: protocol.RuntimeMeta{
			Command:         "codex",
			Engine:          "codex",
			CWD:             "/tmp/project",
			ResumeSessionID: "thread-1",
		},
	}, func(any) {})
	if err != nil {
		t.Fatalf("execute codex session: %v", err)
	}
	stub.WaitStarted(t)
	deadline := time.Now().Add(2 * time.Second)
	for service.IsRunning() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if service.IsRunning() {
		t.Fatal("expected runtime service to finish")
	}

	snapshot := session.WithRuntimeSnapshot(data.ProjectionSnapshot{
		Runtime: data.SessionRuntime{
			Command: "codex",
			Engine:  "codex",
			CWD:     "/tmp/project",
			Source:  "mobilevc",
		},
	}, service)
	if snapshot.Runtime.ResumeSessionID != "thread-1" {
		t.Fatalf("expected resume session id to be preserved, got %#v", snapshot.Runtime)
	}
	if snapshot.Runtime.Command != "codex" || snapshot.Runtime.Engine != "codex" || snapshot.Runtime.CWD != "/tmp/project" {
		t.Fatalf("expected codex runtime metadata to survive cleanup, got %#v", snapshot.Runtime)
	}
	if snapshot.Runtime.ClaudeLifecycle != "resumable" {
		t.Fatalf("expected resumable lifecycle after command finish, got %#v", snapshot.Runtime)
	}
}

func TestSessionHistoryNormalizesStaleStartingToResumable(t *testing.T) {
	history := session.SessionHistoryEventFromRecord(data.SessionRecord{
		Summary: data.SessionSummary{ID: "session-1", Title: "history"},
		Projection: data.ProjectionSnapshot{
			Controller: session.ControllerSnapshot{
				SessionID:       "session-1",
				CurrentCommand:  "claude",
				ResumeSession:   "resume-1",
				ClaudeLifecycle: "starting",
				ActiveMeta:      protocol.RuntimeMeta{Command: "claude", ResumeSessionID: "resume-1", ClaudeLifecycle: "starting"},
			},
			Runtime:             data.SessionRuntime{ResumeSessionID: "resume-1", Command: "claude", ClaudeLifecycle: "starting"},
			RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		},
	}, false)
	if history.ResumeRuntimeMeta.ClaudeLifecycle != "resumable" {
		t.Fatalf("expected resumable lifecycle in history, got %#v", history.ResumeRuntimeMeta)
	}
}

func TestHandlerSessionLoadCollapsesAdjacentDuplicateLogEntries(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "duplicate-history")
	if _, err := tempStore.SaveProjection(context.Background(), sessionID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		LogEntries: []data.SnapshotLogEntry{
			{Kind: "user", Message: "重复输入", Timestamp: "2026-05-27T08:01:59Z"},
			{Kind: "user", Message: "重复输入", Timestamp: "2026-05-27T08:01:59Z"},
			{Kind: "markdown", Message: "重复回复", Timestamp: "2026-05-27T08:02:16Z"},
			{Kind: "markdown", Message: "重复回复", Timestamp: "2026-05-27T08:02:16Z"},
		},
		Runtime: data.SessionRuntime{Source: "mobilevc"},
	}); err != nil {
		t.Fatalf("save duplicate projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	entries, ok := history["logEntries"].([]any)
	if !ok {
		t.Fatalf("expected logEntries payload, got %#v", history)
	}
	if len(entries) != 2 {
		t.Fatalf("expected duplicate history entries to collapse, got %#v", entries)
	}
	first, _ := entries[0].(map[string]any)
	second, _ := entries[1].(map[string]any)
	if first["message"] != "重复输入" || second["message"] != "重复回复" {
		t.Fatalf("unexpected deduped history entries: %#v", entries)
	}
	closeConnAndCleanupRuntime(t, conn, h)
}

func TestProjectionHistoryIncludesTerminalExecutions(t *testing.T) {
	executionID := "exec-test-1"
	snapshot := data.ProjectionSnapshot{RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""}}

	var changed bool
	snapshot, changed = session.ApplyEventToProjection(snapshot, protocol.ApplyRuntimeMeta(protocol.NewExecutionLogEvent("session-1", executionID, "echo hello", "", "started", nil), protocol.RuntimeMeta{Command: "echo hello", CWD: "/tmp"}))
	if !changed {
		t.Fatal("expected started event to change projection")
	}
	snapshot, changed = session.ApplyEventToProjection(snapshot, protocol.ApplyRuntimeMeta(protocol.NewExecutionLogEvent("session-1", executionID, "hello from runner", "stdout", "stdout", nil), protocol.RuntimeMeta{Command: "echo hello", CWD: "/tmp"}))
	if !changed {
		t.Fatal("expected stdout event to change projection")
	}
	snapshot, changed = session.ApplyEventToProjection(snapshot, protocol.ApplyRuntimeMeta(protocol.NewExecutionLogEvent("session-1", executionID, "second stdout", "stdout", "stdout", nil), protocol.RuntimeMeta{Command: "echo hello", CWD: "/tmp"}))
	if !changed {
		t.Fatal("expected second stdout event to change projection")
	}
	snapshot, changed = session.ApplyEventToProjection(snapshot, protocol.ApplyRuntimeMeta(protocol.NewExecutionLogEvent("session-1", executionID, "stderr from runner", "stderr", "stderr", nil), protocol.RuntimeMeta{Command: "echo hello", CWD: "/tmp"}))
	if !changed {
		t.Fatal("expected stderr event to change projection")
	}
	snapshot, changed = session.ApplyEventToProjection(snapshot, protocol.ApplyRuntimeMeta(protocol.NewExecutionLogEvent("session-1", executionID, "", "", "finished", intPtr(0)), protocol.RuntimeMeta{Command: "echo hello", CWD: "/tmp"}))
	if !changed {
		t.Fatal("expected finished event to change projection")
	}

	if got := snapshot.RawTerminalByStream["stdout"]; got != "hello from runner\nsecond stdout" {
		t.Fatalf("expected aggregated stdout stream, got %q", got)
	}
	if got := snapshot.RawTerminalByStream["stderr"]; got != "stderr from runner" {
		t.Fatalf("expected aggregated stderr stream, got %q", got)
	}
	if len(snapshot.TerminalExecutions) != 1 {
		t.Fatalf("expected one terminal execution in snapshot, got %#v", snapshot.TerminalExecutions)
	}
	item := snapshot.TerminalExecutions[0]
	if item.ExecutionID != executionID {
		t.Fatalf("expected execution id %q, got %#v", executionID, item)
	}
	if item.Command != "echo hello" {
		t.Fatalf("expected command echo hello, got %#v", item)
	}
	if item.CWD != "/tmp" {
		t.Fatalf("expected cwd /tmp, got %#v", item)
	}
	if item.Stdout != "hello from runner\nsecond stdout" {
		t.Fatalf("expected stdout aggregation, got %#v", item)
	}
	if item.Stderr != "stderr from runner" {
		t.Fatalf("expected stderr aggregation, got %#v", item)
	}
	if item.ExitCode == nil || *item.ExitCode != 0 {
		t.Fatalf("expected exitCode 0, got %#v", item)
	}

	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "session-1", Title: "exec-history"},
		Projection: data.ProjectionSnapshot{
			RawTerminalByStream: snapshot.RawTerminalByStream,
			TerminalExecutions:  snapshot.TerminalExecutions,
			ReviewGroups: []session.ReviewGroup{{
				ID:            "group-1",
				Title:         "修改组 1",
				ExecutionID:   executionID,
				PendingReview: true,
				ReviewStatus:  "pending",
				CurrentFileID: "diff-1",
				CurrentPath:   "internal/ws/handler.go",
				PendingCount:  1,
				Files: []session.ReviewFile{{
					ContextID:     "diff-1",
					Title:         "handler diff",
					Path:          "internal/ws/handler.go",
					Diff:          "@@ -1 +1 @@",
					Lang:          "go",
					PendingReview: true,
					ExecutionID:   executionID,
					ReviewStatus:  "pending",
				}},
			}},
			ActiveReviewGroup: &session.ReviewGroup{ID: "group-1", Title: "修改组 1", ExecutionID: executionID, PendingReview: true},
		},
	}
	history := session.SessionHistoryEventFromRecord(record, false)
	if len(history.ReviewGroups) != 1 {
		t.Fatalf("expected one review group in history, got %#v", history.ReviewGroups)
	}
	if history.ActiveReviewGroup == nil || history.ActiveReviewGroup.ID != "group-1" {
		t.Fatalf("expected active review group in history, got %#v", history.ActiveReviewGroup)
	}
	if history.RawTerminalByStream["stdout"] != "hello from runner\nsecond stdout" {
		t.Fatalf("expected history stdout stream, got %#v", history.RawTerminalByStream)
	}
	if history.RawTerminalByStream["stderr"] != "stderr from runner" {
		t.Fatalf("expected history stderr stream, got %#v", history.RawTerminalByStream)
	}
	if len(history.TerminalExecutions) != 1 {
		t.Fatalf("expected one terminal execution in history, got %#v", history.TerminalExecutions)
	}
	historyItem := history.TerminalExecutions[0]
	if historyItem.ExecutionID != executionID {
		t.Fatalf("expected history execution id %q, got %#v", executionID, historyItem)
	}
	if historyItem.Command != "echo hello" || historyItem.CWD != "/tmp" || historyItem.Stdout != "hello from runner\nsecond stdout" || historyItem.Stderr != "stderr from runner" {
		t.Fatalf("unexpected history execution payload: %#v", historyItem)
	}
	if historyItem.ExitCode == nil || *historyItem.ExitCode != 0 {
		t.Fatalf("expected history exitCode 0, got %#v", historyItem)
	}
}

func intPtr(v int) *int {
	return &v
}

func TestHandlerRegisterPushTokenPersistsExplicitSessionWithoutSuccessLog(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "push-register-explicit")
	if err := conn.WriteJSON(protocol.RegisterPushTokenRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "register_push_token"},
		SessionID:   sessionID,
		Token:       "device-token-explicit",
	}); err != nil {
		t.Fatalf("write register_push_token request: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		token, platform, err := tempStore.GetPushToken(context.Background(), sessionID)
		if err == nil && token != "" {
			if token != "device-token-explicit" || platform != "ios" {
				t.Fatalf("unexpected push token payload: token=%q platform=%q", token, platform)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("get push token before deadline: token=%q platform=%q err=%v", token, platform, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected no success log event after register_push_token")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("expected read timeout, got %v", err)
		}
	}
	_ = conn.SetReadDeadline(time.Time{})
}

func TestHandlerRegisterPushTokenFallsBackToSelectedSessionWithoutSuccessLog(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "push-register-selected")
	if err := conn.WriteJSON(protocol.RegisterPushTokenRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "register_push_token"},
		Token:       "device-token-selected",
	}); err != nil {
		t.Fatalf("write register_push_token request: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		token, platform, err := tempStore.GetPushToken(context.Background(), sessionID)
		if err == nil && token != "" {
			if token != "device-token-selected" || platform != "ios" {
				t.Fatalf("unexpected push token payload: token=%q platform=%q", token, platform)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("get push token before deadline: token=%q platform=%q err=%v", token, platform, err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("expected no success log event after register_push_token")
	} else {
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("expected read timeout, got %v", err)
		}
	}
	_ = conn.SetReadDeadline(time.Time{})
}

func TestHandlerRegisterPushTokenRejectsMissingSession(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.RegisterPushTokenRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "register_push_token"},
		Token:       "device-token-missing-session",
	}); err != nil {
		t.Fatalf("write register_push_token request: %v", err)
	}

	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if errorEvent["msg"] != "sessionId is required" {
		t.Fatalf("expected missing session error, got %#v", errorEvent)
	}

	data, err := os.ReadFile(filepath.Join(tempStore.BaseDir(), "push_tokens.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read push_tokens.json: %v", err)
	}
	if strings.Contains(string(data), "\"\":") {
		t.Fatalf("expected no empty session key, got %s", string(data))
	}
}

func TestHandlerRegisterPushTokenRejectsEmptyToken(t *testing.T) {
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new file store: %v", err)
	}
	h := NewHandler("test", tempStore)
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "push-register-empty-token")
	if err := conn.WriteJSON(protocol.RegisterPushTokenRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "register_push_token"},
		SessionID:   sessionID,
		Token:       "   ",
	}); err != nil {
		t.Fatalf("write register_push_token request: %v", err)
	}

	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if errorEvent["msg"] != "token is required" {
		t.Fatalf("expected empty token error, got %#v", errorEvent)
	}

	data, err := os.ReadFile(filepath.Join(tempStore.BaseDir(), "push_tokens.json"))
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		t.Fatalf("read push_tokens.json: %v", err)
	}
	if strings.Contains(string(data), sessionID) {
		t.Fatalf("expected no persisted token for %q, got %s", sessionID, string(data))
	}
}

func TestHandlerPtyInputFlowSendsPushWhenTokenRegistered(t *testing.T) {
	ptyRunner := newHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Proceed? [y/N]", []string{"y", "n"}),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	mockPush := push.NewMockAPNsService()
	h.SessionStore = tempStore
	h.PushService = mockPush
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "pty-input-push")
	if err := tempStore.SavePushToken(context.Background(), sessionID, "device-token-1", "ios"); err != nil {
		t.Fatalf("save push token: %v", err)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'ignored'",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	defer closeConnAndCleanupRuntime(t, conn, h, ptyRunner)

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(mockPush.SentNotifications) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(mockPush.SentNotifications) != 1 {
		t.Fatalf("expected 1 push notification, got %#v", mockPush.SentNotifications)
	}
	if mockPush.SentNotifications[0].Token != "device-token-1" {
		t.Fatalf("unexpected push token: %#v", mockPush.SentNotifications[0])
	}
	if mockPush.SentNotifications[0].Body != "Proceed? [y/N]" {
		t.Fatalf("unexpected push body: %#v", mockPush.SentNotifications[0])
	}
	if mockPush.SentNotifications[0].Data["sessionId"] != sessionID {
		t.Fatalf("unexpected push sessionId: %#v", mockPush.SentNotifications[0])
	}
	if mockPush.SentNotifications[0].Data["type"] != "action_needed" {
		t.Fatalf("unexpected push type: %#v", mockPush.SentNotifications[0])
	}
	if mockPush.SentNotifications[0].Data["eventType"] != protocol.EventTypePromptRequest {
		t.Fatalf("unexpected push eventType: %#v", mockPush.SentNotifications[0])
	}
	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "y\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		if string(payload) != "y\n" {
			t.Fatalf("expected y\\n payload, got %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
}

func TestHandlerPtyInputFlow(t *testing.T) {
	ptyRunner := newHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Proceed? [y/N]", []string{"y", "n"}),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "pty-input-flow")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'ignored'",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "y\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		if string(payload) != "y\n" {
			t.Fatalf("expected y\\n payload, got %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
}

func TestHandlerEmitsAgentStateForToolEventsAndFinish(t *testing.T) {
	ptyRunner := newStubRunner(
		protocol.NewStepUpdateEvent("ignored", "Reading internal/ws/handler.go", "running", "internal/ws/handler.go", "reading", "Reading internal/ws/handler.go"),
		protocol.NewFileDiffEvent("ignored", "internal/ws/handler.go", "Updating internal/ws/handler.go", "diff --git a/internal/ws/handler.go b/internal/ws/handler.go", "go"),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "tool-events")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypeStepUpdate)
	toolEvent := readUntilType(t, conn, protocol.EventTypeAgentState)
	requireAgentState(t, toolEvent, "RUNNING_TOOL", false)
	if toolEvent["step"] != "Reading internal/ws/handler.go" {
		t.Fatalf("expected step in agent state, got %#v", toolEvent)
	}
	_ = readUntilType(t, conn, protocol.EventTypeFileDiff)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "RUNNING_TOOL", false)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "IDLE", false)
}

func TestHandlerClaudeSessionReturnsToWaitInputAfterResult(t *testing.T) {
	ptyRunner := newHoldingStubRunner(
		protocol.ApplyRuntimeMeta(
			protocol.NewPromptRequestEvent("ignored", "Claude 会话已就绪，可继续输入", nil),
			protocol.RuntimeMeta{ResumeSessionID: "resume-chat-456"},
		),
	)
	ptyRunner.claudeSessionID = "resume-chat-456"

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "claude-second-turn-wait-input")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "你是谁\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		if got := string(payload); got != "你是谁\n" {
			t.Fatalf("unexpected input payload: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive follow-up input payload")
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	ptyRunner.Emit(protocol.ApplyRuntimeMeta(
		protocol.NewLogEvent("ignored", "我是 Claude", "stdout"),
		protocol.RuntimeMeta{ResumeSessionID: "resume-chat-456"},
	))
	ptyRunner.Emit(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "等待输入", nil),
		protocol.RuntimeMeta{ResumeSessionID: "resume-chat-456", ClaudeLifecycle: "waiting_input"},
	))

	promptEvent := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if promptEvent["resumeSessionId"] != "resume-chat-456" {
		t.Fatalf("expected ready prompt resumeSessionId, got %#v", promptEvent)
	}
	agentState := readUntilType(t, conn, protocol.EventTypeAgentState)
	requireAgentState(t, agentState, "WAIT_INPUT", true)
	if agentState["claudeLifecycle"] != "waiting_input" {
		t.Fatalf("expected waiting_input lifecycle after second turn, got %#v", agentState)
	}
	if agentState["resumeSessionId"] != "resume-chat-456" {
		t.Fatalf("expected resumeSessionId after second turn, got %#v", agentState)
	}
}

func TestHandlerClaudeSessionStartsInWaitInput(t *testing.T) {
	ptyRunner := newHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Claude 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "claude-wait-input")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)
}

func TestHandlerInputInjectsEnabledSkillsIntoAIConversation(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Codex 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "codex-enabled-skills")

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:       protocol.ClientEvent{Action: "session_context_update"},
		EnabledSkillNames: []string{"review"},
	}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionContextResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "当前开启了哪些 skill？\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if !strings.Contains(got, "[MobileVC Enabled Skills]") {
			t.Fatalf("expected enabled skills prefix, got %q", got)
		}
		if !strings.Contains(got, "- review") {
			t.Fatalf("expected review skill in injected payload, got %q", got)
		}
		if !strings.Contains(got, "当前开启了哪些 skill？") {
			t.Fatalf("expected original user input in injected payload, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerInputInjectsEnabledMemoryIntoAIConversation(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Codex 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	if err := tempStore.SaveMemoryCatalogSnapshot(context.Background(), data.MemoryCatalogSnapshot{
		Meta: data.CatalogMetadata{
			Domain:    data.CatalogDomainMemory,
			SyncState: data.CatalogSyncStateSynced,
		},
		Items: []data.MemoryItem{{
			ID:        "mem-flutter",
			Title:     "Flutter Index",
			Content:   "session controller manages session_context_update and current chat state",
			SyncState: data.CatalogSyncStateSynced,
		}},
	}); err != nil {
		t.Fatalf("save memory snapshot: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "codex-enabled-memory")

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:      protocol.ClientEvent{Action: "session_context_update"},
		EnabledMemoryIDs: []string{"mem-flutter"},
	}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionContextResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "当前启用了哪些 memory？\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if !strings.Contains(got, "[MobileVC Memory]") {
			t.Fatalf("expected memory prefix, got %q", got)
		}
		if !strings.Contains(got, "Flutter Index") {
			t.Fatalf("expected memory title in injected payload, got %q", got)
		}
		if !strings.Contains(got, "session controller manages session_context_update") {
			t.Fatalf("expected memory content in injected payload, got %q", got)
		}
		if !strings.Contains(got, "当前启用了哪些 memory？") {
			t.Fatalf("expected original user input in injected payload, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}

	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerInputDoesNotInjectEmptySessionContextBeforeUserConfiguresIt(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Codex 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "codex-unconfigured-empty-session-context")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "正常聊一句，不问 skill 或 memory。\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if strings.Contains(got, "[MobileVC Enabled Skills]") {
			t.Fatalf("did not expect enabled skills prefix before configuration, got %q", got)
		}
		if strings.Contains(got, "[MobileVC Memory]") {
			t.Fatalf("did not expect memory prefix before configuration, got %q", got)
		}
		if !strings.Contains(got, "正常聊一句，不问 skill 或 memory。") {
			t.Fatalf("expected original user input in payload, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}

	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerInputInjectsEnabledSkillAndMemoryIntoAIConversation(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Codex 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	if err := tempStore.SaveMemoryCatalogSnapshot(context.Background(), data.MemoryCatalogSnapshot{
		Meta: data.CatalogMetadata{
			Domain:    data.CatalogDomainMemory,
			SyncState: data.CatalogSyncStateSynced,
		},
		Items: []data.MemoryItem{{
			ID:        "mem-flutter",
			Title:     "Flutter Index",
			Content:   "session controller manages session_context_update and current chat state",
			SyncState: data.CatalogSyncStateSynced,
		}},
	}); err != nil {
		t.Fatalf("save memory snapshot: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "codex-enabled-skill-memory")

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:       protocol.ClientEvent{Action: "session_context_update"},
		EnabledSkillNames: []string{"review"},
		EnabledMemoryIDs:  []string{"mem-flutter"},
	}); err != nil {
		t.Fatalf("write session_context_update request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionContextResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "当前启用了哪些 skill 和 memory？\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if !strings.Contains(got, "[MobileVC Enabled Skills]") {
			t.Fatalf("expected enabled skills prefix, got %q", got)
		}
		if !strings.Contains(got, "- review") {
			t.Fatalf("expected review skill in injected payload, got %q", got)
		}
		if !strings.Contains(got, "[MobileVC Memory]") {
			t.Fatalf("expected memory prefix, got %q", got)
		}
		if !strings.Contains(got, "Flutter Index") {
			t.Fatalf("expected memory title in injected payload, got %q", got)
		}
		if !strings.Contains(got, "当前启用了哪些 skill 和 memory？") {
			t.Fatalf("expected original user input in injected payload, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}
}

func TestHandlerInputInjectsEmptySessionContextToOverrideEarlierSkillAndMemoryState(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "Codex 会话已就绪，可继续输入", nil),
	)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	if err := tempStore.SaveMemoryCatalogSnapshot(context.Background(), data.MemoryCatalogSnapshot{
		Meta: data.CatalogMetadata{
			Domain:    data.CatalogDomainMemory,
			SyncState: data.CatalogSyncStateSynced,
		},
		Items: []data.MemoryItem{{
			ID:        "mem-flutter",
			Title:     "Flutter Index",
			Content:   "session controller manages session_context_update and current chat state",
			SyncState: data.CatalogSyncStateSynced,
		}},
	}); err != nil {
		t.Fatalf("save memory snapshot: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "codex-cleared-skill-memory")

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent:       protocol.ClientEvent{Action: "session_context_update"},
		EnabledSkillNames: []string{"review"},
		EnabledMemoryIDs:  []string{"mem-flutter"},
	}); err != nil {
		t.Fatalf("write initial session_context_update request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionContextResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "WAIT_INPUT", true)

	if err := conn.WriteJSON(protocol.SessionContextUpdateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_context_update"},
	}); err != nil {
		t.Fatalf("write clearing session_context_update request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionContextResult)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "现在启用了哪些 skill 和 memory？\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if !strings.Contains(got, "[MobileVC Enabled Skills]") {
			t.Fatalf("expected enabled skills prefix, got %q", got)
		}
		if !strings.Contains(got, "[MobileVC Memory]") {
			t.Fatalf("expected memory prefix, got %q", got)
		}
		if !strings.Contains(got, "- (无)") {
			t.Fatalf("expected explicit empty state in injected payload, got %q", got)
		}
		if strings.Contains(got, "- review") {
			t.Fatalf("did not expect stale skill name in injected payload, got %q", got)
		}
		if strings.Contains(got, "Flutter Index") {
			t.Fatalf("did not expect stale memory title in injected payload, got %q", got)
		}
		if !strings.Contains(got, "现在启用了哪些 skill 和 memory？") {
			t.Fatalf("expected original user input in injected payload, got %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive input payload")
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerInputBlocksPendingPermissionRequest(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}),
		protocol.RuntimeMeta{ResumeSessionID: "resume-input-123"},
	))
	firstRunner.claudeSessionID = "resume-input-123"
	runnerIndex := 0
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "input-pending-permission")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty", PermissionMode: "default"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.InputRequestEvent{ClientEvent: protocol.ClientEvent{Action: "input"}, Data: "请创建 README.md 并写入 hello\n"}); err != nil {
		t.Fatalf("write input request: %v", err)
	}
	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "有权限请求待处理，请先在 App 中完成授权" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart while permission is pending, got runner count=%d", runnerIndex)
	}
	select {
	case payload := <-firstRunner.writeCh:
		t.Fatalf("unexpected input payload while permission is pending: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}

	closeConnAndCleanupRuntime(t, conn, h, firstRunner)
}

func TestHandlerInputAutoResumesDetachedClaudeSession(t *testing.T) {
	firstRunner := newStubRunner(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "已暂停，可继续", nil),
		protocol.RuntimeMeta{ResumeSessionID: "resume-chat-123"},
	))
	firstRunner.claudeSessionID = "resume-chat-123"
	secondRunner := newHoldingStubRunner()
	runnerIndex := 0

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		if runnerIndex == 1 {
			return firstRunner
		}
		return secondRunner
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "input-auto-resume")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty", PermissionMode: "default"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	time.Sleep(100 * time.Millisecond)

	if err := conn.WriteJSON(protocol.InputRequestEvent{ClientEvent: protocol.ClientEvent{Action: "input"}, Data: "second turn\n"}); err != nil {
		t.Fatalf("write input request: %v", err)
	}
	var thinking map[string]any
	for i := 0; i < 5; i++ {
		event := readUntilType(t, conn, protocol.EventTypeAgentState)
		if event["state"] == "THINKING" {
			thinking = event
			break
		}
	}
	if thinking == nil {
		t.Fatal("expected transient THINKING event after detached resume input")
	}
	requireAgentState(t, thinking, "THINKING", false)
	if thinking["msg"] != "恢复会话中" {
		t.Fatalf("expected transient resume message, got %#v", thinking)
	}
	if thinking["resumeSessionId"] != "resume-chat-123" {
		t.Fatalf("expected resumeSessionId on transient thinking event, got %#v", thinking)
	}
	if thinking["claudeLifecycle"] != "active" {
		t.Fatalf("expected active claudeLifecycle on transient thinking event, got %#v", thinking)
	}
	if thinking["permissionMode"] != "default" {
		t.Fatalf("expected default permission mode on transient thinking event, got %#v", thinking)
	}
	secondRunner.WaitStarted(t)
	if !strings.Contains(secondRunner.lastReq.Command, "--resume ") {
		t.Fatalf("expected resumed command, got %q", secondRunner.lastReq.Command)
	}
	select {
	case payload := <-secondRunner.writeCh:
		if got := string(payload); got != "second turn\n" {
			t.Fatalf("unexpected resumed input payload: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive resumed input payload")
	}

	closeConnAndCleanupRuntime(t, conn, h, secondRunner)
}

func TestHandlerInputWithoutRunner(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	server := newLocalHTTPServer(t, h)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var initial protocol.SessionStateEvent
	if err := conn.ReadJSON(&initial); err != nil {
		t.Fatalf("read initial event: %v", err)
	}
	var initialAgent map[string]any
	if err := conn.ReadJSON(&initialAgent); err != nil {
		t.Fatalf("read initial agent event: %v", err)
	}
	_ = createHistorySessionForHandlerTest(t, h, conn, "input-without-runner")

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "x\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == protocol.EventTypeError {
			if event["msg"] != "当前没有活跃会话，且没有可恢复的 Claude 会话，请重新发起命令" {
				t.Fatalf("unexpected error event: %#v", event)
			}
			return
		}
	}
}

func TestHandlerStopWithoutActiveRunnerEmitsStoppedState(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	defer closeConnAndCleanupRuntime(t, conn, h)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "stop target")
	if err := conn.WriteJSON(protocol.ClientEvent{
		Action:         "stop",
		ClientActionID: "stop-no-runner-1",
	}); err != nil {
		t.Fatalf("write stop request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeSessionState)
	if got, _ := event["sessionId"].(string); got != sessionID {
		t.Fatalf("session id: got %q want %q", got, sessionID)
	}
	if got, _ := event["state"].(string); got != "stopped" {
		t.Fatalf("state: got %q want stopped; event=%#v", got, event)
	}
	if msg, _ := event["msg"].(string); !strings.Contains(msg, "没有可停止") {
		t.Fatalf("message should explain no active runner, got %#v", event)
	}
}

func TestHandlerInputRejectedForExecRunner(t *testing.T) {
	execRunner := newHoldingStubRunner()
	execRunner.writeErr = engine.ErrInputNotSupported

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewExecRunner = func() engine.Runner { return execRunner }

	server := newLocalHTTPServer(t, h)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var initial protocol.SessionStateEvent
	if err := conn.ReadJSON(&initial); err != nil {
		t.Fatalf("read initial event: %v", err)
	}
	_ = createHistorySessionForHandlerTest(t, h, conn, "input-exec-runner")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'ignored'",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "x\n",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == protocol.EventTypeError {
			if event["msg"] != "input is only supported for pty sessions" {
				t.Fatalf("unexpected error event: %#v", event)
			}
			return
		}
	}
}

func TestHandlerRecoversRunnerPanicAndReturnsErrorEvent(t *testing.T) {
	var logs bytes.Buffer
	originalWriter := log.Writer()
	originalFlags := log.Flags()
	log.SetOutput(&logs)
	log.SetFlags(0)
	defer log.SetOutput(originalWriter)
	defer log.SetFlags(originalFlags)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.Upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	h.NewExecRunner = func() engine.Runner {
		return &panicRunner{}
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "panic-runner")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "panic please",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "internal server error" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	stack, _ := event["stack"].(string)
	if stack == "" {
		t.Fatalf("expected panic stack in error event, got %#v", event)
	}
	if !strings.Contains(logs.String(), "runner panic recovered") {
		t.Fatalf("expected runtime panic log, got %q", logs.String())
	}
}

type panicRunner struct{}

func (p *panicRunner) Run(ctx context.Context, req engine.ExecRequest, sink engine.EventSink) error {
	panic("boom")
}

func (p *panicRunner) Write(ctx context.Context, data []byte) error {
	return nil
}

func (p *panicRunner) Close() error {
	return nil
}

func (p *panicRunner) SetPermissionMode(mode string) {}

func TestParseMode(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    engine.Mode
		wantErr error
	}{
		{name: "default", input: "", want: engine.ModeExec},
		{name: "exec", input: "exec", want: engine.ModeExec},
		{name: "pty", input: "pty", want: engine.ModePTY},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMode(tt.input)
			if err != nil {
				t.Fatalf("parse mode returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, got)
			}
		})
	}

	if _, err := parseMode("weird"); err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

func TestHandlerRejectsEmptyInput(t *testing.T) {
	h := newTestHandler()
	server := newLocalHTTPServer(t, h)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var initial protocol.SessionStateEvent
	if err := conn.ReadJSON(&initial); err != nil {
		t.Fatalf("read initial event: %v", err)
	}

	if err := conn.WriteJSON(protocol.InputRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "input"},
		Data:        "",
	}); err != nil {
		t.Fatalf("write input request: %v", err)
	}

	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == protocol.EventTypeError {
			if event["msg"] != "input data is required" {
				t.Fatalf("unexpected error event: %#v", event)
			}
			return
		}
	}
}

func TestHandlerUnknownAction(t *testing.T) {
	h := newTestHandler()
	server := newLocalHTTPServer(t, h)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var initial protocol.SessionStateEvent
	if err := conn.ReadJSON(&initial); err != nil {
		t.Fatalf("read initial event: %v", err)
	}

	if err := conn.WriteJSON(map[string]any{"action": "nope"}); err != nil {
		t.Fatalf("write request: %v", err)
	}

	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == protocol.EventTypeError {
			if event["msg"] != "unknown action: nope" {
				t.Fatalf("unexpected error event: %#v", event)
			}
			return
		}
	}
}

func TestHandlerUnknownMode(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	server := newLocalHTTPServer(t, h)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/?token=test"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var initial protocol.SessionStateEvent
	if err := conn.ReadJSON(&initial); err != nil {
		t.Fatalf("read initial event: %v", err)
	}
	var initialAgent map[string]any
	if err := conn.ReadJSON(&initialAgent); err != nil {
		t.Fatalf("read initial agent event: %v", err)
	}
	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "unknown-mode"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionCreated)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "printf 'ignored'",
		Mode:        "weird",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}

	for {
		var event map[string]any
		if err := conn.ReadJSON(&event); err != nil {
			t.Fatalf("read event: %v", err)
		}
		if event["type"] == protocol.EventTypeError {
			if event["msg"] != "unknown mode: weird" {
				t.Fatalf("unexpected error event: %#v", event)
			}
			return
		}
	}
}

func TestHandlerPermissionDecisionApproveUsesDirectPermissionResponse(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}),
		protocol.RuntimeMeta{PermissionRequestID: "perm-claude-1", BlockingKind: "permission"},
	))
	firstRunner.currentPermissionRequestID = "perm-claude-1"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerIndex := 0
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-approve")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	prompt := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if prompt["msg"] != "写 README 需要你的授权" {
		t.Fatalf("unexpected prompt event: %#v", prompt)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision"},
		Decision:            "approve",
		PermissionMode:      "default",
		PermissionRequestID: "perm-claude-1",
		TargetPath:          "README.md",
		ContextTitle:        "README",
		PromptMessage:       "写 README 需要你的授权",
		FallbackCommand:     "claude",
		FallbackEngine:      "claude",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		if decision != "approve" {
			t.Fatalf("unexpected direct permission response: %q", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive direct permission response")
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart, got runner count=%d", runnerIndex)
	}
	select {
	case payload := <-firstRunner.writeCh:
		t.Fatalf("unexpected continuation payload: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}

	var thinking map[string]any
	for i := 0; i < 6; i++ {
		event := readUntilType(t, conn, protocol.EventTypeAgentState)
		if event["source"] == "permission-decision" {
			thinking = event
			break
		}
	}
	if thinking == nil {
		t.Fatal("did not receive permission-decision agent state")
	}
	if thinking["state"] != "THINKING" {
		t.Fatalf("expected THINKING state, got %#v", thinking)
	}
	if thinking["source"] != "permission-decision" {
		t.Fatalf("expected permission-decision source, got %#v", thinking)
	}
	if thinking["permissionMode"] != "default" {
		t.Fatalf("expected default permission mode after approval, got %#v", thinking)
	}
}

func TestHandlerPermissionDecisionBeforeResumeBindsSelectedSession(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}),
		protocol.RuntimeMeta{
			PermissionRequestID: "perm-reconnect-1",
			ResumeSessionID:     "resume-reconnect-123",
			BlockingKind:        "permission",
		},
	))
	firstRunner.currentPermissionRequestID = "perm-reconnect-1"
	firstRunner.claudeSessionID = "resume-reconnect-123"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return firstRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "permission-reconnect")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	closeTestConnGracefully(t, conn)

	conn2 := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn2)
	if err := conn2.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision", SessionID: sessionID},
		Decision:            "approve",
		PermissionMode:      "default",
		PermissionRequestID: "perm-reconnect-1",
		ResumeSessionID:     "resume-reconnect-123",
		TargetPath:          "README.md",
		PromptMessage:       "写 README 需要你的授权",
		FallbackCommand:     "claude",
		FallbackEngine:      "claude",
	}); err != nil {
		t.Fatalf("write reconnect permission decision: %v", err)
	}
	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		if decision != "approve" {
			t.Fatalf("unexpected direct permission response: %q", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive direct permission response after reconnect")
	}
	closeConnAndCleanupRuntime(t, conn2, h, firstRunner)
}

func TestHandlerPermissionDecisionApproveForCodexUsesDirectPermissionResponse(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "需要权限确认", []string{"approve", "deny"}))
	firstRunner.currentPermissionRequestID = "perm-codex-1"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerIndex := 0
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-approve-codex")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "codex",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision"},
		Decision:            "approve",
		PermissionMode:      "default",
		PermissionRequestID: "perm-codex-1",
		PromptMessage:       "需要权限确认",
		FallbackCommand:     "codex",
		FallbackEngine:      "codex",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		if decision != "approve" {
			t.Fatalf("unexpected permission response: %q", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive direct codex permission response")
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart for codex, got runner count=%d", runnerIndex)
	}
	closeConnAndCleanupRuntime(t, conn, h, firstRunner)
}

func TestHandlerPermissionDecisionApproveForCodexWithExpiredPendingRequestReturnsError(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "需要权限确认", []string{"approve", "deny"}))
	firstRunner.currentPermissionRequestID = "perm-codex-1"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	runnerIndex := 0
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-approve-codex-expired")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "codex",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	firstRunner.mu.Lock()
	firstRunner.hasPendingPermission = false
	firstRunner.mu.Unlock()

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision"},
		Decision:            "approve",
		PermissionMode:      "default",
		PermissionRequestID: "perm-codex-1",
		PromptMessage:       "需要权限确认",
		FallbackCommand:     "codex",
		FallbackEngine:      "codex",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前权限请求已失效，请等待 AI 重新发起操作后再确认" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart for expired codex permission, got runner count=%d", runnerIndex)
	}
	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		t.Fatalf("unexpected direct permission response: %q", decision)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case payload := <-firstRunner.writeCh:
		t.Fatalf("unexpected continuation payload: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerPermissionDecisionApproveForCodexWithMismatchedRequestIDRefreshesPrompt(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "需要权限确认", []string{"approve", "deny"}))
	firstRunner.currentPermissionRequestID = "perm-codex-1"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return firstRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-approve-codex-mismatch")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "codex",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision"},
		Decision:            "approve",
		PermissionMode:      "default",
		PermissionRequestID: "perm-codex-2",
		PromptMessage:       "需要权限确认",
		FallbackCommand:     "codex",
		FallbackEngine:      "codex",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if event["permissionRequestId"] != "perm-codex-1" {
		t.Fatalf("expected refreshed prompt for current request, got %#v", event)
	}
	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		t.Fatalf("unexpected direct permission response: %q", decision)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerPermissionDecisionDenyForCodexWithMismatchedRequestIDStillRefreshesPrompt(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "需要权限确认", []string{"approve", "deny"}))
	firstRunner.currentPermissionRequestID = "perm-deny-current"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return firstRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-deny-codex-mismatch")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "codex",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:         protocol.ClientEvent{Action: "permission_decision"},
		Decision:            "deny",
		PermissionMode:      "default",
		PermissionRequestID: "perm-deny-stale",
		PromptMessage:       "需要权限确认",
		FallbackCommand:     "codex",
		FallbackEngine:      "codex",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	// deny 仍要求用户对真正的当前请求做明确拒绝，不应自动套到 current pending。
	event := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if event["permissionRequestId"] != "perm-deny-current" {
		t.Fatalf("expected refreshed current permission request id, got %#v", event)
	}
	if event["blockingKind"] != "permission" {
		t.Fatalf("expected refreshed permission prompt, got %#v", event)
	}
	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		t.Fatalf("unexpected direct permission response on stale deny: %q", decision)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerPermissionDecisionDenySendsPromptAsNormalInput(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}))
	ptyRunner.claudeSessionID = "resume-deny-123"
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-deny")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "permission_decision"},
		Decision:       "deny",
		PermissionMode: "default",
		TargetPath:     "README.md",
		PromptMessage:  "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		if !strings.Contains(string(payload), "用户拒绝了刚才请求的文件修改/写入权限") {
			t.Fatalf("unexpected deny payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive deny prompt payload")
	}
	select {
	case decision := <-ptyRunner.permissionResponseWriteCh:
		t.Fatalf("unexpected structured deny decision: %q", decision)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerPlanDecisionWritesDecisionPayloadToRunner(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "Claude 会话已就绪，可继续输入", nil))
	ptyRunner.claudeSessionID = "resume-plan-123"

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "plan-decision")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	decision := `{"kind":"plan","sessionId":"session-test","answers":{"question-1":"继续"}}`
	if err := conn.WriteJSON(protocol.PlanDecisionRequestEvent{
		ClientEvent:     protocol.ClientEvent{Action: "plan_decision"},
		Decision:        decision,
		SessionID:       "session-test",
		ResumeSessionID: "resume-plan-123",
		ExecutionID:     "exec-plan-1",
		GroupID:         "group-plan-1",
		GroupTitle:      "Plan group",
		ContextID:       "ctx-plan-1",
		ContextTitle:    "Plan context",
		PromptMessage:   "请选择下一步",
		PermissionMode:  "default",
		Command:         "claude",
		CWD:             ".",
		TargetPath:      "README.md",
	}); err != nil {
		t.Fatalf("write plan decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		if string(payload) != decision+"\n" {
			t.Fatalf("unexpected plan payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive plan payload")
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerPermissionDecisionWithoutRunnerReturnsError(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "permission_decision"},
		Decision:       "approve",
		PermissionMode: "default",
		TargetPath:     "README.md",
		PromptMessage:  "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前没有可交互的 Claude 会话，无法继续处理该权限请求" {
		t.Fatalf("unexpected error event: %#v", event)
	}
}

func TestHandlerPermissionDecisionWithManagedFreshClaudeSessionUsesDirectPermissionResponse(t *testing.T) {
	firstRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}))
	runnerIndex := 0

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-no-pending")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	prompt := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if resumeID, _ := prompt["resumeSessionId"].(string); strings.TrimSpace(resumeID) == "" {
		t.Fatalf("expected managed resume session id on prompt, got %#v", prompt)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "permission_decision"},
		Decision:       "approve",
		PermissionMode: "default",
		TargetPath:     "README.md",
		PromptMessage:  "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		if decision != "approve" {
			t.Fatalf("unexpected direct permission response: %q", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive direct permission response")
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart, got runner count=%d", runnerIndex)
	}
	closeConnAndCleanupRuntime(t, conn, h, firstRunner)
}

func TestHandlerPermissionDecisionWithoutPermissionResponseSupportReturnsError(t *testing.T) {
	base := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}))
	runnerWithoutClaudeSession := &writeOnlyStubRunner{base: base}
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return runnerWithoutClaudeSession }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-no-control-support")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "bash",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	runnerWithoutClaudeSession.WaitStarted(t)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "permission_decision"},
		Decision:       "approve",
		PermissionMode: "default",
		TargetPath:     "README.md",
		PromptMessage:  "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前会话不支持交互输入，请先恢复 Claude PTY 会话" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	select {
	case payload := <-base.writeCh:
		t.Fatalf("unexpected normal input payload: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerPermissionDecisionApproveAfterRunnerEndedReturnsError(t *testing.T) {
	firstRunner := newStubRunner(protocol.ApplyRuntimeMeta(
		protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}),
		protocol.RuntimeMeta{ResumeSessionID: "resume-123"},
	))
	firstRunner.claudeSessionID = "resume-123"
	runnerIndex := 0

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-resume")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	prompt := readUntilType(t, conn, protocol.EventTypePromptRequest)
	if resumeID, _ := prompt["resumeSessionId"].(string); strings.TrimSpace(resumeID) == "" {
		t.Fatalf("expected managed resume session id on prompt, got %#v", prompt)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:     protocol.ClientEvent{Action: "permission_decision"},
		Decision:        "approve",
		PermissionMode:  "default",
		ResumeSessionID: "resume-123",
		FallbackCommand: "claude",
		FallbackCWD:     "/tmp",
		TargetPath:      "README.md",
		PromptMessage:   "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前没有可交互的 Claude 会话，无法继续处理该权限请求" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart, got runner count=%d", runnerIndex)
	}
}

func TestHandlerPermissionDecisionWithNonInteractiveRunnerUsesDirectPermissionResponse(t *testing.T) {
	firstRunner := newNonInteractiveHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "写 README 需要你的授权", []string{"y", "n"}))
	firstRunner.claudeSessionID = "resume-non-interactive-123"
	runnerIndex := 0
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		runnerIndex++
		return firstRunner
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-non-interactive")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.InputRequestEvent{ClientEvent: protocol.ClientEvent{Action: "input"}, Data: "请创建 README.md 并写入 hello\n"}); err != nil {
		t.Fatalf("write input request: %v", err)
	}
	select {
	case payload := <-firstRunner.writeCh:
		if string(payload) != "请创建 README.md 并写入 hello\n" {
			t.Fatalf("unexpected initial input payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive initial input payload")
	}

	if err := conn.WriteJSON(protocol.PermissionDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "permission_decision"},
		Decision:       "approve",
		PermissionMode: "default",
		TargetPath:     "README.md",
		PromptMessage:  "写 README 需要你的授权",
	}); err != nil {
		t.Fatalf("write permission decision request: %v", err)
	}
	select {
	case decision := <-firstRunner.permissionResponseWriteCh:
		if decision != "approve" {
			t.Fatalf("unexpected direct permission response: %q", decision)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive direct permission response")
	}
	if runnerIndex != 1 {
		t.Fatalf("expected no runner restart, got runner count=%d", runnerIndex)
	}
	thinking := readUntilType(t, conn, protocol.EventTypeAgentState)
	if thinking["state"] != "THINKING" {
		t.Fatalf("expected THINKING state, got %#v", thinking)
	}
	closeConnAndCleanupRuntime(t, conn, h, firstRunner)
}

func TestHandlerReviewDecisionWithNonInteractiveRunnerReturnsError(t *testing.T) {
	ptyRunner := newNonInteractiveHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "review-non-interactive")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{ClientEvent: protocol.ClientEvent{Action: "review_decision"}, Decision: "accept"}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前 Claude 会话尚未进入可直接确认的交互阶段，请先等待当前会话就绪后再提交审核决策" {
		t.Fatalf("unexpected error event: %#v", event)
	}
	select {
	case payload := <-ptyRunner.writeCh:
		t.Fatalf("unexpected payload written to non-interactive runner: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}
}

func TestHandlerReviewDecisionSendsPromptToRunner(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "review-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "review_decision"},
		Decision:       "accept",
		ContextID:      "diff:1",
		ContextTitle:   "最近 Diff",
		TargetPath:     "internal/ws/handler.go",
		PermissionMode: "auto",
		IsReviewOnly:   true,
	}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		t.Fatalf("unexpected review decision payload: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}

	var thinking map[string]any
	for i := 0; i < 5; i++ {
		event := readUntilType(t, conn, protocol.EventTypeAgentState)
		if event["source"] == "review-decision" {
			thinking = event
			break
		}
	}
	if thinking == nil {
		t.Fatal("did not receive review-decision agent state")
	}
	if thinking["state"] != "THINKING" {
		t.Fatalf("expected THINKING state, got %#v", thinking)
	}
	if thinking["source"] != "review-decision" {
		t.Fatalf("expected review-decision source, got %#v", thinking)
	}
	if thinking["permissionMode"] != "auto" {
		t.Fatalf("expected review permission mode, got %#v", thinking)
	}
}

func TestHandlerReviewDecisionRevertSendsPromptEvenWhenReviewOnly(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "review-revert-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "review_decision"},
		Decision:       "revert",
		ContextID:      "diff:revert-1",
		ContextTitle:   "最近 Diff",
		TargetPath:     "internal/ws/handler.go",
		PermissionMode: "auto",
		IsReviewOnly:   true,
	}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		got := string(payload)
		if !strings.Contains(got, "Review decision: REVERT.") ||
			!strings.Contains(got, "Please drop the change and restore the previous state before proceeding.") {
			t.Fatalf("unexpected revert review payload: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive revert review payload")
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerReviewDecisionUpdatesProjectionAndReviewState(t *testing.T) {
	ptyRunner := newHoldingStubRunner(
		protocol.NewPromptRequestEvent("ignored", "等待输入", nil),
		protocol.NewFileDiffEvent("ignored", "hhh.txt", "新增 hhh.txt", "+++ b/hhh.txt\n@@\n+测试功能\n", "text"),
	)
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "review-flow")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeFileDiff)
	_ = waitForPersistedReviewFile(t, h, sessionID, "hhh.txt")

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "review_decision"},
		Decision:       "accept",
		ContextID:      "hhh.txt",
		TargetPath:     "hhh.txt",
		GroupID:        "hhh.txt",
		GroupTitle:     "hhh.txt",
		PermissionMode: "default",
		IsReviewOnly:   true,
	}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		t.Fatalf("unexpected review decision payload: %q", string(payload))
	case <-time.After(200 * time.Millisecond):
	}

	reviewState := readUntilReviewFileStatus(t, conn, "hhh.txt", "accepted")
	groups, ok := reviewState["groups"].([]any)
	if !ok || len(groups) != 1 {
		t.Fatalf("expected one review group, got %#v", reviewState)
	}
	activeGroup, ok := reviewState["activeGroup"].(map[string]any)
	if !ok {
		t.Fatalf("expected activeGroup payload, got %#v", reviewState)
	}
	if activeGroup["reviewStatus"] != "accepted" {
		t.Fatalf("expected accepted active group, got %#v", activeGroup)
	}
	files, ok := groups[0].(map[string]any)["files"].([]any)
	if !ok || len(files) != 1 {
		t.Fatalf("expected one review file, got %#v", groups[0])
	}
	file, ok := files[0].(map[string]any)
	if !ok {
		t.Fatalf("expected review file payload, got %#v", files[0])
	}
	if pending, _ := file["pendingReview"].(bool); pending {
		t.Fatalf("expected review to be cleared, got %#v", file)
	}
	if status := file["reviewStatus"]; status != "accepted" {
		t.Fatalf("expected accepted review status, got %#v", file)
	}

	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get session: %v", err)
	}
	if len(record.Projection.Diffs) != 1 {
		t.Fatalf("expected one persisted diff, got %#v", record.Projection.Diffs)
	}
	if record.Projection.Diffs[0].PendingReview {
		t.Fatalf("expected persisted diff review to be cleared, got %#v", record.Projection.Diffs[0])
	}
	if record.Projection.Diffs[0].ReviewStatus != "accepted" {
		t.Fatalf("expected persisted diff to be accepted, got %#v", record.Projection.Diffs[0])
	}
}

func TestHandlerSetPermissionModeUpdatesRunner(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "permission-mode-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty", PermissionMode: "auto"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = waitForPersistedPermissionMode(t, tempStore, sessionID, "auto")

	if err := conn.WriteJSON(protocol.PermissionModeUpdateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "set_permission_mode"}, PermissionMode: "default"}); err != nil {
		t.Fatalf("write permission mode request: %v", err)
	}

	var state map[string]any
	for i := 0; i < 5; i++ {
		event := readUntilType(t, conn, protocol.EventTypeAgentState)
		if event["permissionMode"] == "default" {
			state = event
			break
		}
	}
	if state == nil {
		t.Fatal("did not receive updated permissionMode agent state")
	}
	if state["permissionMode"] != "default" {
		t.Fatalf("expected updated permission mode, got %#v", state)
	}
	if ptyRunner.lastPermissionMode != "default" {
		t.Fatalf("expected runner permission mode to update, got %q", ptyRunner.lastPermissionMode)
	}
	record := waitForPersistedPermissionMode(t, tempStore, sessionID, "default")
	if record.Projection.Runtime.PermissionMode != "default" {
		t.Fatalf("expected persisted permission mode default, got %#v", record.Projection.Runtime)
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerSetPermissionModeUpdatesActiveRunner(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "permission-mode-active-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "exec"},
		Command:        "claude",
		Mode:           "pty",
		PermissionMode: "auto",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.PermissionModeUpdateRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "set_permission_mode"},
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write set_permission_mode request: %v", err)
	}

	var state map[string]any
	for i := 0; i < 5; i++ {
		event := readUntilType(t, conn, protocol.EventTypeAgentState)
		if event["permissionMode"] == "default" {
			state = event
			break
		}
	}
	if state == nil {
		t.Fatal("did not receive updated permissionMode agent state")
	}
	if state["permissionMode"] != "default" {
		t.Fatalf("expected permissionMode to be default, got %#v", state)
	}
	if ptyRunner.lastPermissionMode != "default" {
		t.Fatalf("expected runner permission mode to update, got %q", ptyRunner.lastPermissionMode)
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerReviewDecisionWithoutRunner(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{ClientEvent: protocol.ClientEvent{Action: "review_decision"}, Decision: "revert"}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "当前 Claude 会话尚未进入可直接确认的交互阶段，请先等待当前会话就绪后再提交审核决策" {
		t.Fatalf("unexpected error event: %#v", event)
	}
}

func TestHandlerReviewDecisionAcceptAllowedInDefaultMode(t *testing.T) {
	ptyRunner := newHoldingStubRunner(protocol.NewPromptRequestEvent("ignored", "等待输入", nil))
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = createHistorySessionForHandlerTest(t, h, conn, "review-default-mode")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty", PermissionMode: "default"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)
	_ = readUntilType(t, conn, protocol.EventTypePromptRequest)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{
		ClientEvent:    protocol.ClientEvent{Action: "review_decision"},
		Decision:       "accept",
		ContextID:      "diff:1",
		ContextTitle:   "最近 Diff",
		TargetPath:     "internal/ws/handler.go",
		PermissionMode: "default",
	}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	select {
	case payload := <-ptyRunner.writeCh:
		want := "Review decision: ACCEPT.\nTarget: internal/ws/handler.go.\nPlease land the change and continue; no further review is required.\n"
		if string(payload) != want {
			t.Fatalf("unexpected review payload: %q", string(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive review decision payload")
	}

	thinking := readUntilType(t, conn, protocol.EventTypeAgentState)
	if thinking["state"] != "THINKING" {
		t.Fatalf("expected THINKING state, got %#v", thinking)
	}
	if thinking["permissionMode"] != "default" {
		t.Fatalf("expected default permission mode, got %#v", thinking)
	}
}

func TestHandlerReviewDecisionRejectsUnknownDecision(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.ReviewDecisionRequestEvent{ClientEvent: protocol.ClientEvent{Action: "review_decision"}, Decision: "shipit"}); err != nil {
		t.Fatalf("write review decision request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "review decision must be one of: accept, revert, revise" {
		t.Fatalf("unexpected error event: %#v", event)
	}
}

func TestHandlerRuntimeInfoReturnsContextSnapshot(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.RuntimeInfoRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_info"},
		Query:       "context",
		CWD:         ".",
	}); err != nil {
		t.Fatalf("write runtime_info request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeRuntimeInfoResult)
	if event["query"] != "context" {
		t.Fatalf("expected context query, got %#v", event)
	}
	items, ok := event["items"].([]any)
	if !ok || len(items) == 0 {
		t.Fatalf("expected runtime info items, got %#v", event)
	}
}

func TestHandlerRuntimeInfoRejectsUnknownQuery(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.RuntimeInfoRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "runtime_info"},
		Query:       "mystery",
		CWD:         ".",
	}); err != nil {
		t.Fatalf("write runtime_info request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "unsupported runtime_info query: mystery" {
		t.Fatalf("unexpected error event: %#v", event)
	}
}

func TestHandlerContextWindowsUsageAliasReturnsUsage(t *testing.T) {
	ptyRunner := newInteractiveHoldingStubRunner()
	ptyRunner.contextUsage = protocol.ContextWindowUsage{
		TokensUsed: 1200,
		TokenLimit: 8000,
	}
	ptyRunner.contextUsageOK = true
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner { return ptyRunner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "context-window-usage-alias")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "codex",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	ptyRunner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.ContextWindowUsageRequestEvent{
		ClientEvent: protocol.ClientEvent{
			Action:    protocol.ActionContextWindowsUsageGetAlias,
			SessionID: sessionID,
		},
	}); err != nil {
		t.Fatalf("write context window usage alias request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeContextWindowUsage)
	if event["sessionId"] != sessionID {
		t.Fatalf("expected session %q, got %#v", sessionID, event)
	}
	usage, ok := event["usage"].(map[string]any)
	if !ok {
		t.Fatalf("expected usage map, got %#v", event)
	}
	if usage["tokensUsed"] != float64(1200) || usage["tokenLimit"] != float64(8000) {
		t.Fatalf("unexpected context window usage: %#v", usage)
	}
	closeConnAndCleanupRuntime(t, conn, h, ptyRunner)
}

func TestHandlerSlashCommandRuntimeInfoQueries(t *testing.T) {
	tests := []struct {
		name    string
		command string
		query   string
	}{
		{name: "help", command: "/help", query: "help"},
		{name: "context", command: "/context", query: "context"},
		{name: "model", command: "/model", query: "model"},
		{name: "cost", command: "/cost", query: "cost"},
		{name: "doctor", command: "/doctor", query: "doctor"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler()
			conn := newTestConn(t, h)
			_, _ = readInitialEvents(t, conn)

			if err := conn.WriteJSON(protocol.SlashCommandRequestEvent{
				ClientEvent: protocol.ClientEvent{Action: "slash_command"},
				Command:     tt.command,
				CWD:         ".",
			}); err != nil {
				t.Fatalf("write slash command request: %v", err)
			}

			event := readUntilType(t, conn, protocol.EventTypeRuntimeInfoResult)
			if event["query"] != tt.query {
				t.Fatalf("expected query %q, got %#v", tt.query, event)
			}
		})
	}
}

func TestHandlerSlashCommandLocalOnlyCommands(t *testing.T) {
	tests := []string{"/clear", "/exit", "/quit", "/fast"}
	for _, command := range tests {
		t.Run(command, func(t *testing.T) {
			h := newTestHandler()
			conn := newTestConn(t, h)
			_, _ = readInitialEvents(t, conn)

			if err := conn.WriteJSON(protocol.SlashCommandRequestEvent{
				ClientEvent: protocol.ClientEvent{Action: "slash_command"},
				Command:     command,
			}); err != nil {
				t.Fatalf("write slash command request: %v", err)
			}

			event := readUntilType(t, conn, protocol.EventTypeRuntimeInfoResult)
			items, ok := event["items"].([]any)
			if !ok || len(items) == 0 {
				t.Fatalf("expected runtime info items, got %#v", event)
			}
			first, ok := items[0].(map[string]any)
			if !ok || first["status"] != "local-only" {
				t.Fatalf("expected local-only status, got %#v", event)
			}
		})
	}
}

func TestHandlerSlashCommandDiffRequiresContext(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SlashCommandRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "slash_command"},
		Command:     "/diff",
	}); err != nil {
		t.Fatalf("write slash command request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeError)
	if event["msg"] != "/diff requires targetDiff context" {
		t.Fatalf("unexpected error event: %#v", event)
	}
}

func TestHandlerSlashCommandExecMappings(t *testing.T) {
	tests := []struct {
		name    string
		command string
		want    string
	}{
		{name: "init", command: "/init", want: "claude /init"},
		{name: "compact", command: "/compact", want: "claude /compact"},
		{name: "run", command: "/run echo hi", want: "echo hi"},
		{name: "add-dir", command: "/add-dir /tmp/demo", want: "claude /add-dir /tmp/demo"},
		{name: "git commit quote", command: "/git commit hello", want: "git commit -m \"hello\""},
		{name: "test fallback", command: "/test path/to/file", want: "go test ./..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runnerStub := newHoldingStubRunner()
			h := newTestHandler()
			h.NewPtyRunner = func() engine.Runner { return runnerStub }
			conn := newTestConn(t, h)
			_, _ = readInitialEvents(t, conn)

			if err := conn.WriteJSON(protocol.SlashCommandRequestEvent{
				ClientEvent: protocol.ClientEvent{Action: "slash_command"},
				Command:     tt.command,
				CWD:         ".",
			}); err != nil {
				t.Fatalf("write slash command request: %v", err)
			}

			thinking := readUntilType(t, conn, protocol.EventTypeAgentState)
			if thinking["state"] != "THINKING" {
				t.Fatalf("expected THINKING state, got %#v", thinking)
			}
			select {
			case <-runnerStub.writeCh:
				// ignore stray writes
			default:
			}
		})
	}
}

func TestHandlerCompactActionRequiresLoadedSession(t *testing.T) {
	h := newTestHandler()
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact"},
	}); err != nil {
		t.Fatalf("write compact request: %v", err)
	}

	event := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if event["accepted"] != false {
		t.Fatalf("expected compact rejection, got %#v", event)
	}
}

func TestHandlerCompactDoesNotBlockPingPong(t *testing.T) {
	runner := newBlockingCompactStubRunner()
	h := newTestHandler()
	h.NewPtyRunner = func() engine.Runner { return runner }
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "compact-ping-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec", SessionID: sessionID, ClientActionID: "exec-compact-ping"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "exec", "exec-compact-ping")
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	runner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact", SessionID: sessionID, ClientActionID: "compact-blocking"},
	}); err != nil {
		t.Fatalf("write compact request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "compact", "compact-blocking")
	runner.WaitCompactStarted(t)

	if err := conn.WriteJSON(map[string]any{"action": "ping", "pingId": "during-compact"}); err != nil {
		t.Fatalf("write ping request: %v", err)
	}
	pong := readUntilPongID(t, conn, "during-compact")
	if pong["sessionId"] != sessionID {
		t.Fatalf("expected pong for loaded session %q, got %#v", sessionID, pong)
	}

	runner.ReleaseCompact()
	compact := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if compact["accepted"] != true {
		t.Fatalf("expected compact success after release, got %#v", compact)
	}
}

func TestHandlerProjectionSaveDoesNotBlockPingPong(t *testing.T) {
	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	blockingStore := newBlockingProjectionSaveStore(fileStore)
	runner := newInteractiveHoldingStubRunner()
	h := newTestHandler()
	h.SessionStore = blockingStore
	h.NewPtyRunner = func() engine.Runner { return runner }
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "projection-save-ping-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec", SessionID: sessionID, ClientActionID: "exec-blocking-save"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "exec", "exec-blocking-save")
	blockingStore.WaitSaveStarted(t)

	if err := conn.WriteJSON(map[string]any{"action": "ping", "pingId": "during-projection-save"}); err != nil {
		t.Fatalf("write ping request: %v", err)
	}
	pong := readUntilPongID(t, conn, "during-projection-save")
	if pong["sessionId"] != sessionID {
		t.Fatalf("expected pong for loaded session %q, got %#v", sessionID, pong)
	}

	blockingStore.ReleaseSave()
	blockingStore.WaitSaveDone(t)
	runner.WaitStarted(t)
}

func TestHandlerCompactRejectsConcurrentSameSessionRequest(t *testing.T) {
	runner := newBlockingCompactStubRunner()
	h := newTestHandler()
	h.NewPtyRunner = func() engine.Runner { return runner }
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	sessionID := createHistorySessionForHandlerTest(t, h, conn, "compact-busy-session")

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec", SessionID: sessionID, ClientActionID: "exec-compact-busy"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "exec", "exec-compact-busy")
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)
	runner.WaitStarted(t)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact", SessionID: sessionID, ClientActionID: "compact-first"},
	}); err != nil {
		t.Fatalf("write first compact request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "compact", "compact-first")
	runner.WaitCompactStarted(t)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact", SessionID: sessionID, ClientActionID: "compact-second"},
	}); err != nil {
		t.Fatalf("write second compact request: %v", err)
	}
	_ = readUntilClientActionAck(t, conn, "compact", "compact-second")
	busy := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if busy["accepted"] != false {
		t.Fatalf("expected concurrent compact rejection, got %#v", busy)
	}
	if msg, _ := busy["error"].(string); !strings.Contains(msg, "already running") {
		t.Fatalf("expected busy error, got %#v", busy)
	}

	runner.ReleaseCompact()
	success := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if success["accepted"] != true {
		t.Fatalf("expected first compact success after release, got %#v", success)
	}
}

func TestHandlerSessionDeleteRemovesHistorySessionFromList(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected session A id, got %#v", createdA)
	}

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	createdB := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryB, ok := createdB["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdB)
	}
	sessionB, _ := summaryB["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected distinct session B id, got %q", sessionB)
	}

	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_delete"}, SessionID: sessionA}); err != nil {
		t.Fatalf("write session delete request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == sessionA {
			t.Fatalf("expected deleted session removed from list, got %#v", items)
		}
	}
	if _, err := h.SessionStore.GetSession(context.Background(), sessionA); err == nil {
		t.Fatal("expected deleted history session lookup to fail")
	}
	if _, err := h.SessionStore.GetSession(context.Background(), sessionB); err != nil {
		t.Fatalf("expected current session to remain, got %v", err)
	}
}

func TestHandlerInitialSessionListIncludesNativeCodexSessions(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == "codex-thread:"+threadID {
			return
		}
	}
	t.Fatalf("expected native Codex mirror in initial session list, got %#v", items)
}

func TestHandlerSessionListWithoutCWDIncludesAllStoreProjects(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	otherDir := filepath.Join(homeDir, "workspace", "Other")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	if err := os.MkdirAll(otherDir, 0o755); err != nil {
		t.Fatalf("mkdir other dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "mobile-project",
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write project session create request: %v", err)
	}
	projectCreated := readUntilSessionCreated(t, conn)
	projectSummary, _ := projectCreated["summary"].(map[string]any)
	projectSessionID, _ := projectSummary["id"].(string)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "mobile-other",
		CWD:         otherDir,
	}); err != nil {
		t.Fatalf("write other session create request: %v", err)
	}
	otherCreated := readUntilSessionCreated(t, conn)
	otherSummary, _ := otherCreated["summary"].(map[string]any)
	otherSessionID, _ := otherSummary["id"].(string)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
	}); err != nil {
		t.Fatalf("write global session list request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	var foundProject, foundOther bool
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		switch item["id"] {
		case projectSessionID:
			foundProject = true
		case otherSessionID:
			foundOther = true
		}
	}
	if !foundProject || !foundOther {
		t.Fatalf("expected global session_list to include both projects project=%v other=%v items=%#v", foundProject, foundOther, items)
	}
}

func TestHandlerSessionListUsesShortTTLCache(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	counting := &countingStore{inner: tempStore}
	h.SessionStore = counting

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         "",
	}); err != nil {
		t.Fatalf("write first session_list request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	afterFirst := counting.listCallCount()

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         "",
	}); err != nil {
		t.Fatalf("write second session_list request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	if afterSecond := counting.listCallCount(); afterSecond != afterFirst {
		t.Fatalf("expected cached session_list to avoid store rescan, calls before=%d after=%d", afterFirst, afterSecond)
	}
}

func TestNormalizeSessionCWDTrimsWindowsDevicePrefix(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}

	if got, want := normalizeSessionCWD(`\\?\`+projectDir), normalizeSessionCWD(projectDir); got != want {
		t.Fatalf("expected Windows device path to normalize to %q, got %q", want, got)
	}
	if got, want := normalizeSessionCWD(`\\?\UNC\server\share\repo`), normalizeSessionCWD(`\\server\share\repo`); got != want {
		t.Fatalf("expected Windows UNC device path to normalize to %q, got %q", want, got)
	}
}

func TestHandlerSessionCreateInvalidatesSessionListCache(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	counting := &countingStore{inner: tempStore}
	h.SessionStore = counting

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         "",
	}); err != nil {
		t.Fatalf("write warm session_list request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	beforeCreate := counting.listCallCount()

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "cache-created",
	}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	created := readUntilSessionCreated(t, conn)
	summary, ok := created["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", created)
	}
	sessionID, _ := summary["id"].(string)
	if sessionID == "" {
		t.Fatalf("expected created session id, got %#v", created)
	}
	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	if afterCreate := counting.listCallCount(); afterCreate <= beforeCreate {
		t.Fatalf("expected session_create to invalidate cache and rescan, calls before=%d after=%d", beforeCreate, afterCreate)
	}

	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == sessionID {
			return
		}
	}
	t.Fatalf("expected created session %q in invalidated list, got %#v", sessionID, items)
}

func TestHandlerSessionListGenerationInvalidatesCacheAcrossConnections(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	counting := &countingStore{inner: tempStore}
	h.SessionStore = counting

	connA := newTestConn(t, h)
	_ = connA.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, _ = readInitialEvents(t, connA)
	_ = readUntilType(t, connA, protocol.EventTypeSessionListResult)

	if err := connA.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         "",
	}); err != nil {
		t.Fatalf("write first session_list request: %v", err)
	}
	_ = readUntilType(t, connA, protocol.EventTypeSessionListResult)
	afterWarm := counting.listCallCount()

	connB := newTestConn(t, h)
	_ = connB.SetReadDeadline(time.Now().Add(30 * time.Second))
	_, _ = readInitialEvents(t, connB)
	_ = readUntilType(t, connB, protocol.EventTypeSessionListResult)
	if err := connB.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "cross-connection-created",
	}); err != nil {
		t.Fatalf("write session_create on second connection: %v", err)
	}
	created := readUntilSessionCreated(t, connB)
	summary, ok := created["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", created)
	}
	sessionID, _ := summary["id"].(string)
	if sessionID == "" {
		t.Fatalf("expected created session id, got %#v", created)
	}
	_ = readUntilType(t, connB, protocol.EventTypeSessionListResult)

	if err := connA.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         "",
	}); err != nil {
		t.Fatalf("write cached connection session_list request: %v", err)
	}
	listEvent := readUntilType(t, connA, protocol.EventTypeSessionListResult)
	if afterRefresh := counting.listCallCount(); afterRefresh <= afterWarm {
		t.Fatalf("expected cross-connection generation bump to rescan, calls before=%d after=%d", afterWarm, afterRefresh)
	}
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == sessionID {
			return
		}
	}
	t.Fatalf("expected created session %q in refreshed first-connection list, got %#v", sessionID, items)
}

func TestHandlerSessionListMergesNativeCodexSessionsByCWD(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	otherDir := filepath.Join(homeDir, "workspace", "Other")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	if err := os.MkdirAll(otherDir, 0o755); err != nil {
		t.Fatalf("mkdir other dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "mobile-project",
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write project session create request: %v", err)
	}
	projectCreated := readUntilSessionCreated(t, conn)
	projectSummary, ok := projectCreated["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", projectCreated)
	}
	projectSessionID, _ := projectSummary["id"].(string)
	if projectSessionID == "" {
		t.Fatalf("expected project session id, got %#v", projectCreated)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "mobile-other",
		CWD:         otherDir,
	}); err != nil {
		t.Fatalf("write other session create request: %v", err)
	}
	otherCreated := readUntilSessionCreated(t, conn)
	otherSummary, ok := otherCreated["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", otherCreated)
	}
	otherSessionID, _ := otherSummary["id"].(string)
	if otherSessionID == "" || otherSessionID == projectSessionID {
		t.Fatalf("expected distinct other session id, got %q", otherSessionID)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session list request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}

	var foundProject, foundNative, foundOther bool
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		switch item["id"] {
		case projectSessionID:
			foundProject = true
		case otherSessionID:
			foundOther = true
		case "codex-thread:" + threadID:
			foundNative = true
			if item["source"] != "codex-native" {
				t.Fatalf("expected codex-native source, got %#v", item)
			}
			if item["external"] != true {
				t.Fatalf("expected external native session, got %#v", item)
			}
			runtime, _ := item["runtime"].(map[string]any)
			if runtime["resumeSessionId"] != threadID {
				t.Fatalf("expected native resume session id %q, got %#v", threadID, runtime)
			}
		}
	}
	if !foundProject {
		t.Fatalf("expected project-scoped session in list, got %#v", items)
	}
	if foundOther {
		t.Fatalf("did not expect other cwd session in filtered list, got %#v", items)
	}
	if !foundNative {
		t.Fatalf("expected native Codex session in filtered list, got %#v", items)
	}
}

func TestHandlerSessionListKeepsNativeCodexMirrorWhenTrackedByMobileVC(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	now := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	localRecord := data.SessionRecord{
		Summary: data.SessionSummary{
			ID:          "mobilevc-session-1",
			Title:       "MobileVC Codex",
			CreatedAt:   now.Add(-time.Hour),
			UpdatedAt:   now,
			LastPreview: "Follow up from mobile",
			Runtime: data.SessionRuntime{
				Source: "mobilevc",
			},
			Source: "mobilevc",
		},
		Projection: data.ProjectionSnapshot{
			Runtime: data.SessionRuntime{
				Command: "codex",
				Engine:  "codex",
				CWD:     projectDir,
				Source:  "mobilevc",
			},
			Controller: session.ControllerSnapshot{
				SessionID:       "codex-thread:" + threadID,
				ClaudeLifecycle: "resumable",
			},
			LogEntries: []data.SnapshotLogEntry{
				{Kind: "user", Message: "continue this task", Timestamp: now.Format(time.RFC3339)},
			},
		},
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), localRecord); err != nil {
		t.Fatalf("upsert local session: %v", err)
	}
	thread, err := codexsync.FindNativeThread(context.Background(), threadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), codexsync.MirrorRecord(thread)); err != nil {
		t.Fatalf("upsert mirror session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session list request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}

	var foundLocal, foundNative bool
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		switch item["id"] {
		case localRecord.Summary.ID:
			foundLocal = true
		case "codex-thread:" + threadID:
			foundNative = true
		}
	}
	if !foundLocal {
		t.Fatalf("expected managed MobileVC Codex session in list, got %#v", items)
	}
	if !foundNative {
		t.Fatalf("expected native Codex mirror to stay visible beside MobileVC session, got %#v", items)
	}
}

func TestHandlerIgnoresAutoBindSessionLoadWhenSessionAlreadySelected(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "session-a",
	}); err != nil {
		t.Fatalf("write session create request A: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload A, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected session A id, got %#v", createdA)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "session-b",
	}); err != nil {
		t.Fatalf("write session create request B: %v", err)
	}
	createdB := readUntilSessionCreated(t, conn)
	summaryB, ok := createdB["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload B, got %#v", createdB)
	}
	sessionB, _ := summaryB["id"].(string)
	if sessionB == "" {
		t.Fatalf("expected session B id, got %#v", createdB)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   sessionA,
		Reason:      "auto_bind",
	}); err != nil {
		t.Fatalf("write auto_bind session load request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	if got, _ := listEvent["sessionId"].(string); got != sessionB {
		t.Fatalf("expected current session %q to remain selected, got %#v", sessionB, listEvent)
	}
}

func TestFilterStoreSessionsByCWDAcceptsSymlinkEquivalentPaths(t *testing.T) {
	rootDir := t.TempDir()
	projectDir := filepath.Join(rootDir, "workspace", "MobileVC")
	aliasDir := filepath.Join(rootDir, "alias", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(aliasDir), 0o755); err != nil {
		t.Fatalf("mkdir alias parent: %v", err)
	}
	if err := os.Symlink(projectDir, aliasDir); err != nil {
		t.Skipf("symlink not available: %v", err)
	}

	items := []data.SessionSummary{
		{
			ID: "matching",
			Runtime: data.SessionRuntime{
				CWD: projectDir,
			},
		},
		{
			ID: "other",
			Runtime: data.SessionRuntime{
				CWD: filepath.Join(rootDir, "workspace", "Other"),
			},
		},
	}

	filtered := filterStoreSessionsByCWD(items, aliasDir)
	if len(filtered) != 1 || filtered[0].ID != "matching" {
		t.Fatalf("expected symlink-equivalent cwd to match, got %#v", filtered)
	}
}

func TestDedupeCodexThreadSummariesKeepsMobileVCAndNativeThread(t *testing.T) {
	threadID := "thread-1"
	projectDir := "/tmp/project"
	local := data.SessionSummary{
		ID:        "mobilevc-session",
		Title:     "MobileVC Codex",
		UpdatedAt: time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC),
		Runtime: data.SessionRuntime{
			ResumeSessionID: threadID,
			Command:         "codex",
			Engine:          "codex",
			CWD:             projectDir,
			Source:          "mobilevc",
		},
		Source: "mobilevc",
	}
	external := data.SessionSummary{
		ID:        "codex-thread:" + threadID,
		Title:     "Desktop Codex",
		UpdatedAt: time.Date(2026, 3, 30, 11, 30, 0, 0, time.UTC),
		Runtime: data.SessionRuntime{
			ResumeSessionID: threadID,
			Command:         "codex",
			Engine:          "codex",
			CWD:             projectDir,
			Source:          "codex-native",
		},
		Source:   "codex-native",
		External: true,
	}

	items := dedupeCodexThreadSummaries([]data.SessionSummary{local, external})
	if len(items) != 2 {
		t.Fatalf("expected MobileVC and native Codex entries to remain distinct, got %#v", items)
	}
	ids := map[string]bool{}
	for _, item := range items {
		ids[item.ID] = true
	}
	if !ids[local.ID] || !ids[external.ID] {
		t.Fatalf("expected both MobileVC and native Codex entries, got %#v", items)
	}
}

func TestMergeSessionSummariesExcludesStoredCodexSubagentMirrors(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	seedNativeCodexThreadsFixture(t, homeDir, []nativeCodexThreadFixture{
		{ID: "main-thread", CWD: projectDir, Title: "Main desktop session"},
		{ID: "exec-thread", CWD: projectDir, Title: "Exec session", Source: "exec"},
		{ID: "subagent-thread", CWD: projectDir, Title: "Worker session", ThreadSource: "subagent"},
	})

	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	createdAt := time.Date(2026, 3, 30, 10, 0, 0, 0, time.UTC)
	if _, err := tempStore.UpsertSession(context.Background(), data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "codex-thread:subagent-thread",
			Title:     "Stored worker mirror",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Runtime: data.SessionRuntime{
				ResumeSessionID: "subagent-thread",
				Command:         "codex",
				Engine:          "codex",
				CWD:             projectDir,
				Source:          "codex-native",
			},
			Source:   "codex-native",
			External: true,
		},
	}); err != nil {
		t.Fatalf("upsert stored subagent mirror: %v", err)
	}
	if _, err := tempStore.UpsertSession(context.Background(), data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "codex-thread:exec-thread",
			Title:     "Stored exec mirror",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Runtime: data.SessionRuntime{
				ResumeSessionID: "exec-thread",
				Command:         "codex",
				Engine:          "codex",
				CWD:             projectDir,
				Source:          "codex-native",
			},
			Source:   "codex-native",
			External: true,
		},
	}); err != nil {
		t.Fatalf("upsert stored exec mirror: %v", err)
	}

	items, err := tempStore.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("list stored sessions: %v", err)
	}
	merged, err := mergeSessionSummaries(context.Background(), tempStore, items, projectDir)
	if err != nil {
		t.Fatalf("merge session summaries: %v", err)
	}
	for _, item := range merged {
		if item.ID == "codex-thread:subagent-thread" {
			t.Fatalf("did not expect stored subagent mirror in list, got %#v", merged)
		}
		if item.ID == "codex-thread:exec-thread" {
			t.Fatalf("did not expect stored exec mirror in list, got %#v", merged)
		}
	}
	if len(merged) != 1 || merged[0].ID != "codex-thread:main-thread" {
		t.Fatalf("expected only main native thread, got %#v", merged)
	}
}

func TestMergeSessionSummariesEmptyCWDIncludesNativeProjects(t *testing.T) {
	homeDir := t.TempDir()
	projectA := filepath.Join(homeDir, "workspace", "MobileVC")
	projectB := filepath.Join(homeDir, "workspace", "ClaudeProject")
	if err := os.MkdirAll(projectA, 0o755); err != nil {
		t.Fatalf("mkdir project A: %v", err)
	}
	if err := os.MkdirAll(projectB, 0o755); err != nil {
		t.Fatalf("mkdir project B: %v", err)
	}
	t.Setenv("HOME", homeDir)
	seedNativeCodexThreadsFixture(t, homeDir, []nativeCodexThreadFixture{
		{ID: "codex-main", CWD: projectA, Title: "Desktop Codex"},
	})
	if err := claudesync.WriteSessionToJSONL(projectB, "claude-main", []claudesync.JSONLEvent{
		{Type: "user", Text: "Claude project prompt"},
	}); err != nil {
		t.Fatalf("write claude fixture: %v", err)
	}

	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	merged, err := mergeSessionSummaries(context.Background(), tempStore, nil, "")
	if err != nil {
		t.Fatalf("merge session summaries: %v", err)
	}

	ids := map[string]data.SessionSummary{}
	for _, item := range merged {
		ids[item.ID] = item
	}
	if item, ok := ids["codex-thread:codex-main"]; !ok || item.Runtime.CWD != projectA {
		t.Fatalf("expected native codex project session, got %#v", merged)
	}
	if item, ok := ids["claude-session:claude-main"]; !ok || item.Runtime.CWD != projectB {
		t.Fatalf("expected native claude project session, got %#v", merged)
	}
}

func TestMergeSessionSummariesListsAllVisibleCodexTUIThreads(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	seedNativeCodexThreadsFixture(t, homeDir, []nativeCodexThreadFixture{
		{ID: "visible-cli-user", CWD: projectDir, Title: "CLI user", Source: "cli", ThreadSource: "user"},
		{ID: "visible-vscode-user", CWD: projectDir, Title: "VS Code user", Source: "vscode", ThreadSource: "user"},
		{ID: "visible-vscode-blank-1", CWD: projectDir, Title: "VS Code blank 1", Source: "vscode"},
		{ID: "visible-vscode-blank-2", CWD: projectDir, Title: "VS Code blank 2", Source: "vscode"},
		{ID: "visible-cli-blank", CWD: projectDir, Title: "CLI blank", Source: "cli"},
		{ID: "visible-cli-user-2", CWD: projectDir, Title: "CLI user 2", Source: "cli", ThreadSource: "user"},
		{ID: "hidden-subagent", CWD: projectDir, Title: "Worker", Source: "cli", ThreadSource: "subagent"},
		{ID: "hidden-exec", CWD: projectDir, Title: "Exec", Source: "exec"},
	})

	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	createdAt := time.Date(2026, 3, 30, 12, 0, 0, 0, time.UTC)
	if _, err := tempStore.UpsertSession(context.Background(), data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "mobilevc-codex",
			Title:     "MobileVC tracked",
			CreatedAt: createdAt,
			UpdatedAt: createdAt,
			Runtime: data.SessionRuntime{
				ResumeSessionID: "visible-cli-user",
				Command:         "codex",
				Engine:          "codex",
				CWD:             projectDir,
				Source:          "mobilevc",
			},
			Source: "mobilevc",
		},
		Projection: data.ProjectionSnapshot{
			Runtime: data.SessionRuntime{
				ResumeSessionID: "visible-cli-user",
				Command:         "codex",
				Engine:          "codex",
				CWD:             projectDir,
				Source:          "mobilevc",
			},
		},
	}); err != nil {
		t.Fatalf("upsert tracked MobileVC session: %v", err)
	}

	items, err := tempStore.ListSessions(context.Background())
	if err != nil {
		t.Fatalf("list stored sessions: %v", err)
	}
	merged, err := mergeSessionSummaries(context.Background(), tempStore, items, projectDir)
	if err != nil {
		t.Fatalf("merge session summaries: %v", err)
	}

	visibleIDs := []string{
		"visible-cli-user",
		"visible-vscode-user",
		"visible-vscode-blank-1",
		"visible-vscode-blank-2",
		"visible-cli-blank",
		"visible-cli-user-2",
	}
	for _, threadID := range visibleIDs {
		if !containsSessionSummaryID(merged, "codex-thread:"+threadID) {
			t.Fatalf("expected visible Codex thread %q in merged list, got %#v", threadID, merged)
		}
	}
	if !containsSessionSummaryID(merged, "mobilevc-codex") {
		t.Fatalf("expected MobileVC tracked Codex session to remain, got %#v", merged)
	}
	if containsSessionSummaryID(merged, "codex-thread:hidden-subagent") || containsSessionSummaryID(merged, "codex-thread:hidden-exec") {
		t.Fatalf("did not expect hidden Codex threads in merged list, got %#v", merged)
	}
}

func TestHandlerSessionLoadMirrorsNativeCodexSession(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	mirrorID := "codex-thread:" + threadID
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}

	history := readUntilSessionHistory(t, conn)
	if history["sessionId"] != mirrorID {
		t.Fatalf("expected mirror session history, got %#v", history)
	}
	if history["canResume"] != true {
		t.Fatalf("expected resumable native history, got %#v", history)
	}
	summary, _ := history["summary"].(map[string]any)
	if summary["source"] != "codex-native" || summary["external"] != true {
		t.Fatalf("expected codex-native external summary, got %#v", summary)
	}
	resumeMeta, _ := history["resumeRuntimeMeta"].(map[string]any)
	if resumeMeta["engine"] != "codex" || resumeMeta["resumeSessionId"] != threadID {
		t.Fatalf("expected codex resume runtime meta, got %#v", resumeMeta)
	}
	entries, ok := history["logEntries"].([]any)
	if !ok || len(entries) == 0 {
		t.Fatalf("expected mirrored native history log entries, got %#v", history)
	}
	foundAssistant := false
	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if entry["kind"] == "markdown" &&
			strings.Contains(
				fmt.Sprint(entry["message"], entry["text"]),
				"Mobile labels aligned",
			) {
			foundAssistant = true
			break
		}
	}
	if !foundAssistant {
		t.Fatalf("expected assistant markdown entry mirrored from rollout, got %#v", entries)
	}
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	agentState := readUntilType(t, conn, protocol.EventTypeAgentState)
	requireAgentState(t, agentState, "IDLE", false)
	if agentState["claudeLifecycle"] != "resumable" {
		t.Fatalf("expected native mirrored session to restore resumable lifecycle, got %#v", agentState)
	}

	record, err := h.SessionStore.GetSession(context.Background(), mirrorID)
	if err != nil {
		t.Fatalf("expected mirrored native session persisted: %v", err)
	}
	if record.Summary.Source != "codex-native" || !record.Summary.External {
		t.Fatalf("expected mirrored native summary flags, got %#v", record.Summary)
	}
	if !strings.Contains(record.Summary.LastPreview, "Mobile labels aligned") {
		t.Fatalf("expected mirrored preview to include assistant reply, got %#v", record.Summary)
	}
}

func TestLoadSessionRecordReturnsCodexMirrorAfterUpsertWithoutSecondRead(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	store := &countingStore{inner: fileStore}
	mirrorID := "codex-thread:" + threadID

	record, err := loadSessionRecord(context.Background(), store, mirrorID)
	if err != nil {
		t.Fatalf("load codex mirror session: %v", err)
	}
	if record.Summary.ID != mirrorID {
		t.Fatalf("expected mirror record %q, got %#v", mirrorID, record.Summary)
	}
	if got := store.getCallCount(); got != 1 {
		t.Fatalf("expected only the initial cache lookup GetSession call, got %d", got)
	}
}

func TestLoadSessionRecordUsesNativeCodexHistoryOverStaleMirrorCache(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	mirrorID := "codex-thread:" + threadID
	thread, err := codexsync.FindNativeThread(context.Background(), threadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	record := codexsync.MirrorRecord(thread)
	if _, err := fileStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert mirror session: %v", err)
	}
	if _, err := fileStore.SaveProjection(context.Background(), mirrorID, data.ProjectionSnapshot{
		LogEntries: []data.SnapshotLogEntry{{
			Kind:      "markdown",
			Message:   "stale cached MobileVC-only message",
			Timestamp: "2026-05-27T05:54:13Z",
		}},
		Runtime: data.SessionRuntime{
			ResumeSessionID: threadID,
			Command:         "codex resume " + threadID,
			Engine:          "codex",
			CWD:             projectDir,
			Source:          "codex-native",
		},
	}); err != nil {
		t.Fatalf("save cached projection: %v", err)
	}

	loaded, err := loadSessionRecord(context.Background(), fileStore, mirrorID)
	if err != nil {
		t.Fatalf("load codex mirror session: %v", err)
	}
	foundNative := false
	for _, entry := range loaded.Projection.LogEntries {
		if entry.Message == "stale cached MobileVC-only message" {
			t.Fatalf("expected stale cached mirror log entry to be dropped, got %#v", loaded.Projection.LogEntries)
		}
		if strings.Contains(entry.Message, "Mobile labels aligned") {
			foundNative = true
		}
	}
	if !foundNative {
		t.Fatalf("expected native codex history to remain authoritative, got %#v", loaded.Projection.LogEntries)
	}
}

func TestLoadSessionRecordUsesNativeClaudeHistoryOverStaleMirrorCache(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "ClaudeProject")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	uuid := "claude-main"
	if err := claudesync.WriteSessionToJSONL(projectDir, uuid, []claudesync.JSONLEvent{
		{Type: "user", Text: "native claude prompt", Timestamp: "2026-05-27T01:00:00Z"},
		{Type: "assistant", Text: "native claude answer", Timestamp: "2026-05-27T01:01:00Z"},
	}); err != nil {
		t.Fatalf("write claude fixture: %v", err)
	}

	fileStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	mirrorID := claudesync.MirrorSessionID(uuid)
	native, err := claudesync.FindNativeSession(context.Background(), mirrorID)
	if err != nil {
		t.Fatalf("find native claude session: %v", err)
	}
	record := claudesync.MirrorRecord(native)
	if _, err := fileStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert claude mirror session: %v", err)
	}
	if _, err := fileStore.SaveProjection(context.Background(), mirrorID, data.ProjectionSnapshot{
		LogEntries: []data.SnapshotLogEntry{{
			Kind:      "markdown",
			Message:   "stale cached claude mirror message",
			Timestamp: "2026-05-27T02:00:00Z",
		}},
		Runtime: data.SessionRuntime{
			ResumeSessionID: uuid,
			Command:         "claude --resume " + uuid,
			Engine:          "claude",
			CWD:             projectDir,
			Source:          "claude-native",
		},
		SessionContext: data.SessionContext{
			EnabledSkillNames: []string{"review"},
			Configured:        true,
		},
		SessionContextSet: true,
	}); err != nil {
		t.Fatalf("save cached claude projection: %v", err)
	}

	loaded, err := loadSessionRecord(context.Background(), fileStore, mirrorID)
	if err != nil {
		t.Fatalf("load claude mirror session: %v", err)
	}
	foundNative := false
	for _, entry := range loaded.Projection.LogEntries {
		if entry.Message == "stale cached claude mirror message" {
			t.Fatalf("expected stale cached claude mirror log entry to be dropped, got %#v", loaded.Projection.LogEntries)
		}
		if entry.Message == "native claude answer" {
			foundNative = true
		}
	}
	if !foundNative {
		t.Fatalf("expected native claude history to remain authoritative, got %#v", loaded.Projection.LogEntries)
	}
	if got := loaded.Projection.SessionContext.EnabledSkillNames; len(got) != 1 || got[0] != "review" {
		t.Fatalf("expected MobileVC overlay session context to be preserved, got %#v", loaded.Projection.SessionContext)
	}
}

func TestMergeSnapshotLogEntriesPreservesAppendOrder(t *testing.T) {
	merged := mergeSnapshotLogEntries(
		[]data.SnapshotLogEntry{{
			Kind:      "markdown",
			Message:   "new runtime state",
			Timestamp: "2026-05-27T05:54:13Z",
		}},
		[]data.SnapshotLogEntry{{
			Kind:      "markdown",
			Message:   "older cached state",
			Timestamp: "2026-05-27T00:13:17Z",
		}},
	)
	if len(merged) != 2 {
		t.Fatalf("expected two merged entries, got %#v", merged)
	}
	if merged[0].Message != "new runtime state" || merged[1].Message != "older cached state" {
		t.Fatalf("expected append order to be preserved for generic merge, got %#v", merged)
	}
}

func TestHandlerSessionLoadReturnsLimitedHistoryWindowAndOlderPage(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	now := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC)
	record := data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "history-window-session",
			Title:     "History Window",
			CreatedAt: now,
			UpdatedAt: now,
		},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{
				{Kind: "markdown", Message: "one", Timestamp: now.Add(time.Second).Format(time.RFC3339)},
				{Kind: "markdown", Message: "two", Timestamp: now.Add(2 * time.Second).Format(time.RFC3339)},
				{Kind: "markdown", Message: "three", Timestamp: now.Add(3 * time.Second).Format(time.RFC3339)},
				{Kind: "markdown", Message: "four", Timestamp: now.Add(4 * time.Second).Format(time.RFC3339)},
			},
		},
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   record.Summary.ID,
		Limit:       2,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	if got := int(history["logEntryStart"].(float64)); got != 2 {
		t.Fatalf("history start: got %d", got)
	}
	if got := int(history["logEntryTotal"].(float64)); got != 4 {
		t.Fatalf("history total: got %d", got)
	}
	if history["hasMoreBefore"] != true {
		t.Fatalf("expected hasMoreBefore=true, got %#v", history)
	}
	entries := history["logEntries"].([]any)
	if len(entries) != 2 || entries[0].(map[string]any)["message"] != "three" || entries[1].(map[string]any)["message"] != "four" {
		t.Fatalf("unexpected limited history entries: %#v", entries)
	}

	if err := conn.WriteJSON(protocol.SessionHistoryPageRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_history_page"},
		SessionID:   record.Summary.ID,
		Before:      2,
		Limit:       2,
	}); err != nil {
		t.Fatalf("write session_history_page request: %v", err)
	}
	page := readUntilSessionHistoryPage(t, conn)
	if got := intFromJSONNumber(page["logEntryStart"]); got != 0 {
		t.Fatalf("page start: got %d", got)
	}
	if page["hasMoreBefore"] == true {
		t.Fatalf("expected page hasMoreBefore=false, got %#v", page)
	}
	pageEntries := page["logEntries"].([]any)
	if len(pageEntries) != 2 || pageEntries[0].(map[string]any)["message"] != "one" || pageEntries[1].(map[string]any)["message"] != "two" {
		t.Fatalf("unexpected page entries: %#v", pageEntries)
	}
}

func TestHandlerSessionLoadDefaultsToBoundedHistoryWindow(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	now := time.Date(2026, 5, 31, 19, 0, 0, 0, time.UTC)
	entries := make([]data.SnapshotLogEntry, sessionResumeHistoryLimit+4)
	for i := range entries {
		entries[i] = data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   fmt.Sprintf("load-entry-%03d", i+1),
			Timestamp: now.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
	}
	record := data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        "load-default-window-session",
			Title:     "Load Default Window",
			CreatedAt: now,
			UpdatedAt: now,
		},
		Projection: data.ProjectionSnapshot{LogEntries: entries},
	}
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   record.Summary.ID,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	if got := intFromJSONNumber(history["logEntryStart"]); got != 4 {
		t.Fatalf("history start: got %d", got)
	}
	if got := intFromJSONNumber(history["logEntryTotal"]); got != sessionResumeHistoryLimit+4 {
		t.Fatalf("history total: got %d", got)
	}
	if history["hasMoreBefore"] != true {
		t.Fatalf("expected hasMoreBefore=true, got %#v", history)
	}
	gotEntries := history["logEntries"].([]any)
	if len(gotEntries) != sessionResumeHistoryLimit {
		t.Fatalf("expected %d entries, got %d", sessionResumeHistoryLimit, len(gotEntries))
	}
	if gotEntries[0].(map[string]any)["message"] != "load-entry-005" {
		t.Fatalf("unexpected first bounded entry: %#v", gotEntries[0])
	}
}

func TestHandlerSessionLoadLimitsOversizedPayloadBeforeWebsocketEmission(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	record := newOversizedGatewayPayloadRecord("oversized-load-session", 9, 1024*1024)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   record.Summary.ID,
		Limit:       len(record.Projection.LogEntries),
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	requireGatewayPayloadWithinBudget(t, history)
	requirePayloadLimited(t, history)
	if got := jsonArrayLength(history, "logEntries"); got >= len(record.Projection.LogEntries) {
		t.Fatalf("expected oversized session_load history to be trimmed, got %d entries", got)
	}
	requireLatestLogEntryCount(t, history, len(record.Projection.LogEntries))
}

func TestHandlerSessionHistoryPageLimitsOversizedPayloadBeforeWebsocketEmission(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	record := newOversizedGatewayPayloadRecord("oversized-page-session", 9, 1024*1024)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   record.Summary.ID,
		Limit:       1,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)
	if err := conn.WriteJSON(protocol.SessionHistoryPageRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_history_page"},
		SessionID:   record.Summary.ID,
		Before:      len(record.Projection.LogEntries),
		Limit:       len(record.Projection.LogEntries),
	}); err != nil {
		t.Fatalf("write session_history_page request: %v", err)
	}

	page := readUntilSessionHistoryPage(t, conn)
	requireGatewayPayloadWithinBudget(t, page)
	requirePayloadLimited(t, page)
	if got := jsonArrayLength(page, "logEntries"); got >= len(record.Projection.LogEntries) {
		t.Fatalf("expected oversized history page to be trimmed, got %d entries", got)
	}
	if got := intFromJSONNumber(page["logEntryTotal"]); got != len(record.Projection.LogEntries) {
		t.Fatalf("page total: got %d want %d", got, len(record.Projection.LogEntries))
	}
}

func TestHandlerSessionDeltaOversizedPayloadRequiresFullSync(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	record := newOversizedGatewayPayloadRecord("oversized-delta-session", 9, 1024*1024)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert session: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   record.Summary.ID,
		Limit:       1,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)
	if err := conn.WriteJSON(protocol.SessionDeltaRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delta_get"},
		SessionID:   record.Summary.ID,
		Known:       protocol.SessionDeltaKnown{},
	}); err != nil {
		t.Fatalf("write session_delta_get request: %v", err)
	}

	delta := readUntilType(t, conn, protocol.EventTypeSessionDelta)
	requireGatewayPayloadWithinBudget(t, delta)
	requirePayloadLimited(t, delta)
	if delta["requiresFullSync"] != true {
		t.Fatalf("expected oversized delta to require full sync, got %#v", delta)
	}
	if entries, ok := delta["appendLogEntries"].([]any); ok && len(entries) > 0 {
		t.Fatalf("expected oversized delta to omit append entries, got %d", len(entries))
	}
	requireLatestLogEntryCount(t, delta, len(record.Projection.LogEntries))
}

func newOversizedGatewayPayloadRecord(sessionID string, entryCount, messageBytes int) data.SessionRecord {
	now := time.Date(2026, 6, 6, 17, 10, 0, 0, time.UTC)
	entries := make([]data.SnapshotLogEntry, entryCount)
	for i := range entries {
		entries[i] = data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   strings.Repeat(string(rune('a'+i)), messageBytes),
			Timestamp: now.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
	}
	return data.SessionRecord{
		Summary: data.SessionSummary{
			ID:        sessionID,
			Title:     sessionID,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Projection: data.ProjectionSnapshot{LogEntries: entries},
	}
}

func requireGatewayPayloadWithinBudget(t *testing.T, event map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal emitted event: %v", err)
	}
	if len(encoded) > gatewayRelaySafePayloadBudgetBytes {
		t.Fatalf("emitted event is %d bytes, exceeds gateway budget %d", len(encoded), gatewayRelaySafePayloadBudgetBytes)
	}
}

func requirePayloadLimited(t *testing.T, event map[string]any) {
	t.Helper()
	if limited, _ := event["payloadLimited"].(bool); !limited {
		t.Fatalf("expected payloadLimited=true")
	}
	if reason, _ := event["payloadLimitReason"].(string); strings.TrimSpace(reason) == "" {
		t.Fatalf("expected payloadLimitReason")
	}
}

func jsonArrayLength(event map[string]any, key string) int {
	if entries, ok := event[key].([]any); ok {
		return len(entries)
	}
	return 0
}

func requireLatestLogEntryCount(t *testing.T, event map[string]any, want int) {
	t.Helper()
	latest, ok := event["latest"].(map[string]any)
	if !ok {
		t.Fatalf("expected latest cursor metadata")
	}
	if got := intFromJSONNumber(latest["logEntryCount"]); got != want {
		t.Fatalf("latest log entry count: got %d want %d", got, want)
	}
}

func intFromJSONNumber(value any) int {
	if value == nil {
		return 0
	}
	return int(value.(float64))
}

func TestHandlerSessionLoadPreservesCodexMirrorOverlayButDropsStaleCachedLogs(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	thread, err := codexsync.FindNativeThread(context.Background(), threadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	record := codexsync.MirrorRecord(thread)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert mirror session: %v", err)
	}
	if _, err := h.SessionStore.SaveProjection(context.Background(), record.Summary.ID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "cached assistant output", "stderr": ""},
		LogEntries: append(record.Projection.LogEntries, data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   "Cached MobileVC follow-up",
			Timestamp: "2026-04-04T02:05:00Z",
		}),
		Runtime: data.SessionRuntime{
			ResumeSessionID: threadID,
			Command:         "codex resume " + threadID,
			Engine:          "codex",
			CWD:             projectDir,
			PermissionMode:  "default",
			ClaudeLifecycle: "waiting_input",
			Source:          "codex-native",
		},
		Controller: session.ControllerSnapshot{
			SessionID:       record.Summary.ID,
			State:           session.ControllerStateWaitInput,
			CurrentCommand:  "codex resume " + threadID,
			ResumeSession:   threadID,
			ClaudeLifecycle: "waiting_input",
			ActiveMeta: protocol.RuntimeMeta{
				ResumeSessionID: threadID,
				Command:         "codex resume " + threadID,
				Engine:          "codex",
				CWD:             projectDir,
				PermissionMode:  "default",
				ClaudeLifecycle: "waiting_input",
			},
		},
		CurrentStep: &data.SnapshotContext{
			ID:      "step-cached",
			Type:    "step",
			Message: "等待输入",
			Status:  "WAIT_INPUT",
			Title:   "等待输入",
		},
	}); err != nil {
		t.Fatalf("save cached mirror projection: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	mirrorID := "codex-thread:" + threadID
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}

	history := readUntilSessionHistory(t, conn)
	logEntries, ok := history["logEntries"].([]any)
	if !ok {
		t.Fatalf("expected history log entries, got %#v", history)
	}
	for _, raw := range logEntries {
		entry, _ := raw.(map[string]any)
		if entry["message"] == "Cached MobileVC follow-up" {
			t.Fatalf("expected cached mirror-only log entry to be dropped, got %#v", logEntries)
		}
	}
	resumeMeta, _ := history["resumeRuntimeMeta"].(map[string]any)
	if resumeMeta["claudeLifecycle"] != "waiting_input" {
		t.Fatalf("expected cached waiting_input lifecycle on history, got %#v", resumeMeta)
	}

	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	agentState := readUntilType(t, conn, protocol.EventTypeAgentState)
	if agentState["state"] != "WAIT_INPUT" {
		t.Fatalf("expected restored WAIT_INPUT state, got %#v", agentState)
	}
	if agentState["msg"] != "等待输入" {
		t.Fatalf("expected restored waiting message, got %#v", agentState)
	}
}

func TestHandlerCompactRestartsLoadedCodexMirrorSession(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	resumed := newInteractiveHoldingStubRunner()
	h.NewPtyRunner = func() engine.Runner { return resumed }

	thread, err := codexsync.FindNativeThread(context.Background(), threadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	record := codexsync.MirrorRecord(thread)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert mirror session: %v", err)
	}
	if _, err := h.SessionStore.SaveProjection(context.Background(), record.Summary.ID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		LogEntries:          record.Projection.LogEntries,
		Runtime: data.SessionRuntime{
			ResumeSessionID:  threadID,
			Command:          "codex resume " + threadID,
			Engine:           "codex",
			CWD:              projectDir,
			PermissionMode:   "default",
			CodexSandboxMode: "danger-full-access",
			ClaudeLifecycle:  "resumable",
			Source:           "codex-native",
		},
		Controller: session.ControllerSnapshot{
			SessionID:       record.Summary.ID,
			State:           session.ControllerStateIdle,
			CurrentCommand:  "codex resume " + threadID,
			ResumeSession:   threadID,
			ClaudeLifecycle: "resumable",
			ActiveMeta: protocol.RuntimeMeta{
				ResumeSessionID:  threadID,
				Command:          "codex resume " + threadID,
				Engine:           "codex",
				CWD:              projectDir,
				PermissionMode:   "default",
				CodexSandboxMode: "danger-full-access",
				ClaudeLifecycle:  "resumable",
			},
		},
	}); err != nil {
		t.Fatalf("save mirror projection: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	mirrorID := "codex-thread:" + threadID
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact", SessionID: mirrorID},
	}); err != nil {
		t.Fatalf("write compact request: %v", err)
	}

	compact := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if compact["accepted"] != false {
		t.Fatalf("expected compact to fail against stub runner lacking compactor, got %#v", compact)
	}
	if msg, _ := compact["error"].(string); !strings.Contains(msg, "input not supported") {
		t.Fatalf("expected compactor-not-supported error after resume restart, got %#v", compact)
	}

	resumed.WaitStarted(t)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(resumed.lastReq.Command)), "codex resume "+strings.ToLower(threadID)) {
		t.Fatalf("expected codex resume command on compact restore, got %q", resumed.lastReq.Command)
	}
	if got := resumed.lastReq.RuntimeMeta.CodexSandboxMode; got != "danger-full-access" {
		t.Fatalf("expected codex sandbox on compact restore, got %q", got)
	}
}

func TestHandlerCompactLoadedCodexMirrorIgnoresStaleClaudeRuntimeCache(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore

	resumed := newInteractiveHoldingStubRunner()
	h.NewPtyRunner = func() engine.Runner { return resumed }

	thread, err := codexsync.FindNativeThread(context.Background(), threadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	record := codexsync.MirrorRecord(thread)
	if _, err := h.SessionStore.UpsertSession(context.Background(), record); err != nil {
		t.Fatalf("upsert mirror session: %v", err)
	}
	staleResumeID := "29fbef9d-63a2-4d04-bb84-f574b3ac6c26"
	staleCommand := "claude --resume " + staleResumeID + " --print --verbose --output-format stream-json --input-format stream-json --permission-prompt-tool stdio"
	if _, err := h.SessionStore.SaveProjection(context.Background(), record.Summary.ID, data.ProjectionSnapshot{
		RawTerminalByStream: map[string]string{"stdout": "stale output", "stderr": ""},
		LogEntries: append(record.Projection.LogEntries, data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   "Cached note from stale runtime",
			Timestamp: "2026-04-04T02:05:00Z",
		}),
		Runtime: data.SessionRuntime{
			ResumeSessionID: staleResumeID,
			Command:         staleCommand,
			Engine:          "codex",
			CWD:             projectDir,
			PermissionMode:  "auto",
			ClaudeLifecycle: "waiting_input",
			Source:          "mobilevc",
		},
		Controller: session.ControllerSnapshot{
			SessionID:       record.Summary.ID,
			State:           session.ControllerStateWaitInput,
			CurrentCommand:  staleCommand,
			ResumeSession:   staleResumeID,
			ClaudeLifecycle: "waiting_input",
			ActiveMeta: protocol.RuntimeMeta{
				ResumeSessionID: staleResumeID,
				Command:         staleCommand,
				Engine:          "codex",
				Model:           "sonnet",
				CWD:             projectDir,
				PermissionMode:  "auto",
				ClaudeLifecycle: "waiting_input",
			},
		},
		CurrentStep: &data.SnapshotContext{
			ID:      "step-stale",
			Type:    "step",
			Message: "等待输入",
			Status:  "WAIT_INPUT",
			Title:   "等待输入",
		},
	}); err != nil {
		t.Fatalf("save stale mirror projection: %v", err)
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	mirrorID := "codex-thread:" + threadID
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	resumeMeta, _ := history["resumeRuntimeMeta"].(map[string]any)
	if got := fmt.Sprint(resumeMeta["resumeSessionId"]); got != threadID {
		t.Fatalf("expected native codex thread resume id, got %#v", resumeMeta)
	}
	if got := strings.ToLower(strings.TrimSpace(fmt.Sprint(resumeMeta["command"]))); !strings.HasPrefix(got, "codex") {
		t.Fatalf("expected native codex command after stale cache sanitize, got %#v", resumeMeta)
	}
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	if err := conn.WriteJSON(protocol.CompactRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "compact", SessionID: mirrorID},
	}); err != nil {
		t.Fatalf("write compact request: %v", err)
	}

	compact := readUntilType(t, conn, protocol.EventTypeCompactResult)
	if compact["accepted"] != false {
		t.Fatalf("expected compact to fail against stub runner lacking compactor, got %#v", compact)
	}
	if msg, _ := compact["error"].(string); !strings.Contains(msg, "input not supported") {
		t.Fatalf("expected compactor-not-supported error after codex resume restart, got %#v", compact)
	}

	resumed.WaitStarted(t)
	lower := strings.ToLower(strings.TrimSpace(resumed.lastReq.Command))
	if !strings.HasPrefix(lower, "codex resume "+strings.ToLower(threadID)) {
		t.Fatalf("expected stale claude cache to be ignored in compact restore, got %q", resumed.lastReq.Command)
	}
}

func TestHandlerSessionDeleteRejectsNativeCodexMirror(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	mirrorID := "codex-thread:" + threadID
	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)

	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delete"},
		SessionID:   mirrorID,
	}); err != nil {
		t.Fatalf("write session delete request: %v", err)
	}

	errorEvent := readUntilType(t, conn, protocol.EventTypeError)
	if errorEvent["msg"] != "Codex 原生会话仅支持恢复，不支持在 MobileVC 内删除" {
		t.Fatalf("unexpected error event: %#v", errorEvent)
	}
	if errorEvent["code"] != "session_delete_failed" {
		t.Fatalf("expected session_delete_failed code, got %#v", errorEvent)
	}
	if _, err := h.SessionStore.GetSession(context.Background(), mirrorID); err != nil {
		t.Fatalf("expected mirrored native session to remain after delete rejection: %v", err)
	}
}

func TestHandlerSessionDeleteCurrentSessionCleansRuntimeAndFallsBack(t *testing.T) {
	runnerA := newSwitchableStubRunner()
	firstRunner := runnerA
	runnerB := newSwitchableStubRunner()
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		if runnerA != nil {
			r := runnerA
			runnerA = nil
			return r
		}
		return runnerB
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write initial session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected session A id, got %#v", createdA)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	createdB := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryB, ok := createdB["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdB)
	}
	sessionB, _ := summaryB["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected distinct session B id, got %q", sessionB)
	}
	firstRunner.AssertNotClosed(t, 10*time.Millisecond)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request for session B: %v", err)
	}
	runnerB.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("refresh read deadline before delete: %v", err)
	}
	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_delete"}, SessionID: sessionB}); err != nil {
		t.Fatalf("write session delete request: %v", err)
	}

	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == sessionB {
			t.Fatalf("expected deleted current session removed from list, got %#v", items)
		}
	}
	history := readUntilSessionHistory(t, conn)
	if history["sessionId"] != sessionA {
		t.Fatalf("expected fallback history for session A, got %#v", history)
	}
	runnerB.WaitClosed(t)
	if _, err := h.SessionStore.GetSession(context.Background(), sessionB); err == nil {
		t.Fatal("expected deleted current session lookup to fail")
	}

	runnerB.Emit(protocol.NewLogEvent("ignored", "late output from deleted session B", "stdout"))
	runnerB.Emit(protocol.NewStepUpdateEvent("ignored", "late step from deleted session B", "running", "internal/ws/handler.go", "reading", "claude"))

	recordA, err := h.SessionStore.GetSession(context.Background(), sessionA)
	if err != nil {
		t.Fatalf("get session A: %v", err)
	}
	textsA := sessionLogTexts(recordA)
	if containsText(textsA, "late output from deleted session B") || containsText(textsA, "late step from deleted session B") {
		t.Fatalf("did not expect deleted session events to leak into fallback session, got %#v", textsA)
	}
}

func TestHandlerSessionDeleteBackgroundRunningSessionCleansRuntime(t *testing.T) {
	runnerA := newSwitchableStubRunner()
	firstRunner := runnerA
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		if runnerA != nil {
			r := runnerA
			runnerA = nil
			return r
		}
		return newSwitchableStubRunner()
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write initial session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected session A id, got %#v", createdA)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{ClientEvent: protocol.ClientEvent{Action: "exec"}, Command: "claude", Mode: "pty"}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	createdB := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryB, ok := createdB["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdB)
	}
	sessionB, _ := summaryB["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected distinct session B id, got %q", sessionB)
	}
	firstRunner.AssertNotClosed(t, 10*time.Millisecond)

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("refresh read deadline before delete: %v", err)
	}
	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_delete"}, SessionID: sessionA}); err != nil {
		t.Fatalf("write session delete request: %v", err)
	}
	listEvent := readUntilType(t, conn, protocol.EventTypeSessionListResult)
	items, ok := listEvent["items"].([]any)
	if !ok {
		t.Fatalf("expected session list items, got %#v", listEvent)
	}
	for _, raw := range items {
		item, _ := raw.(map[string]any)
		if item["id"] == sessionA {
			t.Fatalf("expected deleted background session removed from list, got %#v", items)
		}
	}
	firstRunner.WaitClosed(t)
	if entry := h.runtimeSessions.Get(sessionA); entry != nil {
		t.Fatalf("expected deleted background runtime removed, got %#v", entry)
	}
	if _, err := h.SessionStore.GetSession(context.Background(), sessionA); err == nil {
		t.Fatal("expected deleted background session lookup to fail")
	}

	firstRunner.Emit(protocol.NewLogEvent("ignored", "late output from deleted session A", "stdout"))
	assertNoEventType(t, conn, protocol.EventTypeLog, 150*time.Millisecond)
}

func TestHandlerSessionDeleteFallbackReturnsBoundedHistoryWindow(t *testing.T) {
	runnerA := newSwitchableStubRunner()
	runnerB := newSwitchableStubRunner()
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		if runnerA != nil {
			r := runnerA
			runnerA = nil
			return r
		}
		return runnerB
	}
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write initial session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected session A id, got %#v", createdA)
	}
	now := time.Date(2026, 5, 31, 18, 0, 0, 0, time.UTC)
	entries := make([]data.SnapshotLogEntry, sessionResumeHistoryLimit+3)
	for i := range entries {
		entries[i] = data.SnapshotLogEntry{
			Kind:      "markdown",
			Message:   fmt.Sprintf("fallback-entry-%03d", i+1),
			Timestamp: now.Add(time.Duration(i) * time.Second).Format(time.RFC3339),
		}
	}
	if _, err := h.SessionStore.SaveProjection(context.Background(), sessionA, data.ProjectionSnapshot{LogEntries: entries}); err != nil {
		t.Fatalf("save fallback projection: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	createdB := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryB, ok := createdB["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdB)
	}
	sessionB, _ := summaryB["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected distinct session B id, got %q", sessionB)
	}

	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_delete"}, SessionID: sessionB}); err != nil {
		t.Fatalf("write session delete request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	history := readUntilSessionHistory(t, conn)
	if history["sessionId"] != sessionA {
		t.Fatalf("expected fallback history for session A, got %#v", history)
	}
	if got := intFromJSONNumber(history["logEntryStart"]); got != 3 {
		t.Fatalf("history start: got %d", got)
	}
	if got := intFromJSONNumber(history["logEntryTotal"]); got != sessionResumeHistoryLimit+3 {
		t.Fatalf("history total: got %d", got)
	}
	if history["hasMoreBefore"] != true {
		t.Fatalf("expected hasMoreBefore=true, got %#v", history)
	}
	gotEntries := history["logEntries"].([]any)
	if len(gotEntries) != sessionResumeHistoryLimit {
		t.Fatalf("expected %d entries, got %d", sessionResumeHistoryLimit, len(gotEntries))
	}
	if gotEntries[0].(map[string]any)["message"] != "fallback-entry-004" {
		t.Fatalf("unexpected first fallback entry: %#v", gotEntries[0])
	}
}

func TestHandlerSessionDeleteCurrentSessionSkipsNativeCodexFallback(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	seedNativeCodexSessionFixture(t, homeDir, projectDir)

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "delete-me")
	if err := conn.WriteJSON(protocol.SessionListRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_list"},
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session_list request: %v", err)
	}
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)

	if err := conn.WriteJSON(protocol.SessionDeleteRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delete"},
		SessionID:   sessionID,
	}); err != nil {
		t.Fatalf("write session_delete request: %v", err)
	}

	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	state := readUntilType(t, conn, protocol.EventTypeSessionState)
	if sessionID, _ := state["sessionId"].(string); sessionID != "" {
		t.Fatalf("expected empty session state instead of native codex fallback, got %#v", state)
	}
	if state["msg"] != "session cleared" {
		t.Fatalf("expected session cleared state, got %#v", state)
	}
}

func TestHandlerSessionDeltaRefreshesNativeCodexMirror(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	threadID := seedNativeCodexSessionFixture(t, homeDir, projectDir)

	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	mirrorID := "codex-thread:" + threadID
	record, err := loadSessionRecord(context.Background(), tempStore, mirrorID)
	if err != nil {
		t.Fatalf("load codex mirror session: %v", err)
	}

	rolloutPath := ""
	if strings.TrimPrefix(record.Summary.Runtime.ResumeSessionID, "codex-thread:") != "" {
		thread, err := codexsync.FindNativeThread(context.Background(), mirrorID)
		if err != nil {
			t.Fatalf("find native thread: %v", err)
		}
		rolloutPath = thread.RolloutPath
	}
	if rolloutPath == "" {
		t.Fatal("expected native rollout path")
	}
	h := newTestHandler()
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   mirrorID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeReviewState)
	_ = readUntilType(t, conn, protocol.EventTypeAgentState)

	file, err := os.OpenFile(rolloutPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatalf("open rollout fixture: %v", err)
	}
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(map[string]any{
		"timestamp": "2026-04-04T02:06:00Z",
		"type":      "event_msg",
		"payload": map[string]any{
			"type":    "agent_message",
			"message": "Fresh native Codex delta",
			"phase":   "final_answer",
		},
	}); err != nil {
		_ = file.Close()
		t.Fatalf("append rollout fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close rollout fixture: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionDeltaRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delta_get"},
		SessionID:   mirrorID,
		Known: protocol.SessionDeltaKnown{
			LogEntryCount: len(record.Projection.LogEntries),
		},
	}); err != nil {
		t.Fatalf("write session_delta_get request: %v", err)
	}
	delta := readUntilType(t, conn, protocol.EventTypeSessionDelta)
	if delta["requiresFullSync"] == true {
		t.Fatalf("expected incremental delta, got full sync request: %#v", delta)
	}
	entries, ok := delta["appendLogEntries"].([]any)
	if !ok || len(entries) == 0 {
		t.Fatalf("expected appended cached entries, got %#v", delta)
	}
	foundFresh := false
	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if entry["message"] == "Fresh native Codex delta" {
			foundFresh = true
			break
		}
	}
	if !foundFresh {
		t.Fatalf("expected fresh native mirror log entry in delta, got %#v", entries)
	}
}

func TestHandlerSessionDeltaMergesClaudeJSONLForMobileVCSession(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "ClaudeProject")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	sessionID := createHistorySessionForHandlerTest(t, h, conn, "mobilevc-claude")
	record, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get created session: %v", err)
	}
	record.Summary.Runtime.CWD = projectDir
	record.Projection.Runtime = data.SessionRuntime{
		ResumeSessionID: record.Summary.ClaudeSessionUUID,
		Command:         "claude --resume " + record.Summary.ClaudeSessionUUID,
		Engine:          "claude",
		CWD:             projectDir,
		Source:          "mobilevc",
	}
	record.Projection.Controller.SessionID = sessionID
	record.Projection.LogEntries = []data.SnapshotLogEntry{{
		Kind:      "user",
		Label:     "历史输入",
		Message:   "mobile existing prompt",
		Timestamp: "2026-05-28T01:00:00Z",
	}}
	summary, err := h.SessionStore.UpsertSession(context.Background(), record)
	if err != nil {
		t.Fatalf("upsert mobilevc claude session: %v", err)
	}
	record.Summary = summary

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_load"},
		SessionID:   sessionID,
		CWD:         projectDir,
	}); err != nil {
		t.Fatalf("write session_load request: %v", err)
	}
	_ = readUntilSessionHistory(t, conn)

	if err := claudesync.WriteSessionToJSONL(projectDir, record.Summary.ClaudeSessionUUID, []claudesync.JSONLEvent{
		{Type: "assistant", Text: "Fresh desktop Claude answer", Timestamp: "2026-05-28T01:01:00Z"},
	}); err != nil {
		t.Fatalf("append claude jsonl: %v", err)
	}

	if err := conn.WriteJSON(protocol.SessionDeltaRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delta_get"},
		SessionID:   sessionID,
		Known: protocol.SessionDeltaKnown{
			LogEntryCount: len(record.Projection.LogEntries),
		},
	}); err != nil {
		t.Fatalf("write session_delta_get request: %v", err)
	}
	delta := readUntilType(t, conn, protocol.EventTypeSessionDelta)
	if delta["requiresFullSync"] == true {
		t.Fatalf("expected incremental delta, got full sync request: %#v", delta)
	}
	entries, ok := delta["appendLogEntries"].([]any)
	if !ok || len(entries) == 0 {
		t.Fatalf("expected claude jsonl entry in delta, got %#v", delta)
	}
	foundFresh := false
	for _, raw := range entries {
		entry, _ := raw.(map[string]any)
		if entry["message"] == "Fresh desktop Claude answer" {
			foundFresh = true
			break
		}
	}
	if !foundFresh {
		t.Fatalf("expected fresh desktop Claude entry in delta, got %#v", entries)
	}

	updated, err := h.SessionStore.GetSession(context.Background(), sessionID)
	if err != nil {
		t.Fatalf("get updated session: %v", err)
	}
	if updated.Summary.Ownership != "mobilevc" || updated.Summary.External {
		t.Fatalf("mobilevc-owned session should not become external: %#v", updated.Summary)
	}
}

func TestHandlerSessionDeltaRequiresFullSyncForMismatchedTarget(t *testing.T) {
	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	activeID := createHistorySessionForHandlerTest(t, h, conn, "active-session")
	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_create"},
		Title:       "other-session",
	}); err != nil {
		t.Fatalf("write other session create request: %v", err)
	}
	createdOther := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	otherSummary, _ := createdOther["summary"].(map[string]any)
	otherID, _ := otherSummary["id"].(string)
	if activeID == "" || otherID == "" || activeID == otherID {
		t.Fatalf("expected distinct sessions, active=%q other=%q", activeID, otherID)
	}

	if err := conn.WriteJSON(protocol.SessionDeltaRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "session_delta_get"},
		SessionID:   activeID,
		Known: protocol.SessionDeltaKnown{
			LogEntryCount: 1,
		},
	}); err != nil {
		t.Fatalf("write stale target session_delta_get request: %v", err)
	}
	delta := readUntilType(t, conn, protocol.EventTypeSessionDelta)
	if delta["sessionId"] != activeID {
		t.Fatalf("expected full sync delta for stale target %q, got %#v", activeID, delta)
	}
	if delta["requiresFullSync"] != true {
		t.Fatalf("expected requiresFullSync for non-selected target, got %#v", delta)
	}
}

func TestHandlerSessionLoadKeepsOldRunnerEventsInOriginalSessionProjection(t *testing.T) {
	runnerA := newSwitchableStubRunner()
	firstRunner := runnerA
	runnerB := newSwitchableStubRunner()

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		if runnerA != nil {
			r := runnerA
			runnerA = nil
			return r
		}
		return runnerB
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write initial session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected initial session id, got %#v", createdA)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	created := readUntilSessionCreated(t, conn)
	summary, ok := created["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", created)
	}
	sessionB, _ := summary["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected new session id, got %q", sessionB)
	}
	firstRunner.AssertNotClosed(t, 10*time.Millisecond)

	firstRunner.Emit(protocol.NewLogEvent("ignored", "late output from session A", "stdout"))
	firstRunner.Emit(protocol.NewStepUpdateEvent("ignored", "late step from session A", "running", "internal/ws/handler.go", "reading", "claude"))

	recordA := waitForPersistedSessionText(t, h.SessionStore, sessionA, "late output from session A")
	recordB, err := h.SessionStore.GetSession(context.Background(), sessionB)
	if err != nil {
		t.Fatalf("get session B: %v", err)
	}

	textsA := sessionLogTexts(recordA)
	textsB := sessionLogTexts(recordB)
	if !containsText(textsA, "late output from session A") {
		t.Fatalf("expected late output in session A projection, got %#v", textsA)
	}
	if !containsText(textsA, "late step from session A") {
		t.Fatalf("expected late step in session A projection, got %#v", textsA)
	}
	if containsText(textsB, "late output from session A") || containsText(textsB, "late step from session A") {
		t.Fatalf("did not expect session A events in session B projection, got %#v", textsB)
	}
}

func TestHandlerSessionLoadDetachesPreviousRuntimeWithoutStoppingIt(t *testing.T) {
	runnerA := newSwitchableStubRunner()
	firstRunner := runnerA
	runnerB := newSwitchableStubRunner()

	h := newTestHandler()
	tempStore, err := data.NewFileStore(t.TempDir())
	if err != nil {
		t.Fatalf("new temp store: %v", err)
	}
	h.SessionStore = tempStore
	h.NewPtyRunner = func() engine.Runner {
		if runnerA != nil {
			r := runnerA
			runnerA = nil
			return r
		}
		return runnerB
	}

	conn := newTestConn(t, h)
	_, _ = readInitialEvents(t, conn)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-a"}); err != nil {
		t.Fatalf("write initial session create request: %v", err)
	}
	createdA := readUntilSessionCreated(t, conn)
	_ = readUntilType(t, conn, protocol.EventTypeSessionListResult)
	summaryA, ok := createdA["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", createdA)
	}
	sessionA, _ := summaryA["id"].(string)
	if sessionA == "" {
		t.Fatalf("expected initial session id, got %#v", createdA)
	}

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request: %v", err)
	}
	firstRunner.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	if err := conn.WriteJSON(protocol.SessionCreateRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_create"}, Title: "session-b"}); err != nil {
		t.Fatalf("write session create request: %v", err)
	}
	created := readUntilSessionCreated(t, conn)
	summary, ok := created["summary"].(map[string]any)
	if !ok {
		t.Fatalf("expected summary payload, got %#v", created)
	}
	sessionB, _ := summary["id"].(string)
	if sessionB == "" || sessionB == sessionA {
		t.Fatalf("expected new session id, got %q", sessionB)
	}
	firstRunner.AssertNotClosed(t, 10*time.Millisecond)

	if err := conn.WriteJSON(protocol.ExecRequestEvent{
		ClientEvent: protocol.ClientEvent{Action: "exec"},
		Command:     "claude",
		Mode:        "pty",
	}); err != nil {
		t.Fatalf("write exec request for session B: %v", err)
	}
	runnerB.WaitStarted(t)
	requireAgentState(t, readUntilType(t, conn, protocol.EventTypeAgentState), "THINKING", false)

	runnerB.Emit(protocol.NewLogEvent("ignored", "live output from session B", "stdout"))
	logEvent := readUntilType(t, conn, protocol.EventTypeLog)
	if logEvent["msg"] != "live output from session B" {
		t.Fatalf("unexpected log event payload: %#v", logEvent)
	}

	if err := conn.WriteJSON(protocol.SessionLoadRequestEvent{ClientEvent: protocol.ClientEvent{Action: "session_load"}, SessionID: sessionA}); err != nil {
		t.Fatalf("write session load request: %v", err)
	}
	history := readUntilSessionHistory(t, conn)
	if history["sessionId"] != sessionA {
		t.Fatalf("expected session history for session A, got %#v", history)
	}
	runnerB.AssertNotClosed(t, 10*time.Millisecond)

	runnerB.Emit(protocol.NewLogEvent("ignored", "background output from session B", "stdout"))
	recordB := waitForPersistedSessionText(t, h.SessionStore, sessionB, "background output from session B")
	textsB := sessionLogTexts(recordB)
	if !containsText(textsB, "background output from session B") {
		t.Fatalf("expected background output in session B projection, got %#v", textsB)
	}
	assertNoEventType(t, conn, protocol.EventTypeLog, 150*time.Millisecond)
}
