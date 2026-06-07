package data

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"mobilevc/internal/protocol"
)

type FileStore struct {
	mu                  sync.Mutex
	baseDir             string
	indexPath           string
	skillCatalogPath    string
	memoryCatalogPath   string
	permissionRulesPath string
	pushTokensPath      string
	sessionMetaCache    map[string]sessionRecordMeta
}

type fileIndex struct {
	Sessions []SessionSummary `json:"sessions"`
}

type sessionRecordMeta struct {
	Summary           SessionSummary
	ClientActions     []ClientActionRecord
	SessionContext    SessionContext
	SessionContextSet bool
}

type sessionLogEntriesSidecar struct {
	SessionID  string             `json:"sessionId"`
	EntryCount int                `json:"entryCount"`
	LogEntries []SnapshotLogEntry `json:"logEntries"`
}

const sessionLogEntriesSidecarVersion = 1

type sessionLogEntriesSidecarHeader struct {
	Version    int    `json:"version"`
	SessionID  string `json:"sessionId"`
	EntryCount int    `json:"entryCount"`
}

type sessionLogEntriesIndex struct {
	Version    int     `json:"version"`
	SessionID  string  `json:"sessionId"`
	EntryCount int     `json:"entryCount"`
	Offsets    []int64 `json:"offsets"`
}

type sessionRecordLightweight struct {
	Summary       SessionSummary            `json:"summary"`
	Projection    projectionLightweightJSON `json:"projection"`
	ClientActions []ClientActionRecord      `json:"clientActions,omitempty"`
}

type projectionLightweightJSON struct {
	Diffs                  []DiffContext       `json:"diffs,omitempty"`
	CurrentDiff            *DiffContext        `json:"currentDiff,omitempty"`
	ReviewGroups           []ReviewGroup       `json:"reviewGroups,omitempty"`
	ActiveReviewGroup      *ReviewGroup        `json:"activeReviewGroup,omitempty"`
	CurrentStep            *SnapshotContext    `json:"currentStep,omitempty"`
	LatestError            *SnapshotContext    `json:"latestError,omitempty"`
	RawTerminalByStream    map[string]string   `json:"rawTerminalByStream,omitempty"`
	TerminalExecutions     []TerminalExecution `json:"terminalExecutions,omitempty"`
	Controller             ControllerSnapshot  `json:"controller,omitempty"`
	Runtime                SessionRuntime      `json:"runtime,omitempty"`
	ContextWindowUsage     ContextWindowUsage  `json:"contextWindowUsage,omitempty"`
	SessionContext         SessionContext      `json:"sessionContext,omitempty"`
	SessionContextSet      bool                `json:"sessionContextSet,omitempty"`
	PermissionRulesEnabled bool                `json:"permissionRulesEnabled,omitempty"`
	PermissionRules        []PermissionRule    `json:"permissionRules,omitempty"`
	SkillCatalogMeta       CatalogMetadata     `json:"skillCatalogMeta,omitempty"`
	MemoryCatalogMeta      CatalogMetadata     `json:"memoryCatalogMeta,omitempty"`
}

var (
	sessionPlaceholderPattern = regexp.MustCompile(`^session(?:[-_\s][a-z0-9]+)?$`)
	sessionTimestampPattern   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}(?:[ t]\d{2}:\d{2}(?::\d{2})?)?$`)
)

func NewFileStore(baseDir string) (*FileStore, error) {
	if strings.TrimSpace(baseDir) == "" {
		baseDir = defaultBaseDir()
	}
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}
	return &FileStore{
		baseDir:             baseDir,
		indexPath:           filepath.Join(baseDir, "index.json"),
		skillCatalogPath:    filepath.Join(baseDir, "skills.catalog.json"),
		memoryCatalogPath:   filepath.Join(baseDir, "memory.catalog.json"),
		permissionRulesPath: filepath.Join(baseDir, "permissions.rules.json"),
		pushTokensPath:      filepath.Join(baseDir, "push_tokens.json"),
		sessionMetaCache:    make(map[string]sessionRecordMeta),
	}, nil
}

func defaultBaseDir() string {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return filepath.Join(".", ".mobilevc", "sessions")
	}
	return filepath.Join(home, ".mobilevc", "sessions")
}

func (s *FileStore) CreateSession(ctx context.Context, title string) (SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SessionSummary{}, ctx.Err()
	default:
	}
	now := time.Now().UTC()
	summary := SessionSummary{
		ID:                fmt.Sprintf("session-%d", now.UnixNano()),
		Title:             fallbackTitle(title, now),
		CreatedAt:         now,
		UpdatedAt:         now,
		Source:            "mobilevc",
		Ownership:         "mobilevc",
		Runtime:           SessionRuntime{Source: "mobilevc"},
		ClaudeSessionUUID: uuid.NewString(),
	}
	record := SessionRecord{Summary: summary, Projection: normalizeProjection(ProjectionSnapshot{RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""}})}
	index, err := s.readIndexLocked()
	if err != nil {
		return SessionSummary{}, err
	}
	index.Sessions = append([]SessionSummary{summary}, filterOut(index.Sessions, summary.ID)...)
	if err := s.writeSessionLocked(record); err != nil {
		return SessionSummary{}, err
	}
	s.cacheSessionMetaLocked(record)
	if err := s.writeIndexLocked(index); err != nil {
		return SessionSummary{}, err
	}
	return summary, nil
}

func (s *FileStore) UpsertSession(ctx context.Context, record SessionRecord) (SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SessionSummary{}, ctx.Err()
	default:
	}
	record = normalizeSessionRecord(record)
	if strings.TrimSpace(record.Summary.ID) == "" {
		return SessionSummary{}, fmt.Errorf("session id is required")
	}
	if record.Summary.CreatedAt.IsZero() {
		record.Summary.CreatedAt = time.Now().UTC()
	}
	if record.Summary.UpdatedAt.IsZero() {
		record.Summary.UpdatedAt = record.Summary.CreatedAt
	}
	index, err := s.readIndexLocked()
	if err != nil {
		return SessionSummary{}, err
	}
	if existing, err := s.readSessionWithoutLogEntriesLocked(record.Summary.ID); err == nil {
		if record.Summary.Title == "" {
			record.Summary.Title = existing.Summary.Title
		}
		if record.Summary.LastPreview == "" {
			record.Summary.LastPreview = existing.Summary.LastPreview
		}
		if record.Summary.CreatedAt.IsZero() {
			record.Summary.CreatedAt = existing.Summary.CreatedAt
		}
		if record.Summary.ClaudeSessionUUID == "" {
			record.Summary.ClaudeSessionUUID = existing.Summary.ClaudeSessionUUID
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return SessionSummary{}, err
	}
	record.Summary = deriveProjectionSummary(record.Summary, record.Projection)
	if err := s.writeSessionLocked(record); err != nil {
		return SessionSummary{}, err
	}
	s.cacheSessionMetaLocked(record)
	updated := false
	for i := range index.Sessions {
		if index.Sessions[i].ID == record.Summary.ID {
			index.Sessions[i] = record.Summary
			updated = true
			break
		}
	}
	if !updated {
		index.Sessions = append(index.Sessions, record.Summary)
	}
	sort.Slice(index.Sessions, func(i, j int) bool {
		return index.Sessions[i].UpdatedAt.After(index.Sessions[j].UpdatedAt)
	})
	if err := s.writeIndexLocked(index); err != nil {
		return SessionSummary{}, err
	}
	return record.Summary, nil
}

func (s *FileStore) SaveProjection(ctx context.Context, sessionID string, projection ProjectionSnapshot) (SessionSummary, error) {
	return s.SaveProjectionWithOptions(ctx, sessionID, projection)
}

func (s *FileStore) SaveProjectionWithOptions(ctx context.Context, sessionID string, projection ProjectionSnapshot, opts ...ProjectionSaveOption) (SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SessionSummary{}, ctx.Err()
	default:
	}
	meta, ok := s.sessionMetaLocked(sessionID)
	if !ok {
		record, err := s.readSessionWithoutLogEntriesLocked(sessionID)
		if err != nil {
			return SessionSummary{}, err
		}
		meta = sessionMetaFromLightweightRecord(record)
	}
	now := time.Now().UTC()
	if !projection.SessionContextSet {
		projection.SessionContextSet = projection.SessionContext.Configured ||
			len(projection.SessionContext.EnabledSkillNames) > 0 ||
			len(projection.SessionContext.EnabledMemoryIDs) > 0
	}
	if !projection.SessionContextSet {
		projection.SessionContext = meta.SessionContext
		projection.SessionContextSet = meta.SessionContextSet
	}
	preserveExistingLogEntries := len(projection.LogEntries) == 0 && meta.Summary.EntryCount > 0
	preservedLogEntryCount := meta.Summary.EntryCount
	if preserveExistingLogEntries {
		index, err := s.readSessionLogEntriesIndexLocked(sessionID)
		if err != nil {
			return SessionSummary{}, err
		}
		preservedLogEntryCount = index.EntryCount
	}
	record := SessionRecord{
		Summary:       meta.Summary,
		Projection:    projection,
		ClientActions: append([]ClientActionRecord(nil), meta.ClientActions...),
	}
	record.Projection = normalizeProjection(projection)
	record = normalizeSessionRecord(record)
	record.Summary.UpdatedAt = now
	saveOpts := ProjectionSaveOptions{}
	for _, opt := range opts {
		if opt != nil {
			opt(&saveOpts)
		}
	}
	if saveOpts.JSONLSyncEntryCount != nil {
		record.Summary.JSONLSyncEntryCount = *saveOpts.JSONLSyncEntryCount
	}
	if preserveExistingLogEntries {
		record.Summary.EntryCount = preservedLogEntryCount
		record.Summary.Title = meta.Summary.Title
		record.Summary.LastPreview = meta.Summary.LastPreview
		if err := s.writeSessionRecordOnlyLocked(record); err != nil {
			return SessionSummary{}, err
		}
	} else if err := s.writeSessionLocked(record); err != nil {
		return SessionSummary{}, err
	}
	s.cacheSessionMetaLocked(record)
	index, err := s.readIndexLocked()
	if err != nil {
		return SessionSummary{}, err
	}
	updated := false
	for i := range index.Sessions {
		if index.Sessions[i].ID == sessionID {
			index.Sessions[i] = record.Summary
			updated = true
			break
		}
	}
	if !updated {
		index.Sessions = append(index.Sessions, record.Summary)
	}
	sort.Slice(index.Sessions, func(i, j int) bool {
		return index.Sessions[i].UpdatedAt.After(index.Sessions[j].UpdatedAt)
	})
	if err := s.writeIndexLocked(index); err != nil {
		return SessionSummary{}, err
	}
	return record.Summary, nil
}

func (s *FileStore) GetSession(ctx context.Context, sessionID string) (SessionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SessionRecord{}, ctx.Err()
	default:
	}
	record, err := s.readSessionLocked(sessionID)
	if err != nil {
		return SessionRecord{}, err
	}
	s.cacheSessionMetaLocked(record)
	return record, nil
}

func (s *FileStore) GetSessionHistoryWindow(ctx context.Context, req SessionHistoryWindowRequest) (SessionHistoryWindow, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SessionHistoryWindow{}, ctx.Err()
	default:
	}
	sessionID := strings.TrimSpace(req.SessionID)
	if sessionID == "" {
		return SessionHistoryWindow{}, fmt.Errorf("session ID is required")
	}
	record, err := s.readSessionWithoutLogEntriesLocked(sessionID)
	if err != nil {
		return SessionHistoryWindow{}, err
	}
	entries, start, total, err := s.readSessionLogEntryWindowLocked(sessionID, req.Before, req.Limit)
	if err != nil {
		return SessionHistoryWindow{}, err
	}
	record.Projection.LogEntries = nil
	record.Summary.EntryCount = total
	return SessionHistoryWindow{
		Record:        record,
		LogEntries:    entries,
		LogEntryStart: start,
		LogEntryTotal: total,
	}, nil
}

func (s *FileStore) ListSessions(ctx context.Context) ([]SessionSummary, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	index, err := s.readIndexLocked()
	if err != nil {
		return nil, err
	}
	index, err = s.reconcileIndexLocked(index)
	if err != nil {
		return nil, err
	}
	items := append([]SessionSummary(nil), index.Sessions...)
	sort.Slice(items, func(i, j int) bool {
		return items[i].UpdatedAt.After(items[j].UpdatedAt)
	})
	return items, nil
}

func (s *FileStore) DeleteSession(ctx context.Context, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	if _, err := s.readSessionLocked(sessionID); err != nil {
		return err
	}
	if err := os.Remove(s.sessionPath(sessionID)); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("session not found: %s: %w", sessionID, os.ErrNotExist)
		}
		return fmt.Errorf("delete session record: %w", err)
	}
	if err := os.Remove(s.sessionLogEntriesPath(sessionID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete session log entries sidecar: %w", err)
	}
	if err := os.Remove(s.sessionLogEntriesIndexPath(sessionID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("delete session log entries index: %w", err)
	}
	index, err := s.readIndexLocked()
	if err != nil {
		return err
	}
	index.Sessions = filterOut(index.Sessions, sessionID)
	if err := s.writeIndexLocked(index); err != nil {
		return err
	}
	delete(s.sessionMetaCache, sessionID)
	return nil
}

func (s *FileStore) ListSkillCatalog(ctx context.Context) ([]SkillDefinition, error) {
	snapshot, err := s.GetSkillCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Items, nil
}

func (s *FileStore) SaveSkillCatalog(ctx context.Context, items []SkillDefinition) error {
	snapshot, err := s.GetSkillCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	snapshot.Items = items
	if snapshot.Meta.Domain == "" {
		snapshot.Meta.Domain = CatalogDomainSkill
	}
	return s.SaveSkillCatalogSnapshot(ctx, snapshot)
}

func (s *FileStore) GetSkillCatalogSnapshot(ctx context.Context) (SkillCatalogSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return SkillCatalogSnapshot{}, ctx.Err()
	default:
	}
	return s.readSkillCatalogSnapshotLocked()
}

func (s *FileStore) SaveSkillCatalogSnapshot(ctx context.Context, snapshot SkillCatalogSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	snapshot = normalizeSkillCatalogSnapshot(snapshot)
	return s.writeJSONFileLocked(s.skillCatalogPath, snapshot, "encode skill catalog")
}

func (s *FileStore) ListMemoryCatalog(ctx context.Context) ([]MemoryItem, error) {
	snapshot, err := s.GetMemoryCatalogSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	return snapshot.Items, nil
}

func (s *FileStore) SaveMemoryCatalog(ctx context.Context, items []MemoryItem) error {
	snapshot, err := s.GetMemoryCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	snapshot.Items = items
	if snapshot.Meta.Domain == "" {
		snapshot.Meta.Domain = CatalogDomainMemory
	}
	return s.SaveMemoryCatalogSnapshot(ctx, snapshot)
}

func (s *FileStore) GetMemoryCatalogSnapshot(ctx context.Context) (MemoryCatalogSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return MemoryCatalogSnapshot{}, ctx.Err()
	default:
	}
	return s.readMemoryCatalogSnapshotLocked()
}

func (s *FileStore) SaveMemoryCatalogSnapshot(ctx context.Context, snapshot MemoryCatalogSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	snapshot = normalizeMemoryCatalogSnapshot(snapshot)
	return s.writeJSONFileLocked(s.memoryCatalogPath, snapshot, "encode memory catalog")
}

func (s *FileStore) GetPermissionRuleSnapshot(ctx context.Context) (PermissionRuleSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return PermissionRuleSnapshot{}, ctx.Err()
	default:
	}
	return s.readPermissionRuleSnapshotLocked()
}

func (s *FileStore) SavePermissionRuleSnapshot(ctx context.Context, snapshot PermissionRuleSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	snapshot = normalizePermissionRuleSnapshot(snapshot)
	return s.writeJSONFileLocked(s.permissionRulesPath, snapshot, "encode permission rules")
}

func (s *FileStore) readIndexLocked() (fileIndex, error) {
	var index fileIndex
	data, err := os.ReadFile(s.indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fileIndex{}, nil
		}
		return fileIndex{}, fmt.Errorf("read session index: %w", err)
	}
	if len(data) == 0 {
		return fileIndex{}, nil
	}
	if err := json.Unmarshal(data, &index); err != nil {
		return fileIndex{}, fmt.Errorf("decode session index: %w", err)
	}
	return index, nil
}

func (s *FileStore) writeIndexLocked(index fileIndex) error {
	data, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("encode session index: %w", err)
	}
	return writeJSONFileAtomic(s.indexPath, data, 0o644)
}

func (s *FileStore) readSessionLocked(sessionID string) (SessionRecord, error) {
	record, err := s.readSessionFileLocked(sessionID)
	if err != nil {
		return SessionRecord{}, err
	}
	embeddedEntries := append([]SnapshotLogEntry(nil), record.Projection.LogEntries...)
	entries, err := s.readAllSessionLogEntriesIfAvailableLocked(sessionID)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return SessionRecord{}, err
		}
		if len(embeddedEntries) == 0 && record.Summary.EntryCount > 0 {
			return SessionRecord{}, fmt.Errorf("session log entries sidecar missing for %s: %w", sessionID, os.ErrNotExist)
		}
		entries = dedupeAdjacentLogEntries(embeddedEntries)
		if writeErr := s.writeSessionLocked(SessionRecord{
			Summary:       record.Summary,
			Projection:    projectionWithLogEntries(record.Projection, entries),
			ClientActions: record.ClientActions,
		}); writeErr != nil {
			return SessionRecord{}, writeErr
		}
	} else if len(embeddedEntries) > 0 && len(entries) != len(dedupeAdjacentLogEntries(embeddedEntries)) {
		entries = dedupeAdjacentLogEntries(embeddedEntries)
		if writeErr := s.writeSessionLocked(SessionRecord{
			Summary:       record.Summary,
			Projection:    projectionWithLogEntries(record.Projection, entries),
			ClientActions: record.ClientActions,
		}); writeErr != nil {
			return SessionRecord{}, writeErr
		}
	}
	record.Projection.LogEntries = entries
	return normalizeSessionRecord(record), nil
}

func (s *FileStore) readSessionFileLocked(sessionID string) (SessionRecord, error) {
	data, err := os.ReadFile(s.sessionPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return SessionRecord{}, fmt.Errorf("session not found: %s: %w", sessionID, os.ErrNotExist)
		}
		return SessionRecord{}, fmt.Errorf("read session record: %w", err)
	}
	var record SessionRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return SessionRecord{}, fmt.Errorf("decode session record: %w", err)
	}
	return record, nil
}

func (s *FileStore) readSessionWithoutLogEntriesLocked(sessionID string) (SessionRecord, error) {
	file, err := os.Open(s.sessionPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return SessionRecord{}, fmt.Errorf("session not found: %s: %w", sessionID, os.ErrNotExist)
		}
		return SessionRecord{}, fmt.Errorf("read session record: %w", err)
	}
	defer file.Close()
	var record sessionRecordLightweight
	if err := json.NewDecoder(file).Decode(&record); err != nil {
		return SessionRecord{}, fmt.Errorf("decode session record: %w", err)
	}
	return normalizeSessionRecordLightweight(SessionRecord{
		Summary:       record.Summary,
		Projection:    record.Projection.toProjectionSnapshot(),
		ClientActions: record.ClientActions,
	}), nil
}

func (s *FileStore) writeSessionLocked(record SessionRecord) error {
	record = normalizeSessionRecord(record)
	entries := append([]SnapshotLogEntry(nil), record.Projection.LogEntries...)
	record.Projection.LogEntries = nil
	if err := s.writeSessionRecordOnlyLocked(record); err != nil {
		return err
	}
	return s.writeSessionLogEntriesLocked(record.Summary.ID, entries)
}

func (s *FileStore) writeSessionRecordOnlyLocked(record SessionRecord) error {
	record.Projection.LogEntries = nil
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode session record: %w", err)
	}
	return writeJSONFileAtomic(s.sessionPath(record.Summary.ID), data, 0o644)
}

func (s *FileStore) readSessionLogEntryWindowLocked(sessionID string, before, limit int) ([]SnapshotLogEntry, int, int, error) {
	requestBefore := before
	index, err := s.readSessionLogEntriesIndexLocked(sessionID)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, 0, 0, err
		}
		record, rebuildErr := s.readSessionFileLocked(sessionID)
		if rebuildErr != nil {
			return nil, 0, 0, rebuildErr
		}
		record = normalizeSessionRecord(record)
		if len(record.Projection.LogEntries) == 0 && record.Summary.EntryCount > 0 {
			return nil, 0, 0, fmt.Errorf("session log entries sidecar missing for %s: %w", sessionID, os.ErrNotExist)
		}
		if writeErr := s.writeSessionLocked(record); writeErr != nil {
			return nil, 0, 0, writeErr
		}
		index, err = s.readSessionLogEntriesIndexLocked(sessionID)
		if err != nil {
			return nil, 0, 0, err
		}
	}
	total := index.EntryCount
	if total < 0 {
		return nil, 0, 0, fmt.Errorf("session log entries index has negative count: %d", total)
	}
	if len(index.Offsets) != total {
		return nil, 0, 0, fmt.Errorf("session log entries index count mismatch: got %d offsets, index count %d", len(index.Offsets), total)
	}
	before = normalizeWindowBefore(before, total)
	start := normalizeWindowStart(before, limit)
	entries, err := s.readSessionLogEntriesRangeLocked(sessionID, index.Offsets, start, before)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			record, rebuildErr := s.readSessionLocked(sessionID)
			if rebuildErr != nil {
				return nil, 0, 0, rebuildErr
			}
			if writeErr := s.writeSessionLogEntriesLocked(sessionID, record.Projection.LogEntries); writeErr != nil {
				return nil, 0, 0, writeErr
			}
			return s.readSessionLogEntryWindowLocked(sessionID, requestBefore, limit)
		}
		return nil, 0, 0, err
	}
	return entries, start, total, nil
}

func (s *FileStore) readAllSessionLogEntriesIfAvailableLocked(sessionID string) ([]SnapshotLogEntry, error) {
	index, err := s.readSessionLogEntriesIndexLocked(sessionID)
	if err != nil {
		return nil, err
	}
	return s.readSessionLogEntriesRangeLocked(sessionID, index.Offsets, 0, index.EntryCount)
}

func (s *FileStore) readSessionLogEntriesIndexLocked(sessionID string) (sessionLogEntriesIndex, error) {
	data, err := os.ReadFile(s.sessionLogEntriesIndexPath(sessionID))
	if err == nil {
		var index sessionLogEntriesIndex
		if decodeErr := json.Unmarshal(data, &index); decodeErr != nil {
			return sessionLogEntriesIndex{}, fmt.Errorf("decode session log entries index: %w", decodeErr)
		}
		if err := validateSessionLogEntriesIndex(sessionID, index); err != nil {
			return sessionLogEntriesIndex{}, err
		}
		return index, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return sessionLogEntriesIndex{}, fmt.Errorf("read session log entries index: %w", err)
	}
	entries, legacyErr := s.readLegacySessionLogEntriesSidecarLocked(sessionID)
	if legacyErr != nil {
		if errors.Is(legacyErr, os.ErrNotExist) {
			return sessionLogEntriesIndex{}, os.ErrNotExist
		}
		return sessionLogEntriesIndex{}, legacyErr
	}
	if writeErr := s.writeSessionLogEntriesLocked(sessionID, entries); writeErr != nil {
		return sessionLogEntriesIndex{}, writeErr
	}
	data, err = os.ReadFile(s.sessionLogEntriesIndexPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return sessionLogEntriesIndex{}, os.ErrNotExist
		}
		return sessionLogEntriesIndex{}, fmt.Errorf("read rebuilt session log entries index: %w", err)
	}
	var index sessionLogEntriesIndex
	if decodeErr := json.Unmarshal(data, &index); decodeErr != nil {
		return sessionLogEntriesIndex{}, fmt.Errorf("decode rebuilt session log entries index: %w", decodeErr)
	}
	if err := validateSessionLogEntriesIndex(sessionID, index); err != nil {
		return sessionLogEntriesIndex{}, err
	}
	return index, nil
}

func (s *FileStore) readLegacySessionLogEntriesSidecarLocked(sessionID string) ([]SnapshotLogEntry, error) {
	data, err := os.ReadFile(s.sessionLogEntriesPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, os.ErrNotExist
		}
		return nil, fmt.Errorf("read session log entries sidecar: %w", err)
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, fmt.Errorf("session log entries sidecar is empty")
	}
	if trimmed[0] != '{' {
		return nil, os.ErrNotExist
	}
	var sidecar sessionLogEntriesSidecar
	if err := json.Unmarshal(trimmed, &sidecar); err != nil {
		return nil, fmt.Errorf("decode legacy session log entries sidecar: %w", err)
	}
	if strings.TrimSpace(sidecar.SessionID) != "" && strings.TrimSpace(sidecar.SessionID) != strings.TrimSpace(sessionID) {
		return nil, fmt.Errorf("session log entries sidecar belongs to %s, not %s", sidecar.SessionID, sessionID)
	}
	entries := dedupeAdjacentLogEntries(sidecar.LogEntries)
	if sidecar.EntryCount != 0 && sidecar.EntryCount != len(entries) {
		return nil, fmt.Errorf("session log entries sidecar count mismatch: got %d entries, sidecar count %d", len(entries), sidecar.EntryCount)
	}
	return append([]SnapshotLogEntry(nil), entries...), nil
}

func (s *FileStore) readSessionLogEntriesRangeLocked(sessionID string, offsets []int64, start, before int) ([]SnapshotLogEntry, error) {
	if start < 0 || before < start || before > len(offsets) {
		return nil, fmt.Errorf("invalid session log entries window: start=%d before=%d total=%d", start, before, len(offsets))
	}
	if start == before {
		return []SnapshotLogEntry{}, nil
	}
	file, err := os.Open(s.sessionLogEntriesPath(sessionID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("session log entries sidecar missing for %s: %w", sessionID, os.ErrNotExist)
		}
		return nil, fmt.Errorf("read session log entries sidecar: %w", err)
	}
	defer file.Close()
	reader := bufio.NewReader(file)
	headerLine, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("read session log entries sidecar header: %w", err)
	}
	var header sessionLogEntriesSidecarHeader
	if err := json.Unmarshal(bytes.TrimSpace(headerLine), &header); err != nil {
		return nil, fmt.Errorf("decode session log entries sidecar header: %w", err)
	}
	if err := validateSessionLogEntriesHeader(sessionID, header, len(offsets)); err != nil {
		return nil, err
	}
	if _, err := file.Seek(offsets[start], io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek session log entries sidecar: %w", err)
	}
	reader.Reset(file)
	entries := make([]SnapshotLogEntry, 0, before-start)
	for row := start; row < before; row++ {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, fmt.Errorf("session log entries sidecar ended at row %d before expected %d", row, before)
			}
			return nil, fmt.Errorf("read session log entries sidecar row %d: %w", row, err)
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			return nil, fmt.Errorf("session log entries sidecar ended at row %d before expected %d", row, before)
		}
		var entry SnapshotLogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("decode session log entry row %d: %w", row, err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func (s *FileStore) writeSessionLogEntriesLocked(sessionID string, entries []SnapshotLogEntry) error {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return fmt.Errorf("session id is required")
	}
	entries = dedupeAdjacentLogEntries(entries)
	var buf bytes.Buffer
	header := sessionLogEntriesSidecarHeader{
		Version:    sessionLogEntriesSidecarVersion,
		SessionID:  sessionID,
		EntryCount: len(entries),
	}
	headerData, err := json.Marshal(header)
	if err != nil {
		return fmt.Errorf("encode session log entries sidecar header: %w", err)
	}
	buf.Write(headerData)
	buf.WriteByte('\n')
	offsets := make([]int64, 0, len(entries))
	for i, entry := range entries {
		offsets = append(offsets, int64(buf.Len()))
		row, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("encode session log entry row %d: %w", i, err)
		}
		buf.Write(row)
		buf.WriteByte('\n')
	}
	if err := writeJSONFileAtomic(s.sessionLogEntriesPath(sessionID), buf.Bytes(), 0o644); err != nil {
		return err
	}
	index := sessionLogEntriesIndex{
		Version:    sessionLogEntriesSidecarVersion,
		SessionID:  sessionID,
		EntryCount: len(entries),
		Offsets:    offsets,
	}
	indexData, err := json.Marshal(index)
	if err != nil {
		return fmt.Errorf("encode session log entries index: %w", err)
	}
	return writeJSONFileAtomic(s.sessionLogEntriesIndexPath(sessionID), indexData, 0o644)
}

func writeJSONFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create parent dir: %w", err)
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace file: %w", err)
	}
	cleanup = false
	return nil
}

func validateSessionLogEntriesIndex(sessionID string, index sessionLogEntriesIndex) error {
	if index.Version != sessionLogEntriesSidecarVersion {
		return fmt.Errorf("unsupported session log entries index version: %d", index.Version)
	}
	if strings.TrimSpace(index.SessionID) != strings.TrimSpace(sessionID) {
		return fmt.Errorf("session log entries index belongs to %s, not %s", index.SessionID, sessionID)
	}
	if index.EntryCount < 0 {
		return fmt.Errorf("session log entries index has negative count: %d", index.EntryCount)
	}
	if len(index.Offsets) != index.EntryCount {
		return fmt.Errorf("session log entries index count mismatch: got %d offsets, index count %d", len(index.Offsets), index.EntryCount)
	}
	var previous int64 = -1
	for i, offset := range index.Offsets {
		if offset < 0 {
			return fmt.Errorf("session log entries index offset %d is negative: %d", i, offset)
		}
		if offset <= previous {
			return fmt.Errorf("session log entries index offset %d is not increasing: %d <= %d", i, offset, previous)
		}
		previous = offset
	}
	return nil
}

func validateSessionLogEntriesHeader(sessionID string, header sessionLogEntriesSidecarHeader, expectedCount int) error {
	if header.Version != sessionLogEntriesSidecarVersion {
		return fmt.Errorf("unsupported session log entries sidecar version: %d", header.Version)
	}
	if strings.TrimSpace(header.SessionID) != strings.TrimSpace(sessionID) {
		return fmt.Errorf("session log entries sidecar belongs to %s, not %s", header.SessionID, sessionID)
	}
	if header.EntryCount != expectedCount {
		return fmt.Errorf("session log entries sidecar count mismatch: header count %d, index count %d", header.EntryCount, expectedCount)
	}
	return nil
}

func normalizeWindowBefore(before, total int) int {
	if before <= 0 || before > total {
		return total
	}
	return before
}

func normalizeWindowStart(before, limit int) int {
	if before <= 0 {
		return 0
	}
	if limit <= 0 || limit >= before {
		return 0
	}
	return before - limit
}

func projectionWithLogEntries(projection ProjectionSnapshot, entries []SnapshotLogEntry) ProjectionSnapshot {
	projection.LogEntries = append([]SnapshotLogEntry(nil), entries...)
	return projection
}

func (s *FileStore) readSkillCatalogSnapshotLocked() (SkillCatalogSnapshot, error) {
	data, err := os.ReadFile(s.skillCatalogPath)
	if err != nil {
		if os.IsNotExist(err) {
			return normalizeSkillCatalogSnapshot(SkillCatalogSnapshot{}), nil
		}
		return SkillCatalogSnapshot{}, fmt.Errorf("read skill catalog: %w", err)
	}
	if len(data) == 0 {
		return normalizeSkillCatalogSnapshot(SkillCatalogSnapshot{}), nil
	}

	var snapshot SkillCatalogSnapshot
	if err := json.Unmarshal(data, &snapshot); err == nil {
		return normalizeSkillCatalogSnapshot(snapshot), nil
	}

	var items []SkillDefinition
	if err := json.Unmarshal(data, &items); err == nil {
		return normalizeSkillCatalogSnapshot(SkillCatalogSnapshot{Items: items}), nil
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		return SkillCatalogSnapshot{}, fmt.Errorf("decode skill catalog: %w", err)
	}
	return normalizeSkillCatalogSnapshot(snapshot), nil
}

func (s *FileStore) readMemoryCatalogSnapshotLocked() (MemoryCatalogSnapshot, error) {
	var snapshot MemoryCatalogSnapshot
	if err := s.readJSONFileLocked(s.memoryCatalogPath, &snapshot, "read memory catalog", "decode memory catalog"); err != nil {
		return MemoryCatalogSnapshot{}, err
	}
	return normalizeMemoryCatalogSnapshot(snapshot), nil
}

func (s *FileStore) readPermissionRuleSnapshotLocked() (PermissionRuleSnapshot, error) {
	var snapshot PermissionRuleSnapshot
	if err := s.readJSONFileLocked(s.permissionRulesPath, &snapshot, "read permission rules", "decode permission rules"); err != nil {
		return PermissionRuleSnapshot{}, err
	}
	return normalizePermissionRuleSnapshot(snapshot), nil
}

func (s *FileStore) readJSONFileLocked(path string, target any, readErrLabel, decodeErrLabel string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("%s: %w", readErrLabel, err)
	}
	if len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("%s: %w", decodeErrLabel, err)
	}
	return nil
}

func (s *FileStore) writeJSONFileLocked(path string, value any, encodeErrLabel string) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("%s: %w", encodeErrLabel, err)
	}
	return writeJSONFileAtomic(path, data, 0o644)
}

func (projection projectionLightweightJSON) toProjectionSnapshot() ProjectionSnapshot {
	return ProjectionSnapshot{
		Diffs:                  projection.Diffs,
		CurrentDiff:            projection.CurrentDiff,
		ReviewGroups:           projection.ReviewGroups,
		ActiveReviewGroup:      projection.ActiveReviewGroup,
		CurrentStep:            projection.CurrentStep,
		LatestError:            projection.LatestError,
		RawTerminalByStream:    projection.RawTerminalByStream,
		TerminalExecutions:     projection.TerminalExecutions,
		Controller:             projection.Controller,
		Runtime:                projection.Runtime,
		ContextWindowUsage:     projection.ContextWindowUsage,
		SessionContext:         projection.SessionContext,
		SessionContextSet:      projection.SessionContextSet,
		PermissionRulesEnabled: projection.PermissionRulesEnabled,
		PermissionRules:        projection.PermissionRules,
		SkillCatalogMeta:       projection.SkillCatalogMeta,
		MemoryCatalogMeta:      projection.MemoryCatalogMeta,
	}
}

func (s *FileStore) sessionPath(sessionID string) string {
	return filepath.Join(s.baseDir, sessionID+".json")
}

func (s *FileStore) sessionLogEntriesPath(sessionID string) string {
	return filepath.Join(s.baseDir, sessionID+".log_entries.json")
}

func (s *FileStore) sessionLogEntriesIndexPath(sessionID string) string {
	return filepath.Join(s.baseDir, sessionID+".log_entries.idx.json")
}

func (s *FileStore) BaseDir() string {
	if s == nil {
		return ""
	}
	return s.baseDir
}

func (s *FileStore) MarkClientAction(ctx context.Context, sessionID string, record ClientActionRecord, ttl time.Duration, limit int) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}
	sessionID = strings.TrimSpace(sessionID)
	record.ClientActionID = strings.TrimSpace(record.ClientActionID)
	record.Action = strings.TrimSpace(record.Action)
	record.Status = strings.TrimSpace(record.Status)
	if sessionID == "" {
		return false, fmt.Errorf("session id is required")
	}
	if record.ClientActionID == "" {
		return false, fmt.Errorf("client action id is required")
	}
	if record.Status == "" {
		record.Status = "accepted"
	}
	now := time.Now().UTC()
	if record.AckedAt.IsZero() {
		record.AckedAt = now
	}
	if limit <= 0 {
		limit = 500
	}
	sessionRecord, err := s.readSessionLocked(sessionID)
	if err != nil {
		return false, err
	}
	sessionRecord.ClientActions = normalizeClientActionRecords(sessionRecord.ClientActions, now, ttl, limit)
	for _, item := range sessionRecord.ClientActions {
		if item.ClientActionID == record.ClientActionID {
			return true, nil
		}
	}
	sessionRecord.ClientActions = append(sessionRecord.ClientActions, record)
	sessionRecord.ClientActions = normalizeClientActionRecords(sessionRecord.ClientActions, now, ttl, limit)
	if err := s.writeSessionLocked(sessionRecord); err != nil {
		return false, err
	}
	s.cacheSessionMetaLocked(sessionRecord)
	return false, nil
}

func filterOut(items []SessionSummary, id string) []SessionSummary {
	out := make([]SessionSummary, 0, len(items))
	for _, item := range items {
		if item.ID != id {
			out = append(out, item)
		}
	}
	return out
}

func sessionMetaFromRecord(record SessionRecord) sessionRecordMeta {
	record = normalizeSessionRecord(record)
	return sessionRecordMeta{
		Summary:           record.Summary,
		ClientActions:     append([]ClientActionRecord(nil), record.ClientActions...),
		SessionContext:    record.Projection.SessionContext,
		SessionContextSet: record.Projection.SessionContextSet || record.Projection.SessionContext.Configured,
	}
}

func sessionMetaFromLightweightRecord(record SessionRecord) sessionRecordMeta {
	record = normalizeSessionRecordLightweight(record)
	return sessionRecordMeta{
		Summary:           record.Summary,
		ClientActions:     append([]ClientActionRecord(nil), record.ClientActions...),
		SessionContext:    record.Projection.SessionContext,
		SessionContextSet: record.Projection.SessionContextSet || record.Projection.SessionContext.Configured,
	}
}

func (s *FileStore) cacheSessionMetaLocked(record SessionRecord) {
	if s.sessionMetaCache == nil {
		s.sessionMetaCache = make(map[string]sessionRecordMeta)
	}
	meta := sessionMetaFromRecord(record)
	sessionID := strings.TrimSpace(meta.Summary.ID)
	if sessionID == "" {
		return
	}
	s.sessionMetaCache[sessionID] = meta
}

func (s *FileStore) sessionMetaLocked(sessionID string) (sessionRecordMeta, bool) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" || s.sessionMetaCache == nil {
		return sessionRecordMeta{}, false
	}
	meta, ok := s.sessionMetaCache[sessionID]
	if !ok {
		return sessionRecordMeta{}, false
	}
	meta.ClientActions = append([]ClientActionRecord(nil), meta.ClientActions...)
	return meta, true
}

func normalizeProjection(projection ProjectionSnapshot) ProjectionSnapshot {
	if projection.RawTerminalByStream == nil {
		projection.RawTerminalByStream = map[string]string{"stdout": "", "stderr": ""}
	}
	if _, ok := projection.RawTerminalByStream["stdout"]; !ok {
		projection.RawTerminalByStream["stdout"] = ""
	}
	if _, ok := projection.RawTerminalByStream["stderr"]; !ok {
		projection.RawTerminalByStream["stderr"] = ""
	}
	if projection.LogEntries == nil {
		projection.LogEntries = []SnapshotLogEntry{}
	}
	projection.LogEntries = dedupeAdjacentLogEntries(projection.LogEntries)
	if projection.TerminalExecutions == nil {
		projection.TerminalExecutions = []TerminalExecution{}
	}
	if projection.ReviewGroups == nil {
		projection.ReviewGroups = []ReviewGroup{}
	}
	if projection.PermissionRules == nil {
		projection.PermissionRules = []PermissionRule{}
	}
	projection.PermissionRules = normalizePermissionRules(projection.PermissionRules)
	projection.ContextWindowUsage = normalizeContextWindowUsage(projection.ContextWindowUsage)
	projection.SessionContext = normalizeSessionContext(projection.SessionContext)
	projection.SkillCatalogMeta = normalizeCatalogMetadata(projection.SkillCatalogMeta, CatalogDomainSkill)
	projection.MemoryCatalogMeta = normalizeCatalogMetadata(projection.MemoryCatalogMeta, CatalogDomainMemory)
	return projection
}

func dedupeAdjacentLogEntries(entries []SnapshotLogEntry) []SnapshotLogEntry {
	if len(entries) < 2 {
		return entries
	}
	out := make([]SnapshotLogEntry, 0, len(entries))
	for _, entry := range entries {
		if len(out) > 0 && equivalentAdjacentLogEntry(out[len(out)-1], entry) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func equivalentAdjacentLogEntry(left SnapshotLogEntry, right SnapshotLogEntry) bool {
	if left.Kind != right.Kind {
		return false
	}
	switch left.Kind {
	case "markdown", "system", "user":
		return displayLogEntryDedupeKey(left) == displayLogEntryDedupeKey(right)
	default:
		return snapshotLogEntryDedupeKey(left) == snapshotLogEntryDedupeKey(right)
	}
}

func displayLogEntryDedupeKey(entry SnapshotLogEntry) string {
	return strings.Join([]string{
		entry.Kind,
		logEntryDisplayTimestamp(entry),
		normalizeLogEntryText(logEntryDisplayText(entry)),
	}, "\x1f")
}

func snapshotLogEntryDedupeKey(entry SnapshotLogEntry) string {
	exitCode := ""
	if entry.ExitCode != nil {
		exitCode = fmt.Sprintf("%d", *entry.ExitCode)
	}
	contextFields := []string{"", "", "", "", "", "", "", "", "", "", ""}
	if entry.Context != nil {
		contextFields = []string{
			entry.Context.ID,
			entry.Context.Type,
			entry.Context.Path,
			entry.Context.Title,
			entry.Context.Message,
			entry.Context.Target,
			entry.Context.TargetPath,
			entry.Context.Tool,
			entry.Context.Command,
			entry.Context.Source,
			entry.Context.ExecutionID,
		}
	}
	fields := []string{
		entry.Kind,
		logEntryDisplayTimestamp(entry),
		normalizeLogEntryText(logEntryDisplayText(entry)),
		entry.Label,
		entry.Stream,
		entry.ExecutionID,
		entry.Phase,
		exitCode,
		snapshotLogEntryAttachmentDedupeKey(entry.Attachments),
	}
	fields = append(fields, contextFields...)
	return strings.Join(fields, "\x1f")
}

func snapshotLogEntryAttachmentDedupeKey(attachments []protocol.TimelineAttachment) string {
	if len(attachments) == 0 {
		return ""
	}
	fields := make([]string, 0, len(attachments))
	for _, attachment := range attachments {
		fields = append(fields, strings.Join([]string{
			attachment.ID,
			attachment.Kind,
			attachment.Name,
			attachment.MIMEType,
			fmt.Sprintf("%d", attachment.Size),
			attachment.Path,
			attachment.PreviewStatus,
			attachment.Source,
		}, "\x1e"))
	}
	return strings.Join(fields, "\x1d")
}

func logEntryDisplayTimestamp(entry SnapshotLogEntry) string {
	contextTimestamp := ""
	if entry.Context != nil {
		contextTimestamp = strings.TrimSpace(entry.Context.Timestamp)
	}
	return firstNonEmptyString(strings.TrimSpace(entry.Timestamp), contextTimestamp)
}

func logEntryDisplayText(entry SnapshotLogEntry) string {
	text := firstNonEmptyString(entry.Message, entry.Text)
	if text == "" && entry.Context != nil {
		text = entry.Context.Message
	}
	return text
}

func normalizeLogEntryText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func normalizeSessionRecord(record SessionRecord) SessionRecord {
	record.Projection = normalizeProjection(record.Projection)
	record.ClientActions = normalizeClientActionRecords(record.ClientActions, time.Time{}, 0, 0)
	runtimeSource := strings.ToLower(strings.TrimSpace(firstNonEmptyString(
		record.Projection.Runtime.Source,
		record.Summary.Runtime.Source,
		record.Summary.Source,
	)))
	defaultSource := "mobilevc"
	if record.Summary.External || runtimeSource == "codex-native" {
		record.Summary.External = true
		defaultSource = "codex-native"
	}
	if record.Summary.Ownership == "" {
		if record.Summary.External {
			record.Summary.Ownership = "claude-native"
		} else {
			record.Summary.Ownership = "mobilevc"
		}
	}
	record.Projection.Runtime = mergeSessionRuntime(record.Summary.Runtime, record.Projection.Runtime)
	if record.Projection.Runtime.Source == "" {
		record.Projection.Runtime.Source = defaultSource
	}
	record.Summary.Runtime = mergeSessionRuntime(record.Summary.Runtime, record.Projection.Runtime)
	if record.Summary.Runtime.Source == "" {
		record.Summary.Runtime.Source = defaultSource
	}
	if record.Summary.Source == "" {
		record.Summary.Source = defaultSource
	}
	// ExecutionActive latches: true when controller state is non-IDLE
	// (THINKING, RUNNING_TOOL, WAIT_INPUT), false only when IDLE/empty.
	controllerState := strings.TrimSpace(string(record.Projection.Controller.State))
	if controllerState == "" || controllerState == string(ControllerStateIdle) {
		record.Summary.ExecutionActive = false
	} else {
		record.Summary.ExecutionActive = true
	}
	record.Summary = deriveProjectionSummary(record.Summary, record.Projection)
	return record
}

func normalizeSessionRecordLightweight(record SessionRecord) SessionRecord {
	record.Projection = normalizeProjection(record.Projection)
	record.Projection.LogEntries = nil
	record.ClientActions = normalizeClientActionRecords(record.ClientActions, time.Time{}, 0, 0)
	runtimeSource := strings.ToLower(strings.TrimSpace(firstNonEmptyString(
		record.Projection.Runtime.Source,
		record.Summary.Runtime.Source,
		record.Summary.Source,
	)))
	defaultSource := "mobilevc"
	if record.Summary.External || runtimeSource == "codex-native" {
		record.Summary.External = true
		defaultSource = "codex-native"
	}
	if record.Summary.Ownership == "" {
		if record.Summary.External {
			record.Summary.Ownership = "claude-native"
		} else {
			record.Summary.Ownership = "mobilevc"
		}
	}
	record.Projection.Runtime = mergeSessionRuntime(record.Summary.Runtime, record.Projection.Runtime)
	if record.Projection.Runtime.Source == "" {
		record.Projection.Runtime.Source = defaultSource
	}
	record.Summary.Runtime = mergeSessionRuntime(record.Summary.Runtime, record.Projection.Runtime)
	if record.Summary.Runtime.Source == "" {
		record.Summary.Runtime.Source = defaultSource
	}
	if record.Summary.Source == "" {
		record.Summary.Source = defaultSource
	}
	controllerState := strings.TrimSpace(string(record.Projection.Controller.State))
	if controllerState == "" || controllerState == string(ControllerStateIdle) {
		record.Summary.ExecutionActive = false
	} else {
		record.Summary.ExecutionActive = true
	}
	return record
}

func normalizeClientActionRecords(items []ClientActionRecord, now time.Time, ttl time.Duration, limit int) []ClientActionRecord {
	if len(items) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(items))
	cutoff := time.Time{}
	if ttl > 0 && !now.IsZero() {
		cutoff = now.Add(-ttl)
	}
	normalized := make([]ClientActionRecord, 0, len(items))
	for i := len(items) - 1; i >= 0; i-- {
		item := items[i]
		item.ClientActionID = strings.TrimSpace(item.ClientActionID)
		item.Action = strings.TrimSpace(item.Action)
		item.Status = strings.TrimSpace(item.Status)
		if item.ClientActionID == "" {
			continue
		}
		if !cutoff.IsZero() && !item.AckedAt.IsZero() && item.AckedAt.Before(cutoff) {
			continue
		}
		if _, exists := seen[item.ClientActionID]; exists {
			continue
		}
		seen[item.ClientActionID] = struct{}{}
		normalized = append(normalized, item)
	}
	for i, j := 0, len(normalized)-1; i < j; i, j = i+1, j-1 {
		normalized[i], normalized[j] = normalized[j], normalized[i]
	}
	if limit > 0 && len(normalized) > limit {
		normalized = append([]ClientActionRecord(nil), normalized[len(normalized)-limit:]...)
	}
	if len(normalized) == 0 {
		return nil
	}
	return normalized
}

func mergeSessionRuntime(base SessionRuntime, overlay SessionRuntime) SessionRuntime {
	return SessionRuntime{
		ResumeSessionID:  firstNonEmptyString(overlay.ResumeSessionID, base.ResumeSessionID),
		Command:          firstNonEmptyString(overlay.Command, base.Command),
		Engine:           firstNonEmptyString(overlay.Engine, base.Engine),
		PermissionMode:   firstNonEmptyString(overlay.PermissionMode, base.PermissionMode),
		CodexSandboxMode: firstNonEmptyString(overlay.CodexSandboxMode, base.CodexSandboxMode),
		CWD:              firstNonEmptyString(overlay.CWD, base.CWD),
		ClaudeLifecycle:  firstNonEmptyString(overlay.ClaudeLifecycle, base.ClaudeLifecycle),
		Source:           firstNonEmptyString(overlay.Source, base.Source),
	}
}

func normalizeContextWindowUsage(usage ContextWindowUsage) ContextWindowUsage {
	if usage.TokenLimit < 0 {
		usage.TokenLimit = 0
	}
	if usage.TokensUsed < 0 {
		usage.TokensUsed = 0
	}
	if usage.TokenLimit > 0 && usage.TokensUsed > usage.TokenLimit {
		usage.TokensUsed = usage.TokenLimit
	}
	if usage.TokenLimit == 0 {
		usage.TokensUsed = 0
	}
	return usage
}

func normalizeSessionContext(ctx SessionContext) SessionContext {
	ctx.EnabledSkillNames = normalizeStringSlice(ctx.EnabledSkillNames)
	ctx.EnabledMemoryIDs = normalizeStringSlice(ctx.EnabledMemoryIDs)
	if len(ctx.EnabledSkillNames) > 0 || len(ctx.EnabledMemoryIDs) > 0 {
		ctx.Configured = true
	}
	return ctx
}

func normalizeSkillCatalogSnapshot(snapshot SkillCatalogSnapshot) SkillCatalogSnapshot {
	snapshot.Meta = normalizeCatalogMetadata(snapshot.Meta, CatalogDomainSkill)
	snapshot.Items = normalizeSkillCatalog(snapshot.Items)
	return snapshot
}

func normalizeMemoryCatalogSnapshot(snapshot MemoryCatalogSnapshot) MemoryCatalogSnapshot {
	snapshot.Meta = normalizeCatalogMetadata(snapshot.Meta, CatalogDomainMemory)
	snapshot.Items = normalizeMemoryCatalog(snapshot.Items)
	return snapshot
}

func normalizePermissionRuleSnapshot(snapshot PermissionRuleSnapshot) PermissionRuleSnapshot {
	snapshot.Items = normalizePermissionRules(snapshot.Items)
	return snapshot
}

func normalizeCatalogMetadata(meta CatalogMetadata, domain CatalogDomain) CatalogMetadata {
	if meta.Domain == "" {
		meta.Domain = domain
	}
	if meta.SourceOfTruth == "" {
		meta.SourceOfTruth = CatalogSourceTruthClaude
	}
	if meta.SyncState == "" {
		meta.SyncState = CatalogSyncStateIdle
	}
	return meta
}

func normalizeSkillCatalog(items []SkillDefinition) []SkillDefinition {
	if len(items) == 0 {
		return []SkillDefinition{}
	}
	out := make([]SkillDefinition, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		name := strings.TrimSpace(item.Name)
		if name == "" {
			continue
		}
		item.Name = name
		item.Description = strings.TrimSpace(item.Description)
		item.Prompt = strings.TrimSpace(item.Prompt)
		item.ResultView = strings.TrimSpace(item.ResultView)
		item.TargetType = strings.TrimSpace(item.TargetType)
		if item.Source == "" {
			item.Source = SkillSourceLocal
		}
		if item.SourceOfTruth == "" {
			item.SourceOfTruth = CatalogSourceTruthClaude
		}
		if item.SyncState == "" {
			if item.Source == SkillSourceLocal {
				item.SyncState = CatalogSyncStateDraft
			} else {
				item.SyncState = CatalogSyncStateIdle
			}
		}
		if item.Source == SkillSourceBuiltin {
			item.Editable = false
		} else if !item.Editable {
			item.Editable = true
		}
		if _, ok := seen[item.Name]; ok {
			continue
		}
		seen[item.Name] = struct{}{}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out
}

func normalizeMemoryCatalog(items []MemoryItem) []MemoryItem {
	if len(items) == 0 {
		return []MemoryItem{}
	}
	out := make([]MemoryItem, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		id := strings.TrimSpace(item.ID)
		if id == "" {
			continue
		}
		item.ID = id
		item.Title = strings.TrimSpace(item.Title)
		item.Content = strings.TrimSpace(item.Content)
		item.Source = strings.TrimSpace(item.Source)
		if item.Source == "" {
			item.Source = "local"
		}
		if item.SourceOfTruth == "" {
			item.SourceOfTruth = CatalogSourceTruthClaude
		}
		if item.SyncState == "" {
			if item.Source == "local" {
				item.SyncState = CatalogSyncStateDraft
			} else {
				item.SyncState = CatalogSyncStateIdle
			}
		}
		if item.Source == "builtin" {
			item.Editable = false
		} else if !item.Editable {
			item.Editable = true
		}
		if _, ok := seen[item.ID]; ok {
			continue
		}
		seen[item.ID] = struct{}{}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt.After(out[j].UpdatedAt)
	})
	return out
}

func normalizeStringSlice(items []string) []string {
	if len(items) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}
	sort.Strings(out)
	return out
}

func normalizePermissionRules(items []PermissionRule) []PermissionRule {
	if len(items) == 0 {
		return []PermissionRule{}
	}
	out := make([]PermissionRule, 0, len(items))
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		item.ID = strings.TrimSpace(item.ID)
		if item.ID == "" {
			continue
		}
		if _, ok := seen[item.ID]; ok {
			continue
		}
		seen[item.ID] = struct{}{}
		item.Engine = strings.TrimSpace(strings.ToLower(item.Engine))
		item.CommandHead = strings.TrimSpace(strings.ToLower(item.CommandHead))
		item.TargetPathPrefix = strings.TrimSpace(item.TargetPathPrefix)
		item.Summary = strings.TrimSpace(item.Summary)
		if item.Scope == "" {
			item.Scope = PermissionScopeSession
		}
		if item.Kind == "" {
			item.Kind = PermissionKindGeneric
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out
}

func buildPreview(summary SessionSummary, projection ProjectionSnapshot) string {
	if summary.External ||
		strings.EqualFold(strings.TrimSpace(summary.Source), "codex-native") ||
		strings.EqualFold(strings.TrimSpace(summary.Runtime.Source), "codex-native") {
		if text := latestMeaningfulProjectionText(projection, false); text != "" {
			return truncatePreview(text)
		}
	}
	if text := latestMeaningfulProjectionText(projection, true); text != "" {
		return truncatePreview(text)
	}
	if text := latestMeaningfulProjectionText(projection, false); text != "" {
		return truncatePreview(text)
	}
	return ""
}

func deriveProjectionSummary(summary SessionSummary, projection ProjectionSnapshot) SessionSummary {
	summary.EntryCount = len(projection.LogEntries)
	if title := buildSummaryTitle(summary.Title, projection, summary.CreatedAt); title != "" {
		summary.Title = title
	}
	summary.LastPreview = buildPreview(summary, projection)
	return summary
}

func buildSummaryTitle(current string, projection ProjectionSnapshot, createdAt time.Time) string {
	normalizedCurrent := normalizeSummaryText(current)
	if isMeaningfulSummaryTitle(normalizedCurrent) {
		return normalizedCurrent
	}
	if text := firstMeaningfulProjectionText(projection, true); text != "" {
		return truncatePreview(text)
	}
	if text := firstMeaningfulProjectionText(projection, false); text != "" {
		return truncatePreview(text)
	}
	if normalizedCurrent != "" {
		if looksLikeSummaryNoise(normalizedCurrent) || looksLikeBootstrapCommand(normalizedCurrent) {
			return fallbackTitle("", nonZeroTime(createdAt, time.Now().UTC()))
		}
		return normalizedCurrent
	}
	return fallbackTitle("", nonZeroTime(createdAt, time.Now().UTC()))
}

func firstMeaningfulProjectionText(projection ProjectionSnapshot, userOnly bool) string {
	for _, entry := range projection.LogEntries {
		if text := meaningfulProjectionEntryText(entry, userOnly); text != "" {
			return text
		}
	}
	return ""
}

func latestMeaningfulProjectionText(projection ProjectionSnapshot, userOnly bool) string {
	for i := len(projection.LogEntries) - 1; i >= 0; i-- {
		if text := meaningfulProjectionEntryText(projection.LogEntries[i], userOnly); text != "" {
			return text
		}
	}
	return ""
}

func meaningfulProjectionEntryText(entry SnapshotLogEntry, userOnly bool) string {
	if userOnly && entry.Kind != "user" {
		return ""
	}
	if isOperationalNativeCodexSummaryEntry(entry) {
		return ""
	}
	var text string
	switch entry.Kind {
	case "markdown", "system", "user":
		text = firstNonEmptyString(entry.Message, entry.Text)
	case "error":
		if entry.Context != nil {
			text = entry.Context.Message
		}
	default:
		return ""
	}
	text = normalizeSummaryText(text)
	if !isMeaningfulSummaryText(text) {
		return ""
	}
	return text
}

func isOperationalNativeCodexSummaryEntry(entry SnapshotLogEntry) bool {
	if entry.Kind != "system" || entry.Context == nil {
		return false
	}
	if strings.TrimSpace(entry.Context.Source) != "codex-native" {
		return false
	}
	switch strings.TrimSpace(entry.Context.Type) {
	case "codex_task", "codex_tool_call", "codex_tool_output", "codex_patch":
		return true
	default:
		return false
	}
}

func truncatePreview(text string) string {
	runes := []rune(text)
	if len(runes) <= 80 {
		return text
	}
	return string(runes[:80]) + "…"
}

func fallbackTitle(title string, now time.Time) string {
	title = strings.TrimSpace(title)
	if title != "" {
		return title
	}
	return now.Local().Format("2006-01-02 15:04")
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func normalizeSummaryText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func isMeaningfulSummaryText(text string) bool {
	if text == "" {
		return false
	}
	return !looksLikeSummaryNoise(text) && !looksLikeBootstrapCommand(text)
}

func isMeaningfulSummaryTitle(text string) bool {
	if !isMeaningfulSummaryText(text) {
		return false
	}
	return !looksLikePlaceholderTitle(text)
}

func looksLikeSummaryNoise(text string) bool {
	lower := strings.ToLower(normalizeSummaryText(text))
	if lower == "" {
		return true
	}
	switch lower {
	case "ok", "done", "running", "thinking", "processing", "active", "ready", "idle", "is ready", "已就绪",
		"session active", "session ready", "command started", "command finished", "status: active", "status: ready", "status: idle":
		return true
	}
	return strings.HasPrefix(lower, "command started ") ||
		strings.HasPrefix(lower, "command finished ") ||
		strings.HasPrefix(lower, "active:") ||
		strings.HasPrefix(lower, "ready:") ||
		strings.HasPrefix(lower, "idle:") ||
		strings.HasPrefix(lower, "--config model_reasoning_effort=") ||
		strings.HasPrefix(lower, "model_reasoning_effort=") ||
		looksLikeTimestampText(lower) ||
		looksLikeModelSummary(lower)
}

func looksLikePlaceholderTitle(text string) bool {
	lower := strings.ToLower(normalizeSummaryText(text))
	if lower == "" {
		return true
	}
	return lower == "session" ||
		lower == "new session" ||
		lower == "history" ||
		sessionPlaceholderPattern.MatchString(lower)
}

func looksLikeTimestampText(text string) bool {
	return sessionTimestampPattern.MatchString(strings.ToLower(normalizeSummaryText(text)))
}

func looksLikeModelSummary(text string) bool {
	fields := strings.Fields(strings.ToLower(normalizeSummaryText(text)))
	if len(fields) < 2 {
		return false
	}
	switch fields[0] {
	case "claude", "codex", "gemini":
	default:
		return false
	}
	for _, field := range fields[1:] {
		if strings.Contains(field, "gpt-") ||
			strings.Contains(field, "sonnet") ||
			strings.Contains(field, "opus") ||
			field == "-low" ||
			field == "-medium" ||
			field == "-high" ||
			field == "low" ||
			field == "medium" ||
			field == "high" {
			return true
		}
	}
	return false
}

func looksLikeBootstrapCommand(text string) bool {
	normalized := normalizeSummaryText(text)
	if normalized == "" {
		return false
	}
	lower := strings.ToLower(normalized)
	if strings.HasPrefix(lower, "--config ") ||
		strings.HasPrefix(lower, "--model ") ||
		strings.HasPrefix(lower, "-m ") {
		return true
	}
	startsWithAICommand := lower == "claude" ||
		strings.HasPrefix(lower, "claude ") ||
		lower == "codex" ||
		strings.HasPrefix(lower, "codex ") ||
		lower == "gemini" ||
		strings.HasPrefix(lower, "gemini ")
	if !startsWithAICommand {
		return false
	}
	if !strings.Contains(normalized, " ") {
		return true
	}
	return strings.Contains(lower, " --model ") ||
		strings.Contains(lower, " -m ") ||
		strings.Contains(lower, " --config ") ||
		strings.Contains(lower, " --permission-mode ") ||
		strings.Contains(lower, " --approval-mode ") ||
		strings.Contains(lower, " --dangerously-skip-permissions") ||
		looksLikeModelSummary(lower)
}

func nonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Time{}
}

func isUntouchedAutoSessionRecord(record SessionRecord) bool {
	summary := record.Summary
	if summary.External || strings.EqualFold(strings.TrimSpace(summary.Source), "codex-native") {
		return false
	}
	runtimeSource := strings.ToLower(strings.TrimSpace(firstNonEmptyString(
		summary.Runtime.Source,
		record.Projection.Runtime.Source,
		summary.Source,
	)))
	if runtimeSource != "" && runtimeSource != "mobilevc" {
		return false
	}
	if summary.EntryCount > 0 || len(record.Projection.LogEntries) > 0 {
		return false
	}
	if strings.TrimSpace(summary.LastPreview) != "" {
		return false
	}
	if strings.TrimSpace(firstNonEmptyString(summary.Runtime.ResumeSessionID, record.Projection.Runtime.ResumeSessionID)) != "" {
		return false
	}
	if strings.TrimSpace(firstNonEmptyString(summary.Runtime.Command, record.Projection.Runtime.Command)) != "" {
		return false
	}
	title := normalizeSummaryText(summary.Title)
	if title == "" {
		return true
	}
	return looksLikePlaceholderTitle(title) || looksLikeTimestampText(title)
}

func selectVisibleSessions(items []SessionSummary, untouched map[string]bool) []SessionSummary {
	if len(items) == 0 {
		return nil
	}
	visible := make([]SessionSummary, 0, len(items))
	placeholderCount := 0
	for _, item := range items {
		if untouched[item.ID] {
			placeholderCount++
			continue
		}
		visible = append(visible, item)
	}
	if len(visible) > 0 {
		return visible
	}
	if placeholderCount <= 1 {
		return append([]SessionSummary(nil), items...)
	}
	newest := items[0]
	for _, item := range items[1:] {
		if item.UpdatedAt.After(newest.UpdatedAt) {
			newest = item
			continue
		}
		if item.UpdatedAt.Equal(newest.UpdatedAt) && item.CreatedAt.After(newest.CreatedAt) {
			newest = item
		}
	}
	return []SessionSummary{newest}
}

func sameSessionSummaryList(a, b []SessionSummary) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (s *FileStore) reconcileIndexLocked(index fileIndex) (fileIndex, error) {
	updated := false
	reconciled := make([]SessionSummary, 0, len(index.Sessions))
	untouched := make(map[string]bool, len(index.Sessions))
	for i := range index.Sessions {
		record, err := s.readSessionLocked(index.Sessions[i].ID)
		if err != nil {
			reconciled = append(reconciled, index.Sessions[i])
			continue
		}
		if index.Sessions[i] != record.Summary {
			updated = true
		}
		reconciled = append(reconciled, record.Summary)
		if isUntouchedAutoSessionRecord(record) {
			untouched[record.Summary.ID] = true
		}
	}
	visible := selectVisibleSessions(reconciled, untouched)
	if !sameSessionSummaryList(index.Sessions, visible) {
		index.Sessions = visible
		updated = true
	}
	if updated {
		if err := s.writeIndexLocked(index); err != nil {
			return fileIndex{}, err
		}
	}
	return index, nil
}

func (s *FileStore) SavePushToken(ctx context.Context, sessionID, token, platform string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tokens := make(map[string]map[string]string)
	if data, err := os.ReadFile(s.pushTokensPath); err == nil {
		_ = json.Unmarshal(data, &tokens)
	}

	if tokens[sessionID] == nil {
		tokens[sessionID] = make(map[string]string)
	}
	tokens[sessionID]["token"] = token
	tokens[sessionID]["platform"] = platform

	data, err := json.MarshalIndent(tokens, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal push tokens: %w", err)
	}

	return os.WriteFile(s.pushTokensPath, data, 0600)
}

func (s *FileStore) GetPushToken(ctx context.Context, sessionID string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.pushTokensPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", "", nil
		}
		return "", "", err
	}

	tokens := make(map[string]map[string]string)
	if err := json.Unmarshal(data, &tokens); err != nil {
		return "", "", err
	}

	if info, ok := tokens[sessionID]; ok {
		return info["token"], info["platform"], nil
	}

	return "", "", nil
}
