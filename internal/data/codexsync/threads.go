package codexsync

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"mobilevc/internal/data"
	"mobilevc/internal/protocol"

	_ "modernc.org/sqlite"
)

const mirrorPrefix = "codex-thread:"

type NativeThread struct {
	ThreadID         string
	MirrorSessionID  string
	Title            string
	CWD              string
	Model            string
	Source           string
	ModelProvider    string
	ThreadSource     string
	CreatedAt        time.Time
	UpdatedAt        time.Time
	FirstUserMessage string
	RolloutPath      string
	HistoryPrompts   []NativePrompt
	LogEntries       []data.SnapshotLogEntry
	ControllerState  data.ControllerState
	ClaudeLifecycle  string
}

type NativePrompt struct {
	Text      string
	Timestamp time.Time
}

type historyLine struct {
	SessionID string `json:"session_id"`
	TS        int64  `json:"ts"`
	Text      string `json:"text"`
}

type rolloutEnvelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type rolloutEventPayload struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type rolloutResponseItemPayload struct {
	Type    string                   `json:"type"`
	Role    string                   `json:"role"`
	Content []rolloutResponseContent `json:"content"`
}

type rolloutResponseContent struct {
	Text string `json:"text"`
}

type nativeRolloutSnapshot struct {
	LogEntries      []data.SnapshotLogEntry
	ControllerState data.ControllerState
	ClaudeLifecycle string
}

func MirrorSessionID(threadID string) string {
	return mirrorPrefix + strings.TrimSpace(threadID)
}

func IsMirrorSessionID(sessionID string) bool {
	return strings.HasPrefix(strings.TrimSpace(sessionID), mirrorPrefix)
}

func ThreadIDFromMirror(sessionID string) string {
	return strings.TrimPrefix(strings.TrimSpace(sessionID), mirrorPrefix)
}

func ListNativeThreads(ctx context.Context, cwdFilter string) ([]NativeThread, error) {
	threads, err := loadNativeThreadRows(ctx)
	if err != nil {
		return nil, err
	}
	if len(threads) == 0 {
		return []NativeThread{}, nil
	}
	normalizedFilter := normalizePath(cwdFilter)
	result := make([]NativeThread, 0, len(threads))
	for _, thread := range threads {
		if IsSubagentThread(thread) {
			continue
		}
		if normalizedFilter != "" && normalizePath(thread.CWD) != normalizedFilter {
			continue
		}
		result = append(result, hydrateNativeThreadSummary(thread))
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].UpdatedAt.After(result[j].UpdatedAt)
	})
	return result, nil
}

func ListNativeSubagentThreadIDs(ctx context.Context, cwdFilter string) (map[string]struct{}, error) {
	threads, err := loadNativeThreadRows(ctx)
	if err != nil {
		return nil, err
	}
	normalizedFilter := normalizePath(cwdFilter)
	result := make(map[string]struct{})
	for _, thread := range threads {
		if !IsSubagentThread(thread) {
			continue
		}
		if normalizedFilter != "" && normalizePath(thread.CWD) != normalizedFilter {
			continue
		}
		threadID := strings.TrimSpace(thread.ThreadID)
		if threadID != "" {
			result[threadID] = struct{}{}
		}
	}
	return result, nil
}

func IsSubagentThread(thread NativeThread) bool {
	return strings.EqualFold(strings.TrimSpace(thread.ThreadSource), "subagent")
}

func loadNativeThreadRows(ctx context.Context) ([]NativeThread, error) {
	dbPath, _, err := codexNativePaths()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return []NativeThread{}, nil
		}
		return nil, fmt.Errorf("stat codex sqlite failed: %w", err)
	}
	return queryThreads(ctx, dbPath)
}

func FindNativeThread(ctx context.Context, sessionID string) (NativeThread, error) {
	threadID := strings.TrimSpace(sessionID)
	if IsMirrorSessionID(threadID) {
		threadID = ThreadIDFromMirror(threadID)
	}
	if threadID == "" {
		return NativeThread{}, fmt.Errorf("empty codex thread id")
	}
	dbPath, historyPath, err := codexNativePaths()
	if err != nil {
		return NativeThread{}, err
	}
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return NativeThread{}, fmt.Errorf("codex thread not found: %s", threadID)
		}
		return NativeThread{}, fmt.Errorf("stat codex sqlite failed: %w", err)
	}
	thread, err := queryThread(ctx, dbPath, threadID)
	if err != nil {
		return NativeThread{}, err
	}
	prompts, err := loadHistoryForSession(historyPath, threadID)
	if err != nil {
		return NativeThread{}, err
	}
	thread.HistoryPrompts = prompts
	return hydrateNativeThread(thread), nil
}

func MirrorRecord(thread NativeThread) data.SessionRecord {
	title := strings.TrimSpace(thread.Title)
	if !isMeaningfulPromptText(title) {
		title = latestMeaningfulPrompt(thread.HistoryPrompts)
	}
	if !isMeaningfulPromptText(title) {
		title = latestMeaningfulNativeLogText(thread.LogEntries)
	}
	if !isMeaningfulPromptText(title) {
		title = strings.TrimSpace(thread.FirstUserMessage)
	}
	if !isMeaningfulPromptText(title) {
		title = "Codex 会话"
	}
	preview := latestMeaningfulNativeLogText(thread.LogEntries)
	if !isMeaningfulPromptText(preview) {
		preview = latestMeaningfulPrompt(thread.HistoryPrompts)
	}
	if !isMeaningfulPromptText(preview) {
		preview = strings.TrimSpace(thread.FirstUserMessage)
	}
	if !isMeaningfulPromptText(preview) {
		preview = title
	}
	entries := append([]data.SnapshotLogEntry(nil), thread.LogEntries...)
	if len(entries) == 0 {
		entries = buildPromptLogEntries(thread.HistoryPrompts)
	}
	lifecycle := strings.TrimSpace(thread.ClaudeLifecycle)
	if lifecycle == "" {
		lifecycle = "resumable"
	}
	runtime := data.SessionRuntime{
		ResumeSessionID: thread.ThreadID,
		Command:         "codex",
		Engine:          "codex",
		CWD:             thread.CWD,
		ClaudeLifecycle: lifecycle,
		Source:          "codex-native",
	}
	controllerState := thread.ControllerState
	if controllerState == "" {
		controllerState = controllerStateFromLifecycle(lifecycle)
	}
	controller := data.ControllerSnapshot{
		SessionID:       MirrorSessionID(thread.ThreadID),
		State:           controllerState,
		CurrentCommand:  "codex",
		ResumeSession:   thread.ThreadID,
		ClaudeLifecycle: lifecycle,
		ActiveMeta: protocol.RuntimeMeta{
			ResumeSessionID: thread.ThreadID,
			Command:         "codex",
			Engine:          "codex",
			Model:           thread.Model,
			CWD:             thread.CWD,
			ClaudeLifecycle: lifecycle,
		},
	}
	projection := data.ProjectionSnapshot{
		LogEntries:          entries,
		RawTerminalByStream: map[string]string{"stdout": "", "stderr": ""},
		Controller:          controller,
		Runtime:             runtime,
	}
	return data.SessionRecord{
		Summary: data.SessionSummary{
			ID:          MirrorSessionID(thread.ThreadID),
			Title:       title,
			CreatedAt:   nonZeroTime(thread.CreatedAt, thread.UpdatedAt, time.Now().UTC()),
			UpdatedAt:   nonZeroTime(thread.UpdatedAt, thread.CreatedAt, time.Now().UTC()),
			LastPreview: preview,
			EntryCount:  len(entries),
			Runtime:     runtime,
			Source:      "codex-native",
			External:    true,
		},
		Projection: projection,
	}
}

func queryThreads(ctx context.Context, dbPath string) ([]NativeThread, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open codex sqlite failed: %w", err)
	}
	defer db.Close()

	rows, err := queryThreadRows(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("query codex threads failed: %w", err)
	}
	defer rows.Close()

	var items []NativeThread
	for rows.Next() {
		thread, err := scanNativeThread(rows)
		if err != nil {
			return nil, fmt.Errorf("scan codex thread row: %w", err)
		}
		items = append(items, thread)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate codex threads: %w", err)
	}
	if items == nil {
		return []NativeThread{}, nil
	}
	return items, nil
}

func queryThread(ctx context.Context, dbPath, threadID string) (NativeThread, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return NativeThread{}, fmt.Errorf("open codex sqlite failed: %w", err)
	}
	defer db.Close()

	thread, err := queryThreadByID(ctx, db, threadID)
	if errors.Is(err, sql.ErrNoRows) {
		return NativeThread{}, fmt.Errorf("codex thread not found: %s", threadID)
	}
	if err != nil {
		return NativeThread{}, fmt.Errorf("query codex thread failed: %w", err)
	}
	return thread, nil
}

func queryThreadRows(ctx context.Context, db *sql.DB) (*sql.Rows, error) {
	const queryWithThreadSource = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), coalesce(rollout_path,''), coalesce(thread_source,'') from threads where archived = 0 order by updated_at desc`
	const queryWithoutThreadSource = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), coalesce(rollout_path,''), '' from threads where archived = 0 order by updated_at desc`
	const queryWithoutRollout = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), '', '' from threads where archived = 0 order by updated_at desc`

	rows, err := db.QueryContext(ctx, queryWithThreadSource)
	if err == nil {
		return rows, nil
	}
	if isMissingColumn(err, "thread_source") {
		rows, err = db.QueryContext(ctx, queryWithoutThreadSource)
		if err == nil {
			return rows, nil
		}
	}
	if isMissingColumn(err, "rollout_path") {
		return db.QueryContext(ctx, queryWithoutRollout)
	}
	return nil, err
}

func queryThreadByID(ctx context.Context, db *sql.DB, threadID string) (NativeThread, error) {
	const queryWithThreadSource = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), coalesce(rollout_path,''), coalesce(thread_source,'') from threads where archived = 0 and id = ? limit 1`
	const queryWithoutThreadSource = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), coalesce(rollout_path,''), '' from threads where archived = 0 and id = ? limit 1`
	const queryWithoutRollout = `select id, cwd, title, coalesce(model,''), coalesce(source,''), coalesce(model_provider,''), created_at, updated_at, coalesce(first_user_message,''), '', '' from threads where archived = 0 and id = ? limit 1`

	thread, err := scanNativeThread(db.QueryRowContext(ctx, queryWithThreadSource, threadID))
	if err == nil {
		return thread, nil
	}
	if isMissingColumn(err, "thread_source") {
		thread, err = scanNativeThread(db.QueryRowContext(ctx, queryWithoutThreadSource, threadID))
		if err == nil {
			return thread, nil
		}
	}
	if isMissingColumn(err, "rollout_path") {
		return scanNativeThread(db.QueryRowContext(ctx, queryWithoutRollout, threadID))
	}
	return NativeThread{}, err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanNativeThread(scanner rowScanner) (NativeThread, error) {
	var (
		id, cwd, title, model, source, modelProvider string
		createdAt, updatedAt                         int64
		firstUserMessage, rolloutPath, threadSource  string
	)
	if err := scanner.Scan(&id, &cwd, &title, &model, &source, &modelProvider, &createdAt, &updatedAt, &firstUserMessage, &rolloutPath, &threadSource); err != nil {
		return NativeThread{}, err
	}
	return NativeThread{
		ThreadID:         id,
		CWD:              cwd,
		Title:            title,
		Model:            model,
		Source:           source,
		ModelProvider:    modelProvider,
		ThreadSource:     threadSource,
		CreatedAt:        time.Unix(createdAt, 0).UTC(),
		UpdatedAt:        time.Unix(updatedAt, 0).UTC(),
		FirstUserMessage: firstUserMessage,
		RolloutPath:      rolloutPath,
	}, nil
}

func isMissingColumn(err error, name string) bool {
	return err != nil && strings.Contains(err.Error(), "no such column: "+name)
}

func loadHistory(path string) (map[string][]NativePrompt, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string][]NativePrompt{}, nil
		}
		return nil, fmt.Errorf("open codex history failed: %w", err)
	}
	defer file.Close()

	items := map[string][]NativePrompt{}
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var line historyLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		sessionID := strings.TrimSpace(line.SessionID)
		text := strings.TrimSpace(line.Text)
		if sessionID == "" || text == "" {
			continue
		}
		items[sessionID] = append(items[sessionID], NativePrompt{Text: text, Timestamp: time.Unix(line.TS, 0).UTC()})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan codex history failed: %w", err)
	}
	return items, nil
}

func loadHistoryForSession(path, targetSessionID string) ([]NativePrompt, error) {
	targetSessionID = strings.TrimSpace(targetSessionID)
	if targetSessionID == "" {
		return nil, nil
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []NativePrompt{}, nil
		}
		return nil, fmt.Errorf("open codex history failed: %w", err)
	}
	defer file.Close()

	var items []NativePrompt
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		var line historyLine
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		sessionID := strings.TrimSpace(line.SessionID)
		text := strings.TrimSpace(line.Text)
		if sessionID != targetSessionID || text == "" {
			continue
		}
		items = append(items, NativePrompt{Text: text, Timestamp: time.Unix(line.TS, 0).UTC()})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan codex history failed: %w", err)
	}
	if items == nil {
		return []NativePrompt{}, nil
	}
	return items, nil
}

func hydrateNativeThread(thread NativeThread) NativeThread {
	thread.MirrorSessionID = MirrorSessionID(thread.ThreadID)
	if rollout, err := loadRollout(thread.RolloutPath); err == nil {
		thread.LogEntries = rollout.LogEntries
		thread.ControllerState = rollout.ControllerState
		thread.ClaudeLifecycle = rollout.ClaudeLifecycle
	}
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = latestMeaningfulPrompt(thread.HistoryPrompts)
	}
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = latestMeaningfulNativeLogText(thread.LogEntries)
	}
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = strings.TrimSpace(thread.FirstUserMessage)
	}
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = "Codex 会话"
	}
	return thread
}

func hydrateNativeThreadSummary(thread NativeThread) NativeThread {
	thread.MirrorSessionID = MirrorSessionID(thread.ThreadID)
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = strings.TrimSpace(thread.FirstUserMessage)
	}
	if !isMeaningfulPromptText(thread.Title) {
		thread.Title = "Codex 会话"
	}
	return thread
}

func loadRollout(path string) (nativeRolloutSnapshot, error) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return nativeRolloutSnapshot{}, nil
	}
	file, err := os.Open(trimmed)
	if err != nil {
		if os.IsNotExist(err) {
			return nativeRolloutSnapshot{}, nil
		}
		return nativeRolloutSnapshot{}, fmt.Errorf("open codex rollout failed: %w", err)
	}
	defer file.Close()

	snapshot := nativeRolloutSnapshot{
		ControllerState: data.ControllerStateIdle,
		ClaudeLifecycle: "resumable",
	}
	scanner := bufio.NewScanner(file)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 2*1024*1024)
	taskOpen := false
	seenMessages := map[string]struct{}{}
	for scanner.Scan() {
		var line rolloutEnvelope
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			continue
		}
		timestamp := normalizeRolloutTimestamp(line.Timestamp)
		switch strings.TrimSpace(line.Type) {
		case "event_msg":
			var payload rolloutEventPayload
			if err := json.Unmarshal(line.Payload, &payload); err != nil {
				continue
			}
			switch strings.TrimSpace(payload.Type) {
			case "task_started":
				taskOpen = true
				snapshot.ControllerState = data.ControllerStateThinking
				snapshot.ClaudeLifecycle = "active"
			case "task_complete", "turn_aborted":
				taskOpen = false
				snapshot.ControllerState = data.ControllerStateIdle
				snapshot.ClaudeLifecycle = "resumable"
			case "user_message":
				message := strings.TrimSpace(payload.Message)
				if !isMeaningfulPromptText(message) {
					continue
				}
				appendNativeUserMessage(&snapshot, seenMessages, message, timestamp)
			case "agent_message":
				appendNativeAssistantMessage(&snapshot, seenMessages, payload.Message, timestamp)
			}
		case "response_item":
			var payload rolloutResponseItemPayload
			if err := json.Unmarshal(line.Payload, &payload); err != nil {
				continue
			}
			if strings.TrimSpace(payload.Type) != "message" {
				continue
			}
			message := strings.TrimSpace(responseItemText(payload.Content))
			switch strings.TrimSpace(payload.Role) {
			case "user":
				if !isMeaningfulPromptText(message) {
					continue
				}
				appendNativeUserMessage(&snapshot, seenMessages, message, timestamp)
			case "assistant":
				appendNativeAssistantMessage(&snapshot, seenMessages, message, timestamp)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return nativeRolloutSnapshot{}, fmt.Errorf("scan codex rollout failed: %w", err)
	}
	if taskOpen {
		snapshot.ControllerState = data.ControllerStateThinking
		snapshot.ClaudeLifecycle = "active"
	}
	return snapshot, nil
}

func codexNativePaths() (string, string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", "", fmt.Errorf("resolve home dir failed: %w", err)
	}
	codexDir := filepath.Join(home, ".codex")
	return filepath.Join(codexDir, "state_5.sqlite"), filepath.Join(codexDir, "history.jsonl"), nil
}

func appendNativeUserMessage(snapshot *nativeRolloutSnapshot, seen map[string]struct{}, message, timestamp string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	key := nativeMessageKey("user", message, timestamp)
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	snapshot.LogEntries = append(snapshot.LogEntries, data.SnapshotLogEntry{
		Kind:      "user",
		Label:     "历史输入",
		Message:   message,
		Text:      message,
		Timestamp: timestamp,
	})
}

func appendNativeAssistantMessage(snapshot *nativeRolloutSnapshot, seen map[string]struct{}, message, timestamp string) {
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	key := nativeMessageKey("assistant", message, timestamp)
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	snapshot.LogEntries = append(snapshot.LogEntries, data.SnapshotLogEntry{
		Kind:      "markdown",
		Message:   message,
		Text:      message,
		Timestamp: timestamp,
	})
}

func nativeMessageKey(role, message, timestamp string) string {
	return strings.Join([]string{strings.TrimSpace(role), strings.TrimSpace(message), strings.TrimSpace(timestamp)}, "\x1f")
}

func responseItemText(items []rolloutResponseContent) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		text := strings.TrimSpace(item.Text)
		if text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func normalizeRolloutTimestamp(value string) string {
	parsed := strings.TrimSpace(value)
	if parsed == "" {
		return time.Now().UTC().Format(time.RFC3339)
	}
	if ts, err := time.Parse(time.RFC3339Nano, parsed); err == nil {
		return ts.UTC().Format(time.RFC3339)
	}
	return parsed
}

func normalizePath(value string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return ""
	}
	absPath, err := filepath.Abs(trimmed)
	if err == nil {
		trimmed = absPath
	}
	if resolved, err := filepath.EvalSymlinks(trimmed); err == nil && strings.TrimSpace(resolved) != "" {
		trimmed = resolved
	}
	cleaned := filepath.Clean(trimmed)
	return strings.TrimSuffix(cleaned, string(filepath.Separator))
}

func latestMeaningfulPrompt(items []NativePrompt) string {
	for i := len(items) - 1; i >= 0; i-- {
		text := strings.TrimSpace(items[i].Text)
		if isMeaningfulPromptText(text) {
			return text
		}
	}
	return ""
}

func latestMeaningfulNativeLogText(entries []data.SnapshotLogEntry) string {
	for i := len(entries) - 1; i >= 0; i-- {
		text := strings.TrimSpace(firstNonEmpty(entries[i].Text, entries[i].Message))
		if text == "" {
			continue
		}
		if entries[i].Kind == "user" && !isMeaningfulPromptText(text) {
			continue
		}
		return text
	}
	return ""
}

func isMeaningfulPromptText(text string) bool {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return false
	}
	lower := strings.ToLower(trimmed)
	if lower == "session" ||
		lower == "new session" ||
		lower == "command started" ||
		lower == "command finished" ||
		strings.HasPrefix(lower, "command finished ") ||
		strings.HasPrefix(lower, "--config ") ||
		strings.HasPrefix(lower, "model_reasoning_effort=") {
		return false
	}
	if strings.HasPrefix(lower, "codex ") || lower == "codex" {
		if strings.Contains(lower, "gpt-") ||
			strings.Contains(lower, "sonnet") ||
			strings.Contains(lower, "opus") ||
			strings.HasSuffix(lower, "-low") ||
			strings.HasSuffix(lower, "-medium") ||
			strings.HasSuffix(lower, "-high") {
			return false
		}
	}
	return true
}

func buildPromptLogEntries(items []NativePrompt) []data.SnapshotLogEntry {
	entries := make([]data.SnapshotLogEntry, 0, len(items))
	for _, item := range items {
		entries = append(entries, data.SnapshotLogEntry{
			Kind:      "user",
			Label:     "历史输入",
			Message:   item.Text,
			Text:      item.Text,
			Timestamp: item.Timestamp.UTC().Format(time.RFC3339),
		})
	}
	return entries
}

func controllerStateFromLifecycle(lifecycle string) data.ControllerState {
	switch strings.TrimSpace(lifecycle) {
	case "waiting_input":
		return data.ControllerStateWaitInput
	case "starting", "active":
		return data.ControllerStateThinking
	case "resumable":
		return data.ControllerStateIdle
	default:
		return data.ControllerStateIdle
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func nonZeroTime(values ...time.Time) time.Time {
	for _, value := range values {
		if !value.IsZero() {
			return value
		}
	}
	return time.Now().UTC()
}
