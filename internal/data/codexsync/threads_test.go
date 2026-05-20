package codexsync

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestListNativeThreadsMatchesWindowsDevicePathPrefix(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	devicePath := `\\?\` + projectDir
	threadID := seedNativeCodexThreadFixture(t, homeDir, devicePath)

	threads, err := ListNativeThreads(context.Background(), projectDir)
	if err != nil {
		t.Fatalf("list native threads: %v", err)
	}
	if len(threads) != 1 {
		t.Fatalf("expected one matched thread, got %#v", threads)
	}
	if threads[0].ThreadID != threadID {
		t.Fatalf("expected thread %q, got %#v", threadID, threads[0])
	}
	if got := normalizePath(threads[0].CWD); got != normalizePath(projectDir) {
		t.Fatalf("expected normalized cwd %q, got %q", normalizePath(projectDir), got)
	}
}

func TestNormalizePathStripsWindowsDevicePrefix(t *testing.T) {
	raw := `\\?\C:\Users\29573\Desktop\fsdownload\codexxm`
	if got := normalizePath(raw); got == raw {
		t.Fatalf("expected device prefix to be stripped, got %q", got)
	}
	if got := trimWindowsDevicePathPrefix(raw); got != `C:\Users\29573\Desktop\fsdownload\codexxm` {
		t.Fatalf("expected trimmed device path, got %q", got)
	}
}

func TestFindNativeThreadLoadsOnlyTargetSessionData(t *testing.T) {
	homeDir := t.TempDir()
	projectDir := filepath.Join(homeDir, "workspace", "MobileVC")
	otherProjectDir := filepath.Join(homeDir, "workspace", "OtherProject")
	if err := os.MkdirAll(projectDir, 0o755); err != nil {
		t.Fatalf("mkdir project dir: %v", err)
	}
	if err := os.MkdirAll(otherProjectDir, 0o755); err != nil {
		t.Fatalf("mkdir other project dir: %v", err)
	}
	t.Setenv("HOME", homeDir)
	t.Setenv("USERPROFILE", homeDir)
	t.Setenv("HOMEDRIVE", "")
	t.Setenv("HOMEPATH", "")

	targetThreadID := seedNativeCodexThreadFixture(t, homeDir, projectDir)
	seedAdditionalNativeCodexThreadFixture(
		t,
		homeDir,
		"019e9999-c538-7420-8028-345f7dd70d63",
		otherProjectDir,
		"Other thread title",
		"Other history prompt",
		"Other assistant output should not appear",
	)

	thread, err := FindNativeThread(context.Background(), targetThreadID)
	if err != nil {
		t.Fatalf("find native thread: %v", err)
	}
	if thread.ThreadID != targetThreadID {
		t.Fatalf("expected target thread %q, got %#v", targetThreadID, thread)
	}
	if got := normalizePath(thread.CWD); got != normalizePath(projectDir) {
		t.Fatalf("expected target cwd %q, got %q", normalizePath(projectDir), got)
	}
	if len(thread.HistoryPrompts) == 0 {
		t.Fatalf("expected target history prompts, got %#v", thread)
	}
	for _, prompt := range thread.HistoryPrompts {
		if strings.Contains(prompt.Text, "Other history prompt") {
			t.Fatalf("unexpected other-thread prompt leaked into target history: %#v", thread.HistoryPrompts)
		}
	}
	for _, entry := range thread.LogEntries {
		text := firstNonEmpty(entry.Text, entry.Message)
		if strings.Contains(text, "Other assistant output should not appear") {
			t.Fatalf("unexpected other-thread rollout content leaked into target log entries: %#v", thread.LogEntries)
		}
	}
}

func seedNativeCodexThreadFixture(t *testing.T, homeDir, cwd string) string {
	t.Helper()

	codexDir := filepath.Join(homeDir, ".codex")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}

	threadID := "019d3c6b-c538-7420-8028-345f7dd70d63"
	createdAt := time.Date(2026, 3, 29, 10, 0, 0, 0, time.UTC).Unix()
	updatedAt := time.Date(2026, 3, 30, 11, 30, 0, 0, time.UTC).Unix()
	rolloutPath := filepath.Join(
		codexDir,
		"sessions",
		"2026",
		"03",
		"30",
		"rollout-2026-03-30T11-30-00-"+threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o755); err != nil {
		t.Fatalf("mkdir rollout dir: %v", err)
	}
	dbPath := filepath.Join(codexDir, "state_5.sqlite")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	statements := []string{
		"create table if not exists threads (id text primary key, cwd text, title text, model text, source text, model_provider text, created_at integer, updated_at integer, first_user_message text, rollout_path text, archived integer default 0);",
		"delete from threads;",
		"insert into threads (id, cwd, title, model, source, model_provider, created_at, updated_at, first_user_message, rollout_path, archived) values ('019d3c6b-c538-7420-8028-345f7dd70d63', '" + escapeSQLiteString(cwd) + "', 'Desktop Codex Session', 'gpt-5-codex', 'codex', 'openai', " + strconv.FormatInt(createdAt, 10) + ", " + strconv.FormatInt(updatedAt, 10) + ", 'Fix the README wording', '" + escapeSQLiteString(rolloutPath) + "', 0);",
	}
	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			t.Fatalf("seed sqlite fixture: %v", err)
		}
	}

	historyPath := filepath.Join(codexDir, "history.jsonl")
	file, err := os.Create(historyPath)
	if err != nil {
		t.Fatalf("create history fixture: %v", err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	for _, line := range []map[string]any{
		{"session_id": threadID, "ts": createdAt, "text": "Fix the README wording"},
		{"session_id": threadID, "ts": updatedAt, "text": "Also align the mobile labels"},
	} {
		if err := encoder.Encode(line); err != nil {
			t.Fatalf("write history fixture: %v", err)
		}
	}

	rolloutFile, err := os.Create(rolloutPath)
	if err != nil {
		t.Fatalf("create rollout fixture: %v", err)
	}
	defer rolloutFile.Close()
	rolloutEncoder := json.NewEncoder(rolloutFile)
	for _, line := range []map[string]any{
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
	} {
		if err := rolloutEncoder.Encode(line); err != nil {
			t.Fatalf("write rollout fixture: %v", err)
		}
	}

	return threadID
}

func seedAdditionalNativeCodexThreadFixture(
	t *testing.T,
	homeDir string,
	threadID string,
	cwd string,
	title string,
	historyText string,
	assistantText string,
) {
	t.Helper()

	codexDir := filepath.Join(homeDir, ".codex")
	if err := os.MkdirAll(codexDir, 0o755); err != nil {
		t.Fatalf("mkdir codex dir: %v", err)
	}
	createdAt := time.Date(2026, 4, 1, 10, 0, 0, 0, time.UTC).Unix()
	updatedAt := time.Date(2026, 4, 1, 11, 0, 0, 0, time.UTC).Unix()
	rolloutPath := filepath.Join(
		codexDir,
		"sessions",
		"2026",
		"04",
		"01",
		"rollout-2026-04-01T11-00-00-"+threadID+".jsonl",
	)
	if err := os.MkdirAll(filepath.Dir(rolloutPath), 0o755); err != nil {
		t.Fatalf("mkdir rollout dir: %v", err)
	}
	dbPath := filepath.Join(codexDir, "state_5.sqlite")

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(
		"insert into threads (id, cwd, title, model, source, model_provider, created_at, updated_at, first_user_message, rollout_path, archived) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)",
		threadID,
		cwd,
		title,
		"gpt-5-codex",
		"codex",
		"openai",
		createdAt,
		updatedAt,
		historyText,
		rolloutPath,
	); err != nil {
		t.Fatalf("insert additional sqlite fixture: %v", err)
	}

	historyPath := filepath.Join(codexDir, "history.jsonl")
	file, err := os.OpenFile(historyPath, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open history fixture for append: %v", err)
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(map[string]any{
		"session_id": threadID,
		"ts":         updatedAt,
		"text":       historyText,
	}); err != nil {
		t.Fatalf("append history fixture: %v", err)
	}

	rolloutFile, err := os.Create(rolloutPath)
	if err != nil {
		t.Fatalf("create additional rollout fixture: %v", err)
	}
	defer rolloutFile.Close()
	rolloutEncoder := json.NewEncoder(rolloutFile)
	for _, line := range []map[string]any{
		{
			"timestamp": time.Unix(createdAt, 0).UTC().Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":    "task_started",
				"turn_id": "turn-other",
			},
		},
		{
			"timestamp": time.Unix(updatedAt, 0).UTC().Format(time.RFC3339),
			"type":      "event_msg",
			"payload": map[string]any{
				"type":    "agent_message",
				"message": assistantText,
			},
		},
	} {
		if err := rolloutEncoder.Encode(line); err != nil {
			t.Fatalf("write additional rollout fixture: %v", err)
		}
	}
}

func escapeSQLiteString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
