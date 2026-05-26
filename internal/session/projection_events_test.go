package session

import (
	"strings"
	"testing"
	"time"

	"mobilevc/internal/data"
	"mobilevc/internal/protocol"
)

func TestToProtocolSummary(t *testing.T) {
	created := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	in := data.SessionSummary{
		ID:              "s1",
		Title:           "title",
		CreatedAt:       created,
		UpdatedAt:       created,
		LastPreview:     "hi",
		EntryCount:      3,
		Source:          "mobilevc",
		External:        true,
		Ownership:       "user",
		ExecutionActive: true,
		Runtime: data.SessionRuntime{
			ResumeSessionID: "r1",
			Command:         "claude",
			Engine:          "claude",
			CWD:             "/p",
			PermissionMode:  "auto",
			ClaudeLifecycle: "active",
			Source:          "mobilevc",
		},
	}
	got := ToProtocolSummary(in)
	if got.ID != "s1" || got.Title != "title" || got.LastPreview != "hi" || got.EntryCount != 3 {
		t.Errorf("basic fields: %+v", got)
	}
	if got.External != true || got.Ownership != "user" || got.ExecutionActive != true {
		t.Errorf("flags: %+v", got)
	}
	if got.Runtime.ResumeSessionID != "r1" || got.Runtime.Command != "claude" || got.Runtime.Engine != "claude" {
		t.Errorf("runtime: %+v", got.Runtime)
	}
	if !strings.HasPrefix(got.CreatedAt, "2026-01-01") {
		t.Errorf("created at format: %q", got.CreatedAt)
	}
}

func TestToProtocolSessionContext(t *testing.T) {
	in := data.SessionContext{
		EnabledSkillNames: []string{"a", "b"},
		EnabledMemoryIDs:  []string{"m1"},
		Configured:        true,
	}
	got := ToProtocolSessionContext(in)
	if len(got.EnabledSkillNames) != 2 || got.EnabledSkillNames[0] != "a" {
		t.Errorf("skills: %+v", got.EnabledSkillNames)
	}
	if len(got.EnabledMemoryIDs) != 1 || got.EnabledMemoryIDs[0] != "m1" {
		t.Errorf("memory: %+v", got.EnabledMemoryIDs)
	}
	// 验证拷贝独立: 修改原 slice 不影响 got
	in.EnabledSkillNames[0] = "x"
	if got.EnabledSkillNames[0] == "x" {
		t.Errorf("expected independent copy")
	}
}

func TestToProtocolContextWindowUsage(t *testing.T) {
	got := ToProtocolContextWindowUsage(data.ContextWindowUsage{
		TokensUsed: 250,
		TokenLimit: 200,
	})
	if got.TokenLimit != 200 {
		t.Fatalf("expected token limit 200, got %d", got.TokenLimit)
	}
	if got.TokensUsed != 200 {
		t.Fatalf("expected clamped tokens used 200, got %d", got.TokensUsed)
	}
}

func TestToProtocolCatalogMetadata(t *testing.T) {
	t.Run("zero time omitted", func(t *testing.T) {
		got := ToProtocolCatalogMetadata(data.CatalogMetadata{})
		if got.LastSyncedAt != "" {
			t.Errorf("expected empty, got %q", got.LastSyncedAt)
		}
	})
	t.Run("synced at formatted", func(t *testing.T) {
		ts := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
		got := ToProtocolCatalogMetadata(data.CatalogMetadata{LastSyncedAt: ts, VersionToken: "v1", LastError: "err"})
		if !strings.HasPrefix(got.LastSyncedAt, "2026-01-01") {
			t.Errorf("formatted: %q", got.LastSyncedAt)
		}
		if got.VersionToken != "v1" || got.LastError != "err" {
			t.Errorf("misc: %+v", got)
		}
	})
}

func TestHistoryContextFromSnapshot(t *testing.T) {
	if got := HistoryContextFromSnapshot(nil); got != nil {
		t.Errorf("expected nil for nil input")
	}
	in := &data.SnapshotContext{
		ID: "id", Type: "step", Message: "msg", Path: "p",
		PendingReview: true, GroupID: "g1",
	}
	got := HistoryContextFromSnapshot(in)
	if got.ID != "id" || got.Path != "p" || !got.PendingReview || got.GroupID != "g1" {
		t.Errorf("got %+v", got)
	}
}

func TestProtocolReviewFile(t *testing.T) {
	in := ReviewFile{ContextID: "f1", Path: "p", Title: "T", Diff: "d", PendingReview: true}
	got := ProtocolReviewFile(in)
	if got.ID != "f1" || got.Path != "p" || got.Title != "T" || !got.PendingReview {
		t.Errorf("got %+v", got)
	}
}

func TestProtocolReviewGroup(t *testing.T) {
	if got := ProtocolReviewGroup(nil); got != nil {
		t.Errorf("expected nil for nil")
	}
	in := &ReviewGroup{
		ID:    "g1",
		Title: "T",
		Files: []ReviewFile{{ContextID: "f1"}, {ContextID: "f2"}},
	}
	got := ProtocolReviewGroup(in)
	if got.ID != "g1" {
		t.Errorf("id: %q", got.ID)
	}
	if len(got.Files) != 2 {
		t.Errorf("files: %+v", got.Files)
	}
}

func TestProtocolReviewGroups(t *testing.T) {
	if got := ProtocolReviewGroups(nil); got != nil {
		t.Errorf("expected nil")
	}
	got := ProtocolReviewGroups([]ReviewGroup{{ID: "g1"}, {ID: "g2"}})
	if len(got) != 2 {
		t.Errorf("expected 2, got %+v", got)
	}
}

func TestProtocolDiffContextAndContexts(t *testing.T) {
	if got := ProtocolDiffContext(nil); got != nil {
		t.Errorf("expected nil")
	}
	d := &DiffContext{ContextID: "c1", Path: "p", Diff: "+ a", PendingReview: true, GroupID: "g1"}
	got := ProtocolDiffContext(d)
	if got.ID != "c1" || got.Type != "diff" || got.Path != "p" {
		t.Errorf("got %+v", got)
	}
	gotMany := ProtocolDiffContexts([]DiffContext{*d})
	if len(gotMany) != 1 {
		t.Errorf("expected 1 context, got %d", len(gotMany))
	}
	if got2 := ProtocolDiffContexts(nil); got2 != nil {
		t.Errorf("expected nil for nil input")
	}
}

func TestReviewStateEventFromProjection(t *testing.T) {
	projection := data.ProjectionSnapshot{
		Diffs: []DiffContext{{ContextID: "f1", PendingReview: true, Path: "p"}},
	}
	got := ReviewStateEventFromProjection("s1", projection)
	if got.SessionID != "s1" {
		t.Errorf("session id: %q", got.SessionID)
	}
	if len(got.Groups) == 0 {
		t.Errorf("expected review groups built")
	}
	if got.ActiveGroup == nil {
		t.Errorf("expected active review group")
	}
}

func TestReviewStatusFromDecision(t *testing.T) {
	cases := []struct{ in, want string }{
		{"accept", "accepted"},
		{"  ACCEPT  ", "accepted"},
		{"revert", "reverted"},
		{"revise", "revised"},
		{"yolo", "pending"},
		{"", "pending"},
	}
	for _, tc := range cases {
		if got := reviewStatusFromDecision(tc.in); got != tc.want {
			t.Errorf("(%q) -> %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSnapshotDiffMatches(t *testing.T) {
	t.Run("by context id", func(t *testing.T) {
		if !snapshotDiffMatches(DiffContext{ContextID: "c1"}, "c1", "") {
			t.Errorf("expected match by ctx id")
		}
	})
	t.Run("by path", func(t *testing.T) {
		if !snapshotDiffMatches(DiffContext{Path: "p"}, "", "p") {
			t.Errorf("expected match by path")
		}
	})
	t.Run("no match", func(t *testing.T) {
		if snapshotDiffMatches(DiffContext{ContextID: "x", Path: "y"}, "c1", "p") {
			t.Errorf("expected no match")
		}
	})
	t.Run("trim whitespace", func(t *testing.T) {
		if !snapshotDiffMatches(DiffContext{ContextID: "  c1  "}, "c1", "") {
			t.Errorf("expected match with trimmed whitespace")
		}
	})
}

func TestApplyReviewDecisionToProjection_Accept(t *testing.T) {
	snapshot := data.ProjectionSnapshot{
		Diffs: []DiffContext{{ContextID: "f1", Path: "p", PendingReview: true}},
	}
	current := DiffContext{ContextID: "f1", Path: "p"}
	got := ApplyReviewDecisionToProjection(snapshot,
		protocol.ReviewDecisionRequestEvent{ContextID: "f1"}, "accept", current)
	if got.Diffs[0].PendingReview {
		t.Errorf("accept should clear pendingReview")
	}
	if got.Diffs[0].ReviewStatus != "accepted" {
		t.Errorf("status: %q", got.Diffs[0].ReviewStatus)
	}
}

func TestApplyReviewDecisionToProjection_RevisePreservesPending(t *testing.T) {
	snapshot := data.ProjectionSnapshot{
		Diffs: []DiffContext{{ContextID: "f1", Path: "p", PendingReview: false}},
	}
	current := DiffContext{ContextID: "f1"}
	got := ApplyReviewDecisionToProjection(snapshot,
		protocol.ReviewDecisionRequestEvent{ContextID: "f1"}, "revise", current)
	if !got.Diffs[0].PendingReview {
		t.Errorf("revise should set pendingReview true")
	}
	if got.Diffs[0].ReviewStatus != "revised" {
		t.Errorf("status: %q", got.Diffs[0].ReviewStatus)
	}
}

func TestApplyReviewDecisionToProjection_FillsGroupID(t *testing.T) {
	snapshot := data.ProjectionSnapshot{
		Diffs: []DiffContext{{ContextID: "f1", Path: "p"}}, // 没有 GroupID
	}
	current := DiffContext{ContextID: "f1"}
	got := ApplyReviewDecisionToProjection(snapshot,
		protocol.ReviewDecisionRequestEvent{ContextID: "f1", GroupID: "g1"}, "revert", current)
	if got.Diffs[0].GroupID != "g1" {
		t.Errorf("expected group id filled, got %q", got.Diffs[0].GroupID)
	}
}

func TestSessionHistoryEventFromRecord(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1", Title: "T"},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{{Kind: "markdown", Message: "hi"}},
			Diffs:      []DiffContext{{ContextID: "f1", PendingReview: true}},
			ContextWindowUsage: data.ContextWindowUsage{
				TokensUsed: 1200,
				TokenLimit: 2000,
			},
			Runtime: data.SessionRuntime{
				ResumeSessionID: "r1",
				Command:         "claude",
				Engine:          "claude",
			},
		},
	}
	got := SessionHistoryEventFromRecord(record, true)
	if got.SessionID != "s1" {
		t.Errorf("session id: %q", got.SessionID)
	}
	if got.Summary.ID != "s1" {
		t.Errorf("summary id: %q", got.Summary.ID)
	}
	if len(got.LogEntries) != 1 {
		t.Errorf("expected 1 log entry, got %d", len(got.LogEntries))
	}
	if got.ContextWindowUsage.TokensUsed != 1200 ||
		got.ContextWindowUsage.TokenLimit != 2000 {
		t.Fatalf("unexpected context window usage: %+v", got.ContextWindowUsage)
	}
	if !got.CanResume {
		t.Errorf("expected CanResume=true")
	}
	if !got.RuntimeAlive {
		t.Errorf("expected RuntimeAlive=true")
	}
}

func TestSessionDeltaEventFromRecord_FullDeliveryWhenKnownEmpty(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{
				{Kind: "markdown", Message: "a"},
				{Kind: "markdown", Message: "b"},
			},
			ContextWindowUsage: data.ContextWindowUsage{
				TokensUsed: 300,
				TokenLimit: 1000,
			},
			RawTerminalByStream: map[string]string{"stdout": "stdout-content"},
			Runtime:             data.SessionRuntime{Command: "claude"},
		},
	}
	got := SessionDeltaEventFromRecord(record, protocol.SessionDeltaKnown{}, DeltaCursorSnapshot{}, false)
	if got.SessionID != "s1" {
		t.Errorf("session id: %q", got.SessionID)
	}
	if len(got.AppendLogEntries) != 2 {
		t.Errorf("expected 2 log entries, got %d", len(got.AppendLogEntries))
	}
	if got.RawTerminalByStream["stdout"] != "stdout-content" {
		t.Errorf("expected terminal content delivered, got %q", got.RawTerminalByStream["stdout"])
	}
	if got.ContextWindowUsage.TokensUsed != 300 ||
		got.ContextWindowUsage.TokenLimit != 1000 {
		t.Fatalf("unexpected context window usage: %+v", got.ContextWindowUsage)
	}
	if got.Latest.LogEntryCount != 2 {
		t.Errorf("latest log entry count: %d", got.Latest.LogEntryCount)
	}
}

func TestSessionDeltaEventFromRecord_RespectsKnownLog(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{
				{Kind: "markdown", Message: "a"},
				{Kind: "markdown", Message: "b"},
				{Kind: "markdown", Message: "c"},
			},
			Runtime: data.SessionRuntime{Command: "claude"},
		},
	}
	known := protocol.SessionDeltaKnown{LogEntryCount: 2}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if len(got.AppendLogEntries) != 1 {
		t.Errorf("expected 1 new entry, got %d", len(got.AppendLogEntries))
	}
}

func TestSessionDeltaEventFromRecord_KnownExceedsLatestResetsToZero(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{{Kind: "markdown", Message: "a"}},
			Runtime:    data.SessionRuntime{Command: "claude"},
		},
	}
	// startLog 超过 totalLog -> reset to 0 -> 全部 deliver
	known := protocol.SessionDeltaKnown{LogEntryCount: 100}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if len(got.AppendLogEntries) != 1 {
		t.Errorf("expected reset to deliver all, got %d", len(got.AppendLogEntries))
	}
}

func TestSessionDeltaEventFromRecord_RespectsKnownTerminalExecutionCount(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			TerminalExecutions: []data.TerminalExecution{
				{ExecutionID: "exec-1", Command: "first"},
				{ExecutionID: "exec-2", Command: "second"},
			},
			Runtime: data.SessionRuntime{Command: "claude"},
		},
	}
	known := protocol.SessionDeltaKnown{TerminalExecutionCount: 1}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if got.Latest.TerminalExecutionCount != 2 {
		t.Fatalf("latest terminal execution count: %d", got.Latest.TerminalExecutionCount)
	}
	if len(got.TerminalExecutions) != 1 || got.TerminalExecutions[0].ExecutionID != "exec-2" {
		t.Fatalf("expected only new terminal execution, got %+v", got.TerminalExecutions)
	}
}

func TestSessionDeltaEventFromRecord_IncludesUpdatedExecutionForAppendedLog(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			LogEntries: []data.SnapshotLogEntry{
				{Kind: "terminal", Message: "started", ExecutionID: "exec-1", Phase: "started"},
				{Kind: "terminal", Message: "done", ExecutionID: "exec-1", Phase: "finished"},
			},
			TerminalExecutions: []data.TerminalExecution{
				{ExecutionID: "exec-1", Command: "go test", FinishedAt: "2026-01-01T00:00:00Z"},
			},
			Runtime: data.SessionRuntime{Command: "claude"},
		},
	}
	known := protocol.SessionDeltaKnown{
		LogEntryCount:          1,
		TerminalExecutionCount: 1,
	}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if len(got.AppendLogEntries) != 1 || got.AppendLogEntries[0].Phase != "finished" {
		t.Fatalf("expected appended finished log, got %+v", got.AppendLogEntries)
	}
	if len(got.TerminalExecutions) != 1 || got.TerminalExecutions[0].FinishedAt == "" {
		t.Fatalf("expected updated terminal execution, got %+v", got.TerminalExecutions)
	}
}

func TestSessionDeltaEventFromRecord_IncludesRunningExecutionsWhenTerminalOutputChanges(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			RawTerminalByStream: map[string]string{"stdout": "new output"},
			TerminalExecutions: []data.TerminalExecution{
				{ExecutionID: "exec-1", Command: "first"},
				{ExecutionID: "exec-2", Command: "second"},
			},
			Runtime: data.SessionRuntime{Command: "claude"},
		},
	}
	known := protocol.SessionDeltaKnown{
		TerminalExecutionCount: 2,
		TerminalStdoutLength:   3,
	}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if got.RequiresFullSync {
		t.Fatalf("expected incremental terminal output delta, got full sync")
	}
	if len(got.TerminalExecutions) != 2 {
		t.Fatalf("expected running executions to be refreshed, got %+v", got.TerminalExecutions)
	}
	if got.TerminalExecutions[0].ExecutionID != "exec-1" || got.TerminalExecutions[1].ExecutionID != "exec-2" {
		t.Fatalf("unexpected execution order: %+v", got.TerminalExecutions)
	}
}

func TestSessionDeltaEventFromRecord_KnownTerminalExecutionExceedsLatestRequiresFullSync(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			TerminalExecutions: []data.TerminalExecution{
				{ExecutionID: "exec-1", Command: "first"},
			},
			Runtime: data.SessionRuntime{Command: "claude"},
		},
	}
	known := protocol.SessionDeltaKnown{TerminalExecutionCount: 100}
	got := SessionDeltaEventFromRecord(record, known, DeltaCursorSnapshot{}, false)
	if !got.RequiresFullSync {
		t.Fatalf("expected full sync when known terminal execution count is invalid")
	}
	if len(got.TerminalExecutions) != 1 {
		t.Fatalf("expected reset to deliver all executions, got %+v", got.TerminalExecutions)
	}
}

func TestRestoredAgentStateEventFromRecord_Idle_NoResume(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			Controller: data.ControllerSnapshot{State: ControllerStateIdle},
			Runtime:    data.SessionRuntime{},
		},
	}
	got := RestoredAgentStateEventFromRecord(record, false, false)
	// Idle + no resume + no lifecycle -> nil
	if got != nil {
		t.Errorf("expected nil, got %+v", got)
	}
}

func TestRestoredAgentStateEventFromRecord_IdleWithResume(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			Controller: data.ControllerSnapshot{State: ControllerStateIdle},
			Runtime:    data.SessionRuntime{ResumeSessionID: "r1"},
		},
	}
	got := RestoredAgentStateEventFromRecord(record, false, false)
	if got == nil {
		t.Fatal("expected event")
	}
	if got.State != string(ControllerStateIdle) {
		t.Errorf("state: %q", got.State)
	}
	// 注入 lifecycle
	if got.RuntimeMeta.ClaudeLifecycle != "waiting_input" {
		t.Errorf("expected lifecycle filled to waiting_input, got %q", got.RuntimeMeta.ClaudeLifecycle)
	}
}

func TestRestoredAgentStateEventFromRecord_DowngradeStaleRunning(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			Controller: data.ControllerSnapshot{State: ControllerStateThinking},
			Runtime:    data.SessionRuntime{}, // 没有 resume -> downgrade to Idle (return nil because Idle without resume)
		},
	}
	got := RestoredAgentStateEventFromRecord(record, false, false)
	if got != nil {
		t.Errorf("expected nil after downgrade with no resume, got %+v", got)
	}
}

func TestRestoredAgentStateEventFromRecord_ThinkingWithResumeBecomesWaitInput(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			Controller: data.ControllerSnapshot{State: ControllerStateThinking, LastStep: "step"},
			Runtime:    data.SessionRuntime{ResumeSessionID: "r1"},
		},
	}
	got := RestoredAgentStateEventFromRecord(record, false, false)
	if got == nil {
		t.Fatal("expected event")
	}
	if got.State != string(ControllerStateWaitInput) {
		t.Errorf("expected WAIT_INPUT downgrade, got %q", got.State)
	}
	if !got.AwaitInput {
		t.Errorf("expected awaitInput true")
	}
}

func TestRestoredAgentStateEventFromRecord_HasActiveRunnerKeepsThinking(t *testing.T) {
	record := data.SessionRecord{
		Summary: data.SessionSummary{ID: "s1"},
		Projection: data.ProjectionSnapshot{
			Controller: data.ControllerSnapshot{State: ControllerStateThinking, LastStep: "step"},
		},
	}
	got := RestoredAgentStateEventFromRecord(record, true, false)
	if got == nil {
		t.Fatal("expected event")
	}
	if got.State != string(ControllerStateThinking) {
		t.Errorf("expected THINKING preserved when runner active, got %q", got.State)
	}
}

func TestRestoredAgentStateEventFromRecord_DerivesStateFromLifecycle(t *testing.T) {
	cases := []struct {
		lifecycle string
		resume    string
		want      ControllerState
	}{
		// "starting" + resume id 会被 NormalizeProjectionLifecycle 规整成 "resumable"
		{"waiting_input", "r1", ControllerStateWaitInput},
		{"active", "r1", ControllerStateThinking},
		{"resumable", "r1", ControllerStateIdle},
		{"starting", "", ControllerStateThinking}, // 无 resume 才保持 starting
	}
	for _, tc := range cases {
		t.Run(tc.lifecycle, func(t *testing.T) {
			record := data.SessionRecord{
				Summary: data.SessionSummary{ID: "s1"},
				Projection: data.ProjectionSnapshot{
					Runtime: data.SessionRuntime{ClaudeLifecycle: tc.lifecycle, ResumeSessionID: tc.resume},
				},
			}
			got := RestoredAgentStateEventFromRecord(record, true, false)
			if got == nil {
				t.Fatal("expected event")
			}
			if got.State != string(tc.want) {
				t.Errorf("expected %q, got %q", tc.want, got.State)
			}
		})
	}
}
