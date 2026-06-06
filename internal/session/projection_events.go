package session

import (
	"encoding/json"
	"strings"
	"time"

	"mobilevc/internal/data"
	"mobilevc/internal/protocol"
)

type DeltaCursorSnapshot struct {
	LatestCursor int64
}

func ToProtocolSummary(item data.SessionSummary) protocol.SessionSummary {
	return protocol.SessionSummary{
		ID:              item.ID,
		Title:           item.Title,
		CreatedAt:       item.CreatedAt.Format(time.RFC3339),
		UpdatedAt:       item.UpdatedAt.Format(time.RFC3339),
		LastPreview:     item.LastPreview,
		EntryCount:      item.EntryCount,
		Source:          item.Source,
		External:        item.External,
		Ownership:       item.Ownership,
		ExecutionActive: item.ExecutionActive,
		Runtime: protocol.RuntimeMeta{
			ResumeSessionID:  item.Runtime.ResumeSessionID,
			Command:          item.Runtime.Command,
			Engine:           item.Runtime.Engine,
			CodexSandboxMode: item.Runtime.CodexSandboxMode,
			CWD:              item.Runtime.CWD,
			PermissionMode:   item.Runtime.PermissionMode,
			ClaudeLifecycle:  item.Runtime.ClaudeLifecycle,
			Source:           item.Runtime.Source,
		},
	}
}

func ToProtocolSessionContext(ctx data.SessionContext) protocol.SessionContext {
	return protocol.SessionContext{
		EnabledSkillNames: append([]string(nil), ctx.EnabledSkillNames...),
		EnabledMemoryIDs:  append([]string(nil), ctx.EnabledMemoryIDs...),
	}
}

func ToProtocolContextWindowUsage(usage data.ContextWindowUsage) protocol.ContextWindowUsage {
	return protocol.NormalizeContextWindowUsage(protocol.ContextWindowUsage{
		TokensUsed: usage.TokensUsed,
		TokenLimit: usage.TokenLimit,
	})
}

func ToProtocolCatalogMetadata(meta data.CatalogMetadata) protocol.CatalogMetadata {
	lastSyncedAt := ""
	if !meta.LastSyncedAt.IsZero() {
		lastSyncedAt = meta.LastSyncedAt.Format(time.RFC3339)
	}
	return protocol.CatalogMetadata{
		Domain:        string(meta.Domain),
		SourceOfTruth: string(meta.SourceOfTruth),
		SyncState:     string(meta.SyncState),
		DriftDetected: meta.DriftDetected,
		LastSyncedAt:  lastSyncedAt,
		VersionToken:  meta.VersionToken,
		LastError:     meta.LastError,
	}
}

func HistoryContextFromSnapshot(ctx *data.SnapshotContext) *protocol.HistoryContext {
	if ctx == nil {
		return nil
	}
	return &protocol.HistoryContext{
		ID:            ctx.ID,
		Type:          ctx.Type,
		Message:       ctx.Message,
		Status:        ctx.Status,
		Trigger:       ctx.Trigger,
		Target:        ctx.Target,
		TargetPath:    ctx.TargetPath,
		Tool:          ctx.Tool,
		Command:       ctx.Command,
		Timestamp:     ctx.Timestamp,
		Title:         ctx.Title,
		Stack:         ctx.Stack,
		Code:          ctx.Code,
		RelatedStep:   ctx.RelatedStep,
		Path:          ctx.Path,
		Diff:          ctx.Diff,
		Lang:          ctx.Lang,
		PendingReview: ctx.PendingReview,
		Source:        ctx.Source,
		SkillName:     ctx.SkillName,
		ExecutionID:   ctx.ExecutionID,
		GroupID:       ctx.GroupID,
		GroupTitle:    ctx.GroupTitle,
		ReviewStatus:  ctx.ReviewStatus,
	}
}

func ProtocolReviewFile(file ReviewFile) protocol.ReviewFile {
	return protocol.ReviewFile{
		ID:            file.ContextID,
		Path:          file.Path,
		Title:         file.Title,
		Diff:          file.Diff,
		Lang:          file.Lang,
		PendingReview: file.PendingReview,
		ReviewStatus:  file.ReviewStatus,
		ExecutionID:   file.ExecutionID,
	}
}

func ProtocolReviewGroup(group *ReviewGroup) *protocol.ReviewGroup {
	if group == nil {
		return nil
	}
	files := make([]protocol.ReviewFile, 0, len(group.Files))
	for _, file := range group.Files {
		files = append(files, ProtocolReviewFile(file))
	}
	return &protocol.ReviewGroup{
		ID:            group.ID,
		Title:         group.Title,
		ExecutionID:   group.ExecutionID,
		PendingReview: group.PendingReview,
		ReviewStatus:  group.ReviewStatus,
		CurrentFileID: group.CurrentFileID,
		CurrentPath:   group.CurrentPath,
		PendingCount:  group.PendingCount,
		AcceptedCount: group.AcceptedCount,
		RevertedCount: group.RevertedCount,
		RevisedCount:  group.RevisedCount,
		Files:         files,
	}
}

func ProtocolReviewGroups(groups []ReviewGroup) []protocol.ReviewGroup {
	if len(groups) == 0 {
		return nil
	}
	result := make([]protocol.ReviewGroup, 0, len(groups))
	for _, group := range groups {
		item := ProtocolReviewGroup(&group)
		if item != nil {
			result = append(result, *item)
		}
	}
	return result
}

func ProtocolDiffContext(diff *DiffContext) *protocol.HistoryContext {
	if diff == nil {
		return nil
	}
	return &protocol.HistoryContext{
		ID:            diff.ContextID,
		Type:          "diff",
		Path:          diff.Path,
		Title:         diff.Title,
		Diff:          diff.Diff,
		Lang:          diff.Lang,
		PendingReview: diff.PendingReview,
		ExecutionID:   diff.ExecutionID,
		GroupID:       diff.GroupID,
		GroupTitle:    diff.GroupTitle,
		ReviewStatus:  diff.ReviewStatus,
	}
}

func ProtocolDiffContexts(diffs []DiffContext) []protocol.HistoryContext {
	if len(diffs) == 0 {
		return nil
	}
	result := make([]protocol.HistoryContext, 0, len(diffs))
	for _, diff := range diffs {
		ctx := ProtocolDiffContext(&diff)
		if ctx != nil {
			result = append(result, *ctx)
		}
	}
	return result
}

func ReviewStateEventFromProjection(sessionID string, projection data.ProjectionSnapshot) protocol.ReviewStateEvent {
	projection = NormalizeProjectionSnapshot(projection)
	return protocol.NewReviewStateEvent(
		sessionID,
		ProtocolReviewGroups(projection.ReviewGroups),
		ProtocolReviewGroup(projection.ActiveReviewGroup),
	)
}

func ApplyReviewDecisionToProjection(snapshot data.ProjectionSnapshot, reviewEvent protocol.ReviewDecisionRequestEvent, decision string, currentDiff DiffContext) data.ProjectionSnapshot {
	snapshot = NormalizeProjectionSnapshot(snapshot)
	targetContextID := firstNonEmptyString(reviewEvent.ContextID, currentDiff.ContextID)
	targetPath := firstNonEmptyString(reviewEvent.TargetPath, currentDiff.Path)
	targetExecutionID := firstNonEmptyString(reviewEvent.ExecutionID, currentDiff.ExecutionID)
	targetGroupID := firstNonEmptyString(reviewEvent.GroupID, reviewEvent.ExecutionID, currentDiff.GroupID, targetContextID, targetPath)
	targetGroupTitle := firstNonEmptyString(reviewEvent.GroupTitle, currentDiff.GroupTitle, currentDiff.Title)
	reviewStatus := reviewStatusFromDecision(decision)
	pending := decision == "revise"

	for i := range snapshot.Diffs {
		item := &snapshot.Diffs[i]
		if !snapshotDiffMatches(*item, targetContextID, targetPath) {
			continue
		}
		item.PendingReview = pending
		item.ReviewStatus = reviewStatus
		if item.GroupID == "" {
			item.GroupID = targetGroupID
		}
		if item.GroupTitle == "" {
			item.GroupTitle = targetGroupTitle
		}
		if item.ExecutionID == "" {
			item.ExecutionID = targetExecutionID
		}
	}

	snapshot.ReviewGroups = RebuildReviewGroups(snapshot.Diffs)
	if active := PickActiveReviewGroup(snapshot.ReviewGroups); active != nil {
		snapshot.ActiveReviewGroup = active
	}
	activeDiff := PickActiveSnapshotDiff(snapshot.Diffs)
	if strings.TrimSpace(activeDiff.ContextID+activeDiff.Path+activeDiff.Title) != "" {
		snapshot.CurrentDiff = &activeDiff
	}
	return snapshot
}

func ApplyAutoReviewAcceptanceToProjection(snapshot data.ProjectionSnapshot) data.ProjectionSnapshot {
	snapshot = NormalizeProjectionSnapshot(snapshot)
	for i := range snapshot.Diffs {
		if !snapshot.Diffs[i].PendingReview {
			continue
		}
		snapshot.Diffs[i].PendingReview = false
		snapshot.Diffs[i].ReviewStatus = "accepted"
	}
	snapshot.ReviewGroups = RebuildReviewGroups(snapshot.Diffs)
	if active := PickActiveReviewGroup(snapshot.ReviewGroups); active != nil {
		snapshot.ActiveReviewGroup = active
	} else {
		snapshot.ActiveReviewGroup = nil
	}
	activeDiff := PickActiveSnapshotDiff(snapshot.Diffs)
	if strings.TrimSpace(activeDiff.ContextID+activeDiff.Path+activeDiff.Title) != "" {
		snapshot.CurrentDiff = &activeDiff
	} else {
		snapshot.CurrentDiff = nil
	}
	return snapshot
}

func reviewStatusFromDecision(decision string) string {
	switch strings.TrimSpace(strings.ToLower(decision)) {
	case "accept":
		return "accepted"
	case "revert":
		return "reverted"
	case "revise":
		return "revised"
	default:
		return "pending"
	}
}

func snapshotDiffMatches(item DiffContext, contextID, targetPath string) bool {
	if strings.TrimSpace(contextID) != "" && strings.TrimSpace(item.ContextID) == strings.TrimSpace(contextID) {
		return true
	}
	if strings.TrimSpace(targetPath) != "" && strings.TrimSpace(item.Path) == strings.TrimSpace(targetPath) {
		return true
	}
	return false
}

func SessionHistoryEventFromRecord(record data.SessionRecord, runtimeAlive bool) protocol.SessionHistoryEvent {
	return SessionHistoryWindowEventFromRecord(record, runtimeAlive, 0)
}

func SessionHistoryWindowEventFromRecord(record data.SessionRecord, runtimeAlive bool, limit int) protocol.SessionHistoryEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	window := snapshotLogEntryWindow(projection.LogEntries, len(projection.LogEntries), limit)
	return sessionHistoryWindowEventFromProjection(record, projection, window, runtimeAlive, DeltaCursorSnapshot{})
}

func SessionHistoryWindowEventFromRecordWithPayloadLimit(record data.SessionRecord, runtimeAlive bool, limit int, maxPayloadBytes int) protocol.SessionHistoryEvent {
	return SessionHistoryWindowEventFromRecordWithCursorAndPayloadLimit(record, runtimeAlive, limit, DeltaCursorSnapshot{}, maxPayloadBytes)
}

func SessionHistoryWindowEventFromRecordWithCursorAndPayloadLimit(record data.SessionRecord, runtimeAlive bool, limit int, cursor DeltaCursorSnapshot, maxPayloadBytes int) protocol.SessionHistoryEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	window := snapshotLogEntryWindow(projection.LogEntries, len(projection.LogEntries), limit)
	event := sessionHistoryWindowEventFromProjection(record, projection, window, runtimeAlive, cursor)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	return shrinkSessionHistoryEventToPayloadBudget(record, projection, window, runtimeAlive, cursor, maxPayloadBytes)
}

func sessionHistoryWindowEventFromProjection(record data.SessionRecord, projection data.ProjectionSnapshot, window snapshotLogWindow, runtimeAlive bool, cursor DeltaCursorSnapshot) protocol.SessionHistoryEvent {
	executions := protocolTerminalExecutions(projection.TerminalExecutions)
	resumeMeta := protocol.RuntimeMeta{
		ResumeSessionID:  projection.Runtime.ResumeSessionID,
		Command:          projection.Runtime.Command,
		Engine:           projection.Runtime.Engine,
		CodexSandboxMode: projection.Runtime.CodexSandboxMode,
		CWD:              projection.Runtime.CWD,
		PermissionMode:   projection.Runtime.PermissionMode,
		ClaudeLifecycle:  NormalizeProjectionLifecycle(projection.Runtime.ClaudeLifecycle, projection.Runtime.ResumeSessionID),
	}
	event := protocol.NewSessionHistoryWindowEvent(
		record.Summary.ID,
		ToProtocolSummary(record.Summary),
		protocolLogEntries(window.entries),
		window.start,
		len(projection.LogEntries),
		ProtocolDiffContexts(projection.Diffs),
		ProtocolDiffContext(projection.CurrentDiff),
		ProtocolReviewGroups(projection.ReviewGroups),
		ProtocolReviewGroup(projection.ActiveReviewGroup),
		HistoryContextFromSnapshot(projection.CurrentStep),
		HistoryContextFromSnapshot(projection.LatestError),
		projection.RawTerminalByStream,
		executions,
		ToProtocolContextWindowUsage(projection.ContextWindowUsage),
		ToProtocolSessionContext(projection.SessionContext),
		ToProtocolCatalogMetadata(projection.SkillCatalogMeta),
		ToProtocolCatalogMetadata(projection.MemoryCatalogMeta),
		strings.TrimSpace(resumeMeta.ResumeSessionID) != "",
		runtimeAlive,
		resumeMeta,
	)
	event.Latest = sessionDeltaKnownFromProjection(projection, cursor)
	return event
}

func SessionHistoryPageEventFromRecord(record data.SessionRecord, before, limit int) protocol.SessionHistoryPageEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	window := snapshotLogEntryWindow(projection.LogEntries, before, limit)
	return sessionHistoryPageEventFromProjection(record, projection, window)
}

func SessionHistoryPageEventFromRecordWithPayloadLimit(record data.SessionRecord, before, limit int, maxPayloadBytes int) protocol.SessionHistoryPageEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	window := snapshotLogEntryWindow(projection.LogEntries, before, limit)
	event := sessionHistoryPageEventFromProjection(record, projection, window)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	return shrinkSessionHistoryPageEventToPayloadBudget(record, projection, window, maxPayloadBytes)
}

func sessionHistoryPageEventFromProjection(record data.SessionRecord, projection data.ProjectionSnapshot, window snapshotLogWindow) protocol.SessionHistoryPageEvent {
	resumeMeta := protocol.RuntimeMeta{
		ResumeSessionID:  projection.Runtime.ResumeSessionID,
		Command:          projection.Runtime.Command,
		Engine:           projection.Runtime.Engine,
		CodexSandboxMode: projection.Runtime.CodexSandboxMode,
		CWD:              projection.Runtime.CWD,
		PermissionMode:   projection.Runtime.PermissionMode,
		ClaudeLifecycle:  NormalizeProjectionLifecycle(projection.Runtime.ClaudeLifecycle, projection.Runtime.ResumeSessionID),
	}
	return protocol.NewSessionHistoryPageEvent(
		record.Summary.ID,
		protocolLogEntries(window.entries),
		window.start,
		len(projection.LogEntries),
		resumeMeta,
	)
}

func shrinkSessionHistoryEventToPayloadBudget(record data.SessionRecord, projection data.ProjectionSnapshot, window snapshotLogWindow, runtimeAlive bool, cursor DeltaCursorSnapshot, maxPayloadBytes int) protocol.SessionHistoryEvent {
	event := sessionHistoryWindowEventFromProjection(record, projection, window, runtimeAlive, cursor)
	markSessionHistoryPayloadLimited(&event, "payload_budget_exceeded")
	event = shrinkSessionHistoryEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.RawTerminalByStream = nil
	event.TerminalExecutions = nil
	event = shrinkSessionHistoryEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.Diffs = nil
	event.CurrentDiff = nil
	event.ReviewGroups = nil
	event.ActiveReviewGroup = nil
	event = shrinkSessionHistoryEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.CurrentStep = nil
	event.LatestError = nil
	event = shrinkSessionHistoryEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.ContextWindowUsage = protocol.ContextWindowUsage{}
	event.SessionContext = protocol.SessionContext{}
	event.SkillCatalogMeta = protocol.CatalogMetadata{}
	event.MemoryCatalogMeta = protocol.CatalogMetadata{}
	event = shrinkSessionHistoryEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.LogEntries = nil
	event.LogEntryStart = window.start + len(window.entries)
	event.HasMoreBefore = event.LogEntryStart > 0
	event.RawTerminalByStream = nil
	event.TerminalExecutions = nil
	event.Diffs = nil
	event.CurrentDiff = nil
	event.ReviewGroups = nil
	event.ActiveReviewGroup = nil
	event.CurrentStep = nil
	event.LatestError = nil
	event.ContextWindowUsage = protocol.ContextWindowUsage{}
	event.SessionContext = protocol.SessionContext{}
	event.SkillCatalogMeta = protocol.CatalogMetadata{}
	event.MemoryCatalogMeta = protocol.CatalogMetadata{}
	return event
}

func shrinkSessionHistoryPageEventToPayloadBudget(record data.SessionRecord, projection data.ProjectionSnapshot, window snapshotLogWindow, maxPayloadBytes int) protocol.SessionHistoryPageEvent {
	event := sessionHistoryPageEventFromProjection(record, projection, window)
	markSessionHistoryPagePayloadLimited(&event, "payload_budget_exceeded")
	event = shrinkSessionHistoryPageEntriesToPayloadBudget(event, window, maxPayloadBytes)
	if fitsProtocolPayloadBudget(event, maxPayloadBytes) {
		return event
	}
	event.ResumeRuntimeMeta = protocol.RuntimeMeta{}
	return shrinkSessionHistoryPageEntriesToPayloadBudget(event, window, maxPayloadBytes)
}

func shrinkSessionHistoryEntriesToPayloadBudget(event protocol.SessionHistoryEvent, window snapshotLogWindow, maxPayloadBytes int) protocol.SessionHistoryEvent {
	entries, start := shrinkSnapshotWindowEntries(window, func(candidate snapshotLogWindow) bool {
		candidateEvent := event
		candidateEvent.LogEntries = protocolLogEntries(candidate.entries)
		candidateEvent.LogEntryStart = candidate.start
		candidateEvent.HasMoreBefore = candidate.start > 0
		return fitsProtocolPayloadBudget(candidateEvent, maxPayloadBytes)
	})
	event.LogEntries = protocolLogEntries(entries)
	event.LogEntryStart = start
	event.HasMoreBefore = start > 0
	return event
}

func shrinkSessionHistoryPageEntriesToPayloadBudget(event protocol.SessionHistoryPageEvent, window snapshotLogWindow, maxPayloadBytes int) protocol.SessionHistoryPageEvent {
	entries, start := shrinkSnapshotWindowEntries(window, func(candidate snapshotLogWindow) bool {
		candidateEvent := event
		candidateEvent.LogEntries = protocolLogEntries(candidate.entries)
		candidateEvent.LogEntryStart = candidate.start
		candidateEvent.HasMoreBefore = candidate.start > 0
		return fitsProtocolPayloadBudget(candidateEvent, maxPayloadBytes)
	})
	event.LogEntries = protocolLogEntries(entries)
	event.LogEntryStart = start
	event.HasMoreBefore = start > 0
	return event
}

func shrinkSnapshotWindowEntries(window snapshotLogWindow, fits func(snapshotLogWindow) bool) ([]data.SnapshotLogEntry, int) {
	if len(window.entries) == 0 {
		return nil, window.start
	}
	before := window.start + len(window.entries)
	low, high := 0, len(window.entries)
	best := 0
	for low <= high {
		count := (low + high) / 2
		start := before - count
		candidate := snapshotLogWindow{
			entries: append([]data.SnapshotLogEntry(nil), window.entries[len(window.entries)-count:]...),
			start:   start,
		}
		if fits(candidate) {
			best = count
			low = count + 1
			continue
		}
		high = count - 1
	}
	start := before - best
	if best == 0 {
		return nil, start
	}
	return append([]data.SnapshotLogEntry(nil), window.entries[len(window.entries)-best:]...), start
}

func fitsProtocolPayloadBudget(event any, maxPayloadBytes int) bool {
	if maxPayloadBytes <= 0 {
		return true
	}
	encoded, err := json.Marshal(event)
	return err == nil && len(encoded) <= maxPayloadBytes
}

func markSessionHistoryPayloadLimited(event *protocol.SessionHistoryEvent, reason string) {
	event.PayloadLimited = true
	event.PayloadLimitReason = strings.TrimSpace(reason)
}

func markSessionHistoryPagePayloadLimited(event *protocol.SessionHistoryPageEvent, reason string) {
	event.PayloadLimited = true
	event.PayloadLimitReason = strings.TrimSpace(reason)
}

type snapshotLogWindow struct {
	entries []data.SnapshotLogEntry
	start   int
}

func snapshotLogEntryWindow(entries []data.SnapshotLogEntry, before, limit int) snapshotLogWindow {
	total := len(entries)
	if before <= 0 || before > total {
		before = total
	}
	if limit <= 0 || limit >= before {
		return snapshotLogWindow{
			entries: append([]data.SnapshotLogEntry(nil), entries[:before]...),
			start:   0,
		}
	}
	start := before - limit
	return snapshotLogWindow{
		entries: append([]data.SnapshotLogEntry(nil), entries[start:before]...),
		start:   start,
	}
}

func SessionDeltaEventFromRecord(record data.SessionRecord, known protocol.SessionDeltaKnown, cursor DeltaCursorSnapshot, runtimeAlive bool) protocol.SessionDeltaEvent {
	return SessionDeltaEventFromRecordWithPayloadLimit(record, known, cursor, runtimeAlive, 0, 0)
}

func SessionDeltaEventFromRecordWithPayloadLimit(record data.SessionRecord, known protocol.SessionDeltaKnown, cursor DeltaCursorSnapshot, runtimeAlive bool, maxPayloadBytes int, maxAppendLogEntries int) protocol.SessionDeltaEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	allEntries := protocolLogEntries(projection.LogEntries)
	latestCursor := cursor.LatestCursor
	latest := sessionDeltaKnownFromProjection(projection, cursor)
	resumeMeta := protocol.RuntimeMeta{
		ResumeSessionID:  projection.Runtime.ResumeSessionID,
		Command:          projection.Runtime.Command,
		Engine:           projection.Runtime.Engine,
		CodexSandboxMode: projection.Runtime.CodexSandboxMode,
		CWD:              projection.Runtime.CWD,
		PermissionMode:   projection.Runtime.PermissionMode,
		ClaudeLifecycle:  NormalizeProjectionLifecycle(projection.Runtime.ClaudeLifecycle, projection.Runtime.ResumeSessionID),
	}
	if deltaKnownEmpty(known) && maxAppendLogEntries > 0 && len(allEntries) > maxAppendLogEntries {
		return sessionDeltaFullSyncEvent(record, known, latest, resumeMeta, runtimeAlive, "payload_log_entry_limit_exceeded")
	}
	startLog := known.LogEntryCount
	logReset := false
	if latestCursor > 0 && known.EventCursor >= latestCursor {
		startLog = len(allEntries)
	}
	if startLog < 0 || startLog > len(allEntries) {
		startLog = 0
		logReset = known.LogEntryCount != 0
	}
	appendEntries := append([]protocol.HistoryLogEntry(nil), allEntries[startLog:]...)

	allDiffs := ProtocolDiffContexts(projection.Diffs)
	startDiff := known.DiffCount
	diffReset := false
	if latestCursor > 0 && known.EventCursor >= latestCursor {
		startDiff = len(allDiffs)
	}
	if startDiff < 0 || startDiff > len(allDiffs) {
		startDiff = 0
		diffReset = known.DiffCount != 0
	}
	upsertDiffs := append([]protocol.HistoryContext(nil), allDiffs[startDiff:]...)
	if startDiff == len(allDiffs) && (projection.CurrentDiff != nil || len(projection.ReviewGroups) > 0 || projection.ActiveReviewGroup != nil) {
		upsertDiffs = append([]protocol.HistoryContext(nil), allDiffs...)
	}

	rawTerminalByStream := make(map[string]string)
	stdout := projection.RawTerminalByStream["stdout"]
	stderr := projection.RawTerminalByStream["stderr"]
	stdoutStart := known.TerminalStdoutLength
	stdoutReset := false
	if latestCursor > 0 && known.EventCursor >= latestCursor {
		stdoutStart = len(stdout)
	}
	if stdoutStart < 0 || stdoutStart > len(stdout) {
		stdoutStart = 0
		stdoutReset = known.TerminalStdoutLength != 0
	}
	stderrStart := known.TerminalStderrLength
	stderrReset := false
	if latestCursor > 0 && known.EventCursor >= latestCursor {
		stderrStart = len(stderr)
	}
	if stderrStart < 0 || stderrStart > len(stderr) {
		stderrStart = 0
		stderrReset = known.TerminalStderrLength != 0
	}
	if stdoutStart < len(stdout) {
		rawTerminalByStream["stdout"] = stdout[stdoutStart:]
	}
	if stderrStart < len(stderr) {
		rawTerminalByStream["stderr"] = stderr[stderrStart:]
	}

	allExecutions := protocolTerminalExecutions(projection.TerminalExecutions)
	executions, terminalExecutionReset := terminalExecutionsForDelta(terminalExecutionDeltaInput{
		all:                   allExecutions,
		knownCount:            known.TerminalExecutionCount,
		cursorCaughtUp:        latestCursor > 0 && known.EventCursor >= latestCursor,
		entries:               appendEntries,
		terminalOutputChanged: stdoutStart < len(stdout) || stderrStart < len(stderr),
	})
	latest.DiffCount = len(allDiffs)
	latest.TerminalExecutionCount = len(allExecutions)
	latest.TerminalStdoutLength = len(stdout)
	latest.TerminalStderrLength = len(stderr)
	requiresFullSync := logReset || diffReset || stdoutReset || stderrReset || terminalExecutionReset
	event := protocol.NewSessionDeltaEvent(
		record.Summary.ID,
		ToProtocolSummary(record.Summary),
		known,
		latest,
		appendEntries,
		upsertDiffs,
		ProtocolDiffContext(projection.CurrentDiff),
		ProtocolReviewGroups(projection.ReviewGroups),
		ProtocolReviewGroup(projection.ActiveReviewGroup),
		HistoryContextFromSnapshot(projection.CurrentStep),
		HistoryContextFromSnapshot(projection.LatestError),
		rawTerminalByStream,
		executions,
		ToProtocolContextWindowUsage(projection.ContextWindowUsage),
		ToProtocolSessionContext(projection.SessionContext),
		ToProtocolCatalogMetadata(projection.SkillCatalogMeta),
		ToProtocolCatalogMetadata(projection.MemoryCatalogMeta),
		strings.TrimSpace(resumeMeta.ResumeSessionID) != "",
		runtimeAlive,
		resumeMeta,
		requiresFullSync,
	)
	if requiresFullSync {
		return sessionDeltaFullSyncEvent(record, known, latest, resumeMeta, runtimeAlive, "")
	}
	if deltaPayloadExceedsBudget(event, maxPayloadBytes, maxAppendLogEntries) {
		return sessionDeltaFullSyncEvent(record, known, latest, resumeMeta, runtimeAlive, "payload_budget_exceeded")
	}
	return event
}

func sessionDeltaKnownFromProjection(projection data.ProjectionSnapshot, cursor DeltaCursorSnapshot) protocol.SessionDeltaKnown {
	return protocol.SessionDeltaKnown{
		EventCursor:            cursor.LatestCursor,
		LogEntryCount:          len(protocolLogEntries(projection.LogEntries)),
		DiffCount:              len(ProtocolDiffContexts(projection.Diffs)),
		TerminalExecutionCount: len(protocolTerminalExecutions(projection.TerminalExecutions)),
		TerminalStdoutLength:   len(projection.RawTerminalByStream["stdout"]),
		TerminalStderrLength:   len(projection.RawTerminalByStream["stderr"]),
	}
}

func deltaKnownEmpty(known protocol.SessionDeltaKnown) bool {
	return known.EventCursor == 0 &&
		known.LogEntryCount == 0 &&
		known.DiffCount == 0 &&
		known.TerminalExecutionCount == 0 &&
		known.TerminalStdoutLength == 0 &&
		known.TerminalStderrLength == 0
}

func sessionDeltaFullSyncEvent(record data.SessionRecord, known, latest protocol.SessionDeltaKnown, resumeMeta protocol.RuntimeMeta, runtimeAlive bool, payloadLimitReason string) protocol.SessionDeltaEvent {
	event := protocol.NewSessionDeltaEvent(
		record.Summary.ID,
		ToProtocolSummary(record.Summary),
		known,
		latest,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		protocol.ContextWindowUsage{},
		protocol.SessionContext{},
		protocol.CatalogMetadata{},
		protocol.CatalogMetadata{},
		strings.TrimSpace(resumeMeta.ResumeSessionID) != "",
		runtimeAlive,
		resumeMeta,
		true,
	)
	if strings.TrimSpace(payloadLimitReason) != "" {
		event.PayloadLimited = true
		event.PayloadLimitReason = strings.TrimSpace(payloadLimitReason)
	}
	return event
}

func deltaPayloadExceedsBudget(event protocol.SessionDeltaEvent, maxPayloadBytes int, maxAppendLogEntries int) bool {
	if maxAppendLogEntries > 0 && len(event.AppendLogEntries) > maxAppendLogEntries {
		return true
	}
	if maxPayloadBytes <= 0 {
		return false
	}
	return !fitsProtocolPayloadBudget(event, maxPayloadBytes)
}

type terminalExecutionDeltaInput struct {
	all                   []protocol.TerminalExecution
	knownCount            int
	cursorCaughtUp        bool
	entries               []protocol.HistoryLogEntry
	terminalOutputChanged bool
}

func terminalExecutionsForDelta(input terminalExecutionDeltaInput) ([]protocol.TerminalExecution, bool) {
	all := input.all
	start := input.knownCount
	if input.cursorCaughtUp {
		start = len(all)
	}
	reset := false
	if start < 0 || start > len(all) {
		start = 0
		reset = input.knownCount != 0
	}

	indexByID := make(map[string]int, len(all))
	for i, item := range all {
		if id := strings.TrimSpace(item.ExecutionID); id != "" {
			indexByID[id] = i
		}
	}

	out := make([]protocol.TerminalExecution, 0, len(all)-start+1)
	included := make(map[string]struct{}, len(all)-start+1)
	addAt := func(index int) {
		if index < 0 || index >= len(all) {
			return
		}
		item := all[index]
		id := strings.TrimSpace(item.ExecutionID)
		if id != "" {
			if _, ok := included[id]; ok {
				return
			}
			included[id] = struct{}{}
		}
		out = append(out, item)
	}

	for i := start; i < len(all); i++ {
		addAt(i)
	}
	for _, entry := range input.entries {
		if index, ok := indexByID[strings.TrimSpace(entry.ExecutionID)]; ok {
			addAt(index)
		}
	}
	if input.terminalOutputChanged {
		for i, item := range all {
			if terminalExecutionStillRunning(item) {
				addAt(i)
			}
		}
	}
	return out, reset
}

func terminalExecutionStillRunning(item protocol.TerminalExecution) bool {
	return strings.TrimSpace(item.ExecutionID) != "" &&
		strings.TrimSpace(item.FinishedAt) == "" &&
		item.ExitCode == nil
}

func RestoredAgentStateEventFromRecord(record data.SessionRecord, hasActiveRunner bool, externalNativeActive bool) *protocol.AgentStateEvent {
	projection := NormalizeProjectionSnapshot(record.Projection)
	runtimeMeta := protocol.MergeRuntimeMeta(projection.Controller.ActiveMeta, protocol.RuntimeMeta{
		ResumeSessionID: firstNonEmptyString(
			projection.Controller.ResumeSession,
			projection.Controller.ActiveMeta.ResumeSessionID,
			projection.Runtime.ResumeSessionID,
		),
		Command: firstNonEmptyString(
			projection.Controller.CurrentCommand,
			projection.Controller.ActiveMeta.Command,
			projection.Runtime.Command,
		),
		Engine: firstNonEmptyString(
			projection.Controller.ActiveMeta.Engine,
			projection.Runtime.Engine,
		),
		CodexSandboxMode: firstNonEmptyString(
			projection.Controller.ActiveMeta.CodexSandboxMode,
			projection.Runtime.CodexSandboxMode,
		),
		CWD: firstNonEmptyString(
			projection.Controller.ActiveMeta.CWD,
			projection.Runtime.CWD,
		),
		PermissionMode: firstNonEmptyString(
			projection.Controller.ActiveMeta.PermissionMode,
			projection.Runtime.PermissionMode,
			"auto",
		),
		ClaudeLifecycle: NormalizeProjectionLifecycle(
			firstNonEmptyString(
				projection.Controller.ClaudeLifecycle,
				projection.Controller.ActiveMeta.ClaudeLifecycle,
				projection.Runtime.ClaudeLifecycle,
			),
			firstNonEmptyString(
				projection.Controller.ResumeSession,
				projection.Controller.ActiveMeta.ResumeSessionID,
				projection.Runtime.ResumeSessionID,
			),
		),
	})
	state := projection.Controller.State
	downgradedStaleRunning := false
	if state == "" {
		switch strings.TrimSpace(runtimeMeta.ClaudeLifecycle) {
		case "waiting_input":
			state = ControllerStateWaitInput
		case "starting", "active":
			state = ControllerStateThinking
		case "resumable":
			state = ControllerStateIdle
		}
	}
	if !hasActiveRunner && !externalNativeActive {
		switch state {
		case ControllerStateThinking, ControllerStateRunningTool:
			if strings.TrimSpace(runtimeMeta.ResumeSessionID) != "" {
				state = ControllerStateWaitInput
				runtimeMeta.ClaudeLifecycle = "waiting_input"
			} else {
				state = ControllerStateIdle
				runtimeMeta.ClaudeLifecycle = ""
				downgradedStaleRunning = true
			}
		}
	}
	if state == "" {
		return nil
	}
	awaitInput := state == ControllerStateWaitInput
	currentStepMessage := ""
	if projection.CurrentStep != nil && !isTerminalStepStatus(projection.CurrentStep.Status) && !isTerminalStepMessage(projection.CurrentStep.Message) {
		currentStepMessage = projection.CurrentStep.Message
	}
	message := firstNonEmptyString(projection.Controller.LastStep, currentStepMessage)
	if downgradedStaleRunning {
		message = ""
	}
	switch state {
	case ControllerStateWaitInput:
		message = firstNonEmptyString(message, "等待输入")
	case ControllerStateThinking:
		message = firstNonEmptyString(message, "思考中")
	case ControllerStateRunningTool:
		message = firstNonEmptyString(message, "执行工具中")
	case ControllerStateIdle:
		if strings.TrimSpace(runtimeMeta.ResumeSessionID) == "" &&
			strings.TrimSpace(runtimeMeta.ClaudeLifecycle) == "" {
			return nil
		}
		if strings.TrimSpace(runtimeMeta.ClaudeLifecycle) == "" &&
			strings.TrimSpace(runtimeMeta.ResumeSessionID) != "" {
			runtimeMeta.ClaudeLifecycle = "waiting_input"
		}
		message = firstNonEmptyString(message, "会话已暂停，可继续对话")
	default:
		if strings.TrimSpace(message) == "" {
			return nil
		}
	}
	event := protocol.NewAgentStateEvent(
		record.Summary.ID,
		string(state),
		message,
		awaitInput,
		runtimeMeta.Command,
		projection.Controller.LastStep,
		projection.Controller.LastTool,
	)
	event.RuntimeMeta = runtimeMeta
	return &event
}

func protocolLogEntries(items []data.SnapshotLogEntry) []protocol.HistoryLogEntry {
	entries := make([]protocol.HistoryLogEntry, 0, len(items))
	for _, entry := range items {
		entries = append(entries, protocol.HistoryLogEntry{
			Kind:        entry.Kind,
			Message:     entry.Message,
			Label:       entry.Label,
			Timestamp:   entry.Timestamp,
			Stream:      entry.Stream,
			Text:        entry.Text,
			ExecutionID: entry.ExecutionID,
			Phase:       entry.Phase,
			ExitCode:    entry.ExitCode,
			Context:     HistoryContextFromSnapshot(entry.Context),
			Attachments: append([]protocol.TimelineAttachment(nil), entry.Attachments...),
		})
	}
	return entries
}

func protocolTerminalExecutions(items []data.TerminalExecution) []protocol.TerminalExecution {
	executions := make([]protocol.TerminalExecution, 0, len(items))
	for _, item := range items {
		executions = append(executions, protocol.TerminalExecution{
			ExecutionID: item.ExecutionID,
			Command:     item.Command,
			CWD:         item.CWD,
			StartedAt:   item.StartedAt,
			FinishedAt:  item.FinishedAt,
			ExitCode:    item.ExitCode,
			Stdout:      item.Stdout,
			Stderr:      item.Stderr,
		})
	}
	return executions
}
