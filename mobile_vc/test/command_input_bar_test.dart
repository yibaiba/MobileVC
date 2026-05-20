import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mobile_vc/features/chat/command_input_bar.dart';
import 'package:mobile_vc/data/models/session_models.dart';

void main() {
  group('CommandInputBar', () {
    testWidgets('permission 场景下禁用输入框并提示先确认授权', (tester) async {
      String? submitted;
      await tester.pumpWidget(
        _buildTestApp(
          shouldShowPermissionChoices: true,
          onSubmit: (value) => submitted = value,
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      final button = tester.widget<FilledButton>(find.byType(FilledButton));

      expect(field.enabled, isFalse);
      expect(field.readOnly, isTrue);
      expect(field.canRequestFocus, isFalse);
      expect(field.decoration?.hintText, '请先在上方确认授权');
      expect(button.onPressed, isNull);

      expect(submitted, isNull);
    });

    testWidgets('review 场景下禁用输入框并提示先完成审核', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          shouldShowReviewChoices: true,
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      final button = tester.widget<FilledButton>(find.byType(FilledButton));

      expect(field.enabled, isFalse);
      expect(field.readOnly, isTrue);
      expect(field.canRequestFocus, isFalse);
      expect(field.decoration?.hintText, '请先在上方完成审核');
      expect(button.onPressed, isNull);
    });

    testWidgets('普通场景下仍可输入并发送', (tester) async {
      String? submitted;
      await tester.pumpWidget(
        _buildTestApp(
          onSubmit: (value) => submitted = value,
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      final button = tester.widget<FilledButton>(find.byType(FilledButton));

      expect(field.enabled, isTrue);
      expect(button.onPressed, isNotNull);

      await tester.enterText(find.byType(TextField), 'hello');
      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(submitted, 'hello');
    });

    testWidgets('Claude 模式显示 Claude 状态与 hint', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          awaitInput: true,
          showClaudeMode: true,
          currentEngine: 'claude',
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      expect(field.decoration?.hintText, '回复 Claude');
    });

    testWidgets('等待输入时即使 canStop 为 true 也显示发送按钮', (tester) async {
      var submitted = false;
      var stopped = false;
      await tester.pumpWidget(
        _buildTestApp(
          awaitInput: true,
          isBusy: true,
          showClaudeMode: true,
          currentEngine: 'claude',
          canStop: true,
          onSubmit: (_) => submitted = true,
          onStop: () => stopped = true,
        ),
      );

      expect(find.byIcon(Icons.arrow_upward), findsOneWidget);
      expect(find.byIcon(Icons.stop_rounded), findsNothing);

      await tester.enterText(find.byType(TextField), '继续');
      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(submitted, isTrue);
      expect(stopped, isFalse);
    });

    testWidgets('Codex 模式显示 Codex 状态与 hint', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          awaitInput: true,
          showClaudeMode: true,
          currentEngine: 'codex',
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      expect(field.decoration?.hintText, '回复 Codex');
    });

    testWidgets('shell 模式显示 Shell 状态与 hint', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          isBusy: false,
          showClaudeMode: false,
          currentEngine: 'shell',
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      expect(field.decoration?.hintText, '输入命令');
    });

    testWidgets('busy 且非等待输入时发送按钮切为停止按钮', (tester) async {
      var stopped = false;
      await tester.pumpWidget(
        _buildTestApp(
          isBusy: true,
          canStop: true,
          showClaudeMode: true,
          currentEngine: 'codex',
          onStop: () => stopped = true,
        ),
      );

      expect(find.byIcon(Icons.stop_rounded), findsOneWidget);

      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(stopped, isTrue);
    });

    testWidgets('loading 期间显示会话切换中 hint 并禁用输入', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          isSessionLoading: true,
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      final button = tester.widget<FilledButton>(find.byType(FilledButton));
      expect(field.enabled, isFalse);
      expect(field.decoration?.hintText, '会话切换中...');
      expect(button.onPressed, isNull);
    });

    testWidgets('支持 compact 时显示压缩按钮并触发回调', (tester) async {
      var compacted = false;
      await tester.pumpWidget(
        _buildTestApp(
          canCompact: true,
          onCompact: () => compacted = true,
        ),
      );

      expect(find.text('压缩'), findsOneWidget);

      await tester.tap(find.text('压缩'));
      await tester.pump();

      expect(compacted, isTrue);
    });

    testWidgets('compact 进行中时显示压缩中状态并禁用点击', (tester) async {
      var compacted = false;
      await tester.pumpWidget(
        _buildTestApp(
          canCompact: false,
          isCompacting: true,
          compactStatusLabel: '正在压缩上下文…',
          onCompact: () => compacted = true,
        ),
      );

      expect(find.text('压缩中'), findsOneWidget);
      expect(find.byType(CircularProgressIndicator), findsWidgets);

      await tester.tap(find.text('压缩中'));
      await tester.pump();

      expect(compacted, isFalse);
    });

    testWidgets('支持 compact 时按钮顺序为压缩在前 日志在最后', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          canCompact: true,
        ),
      );

      final compactX = tester.getCenter(find.text('压缩')).dx;
      final skillX = tester.getCenter(find.text('Skill')).dx;
      final logsX = tester.getCenter(find.text('日志')).dx;
      final memoryX = tester.getCenter(find.text('Memory')).dx;

      expect(compactX, lessThan(skillX));
      expect(logsX, greaterThan(memoryX));
    });

    testWidgets('不支持 compact 时不显示压缩按钮', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          canCompact: false,
        ),
      );

      expect(find.text('压缩'), findsNothing);
    });

    testWidgets('始终显示上下文圆形入口并支持点击', (tester) async {
      var opened = false;
      await tester.pumpWidget(
        _buildTestApp(
          contextWindowUsage: const ContextWindowUsage(
            tokensUsed: 120000,
            tokenLimit: 200000,
          ),
          onOpenContextWindowUsage: () => opened = true,
        ),
      );

      expect(find.byKey(const ValueKey('context-window-button')), findsOneWidget);

      await tester.tap(find.byKey(const ValueKey('context-window-button')));
      await tester.pump();

      expect(opened, isTrue);
    });
  });
}

Widget _buildTestApp({
  bool shouldShowPermissionChoices = false,
  bool shouldShowReviewChoices = false,
  bool awaitInput = false,
  bool isBusy = false,
  bool canStop = false,
  bool canCompact = false,
  bool isCompacting = false,
  String compactStatusLabel = '',
  ContextWindowUsage contextWindowUsage = const ContextWindowUsage(),
  bool showClaudeMode = true,
  String currentEngine = 'claude',
  bool isSessionLoading = false,
  ValueChanged<String>? onSubmit,
  VoidCallback? onStop,
  VoidCallback? onCompact,
  VoidCallback? onOpenContextWindowUsage,
}) {
  return MaterialApp(
    home: Scaffold(
      bottomNavigationBar: CommandInputBar(
        awaitInput: awaitInput,
        isBusy: isBusy,
        canStop: canStop,
        canCompact: canCompact,
        isCompacting: isCompacting,
        compactStatusLabel: compactStatusLabel,
        contextWindowUsage: contextWindowUsage,
        onOpenContextWindowUsage: onOpenContextWindowUsage ?? () {},
        hasPendingReview: false,
        fastMode: false,
        permissionMode: 'default',
        shouldShowPermissionChoices: shouldShowPermissionChoices,
        shouldShowReviewChoices: shouldShowReviewChoices,
        onSubmit: onSubmit ?? (_) {},
        onStop: onStop ?? () {},
        onCompact: onCompact ?? () {},
        onOpenSessions: () {},
        onOpenRuntimeInfo: () {},
        onOpenLogs: () {},
        onOpenSkills: () {},
        onOpenMemory: () {},
        onOpenPermissions: () {},
        onOpenModels: () {},
        onPermissionModeChanged: (_) {},
        showClaudeMode: showClaudeMode,
        currentEngine: currentEngine,
        modelSummary: 'Sonnet',
        permissionRuleSummary: '默认',
        shouldShowPlanChoices: false,
        isSessionLoading: isSessionLoading,
      ),
    ),
  );
}
