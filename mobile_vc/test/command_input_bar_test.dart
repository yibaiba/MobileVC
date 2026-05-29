import 'dart:typed_data';

import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mobile_vc/data/models/session_models.dart';
import 'package:mobile_vc/features/chat/command_input_bar.dart';

void main() {
  group('CommandInputBar', () {
    testWidgets('permission 场景下禁用输入框并提示先确认授权', (tester) async {
      String? submitted;
      await tester.pumpWidget(
        _buildTestApp(
          shouldShowPermissionChoices: true,
          onSubmit: (value, _) => submitted = value,
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
          onSubmit: (value, _) => submitted = value,
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

    testWidgets('canStop 为 true 时优先显示停止按钮', (tester) async {
      var submitted = false;
      var stopped = false;
      await tester.pumpWidget(
        _buildTestApp(
          awaitInput: true,
          isBusy: true,
          showClaudeMode: true,
          currentEngine: 'claude',
          canStop: true,
          onSubmit: (text, images) => submitted = true,
          onStop: () => stopped = true,
        ),
      );

      expect(
          find.descendant(
            of: find.byType(FilledButton),
            matching: find.byIcon(Icons.stop_rounded),
          ),
          findsOneWidget);
      expect(find.byIcon(Icons.arrow_upward), findsNothing);

      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(submitted, isFalse);
      expect(stopped, isTrue);
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
          isBusy: true,
          showClaudeMode: false,
          currentEngine: 'shell',
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      expect(field.decoration?.hintText, '正在停止，请稍候...');
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

    testWidgets('权限提示锁住输入时仍允许停止运行任务', (tester) async {
      var stopped = false;
      await tester.pumpWidget(
        _buildTestApp(
          shouldShowPermissionChoices: true,
          isBusy: true,
          canStop: true,
          onStop: () => stopped = true,
        ),
      );

      final field = tester.widget<TextField>(find.byType(TextField));
      final button = tester.widget<FilledButton>(find.byType(FilledButton));

      expect(field.enabled, isFalse);
      expect(find.byIcon(Icons.stop_rounded), findsOneWidget);
      expect(button.onPressed, isNotNull);

      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(stopped, isTrue);
    });

    testWidgets('选择图片后先显示预览，发送时连同用户文本一起提交', (tester) async {
      String? submittedText;
      List<ChatImageAttachment> submittedImages = const [];
      var pickCount = 0;
      await tester.pumpWidget(
        _buildTestApp(
          onAttachImage: () async {
            pickCount++;
            return ChatImageAttachment(
              name: 'screen.png',
              mimeType: 'image/png',
              bytes: _transparentPngBytes,
            );
          },
          onSubmit: (value, images) {
            submittedText = value;
            submittedImages = images;
          },
        ),
      );

      await tester.tap(find.byTooltip('添加图片'));
      await tester.pump();

      expect(pickCount, 1);
      expect(find.byKey(const ValueKey('imageAttachmentPreview:screen.png')),
          findsOneWidget);
      expect(submittedText, isNull);

      await tester.enterText(find.byType(TextField), '这张图哪里有问题？');
      await tester.tap(find.byType(FilledButton));
      await tester.pump();

      expect(submittedText, '这张图哪里有问题？');
      expect(submittedImages, hasLength(1));
      expect(submittedImages.single.name, 'screen.png');
      expect(find.byKey(const ValueKey('imageAttachmentPreview:screen.png')),
          findsNothing);
    });

    testWidgets('图片预览可以在发送前移除', (tester) async {
      List<ChatImageAttachment>? submittedImages;
      await tester.pumpWidget(
        _buildTestApp(
          onAttachImage: () async => ChatImageAttachment(
            name: 'screen.png',
            mimeType: 'image/png',
            bytes: _transparentPngBytes,
          ),
          onSubmit: (_, images) => submittedImages = images,
        ),
      );

      await tester.tap(find.byTooltip('添加图片'));
      await tester.pump();
      expect(find.byKey(const ValueKey('imageAttachmentPreview:screen.png')),
          findsOneWidget);

      await tester.tap(find.byTooltip('移除图片'));
      await tester.pump();
      expect(find.byKey(const ValueKey('imageAttachmentPreview:screen.png')),
          findsNothing);

      await tester.enterText(find.byType(TextField), '只发送文字');
      await tester.tap(find.byType(FilledButton));
      await tester.pump();
      expect(submittedImages, isEmpty);
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

    testWidgets('底部工具栏常驻显示上下文圆形入口', (tester) async {
      await tester.pumpWidget(
        _buildTestApp(
          contextWindowUsage: const ContextWindowUsage(
            tokensUsed: 120000,
            tokenLimit: 200000,
          ),
        ),
      );

      expect(
          find.byKey(const ValueKey('context-window-button')), findsOneWidget);
    });
  });
}

Widget _buildTestApp({
  bool shouldShowPermissionChoices = false,
  bool shouldShowReviewChoices = false,
  bool awaitInput = false,
  bool isBusy = false,
  bool canStop = false,
  bool showClaudeMode = true,
  String currentEngine = 'claude',
  bool isSessionLoading = false,
  ContextWindowUsage contextWindowUsage = const ContextWindowUsage(),
  void Function(String text, List<ChatImageAttachment> imageAttachments)?
      onSubmit,
  Future<ChatImageAttachment?> Function()? onAttachImage,
  VoidCallback? onStop,
}) {
  return MaterialApp(
    home: Scaffold(
      bottomNavigationBar: CommandInputBar(
        awaitInput: awaitInput,
        isBusy: isBusy,
        canStop: canStop,
        canCompact: false,
        isCompacting: false,
        compactStatusLabel: '',
        contextWindowUsage: contextWindowUsage,
        onOpenContextWindowUsage: () {},
        hasPendingReview: false,
        fastMode: false,
        permissionMode: 'default',
        shouldShowPermissionChoices: shouldShowPermissionChoices,
        shouldShowReviewChoices: shouldShowReviewChoices,
        onSubmit: onSubmit ?? (text, images) {},
        onAttachImage: onAttachImage ?? () async => null,
        onStop: onStop ?? () {},
        onCompact: () {},
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

final _transparentPngBytes = Uint8List.fromList([
  0x89,
  0x50,
  0x4e,
  0x47,
  0x0d,
  0x0a,
  0x1a,
  0x0a,
  0x00,
  0x00,
  0x00,
  0x0d,
  0x49,
  0x48,
  0x44,
  0x52,
  0x00,
  0x00,
  0x00,
  0x01,
  0x00,
  0x00,
  0x00,
  0x01,
  0x08,
  0x06,
  0x00,
  0x00,
  0x00,
  0x1f,
  0x15,
  0xc4,
  0x89,
  0x00,
  0x00,
  0x00,
  0x0a,
  0x49,
  0x44,
  0x41,
  0x54,
  0x78,
  0x9c,
  0x63,
  0x00,
  0x01,
  0x00,
  0x00,
  0x05,
  0x00,
  0x01,
  0x0d,
  0x0a,
  0x2d,
  0xb4,
  0x00,
  0x00,
  0x00,
  0x00,
  0x49,
  0x45,
  0x4e,
  0x44,
  0xae,
  0x42,
  0x60,
  0x82,
]);
