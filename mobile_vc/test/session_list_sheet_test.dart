import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:mobile_vc/data/models/session_models.dart';
import 'package:mobile_vc/features/session/session_list_sheet.dart';

void main() {
  testWidgets('外部原生会话点击删除会显示不可删除提示', (tester) async {
    String deletedSessionId = '';

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: SessionListSheet(
            sessions: const [
              SessionSummary(
                id: 'codex-thread:1',
                title: 'Desktop Codex',
                source: 'codex-native',
                external: true,
              ),
            ],
            selectedSessionId: '',
            cwd: '/workspace',
            onCreate: () {},
            onLoad: (_) {},
            onDelete: (id) => deletedSessionId = id,
          ),
        ),
      ),
    );

    await tester.tap(find.byIcon(Icons.delete_outline));
    await tester.pump();

    expect(deletedSessionId, isEmpty);
    expect(find.text('电脑 Codex 只能恢复，不能在 MobileVC 内删除'), findsOneWidget);
  });

  testWidgets('MobileVC 会话点击删除会触发 onDelete', (tester) async {
    String deletedSessionId = '';

    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: SessionListSheet(
            sessions: const [
              SessionSummary(
                id: 'session-1',
                title: 'MobileVC session',
                source: 'mobilevc',
                ownership: 'mobilevc',
              ),
            ],
            selectedSessionId: '',
            cwd: '/workspace',
            onCreate: () {},
            onLoad: (_) {},
            onDelete: (id) => deletedSessionId = id,
          ),
        ),
      ),
    );

    await tester.tap(find.byIcon(Icons.delete_outline));
    await tester.pump();

    expect(deletedSessionId, 'session-1');
    expect(find.byType(SnackBar), findsNothing);
  });
}
