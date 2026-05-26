import 'package:flutter/material.dart';
import 'package:flutter_markdown/flutter_markdown.dart';
import 'package:flutter_test/flutter_test.dart';

import 'package:mobile_vc/data/models/events.dart';
import 'package:mobile_vc/widgets/event_card.dart';

void main() {
  testWidgets('markdown reply uses SelectionArea for cross-block selection',
      (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: EventCard(
            item: TimelineItem(
              id: 'md-1',
              kind: 'markdown',
              timestamp: DateTime(2026, 4, 4, 12),
              body: '# Title\n\nfirst line\nsecond line\n\n- item 1\n- item 2',
            ),
          ),
        ),
      ),
    );

    expect(find.byType(SelectionArea), findsOneWidget);
    final markdown = tester.widget<MarkdownBody>(find.byType(MarkdownBody));
    expect(markdown.selectable, isFalse);
  });

  testWidgets('compaction marker renders loading state', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: EventCard(
            item: TimelineItem(
              id: 'compaction-1',
              kind: 'compaction',
              timestamp: DateTime(2026, 4, 4, 12),
              status: 'loading',
              trigger: 'manual',
            ),
          ),
        ),
      ),
    );

    expect(find.text('压缩中'), findsOneWidget);
    expect(find.byType(CircularProgressIndicator), findsOneWidget);
  });

  testWidgets('compaction marker renders completed state', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: EventCard(
            item: TimelineItem(
              id: 'compaction-2',
              kind: 'compaction',
              timestamp: DateTime(2026, 4, 4, 12),
              status: 'completed',
              trigger: 'manual',
            ),
          ),
        ),
      ),
    );

    expect(find.text('已压缩'), findsOneWidget);
    expect(find.byIcon(Icons.content_cut_rounded), findsOneWidget);
  });

  testWidgets('compaction marker renders failed state', (tester) async {
    await tester.pumpWidget(
      MaterialApp(
        home: Scaffold(
          body: EventCard(
            item: TimelineItem(
              id: 'compaction-3',
              kind: 'compaction',
              timestamp: DateTime(2026, 4, 4, 12),
              status: 'failed',
              trigger: 'manual',
              body: 'backend failed',
            ),
          ),
        ),
      ),
    );

    expect(find.text('压缩失败'), findsOneWidget);
    expect(find.text('backend failed'), findsOneWidget);
    expect(find.byIcon(Icons.error_outline_rounded), findsOneWidget);
  });
}
