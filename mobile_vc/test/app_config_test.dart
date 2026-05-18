import 'package:flutter_test/flutter_test.dart';
import 'package:mobile_vc/core/config/app_config.dart';

void main() {
  group('AppConfig connection urls', () {
    test('defaults to plain http and websocket transport', () {
      const config = AppConfig(
        host: '127.0.0.1',
        port: '8001',
        token: 'test-token',
      );

      expect(
        config.baseHttpUrlFor(secureTransport: false),
        'http://127.0.0.1:8001',
      );
      expect(
        config.wsUrlFor(secureTransport: false),
        'ws://127.0.0.1:8001/ws?token=test-token',
      );
    });

    test('secure transport uses https and wss before connecting', () {
      const config = AppConfig(
        host: 'api.example.com',
        port: '443',
        token: 'token with spaces',
      );

      final httpUri = Uri.parse(config.baseHttpUrlFor(secureTransport: true));
      final wsUri = Uri.parse(config.wsUrlFor(secureTransport: true));
      final downloadUri = config.downloadUri(
        'logs/session.txt',
        secureTransport: true,
      );

      expect(httpUri.scheme, 'https');
      expect(httpUri.host, 'api.example.com');
      expect(wsUri.scheme, 'wss');
      expect(wsUri.path, '/ws');
      expect(wsUri.queryParameters['token'], 'token with spaces');
      expect(downloadUri.scheme, 'https');
      expect(downloadUri.queryParameters['path'], 'logs/session.txt');
    });

    test('secure transport preserves non-default backend ports', () {
      const config = AppConfig(host: 'api.example.com', port: '8443');

      expect(
        config.baseHttpUrlFor(secureTransport: true),
        'https://api.example.com:8443',
      );
      expect(
        config.wsUrlFor(secureTransport: true),
        'wss://api.example.com:8443/ws?token=test',
      );
    });

    test('manual host accepts http URL without treating it as a host', () {
      const config = AppConfig(host: 'http://example.com', port: '9999');

      expect(config.baseHttpUrl, 'http://example.com:9999');
      expect(config.wsUrl, 'ws://example.com:9999/ws?token=test');
    });

    test('manual host URL can carry backend port', () {
      const config = AppConfig(host: 'https://example.com:9999');

      expect(config.baseHttpUrl, 'https://example.com:9999');
      expect(config.wsUrl, 'wss://example.com:9999/ws?token=test');
    });

    test('manual host port accepts a trailing slash', () {
      const config = AppConfig(host: 'example.com:9999/');

      expect(config.baseHttpUrlFor(secureTransport: true),
          'https://example.com:9999');
      expect(config.wsUrlFor(secureTransport: true),
          'wss://example.com:9999/ws?token=test');
    });

    test('copyWith stores manual endpoint URL as host and port', () {
      final config = const AppConfig(port: '19000').copyWith(
        host: 'http://example.com:9999',
      );

      expect(config.host, 'example.com');
      expect(config.port, '9999');
      expect(config.secureTransport, isFalse);
      expect(config.wsUrl, 'ws://example.com:9999/ws?token=test');
    });

    test('fromJson migrates legacy endpoint URL host', () {
      final config = AppConfig.fromJson(const {
        'host': 'http://example.com:9999',
        'port': '19000',
      });

      expect(config.host, 'example.com');
      expect(config.port, '9999');
      expect(config.secureTransport, isFalse);
      expect(config.baseHttpUrl, 'http://example.com:9999');
    });

    test('fromJson preserves explicit secure transport for plain host', () {
      final config = AppConfig.fromJson(const {
        'host': 'example.com',
        'port': '9999',
        'secureTransport': true,
      });

      expect(config.baseHttpUrl, 'https://example.com:9999');
      expect(config.wsUrl, 'wss://example.com:9999/ws?token=test');
    });

    test('invalid port surfaces as a format error', () {
      const config = AppConfig(port: 'not-a-port');

      expect(
        () => config.wsUrlFor(secureTransport: false),
        throwsA(isA<FormatException>()),
      );
    });
  });

  group('AppConfig adb ice', () {
    test('auto config builds stun and turn from host', () {
      const config = AppConfig(
        host: '8.162.1.176',
        adbIceServersJson: '{"username":"mobilevc","credential":"secret"}',
      );

      expect(config.adbIceUsername, 'mobilevc');
      expect(config.adbIceCredential, 'secret');
      expect(config.hasAutoAdbIceConfig, isTrue);
      expect(config.adbIceServers, <Map<String, dynamic>>[
        <String, dynamic>{
          'urls': <String>['stun:8.162.1.176:3478'],
        },
        <String, dynamic>{
          'urls': <String>[
            'turn:8.162.1.176:3478?transport=udp',
            'turn:8.162.1.176:3478?transport=tcp',
          ],
          'username': 'mobilevc',
          'credential': 'secret',
        },
      ]);
    });

    test('auto config honors explicit turn host override', () {
      const config = AppConfig(
        host: '8.162.1.176',
        adbIceServersJson:
            '{"host":"turn.example.com","username":"mobilevc","credential":"secret"}',
      );

      expect(config.adbIceHostOverride, 'turn.example.com');
      expect(config.adbIceServers, <Map<String, dynamic>>[
        <String, dynamic>{
          'urls': <String>['stun:turn.example.com:3478'],
        },
        <String, dynamic>{
          'urls': <String>[
            'turn:turn.example.com:3478?transport=udp',
            'turn:turn.example.com:3478?transport=tcp',
          ],
          'username': 'mobilevc',
          'credential': 'secret',
        },
      ]);
    });

    test('legacy json remains readable for username and credential', () {
      const config = AppConfig(
        host: '8.162.1.176',
        adbIceServersJson:
            '[{"urls":["stun:stun.l.google.com:19302"]},{"urls":["turn:8.162.1.176:3478?transport=udp"],"username":"legacy","credential":"cred"}]',
      );

      expect(config.hasAutoAdbIceConfig, isFalse);
      expect(config.adbIceUsername, 'legacy');
      expect(config.adbIceCredential, 'cred');
      expect(config.hasTurnAdbIceServer, isTrue);
      expect(config.shouldForceAdbRelay, isTrue);
      expect(config.adbIceServers.length, 2);
    });

    test('private host keeps mixed ice instead of forcing relay', () {
      const config = AppConfig(
        host: '192.168.0.8',
        adbIceServersJson: '{"username":"mobilevc","credential":"secret"}',
      );

      expect(config.hasTurnAdbIceServer, isTrue);
      expect(config.shouldForceAdbRelay, isFalse);
    });

    test('encode auto config returns empty when both fields are blank', () {
      expect(
        AppConfig.encodeAutoAdbIceConfig(username: ' ', credential: ''),
        isEmpty,
      );
    });

    test('encode auto config keeps explicit turn host override', () {
      expect(
        AppConfig.encodeAutoAdbIceConfig(
          host: 'turn.example.com',
          username: 'mobilevc',
          credential: 'secret',
        ),
        '{"host":"turn.example.com","username":"mobilevc","credential":"secret"}',
      );
    });

    test('legacy model config migrates into the matching engine slot', () {
      final config = AppConfig.fromJson(const {
        'engine': 'codex',
        'model': 'gpt-5.4',
        'reasoningEffort': 'high',
      });

      expect(config.codexModel, 'gpt-5.4');
      expect(config.codexReasoningEffort, 'high');
      expect(config.claudeModel, isEmpty);
    });

    test('separate Claude and Codex model settings are preserved', () {
      const config = AppConfig(
        engine: 'claude',
        claudeModel: 'opus',
        codexModel: 'gpt-5.4',
        codexReasoningEffort: 'high',
      );

      expect(config.modelForEngine('claude'), 'opus');
      expect(config.modelForEngine('codex'), 'gpt-5.4');
      expect(config.reasoningEffortForEngine('codex'), 'high');
    });

    test('codex xhigh reasoning effort is preserved', () {
      const config = AppConfig(
        engine: 'codex',
        codexModel: 'gpt-5.4',
        codexReasoningEffort: 'xhigh',
      );

      expect(config.modelForEngine('codex'), 'gpt-5.4');
      expect(config.reasoningEffortForEngine('codex'), 'xhigh');
    });
  });
}
