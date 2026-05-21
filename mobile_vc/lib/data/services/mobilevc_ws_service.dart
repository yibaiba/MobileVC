import 'dart:async';
import 'dart:convert';

import 'package:flutter/foundation.dart';
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/io.dart';

import '../models/events.dart';
import '../models/runtime_meta.dart';
import 'mobilevc_mapper.dart';

class MobileVcWsService {
  MobileVcWsService({MobileVcMapper? mapper})
      : _mapper = mapper ?? const MobileVcMapper();

  final MobileVcMapper _mapper;
  final StreamController<AppEvent> _events =
      StreamController<AppEvent>.broadcast();
  WebSocketChannel? _channel;
  StreamSubscription? _subscription;
  int _connectionEpoch = 0;

  Stream<AppEvent> get events => _events.stream;
  bool get isConnected => _channel != null;

  Future<void> connect(String url) async {
    await _connectChannel(Uri.parse(url));
  }

  Future<void> connectRelay({
    required String relayUrl,
    required String sessionId,
    String pairingSecret = '',
    String clientId = '',
    String clientReconnectSecret = '',
  }) async {
    final uri = Uri.parse(relayUrl).replace(path: '/relay/client');
    await _connectChannel(
      uri,
      relaySessionId: sessionId,
      relayPairingSecret: pairingSecret,
      relayClientId: clientId,
      relayClientReconnectSecret: clientReconnectSecret,
    );
  }

  Future<void> _connectChannel(
    Uri uri, {
    String relaySessionId = '',
    String relayPairingSecret = '',
    String relayClientId = '',
    String relayClientReconnectSecret = '',
  }) async {
    final previousSubscription = _subscription;
    final previousChannel = _channel;
    _subscription = null;
    _channel = null;
    _relayContext = const _RelayContext();
    _connectionEpoch++;
    await previousSubscription?.cancel();
    await previousChannel?.sink.close();
    final channel = kIsWeb
        ? WebSocketChannel.connect(uri)
        : IOWebSocketChannel.connect(
            uri,
            pingInterval: const Duration(seconds: 15),
            connectTimeout: const Duration(seconds: 15),
          );
    _channel = channel;
    final epoch = _connectionEpoch;
    var activeRelayClientId = relayClientId.trim();
    var activeRelayReconnectSecret = relayClientReconnectSecret.trim();
    final pairedCompleter =
        relaySessionId.isNotEmpty ? Completer<void>() : null;
    if (relaySessionId.isNotEmpty) {
      channel.sink.add(jsonEncode(_relayAuthFrame(
        relaySessionId: relaySessionId,
        relayPairingSecret: relayPairingSecret,
        relayClientId: activeRelayClientId,
        relayClientReconnectSecret: relayClientReconnectSecret,
      )));
    }
    var disconnectEmitted = false;
    void emitDisconnect({
      required String code,
      required String message,
      Map<String, dynamic> raw = const <String, dynamic>{},
    }) {
      if (disconnectEmitted ||
          epoch != _connectionEpoch ||
          _channel != channel) {
        return;
      }
      disconnectEmitted = true;
      _channel = null;
      _subscription = null;
      _events.add(
        ErrorEvent(
          timestamp: DateTime.now(),
          sessionId: '',
          runtimeMeta: const RuntimeMeta(),
          raw: <String, dynamic>{
            'type': 'error',
            'code': code,
            'msg': message,
            ...raw,
          },
          code: code,
          message: message,
        ),
      );
    }

    void completePairingError(Object error) {
      final completer = pairedCompleter;
      if (completer != null && !completer.isCompleted) {
        completer.completeError(error);
      }
    }

    _subscription = channel.stream.listen(
      (dynamic data) {
        if (epoch != _connectionEpoch || _channel != channel) {
          return;
        }
        final decoded = jsonDecode(data as String);
        if (decoded is! Map<String, dynamic>) {
          return;
        }
        if (relaySessionId.isEmpty) {
          _events.add(_mapper.mapEvent(decoded));
          return;
        }
        if (isRelayAuthError(decoded, pairedCompleter?.isCompleted != true)) {
          completePairingError(RelayPairingException.fromFrame(decoded));
          return;
        }
        final relayEvent =
            _decodeRelayFrame(decoded, (clientId, reconnectSecret) {
          activeRelayClientId = clientId;
          if (reconnectSecret.trim().isNotEmpty) {
            activeRelayReconnectSecret = reconnectSecret;
          }
          final completer = pairedCompleter;
          if (completer != null && !completer.isCompleted) {
            completer.complete();
          }
        });
        if (relayEvent != null) {
          _events.add(_mapper.mapEvent(relayEvent));
        }
      },
      onError: (Object error, StackTrace stackTrace) {
        completePairingError(error);
        emitDisconnect(
          code: 'ws_stream_error',
          message: 'WebSocket 连接异常：$error',
          raw: <String, dynamic>{
            'stack': stackTrace.toString(),
          },
        );
      },
      onDone: () {
        final closeCode = channel.closeCode;
        final closeReason = channel.closeReason;
        final message = closeCode == null
            ? 'WebSocket 连接已断开'
            : closeReason == null || closeReason.isEmpty
                ? 'WebSocket 连接已断开（$closeCode）'
                : 'WebSocket 连接已断开（$closeCode: $closeReason）';
        completePairingError(StateError(message));
        emitDisconnect(
          code: 'ws_closed',
          message: message,
          raw: <String, dynamic>{
            'closeCode': closeCode,
            'closeReason': closeReason,
          },
        );
      },
      cancelOnError: false,
    );
    _relayContext = _RelayContext(
      sessionId: relaySessionId,
      clientIdProvider: () => activeRelayClientId,
    );
    if (pairedCompleter != null) {
      try {
        await pairedCompleter.future.timeout(
          const Duration(seconds: 10),
          onTimeout: () {
            throw TimeoutException('Relay 配对超时');
          },
        );
      } catch (_) {
        await disconnect();
        rethrow;
      }
    }
    if (relaySessionId.isNotEmpty) {
      _relaySession = RelaySession(
        sessionId: relaySessionId,
        clientId: activeRelayClientId,
        clientReconnectSecret: activeRelayReconnectSecret,
      );
    }
  }

  Future<void> disconnect() async {
    final previousSubscription = _subscription;
    final previousChannel = _channel;
    _subscription = null;
    _channel = null;
    _connectionEpoch++;
    await previousSubscription?.cancel();
    await previousChannel?.sink.close();
  }

  bool send(Map<String, dynamic> payload) {
    final channel = _channel;
    if (channel == null) {
      _events.add(
        ErrorEvent(
          timestamp: DateTime.now(),
          sessionId: (payload['sessionId'] ?? '').toString(),
          runtimeMeta: const RuntimeMeta(),
          raw: const <String, dynamic>{
            'type': 'error',
            'code': 'ws_not_connected',
            'msg': 'WebSocket is not connected',
          },
          code: 'ws_not_connected',
          message: 'WebSocket is not connected',
        ),
      );
      return false;
    }
    try {
      final relayContext = _relayContext;
      if (relayContext.sessionId.isNotEmpty) {
        channel.sink.add(jsonEncode(_encodeRelayFrame(payload, relayContext)));
        return true;
      }
      channel.sink.add(jsonEncode(payload));
      return true;
    } catch (error, stackTrace) {
      _channel = null;
      _subscription = null;
      _events.add(
        ErrorEvent(
          timestamp: DateTime.now(),
          sessionId: (payload['sessionId'] ?? '').toString(),
          runtimeMeta: const RuntimeMeta(),
          raw: <String, dynamic>{
            'type': 'error',
            'code': 'ws_send_error',
            'msg': 'WebSocket send failed: $error',
            'stack': stackTrace.toString(),
          },
          code: 'ws_send_error',
          message: 'WebSocket send failed: $error',
        ),
      );
      return false;
    }
  }

  Future<void> dispose() async {
    await disconnect();
    await _events.close();
  }

  _RelayContext _relayContext = const _RelayContext();
  RelaySession? _relaySession;

  RelaySession? takeRelaySession() {
    final session = _relaySession;
    _relaySession = null;
    return session;
  }

  Map<String, dynamic>? _decodeRelayFrame(
    Map<String, dynamic> frame,
    void Function(String clientId, String clientReconnectSecret) setClientId,
  ) {
    final type = (frame['type'] ?? '').toString();
    if (type == 'client.paired') {
      setClientId(
        (frame['clientId'] ?? '').toString(),
        (frame['clientReconnectSecret'] ?? '').toString(),
      );
      return null;
    }
    if (type == 'relay.error') {
      return <String, dynamic>{
        'type': 'error',
        'code': frame['code'] ?? 'relay_error',
        'msg': relayErrorMessage(frame),
      };
    }
    if (type != 'relay.forward') {
      return null;
    }
    final raw =
        base64Url.decode(base64Url.normalize(frame['payload'].toString()));
    final decoded = jsonDecode(utf8.decode(raw));
    return decoded is Map<String, dynamic> ? decoded : null;
  }

  Map<String, dynamic> _relayAuthFrame({
    required String relaySessionId,
    required String relayPairingSecret,
    required String relayClientId,
    required String relayClientReconnectSecret,
  }) {
    if (relayClientId.trim().isNotEmpty &&
        relayClientReconnectSecret.trim().isNotEmpty) {
      return <String, dynamic>{
        'type': 'client.reconnect',
        'version': 1,
        'sessionId': relaySessionId,
        'clientId': relayClientId,
        'clientReconnectSecret': relayClientReconnectSecret,
      };
    }
    return <String, dynamic>{
      'type': 'client.pair',
      'version': 1,
      'sessionId': relaySessionId,
      'pairingSecret': relayPairingSecret,
    };
  }

  Map<String, dynamic> _encodeRelayFrame(
    Map<String, dynamic> payload,
    _RelayContext relayContext,
  ) {
    return <String, dynamic>{
      'type': 'relay.forward',
      'version': 1,
      'sessionId': relayContext.sessionId,
      'clientId': relayContext.clientIdProvider(),
      'direction': 'client_to_agent',
      'messageId': 'msg_${const Uuid().v4()}',
      'contentType': 'mobilevc.ws.v1',
      'encryption': 'none',
      'payloadEncoding': 'base64url',
      'payload': base64UrlEncode(utf8.encode(jsonEncode(payload))),
    };
  }
}

class RelaySession {
  const RelaySession({
    required this.sessionId,
    required this.clientId,
    required this.clientReconnectSecret,
  });

  final String sessionId;
  final String clientId;
  final String clientReconnectSecret;
}

class _RelayContext {
  const _RelayContext({
    this.sessionId = '',
    this.clientIdProvider = _emptyClientId,
  });

  final String sessionId;
  final String Function() clientIdProvider;
}

String _emptyClientId() => '';

@visibleForTesting
bool isRelayPairingError(Map<String, dynamic> frame, String clientId) =>
    (frame['type'] ?? '').toString() == 'relay.error' && clientId.isEmpty;

@visibleForTesting
bool isRelayAuthError(Map<String, dynamic> frame, bool authPending) =>
    authPending && (frame['type'] ?? '').toString() == 'relay.error';

String relayErrorMessage(Map<String, dynamic> frame) =>
    switch ((frame['code'] ?? '').toString()) {
      'payload_too_large' => 'Relay 数据包过大：当前消息超过中继限制，请关闭 ADB 截屏流或减少单次同步内容后重试',
      'e2ee_required' => 'Relay 已禁用明文连接：请更新手机端和本机服务，重新配对并启用 E2EE',
      'e2ee_unsupported_version' => 'Relay E2EE 版本不兼容：请更新手机端和本机服务后重新连接',
      'e2ee_fingerprint_mismatch' => 'Relay 节点指纹不一致：请停止连接并重新确认配对链接',
      'e2ee_handshake_failed' => 'Relay E2EE 握手失败：请重新配对，确认链接未过期',
      'e2ee_decrypt_failed' => 'Relay E2EE 解密失败：消息认证未通过，请重新连接',
      'e2ee_replay_detected' => 'Relay 检测到重复或过期的加密消息：请重新连接',
      'device_revoked' => '此设备已被本机撤销：请在本机重新授权后再配对',
      'device_unknown' => '此设备未绑定或本机身份已轮换：请重新配对',
      'stream_cancelled' => 'Relay 加密流已取消',
      'stream_window_exceeded' => 'Relay 加密流窗口超限：请重试或减少并发传输',
      'download_denied' => '本机文件策略拒绝下载：请选择允许的工作区文件',
      'download_failed' => 'Relay 加密下载失败：请重试并查看本机日志',
      _ => (frame['message'] ?? 'Relay error').toString(),
    };

class RelayPairingException implements Exception {
  const RelayPairingException(this.code, this.message);

  factory RelayPairingException.fromFrame(Map<String, dynamic> frame) {
    final code = (frame['code'] ?? 'relay_error').toString();
    if (code == 'pairing_rejected') {
      return const RelayPairingException(
        'pairing_rejected',
        'Relay 认证被拒绝：链接可能已过期，或本机服务已重启，请导入最新中继链接',
      );
    }
    return RelayPairingException(code, relayErrorMessage(frame));
  }

  final String code;
  final String message;

  @override
  String toString() => message;
}
