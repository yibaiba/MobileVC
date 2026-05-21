import 'dart:async';
import 'dart:convert';
import 'package:flutter/foundation.dart';
import 'package:uuid/uuid.dart';
import 'package:web_socket_channel/web_socket_channel.dart';
import 'package:web_socket_channel/io.dart';

import '../../core/relay_e2ee/relay_e2ee_capability.dart';
import '../../core/relay_e2ee/relay_e2ee_crypto.dart';
import '../../core/relay_e2ee/relay_device_identity.dart';
import '../../core/relay_e2ee/relay_e2ee_handshake.dart';
import '../../core/relay_e2ee/relay_e2ee_handshake_frames.dart';
import '../../core/relay_e2ee/relay_mobilevc_stream.dart';
import '../models/events.dart';
import '../models/runtime_meta.dart';
import 'mobilevc_mapper.dart';

class MobileVcWsService {
  MobileVcWsService({
    MobileVcMapper? mapper,
    RelayDeviceIdentityStore? relayDeviceIdentityStore,
    RelayDeviceCredentialStore? relayDeviceCredentialStore,
  })  : _mapper = mapper ?? const MobileVcMapper(),
        _relayDeviceIdentityStore = relayDeviceIdentityStore ??
            RelayDeviceIdentityStore(
              secureStore: const FlutterRelaySecureStore(),
            ),
        _relayDeviceCredentialStore = relayDeviceCredentialStore ??
            RelayDeviceCredentialStore(
              secureStore: const FlutterRelaySecureStore(),
            );

  final MobileVcMapper _mapper;
  final RelayDeviceIdentityStore _relayDeviceIdentityStore;
  final RelayDeviceCredentialStore _relayDeviceCredentialStore;
  final StreamController<AppEvent> _events =
      StreamController<AppEvent>.broadcast();
  WebSocketChannel? _channel;
  StreamSubscription? _subscription;
  int _connectionEpoch = 0;

  Stream<AppEvent> get events => _events.stream;
  bool get isConnected => _channel != null;
  bool get hasRelayE2eeHandshake => _relayE2eeState?.complete == true;

  Future<void> connect(String url) async {
    await _connectChannel(Uri.parse(url));
  }

  Future<void> connectRelay({
    required String relayUrl,
    required String sessionId,
    String pairingSecret = '',
    String clientId = '',
    String clientReconnectSecret = '',
    String nodeFingerprintHex = '',
    RelayE2eeCapabilitySet? relayCapabilities,
  }) async {
    final uri = Uri.parse(relayUrl).replace(path: '/relay/client');
    await _connectChannel(
      uri,
      relaySessionId: sessionId,
      relayPairingSecret: pairingSecret,
      relayClientId: clientId,
      relayClientReconnectSecret: clientReconnectSecret,
      relayNodeFingerprintHex: nodeFingerprintHex,
      relayCapabilities: relayCapabilities,
    );
  }

  Future<void> _connectChannel(
    Uri uri, {
    String relaySessionId = '',
    String relayPairingSecret = '',
    String relayClientId = '',
    String relayClientReconnectSecret = '',
    String relayNodeFingerprintHex = '',
    RelayE2eeCapabilitySet? relayCapabilities,
  }) async {
    final initialRelayClientId = relayClientId.trim();
    final initialRelayReconnectSecret = relayClientReconnectSecret.trim();
    _validateRelayE2EEConnectMode(
      relayPairingSecret: relayPairingSecret,
      relayClientId: initialRelayClientId,
      relayClientReconnectSecret: initialRelayReconnectSecret,
      capabilities: relayCapabilities,
    );
    final previousSubscription = _subscription;
    final previousChannel = _channel;
    _subscription = null;
    _channel = null;
    _relayContext = const _RelayContext();
    _relayE2eeState = null;
    _relaySendQueue = Future<void>.value();
    _relayReceiveQueue = Future<void>.value();
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
    var activeRelayClientId = initialRelayClientId;
    var activeRelayReconnectSecret = initialRelayReconnectSecret;
    final pairedCompleter =
        relaySessionId.isNotEmpty ? Completer<void>() : null;
    final e2eeCompleter = _requiresPairingE2EE(
      relayPairingSecret: relayPairingSecret,
      capabilities: relayCapabilities,
    )
        ? Completer<void>()
        : null;
    _RelayE2eeHandshakeState? e2eeState;
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

    void completeRelayConnectError(Object error) {
      final completer = pairedCompleter;
      if (completer != null && !completer.isCompleted) {
        completer.completeError(error);
      }
      if (e2eeCompleter != null && !e2eeCompleter.isCompleted) {
        e2eeCompleter.completeError(error);
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
          completeRelayConnectError(RelayPairingException.fromFrame(decoded));
          return;
        }
        if (e2eeCompleter != null &&
            _isRelayE2EEFrame(decoded) &&
            _handleRelayE2EEFrame(
              channel: channel,
              frame: decoded,
              state: e2eeState,
              updateState: (next) => e2eeState = next,
              complete: e2eeCompleter,
              paired: pairedCompleter,
            )) {
          return;
        }
        _queueRelayReceive(
          channel: channel,
          frame: decoded,
          epoch: epoch,
          setClientId: (clientId, reconnectSecret) {
            activeRelayClientId = clientId;
            if (reconnectSecret.trim().isNotEmpty) {
              activeRelayReconnectSecret = reconnectSecret;
            }
            if (e2eeCompleter != null &&
                !e2eeCompleter.isCompleted &&
                e2eeState == null) {
              unawaited(() async {
                try {
                  e2eeState = await _startPairingE2EEHandshake(
                    channel: channel,
                    sessionId: relaySessionId,
                    clientId: clientId,
                    pairingSecret: relayPairingSecret,
                    expectedNodeFingerprintHex: relayNodeFingerprintHex,
                    capabilities: relayCapabilities!,
                    updateState: (next) => e2eeState = next,
                  );
                } catch (error, stackTrace) {
                  if (!e2eeCompleter.isCompleted) {
                    e2eeCompleter.completeError(error, stackTrace);
                  }
                }
              }());
            }
            final completer = pairedCompleter;
            if (completer != null &&
                !completer.isCompleted &&
                e2eeCompleter == null) {
              completer.complete();
            }
          },
        );
      },
      onError: (Object error, StackTrace stackTrace) {
        completeRelayConnectError(error);
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
        completeRelayConnectError(StateError(message));
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
        await (e2eeCompleter?.future ?? pairedCompleter.future).timeout(
          const Duration(seconds: 10),
          onTimeout: () {
            throw TimeoutException(
              e2eeCompleter == null ? 'Relay 配对超时' : 'Relay E2EE 握手超时',
            );
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
    _relayE2eeState = null;
    _relaySendQueue = Future<void>.value();
    _relayReceiveQueue = Future<void>.value();
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
        _queueRelaySend(
          channel: channel,
          payload: payload,
          relayContext: relayContext,
          epoch: _connectionEpoch,
        );
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
  _RelayE2eeHandshakeState? _relayE2eeState;
  Future<void> _relaySendQueue = Future<void>.value();
  Future<void> _relayReceiveQueue = Future<void>.value();

  RelaySession? takeRelaySession() {
    final session = _relaySession;
    _relaySession = null;
    return session;
  }

  Future<Map<String, dynamic>?> _decodeRelayFrame(
    Map<String, dynamic> frame,
    void Function(String clientId, String clientReconnectSecret) setClientId,
  ) async {
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
    final e2eeState = _relayE2eeState;
    if (frame['encryption'] == relayE2eeSuite) {
      final codec = e2eeState?.streamCodec;
      if (codec == null) {
        throw StateError(
            'Relay E2EE frame received before encrypted stream is ready');
      }
      return codec.decodeJson(frame);
    }
    if (e2eeState?.complete == true) {
      throw const FormatException(
          'plaintext relay.forward received after E2EE activation');
    }
    final raw =
        base64Url.decode(base64Url.normalize(frame['payload'].toString()));
    final decoded = jsonDecode(utf8.decode(raw));
    return decoded is Map<String, dynamic> ? decoded : null;
  }

  Future<_RelayE2eeHandshakeState> _startPairingE2EEHandshake({
    required WebSocketChannel channel,
    required String sessionId,
    required String clientId,
    required String pairingSecret,
    required String expectedNodeFingerprintHex,
    required RelayE2eeCapabilitySet capabilities,
    required void Function(_RelayE2eeHandshakeState) updateState,
  }) async {
    final clientEphemeral = await RelayE2eeHandshake.newEphemeralKeyPair();
    final handshakeId = 'hs_${const Uuid().v4()}';
    final state = _RelayE2eeHandshakeState(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      pairingSecret: pairingSecret,
      expectedNodeFingerprintHex: expectedNodeFingerprintHex.toLowerCase(),
      capabilities: capabilities,
      clientEphemeral: clientEphemeral,
    );
    final hello = RelayE2eeClientHelloFrame(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      kind: relayE2eeHandshakeKindPairing,
      capabilities: capabilities,
      clientEphemeralPublicKey: clientEphemeral.publicKey,
    );
    updateState(state);
    channel.sink.add(jsonEncode(hello.toJson()));
    return state;
  }

  bool _handleRelayE2EEFrame({
    required WebSocketChannel channel,
    required Map<String, dynamic> frame,
    required _RelayE2eeHandshakeState? state,
    required void Function(_RelayE2eeHandshakeState) updateState,
    required Completer<void> complete,
    required Completer<void>? paired,
  }) {
    final type = (frame['type'] ?? '').toString();
    if (type == relayFrameAgentE2eeHello) {
      unawaited(_completePairingE2EEProof(
        channel: channel,
        pending: state,
        frame: frame,
        updateState: updateState,
        complete: complete,
      ));
      return true;
    }
    if (type == relayFrameAgentE2eeResult) {
      try {
        final result = RelayE2eeAgentResultFrame.fromJson(frame);
        if (!result.ok) {
          throw RelayPairingException(
            result.errorCode,
            relayErrorMessage(<String, dynamic>{
              'type': 'relay.error',
              'code': result.errorCode,
            }),
          );
        }
        final pending = state;
        if (pending == null ||
            !pending.matches(
              result.sessionId,
              result.clientId,
              result.handshakeId,
            ) ||
            pending.trafficKeys == null ||
            pending.input == null) {
          throw StateError('Relay E2EE 握手路由不匹配');
        }
        _relayE2eeState = pending.markComplete();
        _queueRelayDeviceRegister(
          channel: channel,
          state: _relayE2eeState!,
          epoch: _connectionEpoch,
        );
        if (!complete.isCompleted) {
          complete.complete();
        }
        if (paired != null && !paired.isCompleted) {
          paired.complete();
        }
      } catch (error, stackTrace) {
        if (!complete.isCompleted) {
          complete.completeError(error, stackTrace);
        }
      }
      return true;
    }
    return false;
  }

  Future<void> _completePairingE2EEProof({
    required WebSocketChannel channel,
    required _RelayE2eeHandshakeState? pending,
    required Map<String, dynamic> frame,
    required void Function(_RelayE2eeHandshakeState) updateState,
    required Completer<void> complete,
  }) async {
    try {
      if (pending == null) {
        throw StateError('Relay E2EE 握手状态不存在');
      }
      final agentHello = RelayE2eeAgentHelloFrame.fromJson(frame);
      if (!pending.matches(
        agentHello.sessionId,
        agentHello.clientId,
        agentHello.handshakeId,
      )) {
        throw StateError('Relay E2EE 握手路由不匹配');
      }
      final fingerprint = await RelayE2eeCrypto.fingerprint(
        Uint8List.fromList(agentHello.nodeIdentityPublicKey),
      );
      if (_hex(fingerprint) != pending.expectedNodeFingerprintHex) {
        throw const RelayPairingException(
          'e2ee_fingerprint_mismatch',
          'Relay 节点指纹不一致：请停止连接并重新确认配对链接',
        );
      }
      final input = pending.capabilities.applyToHandshake(
        RelayE2eeHandshakeInput(
          kind: relayE2eeHandshakeKindPairing,
          sessionId: pending.sessionId,
          clientId: pending.clientId,
          handshakeId: pending.handshakeId,
          relayProtocolVersion: 0,
          e2eeProtocolVersion: 0,
          tunnelProtocolVersion: 0,
          cryptoSuite: '',
          clientEphemeralPublicKey: pending.clientEphemeral.publicKey,
          nodeEphemeralPublicKey: agentHello.nodeEphemeralPublicKey,
          nodeIdentityPublicKey: agentHello.nodeIdentityPublicKey,
          requiresE2EE: false,
          plaintextTestMode: false,
          supportsMultiplexStreams: false,
          supportsFileDownload: false,
          supportsDeviceManagement: false,
        ),
      );
      final transcript = RelayE2eeHandshake.transcript(input);
      final proof = await RelayE2eeHandshake.pairingProof(
        pairingSecret: pending.pairingSecret,
        transcript: transcript,
      );
      final signatureOk = await RelayDeviceIdentityStore.verifyWithPublicKey(
        publicKey: Uint8List.fromList(agentHello.nodeIdentityPublicKey),
        transcript: transcript,
        signature: Uint8List.fromList(agentHello.nodeSignature),
      );
      if (!signatureOk) {
        throw StateError('Relay E2EE 节点签名验证失败');
      }
      final next = pending.copyWith(
        input: input,
        trafficKeys: await RelayE2eeHandshake.deriveTrafficKeys(
          privateScalar: pending.clientEphemeral.privateScalar,
          remotePublicKey:
              Uint8List.fromList(agentHello.nodeEphemeralPublicKey),
          input: input,
        ),
      );
      updateState(next);
      channel.sink.add(jsonEncode(RelayE2eeClientProofFrame(
        sessionId: pending.sessionId,
        clientId: pending.clientId,
        handshakeId: pending.handshakeId,
        kind: relayE2eeHandshakeKindPairing,
        pairingProof: proof,
      ).toJson()));
    } catch (error, stackTrace) {
      if (!complete.isCompleted) {
        complete.completeError(error, stackTrace);
      }
    }
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

  void _queueRelayReceive({
    required WebSocketChannel channel,
    required Map<String, dynamic> frame,
    required int epoch,
    required void Function(String clientId, String clientReconnectSecret)
        setClientId,
  }) {
    _relayReceiveQueue = _relayReceiveQueue.then((_) async {
      if (epoch != _connectionEpoch || _channel != channel) {
        return;
      }
      final relayEvent = await _decodeRelayFrame(frame, setClientId);
      if (relayEvent != null &&
          epoch == _connectionEpoch &&
          _channel == channel) {
        _events.add(_mapper.mapEvent(relayEvent));
      }
    }).catchError((Object error, StackTrace stackTrace) {
      final code = error is StateError && error.message.contains('replay')
          ? 'e2ee_replay_detected'
          : 'e2ee_decrypt_failed';
      _events.add(ErrorEvent(
        timestamp: DateTime.now(),
        sessionId: '',
        runtimeMeta: const RuntimeMeta(),
        raw: <String, dynamic>{
          'type': 'error',
          'code': code,
          'msg': 'Relay E2EE receive failed: $error',
          'stack': stackTrace.toString(),
        },
        code: code,
        message: relayErrorMessage(<String, dynamic>{
          'type': 'relay.error',
          'code': code,
        }),
      ));
      unawaited(disconnect());
    });
  }

  void _queueRelaySend({
    required WebSocketChannel channel,
    required Map<String, dynamic> payload,
    required _RelayContext relayContext,
    required int epoch,
  }) {
    _relaySendQueue = _relaySendQueue.then((_) async {
      if (epoch != _connectionEpoch || _channel != channel) {
        return;
      }
      final frame = await _encodeRelayFrame(payload, relayContext);
      if (epoch != _connectionEpoch || _channel != channel) {
        return;
      }
      channel.sink.add(jsonEncode(frame));
    }).catchError((Object error, StackTrace stackTrace) {
      _events.add(ErrorEvent(
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
      ));
      unawaited(disconnect());
    });
  }

  void _queueRelayDeviceRegister({
    required WebSocketChannel channel,
    required _RelayE2eeHandshakeState state,
    required int epoch,
  }) {
    _relaySendQueue = _relaySendQueue.then((_) async {
      if (epoch != _connectionEpoch || _channel != channel) {
        return;
      }
      final identity = await _relayDeviceIdentityStore.loadOrCreate();
      final credential = await _relayDeviceCredentialStore.loadOrCreate();
      final frame = await state.streamCodec!.encodeJson(
        messageId: 'msg_${const Uuid().v4()}',
        payload: <String, dynamic>{
          'action': 'relay_device_register',
          'deviceId': identity.fullFingerprintHex,
          'displayName': _defaultRelayDeviceName(),
          'deviceIdentityPublicKey':
              encodeRelayFrameBytes(identity.publicKey),
          'deviceCredential': credential.value,
        },
      );
      if (epoch != _connectionEpoch || _channel != channel) {
        return;
      }
      channel.sink.add(jsonEncode(frame));
    }).catchError((Object error, StackTrace stackTrace) {
      _events.add(ErrorEvent(
        timestamp: DateTime.now(),
        sessionId: '',
        runtimeMeta: const RuntimeMeta(),
        raw: <String, dynamic>{
          'type': 'error',
          'code': 'device_unknown',
          'msg': 'Relay device registration failed: $error',
          'stack': stackTrace.toString(),
        },
        code: 'device_unknown',
        message: 'Relay 设备绑定失败：$error',
      ));
      unawaited(disconnect());
    });
  }

  Future<Map<String, dynamic>> _encodeRelayFrame(
    Map<String, dynamic> payload,
    _RelayContext relayContext,
  ) async {
    final codec = _relayE2eeState?.streamCodec;
    if (codec != null) {
      return codec.encodeJson(
        messageId: 'msg_${const Uuid().v4()}',
        payload: payload,
      );
    }
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

bool _requiresPairingE2EE({
  required String relayPairingSecret,
  required RelayE2eeCapabilitySet? capabilities,
}) {
  if (relayPairingSecret.trim().isEmpty || capabilities == null) {
    return false;
  }
  capabilities.validateRelayMode();
  return capabilities.requiresE2EE && !capabilities.plaintextTestMode;
}

void _validateRelayE2EEConnectMode({
  required String relayPairingSecret,
  required String relayClientId,
  required String relayClientReconnectSecret,
  required RelayE2eeCapabilitySet? capabilities,
}) {
  if (capabilities == null) {
    return;
  }
  capabilities.validateRelayMode();
  if (!capabilities.requiresE2EE || capabilities.plaintextTestMode) {
    return;
  }
  if (relayPairingSecret.trim().isNotEmpty) {
    return;
  }
  if (relayClientId.trim().isNotEmpty &&
      relayClientReconnectSecret.trim().isNotEmpty) {
    throw const RelayPairingException(
      'e2ee_unsupported_version',
      'Relay E2EE 重连暂未完成：请重新扫码配对，避免使用明文中继重连',
    );
  }
}

bool _isRelayE2EEFrame(Map<String, dynamic> frame) {
  final type = (frame['type'] ?? '').toString();
  return type == relayFrameAgentE2eeHello || type == relayFrameAgentE2eeResult;
}

String _hex(Uint8List bytes) {
  final buffer = StringBuffer();
  for (final byte in bytes) {
    buffer.write(byte.toRadixString(16).padLeft(2, '0'));
  }
  return buffer.toString();
}

String _defaultRelayDeviceName() => defaultTargetPlatform == TargetPlatform.iOS
    ? 'iPhone'
    : defaultTargetPlatform == TargetPlatform.android
        ? 'Android Device'
        : 'MobileVC Device';

class _RelayE2eeHandshakeState {
  const _RelayE2eeHandshakeState({
    required this.sessionId,
    required this.clientId,
    required this.handshakeId,
    required this.pairingSecret,
    required this.expectedNodeFingerprintHex,
    required this.capabilities,
    required this.clientEphemeral,
    this.input,
    this.trafficKeys,
    this.streamCodec,
    this.complete = false,
  });

  final String sessionId;
  final String clientId;
  final String handshakeId;
  final String pairingSecret;
  final String expectedNodeFingerprintHex;
  final RelayE2eeCapabilitySet capabilities;
  final RelayE2eeEphemeralKeyPair clientEphemeral;
  final RelayE2eeHandshakeInput? input;
  final RelayE2eeTrafficKeys? trafficKeys;
  final RelayMobileVcStreamCodec? streamCodec;
  final bool complete;

  bool matches(String sessionId, String clientId, String handshakeId) {
    return this.sessionId == sessionId &&
        this.clientId == clientId &&
        this.handshakeId == handshakeId;
  }

  _RelayE2eeHandshakeState copyWith({
    RelayE2eeHandshakeInput? input,
    RelayE2eeTrafficKeys? trafficKeys,
  }) {
    return _RelayE2eeHandshakeState(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      pairingSecret: pairingSecret,
      expectedNodeFingerprintHex: expectedNodeFingerprintHex,
      capabilities: capabilities,
      clientEphemeral: clientEphemeral,
      input: input ?? this.input,
      trafficKeys: trafficKeys ?? this.trafficKeys,
      streamCodec: streamCodec,
      complete: complete,
    );
  }

  _RelayE2eeHandshakeState markComplete() {
    final keys = trafficKeys;
    if (keys == null) {
      throw StateError('Relay E2EE traffic keys missing');
    }
    return _RelayE2eeHandshakeState(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      pairingSecret: pairingSecret,
      expectedNodeFingerprintHex: expectedNodeFingerprintHex,
      capabilities: capabilities,
      clientEphemeral: clientEphemeral,
      input: input,
      trafficKeys: keys,
      streamCodec: RelayMobileVcStreamCodec.client(
        sessionId: sessionId,
        clientId: clientId,
        handshakeId: handshakeId,
        keys: keys,
      ),
      complete: true,
    );
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
