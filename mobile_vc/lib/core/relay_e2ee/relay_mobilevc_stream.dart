import 'dart:convert';
import 'dart:typed_data';

import 'relay_e2ee_crypto.dart';
import 'relay_e2ee_handshake.dart';
import 'relay_tunnel.dart';

const relayMobileVcStreamId = 1;
const relayMobileVcStreamType = tunnelStreamMobileVcWs;
const relayForwardType = 'relay.forward';
const relayForwardContentTypeMobileVc = 'mobilevc.ws.v1';
const relayForwardPayloadBase64Url = 'base64url';

class RelayMobileVcStreamCodec {
  RelayMobileVcStreamCodec({
    required this.sessionId,
    required this.clientId,
    required this.handshakeId,
    required this.sendKey,
    required this.receiveKey,
    required this.sendDirection,
  }) : receiveDirection = _oppositeDirection(sendDirection) {
    if (sendKey.length != relayE2eeKeyLength ||
        receiveKey.length != relayE2eeKeyLength) {
      throw ArgumentError('invalid E2EE stream key length');
    }
    if (sessionId.trim().isEmpty ||
        clientId.trim().isEmpty ||
        handshakeId.trim().isEmpty) {
      throw ArgumentError('session, client, and handshake ids are required');
    }
  }

  factory RelayMobileVcStreamCodec.client({
    required String sessionId,
    required String clientId,
    required String handshakeId,
    required RelayE2eeTrafficKeys keys,
  }) {
    return RelayMobileVcStreamCodec(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      sendKey: keys.clientToAgent,
      receiveKey: keys.agentToClient,
      sendDirection: relayE2eeDirectionClientToAgent,
    );
  }

  factory RelayMobileVcStreamCodec.agent({
    required String sessionId,
    required String clientId,
    required String handshakeId,
    required RelayE2eeTrafficKeys keys,
  }) {
    return RelayMobileVcStreamCodec(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      sendKey: keys.agentToClient,
      receiveKey: keys.clientToAgent,
      sendDirection: relayE2eeDirectionAgentToClient,
    );
  }

  final String sessionId;
  final String clientId;
  final String handshakeId;
  final Uint8List sendKey;
  final Uint8List receiveKey;
  final String sendDirection;
  final String receiveDirection;
  var _sendCounter = 0;
  final Set<int> _seenCounters = <int>{};

  Future<Map<String, dynamic>> encodeJson({
    required String messageId,
    required Map<String, dynamic> payload,
  }) async {
    return encode(
      messageId: messageId,
      plaintext: Uint8List.fromList(utf8.encode(jsonEncode(payload))),
    );
  }

  Future<Map<String, dynamic>> encode({
    required String messageId,
    required Uint8List plaintext,
  }) async {
    if (messageId.trim().isEmpty) {
      throw ArgumentError('message id is required');
    }
    final counter = _sendCounter;
    final sealed = await RelayE2eeCrypto.encrypt(
      key: sendKey,
      plaintext: plaintext,
      context: _frameContext(sendDirection, counter),
    );
    _sendCounter++;
    return <String, dynamic>{
      'type': relayForwardType,
      'version': 1,
      'sessionId': sessionId,
      'clientId': clientId,
      'direction': sendDirection,
      'messageId': messageId,
      'contentType': relayForwardContentTypeMobileVc,
      'encryption': relayE2eeSuite,
      'payloadEncoding': relayForwardPayloadBase64Url,
      'payload': base64Url.encode(sealed).replaceAll('=', ''),
      'streamId': relayMobileVcStreamId,
      'counter': counter,
      'handshakeId': handshakeId,
    };
  }

  Future<Map<String, dynamic>> decodeJson(Map<String, dynamic> frame) async {
    final plaintext = await decode(frame);
    final decoded = jsonDecode(utf8.decode(plaintext));
    if (decoded is! Map<String, dynamic>) {
      throw const FormatException(
          'MobileVC relay payload is not a JSON object');
    }
    return decoded;
  }

  Future<Uint8List> decode(Map<String, dynamic> frame) async {
    _validateFrame(frame);
    final counter = _intField(frame, 'counter');
    if (_seenCounters.contains(counter)) {
      throw StateError('e2ee replay detected');
    }
    final sealed = Uint8List.fromList(
      base64Url.decode(base64Url.normalize(frame['payload'].toString())),
    );
    final plaintext = await RelayE2eeCrypto.decrypt(
      key: receiveKey,
      sealed: sealed,
      context: _frameContext(receiveDirection, counter),
    );
    _seenCounters.add(counter);
    return plaintext;
  }

  RelayE2eeFrameContext _frameContext(String direction, int counter) {
    return RelayE2eeFrameContext(
      sessionId: sessionId,
      clientId: clientId,
      handshakeId: handshakeId,
      direction: direction,
      streamId: relayMobileVcStreamId,
      counter: counter,
    );
  }

  void _validateFrame(Map<String, dynamic> frame) {
    final valid = frame['type'] == relayForwardType &&
        frame['version'] == 1 &&
        frame['sessionId'] == sessionId &&
        frame['clientId'] == clientId &&
        frame['direction'] == receiveDirection &&
        frame['contentType'] == relayForwardContentTypeMobileVc &&
        frame['encryption'] == relayE2eeSuite &&
        frame['payloadEncoding'] == relayForwardPayloadBase64Url &&
        frame['handshakeId'] == handshakeId &&
        _intField(frame, 'streamId') == relayMobileVcStreamId &&
        frame['messageId'].toString().trim().isNotEmpty &&
        frame['payload'].toString().trim().isNotEmpty;
    if (!valid) {
      throw const FormatException('invalid MobileVC E2EE relay frame');
    }
  }

  static int _intField(Map<String, dynamic> frame, String key) {
    final value = frame[key];
    if (value is int) {
      return value;
    }
    if (value is num) {
      return value.toInt();
    }
    return int.parse(value.toString());
  }

  static String _oppositeDirection(String direction) {
    return switch (direction) {
      relayE2eeDirectionClientToAgent => relayE2eeDirectionAgentToClient,
      relayE2eeDirectionAgentToClient => relayE2eeDirectionClientToAgent,
      _ =>
        throw ArgumentError.value(direction, 'direction', 'invalid direction'),
    };
  }
}
