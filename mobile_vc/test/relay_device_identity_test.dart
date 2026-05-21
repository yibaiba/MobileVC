import 'dart:math';
import 'dart:typed_data';

import 'package:flutter_test/flutter_test.dart';
import 'package:mobile_vc/core/relay_e2ee/relay_device_identity.dart';

void main() {
  test('loadOrCreate persists the same private identity in secure store',
      () async {
    final store = MemoryRelaySecureStore();
    final identities = RelayDeviceIdentityStore(
      secureStore: store,
      random: Random(7),
    );

    final first = await identities.loadOrCreate();
    final second = await identities.loadOrCreate();

    expect(second.privateScalar, first.privateScalar);
    expect(second.publicKey, first.publicKey);
    expect(second.fingerprint, first.fingerprint);
    expect(first.publicKey, hasLength(65));
    expect(first.fingerprint, hasLength(32));
    expect(first.shortFingerprint.split('-'), hasLength(5));
    expect(store.values.keys, ['mobilevc.relay.device_identity.v1']);
  });

  test('reset deletes the persisted device identity', () async {
    final store = MemoryRelaySecureStore();
    final identities = RelayDeviceIdentityStore(
      secureStore: store,
      random: Random(11),
    );

    final first = await identities.loadOrCreate();
    await identities.reset();
    final second = await identities.loadOrCreate();

    expect(second.privateScalar, isNot(first.privateScalar));
  });

  test('device identity signs and verifies handshake transcripts', () async {
    final identities = RelayDeviceIdentityStore(
      secureStore: MemoryRelaySecureStore(),
      random: Random(13),
    );
    final identity = await identities.loadOrCreate();
    final transcript = Uint8List.fromList(
      'mobilevc relay device handshake transcript'.codeUnits,
    );

    final signature = await identities.signTranscript(
      identity: identity,
      transcript: transcript,
    );

    final verified = await identities.verifyTranscriptSignature(
      publicKey: identity.publicKey,
      transcript: transcript,
      signature: signature,
    );
    expect(verified, isTrue);

    final tampered = await identities.verifyTranscriptSignature(
      publicKey: identity.publicKey,
      transcript: Uint8List.fromList('tampered'.codeUnits),
      signature: signature,
    );
    expect(tampered, isFalse);
  });
}

class MemoryRelaySecureStore implements RelaySecureStore {
  final Map<String, String> values = <String, String>{};

  @override
  Future<String?> read(String key) async => values[key];

  @override
  Future<void> write(String key, String value) async {
    values[key] = value;
  }

  @override
  Future<void> delete(String key) async {
    values.remove(key);
  }
}
