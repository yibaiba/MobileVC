import 'relay_e2ee_crypto.dart';
import 'relay_e2ee_handshake.dart';

class RelayE2eeCapabilitySet {
  static const supportedRelayProtocolVersion = 1;
  static const supportedTunnelProtocolVersion = 1;

  const RelayE2eeCapabilitySet({
    required this.relayProtocolVersion,
    required this.e2eeProtocolVersion,
    required this.cryptoSuite,
    required this.tunnelProtocolVersion,
    required this.supportsMultiplexStreams,
    required this.supportsFileDownload,
    required this.supportsDeviceManagement,
    required this.requiresE2EE,
    required this.plaintextTestMode,
  });

  factory RelayE2eeCapabilitySet.production() {
    return const RelayE2eeCapabilitySet(
      relayProtocolVersion: supportedRelayProtocolVersion,
      e2eeProtocolVersion: relayE2eeVersion,
      cryptoSuite: relayE2eeSuite,
      tunnelProtocolVersion: supportedTunnelProtocolVersion,
      supportsMultiplexStreams: true,
      supportsFileDownload: true,
      supportsDeviceManagement: true,
      requiresE2EE: true,
      plaintextTestMode: false,
    );
  }

  factory RelayE2eeCapabilitySet.plaintextTestMode() {
    return const RelayE2eeCapabilitySet(
      relayProtocolVersion: supportedRelayProtocolVersion,
      e2eeProtocolVersion: relayE2eeVersion,
      cryptoSuite: relayE2eeSuite,
      tunnelProtocolVersion: supportedTunnelProtocolVersion,
      supportsMultiplexStreams: true,
      supportsFileDownload: true,
      supportsDeviceManagement: true,
      requiresE2EE: false,
      plaintextTestMode: true,
    );
  }

  final int relayProtocolVersion;
  final int e2eeProtocolVersion;
  final String cryptoSuite;
  final int tunnelProtocolVersion;
  final bool supportsMultiplexStreams;
  final bool supportsFileDownload;
  final bool supportsDeviceManagement;
  final bool requiresE2EE;
  final bool plaintextTestMode;

  void validateProduction() {
    _validateVersions();
    if (!requiresE2EE || plaintextTestMode) {
      throw ArgumentError('E2EE production mode required');
    }
    if (!_supportsRequiredTunnelFeatures) {
      throw ArgumentError('missing required E2EE tunnel capability');
    }
  }

  void validatePlaintextTestMode() {
    _validateVersions();
    if (requiresE2EE || !plaintextTestMode) {
      throw ArgumentError('plaintext test mode must be explicit');
    }
  }

  RelayE2eeHandshakeInput applyToHandshake(RelayE2eeHandshakeInput input) {
    return RelayE2eeHandshakeInput(
      kind: input.kind,
      sessionId: input.sessionId,
      clientId: input.clientId,
      handshakeId: input.handshakeId,
      relayProtocolVersion: relayProtocolVersion,
      e2eeProtocolVersion: e2eeProtocolVersion,
      tunnelProtocolVersion: tunnelProtocolVersion,
      cryptoSuite: cryptoSuite,
      clientEphemeralPublicKey: input.clientEphemeralPublicKey,
      nodeEphemeralPublicKey: input.nodeEphemeralPublicKey,
      nodeIdentityPublicKey: input.nodeIdentityPublicKey,
      deviceIdentityPublicKey: input.deviceIdentityPublicKey,
      requiresE2EE: requiresE2EE,
      plaintextTestMode: plaintextTestMode,
      supportsMultiplexStreams: supportsMultiplexStreams,
      supportsFileDownload: supportsFileDownload,
      supportsDeviceManagement: supportsDeviceManagement,
    );
  }

  Map<String, Object> toJson() {
    return <String, Object>{
      'relayProtocolVersion': relayProtocolVersion,
      'e2eeProtocolVersion': e2eeProtocolVersion,
      'cryptoSuite': cryptoSuite,
      'tunnelProtocolVersion': tunnelProtocolVersion,
      'supportsMultiplexStreams': supportsMultiplexStreams,
      'supportsFileDownloadStream': supportsFileDownload,
      'supportsDeviceManagement': supportsDeviceManagement,
      'requiresE2EE': requiresE2EE,
      'plaintextTestMode': plaintextTestMode,
    };
  }

  bool get _supportsRequiredTunnelFeatures =>
      supportsMultiplexStreams &&
      supportsFileDownload &&
      supportsDeviceManagement;

  void _validateVersions() {
    if (relayProtocolVersion != supportedRelayProtocolVersion ||
        e2eeProtocolVersion != relayE2eeVersion ||
        tunnelProtocolVersion != supportedTunnelProtocolVersion ||
        cryptoSuite != relayE2eeSuite) {
      throw ArgumentError('unsupported E2EE capability version');
    }
  }
}
