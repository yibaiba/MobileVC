import '../relay_e2ee/relay_e2ee_capability.dart';

enum ConnectionMode { direct, relay }

class RelayPairing {
  const RelayPairing({
    required this.relayUrl,
    required this.sessionId,
    required this.pairingSecret,
    required this.expiresAt,
    this.capabilities,
  });

  final String relayUrl;
  final String sessionId;
  final String pairingSecret;
  final int expiresAt;
  final RelayE2eeCapabilitySet? capabilities;
}

String normalizeConnectionMode(Object? value) {
  return value?.toString().trim() == ConnectionMode.relay.name
      ? ConnectionMode.relay.name
      : ConnectionMode.direct.name;
}

void validateRelayUrl(String raw) {
  final uri = Uri.parse(raw.trim());
  if (uri.scheme == 'wss') {
    _validateRelayUrlShape(uri);
    return;
  }
  if (uri.scheme != 'ws') {
    throw const FormatException('relay url must use ws:// or wss://');
  }
  _validateRelayUrlShape(uri);
  if (!_isDevelopmentRelayHost(uri.host)) {
    throw const FormatException(
      'ws:// relay urls are allowed only for loopback or LAN hosts',
    );
  }
}

RelayPairing? parseRelayPairingUri(String raw) {
  final uri = Uri.tryParse(raw.trim());
  if (uri == null || uri.scheme != 'mobilevc') {
    return null;
  }
  if (uri.host != 'relay' || uri.path != '/v1') {
    return null;
  }
  final relayUrl = (uri.queryParameters['relay'] ?? '').trim();
  final sessionId = (uri.queryParameters['session'] ?? '').trim();
  final secret = (uri.queryParameters['secret'] ?? '').trim();
  final expiresAt = int.tryParse(uri.queryParameters['exp'] ?? '');
  final capabilities = _parseRelayCapabilities(uri);
  if (relayUrl.isEmpty || sessionId.isEmpty || secret.isEmpty) {
    throw const FormatException('relay pairing uri is missing fields');
  }
  validateRelayUrl(relayUrl);
  return RelayPairing(
    relayUrl: relayUrl,
    sessionId: sessionId,
    pairingSecret: secret,
    expiresAt: expiresAt ?? 0,
    capabilities: capabilities,
  );
}

RelayE2eeCapabilitySet? _parseRelayCapabilities(Uri uri) {
  final hasCapabilities = _relayCapabilityQueryKeys.any(
    uri.queryParameters.containsKey,
  );
  if (!hasCapabilities) {
    return null;
  }
  final capabilities = RelayE2eeCapabilitySet(
    relayProtocolVersion: _requiredInt(uri, 'relayProtocolVersion'),
    e2eeProtocolVersion: _requiredInt(uri, 'e2eeProtocolVersion'),
    cryptoSuite: _requiredString(uri, 'cryptoSuite'),
    tunnelProtocolVersion: _requiredInt(uri, 'tunnelProtocolVersion'),
    supportsMultiplexStreams: _requiredBool(
      uri,
      'supportsMultiplexStreams',
    ),
    supportsFileDownload: _requiredBool(uri, 'supportsFileDownloadStream'),
    supportsDeviceManagement: _requiredBool(
      uri,
      'supportsDeviceManagement',
    ),
    requiresE2EE: _requiredBool(uri, 'requiresE2EE'),
    plaintextTestMode: _requiredBool(uri, 'plaintextTestMode'),
  );
  if (capabilities.plaintextTestMode) {
    capabilities.validatePlaintextTestMode();
  } else {
    capabilities.validateProduction();
  }
  return capabilities;
}

const _relayCapabilityQueryKeys = <String>{
  'relayProtocolVersion',
  'e2eeProtocolVersion',
  'cryptoSuite',
  'tunnelProtocolVersion',
  'supportsMultiplexStreams',
  'supportsFileDownloadStream',
  'supportsDeviceManagement',
  'requiresE2EE',
  'plaintextTestMode',
};

String _requiredString(Uri uri, String key) {
  final value = (uri.queryParameters[key] ?? '').trim();
  if (value.isEmpty) {
    throw FormatException('relay pairing uri is missing capability field $key');
  }
  return value;
}

int _requiredInt(Uri uri, String key) {
  final value = int.tryParse(_requiredString(uri, key));
  if (value == null) {
    throw FormatException(
        'relay pairing uri has invalid capability field $key');
  }
  return value;
}

bool _requiredBool(Uri uri, String key) {
  final value = _requiredString(uri, key).toLowerCase();
  return switch (value) {
    'true' => true,
    'false' => false,
    _ => throw FormatException(
        'relay pairing uri has invalid capability field $key',
      ),
  };
}

void _validateRelayUrlShape(Uri uri) {
  if (uri.host.trim().isEmpty || uri.userInfo.trim().isNotEmpty) {
    throw const FormatException('invalid relay url');
  }
  if (uri.path.isNotEmpty && uri.path != '/') {
    throw const FormatException('relay url path is not allowed');
  }
  if (uri.hasQuery || uri.hasFragment) {
    throw const FormatException('relay url query and fragment are not allowed');
  }
}

bool _isDevelopmentRelayHost(String host) {
  final normalized = host.trim().toLowerCase();
  if (normalized == 'localhost') {
    return true;
  }
  if (normalized.contains('.')) {
    return _isPrivateIPv4(normalized);
  }
  return normalized == '::1' ||
      normalized.startsWith('fc') ||
      normalized.startsWith('fd') ||
      normalized.startsWith('fe80:');
}

bool _isPrivateIPv4(String host) {
  final parts = host.split('.').map(int.tryParse).toList();
  if (parts.length != 4 || parts.any((part) => part == null)) {
    return false;
  }
  final first = parts[0]!;
  final second = parts[1]!;
  return first == 10 ||
      first == 127 ||
      (first == 169 && second == 254) ||
      (first == 192 && second == 168) ||
      (first == 172 && second >= 16 && second <= 31);
}
