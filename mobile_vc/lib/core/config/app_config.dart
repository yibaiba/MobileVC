import 'adb_ice_config.dart';
import 'app_config_engine_models.dart';
import 'app_connection_endpoint.dart';
import 'app_connection_environment.dart';
import 'app_connection_urls.dart';
import 'relay_config.dart';

class AppConfig {
  static const String adbIcePort = AdbIceConfig.port;

  const AppConfig({
    this.host = 'localhost',
    this.port = '19080',
    this.token = 'test',
    this.cwd = '.',
    this.engine = 'claude',
    this.model = '',
    this.reasoningEffort = '',
    this.claudeModel = '',
    this.codexModel = '',
    this.codexReasoningEffort = '',
    this.permissionMode = 'auto',
    this.fastMode = false,
    this.adbIceServersJson = '',
    this.secureTransport,
    this.connectionMode = 'direct',
    this.relayUrl = '',
    this.relaySessionId = '',
    this.relayPairingSecret = '',
    this.relayPairingExpiresAt = 0,
    this.relayClientId = '',
    this.relayClientReconnectSecret = '',
  });

  final String host;
  final String port;
  final String token;
  final String cwd;
  final String engine;
  final String model;
  final String reasoningEffort;
  final String claudeModel;
  final String codexModel;
  final String codexReasoningEffort;
  final String permissionMode;
  final bool fastMode;
  final String adbIceServersJson;
  final bool? secureTransport;
  final String connectionMode;
  final String relayUrl;
  final String relaySessionId;
  final String relayPairingSecret;
  final int relayPairingExpiresAt;
  final String relayClientId;
  final String relayClientReconnectSecret;

  bool get isRelayMode => connectionMode == ConnectionMode.relay.name;

  String get baseHttpUrl => baseHttpUrlFor();

  String get wsUrl => wsUrlFor();

  String get displayEndpoint => _connectionUrls(null).displayEndpoint;

  String get displayHost => _connectionUrls(null).displayHost;

  String baseHttpUrlFor({bool? secureTransport}) =>
      _connectionUrls(secureTransport).baseHttpUrl;

  String wsUrlFor({bool? secureTransport}) =>
      _connectionUrls(secureTransport).wsUrl;

  String get adbIceUsername => _adbIceConfig.settings.username;

  String get adbIceCredential => _adbIceConfig.settings.credential;

  String get adbIceHostOverride => _adbIceConfig.settings.host;

  bool get hasAutoAdbIceConfig => _adbIceConfig.settings.isAuto;

  bool get hasTurnAdbIceServer => _adbIceConfig.hasTurnServer;

  bool get shouldForceAdbRelay => _adbIceConfig.shouldForceRelay;

  List<Map<String, dynamic>> get adbIceServers => _adbIceConfig.servers;

  static String encodeAutoAdbIceConfig({
    String host = '',
    required String username,
    required String credential,
  }) {
    return AdbIceConfig.encodeAuto(
      host: host,
      username: username,
      credential: credential,
    );
  }

  Uri downloadUri(String path, {bool? secureTransport}) =>
      _connectionUrls(secureTransport).downloadUri(path);

  AppConfig copyWith({
    String? host,
    String? port,
    String? token,
    String? cwd,
    String? engine,
    String? model,
    String? reasoningEffort,
    String? claudeModel,
    String? codexModel,
    String? codexReasoningEffort,
    String? permissionMode,
    bool? fastMode,
    String? adbIceServersJson,
    bool? secureTransport,
    String? connectionMode,
    String? relayUrl,
    String? relaySessionId,
    String? relayPairingSecret,
    int? relayPairingExpiresAt,
    String? relayClientId,
    String? relayClientReconnectSecret,
  }) {
    final nextEngine = engine ?? this.engine;
    final nextModels = AppConfigEngineModels.resolve(
      nextEngine: nextEngine,
      model: model,
      reasoningEffort: reasoningEffort,
      currentClaudeModel: this.claudeModel,
      currentCodexModel: this.codexModel,
      currentCodexReasoningEffort: this.codexReasoningEffort,
      claudeModel: claudeModel,
      codexModel: codexModel,
      codexReasoningEffort: codexReasoningEffort,
    );
    final endpoint = AppConnectionEndpoint.parse(
      host ?? this.host,
      fallbackPort: port ?? this.port,
    );
    return AppConfig(
      host: endpoint.host,
      port: endpoint.port,
      token: token ?? this.token,
      cwd: cwd ?? this.cwd,
      engine: nextEngine,
      model: model ?? this.model,
      reasoningEffort: reasoningEffort ?? this.reasoningEffort,
      claudeModel: nextModels.claudeModel,
      codexModel: nextModels.codexModel,
      codexReasoningEffort: nextModels.codexReasoningEffort,
      permissionMode: _normalizePermissionMode(
        permissionMode ?? this.permissionMode,
      ),
      fastMode: fastMode ?? this.fastMode,
      adbIceServersJson: adbIceServersJson ?? this.adbIceServersJson,
      secureTransport:
          secureTransport ?? endpoint.secureTransport ?? this.secureTransport,
      connectionMode:
          normalizeConnectionMode(connectionMode ?? this.connectionMode),
      relayUrl: relayUrl ?? this.relayUrl,
      relaySessionId: relaySessionId ?? this.relaySessionId,
      relayPairingSecret: relayPairingSecret ?? this.relayPairingSecret,
      relayPairingExpiresAt:
          relayPairingExpiresAt ?? this.relayPairingExpiresAt,
      relayClientId: relayClientId ?? this.relayClientId,
      relayClientReconnectSecret:
          relayClientReconnectSecret ?? this.relayClientReconnectSecret,
    );
  }

  String modelForEngine(String targetEngine) {
    switch (targetEngine.trim().toLowerCase()) {
      case 'codex':
        if (codexModel.trim().isNotEmpty) {
          return codexModel.trim();
        }
        if (engine.trim().toLowerCase() == 'codex') {
          return model.trim();
        }
        return '';
      case 'claude':
        if (claudeModel.trim().isNotEmpty) {
          return claudeModel.trim();
        }
        if (engine.trim().toLowerCase() == 'claude') {
          return model.trim();
        }
        return '';
      default:
        return model.trim();
    }
  }

  String reasoningEffortForEngine(String targetEngine) {
    if (targetEngine.trim().toLowerCase() != 'codex') {
      return '';
    }
    if (codexReasoningEffort.trim().isNotEmpty) {
      return codexReasoningEffort.trim();
    }
    if (engine.trim().toLowerCase() == 'codex') {
      return reasoningEffort.trim();
    }
    return '';
  }

  Map<String, Object> toJson() => {
        'host': host,
        'port': port,
        'token': token,
        'cwd': cwd,
        'engine': engine,
        'model': model,
        'reasoningEffort': reasoningEffort,
        'claudeModel': claudeModel,
        'codexModel': codexModel,
        'codexReasoningEffort': codexReasoningEffort,
        'permissionMode': permissionMode,
        'fastMode': fastMode,
        'adbIceServersJson': adbIceServersJson,
        'connectionMode': connectionMode,
        if (relayUrl.trim().isNotEmpty) 'relayUrl': relayUrl,
        if (relaySessionId.trim().isNotEmpty) 'relaySessionId': relaySessionId,
        if (relayClientId.trim().isNotEmpty) 'relayClientId': relayClientId,
        if (relayClientReconnectSecret.trim().isNotEmpty)
          'relayClientReconnectSecret': relayClientReconnectSecret,
        if (secureTransport != null) 'secureTransport': secureTransport!,
      };

  factory AppConfig.fromJson(Map<String, Object?> json) {
    final engine = (json['engine'] ?? 'claude').toString();
    final legacyModel = (json['model'] ?? '').toString();
    final legacyReasoningEffort = (json['reasoningEffort'] ?? '').toString();
    final endpoint = AppConnectionEndpoint.parse(
      (json['host'] ?? 'localhost').toString(),
      fallbackPort: (json['port'] ?? '19080').toString(),
    );
    return AppConfig(
      host: endpoint.host,
      port: endpoint.port,
      token: (json['token'] ?? 'test').toString(),
      cwd: (json['cwd'] ?? '.').toString(),
      engine: engine,
      model: legacyModel,
      reasoningEffort: legacyReasoningEffort,
      claudeModel: (json['claudeModel'] ??
              (engine.trim().toLowerCase() == 'claude' ? legacyModel : ''))
          .toString(),
      codexModel: (json['codexModel'] ??
              (engine.trim().toLowerCase() == 'codex' ? legacyModel : ''))
          .toString(),
      codexReasoningEffort: (json['codexReasoningEffort'] ??
              (engine.trim().toLowerCase() == 'codex'
                  ? legacyReasoningEffort
                  : ''))
          .toString(),
      permissionMode: _normalizePermissionMode(
        (json['permissionMode'] ?? 'auto').toString(),
      ),
      fastMode: json['fastMode'] == true,
      adbIceServersJson: (json['adbIceServersJson'] ?? '').toString(),
      secureTransport: endpoint.secureTransport ??
          parseSecureTransport(json['secureTransport']),
      connectionMode: normalizeConnectionMode(json['connectionMode']),
      relayUrl: (json['relayUrl'] ?? '').toString(),
      relaySessionId: (json['relaySessionId'] ?? '').toString(),
      relayClientId: (json['relayClientId'] ?? '').toString(),
      relayClientReconnectSecret:
          (json['relayClientReconnectSecret'] ?? '').toString(),
    );
  }

  static String _normalizePermissionMode(String value) {
    switch (value.trim()) {
      case 'bypassPermissions':
        return 'bypassPermissions';
      case 'default':
        return 'default';
      default:
        return 'auto';
    }
  }

  static AppConfig? fromLaunchUri(
    String raw, {
    AppConfig fallback = const AppConfig(),
  }) {
    final trimmed = raw.trim();
    if (trimmed.isEmpty) {
      return null;
    }
    final relayPairing = parseRelayPairingUri(trimmed);
    if (relayPairing != null) {
      return fallback.copyWith(
        connectionMode: ConnectionMode.relay.name,
        relayUrl: relayPairing.relayUrl,
        relaySessionId: relayPairing.sessionId,
        relayPairingSecret: relayPairing.pairingSecret,
        relayPairingExpiresAt: relayPairing.expiresAt,
        relayClientId: '',
        relayClientReconnectSecret: '',
      );
    }
    final uri = Uri.tryParse(trimmed);
    if (uri == null || uri.host.trim().isEmpty) {
      return null;
    }
    final port = _launchUriPort(uri, fallback.port);
    final token = (uri.queryParameters['token'] ?? fallback.token).trim();
    final ice =
        (uri.queryParameters['ice'] ?? fallback.adbIceServersJson).trim();
    final cwd = (uri.queryParameters['cwd'] ?? fallback.cwd).trim();
    return fallback.copyWith(
      host: uri.host.trim(),
      port: port,
      token: token,
      cwd: cwd,
      adbIceServersJson: ice,
      secureTransport: secureTransportFromScheme(uri.scheme),
    );
  }

  AppConnectionUrls _connectionUrls(bool? secureTransport) {
    final endpoint = AppConnectionEndpoint.parse(
      host,
      fallbackPort: port,
    );
    final effectiveSecureTransport = secureTransport ??
        (defaultSecureBackendTransport
            ? true
            : endpoint.secureTransport ?? this.secureTransport ?? false);
    return AppConnectionUrls(
      host: endpoint.host,
      port: endpoint.port,
      token: token,
      secureTransport: effectiveSecureTransport,
    );
  }

  AdbIceConfig get _adbIceConfig =>
      AdbIceConfig(host: host, rawJson: adbIceServersJson);
}

String _launchUriPort(Uri uri, String fallbackPort) {
  if (uri.hasPort && uri.port > 0) {
    return uri.port.toString();
  }
  return secureTransportFromScheme(uri.scheme) == null ? fallbackPort : '';
}
