# Quality Guidelines

> Code quality standards for frontend development.

---

## Overview

<!--
Document your project's quality standards here.

Questions to answer:
- What patterns are forbidden?
- What linting rules do you enforce?
- What are your testing requirements?
- What code review standards apply?
-->

(To be filled by the team)

---

## Forbidden Patterns

<!-- Patterns that should never be used and why -->

(To be filled by the team)

---

## Required Patterns

### Scenario: Flutter Web Backend URL Scheme Derivation

#### 1. Scope / Trigger
- Trigger: any change that constructs backend HTTP, download, or WebSocket URLs from `AppConfig`.
- Applies to: Flutter native, Flutter Web over HTTP, and Flutter Web over HTTPS.

#### 2. Signatures
- `AppConfig.baseHttpUrlFor({bool? secureTransport}) -> String`
- `AppConfig.wsUrlFor({bool? secureTransport}) -> String`
- `AppConfig.downloadUri(String path, {bool? secureTransport}) -> Uri`

#### 3. Contracts
- `secureTransport == false` derives `http://...` and `ws://...`.
- `secureTransport == true` derives `https://...` and `wss://...`.
- `secureTransport == null` uses the runtime default; Flutter Web loaded from `https://` must derive secure transport before `WebSocketChannel.connect(...)` is called.
- Persisted config remains host/port/token/cwd focused; callers must not store a generated `wsUrl` string as source-of-truth state.

#### 4. Validation & Error Matrix
- Empty host -> `FormatException('host is required')`.
- Non-numeric or non-positive port -> `FormatException('invalid port: <value>')`.
- HTTPS page plus `ws://` URL -> browser `SecurityError`; this is a bug in URL derivation, not a backend retry case.

#### 5. Good/Base/Bad Cases
- Good: HTTPS Flutter Web calls `AppConfig.wsUrlFor()` and gets `wss://host:port/ws?token=...`.
- Base: local native or HTTP web calls `AppConfig.wsUrlFor()` and gets `ws://host:port/ws?token=...`.
- Bad: hand-building `ws://$host:$port/ws?token=$token` outside the URL helper.

#### 6. Tests Required
- Assert plain transport produces `http` and `ws`.
- Assert secure transport produces `https` and `wss`.
- Assert download URLs use the same HTTP scheme as the WebSocket scheme pair.
- Assert invalid ports surface as `FormatException`.

#### 7. Wrong vs Correct

Wrong:

```dart
final wsUrl = 'ws://$host:$port/ws?token=$token';
```

Correct:

```dart
final wsUrl = config.wsUrlFor();
```

### Scenario: Flutter Relay Pairing Config

#### 1. Scope / Trigger
- Trigger: any Flutter change that parses relay QR URIs, stores relay config, connects relay WebSockets, or handles file actions in relay mode.
- Applies to `AppConfig`, `relay_config.dart`, `MobileVcWsService`, and `SessionController`.

#### 2. Signatures
- `AppConfig.fromLaunchUri(String raw, {AppConfig fallback}) -> AppConfig?`
- `validateRelayUrl(String raw) -> void`
- `MobileVcWsService.connectRelay({required relayUrl, required sessionId, required pairingSecret})`
- Relay E2EE connect metadata: `nodeFingerprintHex`, `relayCapabilities`
- Relay QR: `mobilevc://relay/v1?relay=<url>&session=<id>&secret=<secret>&exp=<unix-seconds>`

#### 3. Contracts
- Parse `mobilevc://relay/v1` before direct launch URI parsing.
- Relay scan selects `ConnectionMode.relay` without overwriting direct `host`, `port`, or `token`.
- Persist only `connectionMode` and `relayUrl`; never persist `relaySessionId`, `relayPairingSecret`, or `relayPairingExpiresAt`.
- Relay connect must wait for `client.paired`; pairing `relay.error` is a connection failure, not a successful connection.
- Production E2EE relay pairing must not consider `client.paired` alone as connected. When the pairing link capabilities require E2EE and plaintext test-mode is off, Flutter must complete `client.e2ee_hello` -> `agent.e2ee_hello` -> `client.e2ee_proof` -> `agent.e2ee_result ok` before `connectRelay` returns.
- Production E2EE relay reconnect must perform a reconnect E2EE handshake with device identity and device credential before any `relay.forward` business payload is sent. Until that reconnect rekey flow is wired, Flutter must fail persisted reconnect attempts before opening the websocket with an actionable E2EE error and require a fresh pairing scan instead of falling back to plaintext.
- Plaintext relay test-mode must remain explicit. If capability hints are absent or `plaintextTestMode=true`, Flutter must not send E2EE handshake frames because older/local test relay agents may not have the local E2EE handler wired.
- Pairing-link capability hints are non-secret metadata and may be persisted as `relayCapabilities`; pairing secret and expiry remain non-persistent.
- Production E2EE pairing must verify that the node identity public key fingerprint from `agent.e2ee_hello` matches `AppConfig.relayNodeFingerprintHex` before sending `client.e2ee_proof`.
- `client.e2ee_proof` must be sent only after node signature verification and transcript-bound pairing proof generation. Do not complete pairing on `agent.e2ee_hello`; wait for `agent.e2ee_result ok`.
- A completed Flutter pairing handshake must create and retain one `RelayMobileVcStreamCodec.client(...)` for the active relay connection. Relay `send()` must encrypt MobileVC business JSON through that codec after E2EE completion; inbound encrypted `relay.forward` must decrypt through the same codec before mapping to `AppEvent`.
- After pairing E2EE completes, Flutter must register its device identity and device credential through an encrypted `relay.forward` business action (`relay_device_register`). Device credentials must stay in secure storage and must never be sent in handshake control frames, pairing links, plaintext relay frames, or logs.
- Flutter relay send and receive paths must serialize async E2EE stream operations per connection. AES-GCM counters and replay state are part of the codec state; do not create a fresh codec per frame, and do not run inbound decrypts concurrently when MobileVC event ordering matters.
- After E2EE completion, Flutter must reject plaintext `relay.forward` frames explicitly and must not retry/decode them as plaintext. Decrypt failure and replay detection must surface as E2EE errors and disconnect the relay channel instead of silently falling back.
- Relay mode must not send direct backend `AUTH_TOKEN` in relay envelopes or control frames.
- HTTP `/download` is disabled in relay mode and must show `Relay 模式暂不支持下载`.

#### 4. Validation & Error Matrix
- `http://` or `https://` relay URL -> `FormatException`.
- Public-host `ws://` relay URL -> `FormatException`.
- Missing relay URL/session/secret before connect -> `FormatException`.
- `relay.error` during pairing -> connection failure and channel close.

#### 5. Good/Base/Bad Cases
- Good: scan relay QR, connect through `/relay/client`, clear one-time fields after successful pairing, persist only URL.
- Base: direct LAN config remains default and continues using `AppConfig.wsUrlFor()`.
- Bad: storing the pairing secret in `SharedPreferences`, or falling back to direct `/download` in relay mode.

#### 6. Tests Required
- Relay URI parsing preserves direct host/port/token and fills in-memory relay fields.
- `toJson()` omits relay session and pairing secret fields.
- Relay URL validation rejects `http(s)://` and public `ws://`.
- Controller relay connect validates URL and clears one-time fields after success.

#### 7. Wrong vs Correct

Wrong:

```dart
await prefs.setString('relayPairingSecret', secret);
```

Correct:

```dart
await prefs.setString('mobilevc.app_config', jsonEncode(config.toJson()));
```

### Scenario: Flutter Relay E2EE Security State

#### 1. Scope / Trigger
- Trigger: any Flutter change that displays relay trust, encrypted status, fingerprint confirmation, E2EE diagnostics, or device revoke/plaintext-disabled errors.
- Applies to `lib/core/relay_e2ee`, relay connection settings, pairing/import UI, and future relay security/device management views.

#### 2. Signatures
- Security input: `RelaySecurityInput`
- Evaluator: `RelaySecurityStateEvaluator.evaluate(input)`
- Capability input: `RelayE2eeCapabilitySet`
- Handshake frame DTOs: `RelayE2eeClientHelloFrame`, `RelayE2eeAgentHelloFrame`, `RelayE2eeClientProofFrame`, `RelayE2eeAgentResultFrame`
- Verified gate: `RelaySecurityState.canShowVerified`
- Blocking states: `fingerprintMismatch`, `deviceRevoked`, `plaintextDisabled`, `encryptionUnavailable`

#### 3. Contracts
- UI may show "E2EE 已验证" only when `canShowVerified == true`.
- `canShowVerified` requires relay mode, confirmed node fingerprint, completed E2EE handshake, compatible protocol/tunnel capabilities, multiplex stream support, file download support, device management support, E2EE required, plaintext test-mode off, device not revoked, and production plaintext rejection active.
- Relay plaintext test-mode must be labeled as test mode and must never look verified.
- Flutter E2EE capability fields must match the Go relay E2EE capability contract: `relayProtocolVersion`, `e2eeProtocolVersion`, `cryptoSuite`, `tunnelProtocolVersion`, `supportsMultiplexStreams`, `supportsFileDownloadStream`, `supportsDeviceManagement`, `requiresE2EE`, and `plaintextTestMode`.
- `RelayE2eeCapabilitySet.production()` must require E2EE, disable plaintext test-mode, and require multiplex streams, file download streams, and device management before it can be used for verified security state.
- `RelayE2eeCapabilitySet.plaintextTestMode()` is explicit test mode only and must not be presented as verified.
- Capability values must be applied to `RelayE2eeHandshakeInput` before generating or validating the transcript.
- Relay E2EE handshake control frame JSON must match Go exactly: `client.e2ee_hello`, `agent.e2ee_hello`, `client.e2ee_proof`, and `agent.e2ee_result`.
- Public keys, signatures, pairing proofs, device proofs, and device signatures in E2EE handshake frames are base64url strings. Flutter must not serialize these fields as JSON byte arrays.
- `RelayE2eeClientHelloFrame` validates production capabilities, required routing IDs, valid handshake kind, valid client ephemeral P-256 key, and reconnect-only device identity fields.
- `RelayE2eeAgentHelloFrame` validates production capabilities, node ephemeral key, node identity key, and non-empty node signature before the app builds/verifies a handshake transcript.
- `RelayE2eeClientProofFrame` must not mix pairing and reconnect proof fields. Pairing uses only `pairingProof`; reconnect uses `deviceProof` plus `deviceSignature`.
- `RelayE2eeAgentResultFrame` must use `ok=true` without `errorCode`, or `ok=false` with an actionable relay/E2EE error code.
- `mobilevc://relay/v1` pairing links may include capability hints using the same capability field names; if any capability hint is present, all capability fields are required and must validate before import succeeds.
- `mobilevc://relay/v1` pairing links must include `nodeFingerprint` as a 64-character hex SHA-256 fingerprint. Import must fail if the fingerprint is missing or malformed.
- `AppConfig.relayNodeFingerprintHex` is non-secret pairing metadata and may be persisted; pairing secrets and pairing expiry remain non-persistent.
- `RelayTunnelFrame.validate()` must enforce both required fields and unexpected-field rejection per frame type; `ping` and `pong` carry no stream metadata.
- `RelayTunnelCounterState.nextSeq(streamId)` must allocate sequence numbers per stream ID, not globally across the relay tunnel.
- Fingerprint mismatch, revoked device, decrypt failure, unsupported version, missing capability, or missing plaintext rejection must produce neutral or blocking copy, not security-positive copy.
- Relay error codes for E2EE, device, stream, and download failures must map to actionable Chinese copy through `relayErrorMessage`; do not show raw server messages such as `e2ee required` to users.
- Full fingerprint and short fingerprint are derived from the node public key; UI must not invent or truncate fingerprint values outside this evaluator.

#### 4. Validation & Error Matrix
- Direct mode -> `LAN 直连`, not verified.
- Relay plaintext test mode -> `Relay 测试模式`, not verified.
- Production capability with plaintext test-mode enabled -> validation error.
- Production capability missing multiplex stream, file download stream, or device management support -> validation error.
- Unsupported relay/e2ee/tunnel version or crypto suite -> validation error.
- Relay pairing URI with partial, malformed, or contradictory capability hints -> import failure, not silent direct/relay fallback.
- Relay pairing URI with missing or malformed `nodeFingerprint` -> import failure.
- Production E2EE pairing with fingerprint mismatch -> connection failure with `e2ee_fingerprint_mismatch`; do not send pairing proof.
- Production E2EE pairing with `agent.e2ee_result ok=false` -> connection failure using the result error code.
- Production E2EE pairing timeout before `agent.e2ee_result ok` -> `Relay E2EE 握手超时`.
- Persisted production E2EE reconnect without a wired device rekey flow -> fail before websocket connect with `e2ee_unsupported_version`; do not send plaintext `client.reconnect` as a successful relay connection.
- Encrypted `relay.forward` before Flutter has an active stream codec -> `e2ee_decrypt_failed`.
- Plaintext `relay.forward` after Flutter E2EE completion -> `e2ee_decrypt_failed` / plaintext-disabled receive failure path; do not decode as plaintext.
- Duplicate encrypted MobileVC stream counter -> `e2ee_replay_detected`.
- MobileVC encrypted payload authentication failure, malformed ciphertext, or bad metadata -> `e2ee_decrypt_failed`.
- E2EE handshake frame with missing capabilities, malformed base64url material, invalid P-256 public key, invalid kind, or pairing/reconnect field mixup -> `FormatException` / explicit connection failure.
- E2EE handshake frame with plaintext-test capabilities where production handshake is required -> validation error; do not silently continue as plaintext.
- `agent.e2ee_result` with `ok=false` and no `errorCode`, or `ok=true` and an `errorCode` -> `FormatException`.
- Tunnel frame with an unknown stream type, missing required field, or unexpected field for its frame type -> `FormatException`.
- Fingerprint mismatch -> `指纹已变化`, blocking.
- Device revoked -> `设备已撤销`, blocking.
- Decrypt failure or unsupported E2EE capability -> `加密不可用`, blocking.
- E2EE required but plaintext rejection not confirmed -> `明文拒绝未启用`, blocking.
- `relay.error` code `e2ee_required` -> copy tells the user plaintext is disabled and both phone/local service must be updated/re-paired for E2EE.
- `relay.error` code `e2ee_unsupported_version` -> copy tells the user the E2EE version is incompatible and both endpoints must be updated.
- `relay.error` codes `device_revoked` / `device_unknown` -> copy tells the user to re-authorize or re-pair instead of retrying direct websocket fallback.

#### 5. Good/Base/Bad Cases
- Good: status chip text is derived from `RelaySecurityState.title` and verified styling is gated by `canShowVerified`.
- Good: pairing UI shows the evaluator-provided short fingerprint plus full copyable fingerprint detail.
- Good: `connectRelay` stores pending E2EE state before sending `client.e2ee_hello`, so a fast `agent.e2ee_hello` cannot arrive before state exists.
- Good: capability hints from QR/import survive config persistence, but pairing secret and expiry do not.
- Good: production E2EE handshake success is tracked separately from verified UI; encrypted forwarding must still be wired before showing verified security.
- Good: production relay sends `relay.forward` with `encryption=p256-ecdsa+p256-ecdh+hkdf-sha256+aes-256-gcm`, `streamId=1`, `handshakeId=<completed handshake>`, and ciphertext payloads that do not contain MobileVC plaintext.
- Good: `MobileVcWsService` keeps relay send/decode queues so async crypto preserves nonce uniqueness and event order.
- Good: production E2EE persisted reconnect is blocked before network connect until device-credential reconnect handshakes are implemented.
- Base: relay test-mode remains usable for local debugging but is visually marked as non-production.
- Bad: showing "安全", "已加密", or shield/verified styling just because `connectionMode == relay`.
- Bad: hiding fingerprint mismatch behind a reconnect retry.
- Bad: completing relay connect immediately after `client.paired` in production E2EE mode.
- Bad: sending `client.e2ee_hello` in plaintext test-mode where the local agent may intentionally have no E2EE handler.
- Bad: marking relay as verified because handshake traffic keys were derived while business frames remain plaintext.
- Bad: constructing a new `RelayMobileVcStreamCodec` for every frame; that resets counters and replay state.
- Bad: accepting `encryption=none` after E2EE completion because a decrypt failed or the app wants to keep the socket alive.
- Bad: using stored `clientReconnectSecret` to reconnect in production E2EE mode and then sending MobileVC business payloads before a fresh ECDH/device proof handshake completes.

#### 6. Tests Required
- Verified state only when every condition is true.
- Capability tests assert production success, production plaintext-test rejection, missing tunnel feature rejection, explicit plaintext test-mode validation, unsupported version rejection, and handshake transcript binding.
- Handshake frame tests assert pairing/reconnect frame round-trip, device identity requirement on reconnect, malformed base64url rejection, production capability enforcement, and proof field mixup rejection.
- Config/import tests assert relay pairing URI capability hints validate and invalid hints fail explicitly.
- Config/import tests assert relay capability hints persist as non-secret config while pairing secret and expiry do not.
- Service tests assert production E2EE relay pairing waits for `agent.e2ee_result ok`, verifies node fingerprint, and rejects fingerprint mismatch.
- Service tests assert production E2EE relay `send()` emits encrypted `relay.forward` without plaintext leakage, and inbound encrypted `relay.forward` decrypts into the expected `AppEvent`.
- Service tests assert production E2EE persisted reconnect is blocked before websocket connect until reconnect rekey is wired; plaintext test-mode reconnect may remain available only with explicit test capabilities.
- Stream tests assert duplicate counters are rejected and async counter allocation cannot reuse nonce values.
- Config/import tests assert relay pairing URI node fingerprint is required.
- Tunnel tests assert required fields, unexpected-field rejection, unknown stream type rejection, per-stream sequence allocation, per-stream replay rejection, and zero-window rejection.
- Test-mode, fingerprint mismatch, revoked device, decrypt failure, unsupported capability, and plaintext-not-rejected states cannot show verified.
- Direct mode never implies E2EE verified.
- Relay E2EE/device/stream/download error-code mapping tests assert Chinese actionable copy and do not expose raw backend English messages.

#### 7. Wrong vs Correct

Wrong:

```dart
final verified = config.connectionMode == 'relay';
```

Correct:

```dart
final state = await RelaySecurityStateEvaluator.evaluate(input);
final verified = state.canShowVerified;
```


---

## Testing Requirements

<!-- What level of testing is expected -->

(To be filled by the team)

---

## Code Review Checklist

<!-- What reviewers should check -->

(To be filled by the team)
