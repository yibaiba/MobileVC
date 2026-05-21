# Quality Guidelines

> Code quality standards for backend development.

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

### Scenario: Relay Mode Backend Boundary

#### 1. Scope / Trigger
- Trigger: any backend change that touches relay mode, relay URLs, relay credentials, or gateway transport adapters.
- Applies to `cmd/relay`, `internal/relay`, `internal/relayclient`, `internal/config`, and `cmd/server`.

#### 2. Signatures
- Relay server command: `go run ./cmd/relay`
- Relay endpoints: `GET /relay/agent`, `GET /relay/client`, `GET /healthz`, `GET /version`
- Local backend env: `RELAY_MODE=true`, `RELAY_URL=<ws-or-wss-url>`, `RELAY_PAIRING_EVENT_PATH=<owner-only-json-path>`
- Local backend exposure env/flag: `NETWORK_EXPOSURE_MODE=lan|relay-only`, `--network-exposure-mode lan|relay-only`
- Local backend URL helpers: `Config.ListenAddress()`, `Config.HealthURL()`, `Config.VersionURL()`, `Config.WebSocketURL()`
- Relay client entry: `relayclient.Run(ctx, cfg, gatewayHandler, relayclient.EmitPairingFile)`
- Relay capacity env: `RELAY_MAX_AGENT_CONNS`, `RELAY_MAX_CLIENT_CONNS`, `RELAY_MAX_CONNS_PER_IP`, `RELAY_FORWARD_QUEUE_SIZE`
- Relay liveness env: `RELAY_PING_INTERVAL`, `RELAY_PONG_TIMEOUT`, `RELAY_AGENT_GRACE_PERIOD`
- Trusted proxy env: `RELAY_TRUSTED_PROXY_CIDRS=<comma-separated-cidrs>`
- Relay E2EE env: `RELAY_REQUIRE_E2EE=true|false`, `RELAY_PLAINTEXT_TEST_MODE=true|false`
- Relay E2EE flags: `--require-e2ee[=true|false]`, `--plaintext-test-mode[=true|false]`

#### 3. Contracts
- Relay server forwards only `relay.forward` envelopes with base64url payloads; it must not parse MobileVC business actions.
- Production relay defaults to `RELAY_REQUIRE_E2EE=true` and `RELAY_PLAINTEXT_TEST_MODE=false`; plaintext `relay.forward` frames are rejected unless plaintext test mode is explicitly enabled.
- Plaintext test mode is for local/debug rollout only. It must be enabled explicitly with `RELAY_REQUIRE_E2EE=false` plus `RELAY_PLAINTEXT_TEST_MODE=true`, or equivalent flags.
- E2EE `relay.forward` frames use `encryption=p256-ecdsa+p256-ecdh+hkdf-sha256+aes-256-gcm`, `payloadEncoding=base64url`, non-zero `streamId`, and non-empty `handshakeId`. Counter `0` is valid because stream counters start at zero.
- `agent.register` sends only secret hashes; plaintext pairing secret is local-only and written through `RELAY_PAIRING_EVENT_PATH`.
- `client.pair` is the only place a client sends the one-time pairing secret.
- Direct backend `AUTH_TOKEN` must not appear in relay control frames, relay envelopes, relay QR URIs, relay logs, or relay event files.
- Local backend listen address and startup URLs must be derived from `Network.ExposureMode`: LAN mode listens on `:<port>` and logs local test URLs using `localhost:<port>`; relay-only mode listens on `127.0.0.1:<port>` and logs loopback URLs using `127.0.0.1:<port>`.
- Relay traffic enters the gateway through `gateway.ClientConn`; do not fake `http.Request` or gorilla request state.
- Relay writes must be serialized per websocket connection; authentication responses, `relay.error`, ping frames, and queued forwards share the same write lock.
- Relay must not write authentication or pairing responses while holding the global relay session mutex. Mutate shared session state under the mutex, release it, then write to the websocket with a write deadline.
- Forwarding must use a bounded per-peer queue. Queue exhaustion is explicit `relay.error` with `queue_full`, never a blocking goroutine leak.
- Relay must enforce global role caps and per-IP caps before websocket upgrade.
- `X-Forwarded-For` and `X-Real-IP` are honored only when the socket peer IP is inside `RELAY_TRUSTED_PROXY_CIDRS`; otherwise the socket IP is the capacity key.
- Agent reconnect uses `agent.reconnect` plus `agentReconnectSecret` only during the grace window. New `agent.register` must not replace an existing session ID.
- When an agent disconnects, relay schedules cleanup at `RELAY_AGENT_GRACE_PERIOD`; if the same session is still disconnected then, remove the session and close the paired client connection. Do not wait for another reconnect attempt to discover expiry.
- When a client pairs, relay sends `client.attached` to the agent with `sessionId` and relay-assigned `clientId`. The local relay client must consume this control frame separately from MobileVC payloads so the first local backend event can use the active `clientId`.
- Local relay client reconnect backoff is bounded and retries only within the current disconnect's `RELAY_AGENT_GRACE_PERIOD`; each later disconnect starts a new grace window.
- Local relay client `agent.register` and `agent.reconnect` writes and response reads must use control-frame deadlines. A relay accepting the websocket but never replying must surface as an explicit error, not an infinite wait.

#### 4. Validation & Error Matrix
- `RELAY_MODE=true` with empty `RELAY_URL` -> config error.
- `http://` or `https://` relay URL -> config error.
- Public `ws://` relay URL -> config error; only loopback/LAN development hosts may use `ws://`.
- Missing `RELAY_PAIRING_EVENT_PATH` in relay mode -> config error.
- Invalid `NETWORK_EXPOSURE_MODE` -> config error.
- Invalid relay duration / integer / byte env value -> config error; do not silently fall back to defaults.
- Invalid relay boolean env value -> config error; do not silently fall back to defaults.
- `RELAY_REQUIRE_E2EE=true` together with `RELAY_PLAINTEXT_TEST_MODE=true` -> config error.
- Oversized decoded relay payload -> `relay.error` with `payload_too_large`.
- Plaintext `relay.forward` while E2EE is required and plaintext test mode is off -> `relay.error` with `e2ee_required`.
- Unknown forward encryption suite -> `relay.error` with `e2ee_unsupported_version`.
- E2EE forward missing `streamId` or `handshakeId` -> `relay.error` with `protocol_error`.
- Forward with missing or mismatched `clientId` -> `relay.error` with `protocol_error`.
- First agent-to-client forward with an empty `clientId` after successful client pairing -> relay fills the current active `clientId`; wrong non-empty `clientId` still -> `protocol_error`.
- Missing `client.attached` before the relay websocket closes -> local relay client write returns the underlying read/close error.
- Per-IP or role capacity exceeded before upgrade -> HTTP 429.
- Bounded forward queue full -> `relay.error` with `queue_full`.
- Invalid `RELAY_TRUSTED_PROXY_CIDRS` -> relay startup config error.
- Pairing reject causes must stay indistinguishable to clients -> `pairing_rejected`.

#### 5. Good/Base/Bad Cases
- Good: backend writes pairing data to an owner-only temp file, launcher reads and deletes it, logs show only redacted URI.
- Good: public relay starts with E2EE required and rejects plaintext before forwarding payloads.
- Good: relay-only backend logs `health=http://127.0.0.1:<port>/healthz` and `ws=ws://127.0.0.1:<port>/ws?token=<redacted>`; it must not concatenate `localhost` with a full host:port listen address.
- Good: local test relay uses explicit `--require-e2ee=false --plaintext-test-mode=true` and UI/logs label it as test-only.
- Good: relay behind a trusted reverse proxy enforces caps by forwarded client IP, while direct internet clients cannot spoof forwarded headers.
- Base: direct `/ws?token=...` path still performs token and origin checks.
- Bad: printing `mobilevc.relay.pairing_ready` JSON to stdout/stderr because server logs then retain the one-time secret.
- Bad: accepting `encryption=none` on a long-lived public relay session because E2EE handshake code is not fully integrated yet.
- Bad: accepting a duplicate `agent.register` for an existing `sessionId` after disconnect; that bypasses reconnect-secret semantics.

#### 6. Tests Required
- Relay pairing, one-time secret consumption, URL validation, oversized payload, and opaque unknown business payload forwarding.
- Network exposure tests must cover listen address plus generated health/version/websocket URLs for LAN and relay-only modes.
- Relay plaintext rejection, plaintext test-mode allowance, E2EE metadata validation, unsupported encryption rejection, config env parsing, and CLI flag parsing.
- Relay per-IP caps, trusted forwarded IP positive/negative cases, ping writer shutdown, mismatched `clientId`, duplicate session register rejection, and reconnect within grace.
- Relay agent-disconnect grace expiry must remove orphan sessions and close paired clients.
- Relay client tests must cover consuming `client.attached` before `relay.forward`, writing with the attached `clientId`, and timing out register/reconnect response reads.
- Config tests for relay env validation and event-path requirement.
- Launcher tests that pairing event files are read locally and removed.

#### 7. Wrong vs Correct

Wrong:

```go
fmt.Println(pairingSecret)
```

Correct:

```go
_ = relayclient.EmitPairingFile(pairingEventPath, event)
```

### Scenario: Relay E2EE Device Trust Store

#### 1. Scope / Trigger
- Trigger: any backend change that stores relay E2EE node/device trust, device credentials, revocation state, or global rotation state.
- Applies to `internal/relay/e2ee` and any future local-node device management API layered above it.

#### 2. Signatures
- Node trust path: `~/.mobilevc/relay/trusted_devices.json`
- Store loader: `LoadOrCreateDeviceTrustStore(path)`
- Credential helpers: `NewDeviceCredential()`, `DeviceCredentialHash(secret)`, `DeviceCredentialMatches(hash, secret)`
- Device actions: `RegisterDevice`, `ListDevices`, `VerifyDeviceCredential`, `MarkDeviceSeen`, `RevokeDevice`, `ClearTrustedDevicesForNodeRotation`

#### 3. Contracts
- Local node is the source of truth for trusted E2EE devices; relay server runtime device maps are not persistent trust stores.
- The store persists device ID, display name, public key, fingerprint, credential hash, timestamps, revoked time, and current active session ID.
- The store must never persist plaintext device credentials, private keys, traffic keys, pairing secrets, file contents, or conversation payloads.
- Store parent directory permission must be owner-only (`0700`) and store file permission must be owner-only (`0600`).
- Same device identity may have only one active session ID; marking the same device seen with a new session returns the replaced session ID so callers can close the older connection explicitly.
- Device trust store methods must serialize access internally; concurrent pairing/reconnect/revoke/rotation must not race the map or JSON file.
- `ClearTrustedDevicesForNodeRotation` only clears device trust as part of a broader node identity rotation flow. It must not be presented as complete global rotate by itself.
- Relay runtime client reconnect must return stable device lifecycle error codes: revoked devices fail with `device_revoked`; unknown devices, wrong reconnect credentials, and post-rotation stale clients fail with `device_unknown`.
- Relay runtime `RotateSessionCredentials` clears runtime device records and reconnect secrets for that session. Single-device `RevokeDevice` keeps the device visible as revoked; global rotation intentionally makes previous runtime devices unknown.

#### 4. Validation & Error Matrix
- Empty store path -> explicit config error.
- Invalid device public key -> registration/load error.
- Stored fingerprint mismatch -> load error.
- Unknown device -> `device_unknown`.
- Revoked device -> `device_revoked`.
- Wrong runtime client reconnect secret -> `device_unknown`.
- Runtime global rotation cleanup -> runtime device list is cleared and previous client reconnect credentials fail as `device_unknown`.
- Duplicate device registration -> `device_already_bound`; revoked devices must not be silently re-enabled by registering the same ID again.
- Wrong credential -> `device_unknown`.
- Node rotation cleanup -> trusted device list is cleared and previous credentials fail as `device_unknown`.

#### 5. Good/Base/Bad Cases
- Good: local node stores only `DeviceCredentialHash(deviceCredential)` while Flutter keeps the plaintext credential in platform secure storage.
- Good: revoke clears `ActiveSessionID` and later reconnect fails before E2EE traffic keys are accepted.
- Good: relay runtime single-device revoke returns `device_revoked`, while global rotation returns `device_unknown` for old reconnect credentials.
- Base: relay server may keep runtime connection state for routing/caps, but not durable device trust.
- Bad: persisting E2EE device trust in the public relay server database.
- Bad: writing plaintext device credential or private key material into JSON, logs, pairing links, or relay frames.

#### 6. Tests Required
- Store creation/reload preserves device public metadata and owner-only permissions.
- Store file does not contain plaintext credential.
- Credential verification accepts valid credential and rejects wrong/revoked/rotated devices.
- Marking a device seen returns the replaced active session ID.
- Relay runtime tests assert wrong reconnect secret -> `device_unknown`, revoked device -> `device_revoked`, and global rotation clears runtime devices.
- Fingerprint/public-key tamper causes load failure.
- Concurrent registration/seen/revoke operations do not race or lose device records.

#### 7. Wrong vs Correct

Wrong:

```go
device.CredentialHash = plaintextCredential
```

Correct:

```go
device.CredentialHash = DeviceCredentialHash(plaintextCredential)
```


---

## Testing Requirements

<!-- What level of testing is expected -->

(To be filled by the team)

---

## Code Review Checklist

<!-- What reviewers should check -->

(To be filled by the team)
