# MobileVC Relay Mode Technical Design

## Summary

Relay mode adds a public WebSocket transport without exposing the local MobileVC backend to the internet.

```text
Flutter/Web client  -> cloud relay <- local MobileVC backend
                            |
                            v
                    opaque payload routing only
```

The relay is a standalone cloud-deployable service. The local Go backend owns command execution, file access, Codex/Claude session state, and gateway/session behavior. The Node launcher owns UX/configuration only.

## Non-Negotiable Boundaries

* `mobilevc start` remains direct local/LAN mode.
* Relay mode is entered only through `mobilevc public` or explicit relay config.
* Relay payload is opaque; relay never parses MobileVC actions.
* Relay exposes no command/file/download HTTP surface.
* Relay holds no `AUTH_TOKEN`, APNs keys, local workspace paths, AI CLI env, or file permissions.
* One relay session has one active controller client in MVP.
* Relay failures are explicit; no silent fallback to direct public exposure.
* Relay session expiry does not delete or mutate local MobileVC/Codex/Claude sessions.

## Components

### Cloud Relay

Likely packages:

```text
cmd/relay
internal/relay
```

Responsibilities:

* Accept WebSocket connections from local agents and remote clients.
* Expose only `/healthz` and `/version` HTTP endpoints.
* Register relay sessions from local agents.
* Verify one-time pairing secrets from clients.
* Route opaque envelopes between the local agent and the single active controller.
* Enforce pairing TTL, one-time pairing consumption, and agent disconnect grace period.
* Keep relay sessions and pairing state in memory only.
* Log routing metadata only.

Non-responsibilities:

* No command execution.
* No file reads/downloads.
* No MobileVC gateway action parsing.
* No Codex/Claude state storage.
* No account system in MVP.
* No HTTP debug/session/payload inspection endpoints.
* No database or disk persistence for relay sessions in MVP.

### Local Go Relay Agent

Likely package:

```text
internal/relayclient
```

Responsibilities:

* Generate `sessionId` and one-time `pairingSecret`.
* Generate an agent-only reconnect secret for the relay session.
* Connect outbound to `RELAY_URL` over WS/WSS.
* Register the local session with the relay.
* Bridge relay frames into the shared gateway message loop.
* Reconnect with bounded exponential backoff.
* Surface relay health and pairing data to launcher/status events.
* Emit local-only pairing status after relay registration.

Non-responsibilities:

* Does not expose a public listener.
* Does not move session state out of local storage.
* Does not change direct LAN behavior.

### Node Launcher

File:

```text
bin/mobilevc.js
```

Responsibilities:

* Provide `mobilevc public`.
* Resolve relay URL priority:
  * CLI `--relay`
  * saved relay URL
  * development default `ws://127.0.0.1:9000`
* Start Go backend with relay env/config.
* Read pairing data from backend local-only status events.
* Display pairing QR/link from backend-generated pairing data.
* Persist relay URL with owner-only permissions.

Non-responsibilities:

* Must not proxy long-lived relay traffic.
* Must not inspect relay payloads.
* Must not generate relay session ids or relay secrets.

### Flutter/Web Client

Likely areas:

```text
mobile_vc/lib/core/config/
mobile_vc/lib/features/session/
```

Responsibilities:

* Add connection mode: `Direct/LAN` vs `Relay`.
* Keep direct `host/port/token/http(s)/trusted roots` fields unchanged.
* Store relay URL separately from direct host.
* Scan relay pairing QR and auto-select Relay mode.
* Send existing MobileVC WS messages through relay after pairing.
* Disable HTTP file download actions while in Relay mode.
* Keep `sessionId` and `pairingSecret` in memory only during pairing.
* Clear `sessionId` and `pairingSecret` after pairing success, failure, expiry, or closing the pairing UI.

## Protocol

### Envelope

Relay routes envelopes only by metadata:

```json
{
  "type": "relay.forward",
  "version": 1,
  "sessionId": "relay-session-id",
  "clientId": "relay-client-id",
  "direction": "client_to_agent",
  "messageId": "uuid-or-random-id",
  "contentType": "mobilevc.ws.v1",
  "encryption": "none",
  "payloadEncoding": "base64url",
  "payload": "base64url-encoded-payload"
}
```

`payload` is the existing MobileVC WebSocket message encoded as base64url opaque bytes. Relay does not inspect fields inside `payload`.

Rules:

* `relay.forward` is a forward/data frame, not a control frame.
* `relay.forward.type` must be `relay.forward`.
* Relay control frames are not MobileVC payload envelopes.
* `payloadEncoding` must be `base64url` in MVP.
* Relay may validate decoded payload length only.
* Relay must reject decoded payloads larger than 1 MiB.
* Relay must reject control frames larger than `RELAY_MAX_CONTROL_FRAME_BYTES`.
* `RELAY_MAX_CONTROL_FRAME_BYTES` does not apply to `relay.forward`.
* Relay must not decode payload for logging, filtering, or business action routing.
* Relay must not accept JSON object payloads in MVP; MobileVC traffic must be encoded bytes.
* Direct backend `AUTH_TOKEN` must not appear in relay envelopes or relay control frames.

Envelope field rules:

* `direction` is one of `client_to_agent` or `agent_to_client`.
* `contentType` must be `mobilevc.ws.v1` in MVP.
* `encryption` must be `none` in MVP.
* `messageId` must be present and unique enough for metadata correlation, but relay must not persist, deduplicate, or replay messages by `messageId` in MVP.
* Relay must reject malformed envelopes before forwarding and log metadata only.

### WebSocket Endpoints

Relay WebSocket endpoints are fixed for MVP:

```text
GET /relay/agent
GET /relay/client
```

Rules:

* `/relay/agent` accepts the local Go backend relay agent.
* `/relay/client` accepts Flutter/Web controller clients.
* The relay must reject unknown WebSocket paths.
* HTTP `/healthz` and `/version` stay separate from relay WebSocket paths.
* Do not multiplex role selection through business payloads.

### Relay URL Validation

Relay URLs are WebSocket URLs:

* Public relay URLs must use `wss://`.
* `ws://` is allowed only for loopback or LAN development URLs.
* Allowed insecure `ws://` hosts are `localhost`, loopback addresses, RFC1918 private IPv4 ranges, IPv6 loopback, IPv6 unique-local, and IPv6 link-local addresses.
* Examples: `ws://127.0.0.1:9000`, `ws://localhost:9000`, `ws://192.168.1.10:9000`, `ws://10.0.0.5:9000`, `ws://172.16.0.5:9000`.
* `http://` and `https://` relay inputs must be rejected with clear errors.
* Relay URL validation applies to launcher config, backend env/config, Flutter relay config, and QR parsing.
* Do not silently rewrite `https://relay.example.com` into `wss://relay.example.com`.

### Frame Types

Relay control messages are separate from MobileVC payloads:

Control frames:

* `agent.register`
* `agent.registered`
* `agent.reconnect`
* `client.pair`
* `client.paired`
* `relay.error`
* `relay.ping`
* `relay.pong`

Forward/data frames:

* `relay.forward`

Only `relay.forward.payload` contains MobileVC business traffic. `relay.forward` is a forward/data frame, not a control frame.

Control frame rules:

* `client.pair` is sent before a `clientId` exists.
* Relay assigns `clientId` only after successful `client.pair`.
* `agent.register`, `agent.reconnect`, `client.pair`, and reconnect credentials stay in control frames, not MobileVC payload envelopes.
* Control frames must not include direct backend `AUTH_TOKEN`.
* `RELAY_MAX_CONTROL_FRAME_BYTES` applies to control frames only.
* Every control frame must include `type` and `version`.
* MVP control frame `version` is `1`.

Control frame schemas:

```json
{
  "type": "agent.register",
  "version": 1,
  "sessionId": "rs_xxx",
  "pairingSecretHash": "base64url-sha256-pairing-secret",
  "agentReconnectSecretHash": "base64url-sha256-agent-reconnect-secret",
  "pairingExpiresAt": 1760000000
}
```

```json
{
  "type": "agent.registered",
  "version": 1,
  "sessionId": "rs_xxx"
}
```

```json
{
  "type": "agent.reconnect",
  "version": 1,
  "sessionId": "rs_xxx",
  "agentReconnectSecret": "secret_xxx"
}
```

```json
{
  "type": "client.pair",
  "version": 1,
  "sessionId": "rs_xxx",
  "pairingSecret": "secret_xxx"
}
```

```json
{
  "type": "client.paired",
  "version": 1,
  "sessionId": "rs_xxx",
  "clientId": "rc_xxx"
}
```

Credential frame rules:

* `agent.register` sends only `pairingSecretHash` and `agentReconnectSecretHash`; it must not send plaintext `pairingSecret` or plaintext `agentReconnectSecret`.
* Secret hashes are `base64url(SHA-256(UTF-8 secret))`.
* Relay compares secret hashes with constant-time comparison.
* `agent.reconnect` may send plaintext `agentReconnectSecret` only over the relay WebSocket control frame; relay hashes it for verification and must not log or persist it.
* `client.pair` may send plaintext `pairingSecret` only over the relay WebSocket control frame; relay hashes it for verification, consumes the stored pairing verifier on success, and must not log or persist it.
* `agent.registered` and `client.paired` must not contain pairing or reconnect secrets.

### Relay Error Frame

`relay.error` is a control frame with a stable schema:

```json
{
  "type": "relay.error",
  "code": "pairing_rejected",
  "message": "pairing rejected"
}
```

Rules:

* `code` is machine-readable and stable.
* `message` is human-readable and safe to show in launcher or Flutter UI.
* `message` must not contain secrets, payload bytes, command text, prompts, file paths, direct backend tokens, or raw relay credentials.
* Pairing errors must use the same public `code` and `message` for unknown session, expired pairing, already paired session, wrong secret, and rate-limited pairing.
* Relay may log internal reason categories as metadata only when those categories do not include secrets or payload bytes.
* MVP error codes are `pairing_rejected`, `unauthorized`, `capacity_reached`, `timeout`, `frame_too_large`, `payload_too_large`, `protocol_error`, `target_unavailable`, `queue_full`, `agent_disconnected`, and `controller_disconnected`.

### Pairing Flow

```text
1. Go backend generates:
   - sessionId
   - pairingSecret with >=128 bits entropy
   - agentReconnectSecret with >=128 bits entropy
   - expiresAt = now + 5m
2. Go backend connects to relay and sends agent.register.
3. Relay stores short-lived session and pairing hash/state.
4. Go backend emits a local-only pairing status event containing relay URL, sessionId, pairingSecret, and expiry.
5. Launcher displays QR/link containing relay URL, sessionId, pairingSecret.
6. Client sends client.pair with sessionId + pairingSecret.
7. Relay verifies and consumes pairingSecret.
8. Relay assigns clientId and marks the client as active controller.
9. Relay forwards opaque payloads until disconnect/session close.
```

Pairing secret must not be logged by relay, backend, launcher, or Flutter.

Pairing and reconnect credential rules:

* Relay must never store `pairingSecret` or `agentReconnectSecret` in plaintext.
* Relay stores credential hashes or HMACs and verifies with constant-time comparison.
* Successful client pairing immediately clears the stored pairing secret verifier.
* The QR/link contains `pairingSecret` only; it must not contain `agentReconnectSecret`.
* The QR/link must not contain direct backend `AUTH_TOKEN`.
* Agent reconnect uses `sessionId` plus `agentReconnectSecret`, not `pairingSecret`.
* Agent reconnect is accepted only during the 60-second grace period.
* Relay logs may include session/client ids, endpoint path, and close reason, but not secrets or payload bytes.
* Pairing rejection responses must not reveal whether the session is unknown, expired, already paired, or has the wrong secret.

Agent registration and reconnect rules:

* `/relay/agent` must send `agent.register` or `agent.reconnect` as the first control frame.
* Initial `agent.register` creates the relay session when relay capacity allows.
* `agent.reconnect` requires `sessionId` plus `agentReconnectSecret`.
* Relay stores only an `agentReconnectSecret` verifier, never plaintext.
* A relay session has one active agent connection.
* Valid `agent.reconnect` during the grace period replaces a disconnected agent connection.
* A second live `agent.register` for an existing active session is rejected.
* A second live `agent.reconnect` for an already connected agent is rejected unless the previous agent connection has already been marked disconnected.
* Agent register/reconnect failures use vague errors and metadata-only logs.
* Agent register/reconnect timeout or capacity failure must not create a partial relay session.

Local pairing status rules:

* Go backend owns all relay credential generation.
* Launcher reads pairing data only from a local backend event, stdout/stderr marker, pipe, or owner-only temp file.
* Preferred event format is stdout JSONL.
* Pairing-ready event schema:
  ```json
  {"type":"mobilevc.relay.pairing_ready","relayUrl":"wss://relay.example.com","sessionId":"rs_xxx","pairingSecret":"secret_xxx","expiresAt":1760000000}
  ```
* `expiresAt` is a Unix timestamp in seconds.
* Launcher must ignore unknown fields on `mobilevc.relay.*` events for forward compatibility.
* Local pairing status must not be exposed by cloud relay endpoints.
* If an owner-only temp file is used, it must be deleted after launcher reads it or backend exits.
* Launcher may render the one-time QR/link, but all other logs must redact `pairingSecret`.

### Relay Auth Boundary

Relay pairing is separate from direct backend token authentication:

* Direct `/ws?token=...` keeps using `AUTH_TOKEN` in Direct/LAN mode.
* Relay mode must not send direct backend `AUTH_TOKEN`.
* Relay QR, relay config, relay control frames, and relay payload envelopes must not contain direct backend `AUTH_TOKEN`.
* Successful relay pairing makes that relay connection the authenticated controller transport.
* The local Go relay adapter enters the shared gateway message loop as a trusted local transport after relay pairing succeeds.
* Relay failure must not fall back to direct `/ws?token=...`.

### Pairing QR URI

Relay pairing QR uses a custom MobileVC URI:

```text
mobilevc://relay/v1?relay=wss%3A%2F%2Frelay.example.com&session=SESSION_ID&secret=PAIRING_SECRET&exp=1760000000
```

Fields:

* `relay`: relay WebSocket URL.
* `session`: relay session ID.
* `secret`: one-time pairing secret.
* `exp`: pairing expiry as Unix timestamp.

Flutter must parse in this order:

1. `mobilevc://relay/v1`
2. existing direct launch URI parsing

If the relay URI matches, Flutter selects Relay mode and stores relay URL/session/secret in relay fields. It must not update direct `host`, `port`, or `token`.

Persistence rules:

* Flutter may persist only the relay URL.
* Flutter must not persist `sessionId` or `pairingSecret` to SharedPreferences or any other storage.
* Flutter may keep `sessionId` and `pairingSecret` in memory only for `client.pair`.
* Flutter must clear in-memory `sessionId` and `pairingSecret` after pairing success, failure, expiry, or closing the pairing UI.
* Launcher may persist relay URL/config only; it must not persist one-time pairing secrets.

### Session Lifecycle

* Pairing TTL defaults to 5 minutes.
* Successful pairing consumes the secret immediately.
* Local agent disconnect starts a 60-second grace period.
* Agent reconnect within 60 seconds restores the relay session only when `agentReconnectSecret` verifies.
* Agent disconnect beyond 60 seconds closes the relay session and disconnects clients.
* Closing relay session does not delete or mutate local MobileVC/Codex/Claude sessions.
* After re-pairing, clients load existing local sessions through normal gateway/session APIs.
* Relay restart closes all relay sessions and requires clients to re-pair.
* Relay session and pairing state are memory-only in MVP.

### Controller Disconnect

MVP does not support controller reconnect:

* Relay does not issue a `clientReconnectSecret`.
* Successful `client.pair` consumes `pairingSecret`, and the secret cannot be reused.
* If the active controller WebSocket disconnects, relay closes the controller side of the relay session.
* The local agent may remain connected until its own lifecycle ends.
* A new controller must re-pair through a newly displayed pairing secret/session from `mobilevc public`.
* Flutter refresh, app restart, or network loss cannot silently restore controller identity in MVP.
* Relay should emit an explicit controller-disconnected status/error when possible.

### Forwarding Failure Semantics

MVP relay forwarding is real-time only:

* Relay does not queue traffic while the opposite side is disconnected.
* Relay does not persist or replay MobileVC payloads.
* If the target side is unavailable, relay returns `relay.error` to the sender and closes that controller stream.
* Each relay session uses a bounded in-memory forwarding queue.
* Default queue size is `RELAY_FORWARD_QUEUE_SIZE=64` frames per direction per session.
* If a queue is full or a write fails, relay emits `relay.error`, logs metadata only, and closes the affected connection.
* Message delivery acknowledgements are out of MVP; ordering is WebSocket order within a single live connection.
* `relay.forward` is governed by the decoded payload size limit, not `RELAY_MAX_CONTROL_FRAME_BYTES`.

### Connection Protection

Relay service defaults:

```text
RELAY_PAIRING_HANDSHAKE_TIMEOUT=10s
RELAY_AGENT_REGISTER_TIMEOUT=10s
RELAY_PAIRING_MAX_FAILURES_PER_SESSION_IP=5
RELAY_MAX_SESSIONS=1000
RELAY_MAX_AGENT_CONNS=1000
RELAY_MAX_CLIENT_CONNS=2000
RELAY_MAX_CONNS_PER_IP=20
RELAY_PING_INTERVAL=30s
RELAY_PONG_TIMEOUT=10s
RELAY_MAX_CONTROL_FRAME_BYTES=16KiB
RELAY_FORWARD_QUEUE_SIZE=64
RELAY_TRUSTED_PROXY_CIDRS=
```

Rules:

* These protections apply only to the relay service and must not change Direct/LAN mode.
* `/relay/agent` must send a valid `agent.register` or `agent.reconnect` before `RELAY_AGENT_REGISTER_TIMEOUT`.
* `/relay/client` must send a valid `client.pair` before `RELAY_PAIRING_HANDSHAKE_TIMEOUT`.
* Pairing failures are counted by `sessionId + remoteAddr`.
* When pairing failures exceed `RELAY_PAIRING_MAX_FAILURES_PER_SESSION_IP`, relay closes the connection.
* Relay enforces total relay session, total agent connection, total client connection, and per-remote-address connection caps.
* Relay sends ping every `RELAY_PING_INTERVAL` and closes a connection when pong is not received within `RELAY_PONG_TIMEOUT`.
* Relay rejects control frames larger than `RELAY_MAX_CONTROL_FRAME_BYTES`.
* Limit and timeout failures must be explicit `relay.error` responses when possible.
* Limit and timeout logs must include metadata only and must not include secrets or payload bytes.

### Reverse Proxy Client Identity

Relay uses socket remote address by default:

* Per-IP limits use the socket `RemoteAddr` unless trusted proxy CIDRs are configured.
* Relay must not trust `X-Forwarded-For`, `X-Real-IP`, or `Forwarded` headers by default.
* `RELAY_TRUSTED_PROXY_CIDRS` may enable forwarded client IP parsing for Caddy/Nginx/load balancer deployments.
* Forwarded headers are honored only when the socket remote address is in `RELAY_TRUSTED_PROXY_CIDRS`.
* Invalid forwarded headers are ignored and logged as metadata only.
* Public managed relay deployments must configure trusted proxy CIDRs explicitly before relying on forwarded headers.

### File Access Boundary

Relay MVP supports WebSocket file actions only:

* `fs_list` may be forwarded through relay as opaque WebSocket payload.
* `fs_read` may be forwarded through relay as opaque WebSocket payload.
* Relay must not decode, implement, authorize, log, or branch on `fs_list`, `fs_read`, `exec`, or any other MobileVC business action.
* Backend `/download` must not be exposed, proxied, or reimplemented on the relay.
* Flutter must not attempt direct HTTP `/download` fallback while in Relay mode.
* Flutter must disable file download in Relay mode and show: `Relay 模式暂不支持下载`.
* Relay file download requires a future explicit WebSocket chunking protocol and is out of MVP.

## Gateway Bridge

Do not fake `http.Request` or gorilla WebSocket state for relay traffic.

Extract the smallest connection boundary needed:

```go
type ClientConn interface {
    ReadJSON(v any) error
    WriteJSON(v any) error
    Close() error
    RemoteAddr() string
    Origin() string
}
```

Relay adapter identity:

* Direct `/ws` keeps HTTP query-token and browser Origin checks.
* Relay traffic enters the gateway only after relay-side pairing succeeds.
* Relay adapter must not construct fake `http.Request` values.
* Relay adapter must provide explicit metadata such as `transport=relay`, `remoteAddr=relay:<sessionId>/<clientId>`, and `origin=relay`.
* Gateway logic must not apply direct public Origin allowlist checks to relay adapter traffic.
* Relay transport identity authorizes only the WebSocket transport; MobileVC business permission flows remain local and unchanged.

Adapters:

* gorilla WebSocket adapter for direct `/ws`
* relay stream adapter for relay payloads

Target:

```text
direct /ws -> ClientConn -> shared gateway message loop
relay     -> ClientConn -> shared gateway message loop
```

This must be incremental. Do not rewrite the entire `gateway.ServeHTTP` in one change.

## Configuration

### Relay Server

```text
RELAY_ADDR=:9000
RELAY_PUBLIC_URL=wss://relay.example.com
RELAY_PAIRING_TTL=5m
RELAY_AGENT_GRACE_PERIOD=60s
RELAY_PAIRING_HANDSHAKE_TIMEOUT=10s
RELAY_AGENT_REGISTER_TIMEOUT=10s
RELAY_PAIRING_MAX_FAILURES_PER_SESSION_IP=5
RELAY_MAX_SESSIONS=1000
RELAY_MAX_AGENT_CONNS=1000
RELAY_MAX_CLIENT_CONNS=2000
RELAY_MAX_CONNS_PER_IP=20
RELAY_PING_INTERVAL=30s
RELAY_PONG_TIMEOUT=10s
RELAY_MAX_CONTROL_FRAME_BYTES=16KiB
RELAY_FORWARD_QUEUE_SIZE=64
RELAY_TRUSTED_PROXY_CIDRS=
```

Non-WebSocket HTTP surface:

```text
GET /healthz
GET /version
```

WebSocket upgrade endpoints:

```text
GET /relay/agent
GET /relay/client
```

Relay WebSocket endpoints are separate from health/version endpoints. Do not add session, debug, payload inspection, command, file, or download HTTP endpoints in MVP.

### Local Backend

```text
RELAY_MODE=true
RELAY_URL=wss://relay.example.com
RELAY_PAIRING_TTL=5m
RELAY_AGENT_GRACE_PERIOD=60s
```

### Launcher UX

```bash
mobilevc public --relay wss://relay.example.com
mobilevc public
```

Local development relay URL:

```text
ws://127.0.0.1:9000
```

This is an explicit local development configuration, not a runtime fallback. Relay failures must not silently switch to direct public exposure or another transport.

## Testing Plan

### Relay Protocol Tests

* unknown session rejected
* expired pairing rejected
* reused pairing rejected
* unpaired client rejected
* second active controller rejected
* pairing and agent reconnect secrets are not stored in plaintext
* agent reconnect without the agent reconnect secret is rejected
* pairing rejection does not reveal unknown session, expiry, or wrong secret causes
* `relay.error` schema and safe error-code behavior are enforced
* relay QR/config/envelope/control frames do not include direct backend `AUTH_TOKEN`
* `client.pair` is accepted only as a control frame before `clientId` exists
* Flutter does not persist `sessionId` or `pairingSecret`
* public relay URLs reject insecure `ws://` except loopback or LAN development URLs
* `http://` and `https://` relay URL inputs are rejected
* launcher receives pairing data only from local backend status events
* launcher parses `mobilevc.relay.pairing_ready` JSONL schema
* client pairing times out after handshake timeout
* agent register/reconnect times out after register timeout
* duplicate active agent registration/reconnect is rejected
* repeated pairing failures are capped by session and remote address
* relay session and connection caps are enforced
* forwarded IP headers are ignored unless socket remote address is in trusted proxy CIDRs
* oversized control frames are rejected
* `relay.forward` is not rejected by control-frame size limit
* opaque unknown MobileVC business payload forwarded unchanged
* JSON object MobileVC payload is rejected unless base64url encoded
* payload larger than 1 MiB rejected
* forward queue full emits `relay.error` and closes the affected connection
* relay logs do not contain payload or pairing secret

### Lifecycle Tests

* agent reconnect within 60 seconds restores session
* agent reconnect after 60 seconds is rejected
* agent reconnect requires `sessionId` plus `agentReconnectSecret`
* agent disconnected longer than 60 seconds closes session
* relay ping/pong timeout closes dead connections
* relay session expiry does not delete local sessions
* relay restart requires re-pairing and does not delete local sessions
* client can re-pair and load existing local session history
* controller disconnect requires re-pairing in MVP
* relay does not replay payloads across disconnect/reconnect

### Gateway Tests

* direct WebSocket path still passes existing behavior tests
* relay adapter and direct adapter share the same gateway message loop
* relay adapter enters gateway only after relay pairing succeeds
* relay adapter does not fake `http.Request` state or use direct Origin allowlist checks
* relay server does not implement, decode, log, authorize, or branch on `fs_read`, `fs_list`, or `exec`
* relay non-WebSocket HTTP surface contains only `/healthz` and `/version`
* relay WebSocket upgrade endpoints contain only `/relay/agent` and `/relay/client`
* unknown relay WebSocket paths are rejected

### Launcher Tests

* `mobilevc start` does not enable relay
* `mobilevc public --relay <url>` saves relay URL
* `mobilevc public --relay http(s)://...` is rejected with a clear error
* `mobilevc public --relay ws://public-host...` is rejected with a clear error
* `mobilevc public` reuses saved relay URL
* launcher renders QR/link from backend-generated local pairing status
* relay failure does not trigger direct public fallback

### Flutter Tests

* direct config remains default for existing saved configs
* relay config does not overwrite direct host/port/token
* relay config persists relay URL only and never persists `sessionId` or `pairingSecret`
* relay mode does not send direct backend `AUTH_TOKEN`
* relay URL validation rejects `http://`, `https://`, and public-host `ws://`
* relay QR scan selects Relay mode
* `mobilevc://relay/v1` parses before direct launch URI parsing
* direct LAN connection still works with relay disabled
* relay mode disables HTTP file download and shows `Relay 模式暂不支持下载`
* relay mode never falls back to direct backend `/download`

## Rollout Plan

1. Protocol structs and relay server unit tests.
2. Minimal relay server with in-memory session registry.
3. Local Go relay client with register/pairing status.
4. Gateway `ClientConn` extraction and relay adapter.
5. Launcher `mobilevc public --relay` wiring.
6. Flutter Relay mode config and QR parsing.
7. Smoke test: local backend + local relay + Flutter/Web client.
8. Documentation: relay mode recommended, direct public mode advanced.

## Open Questions

None for MVP planning.
