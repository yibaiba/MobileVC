# MobileVC Relay Mode Public Access Plan

## Goal

Provide a public access experience comparable to Happy-style remote control without requiring users to expose their local MobileVC backend, configure reverse proxy HTTPS/WSS, or manage browser Origin allowlists manually.

The user-facing target is:

```bash
mobilevc public
```

After a one-time setup, this should make the local MobileVC backend reachable from mobile/web clients through a relay service while preserving WebSocket responsiveness.

## Problem

The current direct public mode is secure but too operational:

* Users must own or configure a public domain.
* HTTPS/WSS reverse proxy setup is external.
* `PUBLIC_EXPOSURE_MODE` and `ALLOWED_ORIGINS` are backend concepts that leak into user workflow.
* Direct public exposure still means an attacker can reach the user's local command/file-capable backend if token or proxy config is compromised.

Happy-style tools feel easier because the local agent usually dials out to a managed relay. Public traffic reaches the relay, not the user's machine.

## Recommended Direction

Build a WebSocket relay mode:

```text
Local MobileVC backend/launcher -> wss://relay.mobilevc.example
Flutter/Web client              -> wss://relay.mobilevc.example
Relay service                   -> authenticated message forwarding only
```

The relay should not directly execute commands, read files, or terminate local backend auth. It should only route encrypted or authenticated frames between a paired local agent and client.

## Grill Decision Blocks

### Decision 1: Direct Public Mode Or Relay Mode?

Recommended answer: Relay mode becomes the primary public UX. Direct public mode remains as an advanced self-hosted option.

Why:

* Relay avoids exposing the local backend to the internet.
* Users do not need DNS, certificates, ports, reverse proxy, or Origin config.
* WebSocket latency remains acceptable because both sides keep persistent outbound connections.

### Decision 2: Managed Relay Or Self-Hosted Cloud Relay First?

Recommended answer: implement a standalone self-hosted cloud relay first, but design the CLI UX so a managed relay can be added later.

Why:

* The repo can ship a relay binary/server that can run on a VPS, Docker host, or behind a reverse proxy without committing to official production infra.
* Protocol and security can be verified locally.
* Later managed service only changes default relay URL and account/session provisioning.

### Decision 3: Encryption Level For MVP?

Recommended answer: require relay transport TLS plus per-session shared secret authentication in MVP; design message envelopes so end-to-end encryption can be added before managed public launch.

Why:

* Full E2EE needs key exchange, device pairing, recovery, and debugging UX.
* MVP can still avoid public exposure of the user's local backend.
* The protocol must not bake in relay-readable command semantics that block future E2EE.

### Decision 4: Pairing UX?

Recommended answer:

```bash
mobilevc public
```

prints or displays a pairing QR/link. Mobile/Web client uses that code to join the same relay session.

For saved setup:

```bash
mobilevc public --relay wss://relay-or-public-url
mobilevc public
```

The first command stores relay/public settings; the second reuses them.

### Decision 5: Relay Payload Boundary?

Accepted answer: relay only forwards opaque MobileVC WebSocket payloads wrapped in relay envelopes.

The relay must not parse, branch on, validate, or log MobileVC business actions such as:

* `exec`
* `ai_turn`
* `input`
* `fs_read`
* `fs_list`
* `permission_decision`
* `register_push_token`

These examples are business actions that relay must not understand. Relay may still forward them as opaque encoded payloads to the local backend.

Why:

* Existing gateway/session behavior stays local.
* Relay cannot accidentally gain command/file authority.
* Future E2EE can encrypt the entire payload without redesigning relay routing.
* Tests can assert payload opacity by sending unknown business actions through the relay.

### Decision 6: Pairing Credential Model?

Accepted answer: MVP uses one-time pairing code plus session secret.

Flow:

```text
mobilevc public
  -> local process generates sessionId + pairingSecret
  -> local process registers the relay session
  -> terminal/local page displays QR
  -> mobile/web scans QR and submits sessionId + pairingSecret to relay
  -> relay verifies the secret and assigns clientId
  -> pairingSecret is consumed immediately
```

Constraints:

* `pairingSecret` must have at least 128 bits of entropy.
* Pairing TTL defaults to 5 minutes.
* Successful pairing consumes the secret immediately.
* Expired pairing secrets are rejected.
* Relay logs must not include pairing secrets.
* MVP does not include persistent trusted devices.
* Restarting `mobilevc public` creates a new relay session and secret.

Why:

* No account system is required for MVP.
* QR/code leakage window is bounded.
* Scope stays focused on relay message forwarding.
* Long-lived device trust can be added later with explicit revoke UX.

### Decision 7: MVP Encryption Boundary?

Accepted answer: MVP does not implement full end-to-end encryption, but the relay protocol must be E2EE-ready.

MVP security layer:

```text
TLS/WSS
+ sessionId
+ one-time pairingSecret
+ opaque payload
+ no relay payload logs
```

Out of MVP:

* Noise/X25519 key exchange.
* Device long-term public/private keys.
* Key rotation.
* Multi-device key synchronization.
* Key recovery.

Envelope must reserve encryption metadata:

```json
{
  "version": 1,
  "sessionId": "...",
  "clientId": "...",
  "direction": "client_to_agent",
  "messageId": "...",
  "contentType": "mobilevc.ws.v1",
  "encryption": "none",
  "payloadEncoding": "base64url",
  "payload": "..."
}
```

Future E2EE should only change `encryption` and `payload`, not relay routing.

Why:

* Relay MVP can ship without key-management scope explosion.
* Relay still cannot inspect MobileVC business semantics.
* E2EE remains a protocol evolution, not a relay rewrite.

### Decision 8: LAN Compatibility Boundary?

Accepted answer: relay mode is additive and must not change the default LAN/local path.

Rules:

* `mobilevc start` keeps existing local/LAN behavior.
* Existing Flutter host/port direct connection remains supported.
* Relay mode is entered only by `mobilevc public` or explicit relay configuration.
* Direct LAN mode must not require relay availability, account login, public internet, or pairing.
* Direct public mode may remain available as advanced/self-hosted, but relay mode must not replace LAN mode.

### Decision 9: Relay MVP Guardrails?

Accepted answer: fix the following MVP guardrails.

1. Relay server shape: implement project-owned self-hosted cloud relay first, likely `cmd/relay` and `internal/relay`.
2. Transport: WebSocket only. Do not add SSE, WebRTC, gRPC, or HTTP polling in MVP.
3. Relay HTTP surface: relay must not expose command, file, download, or MobileVC backend HTTP routes.
4. Controller concurrency: one relay session allows one active controller client in MVP.
5. Viewer mode: no read-only viewer mode in MVP.
6. Direct public mode: keep it as advanced/self-hosted; relay mode is the recommended public UX.
7. Accounts: no account/login system in MVP; use `sessionId`, one-time `pairingSecret`, and relay-assigned `clientId`.
8. Failure strategy: relay failure must be explicit and must not silently fall back to direct public exposure.

Why:

* These constraints keep relay MVP testable and bounded.
* Single active controller avoids ambiguous permission approval and input ownership.
* No silent fallback prevents security expectations from changing at runtime.
* Self-hosted cloud relay lets protocol tests and real VPS deployment land before any managed service commitment.

### Decision 10: Cloud Relay Deployment Boundary?

Accepted answer: relay may be deployed on a cloud server, but it remains a standalone relay service, not a MobileVC backend.

Cloud relay may hold:

* relay listener address
* public relay URL
* short-lived relay sessions
* short-lived pairing state
* client routing metadata

Cloud relay must not hold:

* MobileVC `AUTH_TOKEN`
* APNs keys
* local workspace paths
* Claude/Codex environment
* file access permissions
* command execution capability

Example cloud relay config:

```text
RELAY_ADDR=:9000
RELAY_PUBLIC_URL=wss://relay.example.com
RELAY_PAIRING_TTL=5m
RELAY_SESSION_TTL=optional
```

Expected deployment targets:

* VPS
* Docker
* systemd
* reverse proxy such as Caddy/Nginx

Why:

* Public internet traffic reaches relay, not the user's local MobileVC backend.
* A compromised relay should not immediately become local shell/file access.
* Users can self-host before any official managed relay exists.

### Decision 11: Local Relay Agent Placement?

Accepted answer: local relay agent runs inside the Go backend, not the Node launcher.

Responsibilities:

* Node launcher:
  * parses `mobilevc public`
  * passes relay config to Go backend
  * displays pairing QR/link
  * does not proxy long-lived MobileVC traffic
* Go backend:
  * opens outbound WebSocket connection to cloud relay
  * registers relay session and pairing secret
  * bridges opaque relay payloads into existing gateway/session handling
  * owns reconnect behavior and relay health state

Why:

* Gateway/session/permission/file authority already lives in Go.
* Long-lived relay traffic has fewer moving pieces without a Node traffic bridge.
* Go tests can cover relay bridge behavior directly.
* Packaged server binary keeps public mode capability complete.

### Decision 12: Gateway Bridge Strategy?

Accepted answer: extract a shared gateway connection/message-loop boundary instead of faking an HTTP/WebSocket request for relay traffic.

Do not implement relay bridge by constructing fake `http.Request` or fake gorilla WebSocket state.

Preferred direction:

```go
type ClientConn interface {
    ReadJSON(v any) error
    WriteJSON(v any) error
    Close() error
    RemoteAddr() string
    Origin() string
}
```

Then:

```text
browser gorilla websocket -> ClientConn adapter -> gateway message loop
relay payload stream      -> ClientConn adapter -> same gateway message loop
```

Why:

* Avoid duplicating MobileVC protocol handling.
* Avoid relay-specific fake HTTP/WebSocket state.
* Makes relay tests target the same gateway behavior as direct WebSocket.
* Creates a path to reduce the current large `gateway.ServeHTTP` surface gradually.

Implementation rule:

* Extract the minimum connection boundary needed for relay support.
* Do not rewrite all gateway behavior in one step.
* Keep existing direct WebSocket behavior covered by regression tests.

### Decision 13: Flutter Connection UX?

Accepted answer: add a connection mode switch in the existing connection configuration UI.

Modes:

```text
Direct/LAN
Relay
```

Direct/LAN fields remain:

* host
* port
* token
* http/https transport
* trusted file roots

Relay fields:

* relay URL
* pairing code or QR scan result

Rules:

* Do not overload `host` with relay URL or pairing data.
* Do not remove existing direct/LAN fields.
* QR scan may auto-select Relay mode when the scanned payload is a relay pairing payload.
* Direct/LAN remains the default for existing saved configs unless the user explicitly selects Relay.

Why:

* Existing LAN users keep their current workflow.
* Relay semantics remain separate from direct host/port semantics.
* Avoids repeating earlier host/port ambiguity bugs.

### Decision 14: Relay URL Source?

Accepted answer: MVP relay URL priority is:

```text
1. CLI flag: mobilevc public --relay wss://relay.example.com
2. saved launcher relay URL
3. development default: ws://127.0.0.1:9000
```

Rules:

* Do not hardcode an official managed relay URL in MVP.
* `mobilevc public --relay ...` saves the relay URL for future `mobilevc public` runs.
* Relay URL must be a WebSocket URL: `wss://...` for public relay, or `ws://...` only for loopback/LAN development relay URLs.
* `http://` and `https://` relay inputs must be rejected with a clear error instead of guessed or silently rewritten.
* Saved relay URL must be visible in status/logs without printing secrets.
* Flutter relay pairing payload should include the effective relay URL.

Why:

* Self-hosted cloud relay works before official managed relay exists.
* Local development can run without cloud infrastructure.
* User does not need to repeat the relay URL after first setup.

### Decision 15: Relay Pairing Data Handoff?

Accepted answer: Go backend generates pairing data and emits a launcher-readable local status event.

Rules:

* Go backend owns `sessionId`, `pairingSecret`, `agentReconnectSecret`, and expiry generation.
* Node launcher must not generate or proxy relay credentials.
* Go backend emits a structured local-only status event containing relay URL, `sessionId`, `pairingSecret`, and expiry for launcher display.
* Preferred event format is stdout JSONL with a `mobilevc.relay.` type prefix.
* Pairing-ready event schema:
  ```json
  {"type":"mobilevc.relay.pairing_ready","relayUrl":"wss://relay.example.com","sessionId":"rs_xxx","pairingSecret":"secret_xxx","expiresAt":1760000000}
  ```
* `expiresAt` is a Unix timestamp in seconds.
* Unknown `mobilevc.relay.*` event fields must be ignored by launcher for forward compatibility.
* The event may be emitted on backend stdout/stderr or a local status pipe/file controlled by the launcher, but not through the cloud relay.
* If a file is used, it must be owner-only and deleted after launcher reads it or backend exits.
* Launcher may display QR/link from that local event but must not persist `sessionId` or `pairingSecret`.
* Launcher logs must redact `pairingSecret` unless rendering the deliberate one-time QR/link.

Why:

* Go owns relay connection state and reconnect secrets.
* Launcher still owns UX without becoming a traffic proxy or credential authority.
* A local event avoids adding public HTTP/session/debug endpoints.

### Decision 16: Relay Session Lifecycle?

Accepted answer:

```text
pairing TTL: 5 minutes
relay session TTL: follows local agent connection
agent disconnect grace period: 60 seconds
after grace period: close relay session and disconnect clients
```

Rules:

* Pairing can expire while the local agent session remains active.
* Agent reconnect during the grace period restores the relay session.
* After the grace period, clients must re-pair through a new session.
* Relay must not keep orphaned sessions indefinitely.

Why:

* Short network drops should not immediately break mobile control.
* Exited local agents should not leave public relay sessions alive.
* MVP has no account system, so lifecycle must be short and explicit.

### Decision 17: Relay Session Vs Local AI Session?

Accepted answer: relay session lifecycle is separate from local MobileVC/Codex/Claude session lifecycle.

Rules:

* Relay session expiry must not delete or mutate local MobileVC sessions.
* Relay session expiry must not delete Codex/Claude history or resume metadata.
* After re-pairing, the client may load existing local sessions through normal gateway/session APIs.
* Relay stores no Codex/Claude session state.
* Relay is a public transport channel, not the source of truth for AI/runtime state.

Why:

* Codex/Claude session state already belongs to the local backend/session store.
* Re-pairing should restore public connectivity, not restart AI work.
* Relay should be safe to discard without losing user work.

### Decision 18: Pairing QR Format?

Accepted answer: relay pairing QR uses a custom MobileVC URI, not an ordinary HTTPS backend URL.

Format:

```text
mobilevc://relay/v1?relay=wss%3A%2F%2Frelay.example.com&session=SESSION_ID&secret=PAIRING_SECRET&exp=1760000000
```

Fields:

* `relay`: relay WebSocket URL, such as `wss://relay.example.com`
* `session`: relay session ID
* `secret`: one-time pairing secret
* `exp`: pairing expiry as Unix timestamp

Flutter parse order:

```text
1. Try `mobilevc://relay/v1`.
2. If matched, select Relay mode and store relay URL/session/secret in relay fields.
3. If not matched, fall back to existing direct launch URI parsing.
```

Rules:

* Relay QR must not use direct backend `http(s)://host:port?token=...` format.
* Relay QR must not update direct `host`, `port`, or `token` fields.
* Direct/LAN QR remains the existing HTTP launch URL.
* HTTPS app links can be added later as an additional format, not MVP default.

Why:

* Prevents relay URL from being mistaken for a direct backend host.
* Keeps Direct/LAN and Relay configs separate.
* QR remains copyable/debuggable as a plain URI.

### Decision 19: Relay File Download Boundary?

Accepted answer: Relay MVP does not support HTTP file download.

Rules:

* Relay mode supports file browsing and text preview through existing WebSocket actions such as `fs_list` and `fs_read`.
* Relay mode must not expose or proxy the backend `/download` HTTP route.
* Relay mode must not silently fall back to direct backend HTTP download.
* Flutter must disable the file download action in Relay mode and show: `Relay 模式暂不支持下载`.
* Binary/file download over relay requires a separate future WebSocket chunking protocol and is out of MVP.

Why:

* `/download` is an HTTP route on the local backend and would require exposing or proxying local file access through the public relay.
* Relay MVP's security boundary is opaque WebSocket forwarding only.
* Explicitly disabling download avoids misleading users into thinking direct public access is still required.

### Decision 20: Relay Payload Encoding?

Accepted answer: relay payload is base64url-encoded bytes.

Envelope includes:

```json
{
  "version": 1,
  "sessionId": "rs_xxx",
  "clientId": "rc_xxx",
  "direction": "client_to_agent",
  "messageId": "msg_xxx",
  "contentType": "mobilevc.ws.v1",
  "encryption": "none",
  "payloadEncoding": "base64url",
  "payload": "eyJhY3Rpb24iOiJleGVjIn0"
}
```

Rules:

* `payload` must be base64url-encoded bytes.
* `payloadEncoding` must be explicit.
* Relay must not decode payload except optional length validation.
* Relay logs must not include payload.
* MVP maximum decoded payload size is 1 MiB.

Why:

* Relay cannot casually inspect MobileVC business JSON.
* Future E2EE ciphertext can use the same field and encoding.
* Go and Dart both support base64url.
* Explicit size limits prevent unbounded relay memory pressure.

### Decision 21: Relay HTTP Health Surface?

Accepted answer: relay server exposes only minimal health/version HTTP endpoints.

Allowed:

```text
GET /healthz -> 200 ok
GET /version -> version/build metadata
```

Not allowed in MVP:

* `/session`
* `/debug`
* payload inspection endpoints
* command/file/download endpoints
* metrics containing session IDs, client IDs, pairing state, or payload metadata

Why:

* VPS, Docker, systemd, and reverse proxy deployments need health checks.
* Minimal endpoints do not expose relay runtime state.
* Keeps relay HTTP surface intentionally small.

### Decision 22: Relay Session Persistence?

Accepted answer: Relay MVP stores sessions in memory only.

Rules:

* Relay restart closes all relay sessions.
* Clients must re-pair after relay restart.
* Local MobileVC/Codex/Claude sessions remain unaffected.
* No database, disk persistence, cache restore, or session migration in MVP.
* Pairing secrets and client routing state are memory-only.

Why:

* Relay sessions are short-lived public transport channels.
* Local backend is the source of truth for AI/runtime state.
* Persistence adds cleanup, encryption, migration, and leakage risks.
* MVP can be simpler and safer with explicit re-pairing after relay restart.

### Decision 23: Relay Public Abuse Protection?

Accepted answer: add relay-side connection and pairing protection for public deployments.

Defaults:

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
```

Rules:

* These limits apply only to the relay service.
* Direct/LAN mode must not be affected by relay limits.
* `/relay/agent` must complete `agent.register` within the agent register timeout.
* `/relay/client` must complete `client.pair` within the pairing handshake timeout.
* Pairing failures are counted by `sessionId + remoteAddr`.
* Pairing rejection errors must be intentionally vague, such as `pairing rejected`.
* Relay must not reveal whether a session is unknown, expired, or has a wrong secret.
* Relay must enforce maximum sessions, maximum agent connections, maximum client connections, and maximum connections per remote address.
* Relay must use ping/pong deadlines and close connections that miss pong deadlines.
* Relay must reject control frames larger than `RELAY_MAX_CONTROL_FRAME_BYTES`.
* `relay.forward` is governed by decoded payload size and `RELAY_FORWARD_QUEUE_SIZE`, not by control-frame size.
* Limit failures must be explicit and logged as metadata only.

Why:

* Public relay deployments must survive idle sockets, repeated pairing attempts, and connection floods.
* Vague pairing errors reduce session enumeration value.
* These limits protect the cloud relay without changing local MobileVC direct behavior.

### Decision 24: Relay Reverse Proxy Client IP?

Accepted answer: relay uses socket remote address by default and trusts forwarded client IP headers only from configured trusted proxies.

Rules:

* Default client identity for per-IP limits is the socket `RemoteAddr`.
* Relay must not trust `X-Forwarded-For`, `X-Real-IP`, or `Forwarded` headers by default.
* Operators may configure trusted proxy CIDRs for Caddy/Nginx/load balancer deployments.
* Forwarded headers are used only when the socket remote address is in a trusted proxy CIDR.
* When multiple forwarded IPs exist, relay uses the first untrusted client IP according to the configured proxy chain rule.
* Invalid forwarded headers are ignored and logged as metadata only.
* Public managed relay defaults should require explicit trusted proxy config before honoring forwarded headers.

Why:

* Per-IP limits are security controls and cannot depend on spoofable headers.
* Reverse proxy deployments still need correct client identity for rate limits.
* Explicit trusted proxy config keeps local/dev deployments simple and safe.

### Decision 25: Relay Auth And Pairing Persistence?

Accepted answer: Relay mode does not use direct backend `AUTH_TOKEN` and does not persist one-time pairing secrets.

Rules:

* Relay QR must not contain direct backend `AUTH_TOKEN`.
* Relay config must not store direct backend `AUTH_TOKEN`.
* Relay envelope and control frames must not contain direct backend `AUTH_TOKEN`.
* Pairing success makes the relay connection the authenticated controller transport.
* The local Go relay adapter enters the shared gateway loop as a trusted local transport.
* Flutter may keep scanned `sessionId` and `pairingSecret` in memory only until pairing succeeds, fails, expires, or the pairing screen is closed.
* Flutter must not write `sessionId` or `pairingSecret` to SharedPreferences or other persistent storage.
* Flutter may persist only the relay URL for future runs.
* Local launcher may persist only relay URL/config, not one-time pairing secrets.

Why:

* Direct backend token is for local/LAN `/ws?token=...`, not relay pairing.
* One-time pairing secrets have no recovery value after pairing.
* Keeping relay credentials out of persistent client config reduces leakage and stale-secret confusion.

### Decision 26: Relay Control Frames Vs Forward Frames?

Accepted answer: relay control frames and relay forward frames are separate.

Rules:

* `agent.register`, `agent.registered`, `client.pair`, `client.paired`, `relay.error`, `relay.ping`, and `relay.pong` are relay control frames.
* Every control frame must include `type` and `version`; MVP frame version is `1`.
* `client.pair` does not require `clientId` because `clientId` is assigned only after pairing succeeds.
* `relay.forward` is a forward/data frame, not a control frame.
* Only `relay.forward` carries the base64url MobileVC payload envelope.
* Relay must not put control-frame credentials inside MobileVC payload envelopes.
* `RELAY_MAX_CONTROL_FRAME_BYTES` applies to control frames only, not `relay.forward`.
* Relay must not decode `relay.forward.payload` except for size validation.
* `agent.register` sends only `pairingSecretHash` and `agentReconnectSecretHash`, not plaintext secrets.
* Secret hashes are `base64url(SHA-256(UTF-8 secret))` and must be compared in constant time.
* `client.pair` and `agent.reconnect` may send plaintext one-time/reconnect secrets only in their dedicated relay control frames over the WebSocket connection; relay must hash for verification and never log or persist plaintext.

Why:

* Pairing happens before a client identity exists.
* Keeping control frames outside the opaque payload boundary avoids leaking relay auth into MobileVC business traffic.
* Future E2EE only needs to wrap forwarded payloads, not relay control metadata.

### Decision 27: Relay Controller Disconnect?

Accepted answer: controller/client disconnect requires re-pairing in MVP.

Rules:

* MVP does not issue `clientReconnectSecret`.
* Pairing secret is consumed after successful `client.pair` and cannot be reused.
* If the active controller WebSocket disconnects, relay closes the controller side of the relay session.
* The local agent may remain connected until its own lifecycle ends.
* A new controller must re-pair through a newly displayed pairing secret/session from `mobilevc public`.
* Flutter refresh, app restart, or network loss cannot silently restore controller identity in MVP.
* Relay must emit an explicit controller-disconnected status/error when possible.

Why:

* This keeps MVP credential state simple and avoids persisting client reconnect credentials.
* Agent session and local AI/runtime state remain alive; only public controller transport must be re-paired.
* A future client reconnect feature can add explicit short-lived `clientReconnectSecret` and revoke UX.

### Decision 28: Relay Adapter Gateway Identity?

Accepted answer: relay adapter must not reuse direct HTTP `AUTH_TOKEN` or Origin allowlist logic.

Rules:

* Direct `/ws` keeps HTTP query token and browser Origin handling.
* Relay traffic enters the gateway only after relay-side pairing succeeds.
* Relay adapter must not construct fake `http.Request` values.
* Relay adapter must provide explicit connection metadata such as `transport=relay`, `remoteAddr=relay:<sessionId>/<clientId>`, and `origin=relay`.
* Gateway logic must not apply direct public Origin allowlist checks to relay adapter traffic.
* Relay transport identity authorizes only the WebSocket transport; MobileVC business permission flows remain local and unchanged.

Why:

* Browser Origin protects direct public HTTP/WebSocket exposure, not paired relay forwarding.
* Synthetic identity avoids leaking cloud relay HTTP details into gateway business logic.
* Local permission and session behavior stays the source of truth after the transport is accepted.

### Decision 29: Relay Agent Registration And Reconnect?

Accepted answer: agent registration and reconnect are explicit relay control flows with single active agent ownership.

Rules:

* `/relay/agent` accepts only `agent.register` or `agent.reconnect` as the first control frame.
* Initial `agent.register` creates the relay session when capacity allows.
* `agent.reconnect` requires `sessionId` plus `agentReconnectSecret`.
* Relay stores only a verifier for `agentReconnectSecret`, never plaintext.
* A relay session has one active agent connection.
* A reconnect with a valid `agentReconnectSecret` during the grace period replaces the disconnected agent connection.
* A second live `agent.register` for an existing active session is rejected.
* A second live `agent.reconnect` for an already connected agent is rejected unless the previous agent connection has already been marked disconnected.
* Agent register/reconnect failures use vague errors and metadata-only logs.
* Agent register/reconnect timeout or capacity failure must not create a partial relay session.

Why:

* Agent side is also internet-facing on the cloud relay and needs the same explicit control boundary as clients.
* Single active agent avoids split-brain routing and duplicate command streams.
* Reconnect must restore short network drops without allowing session takeover.

## Requirements

### P0 Relay Protocol

* Define a relay envelope with:
  * `type=relay.forward`
  * `version=1`
  * `sessionId`
  * `clientId`
  * `direction`
  * `messageId`
  * `contentType=mobilevc.ws.v1`
  * `encryption=none`
  * `payloadEncoding=base64url`
  * opaque `payload`
* Define stable JSON schemas for `agent.register`, `agent.registered`, `agent.reconnect`, `client.pair`, `client.paired`, `relay.error`, `relay.ping`, and `relay.pong`.
* Relay must route by authenticated `sessionId` and paired client identity.
* Relay must reject unknown sessions and unpaired clients.
* Relay session and pairing state must be memory-only in MVP.
* Relay must not log payload contents.
* Relay must treat payload as opaque bytes and never inspect MobileVC business action fields.
* Relay payload must use explicit `payloadEncoding=base64url`.
* Relay decoded payload size is limited to 1 MiB in MVP.
* `direction` is limited to `client_to_agent` or `agent_to_client`.
* `messageId` is required for metadata correlation, but relay must not persist, deduplicate, or replay messages by `messageId` in MVP.
* Relay control frame size is limited to `RELAY_MAX_CONTROL_FRAME_BYTES`.
* Relay control frames are separate from `relay.forward` forward/data frames.
* `relay.forward` is limited by decoded payload size, not control frame size.
* Forwarding queues default to `RELAY_FORWARD_QUEUE_SIZE=64` frames per direction per session.
* Queue overflow must emit `relay.error`, log metadata only, and close the affected connection.
* `relay.error` must use a stable `{type, code, message}` schema with safe public messages.
* `client.pair` must not require `clientId`; `clientId` is assigned after successful pairing.
* Pairing secrets must be one-time use and expire after 5 minutes by default.
* Pairing secrets must have at least 128 bits of entropy.
* A relay session must allow only one active controller client in MVP.
* Controller/client disconnect requires re-pairing in MVP; no client reconnect secret is issued.
* Agent disconnect grace period defaults to 60 seconds.
* Relay closes sessions that exceed the disconnect grace period.
* Relay enforces pairing handshake timeout, pairing failure limits, connection caps, and ping/pong deadlines.
* Relay enforces agent register timeout, agent connection caps, and single active agent per relay session.
* Relay per-IP limits use socket remote address unless trusted proxy CIDRs are configured.

### P0 Local Agent Connection

* Add a local outbound relay client in backend.
* The local process connects to relay over WSS.
* Relay URL priority is CLI flag, saved launcher config, then development default `ws://127.0.0.1:9000`.
* Public relay URLs must use `wss://`; insecure `ws://` is allowed only for loopback or LAN development relay URLs.
* Allowed insecure `ws://` hosts are loopback, localhost, RFC1918 private IPv4 ranges, IPv6 loopback, IPv6 unique-local, and IPv6 link-local addresses.
* `http://` and `https://` relay URL inputs must be rejected with a clear error.
* Go backend emits a local launcher-readable pairing status event after relay registration.
* Local pairing status uses the `mobilevc.relay.pairing_ready` JSONL schema.
* It registers a relay session using generated pairing and agent reconnect secrets.
* Agent reconnect uses `agent.reconnect` with `sessionId` and `agentReconnectSecret`.
* If relay disconnects, reconnect with bounded exponential backoff and explicit logs.
* Relay connection failure must not start or expose direct public mode automatically.
* Node launcher must not proxy long-lived relay traffic in MVP.
* Relay payload handling must reuse the same gateway message-loop behavior as direct WebSocket via a shared connection abstraction.
* Relay adapter must bypass direct query-token and Origin checks only after relay pairing succeeds.
* Relay adapter must provide explicit relay connection metadata instead of fake HTTP request state.
* Agent reconnect within the relay grace period should restore the session without requiring re-pairing.
* Relay session expiry must not delete or mutate local MobileVC/Codex/Claude sessions.

### P0 Flutter/Web Client Connection

* Add a connection mode that targets the relay URL instead of the local backend URL.
* Client provides pairing code/secret from QR or in-memory pairing state only.
* Existing protocol messages should be forwarded unchanged as much as possible.
* Relay settings must not be stored in the direct `host` field.
* Relay pairing QR must use `mobilevc://relay/v1` and be parsed before direct launch URI parsing.
* Flutter may persist only the relay URL; it must not persist `sessionId` or `pairingSecret`.

### P1 Security

* Do not expose local backend `/ws`, `/download`, or HTTP routes to public traffic in relay mode.
* Relay server must not implement, decode, or branch on MobileVC business actions such as `fs_read`, `fs_list`, `exec`, or equivalent command/file surfaces.
* Relay may forward opaque payloads that contain those actions for the local backend.
* Relay server may expose only `/healthz` and `/version` as non-WebSocket HTTP endpoints in MVP.
* Relay WebSocket upgrade endpoints are `/relay/agent` and `/relay/client`.
* Relay QR, config, envelope, and control frames must not contain direct backend `AUTH_TOKEN`.
* Relay logs must include routing metadata only, not commands, prompts, tokens, file paths, or payloads.
* Relay must not reveal whether pairing failed because the session is unknown, expired, or the secret is wrong.
* Pairing secrets must be generated with cryptographic randomness.
* Pairing secrets must expire if not used.
* Pairing secrets must be consumed immediately after successful pairing.
* Do not add persistent trusted devices in MVP.
* Persisted relay launcher config must be written with owner-only permissions.
* One-time pairing secrets must not be persisted by launcher or Flutter.
* Prepare envelope format for future E2EE.

### P1 UX

* `mobilevc start` remains local/LAN.
* `mobilevc public` starts relay mode.
* Relay mode must be additive and must not alter direct LAN connection behavior.
* Flutter connection UI separates Direct/LAN mode from Relay mode.
* Relay mode disables HTTP file download with `Relay 模式暂不支持下载`.
* Direct public mode remains advanced/self-hosted, not the recommended public path.
* Relay failures must produce explicit errors, not silent direct-public fallback.
* If no relay config exists, show one clear setup prompt or generated local relay default.
* Avoid requiring users to type `PUBLIC_EXPOSURE_MODE`, `ALLOWED_ORIGINS`, `WSS`, or reverse proxy settings.
* Existing direct public mode remains available but documented as advanced.

### P2 Observability

* Show relay connection status in launcher logs and Flutter UI.
* Expose clear user-facing errors for relay unreachable, pairing rejected, relay capacity reached, and local backend unavailable.
* Add smoke tests for reconnect and message forwarding.

## Proposed Implementation Plan

1. Define relay envelope structs and protocol tests.
2. Add a minimal Go relay server package or command for local/self-hosted cloud testing.
3. Add local outbound relay client that bridges relay frames to existing backend WebSocket handling.
4. Extract a minimal gateway `ClientConn` boundary so direct WebSocket and relay payloads share message handling.
5. Add Flutter relay connection mode and QR pairing flow.
6. Add launcher `mobilevc public` UX on top of relay mode.
7. Add security tests:
   * unknown session rejected
   * unpaired client rejected
   * second active controller rejected
   * payload not logged
   * relay forwards unknown business payload unchanged
   * expired pairing rejected
   * reused pairing rejected
   * pairing secret not logged
   * relay control frames follow the fixed JSON schemas
   * `agent.register` sends only secret hashes, not plaintext pairing or reconnect secrets
   * relay secret hash verification uses constant-time comparison
   * relay QR/config/envelope/control frames do not include direct backend `AUTH_TOKEN`
   * client pairing secret is not persisted after pairing success, failure, expiry, or closing pairing UI
   * `client.pair` is a control frame and does not require `clientId`
   * public relay URLs reject insecure `ws://` except loopback or LAN development URLs
   * `http://` and `https://` relay URL inputs are rejected
   * launcher receives pairing data only from local backend status events
   * launcher parses `mobilevc.relay.pairing_ready` JSONL schema
   * `relay.forward` is not limited by `RELAY_MAX_CONTROL_FRAME_BYTES`
   * relay adapter does not fake HTTP request state
   * pairing failures are rate-limited per session and remote address
   * relay connection/session caps are enforced
   * forwarded IP headers are ignored unless the socket remote address is trusted
   * oversized control frames are rejected
   * agent register timeout is enforced
   * duplicate active agent registration/reconnect is rejected
   * relay ping/pong timeout closes dead connections
   * relay failure does not trigger direct-public fallback
8. Add reconnect tests and a local smoke script.
9. Document direct-public mode as advanced and relay mode as recommended.

## Acceptance Criteria

* [ ] `mobilevc start` still starts local/LAN mode.
* [ ] Existing LAN direct Flutter/Web connection still works without relay config.
* [ ] `mobilevc public` starts relay mode without requiring public DNS or Origin config.
* [ ] `mobilevc public --relay <url>` saves and reuses the relay URL.
* [ ] Public relay URLs require `wss://`; `ws://` is allowed only for loopback or LAN development relay URLs.
* [ ] `http://` and `https://` relay URL inputs are rejected with clear errors.
* [ ] Local backend initiates outbound WSS relay connection.
* [ ] Local backend emits a launcher-readable local pairing status event.
* [ ] Pairing status event follows `mobilevc.relay.pairing_ready` JSONL schema.
* [ ] Launcher displays pairing QR/link from the local backend event and does not generate relay credentials.
* [ ] Flutter/Web can pair through relay and exchange existing MobileVC protocol messages.
* [ ] Flutter connection config has separate Direct/LAN and Relay modes.
* [ ] Relay URL/pairing data is not stored in the direct host field.
* [ ] Flutter persists relay URL only and does not persist `sessionId` or `pairingSecret`.
* [ ] Relay QR uses `mobilevc://relay/v1` and does not update direct host/port/token fields.
* [ ] Relay QR, config, envelope, and control frames do not contain direct backend `AUTH_TOKEN`.
* [ ] Relay pairing success makes the relay connection the authenticated controller transport.
* [ ] `client.pair` is a control frame that runs before `clientId` is assigned.
* [ ] Relay and direct WebSocket share the same gateway message-loop behavior through a minimal connection abstraction.
* [ ] Relay adapter enters gateway only after relay pairing succeeds.
* [ ] Relay adapter does not fake `http.Request` state and does not use direct Origin allowlist checks.
* [ ] Relay cannot call local backend routes directly.
* [ ] Relay exposes no command/file/download HTTP surface.
* [ ] Relay non-WebSocket HTTP surface is limited to `/healthz` and `/version`.
* [ ] Relay WebSocket upgrade endpoints are limited to `/relay/agent` and `/relay/client`.
* [ ] Relay mode supports `fs_list` and `fs_read` over WebSocket but disables HTTP file download.
* [ ] Relay mode never falls back to direct backend `/download`.
* [ ] Relay session rejects a second active controller client in MVP.
* [ ] Controller/client disconnect requires re-pairing in MVP.
* [ ] Relay does not log payload contents.
* [ ] Relay forwards opaque payload without parsing MobileVC business actions.
* [ ] Relay payload uses `payloadEncoding=base64url` and enforces a 1 MiB decoded size limit.
* [ ] Relay envelope includes `type=relay.forward`, `version=1`, `contentType=mobilevc.ws.v1`, and `encryption=none`.
* [ ] Relay control frames follow the fixed JSON schemas and include `type` plus `version`.
* [ ] One-time pairing secret expires after 5 minutes and is consumed after successful pairing.
* [ ] `agent.register` sends only `pairingSecretHash` and `agentReconnectSecretHash`, not plaintext secrets.
* [ ] Relay verifies pairing and reconnect secrets with constant-time hash comparison.
* [ ] Client pairing must complete within `RELAY_PAIRING_HANDSHAKE_TIMEOUT`.
* [ ] Pairing failures are capped by `RELAY_PAIRING_MAX_FAILURES_PER_SESSION_IP`.
* [ ] Pairing rejection does not reveal whether session id, expiry, or secret caused the failure.
* [ ] Relay enforces `RELAY_MAX_SESSIONS`, `RELAY_MAX_CLIENT_CONNS`, and `RELAY_MAX_CONNS_PER_IP`.
* [ ] Relay enforces `RELAY_MAX_AGENT_CONNS`.
* [ ] Relay does not trust forwarded IP headers unless trusted proxy CIDRs are configured.
* [ ] Relay rejects control frames larger than `RELAY_MAX_CONTROL_FRAME_BYTES`.
* [ ] `relay.forward` is not rejected by the control-frame size limit and is governed by decoded payload size.
* [ ] Relay forwarding queue overflow emits `relay.error` and closes the affected connection.
* [ ] `relay.error` uses the stable safe `{type, code, message}` schema.
* [ ] Agent registration must complete within `RELAY_AGENT_REGISTER_TIMEOUT`.
* [ ] A relay session allows only one active agent connection.
* [ ] Agent reconnect requires `sessionId` plus `agentReconnectSecret`.
* [ ] Agent reconnect replaces only a disconnected agent during the grace period.
* [ ] Relay closes connections that miss ping/pong deadlines.
* [ ] Relay session survives local agent reconnect within 60 seconds.
* [ ] Relay session closes after local agent is disconnected for more than 60 seconds.
* [ ] Relay restart closes relay sessions and requires re-pairing.
* [ ] Relay session expiry does not delete or mutate local MobileVC/Codex/Claude sessions.
* [ ] After re-pairing, client can load existing local sessions through normal gateway/session APIs.
* [ ] LAN mode tests pass with relay disabled/unconfigured.
* [ ] Relay failure is explicit and does not fall back to direct public exposure.
* [ ] Focused backend, launcher, and Flutter tests pass.

## Out Of Scope For MVP

* Official production managed relay hosting.
* Billing/account management.
* Multi-user team permissions.
* Multiple active controller clients.
* Read-only viewer mode.
* Full E2EE key recovery.
* NAT traversal beyond outbound WebSocket relay.
* HTTP file download over relay.
* WebSocket chunked binary/file transfer over relay.

## Technical Notes

Likely areas:

* Launcher: `bin/mobilevc.js`
* Backend gateway: `internal/gateway/`
* Backend relay agent: likely `internal/relayclient/` or `internal/relay/agent`
* Runtime/session protocol: `internal/protocol/`, `internal/session/`
* New relay package/command: likely `internal/relay/` and `cmd/relay/`
* Flutter connection config: `mobile_vc/lib/core/config/`, `mobile_vc/lib/features/session/`

## Current Decision

Recommended next step is not more direct-public UX polishing. Build relay mode as the primary public path, keep direct public mode as advanced/manual.
