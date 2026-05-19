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
- Relay client entry: `relayclient.Run(ctx, cfg, gatewayHandler, relayclient.EmitPairingFile)`
- Relay capacity env: `RELAY_MAX_AGENT_CONNS`, `RELAY_MAX_CLIENT_CONNS`, `RELAY_MAX_CONNS_PER_IP`, `RELAY_FORWARD_QUEUE_SIZE`
- Relay liveness env: `RELAY_PING_INTERVAL`, `RELAY_PONG_TIMEOUT`, `RELAY_AGENT_GRACE_PERIOD`
- Trusted proxy env: `RELAY_TRUSTED_PROXY_CIDRS=<comma-separated-cidrs>`

#### 3. Contracts
- Relay server forwards only `relay.forward` envelopes with base64url payloads; it must not parse MobileVC business actions.
- `agent.register` sends only secret hashes; plaintext pairing secret is local-only and written through `RELAY_PAIRING_EVENT_PATH`.
- `client.pair` is the only place a client sends the one-time pairing secret.
- Direct backend `AUTH_TOKEN` must not appear in relay control frames, relay envelopes, relay QR URIs, relay logs, or relay event files.
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
- Invalid relay duration / integer / byte env value -> config error; do not silently fall back to defaults.
- Oversized decoded relay payload -> `relay.error` with `payload_too_large`.
- Forward with missing or mismatched `clientId` -> `relay.error` with `protocol_error`.
- First agent-to-client forward with an empty `clientId` after successful client pairing -> relay fills the current active `clientId`; wrong non-empty `clientId` still -> `protocol_error`.
- Missing `client.attached` before the relay websocket closes -> local relay client write returns the underlying read/close error.
- Per-IP or role capacity exceeded before upgrade -> HTTP 429.
- Bounded forward queue full -> `relay.error` with `queue_full`.
- Invalid `RELAY_TRUSTED_PROXY_CIDRS` -> relay startup config error.
- Pairing reject causes must stay indistinguishable to clients -> `pairing_rejected`.

#### 5. Good/Base/Bad Cases
- Good: backend writes pairing data to an owner-only temp file, launcher reads and deletes it, logs show only redacted URI.
- Good: relay behind a trusted reverse proxy enforces caps by forwarded client IP, while direct internet clients cannot spoof forwarded headers.
- Base: direct `/ws?token=...` path still performs token and origin checks.
- Bad: printing `mobilevc.relay.pairing_ready` JSON to stdout/stderr because server logs then retain the one-time secret.
- Bad: accepting a duplicate `agent.register` for an existing `sessionId` after disconnect; that bypasses reconnect-secret semantics.

#### 6. Tests Required
- Relay pairing, one-time secret consumption, URL validation, oversized payload, and opaque unknown business payload forwarding.
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


---

## Testing Requirements

<!-- What level of testing is expected -->

(To be filled by the team)

---

## Code Review Checklist

<!-- What reviewers should check -->

(To be filled by the team)
