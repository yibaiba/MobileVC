# public internet security hardening plan

## Goal

Harden MobileVC for public internet exposure by reducing token-leak blast radius, preventing cross-site WebSocket abuse, tightening shell-equivalent actions, and removing sensitive data from logs and local files.

## What I Already Know

* Existing security audit identified the highest-risk public deployment issues:
  * permissive WebSocket `CheckOrigin`
  * shared query-token authentication
  * authenticated file read/list/download
  * authenticated command execution
  * stale permission approval behavior
  * push token logs and file permissions
  * command/action logs that can capture secrets
* File access policy work is already in progress and should remain the foundation for `/download`, `fs_read`, and `fs_list`.
* MobileVC intentionally provides remote command execution; public hardening must treat the token as shell-equivalent rather than pretending it is a normal app session.
* OWASP WebSocket guidance recommends explicit Origin allowlists, message-level authorization, WSS, and avoiding sensitive token leakage in URLs/logs.
* OWASP logging guidance says access tokens, session IDs, passwords, keys, and similar secrets should not be logged directly.

## Assumptions

* Public deployment means HTTPS/WSS behind a real domain or reverse proxy, not only LAN.
* Native iOS/Android clients may not send browser `Origin`; they need a separate allowed path from browser clients.
* The first phase should reduce remote exploitation risk without redesigning the entire auth system.
* Existing local/LAN developer UX should keep working with explicit configuration.

## Grill Decision Block

Decision: How strict should public mode be?

Recommended answer: introduce an explicit `PUBLIC_EXPOSURE_MODE=true` or equivalent config. In public mode, fail closed unless all required security config is present:

* allowed browser origins configured
* WSS/HTTPS deployment documented or detected via reverse proxy headers
* token not printed automatically in public URLs/QR
* remote command execution requires fresh trust/confirmation
* sensitive logs redacted

Alternative A: harden defaults globally. This is safer but risks breaking local workflows unexpectedly.

Alternative B: only document warnings. This is not enough for public internet exposure.

## Requirements

### P0/P1 Public-Mode Gate

* Add an explicit public exposure mode.
* In public mode, server startup must fail if required security settings are missing.
* Public mode must be visible in bootstrap logs without printing secrets.

### WebSocket Origin And Auth

* Replace `CheckOrigin: true` with explicit browser Origin allowlist.
* Allow missing Origin only for native/non-browser clients after token authentication.
* Add tests for allowed origin, rejected origin, missing origin, and malformed origin.
* Reduce token-in-URL exposure:
  * short term: redact token-bearing URLs from logs/terminal output unless QR is explicitly requested
  * later: move WebSocket auth to first message or subprotocol where Flutter Web/native support allows

### File Access

* Keep shared file policy for `/download`, `fs_read`, and `fs_list`.
* Ensure trusted roots cannot be expanded silently by stale client state.
* Add public-mode guidance that trusted roots are sensitive and should be minimal.

### Remote Command Execution

* Treat `exec`, `ai_turn`, `input` resume flows, permission decisions, and slash exec commands as shell-equivalent.
* Add a public-mode guard for first command execution from a new remote address/origin.
* Require explicit confirmation or trust registration before command execution from untrusted remotes.
* Log remote address/origin/action type, but not full command text by default.

### Permission Decisions

* Remove stale approve auto-apply for mismatched `PermissionRequestID`.
* When stale, emit/return current pending prompt and require a fresh decision.
* Keep any compatibility behavior behind an explicit non-public flag only if absolutely needed.

### Sensitive Logs And Storage

* Remove APNs/push token values from Flutter logs.
* Store `push_tokens.json` with `0600`.
* Redact command/log fields for common secret patterns before writing logs or emitting diagnostic events.
* Confirm launcher config/state files containing auth token are written `0600`.

### Dependency And CI Guardrails

* Add or document `govulncheck ./...` and npm audit against `https://registry.npmjs.org`.
* Do not rely on `npmmirror` audit responses.

## Proposed Fix Order

1. Finish/commit filesystem boundary work already in progress.
2. WebSocket Origin allowlist + public-mode startup guard.
3. Token exposure reduction in launcher/QR/logs.
4. Public-mode remote command trust gate.
5. Strict permission request ID matching.
6. Push token log removal and `push_tokens.json` permission hardening.
7. Command/log redaction.
8. CI dependency security checks.

## Acceptance Criteria

* [x] Public mode refuses to start without explicit allowed origins.
* [x] Browser WebSocket from untrusted Origin is rejected.
* [x] Native/missing-Origin connection path remains supported when token is valid.
* [x] Token-bearing URLs are not printed by default in public mode.
* [ ] First shell-equivalent command from a new public remote requires explicit trust/confirmation.
* [x] Stale permission approve cannot approve a different current prompt.
* [x] Flutter release logs do not print push token values.
* [x] Push token file is written `0600`.
* [x] Command/action logs redact common secrets.
* [x] Focused backend and Flutter tests pass; backend build passes.

## Out Of Scope

* Full multi-user account system.
* OAuth/OIDC login.
* mTLS rollout.
* Enterprise policy engine.
* Cloud WAF configuration.

## Technical Notes

* WebSocket handler: `internal/gateway/gateway.go`
* Server config: `internal/config/config.go`
* Launcher URL/QR output: `bin/mobilevc.js`
* Command execution: `internal/gateway/gateway.go`, `internal/engine/`
* Permission decisions: `internal/gateway/gateway.go`
* Push token logs: `mobile_vc/lib/app/push_notification_service_mobile.dart`, `mobile_vc/lib/app/app.dart`
* Push token storage: `internal/data/file_store.go`
* File access policy: `internal/fileaccess/`, `cmd/server/main.go`, `internal/gateway/gateway.go`

## Research References

* `research/owasp-public-exposure-notes.md` — current OWASP WebSocket/logging guidance mapped to MobileVC public deployment risks.
