# MobileVC Security Audit Report

## Scope

Reviewed backend HTTP/WebSocket routes, filesystem access, command execution paths, Flutter client sensitive-data handling, launcher/package surface, and dependency checks.

## Executive Summary

The main security concern is not unauthenticated access; the backend consistently requires the shared `AUTH_TOKEN` for WebSocket and download paths. The larger risk is that once a token leaks or is obtained from LAN/terminal output/browser URL history, the token grants very broad local capabilities: arbitrary command execution through WebSocket actions and arbitrary local file read/list/download through filesystem routes.

## Findings

### P1: Shared token + permissive WebSocket Origin enables cross-site WebSocket abuse when token leaks

Evidence:

* `internal/gateway/gateway.go:64-69` sets `websocket.Upgrader.CheckOrigin` to always return `true`.
* `internal/gateway/gateway.go:118-123` authenticates WebSocket only with query `token`.
* `bin/mobilevc.js:841-857` prints local/LAN URLs and QR content containing `token`.
* `bin/mobilevc.js:874-883` builds launch URLs with `token` in query parameters.

Impact:

If an attacker obtains the token from terminal output, shell history, browser URL history, logs, screenshots, QR exposure, or same-LAN shoulder-surfing, any website can open a WebSocket to the backend because Origin is not restricted. That authenticated WebSocket can trigger high-impact actions including command execution and filesystem reads.

Recommended fix:

* Restrict Origin for browser requests to expected local/LAN origins and configured public HTTPS origin.
* Treat native/mobile clients separately: allow missing Origin only for non-browser clients if needed.
* Prefer a non-URL token transport where platform support allows it, such as Authorization header for HTTP APIs and WebSocket subprotocol or initial auth message for WebSocket.
* Reduce terminal/QR token exposure, or show QR only on demand.

### P1: Authenticated users can read/list/download arbitrary local files

Evidence:

* `cmd/server/main.go:143-174` handles `/download` by `filepath.Clean`, `filepath.Abs`, `os.Stat`, and `http.ServeFile` on user-supplied `path`; no workspace-root restriction exists.
* `internal/gateway/gateway.go:2264-2289` exposes `fs_list` and `fs_read` WebSocket actions.
* `internal/gateway/gateway.go:3403-3439` lists arbitrary `filepath.Abs(rawPath)` directories.
* `internal/gateway/gateway.go:3442-3470` reads arbitrary `filepath.Abs(rawPath)` file contents.

Impact:

Anyone with the shared token can read sensitive local files reachable by the MobileVC process, such as SSH keys, shell history, project secrets, environment files, and APNs credentials. This is especially risky when the backend is exposed to LAN or behind HTTPS reverse proxy.

Recommended fix:

* Define an explicit file-access policy: default to configured workspace root(s), with opt-in for broader filesystem access.
* Validate paths using `filepath.EvalSymlinks` plus `filepath.Rel` against allowed roots.
* Apply the same policy to `/download`, `fs_read`, and `fs_list`.
* Return explicit authorization errors when paths escape allowed roots.

### P1: Authenticated WebSocket can execute arbitrary local commands by design

Evidence:

* `internal/gateway/gateway.go:1461-1495` accepts `exec` action and dispatches `reqEvent.Command`.
* `internal/engine/exec_engine.go:48-49` passes `req.Command` into `newShellCommand` and sets caller-provided `cwd`.
* `internal/engine/pty_engine.go:334-339` starts PTY commands from `req.Command`.
* `internal/engine/shell.go:23-56` ultimately runs commands through shell specs such as `zsh -lc` / `sh -lc` on non-Windows.

Impact:

This is core product functionality, but from a security perspective the shared token is equivalent to local shell access as the user running MobileVC. Any token leak becomes remote command execution on the host.

Recommended fix:

* Document token as shell-equivalent.
* Consider an explicit "remote command execution enabled" setting for LAN/public deployments.
* Add optional allowlist modes for common safe actions.
* Require fresh user confirmation for first command execution from a new remote address/origin.
* Log remote address and action type, but avoid logging full commands when they may contain secrets.

### P2: Permission decisions can auto-apply stale approve to current pending prompt

Evidence:

* `internal/gateway/gateway.go:1756-1764` auto-applies an `approve` decision with stale `PermissionRequestID` to the current pending request.

Impact:

This improves UX for stale clients, but it weakens prompt identity. A delayed or replayed approve from an older visible prompt can approve a different current prompt in the same session. This is most concerning when combined with token leakage or multiple clients connected to the same session.

Recommended fix:

* Require matching `PermissionRequestID` for approvals.
* If stale, return/emit the current prompt and require a new explicit approve.
* Keep the relaxed behavior only behind a clearly named compatibility flag if truly required.

### P2: APNs/device push tokens are logged in Flutter client

Evidence:

* `mobile_vc/lib/app/push_notification_service_mobile.dart:28-30` logs initialized APNs token.
* `mobile_vc/lib/app/app.dart:152-154` logs refreshed push token.
* `mobile_vc/lib/app/app.dart:172-175` logs device push token.

Impact:

Device push tokens are sensitive routing identifiers. Logging them can leak tokens through device logs, CI logs, screenshots, crash reports, or support diagnostics.

Recommended fix:

* Remove token values from logs.
* If diagnostics need visibility, log only presence or a short redacted hash.
* Gate any detailed logging behind debug-only assertions and ensure release builds cannot print token values.

### P2: Push tokens are stored in plaintext with broad file permissions

Evidence:

* `internal/data/file_store.go:1125-1145` writes `push_tokens.json` using `os.WriteFile(..., 0644)`.
* `internal/data/file_store.go:1137-1138` stores token and platform plaintext.

Impact:

Local users on the same machine may read device push tokens depending on filesystem permissions and parent directory mode. This is lower risk than command/file access but should be hardened.

Recommended fix:

* Write `push_tokens.json` with `0600`.
* Consider encrypting or OS-keychain storage for production use.
* Avoid keeping stale tokens indefinitely.

### P2: Launcher state stores shared auth token in plaintext

Evidence:

* `bin/mobilevc.js:223-234` saves launcher config containing `authToken`.
* `bin/mobilevc.js:539-544` writes state containing `authToken`.

Impact:

The launcher needs the token for restart/QR, and `STATE_DIR` is created with `0700`, so this is partly mitigated. Still, plaintext token persistence means local compromise of the user account gives durable shell-equivalent backend access.

Recommended fix:

* Confirm all config/state writes use `0600` files, not only `0700` directory.
* Consider rotating token on restart or offering a one-session token mode.
* Avoid printing token-bearing URLs unless explicitly requested.

### P2: Command/action logs may capture secrets

Evidence:

* `internal/gateway/gateway.go:1468` logs full `reqEvent.Command` and a preview.
* `internal/gateway/gateway.go:1495` logs command preview.
* `internal/gateway/gateway.go:1744` logs permission prompt/fallback command preview.
* `internal/engine/exec_engine.go:71` emits command text into execution log.

Impact:

Users may run commands containing API keys, bearer tokens, passwords, or secret file paths. These can be persisted to logs/session records and exposed in UI or diagnostics.

Recommended fix:

* Redact common secret patterns before logs/events.
* Avoid logging full commands by default; keep full command only in local session history if required by UX.
* Add tests for redaction of `Authorization`, `*_TOKEN`, `api_key`, `password`, and key-like values.

### P3: Dependency scan status is mixed; npm mirror can hide audit results

Evidence:

* `go run golang.org/x/vuln/cmd/govulncheck@latest ./...` reported no reachable vulnerabilities. It noted 2 vulnerabilities in imported packages that are not called by current code.
* `npm audit --omit=dev --json` failed against `https://registry.npmmirror.com` because the audit endpoint is not implemented.
* `npm audit --omit=dev --json --registry=https://registry.npmjs.org` reported 0 vulnerabilities.
* `flutter pub outdated` shows several direct dependencies behind resolvable/latest versions, including `file_picker`, `flutter_local_notifications`, `mobile_scanner`, and `share_plus`.

Impact:

The dependency situation is acceptable from the commands run, but CI or developer machines using npmmirror may get false "audit failed" instead of vulnerability results.

Recommended fix:

* Run npm audit in CI against `https://registry.npmjs.org`.
* Add `govulncheck ./...` to CI.
* Periodically review Flutter dependency updates and vendor `flutter_webrtc`.

## Areas That Look Reasonable

* Backend refuses to start without `AUTH_TOKEN` (`internal/config/config.go:56-82`).
* Bootstrap logging uses `logx.AuthTokenSummary`, which does not print the token value (`internal/logx/logx.go:36-41`).
* Backend push-token registration logs session/platform only, not token (`internal/gateway/push.go:182-188`).
* npm production audit against official registry found 0 vulnerabilities.
* `govulncheck` found 0 reachable vulnerabilities.

## Recommended Fix Order

1. Add a shared filesystem access policy and apply it to `/download`, `fs_read`, and `fs_list`.
2. Tighten WebSocket Origin handling and reduce token-in-URL exposure.
3. Treat token as shell-equivalent in UX/docs and add optional remote-exec hardening.
4. Remove push token logs and harden push token file permissions.
5. Revisit stale permission approval behavior.
6. Add secret redaction for command and prompt logs.
7. Add CI dependency/security checks.
