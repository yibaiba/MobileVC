# Local Security Surface Notes

## Initial Static Scan

This note records the first repo-local scan for the MobileVC security audit plan.

## High-Risk Surfaces

* WebSocket entrypoint: `internal/gateway/gateway.go`
  * Uses query token authentication.
  * `CheckOrigin` currently allows all origins.
  * WebSocket events can trigger session, file, slash command, skill, push-token, and ADB/WebRTC flows.

* File download route: `cmd/server/main.go`
  * `/download` checks query token, then resolves a user-supplied `path`.
  * Needs explicit review for workspace boundary, absolute paths, symlink escape, and arbitrary local file download.

* Command execution: `internal/engine/`, `internal/session/`, `internal/adb/`
  * Shell and PTY runners execute local commands by design.
  * Audit should focus on who can reach those paths, what user confirmation exists, and whether command strings are safely constructed.

* Client-side secrets and logs: `mobile_vc/lib/app/`, `mobile_vc/lib/core/config/`
  * Flutter config includes connection token and ADB ICE credential.
  * APNs device token currently appears in debug logging paths.

* Dependency and supply-chain inputs:
  * Go dependencies: `gorilla/websocket`, `pion/webrtc`, `apns2`, `modernc/sqlite`, `golang.org/x/*`.
  * npm package publishes platform-specific server binaries.
  * Flutter uses a vendored `flutter_webrtc` path dependency.

## Commands To Run During Audit

* `go test ./...`
* `go run golang.org/x/vuln/cmd/govulncheck@latest ./...`
* `npm audit --omit=dev`
* `npm audit`
* `cd mobile_vc && .fvm/flutter_sdk/bin/flutter analyze`
* `cd mobile_vc && .fvm/flutter_sdk/bin/flutter test`
* `cd mobile_vc && .fvm/flutter_sdk/bin/flutter pub outdated`

## Open Review Questions

* Should `/download` be intentionally allowed to read any local file when token is known, or should it be constrained to configured workspace roots?
* Should WebSocket Origin be restricted for browser clients while still allowing mobile/native clients that may not send a useful Origin?
* Should production/release Flutter builds suppress all token and device-token debug output?
* Should token transport move from URL query to header/subprotocol for WebSocket where platform support permits?
