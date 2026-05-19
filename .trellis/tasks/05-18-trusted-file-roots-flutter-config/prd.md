# trusted file roots from Flutter config

## Goal

Allow users to configure additional trusted file roots from Flutter connection settings, while keeping the backend as the final file-access authorization boundary for `/download`, `fs_read`, and `fs_list`.

## What I already know

* Backend now has `internal/fileaccess.Policy` and `RUNTIME_TRUSTED_FILE_ROOTS` for process startup configuration.
* `/download`, `fs_read`, and `fs_list` already route through the shared backend policy.
* Flutter `AppConfig` persists connection fields in SharedPreferences and `SessionController.connect()` sends bootstrap requests after WebSocket connect.
* Current UX requires editing backend environment variables, which is inconvenient for mobile/web users.
* This is a cross-layer feature: Flutter config → WebSocket action → backend policy → file APIs.

## Assumptions

* Flutter-configured trusted roots are per running backend process and should be applied after WebSocket connection succeeds.
* Backend must reject invalid/nonexistent roots explicitly; Flutter must not silently pretend success.
* Existing env roots remain supported and are combined with Flutter-configured roots.
* The user wants convenience, not a bypass of backend path validation.

## Requirements

* Add a persistent `trustedFileRoots` field to Flutter `AppConfig`.
* Add connection settings UI for additional trusted file roots.
* On connect and config save while connected, Flutter sends a WebSocket management action with the configured roots.
* Backend validates roots using the same path policy construction rules and updates the handler's active file policy only on success.
* Backend returns a structured result or error so failures are visible.
* Empty Flutter roots are valid and mean “use backend default/env roots only”.
* Tests cover serialization, WS payload, backend accept/reject, and existing file access behavior.

## Acceptance Criteria

* [ ] Flutter config round-trips trusted roots via JSON.
* [ ] Connecting sends trusted roots to backend before file list/read requests matter.
* [ ] Saving config while connected re-sends trusted roots.
* [ ] Backend accepts existing directory roots and rejects nonexistent roots without mutating active policy.
* [ ] `/download`, `fs_read`, and `fs_list` still reject paths outside effective roots.
* [ ] Focused Go and Flutter tests pass; backend build passes.

## Out of Scope

* Durable server-side config file management.
* Multi-client conflict resolution beyond “last authenticated config update wins”.
* Removing `RUNTIME_TRUSTED_FILE_ROOTS`.
* Permission prompts for every file read.

## Technical Plan

1. Backend
   * Extend `fileaccess.Policy` to remember base workspace root and environment roots.
   * Add `WithClientTrustedRoots` so client roots are validated and combined without losing env roots.
   * Add mutex-protected file policy update to gateway handler.
   * Add WS action `file_access_config` and result event `file_access_config_result`.

2. Flutter config/model
   * Add `trustedFileRoots` list to `AppConfig`, `copyWith`, `toJson`, `fromJson`.
   * Normalize by trimming and dropping blank entries.

3. Flutter controller/UI
   * Add a multiline text field in connection config for trusted roots, one path per line.
   * After WebSocket connect, send `file_access_config` before file browsing calls.
   * Re-send when saving config while already connected.

4. Tests
   * Go: policy combination, gateway action success/failure, no mutation on failure.
   * Dart: AppConfig JSON and SessionController payload.

## Grill Decision

Recommended answer: Flutter can configure trusted paths, but only as a request to the authenticated backend. The backend remains the source of truth and must validate every root and target path. This preserves convenience without reintroducing arbitrary file read.
