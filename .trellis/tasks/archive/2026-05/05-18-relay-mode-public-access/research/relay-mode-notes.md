# Relay Mode Notes

## Architecture Choice

MobileVC direct public mode exposes the user's local backend. Relay mode changes the public boundary:

```text
local backend -> relay <- client
```

Both local backend and client use outbound WebSocket connections. This avoids user-side DNS, TLS certificate, port forwarding, reverse proxy, and Origin allowlist setup.

## Security Notes

* Relay should authenticate both sides before routing.
* Relay payload should be opaque to the relay service.
* Logs should include session/client routing metadata only.
* Pairing secrets should expire and be generated with cryptographic randomness.
* Direct command/file authority remains local; relay must not implement command execution.

## MVP Tradeoff

Full end-to-end encryption is desirable, but MVP can start with TLS plus per-session pairing secret if the envelope keeps payload opaque. Avoid relay-side parsing of command semantics so E2EE can be added later without protocol redesign.

## UX Target

```bash
mobilevc public
```

First run should produce a pairing QR/link. Later runs should reuse saved relay settings.
