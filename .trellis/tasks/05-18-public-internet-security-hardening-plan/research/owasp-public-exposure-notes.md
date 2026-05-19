# OWASP Public Exposure Notes

## Sources

* OWASP WebSocket Security Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/WebSocket_Security_Cheat_Sheet.html
* OWASP Logging Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
* OWASP Session Management Cheat Sheet: https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html

## Relevant Guidance

* WebSocket handshakes should validate `Origin` with an explicit allowlist; avoid wildcard or substring matching.
* WebSocket authentication should not imply unlimited authorization; sensitive actions still need message-level authorization.
* Query-string tokens can work, but they can appear in logs and should be redacted or moved to a less exposed transport.
* Long-lived sessions benefit from token rotation or expiry to reduce the impact of leakage.
* Logs should not contain access tokens, session identifiers, passwords, keys, or command strings likely to contain secrets.

## MobileVC Mapping

* Current `CheckOrigin` permits every browser origin, which is not acceptable for public deployment.
* The shared `AUTH_TOKEN` is shell-equivalent because authenticated WS can execute local commands.
* Query-token launch URLs and QR output are convenient but increase leakage through terminal logs, screenshots, browser history, and reverse-proxy access logs.
* Command and prompt logs need redaction before public operation because users can type bearer tokens and credentials into commands.
