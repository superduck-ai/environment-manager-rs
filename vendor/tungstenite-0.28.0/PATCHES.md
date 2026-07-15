# Vendored `tungstenite` 0.28.0

## Upstream provenance

- Crate: [`tungstenite`](https://crates.io/crates/tungstenite/0.28.0) 0.28.0
- Repository: <https://github.com/snapview/tungstenite-rs>
- Upstream Git commit recorded by the crate: `2d4abe8dba23b283c1a3d2f4f4937c2f9a8d91e7`
- crates.io archive SHA-256: `8628dcc84e5a09eb3d8423d6cb682965dea9133204e8fb3efee74c2a0c259442`
- License: `MIT OR Apache-2.0`; the original `LICENSE-MIT` and `LICENSE-APACHE`
  files are retained.
- Source: the complete 39-file crate directory was copied from Cargo's local registry cache.

## Local compatibility patch

The patch adds a `WebSocketConfig::gorilla_compatibility` switch. It is disabled by default and
is enabled only by `src/internal/tunnel/client.rs::gorilla_websocket_config`.

When enabled, the client behavior matches `github.com/gorilla/websocket` 1.5.3 in four places:

1. `Connection` and `Upgrade` response headers are parsed as comma-separated HTTP token lists,
   with Gorilla's token and optional-whitespace rules, instead of requiring whole-value equality.
2. A server-selected `Sec-WebSocket-Protocol` value is preserved but is not checked against the
   protocols offered by the client.
3. Incoming text messages are not rejected for invalid UTF-8. Invalid unfragmented and fragmented
   text payloads are returned as final text `Message::Frame` values so the Tunnel adapter can
   forward the original bytes with `is_binary=false`. Valid text still uses `Message::Text`.
4. The optional `gorilla_write_poisoned` marker suppresses read-side automatic flushing after the
   adapter's first fatal write. This prevents a caller-cancelled Tokio write from retrying buffered
   application bytes when a later ping or close frame is read, matching Gorilla's persistent
   `writeErr`. Only the local WS handler installs this marker; the main Tunnel connection does not.

No behavior changes for callers using `WebSocketConfig::default()`.

## Maintenance

When updating `tungstenite`, re-copy the complete crate, update its version, upstream Git commit,
archive checksum and retained licenses, then re-apply the opt-in patch. On Linux ARM64 run the
locked client/WS handler tests, strict Clippy, and the full differential suite. In particular,
retain coverage for token-list handshakes, unrequested response subprotocols, and invalid UTF-8 in
both unfragmented and fragmented text messages, plus persistent write-error and no-late-flush
semantics.
