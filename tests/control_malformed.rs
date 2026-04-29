// Control channel malformed-input handling.
//
// The control surface is exercised end-to-end by `tests/control_protocol_roundtrip.rs`
// (request/response round-trip across the JSON+LengthDelimitedCodec wire),
// `tests/control_peer_cred.rs` (`SO_PEERCRED` peer-uid gate), and
// `tests/control_reload_rate_limit.rs` (1/sec reload-rate-limit cap).
// The codec's max-frame cap and the daemon's per-connection error
// surface are owned by `src/control/server.rs` — its in-module
// `tracing::warn!` lines under `target: "gcit::control"` are the
// observable signal when a malformed frame is rejected (verified via
// the journald layer in tests/log_journald_init.rs).
//
// The original speculative skeletons here described malformed-frame
// scenarios (invalid JSON, unknown variant, oversized length prefix,
// truncated frame) but never carried real assertions — each was a
// `let _ = ...;` placeholder. The actual contracts those skeletons
// described are owned by the codec configuration in
// `src/control/server.rs::serve` (`LengthDelimitedCodec::builder()`)
// and the per-connection serde-deserialize path. Activating them
// would require exposing the server's accept loop on a test-supplied
// listener, which is a separate refactor with its own scope. Deleted
// in favour of the existing per-surface coverage.
