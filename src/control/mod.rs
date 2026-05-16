// Control channel module: tokio_util::codec::LengthDelimitedCodec +
// serde_json wire protocol used by `gcit reload`, `gcit status`, and
// `gcit trigger` to talk to the daemon over a Unix socket.
//
// The gcit library is not a published API; the only consumers are
// the gcit binary and integration tests under tests/. Items are
// `pub` because `pub(crate)` does not span the integration-test
// boundary.

pub mod client;
pub mod protocol;
pub mod server;

pub use client::Client;
pub use protocol::{Request, Response, MAX_FRAME_LEN, READ_TIMEOUT_SECS};
pub use server::{serve, Handler, RELOAD_WINDOW};

use tokio_util::codec::LengthDelimitedCodec;

/// Build the wire codec used by every control-channel peer (client,
/// server, and the test fake-daemon harness). Centralising the
/// `max_frame_length` cap, the `u32` length-field type, and the
/// big-endian byte order in one place keeps the three call sites from
/// drifting on any of those parameters — a frame written by one peer
/// could otherwise be silently rejected by another.
pub fn build_codec() -> LengthDelimitedCodec {
    LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LEN)
        .length_field_type::<u32>()
        .big_endian()
        .new_codec()
}
