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
