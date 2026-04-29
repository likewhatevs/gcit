// Control-channel client used by `gcit reload`, `gcit status`, and
// `gcit trigger`. Connects to the daemon's Unix control socket, sends
// one request, returns the response.
//
// The client is intentionally minimal: a one-shot send/receive on a
// fresh connection. `gcit reload` etc. exit after a single response,
// so connection reuse isn't worth the complexity.

use std::path::Path;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use tokio::net::UnixStream;
use tokio::time::timeout;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

use super::protocol::{Request, Response, MAX_FRAME_LEN, READ_TIMEOUT_SECS};

/// One-shot control client.
///
/// Construction connects; `send` performs a single request/response
/// exchange. Drop closes the connection.
pub struct Client {
    framed: Framed<UnixStream, LengthDelimitedCodec>,
}

impl Client {
    /// Connect to the daemon's control socket at `path`.
    pub async fn connect(path: &Path) -> std::io::Result<Self> {
        let stream = UnixStream::connect(path).await?;
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(MAX_FRAME_LEN)
            .length_field_type::<u32>()
            .big_endian()
            .new_codec();
        Ok(Self {
            framed: Framed::new(stream, codec),
        })
    }

    /// Send a request and wait for the matching response. The response
    /// id must echo the request id; a mismatch is surfaced as an
    /// `io::Error` rather than silently returning an unrelated reply.
    pub async fn send(&mut self, req: Request) -> std::io::Result<Response> {
        let want_id = req.id();
        let body = serde_json::to_vec(&req).map_err(|e| std::io::Error::other(e.to_string()))?;
        self.framed.send(body.into()).await?;

        let next = timeout(Duration::from_secs(READ_TIMEOUT_SECS), self.framed.next()).await;
        match next {
            Err(_) => Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("control reply timed out after {READ_TIMEOUT_SECS}s"),
            )),
            Ok(None) => Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "control connection closed before response",
            )),
            Ok(Some(Err(e))) => Err(std::io::Error::other(format!("frame error: {e}"))),
            Ok(Some(Ok(bytes))) => {
                let resp: Response = serde_json::from_slice(&bytes)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                if resp.id() != want_id {
                    return Err(std::io::Error::other(format!(
                        "response id mismatch: want {} got {}",
                        want_id,
                        resp.id()
                    )));
                }
                Ok(resp)
            }
        }
    }
}
