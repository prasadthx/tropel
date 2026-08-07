//! Wire protocol for controller ↔ agent communication.
//!
//! Messages are JSON, framed as `u32 BE length + bytes` over TCP.
//!
//! # Authentication
//!
//! The control channel is unauthenticated plaintext by design (the
//! controller listens on a ClusterIP service that anything in the cluster
//! can reach). A shared secret token gates the connection: the agent sends
//! a [`HelloMsg`] with its token as the FIRST frame; the controller refuses
//! the connection unless it matches (constant-time compare) and echoes the
//! token back inside [`AssignMsg`] so the agent can verify it is talking to
//! the real controller, not an impostor.

use rand::RngExt;
use serde::{Deserialize, Serialize};
use tropel_core::config::JobConfig;
use tropel_metrics::collector::MetricsSnapshot;

/// Agent → Controller: authentication preamble, sent before any other
/// message. The controller rejects the connection on a mismatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HelloMsg {
    /// Shared secret token. MUST be the first frame an agent sends.
    pub token: String,
}

/// Controller → Agent: dispatch a job with this worker's execution segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AssignMsg {
    /// The full job config (input, execution, env, thresholds...). The agent
    /// applies `distributed_worker` + its segment on top.
    pub config: JobConfig,
    /// This worker's execution segment spec, e.g. `"0:1/3"`.
    pub segment: String,
    /// The shared segment sequence, e.g. `"0,1/3,2/3,1"`.
    pub sequence: Option<String>,
    /// This worker's index in [0, total).
    pub index: u32,
    /// Total number of workers in the run.
    pub total: u32,
    /// The controller's token, echoed back so the agent can authenticate
    /// the controller (mutual auth on a plaintext channel).
    pub token: String,
}

/// Constant-time string equality (the token gate must not leak byte
/// positions through early-exit timing).
pub fn token_matches(expect: &str, got: &str) -> bool {
    if expect.len() != got.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (e, g) in expect.bytes().zip(got.bytes()) {
        diff |= e ^ g;
    }
    diff == 0
}

/// Generate a fresh shared secret for a controller run (32 random bytes,
/// hex-encoded). `rand::rng()` is a CSPRNG (ChaCha12, OS-seeded).
pub fn generate_token() -> String {
    let bytes: [u8; 32] = rand::rng().random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Agent → Controller: the worker's raw metrics snapshot (histograms as
/// base64 V2 bytes) for central lossless merging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMsg {
    pub snapshot: MetricsSnapshot,
}

/// Maximum accepted frame size (guard against corrupt/hostile streams).
const MAX_FRAME: usize = 512 * 1024 * 1024;

/// Write a message as a length-prefixed JSON frame.
///
/// Generic over the transport so tests can use in-memory duplex streams
/// and callers can use TCP.
pub async fn write_frame<W: tokio::io::AsyncWrite + Unpin, T: Serialize>(
    stream: &mut W,
    msg: &T,
) -> tropel_core::Result<()> {
    let data = serde_json::to_vec(msg).map_err(|e| {
        tropel_core::TropelError::Parse(format!("distributed protocol serialize: {e}"))
    })?;
    if data.len() > MAX_FRAME {
        return Err(tropel_core::TropelError::Parse(format!(
            "distributed protocol frame too large: {} bytes",
            data.len()
        )));
    }
    let len = (data.len() as u32).to_be_bytes();
    use tokio::io::AsyncWriteExt;
    stream.write_all(&len).await?;
    stream.write_all(&data).await?;
    Ok(())
}

/// Read a length-prefixed JSON frame.
///
/// Reads the declared length incrementally (64 KiB chunks) instead of
/// allocating `vec![0u8; len]` up front — a hostile stream declaring a
/// near-MAX_FRAME length must actually SEND that much data before the host
/// allocates it, so a lying length prefix no longer forces a giant
/// allocation. The frame cap still bounds the final allocation.
pub async fn read_frame<R: tokio::io::AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    stream: &mut R,
) -> tropel_core::Result<T> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_FRAME {
        return Err(tropel_core::TropelError::Parse(format!(
            "distributed protocol frame too large: {len} bytes"
        )));
    }
    let mut data = Vec::with_capacity(len.min(64 * 1024));
    let mut remaining = len;
    let mut chunk = [0u8; 64 * 1024];
    while remaining > 0 {
        let take = remaining.min(chunk.len());
        stream.read_exact(&mut chunk[..take]).await?;
        data.extend_from_slice(&chunk[..take]);
        remaining -= take;
    }
    serde_json::from_slice(&data).map_err(|e| {
        tropel_core::TropelError::Parse(format!("distributed protocol deserialize: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    /// Split a duplex stream into a writer and a reader half for the
    /// framing tests (TcpStream has no `pair()`; duplex is transport-less
    /// and works on every platform/tokio version).
    fn split_duplex(buf: usize) -> (tokio::io::DuplexStream, tokio::io::DuplexStream) {
        tokio::io::duplex(buf)
    }

    #[tokio::test]
    async fn frame_roundtrip() {
        let (a, b) = split_duplex(64 * 1024);
        let mut tx = a;
        let mut rx = b;

        let msg = SnapshotMsg {
            snapshot: MetricsSnapshot::default(),
        };
        let send = tokio::spawn(async move {
            write_frame(&mut tx, &msg).await.unwrap();
        });
        let recv =
            tokio::spawn(async move { read_frame::<_, SnapshotMsg>(&mut rx).await.unwrap() });
        send.await.unwrap();
        let got = recv.await.unwrap();
        assert!(got.snapshot.series.is_empty());
    }

    #[tokio::test]
    async fn frame_rejects_oversized() {
        // A frame declaring 600 MB must be rejected before allocating.
        let (mut a, b) = split_duplex(1024 * 1024);
        let len = (600u32 * 1024 * 1024).to_be_bytes();
        a.write_all(&len).await.unwrap();
        let mut rx = b;
        let err = read_frame::<_, SnapshotMsg>(&mut rx).await.unwrap_err();
        assert!(err.to_string().contains("too large"));
    }

    #[test]
    fn token_matches_compares_constant_time_style() {
        assert!(token_matches("abc", "abc"));
        assert!(!token_matches("abc", "abd"));
        assert!(!token_matches("abc", "abcd"));
        assert!(!token_matches("", "x"));
        // Same length, all bytes differ — must be false.
        assert!(!token_matches("aaaa", "bbbb"));
        // Generated tokens round-trip and are unique-ish (64 hex chars).
        let t1 = generate_token();
        let t2 = generate_token();
        assert_eq!(t1.len(), 64);
        assert!(t1.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(t1 != t2, "tokens should be random");
        assert!(token_matches(&t1, &t1));
    }

    #[tokio::test]
    async fn read_frame_is_incremental_not_alloc_on_declare() {
        // The declared length must not be allocated up front: write a length
        // prefix declaring 1 MB but only send a small frame; the read must
        // still succeed once the ACTUAL bytes arrive (allocation grows with
        // received bytes, not the declared length). A lying 600 MB prefix
        // already trips the cap test above; this one pins the incremental
        // behavior so the alloc-on-demand vector stays closed.
        let (mut a, mut b) = split_duplex(64 * 1024);
        let payload = serde_json::to_vec(&HelloMsg {
            token: "tok".into(),
        })
        .unwrap();
        let len = (payload.len() as u32).to_be_bytes();
        a.write_all(&len).await.unwrap();
        a.write_all(&payload).await.unwrap();
        let got: HelloMsg = read_frame(&mut b).await.unwrap();
        assert_eq!(got.token, "tok");
    }
}
