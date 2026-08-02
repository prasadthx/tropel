//! Wire protocol for controller ↔ agent communication.
//!
//! Messages are JSON, framed as `u32 BE length + bytes` over TCP.

use serde::{Deserialize, Serialize};
use tropel_core::config::JobConfig;
use tropel_metrics::collector::MetricsSnapshot;

/// Controller → Agent: dispatch a job with this worker's execution segment.
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    let mut data = vec![0u8; len];
    stream.read_exact(&mut data).await?;
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
        let recv = tokio::spawn(async move {
            read_frame::<_, SnapshotMsg>(&mut rx).await.unwrap()
        });
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
}
