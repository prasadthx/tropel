//! # StatsD / Datadog streaming output
//!
//! Streams samples to a StatsD or Datadog agent as UDP datagrams using the
//! Datadog extended format: `metric:value|type|#tag1:val1,tag2:val2`.
//!
//! Sample types map to StatsD types:
//! - `Counter` → `c` (count)
//! - `Gauge` → `g` (gauge)
//! - `Rate` → `c` (count; the agent computes the rate)
//! - `Trend` / `Point` → `g` (raw observation)
//!
//! Samples are buffered and sent every `FLUSH_INTERVAL` (or when the
//! buffer exceeds `MAX_BUFFERED_SAMPLES`) over UDP — best-effort, fire
//! and forget (UDP has no delivery guarantee; failures are logged only).

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tropel_core::types::{Sample, SampleType};
use tropel_core::{Result, TropelError};

use crate::output::TagPolicy;
use crate::Output;

/// How often buffered samples are sent.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Max buffered samples before a forced send.
const MAX_BUFFERED_SAMPLES: usize = 10_000;
/// Max UDP payload per datagram. StatsD/Datadog agents cap around 8 KB;
/// a larger datagram fails with EMSGSIZE or truncates, silently losing
/// every buffered sample — so flush() chunks lines across datagrams.
const MAX_DATAGRAM_BYTES: usize = 8 * 1024;

/// StatsD / Datadog streaming output.
pub struct StatsdOutput {
    addr: SocketAddr,
    /// Datagram payload (single line per sample, joined by `\n` on send).
    buffer: Mutex<Vec<String>>,
    total_buffered: AtomicUsize,
    /// Tag forwarding policy (allowlist + cardinality cap).
    tag_policy: TagPolicy,
}

impl StatsdOutput {
    /// Create an output sending to `addr` (host:port, e.g. `127.0.0.1:8125`).
    pub fn new(addr: impl Into<String>) -> Result<Self> {
        let addr: SocketAddr = addr
            .into()
            .parse()
            .map_err(|e| TropelError::Config(format!("invalid statsd address: {e}")))?;
        Ok(Self {
            addr,
            buffer: Mutex::new(Vec::new()),
            total_buffered: AtomicUsize::new(0),
            tag_policy: TagPolicy::default(),
        })
    }

    /// Set the tag forwarding policy (allowlist + cardinality cap).
    pub fn with_tag_policy(mut self, policy: TagPolicy) -> Self {
        self.tag_policy = policy;
        self
    }

    /// Spawn a consumer task sending samples to the agent.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        addr: String,
        tag_policy: TagPolicy,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = match StatsdOutput::new(addr).map(|o| o.with_tag_policy(tag_policy)) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("statsd output disabled: {e}");
                    return;
                }
            };
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => {
                            output.buffer(&sample);
                            if output.total_buffered.load(Ordering::Relaxed) >= MAX_BUFFERED_SAMPLES {
                                if let Err(e) = output.flush().await {
                                    tracing::warn!("statsd send failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("statsd dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush().await {
                                tracing::warn!("statsd send failed: {e}");
                            }
                        }
                    }
                }
            }

            if let Err(e) = output.flush().await {
                tracing::warn!("statsd final send failed: {e}");
            }
        })
    }

    /// Encode a sample as a Datadog-format datagram line and buffer it.
    fn buffer(&self, sample: &Sample) {
        let stype = match sample.sample_type {
            SampleType::Counter | SampleType::Rate => "c",
            SampleType::Trend | SampleType::Point => "g",
        };
        let mut line = format!("{}:{}|{}", sample.metric, sample.value, stype);
        let tags = self.tag_policy.apply(&sample.tags);
        if !tags.is_empty() {
            let tag_list: Vec<String> = tags.iter().map(|(k, v)| format!("{k}:{v}")).collect();
            line.push_str(&format!("|#{}", tag_list.join(",")));
        }
        self.buffer.lock().unwrap().push(line);
        self.total_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain the buffer and send one UDP datagram (lines joined by `\n`).
    async fn flush(&self) -> Result<()> {
        let lines = {
            let mut guard = self.buffer.lock().unwrap();
            let taken = std::mem::take(&mut *guard);
            self.total_buffered.store(0, Ordering::Relaxed);
            taken
        };
        if lines.is_empty() {
            return Ok(());
        }

        // Bind ephemeral local port for the send; a fresh socket per flush
        // avoids holding a socket open for the whole run.
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| TropelError::Http(format!("statsd bind failed: {e}")))?;
        // Chunk lines into ≤ MAX_DATAGRAM_BYTES datagrams so a large forced
        // flush never exceeds the agent's payload cap.
        let mut chunk: Vec<&str> = Vec::new();
        let mut chunk_len = 0usize;
        for line in &lines {
            if !chunk.is_empty() && chunk_len + line.len() + 1 > MAX_DATAGRAM_BYTES {
                socket
                    .send_to(chunk.join("\n").as_bytes(), self.addr)
                    .await
                    .map_err(|e| TropelError::Http(format!("statsd send failed: {e}")))?;
                chunk.clear();
                chunk_len = 0;
            }
            chunk_len += line.len() + 1;
            chunk.push(line);
        }
        if !chunk.is_empty() {
            socket
                .send_to(chunk.join("\n").as_bytes(), self.addr)
                .await
                .map_err(|e| TropelError::Http(format!("statsd send failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Output for StatsdOutput {
    fn name(&self) -> &str {
        "statsd"
    }

    async fn sample(&self, samples: &[Sample]) -> Result<()> {
        for sample in samples {
            self.buffer(sample);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tropel_core::types::{Sample, SampleType, TagMap};

    fn sample(metric: &str, value: f64, sample_type: SampleType) -> Sample {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(tags),
            timestamp: SystemTime::now(),
            sample_type,
        }
    }

    #[test]
    fn encodes_datadog_format() {
        let output = StatsdOutput::new("127.0.0.1:8125").unwrap();
        output.buffer(&sample("http_reqs", 1.0, SampleType::Counter));
        output.buffer(&sample("http_req_duration", 12.5, SampleType::Trend));
        let lines = output.buffer.lock().unwrap().clone();
        assert_eq!(lines[0], "http_reqs:1|c|#status:200");
        assert_eq!(lines[1], "http_req_duration:12.5|g|#status:200");
    }

    #[test]
    fn rejects_bad_address() {
        assert!(StatsdOutput::new("not-an-addr").is_err());
    }

    /// End-to-end: send to a live UDP socket and verify the datagram.
    #[tokio::test]
    async fn flush_sends_datagram() {
        use tokio::net::UdpSocket;

        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = receiver.local_addr().unwrap();

        let output = StatsdOutput::new(addr.to_string()).unwrap();
        output
            .sample(&[sample("http_reqs", 1.0, SampleType::Counter)])
            .await
            .unwrap();
        output.stop().await.unwrap();

        let mut buf = [0u8; 1024];
        let (n, _from) = receiver.recv_from(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert_eq!(text, "http_reqs:1|c|#status:200");
    }
}
