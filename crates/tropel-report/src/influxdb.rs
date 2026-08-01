//! # InfluxDB streaming output
//!
//! Streams samples to an InfluxDB instance over UDP as line-protocol
//! datagrams (`metric[,tag=val,...] field=value`). Tags are encoded as
//! InfluxDB tags; the numeric sample value is the field `value`.
//!
//! Per InfluxDB's UDP semantics, the line carries **no timestamp** — the
//! server assigns arrival time (the UDP transport ignores client
//! timestamps). Samples are buffered and sent every `FLUSH_INTERVAL` (or
//! when the buffer exceeds `MAX_BUFFERED_SAMPLES`); UDP is best-effort,
//! failures are logged, never fatal.

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tropel_core::types::Sample;
use tropel_core::{Result, TropelError};

use crate::Output;

/// How often buffered samples are sent.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Max buffered samples before a forced send.
const MAX_BUFFERED_SAMPLES: usize = 10_000;
/// Max UDP payload per datagram. InfluxDB's UDP transport caps around
/// 64 KB but typical deployments are lower; chunking keeps the forced-
/// flush path from producing an EMSGSIZE failure that drops everything.
const MAX_DATAGRAM_BYTES: usize = 8 * 1024;

/// InfluxDB line-protocol UDP streaming output.
pub struct InfluxdbOutput {
    addr: SocketAddr,
    /// Buffered lines (joined by `\n` on send).
    buffer: Mutex<Vec<String>>,
    total_buffered: AtomicUsize,
}

impl InfluxdbOutput {
    /// Create an output sending to `addr` (host:port, e.g. `localhost:8089`).
    pub fn new(addr: impl Into<String>) -> Result<Self> {
        let addr: SocketAddr = addr
            .into()
            .parse()
            .map_err(|e| TropelError::Config(format!("invalid influxdb address: {e}")))?;
        Ok(Self {
            addr,
            buffer: Mutex::new(Vec::new()),
            total_buffered: AtomicUsize::new(0),
        })
    }

    /// Spawn a consumer task sending samples to InfluxDB.
    pub fn spawn(mut rx: broadcast::Receiver<Sample>, addr: String) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = match InfluxdbOutput::new(addr) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("influxdb output disabled: {e}");
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
                                    tracing::warn!("influxdb send failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("influxdb dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush().await {
                                tracing::warn!("influxdb send failed: {e}");
                            }
                        }
                    }
                }
            }

            if let Err(e) = output.flush().await {
                tracing::warn!("influxdb final send failed: {e}");
            }
        })
    }

    /// Escape a line-protocol component per InfluxDB rules.
    fn escape(s: &str, in_quotes: bool) -> String {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                ' ' | ',' | '=' if !in_quotes => out.push('\\'),
                '"' if in_quotes => out.push('\\'),
                '\\' => out.push('\\'),
                _ => {}
            }
            out.push(c);
        }
        out
    }

    /// Encode a sample as one line-protocol line and buffer it.
    fn buffer(&self, sample: &Sample) {
        // measurement[,tag=val,...] field=value — no stray space between
        // the measurement and the tag set (line protocol is strict).
        let mut line = Self::escape(&sample.metric, false);
        if !sample.tags.is_empty() {
            let tags: Vec<String> = sample
                .tags
                .iter()
                .map(|(k, v)| {
                    format!("{}={}", Self::escape(k, false), Self::escape(v, false))
                })
                .collect();
            line.push_str(&format!(",{}", tags.join(",")));
        }
        // field set
        let value = if sample.value.fract() == 0.0 && sample.value.abs() < 9_007_199_254_740_992.0 {
            format!("{}i", sample.value as i64)
        } else {
            sample.value.to_string()
        };
        line.push_str(&format!(" value={value}"));
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

        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .map_err(|e| TropelError::Http(format!("influxdb bind failed: {e}")))?;
        // Chunk lines into ≤ MAX_DATAGRAM_BYTES datagrams so a large forced
        // flush never exceeds the UDP payload cap.
        let mut chunk: Vec<&str> = Vec::new();
        let mut chunk_len = 0usize;
        for line in &lines {
            if !chunk.is_empty() && chunk_len + line.len() + 1 > MAX_DATAGRAM_BYTES {
                socket
                    .send_to(chunk.join("\n").as_bytes(), self.addr)
                    .await
                    .map_err(|e| TropelError::Http(format!("influxdb send failed: {e}")))?;
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
                .map_err(|e| TropelError::Http(format!("influxdb send failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Output for InfluxdbOutput {
    fn name(&self) -> &str {
        "influxdb"
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

    fn sample(metric: &str, value: f64, tags: &[(&str, &str)]) -> Sample {
        let mut map = TagMap::new();
        for (k, v) in tags {
            map.insert((*k).to_string(), (*v).to_string());
        }
        Sample {
            metric: metric.to_string(),
            value,
            tags: map,
            timestamp: SystemTime::now(),
            sample_type: SampleType::Trend,
        }
    }

    #[test]
    fn encodes_line_protocol() {
        let output = InfluxdbOutput::new("127.0.0.1:8089").unwrap();
        output.buffer(&sample("http_reqs", 1.0, &[("status", "200")]));
        output.buffer(&sample("http_req_duration", 12.5, &[("status", "200")]));
        let lines = output.buffer.lock().unwrap().clone();
        assert_eq!(lines[0], "http_reqs,status=200 value=1i");
        assert_eq!(lines[1], "http_req_duration,status=200 value=12.5");
    }

    #[test]
    fn escapes_special_chars() {
        let output = InfluxdbOutput::new("127.0.0.1:8089").unwrap();
        output.buffer(&sample("http reqs", 1.0, &[("method=GET", "a,b")]));
        let line = output.buffer.lock().unwrap().first().unwrap().clone();
        assert!(line.starts_with("http\\ reqs,"), "metric escaped: {line}");
        assert!(line.contains("method\\=GET=a\\,b"), "tags escaped: {line}");
    }

    #[test]
    fn rejects_bad_address() {
        assert!(InfluxdbOutput::new("not-an-addr").is_err());
    }

    /// End-to-end: send to a live UDP socket and verify the datagram.
    #[tokio::test]
    async fn flush_sends_datagram() {
        use tokio::net::UdpSocket;

        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = receiver.local_addr().unwrap();

        let output = InfluxdbOutput::new(addr.to_string()).unwrap();
        output
            .sample(&[sample("http_reqs", 1.0, &[("status", "200")])])
            .await
            .unwrap();
        output.stop().await.unwrap();

        let mut buf = [0u8; 1024];
        let (n, _from) = receiver.recv_from(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert_eq!(text, "http_reqs,status=200 value=1i");
    }
}
