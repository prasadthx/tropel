//! # JSON-stream streaming output
//!
//! Appends every sample as one JSON line (NDJSON) to a file while the run
//! is in progress — the k6 `--out json=file` equivalent. Each line is the
//! full sample: `metric`, `value`, `timestamp` (RFC 3339), `tags`, and
//! `sample_type`.
//!
//! Lines are buffered and written every `FLUSH_INTERVAL` (or when the
//! buffer exceeds `MAX_BUFFERED_SAMPLES`), with a final drain on stream
//! close. Write failures are logged, never fatal to the run.

use async_trait::async_trait;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::broadcast;
use tropel_core::types::{Sample, SampleType};
use tropel_core::{Result, TropelError};

use crate::Output;

/// How often buffered samples are written to the file.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Max buffered samples before a forced write.
const MAX_BUFFERED_SAMPLES: usize = 10_000;

/// NDJSON streaming output writing to `path`.
pub struct JsonStreamOutput {
    path: String,
    /// Buffered, serialized lines (plain UTF-8 strings — no shared state
    /// with the Sample type needed since we serialize immediately).
    buffer: Mutex<Vec<String>>,
    total_buffered: AtomicUsize,
}

impl JsonStreamOutput {
    /// Create a JSON-stream output writing to `path`.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            buffer: Mutex::new(Vec::new()),
            total_buffered: AtomicUsize::new(0),
        }
    }

    /// Spawn a consumer task that appends samples as NDJSON lines.
    /// Returns a `JoinHandle` that completes when the stream closes.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        path: String,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = JsonStreamOutput::new(path);
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => {
                            output.buffer(&sample);
                            if output.total_buffered.load(Ordering::Relaxed) >= MAX_BUFFERED_SAMPLES {
                                if let Err(e) = output.flush() {
                                    tracing::warn!("json-stream write failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("json-stream dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush() {
                                tracing::warn!("json-stream write failed: {e}");
                            }
                        }
                    }
                }
            }

            if let Err(e) = output.flush() {
                tracing::warn!("json-stream final write failed: {e}");
            }
        })
    }

    /// Serialize a sample into one NDJSON line and buffer it.
    fn buffer(&self, sample: &Sample) {
        let ts_secs = sample
            .timestamp
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs_f64())
            .unwrap_or(0.0);
        let line = serde_json::json!({
            "metric": sample.metric,
            "value": sample.value,
            "timestamp": ts_secs,
            "sample_type": match sample.sample_type {
                SampleType::Counter => "counter",
                // Gauge metrics are emitted as Point samples (snapshots).
                SampleType::Point => "gauge",
                SampleType::Rate => "rate",
                SampleType::Trend => "trend",
            },
            "tags": sample.tags,
        })
        .to_string();
        self.buffer.lock().unwrap().push(line);
        self.total_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain the buffer and append the lines to the file.
    fn flush(&self) -> Result<()> {
        let lines = {
            let mut guard = self.buffer.lock().unwrap();
            let taken = std::mem::take(&mut *guard);
            self.total_buffered.store(0, Ordering::Relaxed);
            taken
        };
        if lines.is_empty() {
            return Ok(());
        }

        let mut file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| TropelError::Report(format!("json-stream open '{}' failed: {e}", self.path)))?;
        for line in &lines {
            writeln!(file, "{line}")
                .map_err(|e| TropelError::Report(format!("json-stream write failed: {e}")))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Output for JsonStreamOutput {
    fn name(&self) -> &str {
        "json-stream"
    }

    async fn sample(&self, samples: &[Sample]) -> Result<()> {
        for sample in samples {
            self.buffer(sample);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tropel_core::types::{Sample, SampleType, TagMap};

    fn sample(metric: &str, value: f64) -> Sample {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        Sample {
            metric: metric.to_string(),
            value,
            tags,
            timestamp: SystemTime::now(),
            sample_type: SampleType::Trend,
        }
    }

    #[test]
    fn flush_appends_ndjson() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tropel-json-stream-{}.ndjson", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let output = JsonStreamOutput::new(path.to_string_lossy().to_string());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            output.sample(&[sample("http_reqs", 1.0)]).await.unwrap();
            output.sample(&[sample("http_req_duration", 12.5)]).await.unwrap();
            output.stop().await.unwrap();
        });

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2, "one JSON line per sample: {content}");
        for line in lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(v["metric"].is_string());
            assert!(v["tags"]["status"] == "200");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_flush_is_noop() {
        let output = JsonStreamOutput::new("/nonexistent-dir/x.ndjson");
        assert!(output.flush().is_ok(), "empty flush must not touch the file");
    }
}
