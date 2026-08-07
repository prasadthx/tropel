//! # JSON-stream streaming output
//!
//! Appends every sample as NDJSON to a file while the run is in progress —
//! the k6 `--out json=file` equivalent, emitting **k6-compatible records**:
//!
//! - a `Metric` definition record (`{"type":"Metric","data":{...}}`) the
//!   first time each metric is seen, carrying the k6 metric type
//!   (`counter`/`gauge`/`rate`/`trend`), `contains` (`time` for duration
//!   metrics, else `default`), and the metric name;
//! - a `Point` record (`{"type":"Point","data":{...}}`) per sample with
//!   RFC 3339 (nanosecond) `time`, the `measurement`, `tags`, and the value
//!   under `fields.value` — exactly the schema k6's JSON output emits.
//!
//! Lines are buffered and written every `FLUSH_INTERVAL` (or when the
//! buffer exceeds `MAX_BUFFERED_SAMPLES`), with a final drain on stream
//! close. Write failures are logged, never fatal to the run.

use async_trait::async_trait;
use std::collections::HashSet;
use std::io::Write;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
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
    /// Metric names already emitted as a `Metric` definition record. Each
    /// metric's definition is written once, before its first `Point`.
    seen_metrics: Mutex<HashSet<String>>,
}

impl JsonStreamOutput {
    /// Create a JSON-stream output writing to `path`.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            buffer: Mutex::new(Vec::new()),
            total_buffered: AtomicUsize::new(0),
            seen_metrics: Mutex::new(HashSet::new()),
        }
    }

    /// Spawn a consumer task that appends samples as NDJSON lines.
    /// Returns a `JoinHandle` that completes when the stream closes.
    pub fn spawn(mut rx: broadcast::Receiver<Sample>, path: String) -> tokio::task::JoinHandle<()> {
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

    /// Serialize a sample into k6-compatible NDJSON lines and buffer them.
    ///
    /// Emits the metric's `Metric` definition record the first time the
    /// metric is seen, then a `Point` record for this sample. The schema
    /// mirrors k6's `--out json` so consumers (e.g. k6's JSON parser, custom
    /// dashboards) can read the file unchanged.
    fn buffer(&self, sample: &Sample) {
        let metric_name = sample.metric.clone();
        let seen = {
            let mut seen = self.seen_metrics.lock().unwrap();
            !seen.insert(metric_name.to_string())
        };
        if !seen {
            // k6 Metric definition record — emitted once per metric.
            let def = serde_json::json!({
                "type": "Metric",
                "data": {
                    "type": k6_metric_type(&sample.sample_type),
                    "contains": if is_time_metric(&metric_name) {
                        "time"
                    } else {
                        "default"
                    },
                    "tainted": null,
                    "thresholds": [],
                    "submetrics": null,
                    "time": k6_timestamp(sample.timestamp),
                    "name": metric_name,
                    "tags": null,
                    "samples": [],
                },
            })
            .to_string();
            self.buffer.lock().unwrap().push(def);
            self.total_buffered.fetch_add(1, Ordering::Relaxed);
        }

        // k6 Point record — one per sample.
        let point = serde_json::json!({
            "type": "Point",
            "data": {
                "time": k6_timestamp(sample.timestamp),
                "measurement": sample.metric,
                "tags": sample.tags,
                "fields": {"value": sample.value},
            },
        })
        .to_string();
        self.buffer.lock().unwrap().push(point);
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
            .map_err(|e| {
                TropelError::Report(format!("json-stream open '{}' failed: {e}", self.path))
            })?;
        for line in &lines {
            writeln!(file, "{line}")
                .map_err(|e| TropelError::Report(format!("json-stream write failed: {e}")))?;
        }
        Ok(())
    }
}

/// k6 metric type name for a sample type.
fn k6_metric_type(sample_type: &SampleType) -> &'static str {
    match sample_type {
        SampleType::Counter => "counter",
        // Gauge metrics are emitted as Point samples (snapshots).
        SampleType::Point => "gauge",
        SampleType::Rate => "rate",
        SampleType::Trend => "trend",
    }
}

/// k6 RFC 3339 timestamp (nanosecond precision, UTC), e.g.
/// `2026-08-03T12:34:56.123456789Z`.
fn k6_timestamp(t: std::time::SystemTime) -> String {
    let dt: chrono::DateTime<chrono::Utc> = t.into();
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// True for metrics k6 marks `contains: "time"` (rendered in ms). Duration
/// trends carry duration suffixes/prefixes; everything else is `default`.
/// Custom metrics explicitly declared as time metrics (`new Trend(name, true)`
/// — backlog line 154) are consulted via the tropel-metrics registry too, so
/// a custom `my_timer` renders in ms even though its name has no time suffix.
pub(crate) fn is_time_metric(metric: &str) -> bool {
    if tropel_metrics::time_metrics::is_time_metric(metric) {
        return true;
    }
    metric.ends_with("duration")
        || metric.ends_with("_time")
        || metric.ends_with("_waiting")
        || metric.ends_with("_receiving")
        || metric.ends_with("_sending")
        || metric.ends_with("_connecting")
        || metric.ends_with("_blocked")
        || metric.ends_with("_tls_handshaking")
        || metric.ends_with("_lookup")
        || metric.contains("ttfb")
        || metric.contains("latency")
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
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(tags),
            timestamp: SystemTime::now(),
            sample_type: if metric == "http_reqs" {
                SampleType::Counter
            } else {
                SampleType::Trend
            },
        }
    }

    #[test]
    fn flush_appends_k6_ndjson() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tropel-json-stream-{}-flush.ndjson", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let output = JsonStreamOutput::new(path.to_string_lossy().to_string());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            output.sample(&[sample("http_reqs", 1.0)]).await.unwrap();
            output
                .sample(&[sample("http_req_duration", 12.5)])
                .await
                .unwrap();
            output
                .sample(&[sample("http_req_duration", 14.0)])
                .await
                .unwrap();
            output.stop().await.unwrap();
        });

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        // 2 Metric definitions (http_reqs, http_req_duration) + 3 Points.
        assert_eq!(
            lines.len(),
            5,
            "defs once + one point per sample: {content}"
        );
        let mut metric_defs = 0;
        let mut points = 0;
        let mut duration_points = 0;
        for line in lines {
            let v: serde_json::Value = serde_json::from_str(line).unwrap();
            match v["type"].as_str().unwrap() {
                "Metric" => {
                    metric_defs += 1;
                    let data = &v["data"];
                    let name = data["name"].as_str().unwrap();
                    assert!(data["type"].is_string());
                    assert!(data["time"].is_string(), "RFC3339 time");
                    if name == "http_req_duration" {
                        assert_eq!(data["contains"], "time");
                    } else {
                        assert_eq!(data["contains"], "default");
                    }
                }
                "Point" => {
                    points += 1;
                    let data = &v["data"];
                    assert!(data["measurement"].is_string());
                    assert!(data["time"].is_string());
                    assert!(data["fields"]["value"].is_number());
                    assert!(data["tags"]["status"] == "200");
                    if data["measurement"] == "http_req_duration" {
                        duration_points += 1;
                    }
                }
                other => panic!("unexpected record type {other}"),
            }
        }
        assert_eq!(metric_defs, 2);
        assert_eq!(points, 3);
        assert_eq!(duration_points, 2, "no duplicate Metric def per metric");
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn empty_flush_is_noop() {
        let output = JsonStreamOutput::new("/nonexistent-dir/x.ndjson");
        assert!(
            output.flush().is_ok(),
            "empty flush must not touch the file"
        );
    }
}
