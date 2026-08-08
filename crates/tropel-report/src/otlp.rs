//! # OTLP/HTTP streaming output
//!
//! Exports samples to an OpenTelemetry Collector over OTLP/HTTP
//! (e.g. `http://localhost:4318/v1/metrics`) using the JSON encoding
//! (`Content-Type: application/json`).
//!
//! Samples are buffered per metric name and flushed every `FLUSH_INTERVAL`
//! (or when the buffer exceeds `MAX_BUFFERED_SAMPLES`), with a final flush
//! on stream close. Metric type mapping follows the OTLP conventions:
//! - `Counter` samples → monotonic `Sum` with **DELTA** temporality: each
//!   data point is the increment since the last export (aggregated per
//!   tag-set within a flush window). CUMULATIVE would be wrong here — the
//!   raw values are per-event deltas, not running totals from process start.
//! - `Point` / `Gauge` samples → `Gauge`
//! - `Rate` samples → `Gauge` (a rate is a point-in-time ratio)
//! - `Trend` samples → each sample becomes a `Gauge` data point (raw
//!   observations; percentile summarization is left to the backend)
//!
//! Each data point carries the sample's tags as OTLP attributes, and a
//! `service.name` resource attribute identifies the exporter.

use async_trait::async_trait;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};
use tokio::sync::broadcast;
use tropel_core::types::{Sample, SampleType};
use tropel_core::{Result, TropelError};

use crate::output::TagPolicy;
use crate::Output;

/// How often buffered samples are exported to the collector.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
/// Max buffered samples before a forced export.
const MAX_BUFFERED_SAMPLES: usize = 10_000;

/// OTLP/HTTP metrics output.
///
/// Create one with [`OtlpOutput::new`] and either drive it through the
/// [`Output`] trait or spawn the engine-facing consumer task with
/// [`OtlpOutput::spawn`].
pub struct OtlpOutput {
    /// Base endpoint (e.g. `http://localhost:4318`). `/v1/metrics` is
    /// appended when missing.
    endpoint: String,
    client: reqwest::Client,
    /// Buffered samples grouped by metric name.
    metrics: Mutex<HashMap<String, Vec<Sample>>>,
    total_buffered: AtomicUsize,
    /// Tag forwarding policy (allowlist + cardinality cap).
    tag_policy: TagPolicy,
}

impl OtlpOutput {
    /// Create a new OTLP output pushing to `endpoint` (base URL or full
    /// `/v1/metrics` path).
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: normalize_metrics_url(&endpoint.into()),
            client: reqwest::Client::new(),
            metrics: Mutex::new(HashMap::new()),
            total_buffered: AtomicUsize::new(0),
            tag_policy: TagPolicy::default(),
        }
    }

    /// Set the tag forwarding policy (allowlist + cardinality cap).
    pub fn with_tag_policy(mut self, policy: TagPolicy) -> Self {
        self.tag_policy = policy;
        self
    }

    /// Spawn a consumer task that exports samples to the collector.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        endpoint: String,
        tag_policy: TagPolicy,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = OtlpOutput::new(endpoint).with_tag_policy(tag_policy);
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => {
                            output.buffer(sample);
                            if output.total_buffered.load(Ordering::Relaxed) >= MAX_BUFFERED_SAMPLES {
                                if let Err(e) = output.flush_buffered().await {
                                    tracing::warn!("otlp export failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("otlp output dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush_buffered().await {
                                tracing::warn!("otlp export failed: {e}");
                            }
                        }
                    }
                }
            }

            if let Err(e) = output.flush_buffered().await {
                tracing::warn!("otlp final export failed: {e}");
            }
        })
    }

    fn buffer(&self, mut sample: Sample) {
        sample.tags = std::sync::Arc::new(self.tag_policy.apply(&sample.tags));
        self.metrics
            .lock()
            .unwrap()
            .entry(sample.metric.to_string())
            .or_default()
            .push(sample);
        self.total_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain the buffer, build an `ExportMetricsServiceRequest` JSON payload,
    /// and POST it to `/v1/metrics`. Non-2xx responses are logged, not fatal.
    async fn flush_buffered(&self) -> Result<()> {
        let metrics = {
            let mut guard = self.metrics.lock().unwrap();
            let taken = std::mem::take(&mut *guard);
            // Reset the counter inside the lock (see prometheus.rs flush for
            // the race this closes).
            self.total_buffered.store(0, Ordering::Relaxed);
            taken
        };
        if metrics.is_empty() {
            return Ok(());
        }

        let body = build_export_request(&metrics);
        let resp = self
            .client
            .post(&self.endpoint)
            .header("Content-Type", "application/json")
            .body(body.to_string())
            .send()
            .await
            .map_err(|e| TropelError::Http(format!("otlp POST failed: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            tracing::warn!("otlp collector rejected ({status}): {body}");
        }
        Ok(())
    }
}

#[async_trait]
impl Output for OtlpOutput {
    fn name(&self) -> &str {
        "otlp"
    }

    async fn emit(&self, samples: &[Sample]) -> Result<()> {
        for sample in samples {
            self.buffer(sample.clone());
        }
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.flush_buffered().await
    }
}

/// Append `/v1/metrics` to a bare base endpoint.
fn normalize_metrics_url(url: &str) -> String {
    let trimmed = url.trim_end_matches('/');
    if trimmed.ends_with("/v1/metrics") {
        trimmed.to_string()
    } else {
        format!("{trimmed}/v1/metrics")
    }
}

/// Build an OTLP/HTTP JSON `ExportMetricsServiceRequest` from buffered metrics.
///
/// Counters are aggregated per (metric, tag-set) within the flush window and
/// reported with **DELTA** temporality (`aggregationTemporality: 1`): each
/// data point is the sum of events since the last export. CUMULATIVE (2)
/// would be wrong — raw counter samples are per-event increments, not
/// running totals from process start, and a collector would interpret each
/// as a cumulative total.
/// Per-tag-set aggregate: (sorted tag list, summed value, last timestamp nanos).
type TagAggregate = (Vec<(String, String)>, f64, u64);

fn build_export_request(metrics: &HashMap<String, Vec<Sample>>) -> serde_json::Value {
    let mut metric_values = Vec::with_capacity(metrics.len());

    for (name, samples) in metrics {
        let is_counter = samples.iter().any(|s| s.sample_type == SampleType::Counter);

        let data_points: Vec<serde_json::Value> = if is_counter {
            // Sum per (sorted tag-set). Keep the LAST timestamp seen for a
            // tag-set so the delta point carries the newest time.
            let mut per_tags: Vec<TagAggregate> = Vec::new();
            for s in samples {
                let mut tags: Vec<(String, String)> = s
                    .tags
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                tags.sort();
                // Nanos since epoch fits comfortably in u64 (≈584 years).
                let ts_nanos = s
                    .timestamp
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or(0);
                match per_tags.iter_mut().find(|(t, _, _)| *t == tags) {
                    Some((_, sum, ts)) => {
                        *sum += s.value;
                        *ts = ts_nanos;
                    }
                    None => per_tags.push((tags, s.value, ts_nanos)),
                }
            }
            per_tags
                .into_iter()
                .map(|(tags, sum, ts)| {
                    let attrs: Vec<serde_json::Value> = tags
                        .iter()
                        .map(|(k, v)| json!({ "key": k, "value": { "stringValue": v } }))
                        .collect();
                    json!({
                        "timeUnixNano": ts.to_string(),
                        "asDouble": sum,
                        "attributes": attrs,
                    })
                })
                .collect()
        } else {
            // Gauge: one data point per raw observation (unchanged).
            samples
                .iter()
                .map(|s| {
                    let attrs: Vec<serde_json::Value> = s
                        .tags
                        .iter()
                        .map(|(k, v)| json!({ "key": k, "value": { "stringValue": v } }))
                        .collect();
                    let ts_nanos = s
                        .timestamp
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_nanos())
                        .unwrap_or(0)
                        .to_string();
                    json!({
                        "timeUnixNano": ts_nanos,
                        "asDouble": s.value,
                        "attributes": attrs,
                    })
                })
                .collect()
        };

        let value_field = if is_counter {
            json!({
                "sum": {
                    "dataPoints": data_points,
                    "aggregationTemporality": 1, // DELTA
                    "isMonotonic": true,
                }
            })
        } else {
            json!({ "gauge": { "dataPoints": data_points } })
        };

        let mut metric_obj = serde_json::Map::new();
        metric_obj.insert("name".into(), json!(name));
        metric_obj.insert("description".into(), json!(""));
        metric_obj.insert("unit".into(), json!(""));
        if let Some(obj) = value_field.as_object() {
            for (k, v) in obj {
                metric_obj.insert(k.clone(), v.clone());
            }
        }
        metric_values.push(serde_json::Value::Object(metric_obj));
    }

    json!({
        "resourceMetrics": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "tropel" } }
                    ]
                },
                "scopeMetrics": [
                    {
                        "scope": { "name": "tropel" },
                        "metrics": metric_values,
                    }
                ],
            }
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tropel_core::types::{Sample, SampleType, TagMap};

    fn sample(metric: &str, value: f64, sample_type: SampleType) -> Sample {
        let mut tags = TagMap::new();
        tags.insert("status", "200");
        sample_typed(metric, value, sample_type, tags)
    }

    fn sample_typed(metric: &str, value: f64, sample_type: SampleType, tags: TagMap) -> Sample {
        Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(tags),
            timestamp: SystemTime::now(),
            sample_type,
        }
    }

    #[test]
    fn export_request_structure() {
        let mut metrics: HashMap<String, Vec<Sample>> = HashMap::new();
        metrics.insert(
            "http_req_duration".to_string(),
            vec![sample("http_req_duration", 12.5, SampleType::Trend)],
        );
        metrics.insert(
            "http_reqs".to_string(),
            vec![sample("http_reqs", 1.0, SampleType::Counter)],
        );

        let req = build_export_request(&metrics);
        let s = req.to_string();

        // Resource attribute present.
        assert!(s.contains("service.name"));

        // Trend → gauge.
        assert!(s.contains("\"http_req_duration\""));
        assert!(s.contains("\"gauge\""));
        assert!(s.contains("\"asDouble\":12.5") || s.contains("\"asDouble\": 12.5"));

        // Counter → monotonic sum with DELTA temporality (per-event values
        // are increments since the last export, not cumulative totals).
        assert!(s.contains("\"http_reqs\""));
        assert!(s.contains("\"sum\""));
        assert!(s.contains("\"isMonotonic\":true") || s.contains("\"isMonotonic\": true"));
        assert!(s.contains("\"aggregationTemporality\":1"));

        // Tags → attributes.
        assert!(s.contains("status"));
    }

    #[test]
    fn counter_aggregates_per_tag_set_with_delta() {
        // Regression for the temporality bug: raw counter samples are
        // per-event deltas, so they must be SUMMED per tag-set and reported
        // with DELTA temporality — never CUMULATIVE with per-event values.
        let mut tags_a = TagMap::new();
        tags_a.insert("status", "200");
        let mut tags_b = TagMap::new();
        tags_b.insert("status", "500");

        let mut metrics: HashMap<String, Vec<Sample>> = HashMap::new();
        metrics.insert(
            "http_reqs".to_string(),
            vec![
                sample_typed("http_reqs", 1.0, SampleType::Counter, tags_a.clone()),
                sample_typed("http_reqs", 1.0, SampleType::Counter, tags_a.clone()),
                sample_typed("http_reqs", 1.0, SampleType::Counter, tags_b.clone()),
            ],
        );

        let req = build_export_request(&metrics);
        let s = req.to_string();

        // DELTA temporality, monotonic sum.
        assert!(s.contains("\"aggregationTemporality\":1"));
        assert!(s.contains("\"isMonotonic\":true") || s.contains("\"isMonotonic\": true"));

        // Exactly two data points: status=200 summed to 2.0, status=500 to 1.0.
        assert_eq!(
            s.matches("\"timeUnixNano\":").count(),
            2,
            "one aggregated delta point per tag-set, got: {s}"
        );
        assert!(s.contains("\"asDouble\":2.0") || s.contains("\"asDouble\": 2.0"));
        assert!(s.contains("\"asDouble\":1.0") || s.contains("\"asDouble\": 1.0"));
        // Both tag keys and both values appear in the attribute sets.
        assert!(s.contains("\"status\""));
        assert!(s.contains("\"200\"") && s.contains("\"500\""));
    }

    #[test]
    fn url_normalization() {
        assert_eq!(
            normalize_metrics_url("http://localhost:4318"),
            "http://localhost:4318/v1/metrics"
        );
        assert_eq!(
            normalize_metrics_url("http://localhost:4318/"),
            "http://localhost:4318/v1/metrics"
        );
        assert_eq!(
            normalize_metrics_url("http://host:4318/v1/metrics"),
            "http://host:4318/v1/metrics"
        );
    }

    /// End-to-end: buffer samples, export to a live TCP server, and verify the
    /// received payload is valid OTLP JSON.
    #[tokio::test]
    async fn flush_posts_to_endpoint() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut chunk = [0u8; 2048];
            loop {
                let n = sock.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&chunk[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") && {
                    let head = String::from_utf8_lossy(&buf);
                    let content_length: usize = head
                        .lines()
                        .find_map(|l| {
                            let l = l.trim();
                            let lower = l.to_lowercase();
                            lower
                                .strip_prefix("content-length:")
                                .and_then(|v| v.trim().parse().ok())
                        })
                        .unwrap_or(0);
                    let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
                    buf.len() >= head_end + content_length
                } {
                    break;
                }
            }
            let head_end = buf.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
            let body = buf[head_end..].to_vec();
            let resp =
                "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK".to_string();
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.flush().await.unwrap();
            body
        });

        let output = OtlpOutput::new(format!("http://{addr}"));
        output
            .emit(&[sample("http_reqs", 1.0, SampleType::Counter)])
            .await
            .unwrap();
        output.flush().await.unwrap();

        let received = server.await.unwrap();
        let text = String::from_utf8(received).unwrap();
        let json: serde_json::Value = serde_json::from_str(&text).unwrap();
        let metrics = &json["resourceMetrics"][0]["scopeMetrics"][0]["metrics"];
        assert!(metrics.is_array() && !metrics.as_array().unwrap().is_empty());
        assert!(json["resourceMetrics"][0]["resource"]["attributes"]
            .as_array()
            .is_some());
    }
}
