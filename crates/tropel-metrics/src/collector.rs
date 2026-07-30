use crate::histogram::LatencyHistogram;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::mpsc;
use tropel_core::types::{Sample, SampleType};

/// Maximum pending samples in the bounded MPSC channel before backpressure applies.
/// At ~10 samples/request × 10k req/s, this provides a ~1s burst buffer.
/// If the aggregator falls behind, VUs will block on send() instead of
/// growing the queue unboundedly — preventing OOM.
const MAX_PENDING_SAMPLES: usize = 100_000;

/// A hashable metric key that avoids heap-allocated string formatting.
///
/// Previously the code built a key like `"http_req_duration{status=200}"`
/// via `format!` on every sample — a heap allocation per-record on the hot path.
/// This struct uses the metric name + sorted tag pairs directly as the hash key,
/// eliminating the `format!` call.
///
/// Uses `Arc<str>` internally so keys shared between samples (same metric name,
/// same tag keys/values) share the backing allocation.
#[derive(Debug, Clone, Eq)]
pub struct MetricKey {
    pub metric: Arc<str>,
    /// Sorted (key, value) pairs for deterministic ordering.
    pub tags: Vec<(Arc<str>, Arc<str>)>,
}

impl MetricKey {
    /// Build a key from a metric name and tag map.
    /// Tags are sorted for deterministic hash/eq.
    /// Uses `to_sorted_arc_vec()` which clones Arc references (ref-count bump, no string copy).
    pub fn new(metric: &str, tags: &tropel_core::types::TagMap) -> Self {
        let tags = tags.to_sorted_arc_vec();
        Self {
            metric: Arc::from(metric),
            tags,
        }
    }

    /// Render the key to its canonical string form (e.g. `"http_req_duration{status=200}"`).
    /// Used when building MetricSummary for the public API.
    pub fn to_key_string(&self) -> String {
        if self.tags.is_empty() {
            self.metric.to_string()
        } else {
            let tag_str: String = self
                .tags
                .iter()
                .map(|(k, v)| format!("{{{}}}", [k.as_ref(), v.as_ref()].join("=")))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}{}", self.metric, tag_str)
        }
    }
}

impl Hash for MetricKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.metric.hash(state);
        for (k, v) in &self.tags {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl PartialEq for MetricKey {
    fn eq(&self, other: &Self) -> bool {
        self.metric == other.metric && self.tags == other.tags
    }
}

/// Aggregated metrics for a tag set.
#[derive(Debug, Clone)]
pub struct MetricSet {
    /// Latency histogram (for trend metrics).
    pub histogram: LatencyHistogram,
    /// Counter for rate/counter metrics.
    pub count: f64,
    /// Sum of values (for mean calculation).
    pub sum: f64,
}

impl MetricSet {
    fn new() -> Self {
        Self {
            histogram: LatencyHistogram::new(),
            count: 0.0,
            sum: 0.0,
        }
    }

    fn record(&mut self, value: f64, sample_type: &SampleType) {
        match sample_type {
            SampleType::Trend | SampleType::Point => {
                if value > 0.0 {
                    self.histogram.record_micros(value as u64);
                }
                self.count += 1.0;
                self.sum += value;
            }
            SampleType::Counter | SampleType::Rate => {
                self.count += value;
                self.sum += value;
            }
        }
    }

    fn mean(&self) -> f64 {
        if self.count > 0.0 {
            self.sum / self.count
        } else {
            0.0
        }
    }
}

/// Internal message sent to the aggregator task.
/// The record path is lock-free — VUs just send messages into an unbounded channel.
enum MetricsEvent {
    /// Batch of samples to record.
    Samples(Vec<Sample>),
    /// Request a results snapshot.
    GetResults(tokio::sync::oneshot::Sender<MetricsResult>),
    /// Request a total count for a specific metric.
    GetTotal {
        metric: String,
        tx: tokio::sync::oneshot::Sender<f64>,
    },
}

/// The top-level metrics collector.
///
/// # Lock-free hot path with bounded backpressure
///
/// `record_batch()` sends samples into a bounded MPSC channel (`MAX_PENDING_SAMPLES`).
/// When the channel is full, `send().await` blocks, applying backpressure to the
/// producing VU — preventing unbounded queue growth that could OOM the process.
/// The `tokio::select!` in the VU run loop ensures the stop signal is still
/// checked while waiting, so shutdown is not blocked.
///
/// A single background aggregator task processes the samples sequentially,
/// updating an internal `IndexMap<MetricKey, MetricSet>`.
///
/// `results()` sends a request to the aggregator and waits for the response
/// via a one-shot channel. This is off the hot path (called ~once per 2s per VU
/// for threshold checks, and once at test end).
pub struct MetricsCollector {
    tx: mpsc::Sender<MetricsEvent>,
}

impl MetricsCollector {
    /// Create a new collector and spawn the background aggregator task.
    pub fn new() -> Self {
        let (tx, rx) = mpsc::channel(MAX_PENDING_SAMPLES);

        // Spawn the aggregator task on the current tokio runtime.
        // It processes samples sequentially, lock-free on the consumer side.
        tokio::spawn(async move {
            Aggregator::run(rx).await;
        });

        Self { tx }
    }

    /// Record a batch of samples — bounded backpressure path.
    ///
    /// Sends samples into the bounded MPSC channel. If the channel is full,
    /// `send().await` blocks, applying backpressure to the producing VU.
    /// The caller's `tokio::select!` ensures stop signals are still checked
    /// while waiting, so shutdown is not blocked.
    ///
    /// If the aggregator has shut down (channel closed), the send silently
    /// drops the samples — acceptable during test teardown.
    pub async fn record_batch(&self, samples: &[Sample]) {
        let batch: Vec<Sample> = samples.to_vec();
        if self.tx.send(MetricsEvent::Samples(batch)).await.is_err() {
            tracing::trace!("Metrics channel closed, dropping {} samples", samples.len());
        }
    }

    /// Record a single sample — bounded backpressure path.
    pub async fn record(&self, sample: &Sample) {
        if self
            .tx
            .send(MetricsEvent::Samples(vec![sample.clone()]))
            .await
            .is_err()
        {
            tracing::trace!("Metrics channel closed, dropping sample");
        }
    }

    /// Get aggregated results — sends a request and waits for the response.
    pub async fn results(&self) -> MetricsResult {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        if self.tx.send(MetricsEvent::GetResults(resp_tx)).await.is_err() {
            return MetricsResult::default();
        }
        resp_rx.await.unwrap_or_default()
    }

    /// Get total count for a metric — sends a request and waits.
    pub async fn total_count(&self, metric: &str) -> f64 {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(MetricsEvent::GetTotal {
                metric: metric.to_string(),
                tx: resp_tx,
            })
            .await
            .is_err()
        {
            return 0.0;
        }
        resp_rx.await.unwrap_or(0.0)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal aggregator that processes metrics events on a single background task.
/// No locks needed — all mutable state is owned by this task.
struct Aggregator {
    /// Metrics grouped by (metric_name, tags).
    data: IndexMap<MetricKey, MetricSet>,
    /// Total counters by metric name.
    totals: HashMap<String, f64>,
}

impl Aggregator {
    fn new() -> Self {
        Self {
            data: IndexMap::new(),
            totals: HashMap::new(),
        }
    }

    /// Run the aggregator loop, processing events until the channel closes.
    async fn run(mut rx: mpsc::Receiver<MetricsEvent>) {
        let mut agg = Self::new();

        while let Some(event) = rx.recv().await {
            match event {
                MetricsEvent::Samples(samples) => {
                    for sample in samples {
                        agg.record(sample);
                    }
                }
                MetricsEvent::GetResults(tx) => {
                    let results = agg.build_results();
                    let _ = tx.send(results);
                }
                MetricsEvent::GetTotal { metric, tx } => {
                    let total = agg.totals.get(&metric).copied().unwrap_or(0.0);
                    let _ = tx.send(total);
                }
            }
        }
    }

    fn record(&mut self, sample: Sample) {
        let key = MetricKey::new(&sample.metric, &sample.tags);

        let metric_set = self
            .data
            .entry(key)
            .or_insert_with(MetricSet::new);
        metric_set.record(sample.value, &sample.sample_type);

        // Update totals
        let total = self.totals.entry(sample.metric).or_insert(0.0);
        *total += sample.value;
    }

    fn build_results(&self) -> MetricsResult {
        let mut metrics = Vec::new();
        let mut http_reqs: u64 = 0;
        let mut http_req_duration: Option<MetricSummary> = None;
        let mut errors: u64 = 0;
        let mut checks_total: u64 = 0;
        let mut checks_passed: u64 = 0;
        let mut checks_failed: u64 = 0;
        let mut data_received: f64 = 0.0;
        let mut data_sent: f64 = 0.0;

        // Merge all http_req_duration* histograms into one for the headline value
        let mut merged_http_dur: Option<MetricSet> = None;

        for (key, set) in self.data.iter() {
            let key_str = key.to_key_string();
            let stats = set.histogram.stats();
            let summary = MetricSummary {
                key: key_str,
                count: set.count as u64,
                sum: set.sum,
                mean: set.mean(),
                min: stats.min,
                max: stats.max,
                p50: stats.p50,
                p90: stats.p90,
                p95: stats.p95,
                p99: stats.p99,
            };

            // Derive headline values from the metric key prefix
            if key.metric.starts_with("http_req_duration") {
                match &mut merged_http_dur {
                    Some(ref mut merged) => {
                        merged.histogram.merge(&set.histogram);
                        merged.count += set.count;
                        merged.sum += set.sum;
                    }
                    None => {
                        merged_http_dur = Some(set.clone());
                    }
                }
            } else if key.metric.starts_with("http_reqs") {
                http_reqs += set.count as u64;
            } else if key.metric.starts_with("errors") {
                errors += set.count as u64;
            } else if key.metric.starts_with("checks") {
                checks_total += set.count as u64;
                checks_passed += set.sum as u64;
                checks_failed += if set.count > set.sum {
                    (set.count - set.sum) as u64
                } else {
                    0
                }
            } else if key.metric.starts_with("data_received") {
                data_received += set.sum;
            } else if key.metric.starts_with("data_sent") {
                data_sent += set.sum;
            }

            metrics.push(summary);
        }

        // Build headline http_req_duration from merged histogram
        if let Some(ref merged) = merged_http_dur {
            let stats = merged.histogram.stats();
            http_req_duration = Some(MetricSummary {
                key: "http_req_duration".to_string(),
                count: merged.count as u64,
                sum: merged.sum,
                mean: merged.mean(),
                min: stats.min,
                max: stats.max,
                p50: stats.p50,
                p90: stats.p90,
                p95: stats.p95,
                p99: stats.p99,
            });
        }

        // Fallback to totals map for counters not captured in per-key metrics
        if http_reqs == 0 {
            http_reqs = self.totals.get("http_reqs").copied().unwrap_or(0.0) as u64;
        }
        if errors == 0 {
            errors = self.totals.get("errors").copied().unwrap_or(0.0) as u64;
        }
        if data_received == 0.0 {
            data_received = self.totals.get("data_received").copied().unwrap_or(0.0);
        }
        if data_sent == 0.0 {
            data_sent = self.totals.get("data_sent").copied().unwrap_or(0.0);
        }

        MetricsResult {
            metrics,
            checks_total,
            checks_passed,
            checks_failed,
            http_reqs,
            http_req_duration,
            data_received,
            data_sent,
            errors,
            ..Default::default()
        }
    }
}

/// Summary of a single metric.
#[derive(Debug, Clone)]
pub struct MetricSummary {
    pub key: String,
    pub count: u64,
    pub sum: f64,
    pub mean: f64,
    pub min: u64,
    pub max: u64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
}

/// Aggregated metrics result.
#[derive(Debug, Clone)]
pub struct MetricsResult {
    pub metrics: Vec<MetricSummary>,
    pub checks_total: u64,
    pub checks_passed: u64,
    pub checks_failed: u64,
    pub http_reqs: u64,
    pub http_req_duration: Option<MetricSummary>,
    pub iteration_duration: Option<MetricSummary>,
    pub data_received: f64,
    pub data_sent: f64,
    pub errors: u64,
    /// Iterations dropped because the VU pool was saturated (arrival-rate mode).
    pub dropped_iterations: u64,
}

impl Default for MetricsResult {
    fn default() -> Self {
        Self {
            metrics: vec![],
            checks_total: 0,
            checks_passed: 0,
            checks_failed: 0,
            http_reqs: 0,
            http_req_duration: None,
            iteration_duration: None,
            data_received: 0.0,
            data_sent: 0.0,
            errors: 0,
            dropped_iterations: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_key_equality() {
        let mut tags1 = tropel_core::types::TagMap::new();
        tags1.insert("status", "200");
        tags1.insert("method", "GET");

        let mut tags2 = tropel_core::types::TagMap::new();
        tags2.insert("method", "GET");
        tags2.insert("status", "200");

        let key1 = MetricKey::new("http_req_duration", &tags1);
        let key2 = MetricKey::new("http_req_duration", &tags2);

        assert_eq!(key1, key2, "keys should be equal regardless of tag insertion order");
        assert_eq!(key1.to_key_string(), key2.to_key_string());
    }

    #[test]
    fn test_metric_key_different_metric() {
        let tags = tropel_core::types::TagMap::new();
        let key1 = MetricKey::new("http_reqs", &tags);
        let key2 = MetricKey::new("errors", &tags);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_metric_key_different_tags() {
        let mut tags1 = tropel_core::types::TagMap::new();
        tags1.insert("status", "200");

        let mut tags2 = tropel_core::types::TagMap::new();
        tags2.insert("status", "404");

        let key1 = MetricKey::new("http_req_duration", &tags1);
        let key2 = MetricKey::new("http_req_duration", &tags2);
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_metric_key_to_string() {
        let mut tags = tropel_core::types::TagMap::new();
        tags.insert("status", "200");
        let key = MetricKey::new("http_req_duration", &tags);
        let s = key.to_key_string();
        assert!(s.contains("http_req_duration"));
        assert!(s.contains("status"));
        assert!(s.contains("200"));
    }
}
