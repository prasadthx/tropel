use crate::histogram::LatencyHistogram;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tropel_core::types::{Sample, SampleType};

/// Information about the type of a metric — stored alongside MetricSet so the
/// aggregator can report type-appropriate summary statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// Counter — monotonically increasing total (e.g. http_reqs, data_received).
    /// Aggregation: sum only.
    Counter,
    /// Gauge — point-in-time value (e.g. vus, http_req_duration for a single req).
    /// Aggregation: track last, min, max, avg.
    Gauge,
    /// Rate — ratio over time (e.g. http_req_failed, checks).
    /// Aggregation: count = events, sum = sum of values; rate = sum/count.
    Rate,
    /// Trend — distribution of values (e.g. http_req_duration, iteration_duration).
    /// Aggregation: full HdrHistogram with percentiles.
    Trend,
}

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

/// Aggregated metrics for a tag set, with type-aware aggregation.
///
/// Each `MetricSet` stores its type from the first sample recorded.
/// Subsequent samples for the same key use the same aggregation strategy.
///
/// Aggregation strategies by type:
/// - **Counter**: `count` = events, `sum` = total value
/// - **Rate**: `count` = denominator (events), `sum` = numerator (sum of values)
/// - **Gauge**: `min`/`max`/`last`/`count`(samples)/`sum`(for avg)
/// - **Trend**: full HdrHistogram + `count`(samples) + `sum`(for avg)
#[derive(Debug, Clone)]
pub struct MetricSet {
    /// The type of this metric, set from the first sample recorded.
    pub metric_type: MetricType,
    /// Latency histogram (for Trend metrics only).
    pub histogram: LatencyHistogram,
    /// For Counter/Rate: event count; for Gauge/Trend: sample count.
    pub count: f64,
    /// Sum of values (for mean calculation or rate numerator).
    pub sum: f64,
    /// Minimum value observed (Gauge only).
    pub min: f64,
    /// Maximum value observed (Gauge only).
    pub max: f64,
    /// Most recent value (Gauge only).
    pub last: f64,
}

impl MetricSet {
    fn new(metric_type: MetricType) -> Self {
        Self {
            metric_type,
            histogram: LatencyHistogram::new(),
            count: 0.0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
            last: 0.0,
        }
    }

    fn record(&mut self, value: f64, sample_type: &SampleType) {
        // Derive MetricType from SampleType for the record action
        let action_type = match sample_type {
            SampleType::Counter => MetricType::Counter,
            SampleType::Point => MetricType::Gauge,
            SampleType::Rate => MetricType::Rate,
            SampleType::Trend => MetricType::Trend,
        };

        match action_type {
            MetricType::Counter => {
                // Counter: count events, track total sum
                self.count += 1.0;
                self.sum += value;
            }
            MetricType::Rate => {
                // Rate: count = denominator (events), sum = numerator (values)
                self.count += 1.0;
                self.sum += value;
            }
            MetricType::Gauge => {
                // Gauge: track min, max, last, count, sum (for avg)
                self.count += 1.0;
                self.sum += value;
                if value < self.min { self.min = value; }
                if value > self.max { self.max = value; }
                self.last = value;
            }
            MetricType::Trend => {
                // Trend: histogram distribution
                if value > 0.0 {
                    self.histogram.record_micros(value as u64);
                }
                self.count += 1.0;
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

    /// Get the rate (sum/count) — only meaningful for Rate type.
    fn rate(&self) -> f64 {
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
///
/// # Streaming outputs
///
/// An optional `sample_sink` can be set via `set_sample_sink()`. When configured,
/// every sample forwarded to the aggregator is also cloned and broadcast to all
/// subscribed output consumers. The broadcast sender is non-blocking — if the
/// internal buffer is full, the OLDEST message is evicted (lagging consumers
/// skip missed samples). This ensures VUs are never blocked by slow outputs.
pub struct MetricsCollector {
    tx: mpsc::Sender<MetricsEvent>,
    /// Optional broadcast sender for streaming outputs.
    /// Cloned samples are sent via `broadcast::Sender::send()` (non-blocking,
    /// evicts oldest if buffer is full).
    sample_sink: std::sync::Mutex<Option<broadcast::Sender<Sample>>>,
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

        Self {
            tx,
            sample_sink: std::sync::Mutex::new(None),
        }
    }

    /// Set a broadcast sender for forwarding samples to streaming outputs.
    ///
    /// Once set, every sample passed to `record_batch()` or `record()` is also
    /// cloned and broadcast via `sender.send()` (non-blocking). If the
    /// broadcast buffer is full, the oldest sample is evicted — lagging
    /// output consumers will skip ahead via `RecvError::Lagged`.
    ///
    /// To stop forwarding, pass `None`.
    pub fn set_sample_sink(&self, sink: Option<broadcast::Sender<Sample>>) {
        let mut guard = self.sample_sink.lock().unwrap();
        *guard = sink;
    }

    /// Forward a batch of samples to the optional output sink (best-effort).
    /// Called internally by `record_batch()` before sending to the aggregator.
    /// Uses `broadcast::Sender::send()` which is non-blocking and never
    /// stalls the VU hot path.
    fn forward_to_sink(&self, samples: &[Sample]) {
        let sink = {
            let guard = self.sample_sink.lock().unwrap();
            guard.clone()
        };
        if let Some(sink) = sink {
            for sample in samples {
                let _ = sink.send(sample.clone());
            }
        }
    }

    /// Record a batch of samples — bounded backpressure path.
    ///
    /// Before sending to the aggregator, samples are also forwarded to the
    /// optional streaming output sink (best-effort, non-blocking).
    ///
    /// Sends samples into the bounded MPSC channel. If the channel is full,
    /// `send().await` blocks, applying backpressure to the producing VU.
    /// The caller's `tokio::select!` ensures stop signals are still checked
    /// while waiting, so shutdown is not blocked.
    ///
    /// If the aggregator has shut down (channel closed), the send silently
    /// drops the samples — acceptable during test teardown.
    pub async fn record_batch(&self, samples: &[Sample]) {
        // Forward to streaming output sinks (best-effort, non-blocking)
        self.forward_to_sink(samples);

        let batch: Vec<Sample> = samples.to_vec();
        if self.tx.send(MetricsEvent::Samples(batch)).await.is_err() {
            tracing::trace!("Metrics channel closed, dropping {} samples", samples.len());
        }
    }

    /// Record a single sample — bounded backpressure path.
    /// Also forwards to the streaming output sink if configured.
    pub async fn record(&self, sample: &Sample) {
        // Forward to streaming output sinks (best-effort, non-blocking)
        self.forward_to_sink(std::slice::from_ref(sample));

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

        // Derive MetricType from the sample's SampleType
        let metric_type = match sample.sample_type {
            SampleType::Counter => MetricType::Counter,
            SampleType::Point => MetricType::Gauge,
            SampleType::Rate => MetricType::Rate,
            SampleType::Trend => MetricType::Trend,
        };

        // Use the type from the first sample for this key
        let metric_set = self
            .data
            .entry(key)
            .or_insert_with(|| MetricSet::new(metric_type));
        metric_set.record(sample.value, &sample.sample_type);

        // Update totals
        let total = self.totals.entry(sample.metric).or_insert(0.0);
        *total += sample.value;
    }

    fn build_results(&self) -> MetricsResult {
        let mut metrics = Vec::new();
        let mut http_reqs: u64 = 0;
        let mut http_req_duration: Option<MetricSummary> = None;
        let mut http_req_failed_count: f64 = 0.0;
        let mut http_req_failed_total: f64 = 0.0;
        let mut errors: u64 = 0;
        let mut checks_total: u64 = 0;
        let mut checks_passed: u64 = 0;
        let mut checks_failed: u64 = 0;
        let mut data_received: f64 = 0.0;
        let mut data_sent: f64 = 0.0;
        let mut iterations: u64 = 0;
        let mut merged_iter_dur: Option<MetricSet> = None;
        let mut vus_max: u64 = 0;
        let mut iteration_duration: Option<MetricSummary> = None;

        // Merge all http_req_duration* histograms into one for the headline value
        let mut merged_http_dur: Option<MetricSet> = None;

        for (key, set) in self.data.iter() {
            let key_str = key.to_key_string();

            // Build type-appropriate summary
            let summary = match set.metric_type {
                MetricType::Counter => MetricSummary {
                    key: key_str,
                    metric_type: MetricType::Counter,
                    count: set.count as u64,
                    sum: set.sum,
                    mean: set.mean(),
                    min: 0,
                    max: 0,
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: 0.0,
                    rate: 0.0,
                },
                MetricType::Rate => MetricSummary {
                    key: key_str,
                    metric_type: MetricType::Rate,
                    count: set.count as u64,
                    sum: set.sum,
                    mean: set.mean(),
                    min: 0,
                    max: 0,
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: 0.0,
                    rate: set.rate(),
                },
                MetricType::Gauge => MetricSummary {
                    key: key_str,
                    metric_type: MetricType::Gauge,
                    count: set.count as u64,
                    sum: set.sum,
                    mean: set.mean(),
                    min: if set.min == f64::MAX { 0 } else { set.min as u64 },
                    max: if set.max == f64::MIN { 0 } else { set.max as u64 },
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: set.last,
                    rate: 0.0,
                },
                MetricType::Trend => {
                    let stats = set.histogram.stats();
                    MetricSummary {
                        key: key_str,
                        metric_type: MetricType::Trend,
                        count: set.count as u64,
                        sum: set.sum,
                        mean: set.mean(),
                        min: stats.min,
                        max: stats.max,
                        p50: stats.p50,
                        p90: stats.p90,
                        p95: stats.p95,
                        p99: stats.p99,
                        last: 0.0,
                        rate: 0.0,
                    }
                }
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
            } else if key.metric.starts_with("http_req_failed") {
                http_req_failed_total += set.count;
                http_req_failed_count += set.sum;
            } else if key.metric.starts_with("iterations") {
                iterations += set.count as u64;
            } else if key.metric.starts_with("iteration_duration") {
                match &mut merged_iter_dur {
                    Some(ref mut merged) => {
                        merged.histogram.merge(&set.histogram);
                        merged.count += set.count;
                        merged.sum += set.sum;
                    }
                    None => {
                        merged_iter_dur = Some(set.clone());
                    }
                }
            } else if key.metric.starts_with("vus") {
                // vus_max: use the max value observed from Gauge tracking
                if set.metric_type == MetricType::Gauge && set.max != f64::MIN {
                    let obs = set.max as u64;
                    if obs > vus_max {
                        vus_max = obs;
                    }
                } else {
                    // Fallback for non-gauge vus tracking
                    let obs = set.count.max(set.sum) as u64;
                    if obs > vus_max {
                        vus_max = obs;
                    }
                }
            }

            metrics.push(summary);
        }

        // Build headline iteration_duration from merged histogram
        if let Some(ref merged) = merged_iter_dur {
            let stats = merged.histogram.stats();
            iteration_duration = Some(MetricSummary {
                key: "iteration_duration".to_string(),
                metric_type: MetricType::Trend,
                count: merged.count as u64,
                sum: merged.sum,
                mean: merged.mean(),
                min: stats.min,
                max: stats.max,
                p50: stats.p50,
                p90: stats.p90,
                p95: stats.p95,
                p99: stats.p99,
                last: 0.0,
                rate: 0.0,
            });
        }

        // Build headline http_req_duration from merged histogram
        if let Some(ref merged) = merged_http_dur {
            let stats = merged.histogram.stats();
            http_req_duration = Some(MetricSummary {
                key: "http_req_duration".to_string(),
                metric_type: MetricType::Trend,
                count: merged.count as u64,
                sum: merged.sum,
                mean: merged.mean(),
                min: stats.min,
                max: stats.max,
                p50: stats.p50,
                p90: stats.p90,
                p95: stats.p95,
                p99: stats.p99,
                last: 0.0,
                rate: 0.0,
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
            iteration_duration,
            data_received,
            data_sent,
            errors,
            dropped_iterations: self.totals.get("dropped_iterations").copied().unwrap_or(0.0) as u64,
            http_req_failed: if http_req_failed_total > 0.0 { http_req_failed_count / http_req_failed_total } else { 0.0 },
            iterations,
            vus_max,
        }
    }
}

/// Summary of a single metric, with type-aware statistics.
/// Fields not applicable to the metric type are set to 0.
#[derive(Debug, Clone)]
pub struct MetricSummary {
    pub key: String,
    /// The type of this metric — determines which fields are meaningful.
    pub metric_type: MetricType,
    /// Sample count (Counter: events added; Rate: events; Trend: samples; Gauge: samples).
    pub count: u64,
    /// Sum of values (Counter/ Rate: total; Trend/Gauge: sum for avg).
    pub sum: f64,
    /// Mean value (sum / count).
    pub mean: f64,
    /// Minimum value (Trend/Gauge only).
    pub min: u64,
    /// Maximum value (Trend/Gauge only).
    pub max: u64,
    /// p50 / median (Trend only).
    pub p50: u64,
    /// p90 (Trend only).
    pub p90: u64,
    /// p95 (Trend only).
    pub p95: u64,
    /// p99 (Trend only).
    pub p99: u64,
    /// Last/gauge value (Gauge only).
    pub last: f64,
    /// Rate (Rate only: sum/count).
    pub rate: f64,
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
    /// HTTP request failure rate (0.0 - 1.0).
    pub http_req_failed: f64,
    /// Total iterations completed.
    pub iterations: u64,
    /// Maximum concurrent VUs observed.
    pub vus_max: u64,
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
            http_req_failed: 0.0,
            iterations: 0,
            vus_max: 0,
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
