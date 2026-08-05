use crate::histogram::LatencyHistogram;
use crate::thresholds::parse_metric_ref;
use base64::Engine as _;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tropel_core::types::{Sample, SampleType};

/// Information about the type of a metric — stored alongside MetricSet so the
/// aggregator can report type-appropriate summary statistics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    fn new(metric_type: MetricType, histogram_max_micros: Option<u64>) -> Self {
        Self {
            metric_type,
            histogram: LatencyHistogram::with_max(histogram_max_micros),
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
                if value < self.min {
                    self.min = value;
                }
                if value > self.max {
                    self.max = value;
                }
                self.last = value;
            }
            MetricType::Trend => {
                // Trend: histogram distribution
                //
                // Record EVERY sample — including 0 — so the histogram
                // population matches `count`/`sum`. The old `value > 0.0`
                // gate excluded zeros (pooled keep-alive reuse makes
                // blocked/dns/connecting 0 for most requests), so percentiles
                // were computed over a smaller, biased population while
                // count/sum covered everything → arithmetically impossible
                // results like `min > avg` and wrong p-values.
                //
                // Values are rounded (not truncated) because `value as u64`
                // silently dropped sub-µs samples: `myTrend.add(0.25)` (ms)
                // became 0 µs → p(95)=0, max=0. Rounding keeps fractional
                // µs values ≥ 0.5 in the distribution.
                self.histogram.record_micros(value.max(0.0).round() as u64);
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
    /// Request a raw, serializable snapshot (histogram V2 bytes included) for
    /// shipping to a distributed controller.
    GetSnapshot(tokio::sync::oneshot::Sender<MetricsSnapshot>),
    /// Request a total count for a specific metric.
    GetTotal {
        metric: String,
        tx: tokio::sync::oneshot::Sender<f64>,
    },
    /// Configure summary presentation before results are snapshotted.
    SetSummaryConfig {
        summary_trend_stats: Vec<String>,
        effective_thresholds: std::collections::HashMap<
            String,
            tropel_core::config::ThresholdConfig,
        >,
    },
    /// Set the latency histogram ceiling (microseconds) before any samples
    /// are recorded. `None` = auto-resize (no ceiling).
    SetHistogramMax(Option<u64>),
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

    /// Set the latency histogram ceiling (microseconds) before samples are
    /// recorded. `None` selects auto-resize (no ceiling). Best-effort.
    pub async fn set_histogram_max(&self, max_micros: Option<u64>) {
        let _ = self.tx.send(MetricsEvent::SetHistogramMax(max_micros)).await;
    }

    /// Configure summary presentation (trend stats + effective thresholds)
    /// before the end-of-run snapshot. Best-effort: if the aggregator has
    /// already shut down the config is dropped.
    pub async fn set_summary_config(
        &self,
        summary_trend_stats: Vec<String>,
        effective_thresholds: std::collections::HashMap<
            String,
            tropel_core::config::ThresholdConfig,
        >,
    ) {
        let _ = self
            .tx
            .send(MetricsEvent::SetSummaryConfig {
                summary_trend_stats,
                effective_thresholds,
            })
            .await;
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
        if self
            .tx
            .send(MetricsEvent::GetResults(resp_tx))
            .await
            .is_err()
        {
            return MetricsResult::default();
        }
        resp_rx.await.unwrap_or_default()
    }

    /// Get a raw, serializable snapshot of the aggregated series (with
    /// hdr-histogram V2 bytes for Trend metrics). Used by `tropel-agent` to
    /// ship its metrics to a `tropel-controller`, which merges histograms
    /// losslessly via [`merge_snapshots`].
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
        if self
            .tx
            .send(MetricsEvent::GetSnapshot(resp_tx))
            .await
            .is_err()
        {
            return MetricsSnapshot::default();
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
    /// Trend stats to surface in the summary (k6 `summaryTrendStats`).
    summary_trend_stats: Vec<String>,
    /// Effective threshold set (job + script-declared) for reporting.
    effective_thresholds: std::collections::HashMap<
        String,
        tropel_core::config::ThresholdConfig,
    >,
    /// Latency histogram ceiling in microseconds (None = auto-resize).
    histogram_max_micros: Option<u64>,
    /// Whether any configured threshold/summary stat needs EXACT non-tracked
    /// percentiles. Computed once when the summary config arrives (and in
    /// [`merge_snapshots`]) so it stays stable across the many `results()`
    /// calls during a run — the config fields are cloned into every result,
    /// so they must not be re-inspected per call.
    retain_histograms: bool,
}

impl Aggregator {
    fn new() -> Self {
        Self {
            data: IndexMap::new(),
            totals: HashMap::new(),
            summary_trend_stats: k6_default_trend_stats(),
            effective_thresholds: std::collections::HashMap::new(),
            histogram_max_micros: None,
            retain_histograms: false,
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
                MetricsEvent::GetSnapshot(tx) => {
                    let snap = agg.build_snapshot();
                    let _ = tx.send(snap);
                }
                MetricsEvent::GetTotal { metric, tx } => {
                    let total = agg.totals.get(&metric).copied().unwrap_or(0.0);
                    let _ = tx.send(total);
                }
                MetricsEvent::SetSummaryConfig {
                    summary_trend_stats,
                    effective_thresholds,
                } => {
                    agg.retain_histograms =
                        config_needs_histograms(&summary_trend_stats, &effective_thresholds);
                    agg.summary_trend_stats = summary_trend_stats;
                    agg.effective_thresholds = effective_thresholds;
                }
                MetricsEvent::SetHistogramMax(max) => {
                    agg.histogram_max_micros = max;
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
        let metric_set = self.data.entry(key).or_insert_with(|| {
            MetricSet::new(metric_type, self.histogram_max_micros)
        });
        metric_set.record(sample.value, &sample.sample_type);

        // Update totals — zero-alloc on the hot path: `get_mut` with a &str
        // borrow (String: Borrow<str>), only allocating on first sight of a
        // metric name.
        if let Some(total) = self.totals.get_mut(sample.metric.as_ref()) {
            *total += sample.value;
        } else {
            self.totals.insert(sample.metric.into_owned(), sample.value);
        }
    }

    fn build_results(&mut self) -> MetricsResult {
        use std::collections::btree_map::Entry;

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
        // Exact per-URL merge: one MetricSet per distinct `url` (or `name`)
        // tag, so reporters can show true per-URL percentiles instead of
        // approximating from a single series.
        let mut merged_per_url: std::collections::BTreeMap<String, MetricSet> =
            std::collections::BTreeMap::new();

        // Per-group merge: one MetricSet per (metric, group) pair for series
        // carrying a `group` tag — k6 aggregates group-scoped metrics per
        // group in its per-group breakdown, so reporters must render merged
        // histograms, not the raw per-(url,status) series.
        let mut merged_per_group: std::collections::BTreeMap<(String, String), MetricSet> =
            std::collections::BTreeMap::new();

        // Clone full histograms into summaries only when some configured
        // threshold/summary stat needs an EXACT non-tracked percentile
        // (p75, p99.9, …). The default tracked buckets (p50/p90/p95/p99) are
        // precomputed, so this avoids an O(buckets) clone per `results()`
        // call — the ~2s-per-VU threshold-check hot path. The flag is
        // computed ONCE at config time (see `Aggregator::retain_histograms`)
        // because the config fields below are cloned into every result, so
        // repeated `results()` calls (the 2s abort-threshold checks) keep
        // returning the same trend stats / threshold set instead of emptying
        // them after the first call.
        let retain_histograms = self.retain_histograms;

        for (key, set) in self.data.iter() {
            let key_str = key.to_key_string();
            let summary_tags: Vec<(String, String)> = key
                .tags
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            // Build type-appropriate summary
            let summary = match set.metric_type {
                MetricType::Counter => MetricSummary {
                    key: key_str,
                    tags: summary_tags.clone(),
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
                    histogram: None,
                },
                MetricType::Rate => MetricSummary {
                    key: key_str,
                    tags: summary_tags.clone(),
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
                    histogram: None,
                },
                MetricType::Gauge => MetricSummary {
                    key: key_str,
                    tags: summary_tags.clone(),
                    metric_type: MetricType::Gauge,
                    count: set.count as u64,
                    sum: set.sum,
                    mean: set.mean(),
                    min: if set.min == f64::MAX {
                        0
                    } else {
                        set.min as u64
                    },
                    max: if set.max == f64::MIN {
                        0
                    } else {
                        set.max as u64
                    },
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: set.last,
                    rate: 0.0,
                    histogram: None,
                },
                MetricType::Trend => {
                    let stats = set.histogram.stats();
                    MetricSummary {
                        key: key_str,
                        tags: summary_tags,
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
                        histogram: retain_histograms.then(|| set.histogram.clone()),
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
                // Exact per-URL merge (url tag, falling back to name).
                if let Some(url) = key
                    .tags
                    .iter()
                    .find(|(k, _)| k.as_ref() == "url" || k.as_ref() == "name")
                    .map(|(_, v)| v.as_ref())
                {
                    match merged_per_url.entry(url.to_string()) {
                        Entry::Occupied(mut e) => {
                            let merged = e.get_mut();
                            merged.histogram.merge(&set.histogram);
                            merged.count += set.count;
                            merged.sum += set.sum;
                        }
                        Entry::Vacant(v) => {
                            v.insert(set.clone());
                        }
                    }
                }
            }

            // Per-group merge: any series carrying a `group` tag (the runner
            // emits `group=http` by default; named groups from `group()`/
            // `pm.group` produce the meaningful rows) merges into one
            // MetricSet per (metric, group) so per-group breakdowns show
            // true aggregated histograms.
            if let Some(group) = key.tags.iter().find(|(k, _)| k.as_ref() == "group") {
                let g = group.1.as_ref().to_string();
                let fam = key.metric.to_string();
                let entry = merged_per_group.entry((fam, g));
                match entry {
                    Entry::Occupied(mut e) => {
                        let merged = e.get_mut();
                        merged.histogram.merge(&set.histogram);
                        merged.count += set.count;
                        merged.sum += set.sum;
                        if set.metric_type == MetricType::Gauge {
                            merged.min = merged.min.min(set.min);
                            merged.max = merged.max.max(set.max);
                            merged.last = set.last;
                        }
                    }
                    Entry::Vacant(v) => {
                        v.insert(set.clone());
                    }
                }
            }

            if key.metric.starts_with("http_reqs") {
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

        // Build exact per-URL http_req_duration summaries (merged histograms)
        // so reporters can show a true per-URL breakdown. Kept in the
        // dedicated `per_url` field (NOT `metrics`) so threshold evaluation
        // never double-counts samples that also exist as raw series.
        let mut per_url = Vec::with_capacity(merged_per_url.len());
        for (url, merged) in merged_per_url {
            let stats = merged.histogram.stats();
            per_url.push(MetricSummary {
                key: format!("http_req_duration{{url={}}}", url),
                tags: vec![("url".to_string(), url)],
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
                histogram: retain_histograms.then(|| merged.histogram.clone()),
            });
        }

        // Build per-group summaries (merged histograms per (metric, group))
        // so reporters show k6-style per-group breakdowns. Kept OUT of
        // `metrics` like `per_url` so thresholds never double-count.
        let mut per_group = Vec::with_capacity(merged_per_group.len());
        for ((fam, group), merged) in merged_per_group {
            let summary = match merged.metric_type {
                MetricType::Trend => {
                    let stats = merged.histogram.stats();
                    MetricSummary {
                        key: format!("{fam}{{group={group}}}"),
                        tags: vec![("group".to_string(), group.clone())],
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
                        histogram: retain_histograms.then(|| merged.histogram.clone()),
                    }
                }
                MetricType::Counter => MetricSummary {
                    key: format!("{fam}{{group={group}}}"),
                    tags: vec![("group".to_string(), group.clone())],
                    metric_type: MetricType::Counter,
                    count: merged.count as u64,
                    sum: merged.sum,
                    mean: merged.mean(),
                    min: 0,
                    max: 0,
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: 0.0,
                    rate: 0.0,
                    histogram: None,
                },
                MetricType::Rate => MetricSummary {
                    key: format!("{fam}{{group={group}}}"),
                    tags: vec![("group".to_string(), group.clone())],
                    metric_type: MetricType::Rate,
                    count: merged.count as u64,
                    sum: merged.sum,
                    mean: merged.mean(),
                    min: 0,
                    max: 0,
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: 0.0,
                    rate: merged.rate(),
                    histogram: None,
                },
                MetricType::Gauge => MetricSummary {
                    key: format!("{fam}{{group={group}}}"),
                    tags: vec![("group".to_string(), group.clone())],
                    metric_type: MetricType::Gauge,
                    count: merged.count as u64,
                    sum: merged.sum,
                    mean: merged.mean(),
                    min: if merged.min == f64::MAX {
                        0
                    } else {
                        merged.min as u64
                    },
                    max: if merged.max == f64::MIN {
                        0
                    } else {
                        merged.max as u64
                    },
                    p50: 0,
                    p90: 0,
                    p95: 0,
                    p99: 0,
                    last: merged.last,
                    rate: 0.0,
                    histogram: None,
                },
            };
            per_group.push(summary);
        }

        // Build headline iteration_duration from merged histogram
        if let Some(ref merged) = merged_iter_dur {
            let stats = merged.histogram.stats();
            iteration_duration = Some(MetricSummary {
                key: "iteration_duration".to_string(),
                tags: vec![],
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
                histogram: retain_histograms.then(|| merged.histogram.clone()),
            });
        }

        // Build headline http_req_duration from merged histogram
        if let Some(ref merged) = merged_http_dur {
            let stats = merged.histogram.stats();
            http_req_duration = Some(MetricSummary {
                key: "http_req_duration".to_string(),
                tags: vec![],
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
                histogram: retain_histograms.then(|| merged.histogram.clone()),
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
            per_url,
            per_group,
            checks_total,
            checks_passed,
            checks_failed,
            http_reqs,
            http_req_duration,
            iteration_duration,
            data_received,
            data_sent,
            errors,
            dropped_iterations: self
                .totals
                .get("dropped_iterations")
                .copied()
                .unwrap_or(0.0) as u64,
            http_req_failed: if http_req_failed_total > 0.0 {
                http_req_failed_count / http_req_failed_total
            } else {
                0.0
            },
            iterations,
            vus_max,
            summary_trend_stats: self.summary_trend_stats.clone(),
            effective_thresholds: self.effective_thresholds.clone(),
        }
    }

    /// Build a serializable snapshot of the raw aggregated series. Trend
    /// metrics carry their hdr-histogram as base64 V2 bytes so a controller
    /// can deserialize and merge them losslessly.
    fn build_snapshot(&self) -> MetricsSnapshot {
        let mut series = Vec::with_capacity(self.data.len());
        for (key, set) in &self.data {
            series.push(SeriesSnapshot {
                metric: key.metric.to_string(),
                tags: key
                    .tags
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
                metric_type: set.metric_type,
                histogram: if set.metric_type == MetricType::Trend
                    && set.histogram.count() > 0
                {
                    Some(
                        base64::engine::general_purpose::STANDARD
                            .encode(set.histogram.to_bytes()),
                    )
                } else {
                    None
                },
                count: set.count,
                sum: set.sum,
                min: set.min,
                max: set.max,
                last: set.last,
            });
        }
        MetricsSnapshot {
            series,
            totals: self.totals.clone(),
            summary_trend_stats: self.summary_trend_stats.clone(),
        }
    }

    /// Absorb a serialized snapshot from a worker: rebuild each MetricSet
    /// (deserializing Trend histograms) and merge into this aggregator.
    /// Histograms merge losslessly — the controller's total is exactly the
    /// sum of the workers' buckets.
    fn absorb_snapshot(&mut self, snap: &MetricsSnapshot) {
        for s in &snap.series {
            let mut tags = tropel_core::types::TagMap::new();
            for (k, v) in &s.tags {
                tags.insert(k.clone(), v.clone());
            }
            let key = MetricKey::new(&s.metric, &tags);

            let histogram = match &s.histogram {
                Some(b64) => base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .ok()
                    .and_then(|bytes| LatencyHistogram::from_bytes(&bytes))
                    .unwrap_or_default(),
                None => LatencyHistogram::default(),
            };

            match self.data.entry(key) {
                indexmap::map::Entry::Occupied(mut e) => {
                    let existing = e.get_mut();
                    if existing.metric_type == MetricType::Trend {
                        existing.histogram.merge(&histogram);
                    }
                    existing.count += s.count;
                    existing.sum += s.sum;
                    if s.min < existing.min {
                        existing.min = s.min;
                    }
                    if s.max > existing.max {
                        existing.max = s.max;
                    }
                    existing.last = s.last;
                }
                indexmap::map::Entry::Vacant(v) => {
                    v.insert(MetricSet {
                        metric_type: s.metric_type,
                        histogram,
                        count: s.count,
                        sum: s.sum,
                        min: s.min,
                        max: s.max,
                        last: s.last,
                    });
                }
            }
        }
        for (k, v) in &snap.totals {
            let entry = self.totals.entry(k.clone()).or_insert(0.0);
            *entry += v;
        }
        if self.summary_trend_stats.is_empty() && !snap.summary_trend_stats.is_empty() {
            self.summary_trend_stats = snap.summary_trend_stats.clone();
        }
    }
}

/// A serializable snapshot of one aggregated series. Trend metrics carry
/// their hdr-histogram as base64-encoded V2 bytes so a controller can
/// deserialize and merge them losslessly (no percentile estimation, no
/// sampling) over a compact JSON wire format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesSnapshot {
    pub metric: String,
    pub tags: Vec<(String, String)>,
    pub metric_type: MetricType,
    /// base64(hdr-histogram V2 bytes) — Trend metrics with samples only.
    pub histogram: Option<String>,
    pub count: f64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
    pub last: f64,
}

/// A serializable snapshot of a worker's aggregated metrics — the wire type
/// `tropel-agent` ships to `tropel-controller` for central lossless merging.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub series: Vec<SeriesSnapshot>,
    pub totals: HashMap<String, f64>,
    pub summary_trend_stats: Vec<String>,
}

/// Merge worker snapshots into a single `MetricsResult` (🦀 Rust-opt: the
/// hdr-histogram V2 merge is lossless — the controller's buckets are exactly
/// the sum of the workers', so percentiles/means are exact, not estimated).
///
/// The effective threshold set is taken from the `thresholds` argument (the
/// controller's job config); trend stats are inherited from the workers.
pub fn merge_snapshots(
    snapshots: Vec<MetricsSnapshot>,
    thresholds: std::collections::HashMap<String, tropel_core::config::ThresholdConfig>,
) -> MetricsResult {
    let mut agg = Aggregator::new();
    agg.retain_histograms = config_needs_histograms(&agg.summary_trend_stats, &thresholds);
    agg.effective_thresholds = thresholds;
    for snap in &snapshots {
        agg.absorb_snapshot(snap);
    }
    agg.build_results()
}

/// Summary of a single metric, with type-aware statistics.
/// Fields not applicable to the metric type are set to 0.
#[derive(Debug, Clone)]
pub struct MetricSummary {
    pub key: String,
    /// The (key, value) tag pairs that distinguish this series (e.g.
    /// `url`, `status`, `group`, `name`). Populated from the MetricKey so
    /// reporters can build per-URL / per-group breakdowns without parsing
    /// the key string.
    pub tags: Vec<(String, String)>,
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
    /// Retained latency histogram (Trend metrics only; `None` otherwise).
    /// Kept so threshold/summary evaluation can compute EXACT arbitrary
    /// percentiles (e.g. `p75`, `p99.9`, `p(90)`) instead of falling back
    /// to the mean or a nearest-tracked-bucket approximation.
    pub histogram: Option<LatencyHistogram>,
}

/// Aggregated metrics result.
#[derive(Debug, Clone)]
pub struct MetricsResult {
    pub metrics: Vec<MetricSummary>,
    /// Exact per-URL http_req_duration summaries (histograms merged per
    /// distinct `url` tag). Kept OUT of `metrics` so threshold evaluation
    /// (which iterates `metrics`) can't double-count the same samples that
    /// already exist as raw per-(url,method,status) series. Reporters render
    /// these for the per-URL breakdown.
    pub per_url: Vec<MetricSummary>,
    /// Per-group summaries (histograms merged per (metric, group) for
    /// series carrying a `group` tag) — k6-style per-group breakdown data.
    /// Also kept OUT of `metrics` so thresholds never double-count.
    pub per_group: Vec<MetricSummary>,
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
    /// Trend statistics to show in the summary, k6 `summaryTrendStats`
    /// semantics (e.g. `["avg","min","med","max","p(90)","p(95)","p(99)"]`).
    /// Defaults to the k6 set. Reporters must honor this list.
    pub summary_trend_stats: Vec<String>,
    /// The thresholds actually applied to the run (job + script-declared).
    /// Reporters evaluate and display pass/fail against this set.
    pub effective_thresholds: std::collections::HashMap<
        String,
        tropel_core::config::ThresholdConfig,
    >,
}

impl Default for MetricsResult {
    fn default() -> Self {
        Self {
            metrics: vec![],
            per_url: vec![],
            per_group: vec![],
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
            summary_trend_stats: k6_default_trend_stats(),
            effective_thresholds: std::collections::HashMap::new(),
        }
    }
}

/// The k6 default `summaryTrendStats` list.
pub fn k6_default_trend_stats() -> Vec<String> {
    ["avg", "min", "med", "max", "p(90)", "p(95)", "p(99)"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

/// Map a k6 `summaryTrendStats` entry onto the `MetricSummary` field it
/// refers to. Returns `None` for unknown entries (caller should skip).
pub fn trend_stat_value(stat: &str, m: &MetricSummary) -> Option<f64> {
    match stat.trim() {
        "avg" | "mean" => Some(m.mean),
        "min" => Some(m.min as f64),
        "med" | "median" => Some(m.p50 as f64),
        "max" => Some(m.max as f64),
        "count" => Some(m.count as f64),
        "sum" => Some(m.sum),
        "rate" => Some(m.rate),
        s if parse_percentile(s).is_some() => {
            let pct = parse_percentile(s).expect("checked by guard");
            // Exact percentile from the retained histogram when available;
            // falls back to the nearest tracked bucket only when the
            // histogram was not retained (e.g. synthetic summaries).
            Some(percentile_value(m, pct))
        }
        _ => None,
    }
}

/// Do the configured threshold expressions and summary trend stats require
/// the retained histogram (i.e. does any reference a non-tracked percentile)?
fn config_needs_histograms(
    trend_stats: &[String],
    thresholds: &std::collections::HashMap<String, tropel_core::config::ThresholdConfig>,
) -> bool {
    trend_stats.iter().any(|s| stat_needs_histogram(s))
        || thresholds.values().any(|t| {
            // Mirror `evaluate_single_threshold`: the expression is
            // "<metric_ref> <op> <value>", so parse ONLY the first token.
            // Passing the whole expression would make the stat come out as
            // e.g. "75 < 300" (the `.rfind('.')` grabs the trailing value)
            // and retention would never trigger.
            let metric_ref = t.expression.split_whitespace().next().unwrap_or("");
            let (_, _, stat) = parse_metric_ref(metric_ref);
            stat.map(stat_needs_histogram).unwrap_or(false)
        })
}

/// Does a stat reference require the retained histogram to evaluate exactly?
///
/// The tracked buckets (p50/p90/p95/p99 and aliases avg/min/med/max/count/
/// sum/rate/last) are precomputed in `MetricSummary`, so only NON-tracked
/// percentile values (e.g. `p75`, `p(90.5)`, `p99.9`) need the histogram
/// retained. This gates the per-`results()` histogram clone on the hot path:
/// default configs (summaryTrendStats uses p(90)/p(95)/p(99)) never pay it.
pub(crate) fn stat_needs_histogram(stat: &str) -> bool {
    let s = stat.trim();
    if matches!(
        s,
        "avg" | "mean"
            | "min"
            | "max"
            | "count"
            | "sum"
            | "rate"
            | "last"
            | "p50"
            | "median"
            | "med"
            | "p90"
            | "p95"
            | "p99"
    ) {
        return false;
    }
    match parse_percentile(s) {
        // Non-tracked percentile values need the histogram for exactness;
        // tracked values (50/90/95/99) in any syntax are already exact.
        Some(pct) => !(pct == 50.0 || pct == 90.0 || pct == 95.0 || pct == 99.0),
        None => false,
    }
}

/// Parse a percentile stat reference like `p95`, `p75`, `p99.9` or `p(90)`
/// into a percentile value in 0–100. Returns `None` for non-percentile stats.
pub fn parse_percentile(stat: &str) -> Option<f64> {
    let s = stat.trim();
    if s.starts_with("p(") && s.ends_with(')') {
        return s[2..s.len() - 1].trim().parse().ok();
    }
    let num = s.strip_prefix('p')?;
    num.parse().ok()
}

/// Exact percentile from a Trend summary's retained histogram; falls back to
/// the nearest tracked bucket (p50/p90/p95/p99) only when no histogram was
/// retained (e.g. test fixtures, or the pre-config window before the summary
/// config arrives).
///
/// The fallback is deliberately CONSERVATIVE for `<`/`<=` thresholds: for
/// p75 the nearest tracked bucket is p90, which is ≥ p75 in any real
/// distribution, so a `p75 < X` threshold evaluated against it can only
/// false-FAIL, never false-PASS. Note the opposite caveat for `>`/`>=`
/// thresholds (a higher bucket can false-PASS) — which is exactly why
/// `retain_histograms` exists: when a non-tracked percentile is actually
/// referenced in a threshold, the histogram IS retained and the value is
/// exact, so this fallback only fires in the pre-config window / synthetic
/// summaries.
pub fn percentile_value(m: &MetricSummary, pct: f64) -> f64 {
    if let Some(h) = &m.histogram {
        h.percentile(pct) as f64
    } else {
        match pct {
            x if x <= 50.0 => m.p50 as f64,
            x if x <= 90.0 => m.p90 as f64,
            x if x <= 95.0 => m.p95 as f64,
            _ => m.p99 as f64,
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

        assert_eq!(
            key1, key2,
            "keys should be equal regardless of tag insertion order"
        );
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

    #[test]
    fn test_stat_needs_histogram() {
        // Tracked buckets + aliases — no histogram needed.
        for tracked in ["avg", "min", "max", "count", "sum", "rate", "last",
                        "p50", "median", "med", "p90", "p95", "p99"] {
            assert!(!stat_needs_histogram(tracked), "{tracked} should not need a histogram");
        }
        // Tracked values in any syntax (incl. k6 p(NN) form) — exact already.
        assert!(!stat_needs_histogram("p(90)"));
        assert!(!stat_needs_histogram("p(99)"));
        assert!(!stat_needs_histogram("p(50)"));
        // Non-tracked percentiles — need the histogram for exactness.
        assert!(stat_needs_histogram("p75"));
        assert!(stat_needs_histogram("p(75)"));
        assert!(stat_needs_histogram("p99.9"));
        assert!(stat_needs_histogram("p(99.9)"));
        // Non-percentile junk — no histogram, falls to default handling.
        assert!(!stat_needs_histogram("bogus"));
        assert!(!stat_needs_histogram(""));
    }

    #[test]
    fn test_config_needs_histograms_threshold_scan() {
        use tropel_core::config::ThresholdConfig;

        let mut thresholds: std::collections::HashMap<String, ThresholdConfig> =
            std::collections::HashMap::new();
        thresholds.insert(
            "p95".into(),
            ThresholdConfig {
                expression: "http_req_duration.p95 < 500".into(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
        assert!(!config_needs_histograms(&k6_default_trend_stats(), &thresholds));

        thresholds.insert(
            "p75".into(),
            ThresholdConfig {
                expression: "http_req_duration{status=200}.p75 < 300".into(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
        assert!(config_needs_histograms(&k6_default_trend_stats(), &thresholds));

        // summaryTrendStats p(99.9) also triggers retention.
        let stats = vec!["avg".into(), "p(99.9)".into()];
        assert!(config_needs_histograms(&stats, &std::collections::HashMap::new()));
    }

    #[test]
    fn test_trend_population_includes_zero_samples() {
        // Regression (backlog line 65): the Trend arm used to gate histogram
        // recording on `value > 0.0` while `count`/`sum` always incremented.
        // Pooled keep-alive reuse makes sub-timings (blocked/dns/connecting)
        // 0 for most requests, so percentiles were computed over a smaller
        // biased population → `min > avg` (arithmetically impossible).
        let mut set = MetricSet::new(MetricType::Trend, None);
        let trend = SampleType::Trend;

        // Simulate 10 k requests where only 10 actually connected (~25 ms):
        // the rest are pooled reuse with connecting = 0.
        for _ in 0..9990 {
            set.record(0.0, &trend);
        }
        for _ in 0..10 {
            set.record(25_000.0, &trend); // 25 ms in µs
        }

        assert_eq!(set.count, 10_000.0, "count must include zero samples");
        let stats = set.histogram.stats();
        assert_eq!(stats.count, 10_000, "histogram population must match count");
        assert!(
            stats.min <= stats.max,
            "min ({}) must be <= max ({})",
            stats.min,
            stats.max
        );
        let mean = set.mean();
        assert!(
            (stats.min as f64) <= mean,
            "min ({}) must be <= avg ({}) — the old bug produced min > avg",
            stats.min,
            mean
        );
        // With 9990/10000 zeros, even p99 sits in the zero bucket.
        assert_eq!(stats.p99, 1, "p99 should reflect the zero-majority population");
    }

    #[test]
    fn test_trend_fractional_values_round_not_truncate() {
        // Regression: `value as u64` truncated fractional µs, so
        // `myTrend.add(0.25)` (ms) recorded 0 µs → p(95)=0, max=0 while
        // avg stayed meaningful. Values ≥ 0.5 µs must round into the
        // histogram instead of vanishing.
        let mut set = MetricSet::new(MetricType::Trend, None);
        let trend = SampleType::Trend;

        set.record(0.25, &trend); // truncation would drop this to 0
        set.record(0.6, &trend); // rounds to 1 µs
        set.record(2_500.0, &trend); // 2.5 ms

        assert_eq!(set.count, 3.0);
        let stats = set.histogram.stats();
        assert_eq!(stats.count, 3, "all samples must be in the histogram");
        assert!(
            stats.max >= 2_500,
            "2.5 ms sample must be recorded (max={})",
            stats.max
        );
        assert!(
            stats.min >= 1,
            "0.25/0.6 are clamped to the 1 µs floor (min={}) — not dropped to 0",
            stats.min
        );
    }

    #[test]
    fn test_trend_all_zero_samples_stay_in_population() {
        // Direct pin of the clamp path: an all-zero trend (every sub-timing
        // 0 on pooled reuse) must still record every sample — min == max == 1
        // µs (hdrhistogram floor), count fully populated. (min <= avg cannot
        // hold here: the 1 µs clamp makes min == 1 while the mean is 0 µs.)
        let mut set = MetricSet::new(MetricType::Trend, None);
        let trend = SampleType::Trend;
        for _ in 0..100 {
            set.record(0.0, &trend);
        }

        assert_eq!(set.count, 100.0);
        let stats = set.histogram.stats();
        assert_eq!(stats.count, 100, "all 100 zeros must be recorded");
        assert_eq!(stats.min, 1);
        assert_eq!(stats.max, 1);
        assert_eq!(set.sum, 0.0);
    }

    #[test]
    fn test_build_results_keeps_summary_config_across_calls() {
        // Regression: build_results() used to mem::take the summary config,
        // so the SECOND results() call (every abort-threshold check after the
        // first) returned empty trend stats and thresholds. Now cloned into
        // every result.
        use tropel_core::config::ThresholdConfig;

        let mut agg = Aggregator::new();
        let mut thresholds: std::collections::HashMap<String, ThresholdConfig> =
            std::collections::HashMap::new();
        thresholds.insert(
            "p95".into(),
            ThresholdConfig {
                expression: "http_req_duration.p95 < 500".into(),
                abort_on_fail: true,
                delay_abort_eval: None,
            },
        );
        agg.summary_trend_stats = vec!["avg".into(), "p(95)".into()];
        agg.effective_thresholds = thresholds.clone();

        let first = agg.build_results();
        assert_eq!(first.summary_trend_stats, vec!["avg", "p(95)"]);
        assert_eq!(first.effective_thresholds.len(), 1);
        assert!(first.effective_thresholds.contains_key("p95"));

        // Second call must still carry the config (previously drained).
        let second = agg.build_results();
        assert_eq!(second.summary_trend_stats, vec!["avg", "p(95)"]);
        assert_eq!(second.effective_thresholds.len(), 1);
        assert!(second.effective_thresholds.contains_key("p95"));
    }
}
