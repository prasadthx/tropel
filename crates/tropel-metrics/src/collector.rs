use crate::histogram::{HistogramStats, LatencyHistogram};
use crate::thresholds::parse_metric_ref;
use base64::Engine as _;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use tokio::sync::{broadcast, mpsc};
use tropel_core::types::{Sample, SampleType};
use tropel_core::{Result, TropelError};

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

/// Hard cap on distinct series the aggregator will retain. The runner tags
/// every request with the FULL URL in both `url` and `name` tags, so distinct
/// URLs × statuses scale the series map linearly; a hostile or simply
/// high-cardinality input (unique URL per request) must not OOM the process.
/// New series beyond the cap are dropped and counted; existing series keep
/// recording, and `totals` (keyed by metric NAME) stay complete.
const MAX_SERIES: usize = 100_000;

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
    /// Latency histogram — lazily allocated on the FIRST Trend sample so
    /// Counter/Rate/Gauge series (the bulk of per-URL cardinality) never
    /// allocate the ~16 KB structure. `record` on a Trend sample creates it
    /// on demand.
    pub histogram: Option<LatencyHistogram>,
    /// Histogram ceiling captured at creation; used when the histogram is
    /// lazily allocated on the first Trend sample.
    histogram_max_micros: Option<u64>,
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
            histogram: None,
            histogram_max_micros,
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
                let h = self
                    .histogram
                    .get_or_insert_with(|| LatencyHistogram::with_max(self.histogram_max_micros));
                h.record_micros(value.max(0.0).round() as u64);
                self.count += 1.0;
                self.sum += value;
            }
        }
    }

    /// Trend statistics from the lazily-allocated histogram. Returns empty
    /// defaults when no Trend sample has been recorded yet (or the histogram
    /// was never allocated) — callers on the results path read stats without
    /// cloning the histogram.
    fn trend_stats(&self) -> HistogramStats {
        self.histogram
            .as_ref()
            .map(|h| h.stats())
            .unwrap_or_default()
    }

    /// Merge another set's aggregates into this one (count/sum, Trend
    /// histograms bucket-exact, Gauge folds min/max/last). Used to rebuild
    /// the incremental merged accumulators after snapshot absorption.
    fn merge_from(&mut self, other: &MetricSet) {
        let hmax = self.histogram_max_micros;
        self.count += other.count;
        self.sum += other.sum;
        if self.metric_type == MetricType::Trend {
            if let Some(h) = other.histogram.as_ref() {
                let mine = self
                    .histogram
                    .get_or_insert_with(|| LatencyHistogram::with_max(hmax));
                mine.merge(h);
            }
        }
        if self.metric_type == MetricType::Gauge {
            if other.min < self.min {
                self.min = other.min;
            }
            if other.max > self.max {
                self.max = other.max;
            }
            self.last = other.last;
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
    /// Cardinality cap for the series map (see [`MAX_SERIES`]). A field so
    /// tests can shrink it; production uses the const default.
    max_series: usize,
    /// Incremental merged http_req_duration headline (Trend). Maintained on
    /// every recorded sample so `build_results` (the ~2 s abort-path tick)
    /// reads PRE-MERGED stats instead of re-cloning/re-merging every full
    /// histogram per series per tick.
    merged_http_dur: Option<MetricSet>,
    /// Incremental merged iteration_duration headline (Trend).
    merged_iter_dur: Option<MetricSet>,
    /// Incremental merged per-URL http_req_duration series (Trend), keyed by
    /// `url` (or `name`) tag value.
    merged_per_url: std::collections::BTreeMap<String, MetricSet>,
    /// Incremental merged per-(metric, group) series, keyed by
    /// (metric name, group value).
    merged_per_group: std::collections::BTreeMap<(String, String), MetricSet>,
    /// Samples dropped because the `max_series` cardinality cap was reached.
    series_dropped: u64,
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
            max_series: MAX_SERIES,
            merged_http_dur: None,
            merged_iter_dur: None,
            merged_per_url: std::collections::BTreeMap::new(),
            merged_per_group: std::collections::BTreeMap::new(),
            series_dropped: 0,
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

        // Cardinality guard: never let the series map grow past `max_series`.
        // The runner tags every request with the full URL in BOTH `url` and
        // `name`, so a high-cardinality input (unique URL per request) must
        // not OOM the aggregator. New series beyond the cap are dropped
        // (counted, not stored) — existing series keep recording, and the
        // name-keyed `totals` map stays complete so headline counters keep
        // accumulating under pressure. The incremental accumulators are also
        // skipped: they are keyed by the same tags, so they are bounded by
        // the same cap.
        if !self.data.contains_key(&key) && self.data.len() >= self.max_series {
            self.series_dropped += 1;
            if self.series_dropped == 1 {
                tracing::warn!(
                    "metrics: series cardinality reached {}; dropping samples for new \
                     series (check for a high-cardinality url/name tag)",
                    self.max_series
                );
            }
            if let Some(total) = self.totals.get_mut(sample.metric.as_ref()) {
                *total += sample.value;
            } else {
                self.totals.insert(sample.metric.into_owned(), sample.value);
            }
            return;
        }

        // Use the type from the first sample for this key
        let metric_set = self.data.entry(key).or_insert_with(|| {
            MetricSet::new(metric_type, self.histogram_max_micros)
        });
        metric_set.record(sample.value, &sample.sample_type);

        // Maintain the incremental merged accumulators (headline http_req_
        // duration / iteration_duration, per-URL, per-group) on EVERY sample
        // so `build_results` — the ~2 s abort-path tick — reads pre-merged
        // stats instead of re-cloning and re-merging every full histogram per
        // series per tick (the O(N)-per-2s cost that filled the bounded
        // channel and blocked `record_batch().await`).
        let hmax = self.histogram_max_micros;
        if sample.metric.as_ref() == "http_req_duration" {
            let merged = self
                .merged_http_dur
                .get_or_insert_with(|| MetricSet::new(MetricType::Trend, hmax));
            merged.record(sample.value, &sample.sample_type);

            // Exact per-URL merge (url tag, falling back to name). The
            // TagMap is FxHashMap-backed with nondeterministic iteration
            // order, so pin `url` FIRST explicitly — a bare
            // `find(url || name)` could flip the key between samples when a
            // script sets url != name, splitting one URL into two rows.
            if let Some(url) = sample
                .tags
                .iter()
                .find(|(k, _)| *k == "url")
                .or_else(|| sample.tags.iter().find(|(k, _)| *k == "name"))
                .map(|(_, v)| v)
            {
                self.merged_per_url
                    .entry(url.to_string())
                    .or_insert_with(|| MetricSet::new(MetricType::Trend, hmax))
                    .record(sample.value, &sample.sample_type);
            }
        } else if sample.metric.as_ref() == "iteration_duration" {
            let merged = self
                .merged_iter_dur
                .get_or_insert_with(|| MetricSet::new(MetricType::Trend, hmax));
            merged.record(sample.value, &sample.sample_type);
        }

        // Per-group merge: any series carrying a `group` tag (the runner
        // emits `group=http` by default; named groups from `group()` /
        // `pm.group` produce the meaningful rows).
        if let Some(group) = sample.tags.iter().find(|(k, _)| *k == "group") {
            let entry = self
                .merged_per_group
                .entry((sample.metric.to_string(), group.1.to_string()));
            entry
                .or_insert_with(|| MetricSet::new(metric_type, hmax))
                .record(sample.value, &sample.sample_type);
        }

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
        let mut vus_max: u64 = 0;
        let mut iteration_duration: Option<MetricSummary> = None;

        // The headline merged histograms (http_req_duration / iteration_duration
        // / per-URL / per-group) are maintained INCREMENTALLY in `record()`
        // (and rebuilt in `absorb_snapshot`) — see `self.merged_*`. Reading
        // them here instead of re-cloning/re-merging every full histogram per
        // series per `results()` call is what removes the O(N)-per-2s-tick
        // cost that filled the bounded channel and blocked `record_batch()`.

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
                    // k6 semantics: a Counter's `count` IS the accumulated
                    // value (myCounter.add(5)x5 -> count 25, not 5). The
                    // internal MetricSet.count stays sample-count for mean;
                    // the k6-facing summary exposes the sum.
                    count: set.sum as u64,
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
                    let stats = set.trend_stats();
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
                        histogram: retain_histograms.then(|| set.histogram.clone()).flatten(),
                    }
                }
            };

            // Derive headline values from the metric key — EXACT base-name
            // match (k6 semantics). Prefix matching folded unrelated custom
            // metrics into the headlines: a Trend named `checks_latency` hit
            // `starts_with("checks")` → "Total: 1 Passed: 250000 (25000000%)",
            // and `starts_with("vus")` captured the pre-allocated `vus_max`
            // series instead of the observed peak. MetricKey separates the
            // name from tags, so exact equality still merges every tagged
            // variant (e.g. `http_req_duration{status=200}`).
            // The merged headline/per-URL/per-group accumulators are fed in
            // `record()` — nothing to merge here per series.

            // Headline accumulators: EXACT name match only. A custom metric
            // sharing a prefix (checks_latency, errors_custom, http_reqs_total,
            // data_received_bytes, iterations_count, vus_peak) must never fold
            // into these — it still appears as its own series in `metrics`.
            if key.metric.as_ref() == "http_reqs" {
                // Counters: use the ACCUMULATED value (sum) — the totals-map
                // fallback below is also sum-based, so the two paths can
                // never disagree (the old split used sample-count here vs
                // sum in the fallback). http_reqs samples are value 1.0, so
                // this still equals the number of requests.
                http_reqs += set.sum as u64;
            } else if key.metric.as_ref() == "errors" {
                errors += set.sum as u64;
            } else if key.metric.as_ref() == "checks" {
                checks_total += set.count as u64;
                checks_passed += set.sum as u64;
                checks_failed += if set.count > set.sum {
                    (set.count - set.sum) as u64
                } else {
                    0
                }
            } else if key.metric.as_ref() == "data_received" {
                data_received += set.sum;
            } else if key.metric.as_ref() == "data_sent" {
                data_sent += set.sum;
            } else if key.metric.as_ref() == "http_req_failed" {
                http_req_failed_total += set.count;
                http_req_failed_count += set.sum;
            } else if key.metric.as_ref() == "iterations" {
                iterations += set.sum as u64;
            } else if key.metric.as_ref() == "iteration_duration" {
                // Pre-merged incrementally in `record()` — see `merged_iter_dur`.
            } else if key.metric.as_ref() == "vus" {
                // vus_max headline = OBSERVED peak of the active-VU gauge.
                // The separate `vus_max` series carries the config's
                // PRE-ALLOCATED peak — it must not feed this accumulator,
                // or a run that ramps below its cap would report the cap.
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
            // `vus_max` series is intentionally NOT matched here: it would
            // overwrite the observed peak with the config pre-allocation.

            metrics.push(summary);
        }

        // Build exact per-URL http_req_duration summaries (merged histograms)
        // so reporters can show a true per-URL breakdown. Kept in the
        // dedicated `per_url` field (NOT `metrics`) so threshold evaluation
        // never double-counts samples that also exist as raw series.
        let mut per_url = Vec::with_capacity(self.merged_per_url.len());
        for (url, merged) in &self.merged_per_url {
            let stats = merged.trend_stats();
            per_url.push(MetricSummary {
                key: format!("http_req_duration{{url={}}}", url),
                tags: vec![("url".to_string(), url.clone())],
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
                histogram: retain_histograms.then(|| merged.histogram.clone()).flatten(),
            });
        }

        // Build per-group summaries (merged histograms per (metric, group))
        // so reporters show k6-style per-group breakdowns. Kept OUT of
        // `metrics` like `per_url` so thresholds never double-count.
        let mut per_group = Vec::with_capacity(self.merged_per_group.len());
        for ((fam, group), merged) in &self.merged_per_group {
            let summary = match merged.metric_type {
                MetricType::Trend => {
                    let stats = merged.trend_stats();
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
                        histogram: retain_histograms.then(|| merged.histogram.clone()).flatten(),
                    }
                }
                MetricType::Counter => MetricSummary {
                    key: format!("{fam}{{group={group}}}"),
                    tags: vec![("group".to_string(), group.clone())],
                    metric_type: MetricType::Counter,
                    // k6 semantics: Counter count = accumulated value.
                    count: merged.sum as u64,
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

        // Build headline iteration_duration from the incremental accumulator
        if let Some(merged) = self.merged_iter_dur.as_ref() {
            let stats = merged.trend_stats();
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
                histogram: retain_histograms.then(|| merged.histogram.clone()).flatten(),
            });
        }

        // Build headline http_req_duration from the incremental accumulator
        if let Some(merged) = self.merged_http_dur.as_ref() {
            let stats = merged.trend_stats();
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
                histogram: retain_histograms.then(|| merged.histogram.clone()).flatten(),
            });
        }

        // Headline counters: take the MAX of the surviving series and the
        // totals map. `totals` is keyed by metric NAME and accumulates EVERY
        // sample — including series dropped by the `max_series` cardinality
        // cap — so it is >= the per-series sum and is the exact total when
        // the cap fired. max() keeps both paths identical when nothing was
        // dropped (the series sum already covers every sample).
        http_reqs = http_reqs.max(self.totals.get("http_reqs").copied().unwrap_or(0.0) as u64);
        errors = errors.max(self.totals.get("errors").copied().unwrap_or(0.0) as u64);
        data_received = data_received.max(self.totals.get("data_received").copied().unwrap_or(0.0));
        data_sent = data_sent.max(self.totals.get("data_sent").copied().unwrap_or(0.0));

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
                // Lazily-allocated histogram: `None` unless a Trend sample was
                // recorded (Counter/Rate/Gauge series carry no ~16 KB struct).
                histogram: set
                    .histogram
                    .as_ref()
                    .filter(|h| h.count() > 0)
                    .map(|h| base64::engine::general_purpose::STANDARD.encode(h.to_bytes())),
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
    ///
    /// Returns an error when a Trend histogram's payload is corrupt (bad
    /// base64, or valid base64 that fails hdr-histogram V2 deserialization).
    /// A "lossless merge" must NOT substitute an empty histogram while the
    /// series' `count`/`sum` still merge — that silently fabricates
    /// `avg` over all samples with percentiles over fewer, looking
    /// indistinguishable from a clean run.
    fn absorb_snapshot(&mut self, snap: &MetricsSnapshot) -> Result<()> {
        for s in &snap.series {
            let mut tags = tropel_core::types::TagMap::new();
            for (k, v) in &s.tags {
                tags.insert(k.clone(), v.clone());
            }
            let key = MetricKey::new(&s.metric, &tags);

            // The snapshot's histogram payload — `None` for non-Trend series
            // (build_snapshot only encodes Trend histograms), kept as `Option`
            // to match the lazy MetricSet field (no 16 KB struct per series).
            let histogram: Option<LatencyHistogram> = match &s.histogram {
                Some(b64) => {
                    let raw = base64::engine::general_purpose::STANDARD
                        .decode(b64)
                        .map_err(|e| {
                            TropelError::Execution(format!(
                                "corrupt histogram in worker snapshot for '{}' tags {:?}: \
                                 invalid base64: {e}",
                                s.metric, s.tags
                            ))
                        })?;
                    Some(LatencyHistogram::from_bytes(&raw).ok_or_else(|| {
                        TropelError::Execution(format!(
                            "corrupt histogram in worker snapshot for '{}' tags {:?}: \
                             {} bytes do not deserialize as hdr-histogram V2",
                            s.metric,
                            s.tags,
                            raw.len()
                        ))
                    })?)
                }
                None => None,
            };

            match self.data.entry(key) {
                indexmap::map::Entry::Occupied(mut e) => {
                    let existing = e.get_mut();
                    if existing.metric_type == MetricType::Trend {
                        if let Some(h) = &histogram {
                            existing
                                .histogram
                                .get_or_insert_with(|| {
                                    LatencyHistogram::with_max(existing.histogram_max_micros)
                                })
                                .merge(h);
                        }
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
                        histogram_max_micros: self.histogram_max_micros,
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
        // Series arrive pre-aggregated here (not through `record()`), so the
        // incremental merged accumulators must be rebuilt from `self.data` to
        // keep `build_results`' pre-merged headlines correct.
        self.rebuild_merged();
        Ok(())
    }

    /// Rebuild the incremental merged accumulators (headline http_req_duration
    /// / iteration_duration, per-URL, per-group) from the raw series map.
    /// Called after snapshot absorption (distributed path), where series are
    /// added directly to `self.data` instead of via [`Self::record`].
    fn rebuild_merged(&mut self) {
        self.merged_http_dur = None;
        self.merged_iter_dur = None;
        self.merged_per_url.clear();
        self.merged_per_group.clear();
        let hmax = self.histogram_max_micros;
        for (key, set) in &self.data {
            if key.metric.as_ref() == "http_req_duration" {
                let merged = self
                    .merged_http_dur
                    .get_or_insert_with(|| MetricSet::new(MetricType::Trend, hmax));
                merged.merge_from(set);
                if let Some(url) = key
                    .tags
                    .iter()
                    .find(|(k, _)| k.as_ref() == "url")
                    .or_else(|| key.tags.iter().find(|(k, _)| k.as_ref() == "name"))
                    .map(|(_, v)| v.as_ref())
                {
                    self.merged_per_url
                        .entry(url.to_string())
                        .or_insert_with(|| MetricSet::new(MetricType::Trend, hmax))
                        .merge_from(set);
                }
            } else if key.metric.as_ref() == "iteration_duration" {
                let merged = self
                    .merged_iter_dur
                    .get_or_insert_with(|| MetricSet::new(MetricType::Trend, hmax));
                merged.merge_from(set);
            }
            if let Some(group) = key.tags.iter().find(|(k, _)| k.as_ref() == "group") {
                self.merged_per_group
                    .entry((key.metric.to_string(), group.1.as_ref().to_string()))
                    .or_insert_with(|| MetricSet::new(set.metric_type, hmax))
                    .merge_from(set);
            }
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
) -> Result<MetricsResult> {
    let mut agg = Aggregator::new();
    agg.retain_histograms = config_needs_histograms(&agg.summary_trend_stats, &thresholds);
    agg.effective_thresholds = thresholds;
    for snap in &snapshots {
        agg.absorb_snapshot(snap)?;
    }
    Ok(agg.build_results())
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
    /// Sample count. k6 semantics per type: **Counter** = the accumulated
    /// value (myCounter.add(5)x5 -> 25); Rate = events (denominator);
    /// Trend/Gauge = samples.
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
        let stats = set.trend_stats();
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
        let stats = set.trend_stats();
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
        let stats = set.trend_stats();
        assert_eq!(stats.count, 100, "all 100 zeros must be recorded");
        assert_eq!(stats.min, 1);
        assert_eq!(stats.max, 1);
        assert_eq!(set.sum, 0.0);
    }

    #[test]
    fn test_headline_checks_not_folded_by_custom_prefix_metric() {
        // Regression (backlog line 80): a custom Trend named `checks_latency`
        // hit `starts_with("checks")` in the headline derivation and folded
        // into the checks headline → "Total: 1 Passed: 250000 (25000000%)".
        // Headline accumulators must match the metric name EXACTLY.
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());

        // Real checks Rate: 1 pass + 1 fail → total 2, passed 1.
        agg.record(Sample {
            metric: "checks".into(),
            value: 1.0,
            tags: tags.clone(),
            timestamp: ts,
            sample_type: SampleType::Rate,
        });
        agg.record(Sample {
            metric: "checks".into(),
            value: 0.0,
            tags: tags.clone(),
            timestamp: ts,
            sample_type: SampleType::Rate,
        });
        // Custom Trend sharing the "checks" prefix must NOT fold in.
        agg.record(Sample {
            metric: "checks_latency".into(),
            value: 250_000.0,
            tags,
            timestamp: ts,
            sample_type: SampleType::Trend,
        });

        let res = agg.build_results();
        assert_eq!(res.checks_total, 2, "checks_latency must not fold into checks_total");
        assert_eq!(res.checks_passed, 1);
        assert_eq!(res.checks_failed, 1);
        // The custom metric still exists as its own series.
        assert!(res.metrics.iter().any(|m| m.key == "checks_latency"));
    }

    #[test]
    fn test_headline_http_reqs_not_folded_by_custom_prefix_metric() {
        // A custom `http_reqs_total` counter must not inflate the http_reqs
        // headline (prefix matching used to capture it).
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());

        // Real emission: one http_reqs Counter sample (value 1.0) per request.
        for _ in 0..3 {
            agg.record(Sample {
                metric: "http_reqs".into(),
                value: 1.0,
                tags: tags.clone(),
                timestamp: ts,
                sample_type: SampleType::Counter,
            });
        }
        agg.record(Sample {
            metric: "http_reqs_total".into(),
            value: 999.0,
            tags,
            timestamp: ts,
            sample_type: SampleType::Counter,
        });

        let res = agg.build_results();
        assert_eq!(res.http_reqs, 3, "http_reqs_total must not fold into http_reqs");
    }

    #[test]
    fn test_counter_count_is_accumulated_value() {
        // Regression (backlog line 82): k6's Counter `count` IS the summed
        // value. `myCounter.add(5)` x5 -> k6 reports 25, Tropel reported 5
        // (the sample count). `data_received: ['count<10485760']` therefore
        // compared a REQUEST count to a BYTE threshold (off by ~2500x, false
        // PASS) with the correct value sitting in `sum`.
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());

        // Custom counter: 5 adds of value 5.0 each.
        for _ in 0..5 {
            agg.record(Sample {
                metric: "my_counter".into(),
                value: 5.0,
                tags: tags.clone(),
                timestamp: ts,
                sample_type: SampleType::Counter,
            });
        }

        let res = agg.build_results();
        let m = res
            .metrics
            .iter()
            .find(|m| m.key == "my_counter")
            .expect("my_counter series");
        assert_eq!(m.metric_type, MetricType::Counter);
        assert_eq!(m.count, 25, "Counter count must be the accumulated value, not samples");
        assert_eq!(m.sum, 25.0);
    }

    #[test]
    fn test_counter_count_headline_uses_accumulated_value() {
        // Regression (backlog line 82): http_reqs/errors headlines had two
        // conflicting definitions — sample count in the per-series loop vs
        // accumulated sum in the totals-map fallback (selected by whether
        // the first series was empty). Both must be the accumulated value.
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());

        // A counter that adds value 2.0 per event (not 1.0): sample count 3
        // but accumulated value 6 — the headline must be 6, matching the
        // totals-map fallback for an equivalent zero-series run.
        for _ in 0..3 {
            agg.record(Sample {
                metric: "http_reqs".into(),
                value: 2.0,
                tags: tags.clone(),
                timestamp: ts,
                sample_type: SampleType::Counter,
            });
        }

        let res = agg.build_results();
        assert_eq!(res.http_reqs, 6, "headline must use accumulated value, not sample count");
        // A second fresh aggregator with the same input must produce the
        // identical headline — the old code's per-series loop read sample
        // count (3) while the totals-map fallback read the accumulated sum
        // (6), so the result silently depended on internal bookkeeping.
        let mut fresh = Aggregator::new();
        let ts2 = std::time::SystemTime::now();
        for _ in 0..3 {
            fresh.record(Sample {
                metric: "http_reqs".into(),
                value: 2.0,
                tags: Arc::new(tropel_core::types::TagMap::new()),
                timestamp: ts2,
                sample_type: SampleType::Counter,
            });
        }
        assert_eq!(fresh.build_results().http_reqs, 6);
    }

    #[test]
    fn test_headline_vus_max_is_observed_peak_not_prealloc() {
        // Regression: `starts_with("vus")` also captured the `vus_max`
        // series, so the headline reported the config PRE-ALLOCATION instead
        // of the observed peak of the active-VU gauge. A run that ramps below
        // its cap must report what was actually observed.
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());

        // Observed active VUs over time: peak 5.
        for v in [2.0, 5.0, 3.0] {
            agg.record(Sample {
                metric: "vus".into(),
                value: v,
                tags: tags.clone(),
                timestamp: ts,
                sample_type: SampleType::Point, // Gauge
            });
        }
        // Config pre-allocation of 20 must not drive the headline.
        agg.record(Sample {
            metric: "vus_max".into(),
            value: 20.0,
            tags,
            timestamp: ts,
            sample_type: SampleType::Point,
        });

        let res = agg.build_results();
        assert_eq!(res.vus_max, 5, "vus_max headline must be the observed peak, not 20");
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

    #[test]
    fn merge_fails_on_corrupt_base64_histogram() {
        // Regression (backlog line 87): a corrupt agent histogram was
        // silently replaced with an empty one while its `count` still
        // merged — `count = 4,000,000` with percentiles over 3 M,
        // indistinguishable from a clean run. A lossless merge must fail
        // loudly instead.
        let snap = MetricsSnapshot {
            series: vec![SeriesSnapshot {
                metric: "http_req_duration".into(),
                tags: vec![],
                metric_type: MetricType::Trend,
                histogram: Some("!!!not-base64!!!".into()),
                count: 4_000_000.0,
                sum: 4_000_000_000.0,
                min: 1.0,
                max: 2000.0,
                last: 0.0,
            }],
            totals: std::collections::HashMap::new(),
            summary_trend_stats: k6_default_trend_stats(),
        };
        let err = merge_snapshots(vec![snap], std::collections::HashMap::new()).unwrap_err();
        assert!(
            err.to_string().contains("http_req_duration"),
            "error must name the metric: {err}"
        );
        assert!(err.to_string().contains("base64"), "error must explain: {err}");
    }

    #[test]
    fn merge_fails_on_truncated_v2_bytes() {
        // Valid base64 that fails hdr-histogram V2 deserialization must also
        // fail the merge (not substitute an empty histogram).
        let snap = MetricsSnapshot {
            series: vec![SeriesSnapshot {
                metric: "iteration_duration".into(),
                tags: vec![("group".into(), "checkout".into())],
                metric_type: MetricType::Trend,
                // "garbage" base64-encodes to a few bytes that will never
                // parse as a V2 histogram header.
                histogram: Some(
                    base64::engine::general_purpose::STANDARD.encode(b"garbage"),
                ),
                count: 10.0,
                sum: 1000.0,
                min: 1.0,
                max: 100.0,
                last: 0.0,
            }],
            totals: std::collections::HashMap::new(),
            summary_trend_stats: k6_default_trend_stats(),
        };
        let err = merge_snapshots(vec![snap], std::collections::HashMap::new()).unwrap_err();
        assert!(
            err.to_string().contains("iteration_duration"),
            "error must name the metric: {err}"
        );
        assert!(
            err.to_string().contains("hdr-histogram V2"),
            "error must explain: {err}"
        );
    }

    #[test]
    fn merge_roundtrip_clean_snapshots_losslessly() {
        // A clean snapshot must still merge losslessly — the loud-failure
        // change must not break the happy path (2 histograms -> exact total).
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        let tags = Arc::new(tropel_core::types::TagMap::new());
        for ms in [1u64, 2, 3] {
            agg.record(Sample {
                metric: "http_req_duration".into(),
                value: (ms * 1000) as f64,
                tags: tags.clone(),
                timestamp: ts,
                sample_type: SampleType::Trend,
            });
        }
        let snap_a = agg.build_snapshot();

        let mut agg2 = Aggregator::new();
        for ms in [50u64, 100] {
            agg2.record(Sample {
                metric: "http_req_duration".into(),
                value: (ms * 1000) as f64,
                tags: Arc::new(tropel_core::types::TagMap::new()),
                timestamp: ts,
                sample_type: SampleType::Trend,
            });
        }
        let snap_b = agg2.build_snapshot();

        let merged =
            merge_snapshots(vec![snap_a, snap_b], std::collections::HashMap::new()).unwrap();
        let m = merged.http_req_duration.expect("merged duration");
        assert_eq!(m.count, 5, "all 5 samples must merge");
        assert!(m.max >= 100_000, "max must reflect the merged buckets");
    }

    #[test]
    fn test_histogram_lazy_for_non_trend_series() {
        // Regression (backlog line 110): every series allocated a ~16 KB
        // LatencyHistogram regardless of type. Counter/Rate/Gauge series must
        // keep `histogram: None` — only Trend records allocate.
        let mut counter = MetricSet::new(MetricType::Counter, None);
        counter.record(5.0, &SampleType::Counter);
        assert!(counter.histogram.is_none(), "Counter must not allocate a histogram");

        let mut rate = MetricSet::new(MetricType::Rate, None);
        rate.record(1.0, &SampleType::Rate);
        assert!(rate.histogram.is_none(), "Rate must not allocate a histogram");

        let mut gauge = MetricSet::new(MetricType::Gauge, None);
        gauge.record(3.0, &SampleType::Point);
        assert!(gauge.histogram.is_none(), "Gauge must not allocate a histogram");

        let mut trend = MetricSet::new(MetricType::Trend, None);
        trend.record(1.5, &SampleType::Trend);
        assert!(trend.histogram.is_some(), "Trend must allocate on first sample");
        assert_eq!(trend.trend_stats().count, 1, "trend_stats reads the lazy histogram");
    }

    #[test]
    fn test_max_series_cardinality_cap_drops_new_series() {
        // Regression (backlog line 110): unbounded cardinality — the runner
        // tags every request with the FULL URL in `url`/`name`, so a
        // high-cardinality input (unique URL per request) must not OOM the
        // aggregator. New series beyond `max_series` are dropped and counted;
        // existing series keep recording; `totals` stays complete.
        let mut agg = Aggregator::new();
        agg.max_series = 2;
        let ts = std::time::SystemTime::now();

        for (metric, url) in [
            ("http_req_duration", "/a"),
            ("http_req_duration", "/b"),
        ] {
            let tags = Arc::new(tropel_core::types::TagMap::from_pairs([
                ("url", url),
                ("name", url),
            ]));
            agg.record(Sample {
                metric: metric.into(),
                value: 1.0,
                tags,
                timestamp: ts,
                sample_type: SampleType::Trend,
            });
        }
        assert_eq!(agg.data.len(), 2);

        // A third distinct URL is dropped (counted, not stored).
        let tags = Arc::new(tropel_core::types::TagMap::from_pairs([
            ("url", "/c"),
            ("name", "/c"),
        ]));
        agg.record(Sample {
            metric: "http_req_duration".into(),
            value: 1.0,
            tags,
            timestamp: ts,
            sample_type: SampleType::Trend,
        });
        assert_eq!(agg.data.len(), 2, "series map must stay at max_series");
        assert_eq!(agg.series_dropped, 1, "dropped series must be counted");

        // Existing series still record.
        let tags = Arc::new(tropel_core::types::TagMap::from_pairs([
            ("url", "/a"),
            ("name", "/a"),
        ]));
        agg.record(Sample {
            metric: "http_req_duration".into(),
            value: 2.0,
            tags,
            timestamp: ts,
            sample_type: SampleType::Trend,
        });
        let res = agg.build_results();
        assert_eq!(res.http_reqs, 0); // no http_reqs counter samples
        let a = res
            .per_url
            .iter()
            .find(|u| u.key.contains("/a"))
            .expect("/a per-url series kept recording");
        assert_eq!(a.count, 2, "existing /a series records both samples");
    }

    #[test]
    fn test_incremental_merged_accumulators_match_data() {
        // Regression (backlog line 110): build_results used to re-clone and
        // re-merge every full histogram per series on every call (the ~2s
        // threshold tick). The incremental accumulators maintained in
        // `record()` must produce the SAME headline/per-URL/per-group stats
        // as the old per-call merge over `self.data`.
        let mut agg = Aggregator::new();
        let ts = std::time::SystemTime::now();
        for url in ["/a", "/b"] {
            for ms in [10u64, 20, 30] {
                let tags = Arc::new(tropel_core::types::TagMap::from_pairs([
                    ("url", url),
                    ("name", url),
                    ("group", "http"),
                ]));
                agg.record(Sample {
                    metric: "http_req_duration".into(),
                    value: (ms * 1000) as f64,
                    tags,
                    timestamp: ts,
                    sample_type: SampleType::Trend,
                });
            }
        }
        // iteration_duration samples (no url tag → headline only).
        for _ in 0..2 {
            agg.record(Sample {
                metric: "iteration_duration".into(),
                value: 42_000.0,
                tags: Arc::new(tropel_core::types::TagMap::new()),
                timestamp: ts,
                sample_type: SampleType::Trend,
            });
        }

        let res = agg.build_results();

        let hd = res.http_req_duration.expect("headline http_req_duration");
        assert_eq!(hd.count, 6, "all 6 duration samples merged");
        // 10+20+30+10+20+30 ms = 120 ms = 120_000 µs.
        assert_eq!(hd.sum, 120_000.0);
        // p95 over [10,20,30]x2 is the max (30 ms); hdr-histogram bucket
        // rounding lands on the bucket edge (30015), never below the true
        // value — assert within tolerance, not exact.
        assert!(
            hd.p95 >= 30_000 && hd.p95 <= 30_100,
            "p95 over 10/20/30 x2 must be ~30ms, got {}",
            hd.p95
        );

        assert_eq!(res.per_url.len(), 2, "two distinct URLs");
        for u in &res.per_url {
            assert_eq!(u.count, 3, "each URL sees its 3 samples");
        }

        // Per-group: http_req_duration{group=http} plus iteration_duration
        // has NO group tag, so only the duration series land here.
        assert!(res.per_group.iter().any(|g| g.count == 6), "duration group merged");

        let id = res.iteration_duration.expect("headline iteration_duration");
        assert_eq!(id.count, 2);
        assert_eq!(id.sum, 84_000.0);
    }
}
