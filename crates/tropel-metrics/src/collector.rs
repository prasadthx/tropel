use crate::histogram::LatencyHistogram;
use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tropel_core::types::{Sample, SampleType};

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

/// The top-level metrics collector.
/// Aggregates samples from all VUs.
pub struct MetricsCollector {
    /// Metrics grouped by (metric_name, tag_key=tag_value).
    data: Arc<Mutex<IndexMap<String, MetricSet>>>,
    /// Total counters by metric name.
    totals: Arc<Mutex<HashMap<String, f64>>>,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            data: Arc::new(Mutex::new(IndexMap::new())),
            totals: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Record a batch of samples.
    pub async fn record_batch(&self, samples: &[Sample]) {
        let mut data = self.data.lock().await;
        let mut totals = self.totals.lock().await;

        for sample in samples {
            let tag_str = self.tags_to_key(&sample.tags);
            let key = format!("{}{}", sample.metric, tag_str);

            let metric_set = data.entry(key).or_insert_with(MetricSet::new);
            metric_set.record(sample.value, &sample.sample_type);

            // Update totals
            let total = totals.entry(sample.metric.clone()).or_insert(0.0);
            *total += sample.value;
        }
    }

    /// Record a single sample.
    pub async fn record(&self, sample: &Sample) {
        self.record_batch(&[sample.clone()]).await;
    }

    /// Get aggregated results.
    pub async fn results(&self) -> MetricsResult {
        let data = self.data.lock().await;
        let mut metrics = Vec::new();

        for (key, set) in data.iter() {
            let stats = set.histogram.stats();
            metrics.push(MetricSummary {
                key: key.clone(),
                count: set.count as u64,
                sum: set.sum,
                mean: set.mean(),
                min: stats.min,
                max: stats.max,
                p50: stats.p50,
                p90: stats.p90,
                p95: stats.p95,
                p99: stats.p99,
            });
        }

        MetricsResult {
            metrics,
            ..Default::default()
        }
    }

    /// Convert tags to a string key suffix.
    fn tags_to_key(&self, tags: &HashMap<String, String>) -> String {
        if tags.is_empty() {
            String::new()
        } else {
            let mut parts: Vec<String> = tags.iter()
                .map(|(k, v)| format!("{{{}}}", [k.as_str(), v.as_str()].join("=")))
                .collect();
            parts.sort();
            parts.join(",")
        }
    }

    /// Get total count for a metric.
    pub async fn total_count(&self, metric: &str) -> f64 {
        let totals = self.totals.lock().await;
        totals.get(metric).copied().unwrap_or(0.0)
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
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
        }
    }
}
