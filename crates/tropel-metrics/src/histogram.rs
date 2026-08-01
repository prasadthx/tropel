use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A latency histogram backed by HdrHistogram.
#[derive(Debug, Clone)]
pub struct LatencyHistogram {
    inner: Histogram<u64>,
}

impl LatencyHistogram {
    /// Create a new auto-resizing histogram (1 μs low bound, unbounded high).
    ///
    /// hdrhistogram's `Histogram::new(sigfig)` enables auto-resize: values
    /// above the initial ceiling grow the histogram instead of being silently
    /// dropped. The old fixed 60 s ceiling clipped very slow requests, which
    /// skewed p99/max and under-counted latency.
    pub fn new() -> Self {
        let inner = Histogram::new(3).expect("Failed to create auto-resizing histogram");
        Self { inner }
    }

    /// Create a new histogram with custom bounds (fixed ceiling — values above
    /// `high` are silently clamped/dropped, matching k6's bounded histogram
    /// behavior). Prefer [`Self::new`] (auto-resize) unless a bounded ceiling
    /// is explicitly required.
    pub fn with_bounds(low: u64, high: u64) -> Self {
        let inner = Histogram::new_with_bounds(low, high, 3).expect("Failed to create histogram");
        Self { inner }
    }

    /// Create a new histogram with a custom high bound in microseconds
    /// (1 μs low bound). `None` (or an unusable ceiling) selects the
    /// auto-resizing variant — a garbage ceiling must not panic inside the
    /// aggregator task on the first recorded sample.
    ///
    /// hdrhistogram requires `high >= 2 * low` (with `low = 1` that means
    /// `high >= 2`), so ceilings of 0 or 1 fall back to auto-resize. Very
    /// large `high` values are safe: with low=1 the internal magnitude sum
    /// is always < 63, so the constructor never rejects them.
    pub fn with_max(max_micros: Option<u64>) -> Self {
        match max_micros {
            Some(high) if high >= 2 => Self::with_bounds(1, high),
            _ => Self::new(),
        }
    }

    /// Record a duration value.
    pub fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.inner.record(micros).ok();
    }

    /// Record a value in microseconds.
    pub fn record_micros(&mut self, micros: u64) {
        self.inner.record(micros).ok();
    }

    /// Get the total count of recorded values.
    pub fn count(&self) -> u64 {
        self.inner.len()
    }

    /// Get the minimum value (in microseconds).
    pub fn min(&self) -> u64 {
        self.inner.min()
    }

    /// Get the maximum value (in microseconds).
    pub fn max(&self) -> u64 {
        self.inner.max()
    }

    /// Get the mean value (in microseconds).
    pub fn mean(&self) -> f64 {
        self.inner.mean()
    }

    /// Get a percentile value (in microseconds).
    pub fn percentile(&self, p: f64) -> u64 {
        self.inner.value_at_percentile(p)
    }

    /// Get the p50 (median) in microseconds.
    pub fn p50(&self) -> u64 {
        self.percentile(50.0)
    }

    /// Get the p90 in microseconds.
    pub fn p90(&self) -> u64 {
        self.percentile(90.0)
    }

    /// Get the p95 in microseconds.
    pub fn p95(&self) -> u64 {
        self.percentile(95.0)
    }

    /// Get the p99 in microseconds.
    pub fn p99(&self) -> u64 {
        self.percentile(99.0)
    }

    /// Merge another histogram into this one.
    /// All recorded values from `other` are added to this histogram.
    pub fn merge(&mut self, other: &LatencyHistogram) {
        self.inner.add(&other.inner).ok();
    }

    /// Export histogram statistics.
    pub fn stats(&self) -> HistogramStats {
        HistogramStats {
            count: self.count(),
            min: self.min(),
            max: self.max(),
            mean: self.mean(),
            p50: self.p50(),
            p90: self.p90(),
            p95: self.p95(),
            p99: self.p99(),
        }
    }
}

impl Default for LatencyHistogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of histogram statistics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramStats {
    pub count: u64,
    pub min: u64,
    pub max: u64,
    pub mean: f64,
    pub p50: u64,
    pub p90: u64,
    pub p95: u64,
    pub p99: u64,
}
