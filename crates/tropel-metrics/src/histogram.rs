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
        // Fast path: exact bucket add. Fails when `other` has a wider range
        // than `self` and `self` cannot auto-resize (e.g. both sides came
        // from a V2 serialization round-trip, which fixes the bounds).
        if self.inner.add(&other.inner).is_ok() {
            return;
        }
        // Fallback: rebuild a fresh auto-resizing histogram from the recorded
        // bins of both sides. Lossless — HdrHistogram bin iteration yields the
        // exact value and count at every populated bucket.
        let mut merged = Histogram::<u64>::new(3)
            .expect("Failed to create auto-resizing histogram");
        for v in self.inner.iter_recorded() {
            merged
                .record_n(v.value_iterated_to(), v.count_at_value())
                .ok();
        }
        for v in other.inner.iter_recorded() {
            merged
                .record_n(v.value_iterated_to(), v.count_at_value())
                .ok();
        }
        self.inner = merged;
    }

    /// Serialize this histogram to the hdr-histogram V2 binary format.
    ///
    /// Hdr-histogram V2 is a lossless, portable encoding — two histograms
    /// serialized on different machines merge exactly. This is what makes
    /// the distributed `tropel-agent` → `tropel-controller` merge exact:
    /// agents ship bytes, the controller deserializes and `add()`s them
    /// with no precision loss (🦀 Rust-opt: no percentile estimation, no
    /// sampling — real buckets).
    pub fn to_bytes(&self) -> Vec<u8> {
        use hdrhistogram::serialization::{Serializer, V2Serializer};
        let mut serializer = V2Serializer::new();
        let mut buf = Vec::new();
        // Serialization into an in-memory Vec cannot fail in practice.
        let _ = serializer.serialize(&self.inner, &mut buf);
        buf
    }

    /// Deserialize a histogram from hdr-histogram V2 binary bytes.
    /// Returns `None` for corrupted/foreign data (callers treat it as an
    /// empty histogram rather than failing the merge).
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        use hdrhistogram::serialization::Deserializer;
        let mut deserializer = Deserializer::new();
        let mut cursor = std::io::Cursor::new(bytes);
        deserializer
            .deserialize::<u64, _>(&mut cursor)
            .ok()
            .map(|inner| Self { inner })
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
}/// Snapshot of histogram statistics.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v2_roundtrip_preserves_exact_statistics() {
        let mut h = LatencyHistogram::new();
        for ms in [1u64, 2, 3, 4, 5, 50, 100, 250] {
            h.record_micros(ms * 1000);
        }
        let bytes = h.to_bytes();
        assert!(!bytes.is_empty());

        let h2 = LatencyHistogram::from_bytes(&bytes).expect("deserialize");
        assert_eq!(h.count(), h2.count());
        assert_eq!(h.min(), h2.min());
        assert_eq!(h.max(), h2.max());
        assert!((h.mean() - h2.mean()).abs() < 1e-9);
        assert_eq!(h.p50(), h2.p50());
        assert_eq!(h.p90(), h2.p90());
        assert_eq!(h.p95(), h2.p95());
        assert_eq!(h.p99(), h2.p99());
    }

    #[test]
    fn v2_corrupt_bytes_return_none() {
        assert!(LatencyHistogram::from_bytes(b"garbage").is_none());
        assert!(LatencyHistogram::from_bytes(&[]).is_none());
    }

    #[test]
    fn merge_is_exact_sum_of_buckets() {
        let mut a = LatencyHistogram::new();
        let mut b = LatencyHistogram::new();
        a.record_micros(1_000);
        a.record_micros(2_000);
        b.record_micros(50_000);
        b.record_micros(100_000);

        // Serialize, deserialize, merge — must equal recording all four.
        let a2 = LatencyHistogram::from_bytes(&a.to_bytes()).unwrap();
        let b2 = LatencyHistogram::from_bytes(&b.to_bytes()).unwrap();
        let mut merged = a2;
        merged.merge(&b2);

        let mut direct = LatencyHistogram::new();
        direct.record_micros(1_000);
        direct.record_micros(2_000);
        direct.record_micros(50_000);
        direct.record_micros(100_000);

        assert_eq!(merged.count(), 4);
        assert_eq!(merged.count(), direct.count());
        assert_eq!(merged.max(), direct.max());
        assert_eq!(merged.p95(), direct.p95());
        assert_eq!(merged.p99(), direct.p99());
    }
}
