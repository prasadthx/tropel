use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Build an auto-resizing histogram without ever panicking (backlog P3).
///
/// sigfig 3 is always within hdrhistogram's valid `0..=5` range, so the
/// error branch is unreachable in practice; it is handled defensively so a
/// future hdr-histogram constraint change can never unwind out of the
/// aggregator task (which has no panic guard).
fn auto_resizing_histogram() -> Histogram<u64> {
    match Histogram::new(3) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("Failed to create auto-resizing histogram: {e}");
            // sigfig 1 is also always valid; last-resort fallback.
            match Histogram::new(1) {
                Ok(h) => h,
                Err(e2) => {
                    tracing::error!("histogram fallback also failed: {e2}");
                    // Unreachable: sigfig 1 is in the valid range.
                    panic!("hdr-histogram rejected valid sigfig 1: {e2}")
                }
            }
        }
    }
}

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
    ///
    /// Construction is infallible (sigfig 3 is always in the valid 0..=5
    /// range); the fallback exists only to keep the aggregator task panic-free
    /// by construction (backlog P3).
    pub fn new() -> Self {
        Self {
            inner: auto_resizing_histogram(),
        }
    }

    /// Create a new histogram with custom bounds (fixed ceiling — values above
    /// `high` are silently clamped/dropped, matching k6's bounded histogram
    /// behavior). Prefer [`Self::new`] (auto-resize) unless a bounded ceiling
    /// is explicitly required.
    ///
    /// Garbage bounds must not panic inside the aggregator task AND must not
    /// silently become a degenerate tiny ceiling that drops every sample:
    /// hdrhistogram requires `low >= 1` and `high >= 2 * low`, so invalid
    /// inputs fall back to auto-resize with a logged error instead of
    /// panicking or truncating to a useless 2 µs histogram (backlog P3).
    pub fn with_bounds(low: u64, high: u64) -> Self {
        if low < 1 || high < low.saturating_mul(2) {
            tracing::error!(
                "Invalid histogram bounds {low}..{high} (need low >= 1, high >= 2*low); \
                 falling back to auto-resize"
            );
            return Self::new();
        }
        match Histogram::new_with_bounds(low, high, 3) {
            Ok(inner) => Self { inner },
            Err(e) => {
                tracing::error!(
                    "Failed to create histogram with bounds {low}..{high}: {e}; falling back to auto-resize"
                );
                Self::new()
            }
        }
    }

    /// Create a new histogram with a custom high bound in milliseconds
    /// (1 ms low bound). `None` (or an unusable ceiling) selects the
    /// auto-resizing variant — a garbage ceiling must not panic inside the
    /// aggregator task on the first recorded sample.
    ///
    /// hdrhistogram requires `high >= 2 * low` (with `low = 1` that means
    /// `high >= 2`), so ceilings of 0 or 1 fall back to auto-resize. Very
    /// large `high` values are safe: with low=1 the internal magnitude sum
    /// is always < 63, so the constructor never rejects them.
    pub fn with_max(max_ms: Option<u64>) -> Self {
        match max_ms {
            Some(high) if high >= 2 => Self::with_bounds(1, high),
            _ => Self::new(),
        }
    }

    /// Record a duration value (in milliseconds).
    pub fn record(&mut self, duration: Duration) {
        let ms = duration.as_millis() as u64;
        self.inner.record(ms.max(1)).ok();
    }

    /// Record a value in milliseconds.
    ///
    /// Values are clamped to the histogram's lowest trackable value (1 ms)
    /// BEFORE recording: hdrhistogram rejects 0 (returns `RecordError`), and
    /// the caller's `.ok()` would silently drop it — recreating the
    /// population-mismatch bug (zeros excluded from percentiles while still
    /// counted in `count`/`sum`). Clamping keeps zero samples in the
    /// distribution, so `min ≤ avg` always holds.
    pub fn record_ms(&mut self, ms: u64) {
        self.inner.record(ms.max(1)).ok();
    }

    /// Get the total count of recorded values.
    pub fn count(&self) -> u64 {
        self.inner.len()
    }

    /// Get the minimum value (in milliseconds).
    pub fn min(&self) -> u64 {
        self.inner.min()
    }

    /// Get the maximum value (in milliseconds).
    pub fn max(&self) -> u64 {
        self.inner.max()
    }

    /// Get the mean value (in milliseconds).
    pub fn mean(&self) -> f64 {
        self.inner.mean()
    }

    /// Get a percentile value (in milliseconds).
    pub fn percentile(&self, p: f64) -> u64 {
        self.inner.value_at_percentile(p)
    }

    /// Get the p50 (median) in milliseconds.
    pub fn p50(&self) -> u64 {
        self.percentile(50.0)
    }

    /// Get the p90 in milliseconds.
    pub fn p90(&self) -> u64 {
        self.percentile(90.0)
    }

    /// Get the p95 in milliseconds.
    pub fn p95(&self) -> u64 {
        self.percentile(95.0)
    }

    /// Get the p99 in milliseconds.
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
        let mut merged = auto_resizing_histogram();
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
        // Serialization into an in-memory Vec cannot fail in practice, but a
        // failure must never be swallowed silently: an empty/partial buffer
        // shipped to a controller would otherwise deserialize as "no data"
        // and corrupt the merge the same way a truncated frame does.
        if let Err(e) = serializer.serialize(&self.inner, &mut buf) {
            tracing::error!(
                "hdr-histogram V2 serialization failed ({} samples): {e}",
                self.count()
            );
        }
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
}
/// Snapshot of histogram statistics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
            h.record_ms(ms);
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
    fn percentiles_track_ground_truth() {
        // Backlog §6 P1: the old tests were self-consistency only (h == h2,
        // merged == direct) — a p50() returning the mean would pass all of
        // them. Record 1..=1000 ms (mean = 500.5) and assert each percentile
        // tracks its own ground truth, NOT the mean.
        let mut h = LatencyHistogram::new();
        for v in 1..=1000u64 {
            h.record_ms(v);
        }
        // hdr-histogram returns the upper edge of the bucket containing the
        // requested percentile, so allow ±3 ms of quantization tolerance. The
        // point is p50 ≈ 500 while p90 ≈ 900 — a mean-returning p50 would
        // give ~500.5 for ALL of them.
        let approx = |got: u64, want: u64| (got as i64 - want as i64).abs() <= 3;
        assert!(approx(h.p50(), 500), "p50={} ~= 500", h.p50());
        assert!(approx(h.p90(), 900), "p90={} ~= 900", h.p90());
        assert!(approx(h.p95(), 950), "p95={} ~= 950", h.p95());
        assert!(approx(h.p99(), 990), "p99={} ~= 990", h.p99());
        // min/max/sum/mean must also be exact-ish for this uniform set.
        assert_eq!(h.min(), 1);
        assert_eq!(h.max(), 1000);
        assert_eq!(h.count(), 1000);
        assert!((h.mean() - 500.5).abs() < 1.0, "mean={}", h.mean());
    }

    #[test]
    fn garbage_bounds_fall_back_to_auto_resize() {
        // Backlog P3: hdr-histogram requires high >= 2*low (low >= 1); a
        // garbage ceiling must fall back gracefully, never panic inside the
        // aggregator task. The fallback histogram must still record.
        //
        // NOTE: `max()` is the upper edge of the auto-resized bucket holding
        // the sample, not the exact recorded value (5 s lands in a bucket
        // whose top is 5,001,215 µs with sigfig 3) — assert the sample was
        // recorded, not the exact bucket edge.
        let mut h = LatencyHistogram::with_bounds(0, 0);
        h.record_ms(5_000); // 5 s — above any fixed tiny ceiling
        assert_eq!(h.count(), 1, "the sample must not be dropped");
        assert!(
            h.max() >= 5_000,
            "max={} must cover the recorded value",
            h.max()
        );

        let mut h2 = LatencyHistogram::with_bounds(1, 1); // high < 2*low
        h2.record_ms(1);
        assert_eq!(h2.count(), 1);
        assert!(
            h2.max() >= 1,
            "max={} must cover the recorded value",
            h2.max()
        );
    }

    #[test]
    fn merge_is_exact_sum_of_buckets() {
        let mut a = LatencyHistogram::new();
        let mut b = LatencyHistogram::new();
        a.record_ms(1);
        a.record_ms(2);
        b.record_ms(50);
        b.record_ms(100);

        // Serialize, deserialize, merge — must equal recording all four.
        let a2 = LatencyHistogram::from_bytes(&a.to_bytes()).unwrap();
        let b2 = LatencyHistogram::from_bytes(&b.to_bytes()).unwrap();
        let mut merged = a2;
        merged.merge(&b2);

        let mut direct = LatencyHistogram::new();
        direct.record_ms(1);
        direct.record_ms(2);
        direct.record_ms(50);
        direct.record_ms(100);

        assert_eq!(merged.count(), 4);
        assert_eq!(merged.count(), direct.count());
        assert_eq!(merged.max(), direct.max());
        assert_eq!(merged.p95(), direct.p95());
        assert_eq!(merged.p99(), direct.p99());
    }
}
