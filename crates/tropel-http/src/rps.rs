//! Global request-rate limiter (k6 `rps`).
//!
//! A simple token-bucket-free pacing gate: requests are spaced at least
//! `1/rate` seconds apart using a shared "next allowed start" timestamp.
//! The limiter is shared across all VUs of a run (each per-VU `HttpClient`
//! holds an `Arc<RpsLimiter>`), so the cap is truly global — exactly k6's
//! `options.rps` semantics.
//!
//! The wait happens BEFORE the request timer starts, so `rps` pacing never
//! inflates `http_req_duration` / TTFB numbers.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Paces the start of requests to at most `rate` per second.
///
/// Lock-free hot path: `next_slot_ns` is a relative atomic timestamp (nanos
/// since `base`), advanced with a compare-exchange loop. A global
/// `Mutex<Instant>` serialised every request start across all VUs; the CAS
/// lets contending callers retry without blocking each other — the k6 `rps`
/// gate is on EVERY request, so this is a hot path.
#[derive(Debug)]
pub struct RpsLimiter {
    /// Minimum interval between request starts.
    interval: Duration,
    /// Monotonic epoch captured at construction; `next_slot_ns` is relative
    /// to this so the atomic can't wrap from a stale absolute timestamp.
    base: Instant,
    /// When the next request is allowed to start, in ns since `base`.
    next_slot_ns: AtomicU64,
}

/// Longest interval `RpsLimiter` will ever construct: u64::MAX ns ≈ 584
/// years. A rate slower than one request per 584 years is clamped to this so
/// `Duration::from_secs_f64` cannot overflow (it panics past u64::MAX
/// seconds — e.g. `rps: 1e-30` → 1e30 s) and `acquire`'s `as_nanos() as u64`
/// cannot truncate. The limiter then blocks ~forever, which is exactly the
/// declared rate.
const MAX_INTERVAL: Duration = Duration::from_secs(u64::MAX / 1_000_000_000);

impl RpsLimiter {
    /// Create a limiter for the given rate (requests/second). A rate <= 0,
    /// NaN or infinite yields a zero-interval limiter that never blocks; a
    /// rate so slow its interval would overflow `Duration` (e.g. `1e-30` rps
    /// → one request per 1e30 s) is clamped to [`MAX_INTERVAL`] — never a
    /// panic (`Duration::from_secs_f64` panics on NaN/inf AND overflow, so
    /// guard before use).
    pub fn new(rate: f64) -> Self {
        let interval = if rate.is_finite() && rate > 0.0 {
            let secs = 1.0 / rate;
            // `Duration::from_secs_f64` panics on overflow (not just
            // NaN/inf) — guard before use, per this fn's own doc.
            if secs > MAX_INTERVAL.as_secs_f64() {
                MAX_INTERVAL
            } else {
                Duration::from_secs_f64(secs)
            }
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            base: Instant::now(),
            next_slot_ns: AtomicU64::new(0),
        }
    }

    /// Wait until the next request may start, then reserve its slot.
    ///
    /// Idempotent under contention: each caller reserves one slot by
    /// advancing `next_slot_ns` by `interval` from the max(now, next_slot) —
    /// the EXACT pacing math of the old Mutex version:
    ///   claim      = max(now, next_slot)   // when this request may start
    ///   next_slot  = claim + interval       // when the NEXT one may start
    ///   wait       = claim - now            // 0 when the slot is in the past
    /// `next_slot_ns = 0` at construction means "the slot is at construction
    /// time", so the FIRST acquire is immediate (claim = now) — matching the
    /// old code that initialized `next_slot` to `Instant::now()`. The
    /// compare-exchange loop retries when another caller claimed the slot
    /// concurrently — no mutex, no blocking.
    pub async fn acquire(&self) {
        if self.interval.is_zero() {
            return;
        }
        let interval_ns = self.interval.as_nanos() as u64;
        let wait_ns = loop {
            let now_ns = self.base.elapsed().as_nanos() as u64;
            let slot = self.next_slot_ns.load(Ordering::Relaxed);
            let claim = slot.max(now_ns);
            let next_slot = claim.saturating_add(interval_ns);
            if self
                .next_slot_ns
                .compare_exchange_weak(slot, next_slot, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break claim.saturating_sub(now_ns);
            }
        };
        if wait_ns > 0 {
            tokio::time::sleep(Duration::from_nanos(wait_ns)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn paces_requests_at_rate() {
        // 50 rps → 20ms between starts.
        let limiter = RpsLimiter::new(50.0);
        // THE FIRST REQUEST MUST BE IMMEDIATE — the slot starts at
        // construction time, so the first acquire never waits a full
        // interval (regression: an atomic-CAS version once delayed every
        // request by one interval, and this test's total-window assertion
        // could not tell 40ms from 60ms). Pin the invariant explicitly.
        let start = Instant::now();
        limiter.acquire().await;
        assert!(
            start.elapsed() < Duration::from_millis(5),
            "first acquire must be immediate, got {:?}",
            start.elapsed()
        );
        limiter.acquire().await;
        limiter.acquire().await;
        let elapsed = start.elapsed();
        // Three acquires = 2 intervals ≈ 40ms. Allow tolerance for timer jitter.
        assert!(
            elapsed >= Duration::from_millis(35),
            "expected ~40ms pacing, got {elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(200),
            "pacing should not over-sleep, got {elapsed:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn zero_rate_never_blocks() {
        let limiter = RpsLimiter::new(0.0);
        let start = Instant::now();
        limiter.acquire().await;
        assert!(start.elapsed() < Duration::from_millis(10));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn extreme_rates_never_panic() {
        // Regression (backlog line 165): `rps: 1e-30` → 1/rate = 1e30 s,
        // which `Duration::from_secs_f64` cannot represent — it panics on
        // overflow. The guard clamps to MAX_INTERVAL (~584 years) so the
        // limiter BLOCKS instead of panicking, which is the correct meaning
        // of "at most one request per 1e30 s". (The first acquire is always
        // immediate — the slot starts at construction — so the block shows on
        // the SECOND acquire.)
        for rate in [1e-30f64, 1e-20, 1e-19, 1e-300] {
            let limiter = RpsLimiter::new(rate);
            limiter.acquire().await; // first acquire is always immediate
                                     // The clamped wait is ~584 years, so 10 ms is plenty.
            let blocked = tokio::time::timeout(Duration::from_millis(10), limiter.acquire())
                .await
                .is_err();
            assert!(
                blocked,
                "a {rate} rps limiter must block (clamped interval), not fire"
            );
        }
        // Non-positive / NaN / infinite rates stay zero-interval (never
        // block) — and must never panic either.
        for rate in [
            0.0f64,
            -1.0,
            -1e30,
            f64::NAN,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            let limiter = RpsLimiter::new(rate);
            let start = Instant::now();
            limiter.acquire().await;
            limiter.acquire().await;
            assert!(start.elapsed() < Duration::from_millis(10));
        }
        // f64::MAX rps → ~0 s interval → immediate, no panic.
        let max_rate = RpsLimiter::new(f64::MAX);
        let start = Instant::now();
        max_rate.acquire().await;
        max_rate.acquire().await;
        assert!(start.elapsed() < Duration::from_millis(10));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn no_burst_after_idle() {
        // After an idle gap, the first acquire is immediate but the second is
        // still paced — a caller can't drain a backlog of "stored" tokens.
        let limiter = RpsLimiter::new(100.0); // 10ms
        limiter.acquire().await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let start = Instant::now();
        limiter.acquire().await; // immediate
        limiter.acquire().await; // paced 10ms
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(8),
            "second acquire after idle should still be paced, got {elapsed:?}"
        );
    }
}
