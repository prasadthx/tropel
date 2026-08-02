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

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Paces the start of requests to at most `rate` per second.
#[derive(Debug)]
pub struct RpsLimiter {
    /// Minimum interval between request starts.
    interval: Duration,
    /// When the next request is allowed to start.
    next_slot: Mutex<Instant>,
}

impl RpsLimiter {
    /// Create a limiter for the given rate (requests/second). A rate <= 0,
    /// NaN or infinite yields a zero-interval limiter that never blocks
    /// (`Duration::from_secs_f64` panics on NaN/inf, so guard before use).
    pub fn new(rate: f64) -> Self {
        let interval = if rate.is_finite() && rate > 0.0 {
            Duration::from_secs_f64(1.0 / rate)
        } else {
            Duration::ZERO
        };
        Self {
            interval,
            next_slot: Mutex::new(Instant::now()),
        }
    }

    /// Wait until the next request may start, then reserve its slot.
    ///
    /// Idempotent under contention: each caller reserves one slot by
    /// advancing `next_slot` by `interval` from the max(now, next_slot).
    /// A slow first caller doesn't let a burst of fast callers through.
    pub async fn acquire(&self) {
        if self.interval.is_zero() {
            return;
        }
        let wait = {
            let mut slot = self.next_slot.lock().expect("rps limiter mutex poisoned");
            let now = Instant::now();
            let next = (*slot).max(now);
            *slot = next + self.interval;
            next.saturating_duration_since(now)
        };
        if wait > Duration::ZERO {
            tokio::time::sleep(wait).await;
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
        let start = Instant::now();
        limiter.acquire().await; // immediate (first slot = now)
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
