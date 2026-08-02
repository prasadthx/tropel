//! # Streaming outputs
//!
//! Outputs receive individual samples during a load test run (not just
//! the end-of-test summary). This enables live progress display, streaming
//! JSON/CSV files, and real-time dashboards.
//!
//! Outputs run as consumer tasks that receive samples from a `broadcast`
//! channel that the `MetricsCollector` forwards samples to (best-effort,
//! non-blocking). Each output subscribes independently and may lag behind
//! without affecting the VU hot path.

use async_trait::async_trait;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tropel_core::types::Sample;
use tropel_core::Result;

/// The number of seconds between each live progress update.
const PROGRESS_INTERVAL_SECS: u64 = 2;

/// A streaming output that consumes individual `Sample`s during a load test.
///
/// Unlike `Reporter` (which receives a final aggregated `MetricsResult`),
/// an `Output` receives raw samples during the run, enabling live progress
/// bars, streaming JSON/CSV files, and real-time metric forwarding.
///
/// Each `Output` runs as a dedicated consumer task subscribed to the
/// metrics collector's broadcast sample stream.
#[async_trait]
pub trait Output: Send + Sync {
    fn name(&self) -> &str;

    /// Process a batch of individual samples during the test run.
    async fn sample(&self, samples: &[Sample]) -> Result<()>;

    /// Finalize the output — called after the test ends.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

/// A live progress display printed to stdout during the test run.
///
/// Spawned as a background task that receives samples and periodically
/// prints a compact one-line summary:
/// ```text
///   running (0m02.0s), ? VUs, 120 reqs (60 rps), 1.2 MB recv, p95=452ms, max=890ms, 2.3% fail
/// ```
///
/// The line overwrites itself in-place using `\r` (carriage return) and
/// ANSI clear-line, producing a live-updating status bar.
pub struct StreamingStdoutOutput;

impl Default for StreamingStdoutOutput {
    fn default() -> Self {
        Self
    }
}

impl StreamingStdoutOutput {
    /// Create a new streaming stdout output instance.
    pub fn new() -> Self {
        Self
    }

    /// Spawn a consumer task that receives samples from the broadcast
    /// receiver and prints live progress every `PROGRESS_INTERVAL_SECS`.
    ///
    /// Returns a `JoinHandle` that completes when the broadcast sender is
    /// dropped (test end) or the receiver is closed.
    pub fn spawn(mut rx: broadcast::Receiver<Sample>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let start = Instant::now();
            let mut state = LiveState::new();

            loop {
                let result =
                    tokio::time::timeout(Duration::from_secs(PROGRESS_INTERVAL_SECS), rx.recv())
                        .await;

                match result {
                    Ok(Ok(sample)) => {
                        state.record(&sample);
                    }
                    Ok(Err(broadcast::error::RecvError::Closed)) => break,
                    Ok(Err(broadcast::error::RecvError::Lagged(n))) => {
                        tracing::trace!("Streaming output dropped {} samples (consumer lag)", n);
                    }
                    Err(_elapsed) => {
                        state.print_progress(&start);
                    }
                }
            }

            // Final print on exit
            state.print_progress(&start);
            println!();
        })
    }
}

#[async_trait]
impl Output for StreamingStdoutOutput {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn sample(&self, _samples: &[Sample]) -> Result<()> {
        // The trait is implemented for wire-compatibility but the actual
        // consumption happens via `spawn()` which runs its own consumer task.
        // This is a no-op implementation; `spawn()` handles all processing.
        Ok(())
    }
}

/// Live state accumulated during the test for progress display.
struct LiveState {
    total_reqs: u64,
    total_failed: f64,
    total_data_received: f64,
    total_data_sent: f64,
    last_print: Instant,
    rolling_count: u64,
    rolling_max: f64,
    rolling_p95: Vec<u64>,
}

impl LiveState {
    fn new() -> Self {
        Self {
            total_reqs: 0,
            total_failed: 0.0,
            total_data_received: 0.0,
            total_data_sent: 0.0,
            last_print: Instant::now(),
            rolling_count: 0,
            rolling_max: 0.0,
            rolling_p95: Vec::with_capacity(1024),
        }
    }

    fn record(&mut self, sample: &Sample) {
        match sample.metric.as_str() {
            "http_reqs" => {
                self.total_reqs += 1;
            }
            "http_req_failed" if sample.value > 0.5 => {
                self.total_failed += 1.0;
            }
            "data_received" => {
                self.total_data_received += sample.value;
            }
            "data_sent" => {
                self.total_data_sent += sample.value;
            }
            "http_req_duration" => {
                self.rolling_count += 1;
                let val_ms = sample.value / 1000.0; // μs → ms
                if val_ms > self.rolling_max {
                    self.rolling_max = val_ms;
                }
                self.rolling_p95.push(sample.value as u64);
                if self.rolling_p95.len() > 5000 {
                    self.rolling_p95.remove(0);
                }
            }
            _ => {}
        }
    }

    fn compute_p95(&self) -> f64 {
        if self.rolling_p95.is_empty() {
            return 0.0;
        }
        let mut sorted = self.rolling_p95.clone();
        sorted.sort_unstable();
        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted.get(idx).copied().unwrap_or(0) as f64 / 1000.0
    }

    fn print_progress(&mut self, start: &Instant) {
        let elapsed = start.elapsed();
        let secs = elapsed.as_secs();
        let mins = secs / 60;
        let secs_remainder = secs % 60;

        let p95 = self.compute_p95();

        let fail_pct = if self.total_reqs > 0 {
            (self.total_failed / self.total_reqs as f64) * 100.0
        } else {
            0.0
        };

        let rolling_rps = {
            let since_last = self.last_print.elapsed().as_secs_f64().max(0.1);
            let rps = (self.rolling_count as f64 / since_last).round();
            self.rolling_count = 0;
            self.last_print = Instant::now();
            rps
        };

        print!(
            "\r\x1b[K  running ({:02}m{:02}.{:01}s), ? VUs, {} reqs ({} rps), {:.1} MB recv, p95={:.0}ms, max={:.0}ms, {:.1}% fail",
            mins,
            secs_remainder,
            (elapsed.subsec_millis() / 100),
            self.total_reqs,
            rolling_rps,
            self.total_data_received / 1_000_000.0,
            p95,
            self.rolling_max,
            fail_pct,
        );

        use std::io::Write;
        let _ = std::io::stdout().flush();
    }
}
