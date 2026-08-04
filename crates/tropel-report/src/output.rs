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
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tropel_core::types::{Sample, TagMap};
use tropel_core::Result;

/// Tag forwarding policy for network outputs (prometheus/otlp/statsd/influxdb).
///
/// Bounds label cardinality, which otherwise grows unboundedly with unique
/// tag values (e.g. per-request `url` tags) and can balloon the series count
/// at the backend or be rejected outright. Two levers:
///
/// - `allowlist`: only these tag keys are forwarded. Empty (default) =
///   forward everything.
/// - `max_tags`: hard cap on the number of tag keys per sample. When
///   exceeded, tags are kept deterministically (sorted by key, first `cap`
///   kept) so behavior is stable run-to-run.
#[derive(Debug, Clone, Default)]
pub struct TagPolicy {
    /// Only these tag keys are forwarded. Empty = forward all tags.
    pub allowlist: Vec<String>,
    /// Max tag keys per sample. `None` = no cap.
    pub max_tags: Option<usize>,
}

impl TagPolicy {
    /// Apply the policy to a sample's tags: allowlist first, then the cap.
    pub fn apply(&self, tags: &TagMap) -> TagMap {
        let mut out = TagMap::new();
        if self.allowlist.is_empty() {
            for (k, v) in tags.iter() {
                out.insert(k, v);
            }
        } else {
            for (k, v) in tags.iter() {
                if self.allowlist.iter().any(|a| a == k) {
                    out.insert(k, v);
                }
            }
        }
        if let Some(cap) = self.max_tags {
            if out.len() > cap {
                let mut pairs: Vec<(String, String)> = out
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                pairs.sort_by(|a, b| a.0.cmp(&b.0));
                pairs.truncate(cap);
                out = TagMap::from_pairs(pairs);
            }
        }
        out
    }
}

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
/// prints a k6-style two-line progress block:
/// ```text
///   running (0m02.0s), 2/2 VUs, 120 iters, 240 reqs (120 rps), 2.3% fail
///   [██████████░░░░░░░░░░░░░░░░░░░░]  50%  10.0s/20.0s  p95=452ms  max=890ms  1.2 MB recv
/// ```
///
/// The block overwrites itself in-place using ANSI cursor-up + clear-line
/// (`\x1b[2A\r\x1b[K`), producing a live-updating progress bar — the same
/// shape k6 prints during a run.
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
    /// `total_duration` is the planned wall-clock length of the run
    /// (including grace); when `Some`, the bar fills toward a real 100%.
    /// When `None` (externally-controlled / iteration-limited runs without
    /// a fixed duration) the bar shows elapsed time only.
    ///
    /// Returns a `JoinHandle` that completes when the broadcast sender is
    /// dropped (test end) or the receiver is closed.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        total_duration: Option<Duration>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let start = Instant::now();
            let mut state = LiveState::new(total_duration);
            let mut drawn = false;

            // Redraw on a fixed interval, NOT on recv timeouts: under any
            // sustained sample rate `rx.recv()` resolves instantly, so a
            // `timeout(interval, recv)` loop would never hit its timeout arm
            // and the bar would print exactly once at the end of the run.
            // `select!` over an interval ticker guarantees a redraw every
            // PROGRESS_INTERVAL_SECS regardless of traffic.
            let mut ticker = tokio::time::interval(Duration::from_secs(PROGRESS_INTERVAL_SECS));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await; // consume the immediate first tick

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => state.record(&sample),
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("Streaming output dropped {} samples (consumer lag)", n);
                        }
                    },
                    _ = ticker.tick() => {
                        state.print_progress(&start, drawn);
                        drawn = true;
                    }
                }
            }

            // Final print on exit (always overwrites the last block; the
            // summary renderer adds its own leading newline, so no extra
            // blank line is needed here).
            state.print_progress(&start, drawn);
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
    /// Planned total wall-clock duration (including grace). `None` = no
    /// fixed target (externally-controlled / iteration-budget runs).
    total_duration: Option<Duration>,
    total_reqs: u64,
    total_failed: f64,
    total_data_received: f64,
    total_data_sent: f64,
    /// Completed iterations (from the `iterations` counter).
    total_iters: u64,
    /// Latest VU / VU-max gauges (k6 emits these periodically).
    vus: u32,
    vus_max: u32,
    last_print: Instant,
    rolling_count: u64,
    rolling_max: f64,
    /// Rolling window of `http_req_duration` (μs) for the live p95. A
    /// `VecDeque` so evicting the oldest sample is O(1) — the previous `Vec`
    /// did `remove(0)`, shifting up to 5000 elements per sample on the hot
    /// path (~4 GB/s of memmove at 100k samples/s).
    rolling_p95: VecDeque<u64>,
}

impl LiveState {
    fn new(total_duration: Option<Duration>) -> Self {
        Self {
            total_duration,
            total_reqs: 0,
            total_failed: 0.0,
            total_data_received: 0.0,
            total_data_sent: 0.0,
            total_iters: 0,
            vus: 0,
            vus_max: 0,
            last_print: Instant::now(),
            rolling_count: 0,
            rolling_max: 0.0,
            // Match the window bound exactly so the deque never reallocates
            // as it grows from 1024 up to the 5000-sample cap.
            rolling_p95: VecDeque::with_capacity(5000),
        }
    }

    fn record(&mut self, sample: &Sample) {
        match sample.metric.as_ref() {
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
            "iterations" => {
                self.total_iters += sample.value as u64;
            }
            "vus" => {
                self.vus = sample.value as u32;
            }
            "vus_max" => {
                self.vus_max = sample.value as u32;
            }
            "http_req_duration" => {
                self.rolling_count += 1;
                let val_ms = sample.value / 1000.0; // μs → ms
                if val_ms > self.rolling_max {
                    self.rolling_max = val_ms;
                }
                self.rolling_p95.push_back(sample.value as u64);
                if self.rolling_p95.len() > 5000 {
                    self.rolling_p95.pop_front();
                }
            }
            _ => {}
        }
    }

    fn compute_p95(&self) -> f64 {
        if self.rolling_p95.is_empty() {
            return 0.0;
        }
        let mut sorted: Vec<u64> = self.rolling_p95.iter().copied().collect();
        sorted.sort_unstable();
        let idx = (sorted.len() as f64 * 0.95) as usize;
        sorted.get(idx).copied().unwrap_or(0) as f64 / 1000.0
    }

    /// Render the progress block as two lines (k6-style).
    fn render(&mut self, start: &Instant) -> (String, String) {
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

        // k6-style status line: running time, VUs, iterations, requests.
        let line1 = format!(
            "  running ({:02}m{:02}.{:01}s), {}/{} VUs, {} iters, {} reqs ({} rps), {:.1}% fail",
            mins,
            secs_remainder,
            elapsed.subsec_millis() / 100,
            self.vus,
            self.vus_max,
            self.total_iters,
            self.total_reqs,
            rolling_rps,
            fail_pct,
        );

        // Progress bar: fills toward total_duration (when known).
        const BAR_WIDTH: usize = 32;
        let (filled, pct) = match self.total_duration {
            Some(total) if total > Duration::ZERO => {
                let frac = (elapsed.as_secs_f64() / total.as_secs_f64()).clamp(0.0, 1.0);
                (((frac * BAR_WIDTH as f64).round()) as usize, (frac * 100.0) as u64)
            }
            _ => (0, 0),
        };
        let bar: String = (0..BAR_WIDTH)
            .map(|i| if i < filled { '█' } else { '░' })
            .collect();

        let elapsed_str = format!("{:02}m{:02}.{:01}s", mins, secs_remainder, elapsed.subsec_millis() / 100);
        let line2 = match self.total_duration {
            // Fixed target: bar fills toward 100% with elapsed/total.
            Some(t) if t > Duration::ZERO => {
                let total_str = format!(
                    "{:02}m{:02}.{:01}s",
                    t.as_secs() / 60,
                    t.as_secs() % 60,
                    t.subsec_millis() / 100
                );
                format!(
                    "  [{bar}] {pct:3}%  {elapsed_str}/{total_str}  p95={p95:.0}ms  max={:.0}ms  {:.1} MB recv  {:.1} MB sent",
                    self.rolling_max,
                    self.total_data_received / 1_000_000.0,
                    self.total_data_sent / 1_000_000.0,
                )
            }
            // No fixed duration (externally-controlled / iteration-budget
            // runs): elapsed-only, honest about the missing target.
            _ => format!(
                "  [{bar}] {pct:3}%  elapsed {elapsed_str} (no fixed duration)  p95={p95:.0}ms  max={:.0}ms  {:.1} MB recv  {:.1} MB sent",
                self.rolling_max,
                self.total_data_received / 1_000_000.0,
                self.total_data_sent / 1_000_000.0,
            ),
        };

        (line1, line2)
    }

    fn print_progress(&mut self, start: &Instant, drawn: bool) {
        let (line1, line2) = self.render(start);
        use std::io::Write;
        let mut out = std::io::stdout();
        // Overwrite the two previously-drawn lines (if any) in place.
        if drawn {
            let _ = write!(out, "\x1b[2A");
        }
        let _ = writeln!(out, "\r\x1b[K{line1}");
        let _ = writeln!(out, "\r\x1b[K{line2}");
        let _ = out.flush();
    }
}
