use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tropel_core::config::ExecutionConfig;
use tropel_core::Result;

/// Controls the lifecycle of VUs during a load test.
pub struct VUScheduler {
    config: ExecutionConfig,
    active_vus: Arc<Mutex<u32>>,
    total_iterations: Arc<Mutex<u64>>,
    stop_signal: Arc<tokio::sync::Notify>,
    /// Level-triggered stop flag — persists across notify_waiters() wake-ups
    /// so VUs never miss the stop signal even if they're between iterations.
    stop_requested: Arc<AtomicBool>,
    /// Token bucket count for arrival-rate mode (atomic, so ticker and VUs can share).
    arrival_tokens: Arc<AtomicU64>,
    /// Notify for waking VUs when a new token is available.
    arrival_notify: Arc<tokio::sync::Notify>,
    /// Dropped iterations counter for arrival-rate mode.
    arrival_dropped: Arc<AtomicU64>,
    /// Count of VUs currently idle (waiting for an arrival token), not executing.
    /// Used by the ticker to decide when to grow the VU pool.
    idle_vus: Arc<AtomicU32>,
}

impl VUScheduler {
    /// Create a new VU scheduler from config.
    pub fn new(config: &ExecutionConfig) -> Self {
        Self {
            config: config.clone(),
            active_vus: Arc::new(Mutex::new(0)),
            total_iterations: Arc::new(Mutex::new(0)),
            stop_signal: Arc::new(tokio::sync::Notify::new()),
            stop_requested: Arc::new(AtomicBool::new(false)),
            arrival_tokens: Arc::new(AtomicU64::new(0)),
            arrival_notify: Arc::new(tokio::sync::Notify::new()),
            arrival_dropped: Arc::new(AtomicU64::new(0)),
            idle_vus: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Get the stop signal (Notify for waking VUs mid-iteration).
    pub fn stop_signal(&self) -> Arc<tokio::sync::Notify> {
        self.stop_signal.clone()
    }

    /// Request a clean stop: sets the level-triggered flag and wakes all waiters.
    pub fn request_stop(&self) {
        self.stop_requested.store(true, Ordering::Release);
        self.stop_signal.notify_waiters();
    }

    /// Check whether stop has been requested (level-triggered — stays true once set).
    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }

    /// Try to consume one arrival-rate token. Returns true if a token was available.
    pub fn try_acquire_arrival_token(&self) -> bool {
        let current = self.arrival_tokens.load(Ordering::Relaxed);
        current > 0
            && self.arrival_tokens.compare_exchange(
                current, current - 1, Ordering::Relaxed, Ordering::Relaxed,
            ).is_ok()
    }

    /// Get the Notify for waking VUs when tokens are added.
    pub fn arrival_notify(&self) -> Arc<tokio::sync::Notify> {
        self.arrival_notify.clone()
    }

    /// Whether this scheduler is in arrival-rate mode.
    pub fn is_arrival_rate(&self) -> bool {
        matches!(self.config, ExecutionConfig::ConstantArrivalRate { .. })
    }

    /// Mark a VU as idle (waiting for an arrival token).
    pub fn mark_idle(&self) {
        self.idle_vus.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark a VU as busy (acquired a token, about to execute).
    pub fn mark_busy(&self) {
        self.idle_vus.fetch_sub(1, Ordering::Relaxed);
    }

    /// Current count of idle VUs (waiting for tokens).
    pub fn idle_vu_count(&self) -> u32 {
        self.idle_vus.load(Ordering::Relaxed)
    }

    /// Get and reset the dropped iterations counter.
    pub fn take_dropped_iterations(&self) -> u64 {
        self.arrival_dropped.swap(0, Ordering::Relaxed)
    }

    /// Get active VU count.
    pub async fn active_vus(&self) -> u32 {
        *self.active_vus.lock().await
    }

    /// Increment active VU count.
    pub async fn add_active_vu(&self, delta: u32) {
        let mut vus = self.active_vus.lock().await;
        *vus += delta;
    }

    /// Decrement active VU count.
    pub async fn remove_active_vu(&self, delta: u32) {
        let mut vus = self.active_vus.lock().await;
        *vus = vus.saturating_sub(delta);
    }

    /// Get total iterations completed.
    pub async fn total_iterations(&self) -> u64 {
        *self.total_iterations.lock().await
    }

    /// Increment iteration count.
    pub async fn increment_iterations(&self) {
        let mut iters = self.total_iterations.lock().await;
        *iters += 1;
    }

    /// Start executing VUs according to the execution config.
    /// Calls the provided `run_vu` function for each VU.
    pub async fn run<F>(&self, run_vu: F) -> Result<()>
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        match &self.config {
            ExecutionConfig::ConstantVus { vus, duration } => {
                let duration = parse_duration(duration)?;
                self.run_constant(*vus, duration, &run_vu).await;
                Ok(())
            }
            ExecutionConfig::RampingVus { stages, start_vus, .. } => {
                self.run_ramping(*start_vus, stages, &run_vu).await;
                Ok(())
            }
            ExecutionConfig::SharedIterations { iterations, .. } => {
                self.run_shared_iterations(*iterations, &run_vu).await;
                Ok(())
            }
            ExecutionConfig::ConstantArrivalRate { rate, duration, pre_alloc_vus, max_vus, .. } => {
                let duration = parse_duration(duration)?;
                self.run_arrival_rate(*rate, *pre_alloc_vus, *max_vus, duration, &run_vu).await;
                Ok(())
            }
        }
    }

    /// Run with a constant number of VUs.
    async fn run_constant<F>(&self, vus: u32, duration: Duration, run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!("Starting constant VUs: {} for {:?}", vus, duration);

        // Spawn VUs (active count incremented by each VU task itself)
        let mut handles = Vec::new();
        for vu_id in 0..vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // Wait for the duration
        time::sleep(duration).await;

        // Signal stop (level-triggered — sets the flag + wakes waiters)
        self.request_stop();

        // Wait for all VUs to finish
        for handle in handles {
            handle.await.ok();
        }

        tracing::info!("Constant VUs finished");
    }

    /// Run with ramping VUs.
    async fn run_ramping<F>(&self, start_vus: u32, stages: &[tropel_core::config::Stage], run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!("Starting ramping VUs: start={}", start_vus);

        let mut current_vus = start_vus;
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

        // Start initial VUs (active count incremented by each VU task itself)
        for vu_id in 0..current_vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // Process each stage
        for stage in stages {
            let stage_duration = parse_duration(&stage.duration).unwrap_or(Duration::from_secs(10));
            let target = stage.target;

            tracing::info!("Ramping stage: {} -> {} over {:?}", current_vus, target, stage_duration);
            let steps = 10.max((target as i64 - current_vus as i64).unsigned_abs());
            let step_delay = stage_duration / steps as u32;

            if target > current_vus {
                // Ramp up (active count incremented by each VU task itself)
                for _ in 0..(target - current_vus) {
                    let vu_id = current_vus;
                    let handle = run_vu(self.shared_clone(), vu_id);
                    handles.push(handle);
                    current_vus += 1;
                    time::sleep(step_delay).await;
                }
            } else {
                // Ramp down: use per-VU cancellation via the decrement + notify pattern
                let to_stop = current_vus - target;
                // Signal all VUs; each VU checks the level-triggered flag
                self.request_stop();
                time::sleep(Duration::from_millis(500)).await; // Brief wait for VUs to notice
                // Decrement active count — VUs that exit will decrement themselves
                self.remove_active_vu(to_stop).await;
                current_vus = target;
            }
        }

        // Signal stop (level-triggered)
        self.request_stop();

        // Wait for all VUs
        for handle in handles {
            handle.await.ok();
        }

        tracing::info!("Ramping VUs finished");
    }

    /// Run with shared iterations across all VUs.
    async fn run_shared_iterations<F>(&self, total_iterations: u64, run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        // For simplicity, use a fixed set of VUs and shared iteration counter
        let vus = match &self.config {
            ExecutionConfig::SharedIterations { vus, .. } => *vus,
            _ => 1,
        };

        tracing::info!("Starting shared iterations: {} across {} VUs", total_iterations, vus);

        let mut handles = Vec::new();
        for vu_id in 0..vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // Wait for all VUs to complete (they check shared iterations)
        for handle in handles {
            handle.await.ok();
        }

        tracing::info!("Shared iterations finished");
    }

    /// Run with constant arrival rate.
    ///
    /// Uses a time-based token bucket (no 1ms timer floor — resilient at high rates)
    /// and a dynamically growing VU pool (`pre_alloc_vus → max_vus`). VUs are
    /// spawned on demand when the current pool is saturated.
    async fn run_arrival_rate<F>(&self, rate: f64, pre_alloc: u32, max_vus: u32, duration: Duration, run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!(
            "Starting constant arrival rate: {}/s for {:?} (pre_alloc={}, max_vus={})",
            rate, duration, pre_alloc, max_vus
        );

        // Pre-spawn initial VU pool
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        for vu_id in 0..pre_alloc.max(1) {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }
        let mut current_vus = pre_alloc.max(1);
        let max_tokens = max_vus as u64;
        let dropped = self.arrival_dropped.clone();

        // Time-based token bucket: compute tokens from wall-clock elapsed time.
        // This avoids the `sleep(1/rate)` timer-floor bug: even at 10k/s the
        // bucket still refills accurately because we measure elapsed, not ticks.
        let start = time::Instant::now();
        let mut last_target: u64 = 0;

        while start.elapsed() < duration {
            let elapsed_secs = start.elapsed().as_secs_f64();
            let target_tokens = (elapsed_secs * rate) as u64;

            if target_tokens > last_target {
                let to_add = target_tokens - last_target;
                let current = self.arrival_tokens.load(Ordering::Relaxed);
                let capacity = max_tokens.saturating_sub(current);
                let actual_add = to_add.min(capacity);

                if actual_add > 0 {
                    self.arrival_tokens.fetch_add(actual_add, Ordering::Relaxed);
                    // Wake ALL waiters — multiple VUs may be waiting
                    self.arrival_notify.notify_waiters();

                    // Grow the VU pool if the current pool is saturated
                    // (no idle VUs) and we haven't reached max_vus yet.
                    let idle = self.idle_vus.load(Ordering::Relaxed);
                    if idle == 0 && current_vus < max_vus {
                        let grow_cap = (max_vus - current_vus) as u64;
                        let grow_by = to_add.min(grow_cap) as u32;
                        if grow_by > 0 {
                            for vu_id in current_vus..current_vus + grow_by {
                                let handle = run_vu(self.shared_clone(), vu_id);
                                handles.push(handle);
                            }
                            tracing::debug!(
                                "Arrival-rate: VU pool {} → {} (rate={}/s)",
                                current_vus, current_vus + grow_by, rate
                            );
                            current_vus += grow_by;
                        }
                    }
                }

                // Dropped iterations: tokens we couldn't add because the bucket
                // was full (all max_tokens preoccupied — no idle VU to consume).
                // This means VUs can't keep up with the target rate.
                let overflow = to_add.saturating_sub(capacity);
                if overflow > 0 {
                    dropped.fetch_add(overflow, Ordering::Relaxed);
                }
            }

            last_target = target_tokens;

            // 1ms tick — NOT 1/rate. This avoids the tokio ~1ms timer floor
            // that silently under-delivered at high rates. The token bucket
            // accumulates multiple tokens per tick; accuracy is governed by
            // wall-clock elapsed, not tick resolution.
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        self.request_stop();

        for handle in handles {
            handle.await.ok();
        }

        let dropped_total = self.arrival_dropped.load(Ordering::Relaxed);
        tracing::info!("Constant arrival rate finished (dropped: {})", dropped_total);
    }

    /// Create a shared clone of this scheduler for passing to VU tasks.
    fn shared_clone(&self) -> Arc<VUScheduler> {
        Arc::new(VUScheduler {
            config: self.config.clone(),
            active_vus: self.active_vus.clone(),
            total_iterations: self.total_iterations.clone(),
            stop_signal: self.stop_signal.clone(),
            stop_requested: self.stop_requested.clone(),
            arrival_tokens: self.arrival_tokens.clone(),
            arrival_notify: self.arrival_notify.clone(),
            arrival_dropped: self.arrival_dropped.clone(),
            idle_vus: self.idle_vus.clone(),
        })
    }
}

fn parse_duration(s: &str    ) -> Result<Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        let v: u64 = num.parse().map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(v))
    } else if let Some(num) = s.strip_suffix('s') {
        let v: f64 = num.parse().map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v))
    } else if let Some(num) = s.strip_suffix('m') {
        let v: f64 = num.parse().map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v * 60.0))
    } else if let Some(num) = s.strip_suffix('h') {
        let v: f64 = num.parse().map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v * 3600.0))
    } else {
        let v: f64 = s.parse().map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v))
    }
}
