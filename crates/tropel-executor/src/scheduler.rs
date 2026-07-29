use std::sync::Arc;
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
}

impl VUScheduler {
    /// Create a new VU scheduler from config.
    pub fn new(config: &ExecutionConfig) -> Self {
        Self {
            config: config.clone(),
            active_vus: Arc::new(Mutex::new(0)),
            total_iterations: Arc::new(Mutex::new(0)),
            stop_signal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Get the stop signal.
    pub fn stop_signal(&self) -> Arc<tokio::sync::Notify> {
        self.stop_signal.clone()
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

        // Signal stop
        self.stop_signal.notify_waiters();

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
                // Signal all VUs; each VU checks remaining capacity
                self.stop_signal.notify_waiters();
                time::sleep(Duration::from_millis(500)).await; // Brief wait for VUs to notice
                // Decrement active count — VUs that exit will decrement themselves
                self.remove_active_vu(to_stop).await;
                current_vus = target;
            }
        }

        // Signal stop
        self.stop_signal.notify_waiters();

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
    async fn run_arrival_rate<F>(&self, rate: f64, _pre_alloc: u32, max_vus: u32, duration: Duration, run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        let interval = Duration::from_secs_f64(1.0 / rate);
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let mut vu_counter = 0u32;
        let start = time::Instant::now();

        tracing::info!("Starting constant arrival rate: {}/s for {:?}", rate, duration);

        while start.elapsed() < duration {
            if vu_counter < max_vus {
                let handle = run_vu(self.shared_clone(), vu_counter);
                handles.push(handle);
                vu_counter += 1;
            }

            time::sleep(interval).await;
        }

        self.stop_signal.notify_waiters();

        for handle in handles {
            handle.await.ok();
        }

        tracing::info!("Constant arrival rate finished");
    }

    /// Create a shared clone of this scheduler for passing to VU tasks.
    fn shared_clone(&self) -> Arc<VUScheduler> {
        Arc::new(VUScheduler {
            config: self.config.clone(),
            active_vus: self.active_vus.clone(),
            total_iterations: self.total_iterations.clone(),
            stop_signal: self.stop_signal.clone(),
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
