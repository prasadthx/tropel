use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tropel_core::config::ExecutionConfig;
use tropel_core::Result;

/// Hard bound on the trailing VU-handle join. After `grace` expires the
/// scheduler force-stops; if a VU still ignores that (e.g. a runaway JS
/// eval that never trips the interrupt), we abandon it after this bound
/// rather than hang the run forever.
const HANDLE_JOIN_BOUND: Duration = Duration::from_secs(30);

/// Controls the lifecycle of VUs during a load test.
pub struct VUScheduler {
    config: ExecutionConfig,
    /// Lock-free active-VU counter. Atomic so sync JS bridge closures
    /// (inside ctx.with) can read `exec.instance.vusActive` without awaiting
    /// an async mutex — the tokio Mutex made that impossible.
    active_vus: Arc<AtomicU32>,
    /// Lock-free total-iteration counter, shared the same way for
    /// `exec.instance.iterationsCompleted` (a GLOBAL total across all VUs).
    total_iterations: Arc<AtomicU64>,
    stop_signal: Arc<tokio::sync::Notify>,
    /// Level-triggered stop flag — VUs check this between iterations and exit
    /// gracefully (finish current iteration first).
    stop_requested: Arc<AtomicBool>,
    /// Level-triggered force-stop flag — VUs check this during iterations
    /// (e.g., in select! branches) for hard abort after grace period expires.
    force_stop_requested: Arc<AtomicBool>,
    /// Token bucket count for arrival-rate mode (atomic, so ticker and VUs can share).
    arrival_tokens: Arc<AtomicU64>,
    /// Notify for waking VUs when a new token is available.
    arrival_notify: Arc<tokio::sync::Notify>,
    /// Dropped iterations counter for arrival-rate mode.
    arrival_dropped: Arc<AtomicU64>,
    /// Count of VUs currently idle (waiting for an arrival token), not executing.
    /// Used by the ticker to decide when to grow the VU pool.
    idle_vus: Arc<AtomicU32>,
    /// Target VU count for ramp-down — VUs compare `active_vus > ramp_down_target`
    /// and self-select to exit when the pool is above the target.
    ramp_down_target: Arc<AtomicU32>,
    /// Surplus slots remaining for ramp-down. Set to `current_vus - target` when
    /// ramp-down begins; each exiting VU atomically claims one slot. This bounds
    /// the total number of exits to exactly the delta, eliminating the overshoot
    /// race where every VU reads the same active count and all exit.
    ramp_down_remaining: Arc<AtomicU32>,
    /// Desired VU count for the externally-controlled executor, settable at
    /// runtime via the control API. The control loop scales the pool toward
    /// this target (clamped to `control_max_vus`).
    control_target_vus: Arc<AtomicU32>,
    /// Cap on the externally-controlled VU pool.
    control_max_vus: Arc<AtomicU32>,
    /// Wakes the externally-controlled control loop when the target changes.
    control_notify: Arc<tokio::sync::Notify>,
}

impl VUScheduler {
    /// Create a new VU scheduler from config.
    pub fn new(config: &ExecutionConfig) -> Self {
        Self {
            config: config.clone(),
            active_vus: Arc::new(AtomicU32::new(0)),
            total_iterations: Arc::new(AtomicU64::new(0)),
            stop_signal: Arc::new(tokio::sync::Notify::new()),
            stop_requested: Arc::new(AtomicBool::new(false)),
            force_stop_requested: Arc::new(AtomicBool::new(false)),
            arrival_tokens: Arc::new(AtomicU64::new(0)),
            arrival_notify: Arc::new(tokio::sync::Notify::new()),
            arrival_dropped: Arc::new(AtomicU64::new(0)),
            idle_vus: Arc::new(AtomicU32::new(0)),
            ramp_down_target: Arc::new(AtomicU32::new(u32::MAX)),
            ramp_down_remaining: Arc::new(AtomicU32::new(0)),
            control_target_vus: Arc::new(AtomicU32::new(0)),
            control_max_vus: Arc::new(AtomicU32::new(0)),
            control_notify: Arc::new(tokio::sync::Notify::new()),
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
    /// VUs check this between iterations and stop gracefully after finishing the
    /// current iteration.
    pub fn is_stop_requested(&self) -> bool {
        self.stop_requested.load(Ordering::Acquire)
    }

    /// Check whether a force stop has been requested.
    /// VUs check this as a hard-abort signal (e.g., in select! branches)
    /// when the graceful stop deadline has expired.
    pub fn is_force_stop_requested(&self) -> bool {
        self.force_stop_requested.load(Ordering::Acquire)
    }

    /// Request a hard stop — sets the force-stop flag and wakes all waiters.
    /// This is the final deadline expiration: VUs should exit as soon as
    /// possible, potentially mid-iteration.
    pub fn request_force_stop(&self) {
        self.force_stop_requested.store(true, Ordering::Release);
        self.stop_requested.store(true, Ordering::Release);
        self.stop_signal.notify_waiters();
    }

    /// Set the ramp-down target VU count and the number of surplus slots.
    /// `current_vus` is the scheduler's tracked pool size at this moment, so
    /// exactly `current_vus - target` VUs may exit during this ramp-down.
    /// No wake is sent — VUs claim a slot naturally at their next iteration
    /// start via the `try_claim_ramp_down` check in the VU loop.
    pub fn set_ramp_down_target(&self, target: u32, current_vus: u32) {
        self.ramp_down_target.store(target, Ordering::Release);
        self.ramp_down_remaining
            .store(current_vus.saturating_sub(target), Ordering::Release);
    }

    /// Reset ramp-down state back to "not ramping down".
    /// Called after a ramp-down stage drains fully (all surplus VUs exited)
    /// so a later stage's target/remaining can't spuriously claim. When a
    /// ramp-down TIMES OUT with stragglers still mid-iteration, the caller
    /// deliberately does NOT clear, so those VUs can still claim and exit.
    pub fn clear_ramp_down(&self) {
        self.ramp_down_target.store(u32::MAX, Ordering::Release);
        self.ramp_down_remaining.store(0, Ordering::Release);
    }

    /// Try to atomically claim one of the surplus ramp-down slots.
    /// Returns true if THIS VU should exit.
    ///
    /// The surplus counter was set to `current_vus - target` when ramp-down
    /// began, so at most that many VUs can ever claim — this kills the
    /// overshoot race where every VU reads the same `active_vus` snapshot
    /// (all see `active > target`) and all exit below the target. The
    /// `my_active <= target` guard additionally prevents over-exiting below
    /// the target if some VUs already died for other reasons.
    pub async fn try_claim_ramp_down(&self, my_active_vus: u32) -> bool {
        let target = self.ramp_down_target.load(Ordering::Acquire);
        if target == u32::MAX {
            return false;
        }
        if my_active_vus <= target {
            return false;
        }
        self.ramp_down_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |r| {
                if r > 0 {
                    Some(r - 1)
                } else {
                    None
                }
            })
            .is_ok()
    }

    /// Try to consume one arrival-rate token. Returns true if a token was available.
    pub fn try_acquire_arrival_token(&self) -> bool {
        let current = self.arrival_tokens.load(Ordering::Relaxed);
        current > 0
            && self
                .arrival_tokens
                .compare_exchange(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
    }

    /// Get the Notify for waking VUs when tokens are added.
    pub fn arrival_notify(&self) -> Arc<tokio::sync::Notify> {
        self.arrival_notify.clone()
    }

    /// Whether this scheduler is in arrival-rate mode.
    pub fn is_arrival_rate(&self) -> bool {
        matches!(self.config, ExecutionConfig::ConstantArrivalRate { .. })
            || matches!(self.config, ExecutionConfig::RampingArrivalRate { .. })
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

    /// Set the externally-controlled VU target and cap from the control API.
    /// Clamps `vus` to `[0, max]` and wakes the control loop.
    pub fn set_control_target(&self, vus: u32, max_vus: u32) {
        self.control_max_vus.store(max_vus, Ordering::Release);
        self.control_target_vus
            .store(vus.min(max_vus), Ordering::Release);
        self.control_notify.notify_waiters();
    }

    /// Current externally-controlled VU target (as last set by the API).
    pub fn control_target(&self) -> u32 {
        self.control_target_vus.load(Ordering::Acquire)
    }

    /// Current externally-controlled VU cap.
    pub fn control_max(&self) -> u32 {
        self.control_max_vus.load(Ordering::Acquire)
    }

    /// Notify handle for the externally-controlled control loop.
    pub fn control_notify(&self) -> Arc<tokio::sync::Notify> {
        self.control_notify.clone()
    }

    /// Get and reset the dropped iterations counter.
    pub fn take_dropped_iterations(&self) -> u64 {
        self.arrival_dropped.swap(0, Ordering::Relaxed)
    }

    /// Get active VU count.
    pub async fn active_vus(&self) -> u32 {
        self.active_vus.load(Ordering::Acquire)
    }

    /// Increment active VU count.
    pub async fn add_active_vu(&self, delta: u32) {
        self.active_vus.fetch_add(delta, Ordering::AcqRel);
    }

    /// Decrement active VU count.
    pub async fn remove_active_vu(&self, delta: u32) {
        self.active_vus
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(delta))
            })
            .ok();
    }

    /// Get total iterations completed.
    pub async fn total_iterations(&self) -> u64 {
        self.total_iterations.load(Ordering::Acquire)
    }

    /// Increment iteration count.
    pub async fn increment_iterations(&self) {
        self.total_iterations.fetch_add(1, Ordering::AcqRel);
    }

    /// Shared handle to the active-VU counter — handed to a VU's PmState so
    /// the sync `exec.instance.vusActive` bridge can read it live.
    pub fn active_vus_handle(&self) -> Arc<AtomicU32> {
        self.active_vus.clone()
    }

    /// Shared handle to the GLOBAL total-iteration counter — handed to a VU's
    /// PmState so `exec.instance.iterationsCompleted` reflects all VUs, not
    /// just this one.
    pub fn total_iterations_handle(&self) -> Arc<AtomicU64> {
        self.total_iterations.clone()
    }

    /// Start executing VUs according to the execution config.
    /// Calls the provided `run_vu` function for each VU.
    pub async fn run<F>(&self, run_vu: F) -> Result<()>
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        match &self.config {
            ExecutionConfig::ConstantVus {
                vus,
                duration,
                graceful_stop,
                ..
            } => {
                let duration = parse_duration(duration)?;
                let grace = graceful_stop_duration(graceful_stop);
                self.run_constant(*vus, duration, grace, &run_vu).await;
                Ok(())
            }
            ExecutionConfig::RampingVus {
                stages,
                start_vus,
                graceful_ramp_down,
                graceful_stop,
                ..
            } => {
                let grace_rd = graceful_stop_duration(graceful_ramp_down);
                let grace = graceful_stop_duration(graceful_stop);
                self.run_ramping(*start_vus, stages, grace_rd, grace, &run_vu)
                    .await;
                Ok(())
            }
            ExecutionConfig::SharedIterations {
                iterations,
                max_duration,
                graceful_stop,
                ..
            } => {
                // Default maxDuration to 10 minutes (matching k6 behavior)
                let max_dur = max_duration
                    .as_ref()
                    .and_then(|d| parse_duration(d).ok())
                    .or(Some(Duration::from_secs(600)));
                let grace = graceful_stop_duration(graceful_stop);
                // Duration::ZERO here — think_time/pacing is handled in the VU loop in engine.rs
                self.run_shared_iterations(*iterations, max_dur, grace, &run_vu)
                    .await;
                Ok(())
            }
            ExecutionConfig::ConstantArrivalRate {
                rate,
                duration,
                pre_alloc_vus,
                max_vus,
                graceful_stop,
                ..
            } => {
                let duration = parse_duration(duration)?;
                let grace = graceful_stop_duration(graceful_stop);
                // Duration::ZERO here — think_time/pacing is handled in the VU loop in engine.rs
                self.run_arrival_rate(*rate, *pre_alloc_vus, *max_vus, duration, grace, &run_vu)
                    .await;
                Ok(())
            }
            ExecutionConfig::PerVUIterations {
                vus,
                iterations,
                max_duration,
                graceful_stop,
                ..
            } => {
                // Default maxDuration to 10 minutes (matching k6 behavior)
                let max_dur = max_duration
                    .as_ref()
                    .and_then(|d| parse_duration(d).ok())
                    .or(Some(Duration::from_secs(600)));
                let grace = graceful_stop_duration(graceful_stop);
                // Duration::ZERO here — think_time/pacing is handled in the VU loop in engine.rs
                self.run_per_vu_iterations(*vus, *iterations, max_dur, grace, &run_vu)
                    .await;
                Ok(())
            }
            ExecutionConfig::RampingArrivalRate {
                start_rate,
                stages,
                pre_alloc_vus,
                max_vus,
                graceful_stop,
                ..
            } => {
                let grace = graceful_stop_duration(graceful_stop);
                self.run_ramping_arrival_rate(
                    *start_rate,
                    stages,
                    *pre_alloc_vus,
                    *max_vus,
                    grace,
                    &run_vu,
                )
                .await;
                Ok(())
            }
            ExecutionConfig::ExternallyControlled {
                vus,
                max_vus,
                duration,
                graceful_stop,
                ..
            } => {
                let duration = duration
                    .as_ref()
                    .and_then(|d| parse_duration(d).ok());
                let grace = graceful_stop_duration(graceful_stop);
                self.run_externally_controlled(*vus, *max_vus, duration, grace, &run_vu)
                    .await;
                Ok(())
            }
        }
    }

    /// Run with a constant number of VUs.
    async fn run_constant<F>(&self, vus: u32, duration: Duration, grace: Duration, run_vu: &F)
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!(
            "Starting constant VUs: {} for {:?} (graceful_stop: {:?})",
            vus,
            duration,
            grace
        );

        // Spawn VUs (active count incremented by each VU task itself)
        let mut handles = Vec::new();
        for vu_id in 0..vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // Wait for the test duration
        time::sleep(duration).await;

        // Signal soft stop — VUs finish their current iteration
        self.request_stop();

        // Wait for active VUs to drain within the graceful stop window
        self.wait_for_drain(grace).await;

        // Wait for all JoinHandles (VUs that exited should be done).
        // Bounded: a VU ignoring force_stop and never tripping the JS
        // interrupt cannot hang the run forever (P2 · maxDuration trailing
        // join untimed).
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND)
            .await;

        tracing::info!("Constant VUs finished");
    }

    /// Run with ramping VUs.
    async fn run_ramping<F>(
        &self,
        start_vus: u32,
        stages: &[tropel_core::config::Stage],
        grace_rd: Duration,
        grace: Duration,
        run_vu: &F,
    ) where
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

        // Process each stage — VU count is linearly interpolated across the
        // FULL stage duration (k6 semantics): a ramp-up spreads new VUs over
        // `stage.duration`, a constant stage HOLDS for its duration, and a
        // ramp-down gradually lowers the target so VUs exit over the duration
        // instead of as a cliff.
        for stage in stages {
            let stage_duration = parse_duration(&stage.duration).unwrap_or(Duration::from_secs(10));
            let target = stage.target;

            tracing::info!(
                "Ramping stage: {} -> {} over {:?}",
                current_vus,
                target,
                stage_duration
            );

            if target > current_vus {
                // ── Linear ramp-up: spawn one VU every (duration / delta) ──
                // No clear_ramp_down() here: if the previous ramp-down timed
                // out with grace-expired stragglers still mid-iteration, they
                // must still be able to claim a surplus slot and exit during
                // this ramp-up. Stale state from a fully-drained ramp-down is
                // harmless (remaining == 0 blocks claims; see clear below).
                let delta = target - current_vus;
                let step_delay = stage_duration / delta as u32;
                for _ in 0..delta {
                    let vu_id = current_vus;
                    let handle = run_vu(self.shared_clone(), vu_id);
                    handles.push(handle);
                    current_vus += 1;
                    time::sleep(step_delay).await;
                }
            } else if target < current_vus {
                // ── Linear ramp-down: lower the target gradually across the
                //    duration so surplus VUs self-select to exit over time,
                //    not all at once (no cliff). Level-triggered: each VU
                //    claims a surplus slot via try_claim_ramp_down at its next
                //    iteration start.
                let delta = current_vus - target;
                let step_delay = stage_duration / delta as u32;
                for step in 1..=delta {
                    // Interpolate the ramp-down target from current_vus down
                    // to `target`, one unit at a time.
                    let new_target = current_vus - step;
                    if new_target < target {
                        break;
                    }
                    self.set_ramp_down_target(new_target, current_vus);
                    tracing::debug!(
                        "Ramp-down step: target {new_target} (from {current_vus}, grace: {:?})",
                        grace_rd
                    );
                    time::sleep(step_delay).await;
                }
                self.set_ramp_down_target(target, current_vus);

                // Let the final surplus drain within the graceful_ramp_down
                // window (residual VUs exit at their next loop-top claim).
                tracing::debug!(
                    "Ramp-down: waiting for the last {} VUs to exit (grace: {:?}, target: {})",
                    delta,
                    grace_rd,
                    target
                );
                let drained = self
                    .wait_for_drain_while(grace_rd, || async {
                        let active = self.active_vus.load(Ordering::Acquire);
                        active <= target
                    })
                    .await;

                if drained {
                    // All surplus VUs exited — clear ramp-down state so a
                    // subsequent stage can't spuriously claim. If it timed out
                    // (grace-expired stragglers), KEEP the state so those VUs
                    // still exit at their next loop-top claim.
                    self.clear_ramp_down();
                }

                // NOTE: after a timed-out drain, `current_vus` is stale
                // (actual active_vus still includes stragglers + retained
                // VUs). A subsequent ramp-down then computes `remaining` from
                // this under-count, so the pool can settle above its target.
                // Bounded: the kept claims still drain the stragglers, and
                // once they exit, active converges to the tracked count (the
                // next `set_ramp_down_target` recomputes from the stale
                // current_vus, so full correction waits for that drain).
                current_vus = target;
            } else {
                // ── Constant stage: hold the current VU count for the FULL
                //    stage duration (k6 holds `target` VUs). VUs keep
                //    iterating; we simply wait out the stage.
                tracing::debug!(
                    "Hold: {} VUs constant for {:?}",
                    current_vus,
                    stage_duration
                );
                time::sleep(stage_duration).await;
            }
        }

        // Final stage complete — signal soft stop for all remaining VUs
        self.request_stop();

        // Wait for remaining VUs to drain within the final graceful stop window
        self.wait_for_drain(grace).await;

        // Wait for all JoinHandles (bounded — see await_handles_bounded)
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND)
            .await;

        tracing::info!("Ramping VUs finished");
    }

    /// Run with shared iterations across all VUs.
    ///
    /// `max_duration` is treated as a **cap**: VUs get at most this much time
    /// to finish their iterations, but can complete earlier. The method uses
    /// `select!` between VUs draining naturally and the max_duration timeout
    /// so a 10-iteration run doesn't block for the full 10-minute default cap.
    async fn run_shared_iterations<F>(
        &self,
        total_iterations: u64,
        max_duration: Option<Duration>,
        grace: Duration,
        run_vu: &F,
    ) where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        // For simplicity, use a fixed set of VUs and shared iteration counter
        let vus = match &self.config {
            ExecutionConfig::SharedIterations { vus, .. } => *vus,
            _ => 1,
        };

        tracing::info!(
            "Starting shared iterations: {} across {} VUs (max_duration: {:?}, grace: {:?})",
            total_iterations,
            vus,
            max_duration,
            grace
        );

        let mut handles = Vec::new();
        for vu_id in 0..vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // max_duration is a CAP, not a mandatory wait.
        // Race the JOIN of all VU handles against the timeout. The join only
        // completes when every VU task has actually ended — unlike polling
        // active_vus, which is still 0 at this point because VUs increment it
        // asynchronously inside their spawned tasks (the startup race that made
        // the old select! resolve immediately and drop the timeout branch).
        //
        // NOTE: each JoinHandle must be polled EXACTLY ONCE — tokio panics
        // with "JoinHandle polled after completion" if a completed handle is
        // re-polled. The old code joined here and then joined AGAIN in
        // `await_handles_bounded`, which panicked the scenario task on every
        // shared-iterations run. After this block the handles are dropped:
        // drained VUs are already done, and in the max_duration branch the
        // level-triggered stop flag plus the engine's active_vus drain loop
        // let any stragglers exit on their own.
        if let Some(max_dur) = max_duration {
            let all_done = futures::future::join_all(handles.iter_mut());
            tokio::pin!(all_done);
            tokio::select! {
                _ = &mut all_done => {
                    // All VUs finished before max_duration — done.
                    tracing::debug!(
                        "Shared iterations: all VUs drained before max_duration ({:?})",
                        max_dur
                    );
                }
                _ = time::sleep(max_dur) => {
                    // max_duration elapsed — signal soft stop (grace applies).
                    tracing::warn!(
                        "Shared iterations: max_duration ({:?}) reached — requesting stop",
                        max_dur
                    );
                    self.request_stop();
                    self.wait_for_drain(grace).await;
                }
            }
        } else {
            // No cap — join all VU handles directly (single join, no re-poll).
            futures::future::join_all(handles.iter_mut()).await;
        }

        tracing::info!("Shared iterations finished");
    }

    /// Run with constant arrival rate.
    ///
    /// Uses a time-based token bucket (no 1ms timer floor — resilient at high rates)
    /// and a dynamically growing VU pool (`pre_alloc_vus → max_vus`). VUs are
    /// spawned on demand when the current pool is saturated.
    async fn run_arrival_rate<F>(
        &self,
        rate: f64,
        pre_alloc: u32,
        max_vus: u32,
        duration: Duration,
        grace: Duration,
        run_vu: &F,
    ) where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!(
            "Starting constant arrival rate: {}/s for {:?} (pre_alloc={}, max_vus={}, grace: {:?})",
            rate,
            duration,
            pre_alloc,
            max_vus,
            grace
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
                                current_vus,
                                current_vus + grow_by,
                                rate
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

        // Signal soft stop — VUs finish their current iteration
        self.request_stop();

        // Wait for active VUs to drain within the graceful stop window
        self.wait_for_drain(grace).await;

        // Bounded join — a stuck VU cannot hang the run (P2 trailing join)
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND)
            .await;

        let dropped_total = self.arrival_dropped.load(Ordering::Relaxed);
        tracing::info!(
            "Constant arrival rate finished (dropped: {})",
            dropped_total
        );
    }

    /// Run with ramping arrival rate — stages of target rate (iterations/sec).
    /// Similar to k6's `ramping-arrival-rate` executor.
    ///
    /// Uses a time-based token bucket (same as `run_arrival_rate`) but the rate
    /// linearly interpolates across stages over the total stage duration.
    /// VUs are spawned on demand when the current pool is saturated, up to max_vus.
    async fn run_ramping_arrival_rate<F>(
        &self,
        start_rate: f64,
        stages: &[tropel_core::config::ArrivalRateStage],
        pre_alloc: u32,
        max_vus: u32,
        grace: Duration,
        run_vu: &F,
    ) where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        // Compute total duration from all stages
        let mut total_duration = Duration::ZERO;
        for stage in stages {
            if let Ok(d) = parse_duration(&stage.duration) {
                total_duration += d;
            }
        }

        if total_duration == Duration::ZERO {
            tracing::warn!("Ramping arrival rate: total duration is zero, nothing to run");
            return;
        }

        tracing::info!(
            "Starting ramping arrival rate: start_rate={}/s, {} stages, total={:?} (pre_alloc={}, max_vus={}, grace: {:?})",
            start_rate, stages.len(), total_duration, pre_alloc, max_vus, grace
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

        // Time-based token bucket (same as run_arrival_rate) with stage-aware rate
        // Helpers for computing the instantaneous rate at a given elapsed time
        let stage_data: Vec<(f64, f64, f64)> = {
            let mut data = Vec::with_capacity(stages.len());
            let mut prev_target = start_rate;
            for stage in stages {
                let dur = parse_duration(&stage.duration).unwrap_or(Duration::from_secs(10));
                let dur = dur.max(Duration::from_millis(1));
                let dur_secs = dur.as_secs_f64();
                data.push((dur_secs, prev_target, stage.target));
                prev_target = stage.target;
            }
            data
        };

        // Helper to compute the exact token count at a given elapsed time.
        // Uses the integral of the piecewise-linear rate function:
        // - For a completed stage: (start + end) * duration / 2 (trapezoid area)
        // - For a partial stage: d * (s*p + (e-s)*p²/2) where p = remaining/d
        // This avoids the burst bug from point-sampling `elapsed * current_rate`
        // at stage boundaries.
        let tokens_at = |elapsed_secs: f64| -> f64 {
            if stage_data.is_empty() {
                return elapsed_secs * start_rate;
            }
            let mut remaining = elapsed_secs;
            let mut total = 0.0_f64;
            for &(dur_secs, s, e) in &stage_data {
                if remaining <= 0.0 {
                    break;
                }
                if remaining >= dur_secs {
                    // Completed stage: trapezoid area
                    total += (s + e) * dur_secs / 2.0;
                    remaining -= dur_secs;
                } else {
                    // Partial stage: linear ramp integral
                    let p = remaining / dur_secs;
                    total += dur_secs * (s * p + (e - s) * p * p / 2.0);
                    remaining = 0.0;
                }
            }
            // Any remaining time after the last stage uses the final rate
            if remaining > 0.0 {
                let final_rate = stages.last().map(|s| s.target).unwrap_or(start_rate);
                total += remaining * final_rate;
            }
            total
        };

        let start = time::Instant::now();
        let mut last_target: u64 = 0;

        while start.elapsed() < total_duration {
            let elapsed_secs = start.elapsed().as_secs_f64();
            let exact_tokens = tokens_at(elapsed_secs);
            let target = exact_tokens as u64;

            if target > last_target {
                let to_add = target - last_target;
                let current = self.arrival_tokens.load(Ordering::Relaxed);
                let capacity = max_tokens.saturating_sub(current);
                let actual_add = to_add.min(capacity);

                if actual_add > 0 {
                    self.arrival_tokens.fetch_add(actual_add, Ordering::Relaxed);
                    self.arrival_notify.notify_waiters();

                    // Grow VU pool if saturated
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
                                "Ramping arrival-rate: VU pool {} → {} at t={:.1}s",
                                current_vus,
                                current_vus + grow_by,
                                elapsed_secs
                            );
                            current_vus += grow_by;
                        }
                    }
                }

                let overflow = to_add.saturating_sub(capacity);
                if overflow > 0 {
                    dropped.fetch_add(overflow, Ordering::Relaxed);
                }
            }

            last_target = target;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Signal soft stop
        self.request_stop();
        self.wait_for_drain(grace).await;

        // Bounded join — a stuck VU cannot hang the run (P2 trailing join)
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND)
            .await;

        let dropped_total = self.arrival_dropped.load(Ordering::Relaxed);
        tracing::info!("Ramping arrival rate finished (dropped: {})", dropped_total);
    }

    /// Run with per-VU iterations — each VU runs exactly N iterations independently.
    /// Similar to k6's `per-vu-iterations` executor.
    ///
    /// `max_duration` is treated as a **cap**: VUs get at most this much time
    /// to finish their iterations, but can complete earlier. Uses `select!`
    /// between VU drain and the timeout so a fast run doesn't block.
    async fn run_per_vu_iterations<F>(
        &self,
        vus: u32,
        per_vu_iters: u64,
        max_duration: Option<Duration>,
        grace: Duration,
        run_vu: &F,
    ) where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!(
            "Starting per-VU iterations: {} VUs × {} iterations each (max_duration: {:?}, grace: {:?})",
            vus, per_vu_iters, max_duration, grace
        );

        let mut handles = Vec::new();
        for vu_id in 0..vus {
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }

        // max_duration is a CAP, not a mandatory wait.
        // Race the JOIN of all VU handles against the timeout (same startup-race
        // fix as run_shared_iterations — see there). Each JoinHandle is polled
        // exactly once (re-polling a completed handle panics tokio).
        if let Some(max_dur) = max_duration {
            let all_done = futures::future::join_all(handles.iter_mut());
            tokio::pin!(all_done);
            tokio::select! {
                _ = &mut all_done => {
                    // All VUs finished before max_duration — done.
                    tracing::debug!(
                        "Per-VU iterations: all VUs drained before max_duration ({:?})",
                        max_dur
                    );
                }
                _ = tokio::time::sleep(max_dur) => {
                    // max_duration elapsed — signal stop (grace period applies).
                    tracing::warn!(
                        "Per-VU iterations: max_duration ({:?}) reached — requesting stop",
                        max_dur
                    );
                    self.request_stop();
                    self.wait_for_drain(grace).await;
                }
            }
        } else {
            // No cap — join all VU handles directly (single join, no re-poll).
            futures::future::join_all(handles.iter_mut()).await;
        }

        tracing::info!("Per-VU iterations finished");
    }

    /// Run with externally-controlled VUs — the pool scales at runtime via
    /// the control API (`set_control_target`). k6's `externally-controlled`
    /// executor / REST `/v1/status` parity.
    ///
    /// Starts `vus` VUs, then a control loop reconciles the live pool toward
    /// `control_target_vus` (clamped to `control_max_vus`): growing spawns new
    /// VU tasks, shrinking reuses the ramp-down claim mechanism so exactly the
    /// surplus exits. Runs until `duration` elapses (when set) or a stop is
    /// requested (control API stop, signal, threshold abort).
    async fn run_externally_controlled<F>(
        &self,
        vus: u32,
        max_vus: u32,
        duration: Option<Duration>,
        grace: Duration,
        run_vu: &F,
    ) where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        tracing::info!(
            "Starting externally-controlled VUs: initial={}, max={} (duration: {:?}, grace: {:?})",
            vus,
            max_vus,
            duration,
            grace
        );

        // Seed the control state from the config.
        self.control_max_vus.store(max_vus, Ordering::Release);
        self.control_target_vus.store(vus.min(max_vus), Ordering::Release);

        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        // Monotonic VU id counter. Ids are handed to run_vu for data-row
        // rotation / JS context naming; they must NEVER be reused while an
        // old VU with the same id is still exiting (a regrow after a shrink
        // would otherwise collide). Only ever incremented.
        let mut next_vu_id = 0u32;
        let initial = self.control_target();
        for _ in 0..initial {
            let handle = run_vu(self.shared_clone(), next_vu_id);
            handles.push(handle);
            next_vu_id += 1;
        }

        // Wait (bounded) for the initial VUs to register in `active_vus` so
        // the first reconcile doesn't see active=0 and double-spawn. Each VU
        // task increments active at startup, normally within milliseconds.
        let reg_deadline = time::Instant::now() + Duration::from_secs(5);
        while self.active_vus.load(Ordering::Acquire) < initial {
            if time::Instant::now() >= reg_deadline {
                break;
            }
            time::sleep(Duration::from_millis(5)).await;
        }

        let control_notify = self.control_notify();
        let started = time::Instant::now();

        // Control loop: reconcile the LIVE pool toward the target. All
        // decisions use the actual `active_vus` count — VUs exit on their own
        // via stop / ramp-down claims, so a stale logical counter would both
        // overshoot on regrow and leak un-cancelled surplus. The notify
        // prevents busy-waiting; the 100ms tick bounds latency if a wake is
        // missed (notify_waiters is edge-triggered).
        loop {
            if self.is_stop_requested() || self.is_force_stop_requested() {
                break;
            }
            if let Some(dur) = duration {
                if started.elapsed() >= dur {
                    tracing::debug!(
                        "Externally-controlled: duration ({:?}) elapsed — stopping",
                        dur
                    );
                    break;
                }
            }

            let target = self.control_target().min(self.control_max());
            let active = self.active_vus.load(Ordering::Acquire);
            if target > active {
                // Grow: clear any pending ramp-down FIRST — otherwise the
                // freshly spawned VUs would read the stale `ramp_down_target`
                // (active > old target, remaining > 0) and immediately
                // self-exit at their loop top, silently nullifying the grow.
                self.clear_ramp_down();
                for _ in active..target {
                    let handle = run_vu(self.shared_clone(), next_vu_id);
                    handles.push(handle);
                    next_vu_id += 1;
                }
                tracing::debug!("Externally-controlled: VU pool {} → {}", active, target);
            } else if target < active {
                // Shrink: reuse the ramp-down claim mechanism so exactly
                // `active - target` VUs exit (level-triggered; re-armed each
                // tick against the live count, so the drain self-corrects
                // even if some VUs died for other reasons).
                tracing::debug!("Externally-controlled: VU pool {} → {}", active, target);
                self.set_ramp_down_target(target, active);
            }

            tokio::select! {
                _ = control_notify.notified() => {}
                _ = time::sleep(Duration::from_millis(100)) => {}
            }
        }

        // Signal soft stop — VUs finish their current iteration.
        self.request_stop();
        self.wait_for_drain(grace).await;
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND).await;

        tracing::info!("Externally-controlled finished");
    }

    /// Await all VU JoinHandles, but bounded by a hard timeout so a VU that
    /// ignores `force_stop` **and** never trips the JS interrupt cannot hang
    /// the final join loop forever. Resolves when all handles end or `bound`
    /// elapses (the detached tasks are abandoned, matching k6's behaviour of
    /// hard-aborting the run after the grace window).
    async fn await_handles_bounded(handles: &mut [tokio::task::JoinHandle<()>], bound: Duration) {
        let all_done = futures::future::join_all(handles.iter_mut());
        tokio::pin!(all_done);
        match tokio::time::timeout(bound, &mut all_done).await {
            Ok(_) => tracing::debug!("All VU handles resolved"),
            Err(_) => {
                tracing::warn!(
                    "Timed out after {:?} waiting for VU handles — detaching remaining VUs",
                    bound
                );
            }
        }
    }

    /// Public handle for the control API — an `Arc` to this scheduler so
    /// `serve_control_api` can set the VU target / request stop mid-run.
    pub fn control_handle(&self) -> Arc<VUScheduler> {
        self.shared_clone()
    }

    /// Create a shared clone of this scheduler for passing to VU tasks.
    fn shared_clone(&self) -> Arc<VUScheduler> {
        Arc::new(VUScheduler {
            config: self.config.clone(),
            active_vus: self.active_vus.clone(),
            total_iterations: self.total_iterations.clone(),
            stop_signal: self.stop_signal.clone(),
            stop_requested: self.stop_requested.clone(),
            force_stop_requested: self.force_stop_requested.clone(),
            arrival_tokens: self.arrival_tokens.clone(),
            arrival_notify: self.arrival_notify.clone(),
            arrival_dropped: self.arrival_dropped.clone(),
            idle_vus: self.idle_vus.clone(),
            ramp_down_target: self.ramp_down_target.clone(),
            ramp_down_remaining: self.ramp_down_remaining.clone(),
            control_target_vus: self.control_target_vus.clone(),
            control_max_vus: self.control_max_vus.clone(),
            control_notify: self.control_notify.clone(),
        })
    }

    /// Wait up to `grace` for active VUs to drain to 0.
    /// After the deadline, calls `force_stop()` to hard-abort any remaining.
    pub async fn wait_for_drain(&self, grace: Duration) {
        if grace == Duration::ZERO {
            // No grace period — force stop immediately
            self.request_force_stop();
            return;
        }

        let deadline = time::Instant::now() + grace;
        loop {
            let active = self.active_vus.load(Ordering::Acquire);
            if active == 0 {
                tracing::debug!("All VUs drained within grace period");
                return;
            }
            if time::Instant::now() >= deadline {
                tracing::warn!(
                    "Grace period ({:?}) expired with {} active VUs — force stopping",
                    grace,
                    active
                );
                self.request_force_stop();
                return;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Wait up to `grace` for a condition (e.g., active_vus <= target) to become true.
    /// After the deadline, logs a warning but does NOT force-stop — the caller
    /// handles the final state.
    ///
    /// Returns `true` if the condition was satisfied within the grace window,
    /// `false` if the deadline expired first (callers use this to decide
    /// whether to keep or clear drain state).
    pub async fn wait_for_drain_while<F, Fut>(&self, grace: Duration, condition: F) -> bool
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        if grace == Duration::ZERO {
            // Zero-grace: "timed out" is the conservative answer — the caller
            // keeps ramp-down state so surplus VUs self-exit at loop top.
            // (Not clearing is correct: grace=0 means don't wait for in-flight
            // iterations; the remaining surplus still needs to exit.)
            return false;
        }

        let deadline = time::Instant::now() + grace;
        loop {
            if condition().await {
                return true;
            }
            if time::Instant::now() >= deadline {
                tracing::warn!(
                    "Grace period ({:?}) expired while waiting for drain condition",
                    grace
                );
                return false;
            }
            time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// Parse an optional graceful_stop/graceful_ramp_down string into a Duration.
/// Defaults to 30 seconds when the field is None or empty, matching k6's default.
fn graceful_stop_duration(s: &Option<String>) -> Duration {
    match s {
        Some(dur_str) if !dur_str.trim().is_empty() => {
            parse_duration(dur_str).unwrap_or(Duration::from_secs(30))
        }
        _ => Duration::from_secs(30),
    }
}

fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        let v: u64 = num
            .parse()
            .map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(v))
    } else if let Some(num) = s.strip_suffix('s') {
        let v: f64 = num
            .parse()
            .map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v))
    } else if let Some(num) = s.strip_suffix('m') {
        let v: f64 = num
            .parse()
            .map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v * 60.0))
    } else if let Some(num) = s.strip_suffix('h') {
        let v: f64 = num
            .parse()
            .map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v * 3600.0))
    } else {
        let v: f64 = s
            .parse()
            .map_err(|_| tropel_core::TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Locks the ramp-down overshoot invariant: when `current_vus` VUs contend
    /// for `current_vus - target` surplus slots, EXACTLY that many claims
    /// succeed — no VU that reads the same `active > target` snapshot can
    /// over-exit below the target.
    #[tokio::test]
    async fn try_claim_ramp_down_bounds_exits_to_surplus() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        });
        let current_vus: u32 = 10;
        let target: u32 = 5;
        sched.set_ramp_down_target(target, current_vus);

        // Simulate all 10 VUs observing active=10 (the old overshoot race:
        // every VU sees active > target). Exactly 5 may claim.
        let mut claimed = 0usize;
        for _ in 0..current_vus {
            if sched.try_claim_ramp_down(10).await {
                claimed += 1;
            }
        }
        assert_eq!(claimed, (current_vus - target) as usize);

        // A 6th VU arriving late must not exit.
        assert!(!sched.try_claim_ramp_down(10).await);
    }

    /// After a fully-drained ramp-down, clearing resets the target so a later
    /// stage can't spuriously claim.
    #[tokio::test]
    async fn clear_ramp_down_disables_claims() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        });
        sched.set_ramp_down_target(3, 8);
        assert!(sched.try_claim_ramp_down(8).await);
        sched.clear_ramp_down();
        // Stale target reset — no claim, even though a stale snapshot says
        // active > old target.
        assert!(!sched.try_claim_ramp_down(8).await);
    }

    /// Ramp-down claims only apply when the pool is actually above target.
    #[tokio::test]
    async fn try_claim_ramp_down_noop_when_at_or_below_target() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        });
        sched.set_ramp_down_target(5, 10);
        assert!(!sched.try_claim_ramp_down(5).await); // at target
        assert!(!sched.try_claim_ramp_down(4).await); // below target
    }
}
