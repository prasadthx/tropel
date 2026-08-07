use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

/// Process-wide monotonic VU id allocator.
///
/// VU ids are handed to `run_vu` for data-row rotation, JS context naming and
/// `exec.vu.idInTest`, and map onto the shared worker pool via
/// `vu_id % MAX_WORKERS`. They must therefore be unique across ALL scenarios
/// (each scenario used to restart at 0, putting two VUs on one worker) and
/// must NEVER be reused while an old VU with the same id is still live
/// (`run_ramping` used to reuse ids after a ramp-down, colliding with
/// stragglers still mid-iteration). A single process-wide counter guarantees
/// both. Only ever incremented — the id space is u32, ample for any run.
static NEXT_VU_ID: AtomicU32 = AtomicU32::new(0);
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

/// RAII lease that tracks a VU in the scheduler's `active_vus` counter.
///
/// Increments on `acquire` and decrements on drop — including when the VU
/// task panics, which a bare `remove_active_vu().await` at the tail of the
/// task (dropped mid-panic) cannot guarantee. Without this, a panicked VU
/// leaks the counter and the engine's drain loop would spin forever.
///
/// The counter is a plain `Arc<AtomicU32>` so `Drop` is sync and
/// lock-free — no `.await` allowed in `Drop`, and no panic can occur here.
pub struct VuLease {
    active_vus: Arc<AtomicU32>,
}

impl VuLease {
    /// Create a lease, incrementing the active-VU counter.
    pub fn acquire(sched: &VUScheduler) -> Self {
        let active_vus = sched.active_vus_handle();
        active_vus.fetch_add(1, Ordering::AcqRel);
        Self { active_vus }
    }
}

impl Drop for VuLease {
    fn drop(&mut self) {
        self.active_vus
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }
}

/// Controls the lifecycle of VUs during a load test.
pub struct VUScheduler {
    /// Shared execution config — wrapped in `Arc` so `shared_clone()` (called
    /// once per VU task) is a refcount bump instead of a full deep clone of
    /// the whole config (thresholds, env, tags, …) for every VU.
    config: Arc<ExecutionConfig>,
    /// Lock-free active-VU counter. Atomic so sync JS bridge closures
    /// (inside ctx.with) can read `exec.instance.vusActive` without awaiting
    /// an async mutex — the tokio Mutex made that impossible.
    active_vus: Arc<AtomicU32>,
    /// Lock-free total-iteration counter, shared the same way for
    /// `exec.instance.iterationsCompleted` (a GLOBAL total across all VUs).
    total_iterations: Arc<AtomicU64>,
    /// Lock-free claimed-iteration counter for shared-iterations mode.
    /// Pre-claimed (CAS) BEFORE an iteration starts so the iteration budget
    /// can never be overshot by concurrent VUs finishing simultaneously.
    /// Distinct from `total_iterations` (the COMPLETED count backing
    /// exec.instance.iterationsCompleted).
    claimed_iterations: Arc<AtomicU64>,
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
    /// Hard ceiling for `control_max_vus` — the configured `max_vus` from the
    /// executor options. The control API may LOWER `max` but can never raise
    /// it past this value (a client can't exceed the configured cap).
    control_hard_max: Arc<AtomicU32>,
    /// Logical externally-controlled pool size: VUs the control loop has
    /// spawned minus those that claimed a ramp-down exit. Bumped synchronously
    /// at spawn so reconciliation never double-spawns on lagging registration.
    control_spawned: Arc<AtomicU32>,
    /// Pause flag (externally-controlled only): while set, VUs hold at the
    /// top of their loop and the control loop keeps the pool but doesn't grow.
    control_paused: Arc<AtomicBool>,
    /// Wakes the externally-controlled control loop when the target changes.
    control_notify: Arc<tokio::sync::Notify>,
    /// Threshold-taint flag (k6 `tainted` in the status doc): set once any
    /// threshold (with or without abortOnFail) fails. Sticky — once tainted,
    /// the run stays tainted.
    control_tainted: Arc<AtomicBool>,
}

/// RAII guard for the arrival-rate idle count. Marks the VU idle on
/// creation and busy on drop, so the count can't leak if the VU task is
/// aborted or panics while waiting for an arrival token.
pub struct IdleVusGuard<'a> {
    sched: &'a VUScheduler,
}

impl<'a> IdleVusGuard<'a> {
    pub fn new(sched: &'a VUScheduler) -> Self {
        sched.mark_idle();
        Self { sched }
    }
}

impl Drop for IdleVusGuard<'_> {
    fn drop(&mut self) {
        self.sched.mark_busy();
    }
}

/// RAII guard for the externally-controlled spawn count. Decrements
/// [`VUScheduler::vu_exited`] when the VU task exits for ANY reason
/// (stop / force-stop / panic / abort / iteration budget), so a VU that dies
/// outside a ramp-down claim can't leave `target > spawned` permanently
/// false. A successful ramp-down claim already decrements inside
/// `try_claim_ramp_down`, so callers must [`Self::mark_claimed`] there to
/// avoid a double decrement.
pub struct ControlSpawnGuard<'a> {
    sched: &'a VUScheduler,
    claimed: bool,
}

impl<'a> ControlSpawnGuard<'a> {
    pub fn new(sched: &'a VUScheduler) -> Self {
        Self {
            sched,
            claimed: false,
        }
    }

    /// Mark that the ramp-down claim path already decremented the count.
    pub fn mark_claimed(&mut self) {
        self.claimed = true;
    }
}

impl Drop for ControlSpawnGuard<'_> {
    fn drop(&mut self) {
        if !self.claimed {
            self.sched.vu_exited();
        }
    }
}

impl VUScheduler {
    /// Create a new VU scheduler from config.
    pub fn new(config: &ExecutionConfig) -> Self {
        Self {
            config: Arc::new(config.clone()),
            active_vus: Arc::new(AtomicU32::new(0)),
            total_iterations: Arc::new(AtomicU64::new(0)),
            claimed_iterations: Arc::new(AtomicU64::new(0)),
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
            control_hard_max: Arc::new(AtomicU32::new(u32::MAX)),
            control_spawned: Arc::new(AtomicU32::new(0)),
            control_paused: Arc::new(AtomicBool::new(false)),
            control_notify: Arc::new(tokio::sync::Notify::new()),
            control_tainted: Arc::new(AtomicBool::new(false)),
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
        let claimed = self
            .ramp_down_remaining
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |r| {
                if r > 0 {
                    Some(r - 1)
                } else {
                    None
                }
            })
            .is_ok();
        if claimed {
            // A VU is about to exit — keep the logical externally-controlled
            // pool in sync (saturating so non-externally-controlled modes,
            // where the counter stays 0, are unaffected).
            self.control_spawned
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                    Some(v.saturating_sub(1))
                })
                .ok();
        }
        claimed
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
        matches!(
            self.config.as_ref(),
            ExecutionConfig::ConstantArrivalRate { .. }
        ) || matches!(
            self.config.as_ref(),
            ExecutionConfig::RampingArrivalRate { .. }
        )
    }

    /// Mark a VU as idle (waiting for an arrival token). Prefer the RAII
    /// [`IdleVusGuard`] so an aborted/panicking VU can't leak the count.
    pub fn mark_idle(&self) {
        self.idle_vus.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark a VU as busy (acquired a token, about to execute).
    ///
    /// Saturating: a raw `fetch_sub` on an already-zero count would wrap to
    /// `u32::MAX`, and `grow_arrival_pool` treats any non-zero idle count as
    /// "pool has spare VUs" — permanently disabling growth for the run
    /// (backlog line 170).
    pub fn mark_busy(&self) {
        self.idle_vus
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    /// A VU task exited for ANY reason — decrement the logical
    /// externally-controlled pool count so the control loop sees the loss
    /// and re-spawns to target. Saturating: modes where the counter stays 0
    /// are unaffected. Previously only a ramp-down claim decremented, so a
    /// VU dying via stop/force-stop/panic/abort left `target > spawned`
    /// permanently false — a silent undershoot with no re-spawn (backlog
    /// line 170).
    pub fn vu_exited(&self) {
        self.control_spawned
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_sub(1))
            })
            .ok();
    }

    /// Create the RAII guard that marks this VU idle and restores the count
    /// when the VU task ends (covers the `mark_idle`..`mark_busy` window even
    /// under abort/panic).
    pub fn idle_guard(&self) -> IdleVusGuard<'_> {
        IdleVusGuard::new(self)
    }

    /// Create the RAII guard that decrements [`Self::vu_exited`] when the VU
    /// task exits for any reason. Call [`ControlSpawnGuard::mark_claimed`]
    /// after a successful ramp-down claim (which already decrements) to
    /// avoid a double decrement.
    pub fn control_spawn_guard(&self) -> ControlSpawnGuard<'_> {
        ControlSpawnGuard::new(self)
    }

    /// Current count of idle VUs (waiting for tokens).
    pub fn idle_vu_count(&self) -> u32 {
        self.idle_vus.load(Ordering::Relaxed)
    }

    /// Set the externally-controlled VU target and cap from the control API.
    /// The API can lower `max` but never raise it above the configured
    /// ceiling (`control_hard_max`). Clamps `vus` to `[0, max]` and wakes the
    /// control loop.
    pub fn set_control_target(&self, vus: u32, max_vus: u32) {
        let hard = self.control_hard_max.load(Ordering::Acquire);
        let max = max_vus.min(hard);
        self.control_max_vus.store(max, Ordering::Release);
        self.control_target_vus
            .store(vus.min(max), Ordering::Release);
        self.control_notify.notify_waiters();
    }

    /// Set the externally-controlled pause flag (k6 `paused`). While paused,
    /// VUs hold at the top of their loop and the pool is kept, not grown.
    pub fn set_paused(&self, paused: bool) {
        self.control_paused.store(paused, Ordering::Release);
        self.control_notify.notify_waiters();
    }

    /// Whether the externally-controlled executor is paused.
    pub fn is_paused(&self) -> bool {
        self.control_paused.load(Ordering::Acquire)
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

    /// Mark the run as threshold-tainted (sticky). Exposed to the engine so
    /// the threshold monitor can set it when a check fails.
    pub fn set_tainted(&self) {
        self.control_tainted.store(true, Ordering::Release);
    }

    /// Whether any threshold has failed so far (k6 status `tainted`).
    pub fn is_tainted(&self) -> bool {
        self.control_tainted.load(Ordering::Acquire)
    }

    /// Get and reset the dropped iterations counter.
    pub fn take_dropped_iterations(&self) -> u64 {
        self.arrival_dropped.swap(0, Ordering::Relaxed)
    }

    /// Get active VU count.
    pub async fn active_vus(&self) -> u32 {
        self.active_vus.load(Ordering::Acquire)
    }

    /// The PRE-ALLOCATED peak VU count from the execution config — the number
    /// of VUs the scheduler commits to spinning up, regardless of how many are
    /// mid-iteration at any instant. Backs the `vus_max` metric (k6 emits the
    /// configured peak, not a sampled current active count).
    pub fn peak_vus(&self) -> u32 {
        match self.config.as_ref() {
            ExecutionConfig::ConstantVus { vus, .. } => *vus,
            ExecutionConfig::RampingVus {
                stages, start_vus, ..
            } => stages.iter().fold(*start_vus, |acc, s| acc.max(s.target)),
            ExecutionConfig::SharedIterations { vus, .. } => *vus,
            ExecutionConfig::ConstantArrivalRate { max_vus, .. } => *max_vus,
            ExecutionConfig::PerVUIterations { vus, .. } => *vus,
            ExecutionConfig::RampingArrivalRate { max_vus, .. } => *max_vus,
            ExecutionConfig::ExternallyControlled { max_vus, .. } => *max_vus,
        }
    }

    /// Get total iterations completed.
    pub async fn total_iterations(&self) -> u64 {
        self.total_iterations.load(Ordering::Acquire)
    }

    /// Increment iteration count.
    pub async fn increment_iterations(&self) {
        self.total_iterations.fetch_add(1, Ordering::AcqRel);
    }

    /// Try to atomically claim one slot of a shared iteration budget.
    /// Returns `true` if this VU may start an iteration, `false` when the
    /// budget is exhausted.
    ///
    /// Lock-free CAS: across all VUs exactly `budget` claims ever succeed,
    /// so the old run-then-check pattern — where each VU incremented the
    /// completed counter AFTER running and compared against the budget —
    /// could let up to `vus−1` extra iterations slip through when several
    /// VUs finished concurrently.
    pub fn try_claim_shared_iteration(&self, budget: u64) -> bool {
        self.claimed_iterations
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |c| {
                if c >= budget {
                    None
                } else {
                    Some(c + 1)
                }
            })
            .is_ok()
    }

    /// Shared handle to the active-VU counter — handed to a VU's PmState so
    /// the sync `exec.instance.vusActive` bridge can read it live.
    /// Allocate a globally-unique VU id from the process-wide monotonic
    /// counter. See [`NEXT_VU_ID`] for why uniqueness across scenarios and
    /// across a scenario's lifetime is a hard guarantee.
    fn alloc_vu_id(&self) -> u32 {
        NEXT_VU_ID.fetch_add(1, Ordering::Relaxed)
    }

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
        match self.config.as_ref() {
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
                let duration = duration.as_ref().and_then(|d| parse_duration(d).ok());
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
        for _ in 0..vus {
            let vu_id = self.alloc_vu_id();
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
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND).await;

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
        for _ in 0..current_vus {
            let vu_id = self.alloc_vu_id();
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
                // Clear any leftover ramp-down state FIRST. A ramp-down that
                // ended without draining (gracefulRampDown: "0s" makes
                // wait_for_drain_while return false unconditionally, so EVERY
                // ramp-down leaves remaining ≈ delta re-armed after the final
                // set_ramp_down_target(target, current_vus)) would otherwise
                // make each freshly spawned VU read active_vus > the stale
                // OLD target, claim a surplus slot and self-exit at its loop
                // top — silently eating up to delta VUs of this ramp-up.
                // During a GROW stage every existing VU (including
                // grace-expired stragglers from the previous ramp-down) is
                // within the new, higher target, so nothing should exit here
                // — matching the externally-controlled grow path, which
                // clears ramp-down first for exactly this hazard.
                self.clear_ramp_down();
                let delta = target - current_vus;
                let step_delay = stage_duration / delta;
                for _ in 0..delta {
                    let vu_id = self.alloc_vu_id();
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
                let step_delay = stage_duration / delta;
                for step in 1..=delta {
                    // Interpolate the ramp-down target from current_vus down
                    // to `target`, one unit at a time. Arm EXACTLY ONE surplus
                    // slot per step (remaining = new_target+1 - new_target = 1)
                    // so exactly one VU exits per step window — a true linear
                    // ramp. Re-arming to a GROWING value (current_vus -
                    // new_target = step) would let VUs exit in bursts and
                    // overshoot below the final target.
                    let new_target = current_vus - step;
                    if new_target < target {
                        break;
                    }
                    self.set_ramp_down_target(new_target, new_target + 1);
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
                    // subsequent stage can't spuriously claim.
                    self.clear_ramp_down();
                    current_vus = target;
                } else {
                    // Drain timed out (grace-expired stragglers still
                    // mid-iteration). KEEP the ramp-down state so those VUs
                    // still exit at their next loop-top claim, and adopt the
                    // REAL active count as `current_vus` — the old code set
                    // `current_vus = target` here, so a subsequent stage
                    // computed `remaining` from the under-count and the pool
                    // could settle ABOVE its target.
                    let real = self.active_vus.load(Ordering::Acquire);
                    tracing::debug!(
                        "Ramp-down drain timed out: adopting real active count {} as current_vus",
                        real
                    );
                    current_vus = real;
                }
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
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND).await;

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
        let vus = match self.config.as_ref() {
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
        for _ in 0..vus {
            let vu_id = self.alloc_vu_id();
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

    /// Grow the arrival-rate VU pool toward `max_vus` based on queued-token
    /// PRESSURE, not the token-add cadence.
    ///
    /// The original code only grew while a token was being ADDED (inside the
    /// `actual_add > 0` block) and by at most `to_add` (usually 1 per tick) —
    /// under slow latency the backlog built up faster than the pool, and once
    /// the bucket was FULL (`capacity == 0` → `actual_add == 0`) the pool
    /// could never grow again even though iterations were dropping. Now:
    /// whenever tokens are queued and no VU is idle to consume them, grow by
    /// the queued backlog (clamped to max_vus and a per-tick burst cap — each
    /// spawn creates a VU with its own JS context + client, so spawning the
    /// whole backlog at once would be a thundering herd; converging over a
    /// couple of 1ms ticks is just as fast). Re-checked every 1ms tick.
    ///
    /// Returns the number of VUs spawned.
    fn grow_arrival_pool<F>(
        &self,
        run_vu: &F,
        handles: &mut Vec<tokio::task::JoinHandle<()>>,
        current_vus: &mut u32,
        max_vus: u32,
        log_label: &str,
        elapsed_secs: f64,
    ) -> u32
    where
        F: Fn(Arc<VUScheduler>, u32) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
    {
        let queued = self.arrival_tokens.load(Ordering::Relaxed);
        let idle = self.idle_vus.load(Ordering::Relaxed);
        if queued == 0 || idle != 0 || *current_vus >= max_vus {
            return 0;
        }
        let grow_cap = (max_vus - *current_vus) as u64;
        const MAX_SPAWN_PER_TICK: u64 = 32;
        let grow_by = queued.min(grow_cap).min(MAX_SPAWN_PER_TICK) as u32;
        if grow_by == 0 {
            return 0;
        }
        for _ in 0..grow_by {
            let vu_id = self.alloc_vu_id();
            let handle = run_vu(self.shared_clone(), vu_id);
            handles.push(handle);
        }
        tracing::debug!(
            "{}: VU pool {} → {} (queued={}, t={:.1}s)",
            log_label,
            *current_vus,
            *current_vus + grow_by,
            queued,
            elapsed_secs
        );
        *current_vus += grow_by;
        grow_by
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
        for _ in 0..pre_alloc.max(1) {
            let vu_id = self.alloc_vu_id();
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
                }

                // Dropped iterations: tokens we couldn't add because the bucket
                // was full (all max_tokens preoccupied — no idle VU to consume).
                // This means VUs can't keep up with the target rate.
                let overflow = to_add.saturating_sub(capacity);
                if overflow > 0 {
                    dropped.fetch_add(overflow, Ordering::Relaxed);
                }
            }

            // Grow the VU pool based on queued-token PRESSURE (see
            // grow_arrival_pool) — decoupled from the token-add cadence so a
            // saturated bucket can never stall growth.
            self.grow_arrival_pool(
                run_vu,
                &mut handles,
                &mut current_vus,
                max_vus,
                "Arrival-rate",
                elapsed_secs,
            );

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
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND).await;

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
        for _ in 0..pre_alloc.max(1) {
            let vu_id = self.alloc_vu_id();
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
                }

                let overflow = to_add.saturating_sub(capacity);
                if overflow > 0 {
                    dropped.fetch_add(overflow, Ordering::Relaxed);
                }
            }

            // Same queued-token-pressure growth as run_arrival_rate (see
            // grow_arrival_pool).
            self.grow_arrival_pool(
                run_vu,
                &mut handles,
                &mut current_vus,
                max_vus,
                "Ramping arrival-rate",
                elapsed_secs,
            );

            last_target = target;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Signal soft stop
        self.request_stop();
        self.wait_for_drain(grace).await;

        // Bounded join — a stuck VU cannot hang the run (P2 trailing join)
        Self::await_handles_bounded(&mut handles, HANDLE_JOIN_BOUND).await;

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
        for _ in 0..vus {
            let vu_id = self.alloc_vu_id();
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

        // Seed the control state from the config. `control_hard_max` is the
        // configured ceiling the control API can never raise past.
        self.control_hard_max.store(max_vus, Ordering::Release);
        self.control_max_vus.store(max_vus, Ordering::Release);
        self.control_target_vus
            .store(vus.min(max_vus), Ordering::Release);

        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        // Ids come from the process-wide monotonic allocator — globally unique
        // across scenarios and never reused while an old VU with the same id
        // is still exiting (a regrow after a shrink would otherwise collide).
        let initial = self.control_target();
        for _ in 0..initial {
            let handle = run_vu(self.shared_clone(), self.alloc_vu_id());
            handles.push(handle);
        }
        // Logical pool size — bumped synchronously at spawn so reconcile
        // never double-spawns on lagging registration.
        self.control_spawned.store(initial, Ordering::Release);

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
            // Reap finished VU handles so a long grow/shrink run can't
            // accumulate completed JoinHandles unboundedly (each handle is
            // only polled once at final join anyway). Dropping a finished
            // handle is a no-op on the task itself.
            handles.retain(|h| !h.is_finished());

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

            if self.is_paused() {
                // Paused: hold the pool (no grow/shrink). The select keeps
                // this responsive to resume (control_notify) while the tick
                // covers an edge-triggered wake that was missed.
                tokio::select! {
                    _ = control_notify.notified() => {}
                    _ = time::sleep(Duration::from_millis(100)) => {}
                }
                continue;
            }

            let target = self.control_target().min(self.control_max());
            let spawned = self.control_spawned.load(Ordering::Acquire);
            if target > spawned {
                // Grow: clear any pending ramp-down FIRST — otherwise the
                // freshly spawned VUs would read the stale `ramp_down_target`
                // (active > old target, remaining > 0) and immediately
                // self-exit at their loop top, silently nullifying the grow.
                self.clear_ramp_down();
                for _ in spawned..target {
                    let handle = run_vu(self.shared_clone(), self.alloc_vu_id());
                    handles.push(handle);
                }
                // Synchronous bump — the next tick sees this count even if
                // the new VUs haven't registered in `active_vus` yet, so a
                // lagging registration can never trigger a double-spawn.
                self.control_spawned.store(target, Ordering::Release);
                tracing::debug!("Externally-controlled: VU pool {} → {}", spawned, target);
            } else if target < spawned {
                // Shrink: reuse the ramp-down claim mechanism so exactly
                // `spawned - target` VUs exit. Each claim decrements
                // `control_spawned`, so this is armed ONLY while the previous
                // surplus is fully drained (remaining == 0) — re-arming every
                // tick against the lagging `active` counter used to inflate
                // the surplus and overshoot below target.
                if self.ramp_down_remaining.load(Ordering::Relaxed) == 0 {
                    self.set_ramp_down_target(target, spawned);
                }
                tracing::debug!(
                    "Externally-controlled: shrinking pool {} → {}",
                    spawned,
                    target
                );
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
            claimed_iterations: self.claimed_iterations.clone(),
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
            control_hard_max: self.control_hard_max.clone(),
            control_spawned: self.control_spawned.clone(),
            control_paused: self.control_paused.clone(),
            control_notify: self.control_notify.clone(),
            control_tainted: self.control_tainted.clone(),
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
    tropel_core::parse_duration(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// VU ids are handed to `run_vu` for data-row rotation / worker pinning /
    /// `exec.vu.idInTest`. They must be unique across scenarios (each scenario
    /// used to restart at 0, putting two VUs on one shared worker) and never
    /// reused while a VU is live. The process-wide allocator guarantees both.
    #[tokio::test]
    async fn vu_ids_unique_across_schedulers_and_reused_nowhere() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..4 {
            // Simulate four independent scenarios, each spawning 3 VUs (a
            // ramp-down + regrow would previously reuse ids 0..n).
            let sched = VUScheduler::new(&ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "1s".to_string(),
                graceful_stop: None,
                think_time: Default::default(),
            });
            for _ in 0..3 {
                let id = sched.alloc_vu_id();
                assert!(
                    seen.insert(id),
                    "VU id {id} reused — breaks 1-VU-per-worker pinning"
                );
            }
        }
        // Sanity: the counter actually advanced past the per-scenario restart
        // boundary that used to collide (ids 0..n per scenario).
        assert!(*seen.iter().max().unwrap() >= 3);
    }

    /// Locks the ramp-down overshoot invariant: when `current_vus` VUs contend
    /// for `current_vus - target` surplus slots, EXACTLY that many claims
    /// succeed — no VU that reads the same `active > target` snapshot can
    /// over-exit below the target.
    ///
    /// The claims are made from REAL concurrently-running tasks (`tokio::spawn`
    /// + a barrier on a multi-thread runtime), so a `fetch_update` CAS bug that
    /// only manifests under simultaneous contention — invisible to the old
    /// single-threaded `for` loop — would fail here.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn try_claim_ramp_down_bounds_exits_to_surplus() {
        let sched = Arc::new(VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        }));
        let current_vus: u32 = 10;
        let target: u32 = 5;
        sched.set_ramp_down_target(target, current_vus);

        // All 10 VUs observe active=10 (the old overshoot race: every VU sees
        // active > target) and claim at the SAME instant. Exactly 5 succeed.
        let barrier = Arc::new(tokio::sync::Barrier::new(current_vus as usize));
        let mut handles = Vec::with_capacity(current_vus as usize);
        for _ in 0..current_vus {
            let sched = sched.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                sched.try_claim_ramp_down(10).await
            }));
        }
        let mut claimed = 0usize;
        for handle in handles {
            if handle.await.expect("ramp-down claim task panicked") {
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

    /// Locked: shared-iteration pre-claim CAS never overshoots the budget,
    /// no matter how many VUs contend simultaneously (the old run-then-check
    /// allowed up to vus−1 extras).
    ///
    /// 1000 VUs claim from REAL concurrently-running tasks (`tokio::spawn` +
    /// a barrier on a multi-thread runtime) — not the old single-threaded
    /// `for` loop that could never exercise the CAS under contention.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn try_claim_shared_iteration_bounds_to_budget() {
        let sched = Arc::new(VUScheduler::new(&ExecutionConfig::SharedIterations {
            iterations: 5,
            max_duration: None,
            vus: 10,
            graceful_stop: None,
            think_time: Default::default(),
        }));

        const CONTENDERS: usize = 1000;
        // All 1000 tasks trip the barrier before any claims — maximal
        // simultaneous `fetch_update` CAS pressure on the budget counter.
        let barrier = Arc::new(tokio::sync::Barrier::new(CONTENDERS));
        let mut handles = Vec::with_capacity(CONTENDERS);
        for _ in 0..CONTENDERS {
            let sched = sched.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                sched.try_claim_shared_iteration(5)
            }));
        }
        let mut claimed = 0u64;
        for handle in handles {
            if handle.await.expect("shared-iteration claim task panicked") {
                claimed += 1;
            }
        }
        assert_eq!(claimed, 5);

        // Exhausted budget — a late VU must not start.
        assert!(!sched.try_claim_shared_iteration(5));
    }

    /// Locked: the control API can LOWER the pool cap but can never raise it
    /// past the configured `max_vus` ceiling (a client can't exceed the run's
    /// cap just by PATCHing a bigger max).
    #[tokio::test]
    async fn control_max_clamped_to_configured_ceiling() {
        let sched = VUScheduler::new(&ExecutionConfig::ExternallyControlled {
            vus: 1,
            max_vus: 10,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        sched.control_hard_max.store(10, Ordering::Release);

        // Client tries to raise max above the configured 10 → clamped.
        sched.set_control_target(8, 100);
        assert_eq!(sched.control_target(), 8);
        assert_eq!(sched.control_max(), 10);

        // Lowering is allowed.
        sched.set_control_target(3, 7);
        assert_eq!(sched.control_target(), 3);
        assert_eq!(sched.control_max(), 7);

        // vus is also clamped to max.
        sched.set_control_target(50, 6);
        assert_eq!(sched.control_target(), 6);
    }

    /// Locked: pause is level-triggered and independent of the target/cap.
    #[tokio::test]
    async fn pause_is_level_triggered_and_independent() {
        let sched = VUScheduler::new(&ExecutionConfig::ExternallyControlled {
            vus: 1,
            max_vus: 10,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert!(!sched.is_paused());
        sched.set_paused(true);
        assert!(sched.is_paused());
        // Target/cap updates don't clear pause.
        sched.set_control_target(5, 9);
        assert!(sched.is_paused());
        sched.set_paused(false);
        assert!(!sched.is_paused());
    }

    /// Locked: a ramp-down claim decrements the logical externally-controlled
    /// pool (control_spawned), so reconcile growth can't double-spawn after
    /// VUs exit.
    #[tokio::test]
    async fn ramp_down_claim_syncs_control_spawned() {
        let sched = VUScheduler::new(&ExecutionConfig::ExternallyControlled {
            vus: 1,
            max_vus: 10,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        sched.control_spawned.store(8, Ordering::Release);
        sched.set_ramp_down_target(5, 8);
        assert!(sched.try_claim_ramp_down(8).await);
        assert_eq!(sched.control_spawned.load(Ordering::Acquire), 7);
        // Non-claiming mode (counter at 0) never underflows.
        let sched2 = VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 1,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert_eq!(sched2.control_spawned.load(Ordering::Acquire), 0);
        sched2.set_ramp_down_target(0, 1);
        assert!(sched2.try_claim_ramp_down(1).await);
        assert_eq!(sched2.control_spawned.load(Ordering::Acquire), 0);
    }

    /// Locked (backlog line 170): a VU exiting for ANY reason (not just a
    /// ramp-down claim) must decrement the logical externally-controlled
    /// pool, or `target > spawned` stays permanently false and the control
    /// loop never re-spawns. `vu_exited` saturates so modes where the
    /// counter stays 0 are unaffected.
    #[tokio::test]
    async fn vu_exited_decrements_saturating_and_guard_marks_claimed() {
        let sched = VUScheduler::new(&ExecutionConfig::ExternallyControlled {
            vus: 1,
            max_vus: 10,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        sched.control_spawned.store(5, Ordering::Release);

        // An unclaimed guard (VU died outside a ramp-down claim) decrements.
        {
            let guard = sched.control_spawn_guard();
            assert!(!guard.claimed);
        }
        assert_eq!(sched.control_spawned.load(Ordering::Acquire), 4);

        // A claimed guard (ramp-down claim already decremented) must NOT
        // double-decrement.
        {
            let mut guard = sched.control_spawn_guard();
            guard.mark_claimed();
        }
        assert_eq!(sched.control_spawned.load(Ordering::Acquire), 4);

        // Saturating: never underflows to u32::MAX.
        for _ in 0..6 {
            sched.vu_exited();
        }
        assert_eq!(sched.control_spawned.load(Ordering::Acquire), 0);
    }

    /// Locked (backlog line 170): the idle count must be restored when the
    /// VU task ends for any reason (abort/panic while waiting for a token),
    /// and `mark_busy` must saturate — a raw `fetch_sub` underflow to
    /// `u32::MAX` would make `grow_arrival_pool` see a huge idle count and
    /// disable pool growth for the whole run.
    #[tokio::test]
    async fn idle_guard_restores_count_and_mark_busy_saturates() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantArrivalRate {
            rate: 1.0,
            time_unit: "1s".to_string(),
            duration: "1s".to_string(),
            pre_alloc_vus: 1,
            max_vus: 10,
            graceful_stop: Some("1s".to_string()),
            think_time: Default::default(),
        });
        assert_eq!(sched.idle_vu_count(), 0);

        // Guard marks idle on creation and busy on drop.
        {
            let _guard = sched.idle_guard();
            assert_eq!(sched.idle_vu_count(), 1);
        }
        assert_eq!(
            sched.idle_vu_count(),
            0,
            "idle count must be restored on drop"
        );

        // mark_busy at 0 must saturate, not wrap to u32::MAX.
        sched.mark_busy();
        assert_eq!(sched.idle_vu_count(), 0, "mark_busy must saturate at zero");
        sched.mark_busy();
        assert_eq!(sched.idle_vu_count(), 0);
    }

    /// Locked (backlog line 170): the externally-controlled control loop
    /// must reap finished VU handles each tick — otherwise a long grow/shrink
    /// run accumulates completed JoinHandles unboundedly.
    #[tokio::test]
    async fn reap_finished_handles_keeps_only_live() {
        let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        // Two short-lived VU tasks that finish quickly.
        for _ in 0..2 {
            handles.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }));
        }
        // One long-lived VU task that outlives the reap.
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
        }));

        tokio::time::sleep(Duration::from_millis(20)).await;
        let before = handles.len();
        handles.retain(|h| !h.is_finished());
        assert_eq!(
            handles.len(),
            1,
            "finished handles must be reaped ({before} → {})",
            handles.len()
        );

        // The remaining live handle still completes normally.
        handles.remove(0).await.unwrap();
    }

    /// Fake VU body for the arrival-rate pool tests: marks itself idle, waits
    /// for an arrival token (or stop), then simulates `latency` of work per
    /// iteration — mirroring the real run_vu_loop arrival branch.
    fn arrival_test_vu(sched: Arc<VUScheduler>, latency: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let arrival_notify = sched.arrival_notify();
            let stop = sched.stop_signal();
            loop {
                if sched.is_stop_requested() || sched.is_force_stop_requested() {
                    break;
                }
                let mut got_token = false;
                {
                    // RAII idle — same guard the real loop uses, scoped to the
                    // token wait ONLY (it must drop before the simulated work,
                    // or the pool would see every VU as idle and never grow).
                    let _idle_guard = sched.idle_guard();
                    loop {
                        if sched.is_stop_requested() || sched.is_force_stop_requested() {
                            break;
                        }
                        if sched.try_acquire_arrival_token() {
                            got_token = true;
                            break;
                        }
                        tokio::select! {
                            _ = arrival_notify.notified() => {}
                            _ = stop.notified() => {}
                        }
                    }
                }
                if !got_token {
                    break;
                }
                // Simulated per-iteration latency.
                tokio::time::sleep(latency).await;
            }
        })
    }

    /// Locked: 20/s with 10 pre-allocated VUs at 300ms latency must NEVER
    /// drop iterations — 10 VUs can sustain ~33/s, so the pool easily keeps
    /// up. Guards against regressions in the token bucket / growth path.
    #[tokio::test]
    async fn arrival_rate_never_drops_with_10_vus_at_300ms() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantArrivalRate {
            rate: 20.0,
            time_unit: "1s".to_string(),
            duration: "5s".to_string(),
            pre_alloc_vus: 10,
            max_vus: 50,
            graceful_stop: Some("2s".to_string()),
            think_time: Default::default(),
        });
        let run_vu = |sched: Arc<VUScheduler>, _vu_id: u32| {
            arrival_test_vu(sched, Duration::from_millis(300))
        };
        sched
            .run_arrival_rate(
                20.0,
                10,
                50,
                Duration::from_secs(5),
                Duration::from_secs(2),
                &run_vu,
            )
            .await;
        let dropped = sched.take_dropped_iterations();
        assert_eq!(
            dropped, 0,
            "20/s with 10 pre-allocated VUs at 300ms latency must never drop"
        );
    }

    /// Locked: 100/s with only 10 pre-allocated VUs at 300ms latency — the
    /// pool MUST grow toward max_vus (~30 VUs needed) or the bucket saturates
    /// and iterations drop. The old growth code (gated on token-add, by at
    /// most `to_add`) stopped growing entirely once the bucket was full;
    /// queued-token-pressure growth must keep up.
    #[tokio::test]
    async fn arrival_rate_grows_pool_to_keep_up_with_latency() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantArrivalRate {
            rate: 100.0,
            time_unit: "1s".to_string(),
            duration: "5s".to_string(),
            pre_alloc_vus: 10,
            max_vus: 50,
            graceful_stop: Some("2s".to_string()),
            think_time: Default::default(),
        });
        let run_vu = |sched: Arc<VUScheduler>, _vu_id: u32| {
            arrival_test_vu(sched, Duration::from_millis(300))
        };
        sched
            .run_arrival_rate(
                100.0,
                10,
                50,
                Duration::from_secs(5),
                Duration::from_secs(2),
                &run_vu,
            )
            .await;
        let dropped = sched.take_dropped_iterations();
        assert_eq!(
            dropped, 0,
            "pool must grow (10 pre-alloc VUs × 300ms ≈ 33/s cap) so 100/s never drops"
        );
    }

    /// Locked: peak_vus() reports the PRE-ALLOCATED peak per executor type
    /// (k6 semantics for vus_max), not a sampled current active count.
    #[test]
    fn peak_vus_reports_preallocated_peak() {
        let sched = VUScheduler::new(&ExecutionConfig::ConstantVus {
            vus: 4,
            duration: "1s".to_string(),
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert_eq!(sched.peak_vus(), 4);

        let sched = VUScheduler::new(&ExecutionConfig::RampingVus {
            start_vus: 2,
            stages: vec![
                tropel_core::config::Stage {
                    duration: "1s".to_string(),
                    target: 10,
                },
                tropel_core::config::Stage {
                    duration: "1s".to_string(),
                    target: 5,
                },
            ],
            graceful_ramp_down: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert_eq!(sched.peak_vus(), 10); // max stage target, not start

        let sched = VUScheduler::new(&ExecutionConfig::ConstantArrivalRate {
            rate: 10.0,
            time_unit: "1s".to_string(),
            duration: "1s".to_string(),
            pre_alloc_vus: 2,
            max_vus: 50,
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert_eq!(sched.peak_vus(), 50); // max_vus, not pre_alloc

        let sched = VUScheduler::new(&ExecutionConfig::ExternallyControlled {
            vus: 3,
            max_vus: 20,
            duration: None,
            graceful_stop: None,
            think_time: Default::default(),
        });
        assert_eq!(sched.peak_vus(), 20);
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
