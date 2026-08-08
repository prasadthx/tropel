//! Unified VU execution loop.
//!
//! `run_scenario_vus` (declarative Postman-style scenarios) and
//! `run_driver_vus` (imperative k6-style drivers) previously duplicated
//! ~80% of their code: scheduler setup, control API, abort coordinator,
//! stop/ramp-down/pause/arrival-token gating, pacing, and post-run teardown.
//! This module collapses both into one generic scaffolding ([`run_vus`]) plus
//! one shared per-VU iteration loop ([`run_vu_loop`]), parameterized by a
//! per-iteration source ([`VuIterationSource`]).

use crate::js_bootstrap::create_vu_js_context;
use crate::pacing::{apply_think_time, extract_think_time};
use crate::vu_sources::{DriverVuSource, ScenarioVuSource};
use crate::worker::VUWorkerPool;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tropel_core::config::{
    ExecutionConfig, HttpConfig, ThinkTimeConfig, ThresholdConfig, TlsConfig,
};
use tropel_core::scenario::{Scenario, ScenarioItem};
use tropel_core::types::{Request, Response, Sample, TagMap};
use tropel_core::Result;
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::{VUScheduler, VuLease};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, DriverHttpClient, Protocol};
use tropel_http::client::HttpClient;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::{check_abort_on_fail, evaluate_thresholds};

/// Outcome of one VU iteration, normalized across scenario runners and
/// driver instances so the shared loop can drive either.
pub(crate) struct VuIterationOutcome {
    pub(crate) samples: Vec<Sample>,
    pub(crate) abort_message: Option<String>,
    /// Number of script executions (prerequest/test/driver iteration) that
    /// errored this iteration. Aggregated into the run-wide counter so a run
    /// where scripts keep throwing exits non-zero (backlog line 98). `u64`
    /// matches the run-wide counter type (no cast noise).
    pub(crate) script_failures: u64,
}

/// A per-iteration execution source. The shared VU loop calls this once per
/// iteration; scenario runners and driver instances each implement it.
#[async_trait]
pub(crate) trait VuIterationSource: Send {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome;
}

// ── Shared per-VU iteration loop ──

/// The shared VU iteration loop, identical for scenarios and drivers.
/// Stop/ramp-down/pause gating, shared-iteration pre-claim, vus sampling,
/// arrival-rate token waits, iteration metrics, pacing, and the
/// per-VU-iterations budget all live here once.
async fn run_vu_loop(
    sched: Arc<VUScheduler>,
    shared: &VuRunShared,
    vu_id: u32,
    source: &mut dyn VuIterationSource,
) {
    // RAII: whenever this VU task exits for ANY reason (stop / force-stop /
    // ramp-down / budget exhausted / panic / abort), keep the externally-
    // controlled spawn count in sync so the control loop re-spawns to
    // target. A successful ramp-down claim already decrements inside
    // `try_claim_ramp_down`, so that path marks the guard claimed. Harmless
    // in non-externally-controlled modes (counter stays 0, saturating).
    let mut exit_guard = sched.control_spawn_guard();

    let mut iteration_index = 0u64;
    let mut vu_sample_counter: u64 = 0;
    // Time-based VU gauge cadence: vus/vus_max are ALSO sampled every ~2s
    // (not only every 100 iterations), so the live progress bar's VU field
    // is real even on short runs that never reach the iteration threshold.
    let mut last_vu_gauge = Instant::now();

    loop {
        if sched.is_force_stop_requested() || sched.is_stop_requested() {
            break;
        }
        {
            let active = sched.active_vus().await;
            if sched.try_claim_ramp_down(active).await {
                // The claim already decremented control_spawned — don't let
                // the exit guard double-decrement.
                exit_guard.mark_claimed();
                break;
            }
        }

        // Externally-controlled pause gate: level-triggered — the loop
        // re-checks is_paused each wake, so an edge-triggered resume notify
        // can't be missed.
        while sched.is_paused() && !sched.is_stop_requested() && !sched.is_force_stop_requested() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        if sched.is_stop_requested() || sched.is_force_stop_requested() {
            break;
        }

        // Shared-iterations mode: PRE-CLAIM this iteration slot atomically
        // (lock-free CAS) so concurrent VUs can never overshoot the budget.
        if !shared.is_per_vu_iterations
            && shared.total_iterations != u64::MAX
            && !sched.try_claim_shared_iteration(shared.total_iterations)
        {
            break;
        }

        vu_sample_counter += 1;
        let vu_gauge_due =
            vu_sample_counter % 100 == 0 || last_vu_gauge.elapsed() >= Duration::from_secs(2);
        if vu_gauge_due {
            let active = sched.active_vus().await;
            let peak = sched.peak_vus();
            shared
                .last_active_vus
                .store(active, std::sync::atomic::Ordering::Relaxed);
            utils_emit_vus_metrics(&shared.metrics, active, peak, &shared.sc_tags).await;
            last_vu_gauge = Instant::now();
        }

        let iter_start = Instant::now();

        // Arrival-rate mode: wait for an iteration token. The wait is ALSO
        // woken by the stop signal so an idle VU observes the level-triggered
        // stop flag and exits promptly.
        if sched.is_arrival_rate() {
            // RAII idle: restored to busy on drop, so an aborted/panicking
            // VU can't leak the idle count (a leaked count looks like "pool
            // has spare VUs" and permanently disables arrival-pool growth).
            let _idle_guard = sched.idle_guard();
            let arrival_notify = sched.arrival_notify();
            let stop = sched.stop_signal();
            let mut got_token = false;
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
            if !got_token {
                break;
            }
        }

        // Run ONE full iteration to completion. Deliberately no
        // `stop.notified()` select here — gracefulStop must DRAIN in-flight
        // iterations, not cancel them.
        {
            let data_row = if shared.data_rows.is_empty() {
                None
            } else {
                Some(
                    shared.data_rows
                        [(iteration_index as usize + vu_id as usize) % shared.data_rows.len()]
                    .clone(),
                )
            };

            let iter_start_time = Instant::now();
            let outcome = source
                .run_iteration(iteration_index, data_row, &shared.vu_env)
                .await;
            let iter_dur = iter_start_time.elapsed();

            // Backlog line 98: aggregate per-iteration script failures into
            // the run-wide counter so the CLI can exit non-zero when scripts
            // keep erroring (the failure is also visible as a failed check
            // sample in `outcome.samples`).
            if outcome.script_failures > 0 {
                shared
                    .script_failures
                    .fetch_add(outcome.script_failures, Ordering::SeqCst);
            }

            let now = std::time::SystemTime::now();
            let empty_tags = Arc::new(TagMap::new());
            let mut iter_samples = outcome.samples;
            iter_samples.push(Sample {
                metric: "iterations".into(),
                value: 1.0,
                tags: empty_tags.clone(),
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Counter,
            });
            iter_samples.push(Sample {
                metric: "iteration_duration".into(),
                value: iter_dur.as_secs_f64() * 1000.0,
                tags: empty_tags,
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Trend,
            });
            // Merge per-scenario tags into every sample so tag-scoped
            // thresholds (e.g. {scenario=load}) work end-to-end.
            merge_scenario_tags(&mut iter_samples, &shared.sc_tags);
            shared.metrics.record_batch(&iter_samples).await;

            if let Some(msg) = outcome.abort_message {
                tracing::warn!("test.abort(): {} — stopping", msg);
                sched.request_stop();
            }

            sched.increment_iterations().await;
        }
        iteration_index += 1;

        // Skip pacing when stop/force-stop is already requested — a drained
        // VU should exit promptly instead of sleeping out a full pacing
        // period during graceful shutdown.
        if !sched.is_arrival_rate()
            && !sched.is_stop_requested()
            && !sched.is_force_stop_requested()
        {
            apply_think_time(&shared.think_time, Some(iter_start.elapsed())).await;
        }

        if shared.total_iterations != u64::MAX
            && shared.is_per_vu_iterations
            && iteration_index >= shared.total_iterations
        {
            break;
        }
    }
}
// ── Shared VU-run scaffolding ──

/// Shared per-run parameters threaded into every VU task.
#[derive(Clone)]
struct VuRunShared {
    metrics: Arc<MetricsCollector>,
    sc_tags: HashMap<String, String>,
    vu_env: HashMap<String, String>,
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,
    total_iterations: u64,
    is_per_vu_iterations: bool,
    think_time: ThinkTimeConfig,
    executor_name: String,
    /// Count of VUs that failed to START (driver init / HTTP client creation).
    /// Incremented inside the per-VU task before it bails; read after the
    /// executor run so a silently-truncated VU count (e.g. WASM pool
    /// exhaustion) becomes a LOUD error instead of the summary reporting the
    /// requested count.
    vu_init_failures: Arc<AtomicU32>,
    /// Run-wide count of script executions that errored (prerequest/test
    /// scripts and driver iterations). Aggregated from every VU's
    /// [`VuIterationOutcome::script_failures`] so a run where scripts keep
    /// throwing exits non-zero instead of reporting success (backlog line 98).
    script_failures: Arc<AtomicU64>,
    /// Last sampled active VU count (Gauge). The final post-run vus sample
    /// uses this instead of a hardcoded 0 so short runs still report the
    /// real last-known concurrency (backlog line 154).
    last_active_vus: Arc<AtomicU32>,
}

/// Shared VU-run scaffolding used by both the scenario and driver paths:
/// start-delay, scheduler + control API wiring, abort coordinator, the
/// `executor.run(...)` fan-out, and the post-run teardown (abort monitor,
/// final vus/vus_max sample, dropped iterations, bounded drain, control API
/// shutdown). The only difference between the two callers is the per-VU task
/// body (`run_vu`), which the generic parameter provides.
#[allow(clippy::too_many_arguments)]
async fn run_vus<F>(
    sc_name: String,
    start_delay: Duration,
    sc_env: HashMap<String, String>,
    sc_tags: HashMap<String, String>,
    base_env: HashMap<String, String>,
    exec_cfg: ExecutionConfig,
    metrics: Arc<MetricsCollector>,
    thresholds: HashMap<String, ThresholdConfig>,
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,
    test_start: Instant,
    control_port: Option<u16>,
    run_vu: F,
) -> (u32, u64)
where
    F: Fn(Arc<VUScheduler>, u32, &VuRunShared) -> tokio::task::JoinHandle<()>
        + Send
        + Sync
        + 'static,
{
    if start_delay > Duration::ZERO {
        tokio::time::sleep(start_delay).await;
        tracing::info!(
            "Scenario '{}' started after {:?} delay",
            sc_name,
            start_delay
        );
    }

    let mut vu_env = base_env;
    vu_env.extend(sc_env);

    let executor = VUScheduler::new(&exec_cfg);

    // Runtime control API (k6 /v1/status parity): when the executor is
    // externally-controlled and a control port is configured, serve the
    // endpoint so VUs can be scaled mid-run. The task aborts when the
    // scenario finishes (we hold the handle below).
    let control_server = if matches!(exec_cfg, ExecutionConfig::ExternallyControlled { .. }) {
        control_port.map(|port| {
            let sched_handle = executor.control_handle();
            tokio::spawn(crate::control_api::serve_control_api(port, sched_handle))
        })
    } else {
        None
    };

    let total_iterations = match &exec_cfg {
        ExecutionConfig::SharedIterations { iterations, .. } => *iterations,
        ExecutionConfig::PerVUIterations { iterations, .. } => *iterations,
        _ => u64::MAX,
    };

    let is_per_vu_iterations = matches!(exec_cfg, ExecutionConfig::PerVUIterations { .. });
    let think_time_cfg = extract_think_time(&exec_cfg);
    // k6-style executor name (e.g. "constant-vus") — backs exec.scenario.executor().
    let executor_name = exec_cfg.executor_name().to_string();

    let abort_monitor = spawn_abort_coordinator(
        metrics.clone(),
        executor.control_handle(),
        thresholds.clone(),
        test_start,
    );

    let vu_init_failures = Arc::new(AtomicU32::new(0));
    let script_failures = Arc::new(AtomicU64::new(0));
    let last_active_vus = Arc::new(AtomicU32::new(0));
    let shared = VuRunShared {
        metrics: metrics.clone(),
        sc_tags: sc_tags.clone(),
        vu_env: vu_env.clone(),
        data_rows,
        total_iterations,
        is_per_vu_iterations,
        think_time: think_time_cfg,
        executor_name,
        vu_init_failures: vu_init_failures.clone(),
        script_failures: script_failures.clone(),
        last_active_vus: last_active_vus.clone(),
    };

    executor
        .run(move |sched, vu_id| run_vu(sched, vu_id, &shared))
        .await
        .ok();

    // A VU that failed to START (driver init / client creation) means the
    // requested load was NOT delivered — surfacing the count loudly here (and
    // up through run_scenario_vus/run_driver_vus to the engine, which fails
    // the run) turns silent truncation into an explicit failure.
    let init_failures = vu_init_failures.load(Ordering::SeqCst);
    if init_failures > 0 {
        tracing::error!(
            "Scenario '{}': {} VU(s) failed to start — run did not deliver the requested load",
            sc_name,
            init_failures
        );
    }

    // Stop the single abort coordinator — the run has finished, so a
    // lingering 2s poller would otherwise keep the metrics aggregator alive.
    if let Some(monitor) = abort_monitor {
        monitor.abort();
    }

    // Emit a guaranteed final vus/vus_max sample. The periodic sampler only
    // fires every 100 iterations per VU, so a short run would otherwise emit
    // NO vus/vus_max samples and the summary would read vus_max: 0. The
    // active count uses the LAST KNOWN sampled value (not 0) so the vus
    // gauge's `last` reflects real concurrency (backlog line 154).
    let final_active = last_active_vus.load(std::sync::atomic::Ordering::SeqCst);
    utils_emit_vus_metrics(&metrics, final_active, executor.peak_vus(), &sc_tags).await;

    // Record dropped iterations (carries the scenario tags like every other
    // sample this scenario emits).
    {
        let dropped = executor.take_dropped_iterations();
        if dropped > 0 {
            let mut dropped_tags = TagMap::new();
            for (k, v) in &sc_tags {
                dropped_tags.insert(k.clone(), v.clone());
            }
            metrics
                .record(&Sample {
                    metric: "dropped_iterations".into(),
                    value: dropped as f64,
                    tags: Arc::new(dropped_tags),
                    timestamp: std::time::SystemTime::now(),
                    sample_type: tropel_core::types::SampleType::Counter,
                })
                .await;
        }
    }

    // Bound the drain: a panicked VU (leaked `active_vus`) or timeout-less I/O
    // must not hang the run forever. Wait up to 30s for stragglers, then warn
    // and proceed to shutdown.
    let drain_deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    loop {
        let active = executor.active_vus().await;
        if active == 0 {
            break;
        }
        if tokio::time::Instant::now() >= drain_deadline {
            tracing::warn!(
                "VU drain timed out after 30s ({} VU(s) still active) — proceeding to shutdown",
                active
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Shut down the control API now that the scenario is over.
    if let Some(handle) = control_server {
        handle.abort();
    }
    (init_failures, script_failures.load(Ordering::SeqCst))
}

// ── Driver HTTP client adapter ──

struct DriverHttpClientImpl {
    client: HttpClient,
}

#[async_trait]
impl DriverHttpClient for DriverHttpClientImpl {
    async fn execute(&self, req: &Request) -> Result<Response> {
        // Backlog line 140: honor Request.auth (k6 params.auth). Build the
        // signer from the per-request config so bearer/basic/oauth2/sigv4/
        // digest on ONE request don't need the whole scenario to share it.
        let signer = req.auth.as_ref().and_then(|a| self.client.get_signer(a));
        let http_resp = self.client.execute(req, signer.as_deref()).await?;
        Ok(Response::from(&http_resp))
    }
}
// ── Scenario entry point ──

/// Run a declarative (Postman-style) scenario: each VU builds its own
/// HttpClient + VURunner + JS context and drives the shared loop through
/// [`ScenarioVuSource`]. Returns `(vu_init_failures, script_failures)` —
/// VUs that failed to START (so the engine can fail the run loudly when the
/// requested load is not delivered) and script executions that errored (so a
/// run where every script throws exits non-zero; backlog line 98).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_scenario_vus(
    sc_name: String,
    start_delay: Duration,
    sc_env: HashMap<String, String>,
    sc_tags: HashMap<String, String>,
    base_env: HashMap<String, String>,
    exec_cfg: ExecutionConfig,
    scenario: Arc<Scenario>,
    metrics: Arc<MetricsCollector>,
    pool: Arc<VUWorkerPool>,
    http_cfg: HttpConfig,
    tls_cfg: TlsConfig,
    thresholds: HashMap<String, ThresholdConfig>,
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,
    test_start: Instant,
    protocols: Arc<HashMap<String, Arc<dyn Protocol>>>,
    control_port: Option<u16>,
    rps_limiter: Option<Arc<tropel_http::RpsLimiter>>,
) -> (u32, u64) {
    // Expected statuses are read by every VU's VURunner — snapshot them once
    // and share (the closure no longer captures the whole HttpConfig, which
    // is consumed into the shared client build below).
    let expected_statuses_c = http_cfg.expected_statuses.clone();
    let scenario_c = scenario.clone();
    let protocols_c = protocols.clone();
    let pool_c = pool.clone();
    let sc_name_c = sc_name.clone();

    // Build ONE HttpClient per scenario and share it (Arc) across every VU.
    // HttpClient::with_tls_and_rps constructs TWO reqwest::Clients (primary +
    // no-redirect twin), each with its own connection pool, DNS resolver and
    // TLS context — ~2-3 fds and significant state before a socket even
    // opens. reqwest::Client is an Arc-backed handle designed for concurrent
    // use, so a per-VU build duplicated all of that VU-times for zero benefit
    // (the old per-VU `HttpClient::with_tls_and_rps` in the spawn closure).
    // The per-VU cost is now a cheap struct clone (Arc bumps + small config
    // snapshots) sharing the pooled clients. Thread-per-VU itself is
    // deliberate (a blocking script `sleep()` must never freeze a co-located
    // VU — there is no co-located VU), so this cuts the client/RSS/fd part of
    // the per-VU cost without touching the scheduling model.
    let shared_client = match HttpClient::with_tls_and_rps(&http_cfg, &tls_cfg, rps_limiter.clone())
    {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(
                "Scenario '{}': Failed to create shared HTTP client: {}",
                sc_name,
                e
            );
            // Signal a VU-init failure so the engine fails the run loudly
            // instead of treating this as a clean zero-work run.
            return (1, 0);
        }
    };

    // Pre-flatten the item tree ONCE per scenario and share the Arcs with
    // every VU — a large collection must not be re-flattened/re-cloned per
    // VU at runner construction (VURunner::new). Request names for
    // setNextRequest are derived from the same flatten and shared too.
    let flattened_c: Arc<Vec<ScenarioItem>> = Arc::new(
        tropel_executor::runner::flatten_execution_items(&scenario.items),
    );
    let names_c: Arc<Vec<String>> =
        Arc::new(flattened_c.iter().map(|item| item.name.clone()).collect());

    run_vus(
        sc_name,
        start_delay,
        sc_env,
        sc_tags,
        base_env,
        exec_cfg,
        metrics,
        thresholds,
        data_rows,
        test_start,
        control_port,
        move |sched, vu_id, shared| {
            let shared = shared.clone();
            let http_client_vu = shared_client.clone();
            let scenario = scenario_c.clone();
            let protocols_vu = protocols_c.clone();
            let pool = pool_c.clone();
            let sc_name_vu = sc_name_c.clone();
            let executor_name = shared.executor_name.clone();
            // Per-VU Arc bumps (cheap) so the Fn closure isn't moved out of.
            let flattened_vu = flattened_c.clone();
            let names_vu = names_c.clone();
            let expected_statuses_vu = expected_statuses_c.clone();

            // 1-VU-per-task: pin this VU to its own dedicated worker thread so
            // a blocking script `sleep()` (std::thread::sleep) never freezes a
            // co-located VU — there is no co-located VU.
            let handle = pool.spawn_vu(vu_id, async move {
                // Panic-safe lease: increments `active_vus` now and decrements
                // on drop — even if the task panics mid-flight, the counter
                // can't leak and the engine's drain loop can't hang forever.
                let _lease = VuLease::acquire(&sched);

                // Cheap struct clone of the shared client (Arc bumps + small
                // config snapshots) — the pooled reqwest Clients are shared.
                let client = http_client_vu.as_ref().clone();
                let bridge_client = Arc::new(client.clone());
                let mut runner = VURunner::new(
                    scenario,
                    flattened_vu,
                    names_vu,
                    client,
                    vu_id,
                    sc_name_vu.clone(),
                )
                .with_expected_statuses(expected_statuses_vu)
                .with_protocols(protocols_vu.clone())
                .with_exec_context(
                    executor_name,
                    sched.active_vus_handle(),
                    sched.total_iterations_handle(),
                );
                let pm_state = runner.state_handle();

                let js_ctx = create_vu_js_context(vu_id, &pm_state, &bridge_client).await;
                if let Some(ctx) = js_ctx {
                    runner = runner.with_js_context(Box::new(ctx));
                }

                let mut source = ScenarioVuSource { runner, pm_state };
                run_vu_loop(sched, &shared, vu_id, &mut source).await;
            });
            handle
        },
    )
    .await
}

// ── Driver entry point ──

/// Run an imperative (k6-style) driver: each VU re-resolves the driver from
/// the registry, inits a fresh instance, wraps its own HttpClient, and drives
/// the shared loop through [`DriverVuSource`]. Returns
/// `(vu_init_failures, script_failures)` — VUs that failed to START and
/// driver iterations that errored (backlog line 98).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_driver_vus(
    sc_name: String,
    start_delay: Duration,
    sc_env: HashMap<String, String>,
    sc_tags: HashMap<String, String>,
    base_env: HashMap<String, String>,
    exec_cfg: ExecutionConfig,
    sc_exec: Option<String>,
    driver: Box<dyn Driver>,
    metrics: Arc<MetricsCollector>,
    pool: Arc<VUWorkerPool>,
    http_cfg: HttpConfig,
    tls_cfg: TlsConfig,
    thresholds: HashMap<String, ThresholdConfig>,
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,
    test_start: Instant,
    input_path: &str,
    registry: Arc<ExtensionRegistry>,
    control_port: Option<u16>,
    rps_limiter: Option<Arc<tropel_http::RpsLimiter>>,
    // Protocols instantiated from the registry ONCE per scenario (backlog
    // line 230): run_driver_vus used to take NO protocols map, so a k6 or
    // WASM script could never reach a third-party protocol — only the
    // declarative path got the registry-driven scheme dispatch. Thread the
    // shared map into every VU's VuContext so drivers dispatch non-HTTP
    // schemes through the same lookup the declarative runner uses.
    protocols: Arc<HashMap<String, Arc<dyn Protocol>>>,
) -> (u32, u64) {
    let driver_id = driver.id().to_string();
    let input_bytes = match std::fs::read(input_path) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Scenario '{}': failed to read input: {}", sc_name, e);
            return (0, 0);
        }
    };
    let input_p = std::path::Path::new(input_path).to_path_buf();

    let driver_id_c = driver_id.clone();
    let input_bytes_c = input_bytes.clone();
    let input_p_c = input_p.clone();
    let registry_c = registry.clone();
    let sc_exec_c = sc_exec.clone();
    let pool_c = pool.clone();
    let sc_name_c = sc_name.clone();
    let protocols_c = protocols.clone();

    // Build ONE HttpClient per scenario and share it (Arc) across every VU
    // (same rationale as run_scenario_vus): the two reqwest::Clients inside
    // it carry connection pools / DNS resolver / TLS context — Arc-backed
    // handles designed for concurrent use, so per-VU construction would
    // duplicate all of that VU-times. Per-VU cost is now a cheap struct clone.
    let shared_client = match HttpClient::with_tls_and_rps(&http_cfg, &tls_cfg, rps_limiter.clone())
    {
        Ok(c) => Arc::new(c),
        Err(e) => {
            tracing::error!(
                "Scenario '{}': Failed to create shared HTTP client: {}",
                sc_name,
                e
            );
            // Signal a VU-init failure so the engine fails the run loudly
            // instead of treating this as a clean zero-work run.
            return (1, 0);
        }
    };

    // k6 lifecycle: run the script's `setup()` ONCE per scenario, BEFORE any
    // VU spawns. The serialized return value is threaded into every VU's
    // context (so `export default function (data)` receives it) and later
    // passed to `teardown(data)`. `None` when the script declares no setup —
    // VUs then see `undefined` data, matching k6. The env passed to setup is
    // the same merged env the VUs run with (base + scenario overrides).
    let mut setup_env = base_env.clone();
    setup_env.extend(sc_env.clone());
    let setup_data = driver.setup(&input_bytes, Some(&input_p), &setup_env).await;
    let setup_data_c = setup_data.clone();

    let run_vus_result = run_vus(
        sc_name,
        start_delay,
        sc_env,
        sc_tags,
        base_env,
        exec_cfg,
        metrics,
        thresholds,
        data_rows,
        test_start,
        control_port,
        move |sched, vu_id, shared| {
            let shared = shared.clone();
            let driver_id = driver_id_c.clone();
            let input_bytes = input_bytes_c.clone();
            let input_p = input_p_c.clone();
            let registry = registry_c.clone();
            let sc_exec = sc_exec_c.clone();
            let http_client_vu = shared_client.clone();
            let pool = pool_c.clone();
            let sc_name_vu = sc_name_c.clone();
            let executor_name = shared.executor_name.clone();
            // The setup data is cloned into the async block (the outer Fn
            // closure must not be moved out of — same pattern as every other
            // capture above).
            let setup_data_vu = setup_data_c.clone();
            let protocols_vu = protocols_c.clone();

            // 1-VU-per-task: pin this VU to its own dedicated worker thread (see
            // run_scenario_vus for the rationale — blocking sleep() must never
            // freeze a co-located VU).
            let handle = pool.spawn_vu(vu_id, async move {
                let _lease = VuLease::acquire(&sched);

                // Re-resolve driver from registry so each VU gets a fresh instance.
                let driver = match registry.resolve_driver_by_id(&driver_id) {
                    Some(d) => d,
                    None => {
                        tracing::error!(
                            "VU {}: Driver '{}' not found in registry",
                            vu_id,
                            driver_id
                        );
                        shared.vu_init_failures.fetch_add(1, Ordering::SeqCst);
                        // This VU bails BEFORE `run_vu_loop` creates its
                        // ControlSpawnGuard — decrement here so the
                        // externally-controlled pool count can't go stale and
                        // stall re-spawn (backlog line 170).
                        sched.vu_exited();
                        return;
                    }
                };

                let driver_instance = match driver
                    .init(&input_bytes, Some(&input_p), sc_exec.as_deref())
                    .await
                {
                    Ok(inst) => inst,
                    Err(e) => {
                        tracing::error!(
                            "Scenario '{}' VU {}: Driver '{}' init failed: {}",
                            sc_name_vu,
                            vu_id,
                            driver_id,
                            e
                        );
                        shared.vu_init_failures.fetch_add(1, Ordering::SeqCst);
                        // Same accounting as the not-found bail above — the
                        // ControlSpawnGuard hasn't been created yet.
                        sched.vu_exited();
                        return;
                    }
                };

                // Cheap struct clone of the shared client (Arc bumps + small
                // config snapshots) — the pooled reqwest Clients are shared.
                let client = http_client_vu.as_ref().clone();
                let http_client_handle: Arc<dyn DriverHttpClient + Send + Sync> =
                    Arc::new(DriverHttpClientImpl { client });

                let mut source = DriverVuSource {
                    instance: driver_instance,
                    http_client: http_client_handle,
                    executor_name,
                    driver_id,
                    vu_id,
                    sc_name: sc_name_vu,
                    sched: sched.clone(),
                    env: shared.vu_env.clone(),
                    env_attached: false,
                    setup_data: setup_data_vu,
                    protocols: protocols_vu,
                };
                run_vu_loop(sched, &shared, vu_id, &mut source).await;
            });
            handle
        },
    )
    .await;

    // k6 lifecycle: run the script's `teardown(data)` ONCE after all VUs
    // finish, with the `setup()` return value as data. Failures are the
    // driver's to log (a throwing teardown never changes the run's exit
    // status — k6 parity).
    driver
        .teardown(
            &input_bytes,
            Some(&input_p),
            setup_data.as_deref(),
            &setup_env,
        )
        .await;

    // `run_vus` returns (vu_init_failures, script_failures) — VUs that
    // failed to START and driver iterations that errored. Propagate them so
    // the engine can fail the run loudly (silent truncation / throwing
    // scripts must not report green).
    let (init_failures, script_failures) = run_vus_result;
    (init_failures, script_failures)
}
// Helpers
// ══════════════════════════════════════════════════════════════════

/// Merge per-scenario tags into a batch of samples (k6 semantics: scenario
/// tags apply to every metric the scenario emits). Scenario tags win over a
/// sample's own tags on key collision.
/// Single abort-on-fail coordinator: instead of EVERY VU calling
/// `metrics.results()` (a full aggregate rebuild) at each 2s slot boundary
/// — the thundering herd — ONE task polls `results()` every 2s and requests
/// stop on the first breached abortOnFail threshold. VUs only observe the
/// level-triggered stop flag between iterations. Returns `None` when no
/// threshold aborts; the caller must `abort()` the returned handle once the
/// run has finished so the task doesn't keep the metrics aggregator alive.
fn spawn_abort_coordinator(
    metrics: Arc<MetricsCollector>,
    sched: Arc<VUScheduler>,
    thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
    test_start: Instant,
) -> Option<tokio::task::JoinHandle<()>> {
    if thresholds.is_empty() {
        return None;
    }
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(2));
        // Delay, not Burst: if a slow results() call makes us miss ticks,
        // DON'T fire them all back-to-back — that would recreate a
        // mini-herd of aggregate rebuilds (the very problem this fixes).
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // The first tick fires immediately; consume it so the first check
        // happens at ~2s (mirrors the old `elapsed > 1s` gate).
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if sched.is_stop_requested() || sched.is_force_stop_requested() {
                break;
            }
            let elapsed = test_start.elapsed();
            if elapsed > Duration::from_secs(1) {
                let results = metrics.results().await;
                // k6 `tainted`: ANY failed threshold (abortOnFail or not)
                // marks the run so the control API status doc reports
                // `tainted: true` (backlog line 154).
                for tr in evaluate_thresholds(&thresholds, &results) {
                    if !tr.passed {
                        sched.set_tainted();
                        break;
                    }
                }
                if check_abort_on_fail(&thresholds, &results, elapsed) {
                    sched.request_stop();
                    break;
                }
            }
        }
    }))
}

fn merge_scenario_tags(samples: &mut [Sample], tags: &HashMap<String, String>) {
    if tags.is_empty() {
        return;
    }
    for sample in samples.iter_mut() {
        for (k, v) in tags {
            // tags is Arc<TagMap> — mutate through make_mut (cheap here: the
            // fresh per-request Arc has refcount 1).
            Arc::make_mut(&mut sample.tags).insert(k.clone(), v.clone());
        }
    }
}

async fn utils_emit_vus_metrics(
    metrics: &MetricsCollector,
    active: u32,
    peak: u32,
    sc_tags: &HashMap<String, String>,
) {
    let now = std::time::SystemTime::now();
    let mut vus_tags = TagMap::new();
    // k6 tags vus/vus_max per scenario; carry the scenario tags along.
    for (k, v) in sc_tags {
        vus_tags.insert(k.clone(), v.clone());
    }
    let vus_tags = Arc::new(vus_tags);
    metrics
        .record_batch(&[
            Sample {
                metric: "vus".into(),
                // Current ACTIVE VU count, sampled over time (Gauge).
                value: active as f64,
                tags: vus_tags.clone(),
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Point,
            },
            Sample {
                metric: "vus_max".into(),
                // PRE-ALLOCATED peak from the execution config (k6 semantics)
                // — NOT the current active count, which understated the peak
                // whenever it was sampled between VU churn.
                value: peak as f64,
                tags: vus_tags,
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Point,
            },
        ])
        .await;
}
