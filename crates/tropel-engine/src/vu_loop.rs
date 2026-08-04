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
use crate::worker::VUWorkerPool;
use async_trait::async_trait;
use rand::RngExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tropel_core::config::{ExecutionConfig, HttpConfig, ThinkTimeConfig, ThresholdConfig, TlsConfig};
use tropel_core::scenario::Scenario;
use tropel_core::types::{Request, Response, Sample, TagMap};
use tropel_core::{Result, TropelError};
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::{VUScheduler, VuLease};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, DriverHttpClient, DriverInstance, Protocol, VuContext};
use tropel_http::client::HttpClient;
use tropel_http::AuthSigner;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::check_abort_on_fail;
use tropel_pm::bridge::SharedPmState;

/// Outcome of one VU iteration, normalized across scenario runners and
/// driver instances so the shared loop can drive either.
struct VuIterationOutcome {
    samples: Vec<Sample>,
    abort_message: Option<String>,
}

/// A per-iteration execution source. The shared VU loop calls this once per
/// iteration; scenario runners and driver instances each implement it.
#[async_trait]
trait VuIterationSource: Send {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome;
}

// ── Scenario source: VURunner (Postman pm.* declarative execution) ──

struct ScenarioVuSource {
    runner: VURunner,
    pm_state: SharedPmState,
}

#[async_trait]
impl VuIterationSource for ScenarioVuSource {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome {
        let iter_result = self.runner.run_iteration(iteration_index, data_row, vu_env).await;
        let abort_message = {
            let state = self.pm_state.lock().unwrap();
            if state.abort_requested {
                Some(
                    state
                        .abort_message
                        .clone()
                        .unwrap_or_else(|| "Test aborted by script".to_string()),
                )
            } else {
                None
            }
        };
        VuIterationOutcome {
            samples: iter_result.samples,
            abort_message,
        }
    }
}

// ── Driver source: k6-style imperative driver instance ──

struct DriverVuSource {
    instance: Box<dyn DriverInstance>,
    http_client: Arc<dyn DriverHttpClient + Send + Sync>,
    executor_name: String,
    driver_id: String,
    vu_id: u32,
    sc_name: String,
    sched: Arc<VUScheduler>,
}

#[async_trait]
impl VuIterationSource for DriverVuSource {
    async fn run_iteration(
        &mut self,
        iteration_index: u64,
        data_row: Option<HashMap<String, serde_json::Value>>,
        vu_env: &HashMap<String, String>,
    ) -> VuIterationOutcome {
        let mut ctx = VuContext::new(self.vu_id, iteration_index, self.sc_name.clone());
        ctx.env = vu_env.clone();
        ctx.data_row = data_row;
        ctx.http_client = Some(self.http_client.clone());
        ctx.set_exec_context(
            self.executor_name.clone(),
            self.sched.total_iterations().await,
            self.sched.active_vus().await,
        );
        let result = self.instance.run_iteration(&mut ctx).await;
        if let Err(e) = result {
            tracing::warn!(
                "VU {} iteration {} failed: {}",
                self.vu_id,
                iteration_index,
                e
            );
        }
        let abort_message = if ctx.abort_requested {
            Some(format!(
                "Driver '{}' requested abort: {}",
                self.driver_id,
                ctx.abort_message
                    .clone()
                    .unwrap_or_else(|| "Test aborted by driver".to_string())
            ))
        } else {
            None
        };
        VuIterationOutcome {
            samples: std::mem::take(&mut ctx.samples),
            abort_message,
        }
    }
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
    let mut iteration_index = 0u64;
    let mut vu_sample_counter: u64 = 0;

    loop {
        if sched.is_force_stop_requested() || sched.is_stop_requested() {
            break;
        }
        {
            let active = sched.active_vus().await;
            if sched.try_claim_ramp_down(active).await {
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
        if !shared.is_per_vu_iterations && shared.total_iterations != u64::MAX {
            if !sched.try_claim_shared_iteration(shared.total_iterations) {
                break;
            }
        }

        vu_sample_counter += 1;
        if vu_sample_counter % 100 == 0 {
            let active = sched.active_vus().await;
            let peak = sched.peak_vus();
            utils_emit_vus_metrics(&shared.metrics, active, peak, &shared.sc_tags).await;
        }

        let iter_start = Instant::now();

        // Arrival-rate mode: wait for an iteration token. The wait is ALSO
        // woken by the stop signal so an idle VU observes the level-triggered
        // stop flag and exits promptly.
        if sched.is_arrival_rate() {
            sched.mark_idle();
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
            sched.mark_busy();
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
                    shared.data_rows[(iteration_index as usize + vu_id as usize)
                        % shared.data_rows.len()]
                    .clone(),
                )
            };

            let iter_start_time = Instant::now();
            let outcome = source
                .run_iteration(iteration_index, data_row, &shared.vu_env)
                .await;
            let iter_dur = iter_start_time.elapsed();

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
                value: iter_dur.as_micros() as f64,
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

        if shared.total_iterations != u64::MAX {
            if shared.is_per_vu_iterations && iteration_index >= shared.total_iterations {
                break;
            }
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
) where
    F: Fn(Arc<VUScheduler>, u32, &VuRunShared) -> tokio::task::JoinHandle<()> + Send + Sync + 'static,
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

    let has_abort_thresholds = thresholds.values().any(|t| t.abort_on_fail);
    let is_per_vu_iterations = matches!(exec_cfg, ExecutionConfig::PerVUIterations { .. });
    let think_time_cfg = extract_think_time(&exec_cfg);
    // k6-style executor name (e.g. "constant-vus") — backs exec.scenario.executor().
    let executor_name = exec_cfg.executor_name().to_string();

    let abort_monitor = spawn_abort_coordinator(
        has_abort_thresholds,
        metrics.clone(),
        executor.control_handle(),
        thresholds.clone(),
        test_start,
    );

    let shared = VuRunShared {
        metrics: metrics.clone(),
        sc_tags: sc_tags.clone(),
        vu_env: vu_env.clone(),
        data_rows,
        total_iterations,
        is_per_vu_iterations,
        think_time: think_time_cfg,
        executor_name,
    };

    executor
        .run(move |sched, vu_id| run_vu(sched, vu_id, &shared))
        .await
        .ok();

    // Stop the single abort coordinator — the run has finished, so a
    // lingering 2s poller would otherwise keep the metrics aggregator alive.
    if let Some(monitor) = abort_monitor {
        monitor.abort();
    }

    // Emit a guaranteed final vus/vus_max sample. The periodic sampler only
    // fires every 100 iterations per VU, so a short run would otherwise emit
    // NO vus/vus_max samples and the summary would read vus_max: 0.
    utils_emit_vus_metrics(&metrics, 0, executor.peak_vus(), &sc_tags).await;

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
}

// ── Driver HTTP client adapter ──

struct DriverHttpClientImpl {
    client: HttpClient,
}

#[async_trait]
impl DriverHttpClient for DriverHttpClientImpl {
    async fn execute(&self, req: &Request) -> Result<Response> {
        let http_resp = self.client.execute(req, None::<&dyn AuthSigner>).await?;
        Ok(Response::from(&http_resp))
    }
}
// ── Scenario entry point ──

/// Run a declarative (Postman-style) scenario: each VU builds its own
/// HttpClient + VURunner + JS context and drives the shared loop through
/// [`ScenarioVuSource`].
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
) {
    let http_cfg_c = http_cfg.clone();
    let tls_cfg_c = tls_cfg.clone();
    let rps_limiter_c = rps_limiter.clone();
    let scenario_c = scenario.clone();
    let protocols_c = protocols.clone();
    let pool_c = pool.clone();
    let sc_name_c = sc_name.clone();

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
            let http_cfg = http_cfg_c.clone();
            let tls_cfg = tls_cfg_c.clone();
            let rps_vu = rps_limiter_c.clone();
            let scenario = scenario_c.clone();
            let protocols_vu = protocols_c.clone();
            let pool = pool_c.clone();
            let sc_name_vu = sc_name_c.clone();
            let executor_name = shared.executor_name.clone();

            // 1-VU-per-task: pin this VU to its own dedicated worker thread so
            // a blocking script `sleep()` (std::thread::sleep) never freezes a
            // co-located VU — there is no co-located VU.
            let handle = pool.spawn_vu(vu_id, async move {
                // Panic-safe lease: increments `active_vus` now and decrements
                // on drop — even if the task panics mid-flight, the counter
                // can't leak and the engine's drain loop can't hang forever.
                let _lease = VuLease::acquire(&sched);

                let client = match HttpClient::with_tls_and_rps(&http_cfg, &tls_cfg, rps_vu) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                        return;
                    }
                };
                let bridge_client = Arc::new(client.clone());
                let mut runner = VURunner::new(scenario, client, vu_id, sc_name_vu.clone())
                    .with_expected_statuses(http_cfg.expected_statuses.clone())
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

                let mut source = ScenarioVuSource {
                    runner,
                    pm_state,
                };
                run_vu_loop(sched, &shared, vu_id, &mut source).await;
            });
            handle
        },
    )
    .await;
}

// ── Driver entry point ──

/// Run an imperative (k6-style) driver: each VU re-resolves the driver from
/// the registry, inits a fresh instance, wraps its own HttpClient, and drives
/// the shared loop through [`DriverVuSource`].
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
) {
    let driver_id = driver.id().to_string();
    let input_bytes = match std::fs::read(input_path) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Scenario '{}': failed to read input: {}", sc_name, e);
            return;
        }
    };
    let input_p = std::path::Path::new(input_path).to_path_buf();

    let driver_id_c = driver_id.clone();
    let input_bytes_c = input_bytes.clone();
    let input_p_c = input_p.clone();
    let registry_c = registry.clone();
    let sc_exec_c = sc_exec.clone();
    let http_cfg_c = http_cfg.clone();
    let tls_cfg_c = tls_cfg.clone();
    let rps_limiter_c = rps_limiter.clone();
    let pool_c = pool.clone();
    let sc_name_c = sc_name.clone();

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
            let driver_id = driver_id_c.clone();
            let input_bytes = input_bytes_c.clone();
            let input_p = input_p_c.clone();
            let registry = registry_c.clone();
            let sc_exec = sc_exec_c.clone();
            let http_cfg = http_cfg_c.clone();
            let tls_cfg = tls_cfg_c.clone();
            let rps_vu = rps_limiter_c.clone();
            let pool = pool_c.clone();
            let sc_name_vu = sc_name_c.clone();
            let executor_name = shared.executor_name.clone();

            // 1-VU-per-task: pin this VU to its own dedicated worker thread (see
            // run_scenario_vus for the rationale — blocking sleep() must never
            // freeze a co-located VU).
            let handle = pool.spawn_vu(vu_id, async move {
                let _lease = VuLease::acquire(&sched);

                // Re-resolve driver from registry so each VU gets a fresh instance.
                let driver = match registry.resolve_driver_by_id(&driver_id) {
                    Some(d) => d,
                    None => {
                        tracing::error!("VU {}: Driver '{}' not found in registry", vu_id, driver_id);
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
                        return;
                    }
                };

                let client = match HttpClient::with_tls_and_rps(&http_cfg, &tls_cfg, rps_vu) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                        return;
                    }
                };
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
                };
                run_vu_loop(sched, &shared, vu_id, &mut source).await;
            });
            handle
        },
    )
    .await;
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
    has_abort_thresholds: bool,
    metrics: Arc<MetricsCollector>,
    sched: Arc<VUScheduler>,
    thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
    test_start: Instant,
) -> Option<tokio::task::JoinHandle<()>> {
    if !has_abort_thresholds {
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

fn extract_think_time(exec_cfg: &ExecutionConfig) -> ThinkTimeConfig {
    match exec_cfg {
        ExecutionConfig::ConstantVus { think_time, .. } => think_time.clone(),
        ExecutionConfig::RampingVus { think_time, .. } => think_time.clone(),
        ExecutionConfig::ConstantArrivalRate { think_time, .. } => think_time.clone(),
        ExecutionConfig::SharedIterations { think_time, .. } => think_time.clone(),
        ExecutionConfig::PerVUIterations { think_time, .. } => think_time.clone(),
        ExecutionConfig::RampingArrivalRate { think_time, .. } => think_time.clone(),
        ExecutionConfig::ExternallyControlled { think_time, .. } => think_time.clone(),
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

// ── Duration parsing (from old engine.rs) ──

pub(crate) fn parse_duration_str(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "0s" {
        return Ok(Duration::ZERO);
    }
    if let Some(num_str) = s.strip_suffix("ms") {
        let ms: u64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(num_str) = s.strip_suffix('s') {
        let secs: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    } else if let Some(num_str) = s.strip_suffix('m') {
        let mins: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(mins * 60.0))
    } else if let Some(num_str) = s.strip_suffix('h') {
        let hours: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(hours * 3600.0))
    } else {
        let secs: f64 = s
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    }
}

// ── Think time ──

async fn apply_think_time(config: &ThinkTimeConfig, iter_duration: Option<Duration>) {
    if let Some(pacing_str) = &config.iteration_pacing {
        if let Ok(pacing) = parse_duration_str(pacing_str) {
            if let Some(actual_dur) = iter_duration {
                if actual_dur < pacing {
                    let remaining = pacing - actual_dur;
                    if remaining > Duration::from_millis(1) {
                        tokio::time::sleep(remaining).await;
                    }
                }
            }
            return;
        }
    }

    if let Some(delay_str) = &config.delay {
        if let Ok(delay) = parse_duration_str(delay_str) {
            if delay > Duration::from_millis(1) {
                tokio::time::sleep(delay).await;
                return;
            }
        }
    }

    if let (Some(min_str), Some(max_str)) = (&config.min_delay, &config.max_delay) {
        if let (Ok(min), Ok(max)) = (parse_duration_str(min_str), parse_duration_str(max_str)) {
            if max > Duration::ZERO && max > min {
                let range_ms = (max - min).as_millis() as u64;
                let rand_ms = rand::rng().random_range(0..=range_ms);
                tokio::time::sleep(min + Duration::from_millis(rand_ms)).await;
            }
        }
    }
}

// ── JS context creation ──

