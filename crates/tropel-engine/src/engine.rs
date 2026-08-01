use crate::worker::VUWorkerPool;
use async_trait::async_trait;
use rand::RngExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tropel_core::config::{
    ExecutionConfig, HttpConfig, JobConfig, OutputConfig, ScenarioConfig, ThinkTimeConfig, TlsConfig,
};
use tropel_core::scenario::Scenario;
use tropel_core::types::{Request, Response, Sample, TagMap};
use tropel_core::{Result, TropelError};
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::VUScheduler;
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, DriverHttpClient, VuContext};
use tropel_http::client::HttpClient;
use tropel_http::AuthSigner;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::check_abort_on_fail;
use tropel_report::{create_reporter, OtlpOutput, PrometheusRemoteWriteOutput, Reporter, StreamingStdoutOutput};

/// The engine orchestrates a complete load test job.
pub struct Engine {
    extension_registry: ExtensionRegistry,
}

impl Engine {
    pub fn new(registry: ExtensionRegistry) -> Self {
        Self {
            extension_registry: registry,
        }
    }

    pub async fn run(&self, config: &JobConfig) -> Result<EngineResult> {
        let registry = Arc::new(self.extension_registry.clone());
        let format_hint = config.input_type.clone();
        let metrics = Arc::new(MetricsCollector::new());

        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pool = Arc::new(VUWorkerPool::new(num_workers));
        tracing::info!(
            "VU worker pool: {} threads (available cores: {})",
            num_workers,
            num_workers
        );

        let http_config = config.http.clone();
        let tls_config = config.tls.clone();
        let mut thresholds = config.thresholds.clone();
        let data_rows = config.iteration_data.clone();
        let test_start = Instant::now();

        // Script-declared load profile (k6 `export const options`). Applied only
        // when the user did not set an explicit load profile — i.e. no
        // vus/duration/mode/stages/iterations CLI flags (or a config file that
        // marked execution_explicit). This is what makes a k6 script's own
        // vus/duration/stages/scenarios/thresholds drive the run instead of
        // being silently ignored.
        let mut declared_scenarios: Option<HashMap<String, ScenarioConfig>> = None;
        let mut declared_execution: Option<ExecutionConfig> = None;
        if !config.execution_explicit && config.scenarios.is_empty() {
            let input_path = std::path::Path::new(&config.input);
            let bytes = std::fs::read(&config.input).ok();
            if let (Some(bytes), Ok(ResolvedInput::Driver(driver))) = (
                bytes,
                resolve_input_or_driver(
                    &config.input,
                    config.input_type.as_deref(),
                    &registry,
                    &config.env,
                ),
            ) {
                if let Some(decl) = driver
                    .declared_options(&bytes, Some(input_path), &config.env)
                    .await
                {
                    // Merge script-declared thresholds (CLI/config keys win on
                    // collision — CLI keys are "threshold_N", so no clash).
                    for (k, v) in &decl.thresholds {
                        thresholds.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                    if let Some(scs) = decl.scenarios {
                        if !scs.is_empty() {
                            tracing::info!(
                                "Using script-declared scenarios: {}",
                                scs.keys().cloned().collect::<Vec<_>>().join(", ")
                            );
                            declared_scenarios = Some(scs);
                        }
                    } else if let Some(exec) = decl.execution {
                        tracing::info!("Using script-declared execution: {:?}", exec);
                        declared_execution = Some(exec);
                    }
                }
            }
        }

        // Streaming outputs
        let mut output_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let (sample_tx, _) = broadcast::channel::<Sample>(10_000);
        metrics.set_sample_sink(Some(sample_tx.clone()));

        let has_stdout = config.output.reporters.iter().any(|r| r == "stdout");
        if has_stdout {
            let rx = sample_tx.subscribe();
            let handle = StreamingStdoutOutput::spawn(rx);
            output_handles.push(handle);
        }
        // Prometheus remote-write and OTLP outputs (streaming, best-effort).
        if let Some(url) = &config.output.prometheus_remote_write_url {
            let rx = sample_tx.subscribe();
            let handle = PrometheusRemoteWriteOutput::spawn(rx, url.clone());
            output_handles.push(handle);
        }
        if let Some(endpoint) = &config.output.otlp_endpoint {
            let rx = sample_tx.subscribe();
            let handle = OtlpOutput::spawn(rx, endpoint.clone());
            output_handles.push(handle);
        }
        if output_handles.is_empty() {
            metrics.set_sample_sink(None);
        }        // Build scenario configs. Script-declared scenarios/execution (from a
        // k6 `export const options`) take precedence over the default profile
        // but not over explicit user config (execution_explicit check above).
        // Tuple: (name, execution, env, tags, start_delay, input_path).
        let scenario_configs: Vec<(
            String,
            ExecutionConfig,
            HashMap<String, String>,
            HashMap<String, String>,
            Duration,
            String,
            Option<String>,
        )> = if let Some(scs) = declared_scenarios {
            scs.iter()
                .map(|(name, sc)| {
                    let start_delay = parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                    let input_path = sc.input.clone().unwrap_or_else(|| config.input.clone());
                    (
                        name.clone(),
                        sc.execution.clone(),
                        sc.env.clone(),
                        sc.tags.clone(),
                        start_delay,
                        input_path,
                        sc.exec.clone(),
                    )
                })
                .collect()
        } else if !config.scenarios.is_empty() {
            config
                .scenarios
                .iter()
                .map(|(name, sc)| {
                    let start_delay = parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                    let input_path = sc.input.clone().unwrap_or_else(|| config.input.clone());
                    (
                        name.clone(),
                        sc.execution.clone(),
                        sc.env.clone(),
                        sc.tags.clone(),
                        start_delay,
                        input_path,
                        sc.exec.clone(),
                    )
                })
                .collect()
        } else {
            let exec = declared_execution.unwrap_or_else(|| config.execution.clone());
            vec![
                (
                    "default".to_string(),
                    exec,
                    HashMap::new(),
                    HashMap::new(),
                    Duration::ZERO,
                    config.input.clone(),
                    None,
                )
            ]
        };

        tracing::info!(
            "Starting Tropel load test: {} scenario(s)",
            scenario_configs.len()
        );
        let mut scenario_handles = Vec::new();

        for (scenario_name, exec_cfg, sc_env, sc_tags, start_delay, input_path, sc_exec) in
            &scenario_configs
        {
            let sc_name = scenario_name.clone();
            let exec_cfg = exec_cfg.clone();
            let sc_env = sc_env.clone();
            let sc_tags = sc_tags.clone();
            let input_path = input_path.clone();
            let sc_exec = sc_exec.clone();
            let start_delay = *start_delay;
            let metrics = metrics.clone();
            let pool = pool.clone();
            let http_cfg = http_config.clone();
            let tls_cfg = tls_config.clone();
            let thresholds = thresholds.clone();
            let data_rows = data_rows.clone();
            let base_env = config.env.clone();
            let test_start = test_start;
            let registry_sc = registry.clone();
            let fmt_hint = format_hint.clone();

            let handle = tokio::spawn(async move {
                let resolved = resolve_input_or_driver(
                    &input_path,
                    fmt_hint.as_deref(),
                    &registry_sc,
                    &base_env,
                );
                let resolved = match resolved {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::error!(
                            "Scenario '{}': failed to resolve input '{}': {}",
                            sc_name,
                            input_path,
                            e
                        );
                        return;
                    }
                };

                let sc_name_log = sc_name.clone();
                match resolved {
                    ResolvedInput::Scenario(scenario) => {
                        run_scenario_vus(
                            sc_name,
                            start_delay,
                            sc_env,
                            sc_tags,
                            base_env,
                            exec_cfg,
                            scenario,
                            metrics,
                            pool,
                            http_cfg,
                            tls_cfg,
                            thresholds,
                            data_rows,
                            test_start,
                        )
                        .await;
                    }
                    ResolvedInput::Driver(driver) => {
                        run_driver_vus(
                            sc_name,
                            start_delay,
                            sc_env,
                            sc_tags,
                            base_env,
                            exec_cfg,
                            sc_exec,
                            driver,
                            metrics,
                            pool,
                            http_cfg,
                            tls_cfg,
                            thresholds,
                            data_rows,
                            test_start,
                            &input_path,
                            registry_sc,
                        )
                        .await;
                    }
                }

                tracing::info!("Scenario '{}': completed", sc_name_log);
            });

            scenario_handles.push(handle);
        }

        for handle in scenario_handles {
            handle.await.ok();
        }

        // The broadcast channel only closes when ALL senders drop. The
        // collector drops its clone in `set_sample_sink(None)`, but our
        // local `sample_tx` was alive until the end of this function — so
        // the streaming output task never saw `RecvError::Closed` and the
        // `handle.await` below hung, in EVERY run (the 0-req runs merely
        // exposed it most visibly). Dropping it here terminates the
        // output tasks and lets the run finish.
        metrics.set_sample_sink(None);
        drop(sample_tx);
        for handle in output_handles {
            let _ = handle.await;
        }

        let results = metrics.results().await;
        let reporters = self.create_reporters(&config.output);
        for reporter in &reporters {
            reporter.report(&results).await?;
        }

        Ok(EngineResult {
            metrics: results,
            // The effective threshold set — CLI/config thresholds merged with
            // any script-declared (k6 options) thresholds. The CLI reports
            // against THIS set so k6 SLOs appear in the end-of-run summary,
            // not just mid-run abort checks.
            effective_thresholds: thresholds,
        })
    }

    fn create_reporters(&self, config: &OutputConfig) -> Vec<Box<dyn Reporter>> {
        let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();
        for name in &config.reporters {
            if let Some(reporter) = create_reporter(name) {
                reporters.push(reporter);
            } else if let Some(_ext) = self.extension_registry.get_output(name) {
                tracing::warn!(
                    "Extension reporter '{}' not yet supported in engine runner",
                    name
                );
            } else {
                tracing::warn!("Unknown reporter: {}", name);
            }
        }
        reporters
    }

    pub fn extension_registry(&self) -> &ExtensionRegistry {
        &self.extension_registry
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(ExtensionRegistry::new())
    }
}

// ══════════════════════════════════════════════════════════════════
// Result type
// ══════════════════════════════════════════════════════════════════

#[derive(Debug)]
pub struct EngineResult {
    pub metrics: tropel_metrics::collector::MetricsResult,
    /// The thresholds actually applied to the run: the job's own thresholds
    /// merged with any script-declared thresholds (e.g. from a k6
    /// `export const options`). Consumers should report/evaluate against this
    /// set rather than the raw `JobConfig.thresholds`.
    pub effective_thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
}

// ══════════════════════════════════════════════════════════════════
// Input resolution — Driver or Scenario dispatch
// ══════════════════════════════════════════════════════════════════

enum ResolvedInput {
    Scenario(Arc<Scenario>),
    Driver(Box<dyn Driver>),
}

fn resolve_input_or_driver(
    input_path: &str,
    format_hint: Option<&str>,
    registry: &ExtensionRegistry,
    base_env: &HashMap<String, String>,
) -> Result<ResolvedInput> {
    let input_p = std::path::Path::new(input_path);
    let bytes = std::fs::read(input_path)
        .map_err(|e| TropelError::Parse(format!("Failed to read '{}': {}", input_path, e)))?;

    // 1. Try drivers first
    let driver: Option<Box<dyn Driver>> = if let Some(fmt) = format_hint {
        registry.resolve_driver_by_id(fmt)
    } else {
        registry.resolve_driver(&bytes)
    };

    if let Some(driver) = driver {
        tracing::info!(
            "Input '{}' resolved by driver '{}'",
            input_path,
            driver.id()
        );
        return Ok(ResolvedInput::Driver(driver));
    }

    // 2. Fall back to input adapters
    let adapter: Box<dyn tropel_ext::traits::InputAdapter> = if let Some(fmt) = format_hint {
        registry.resolve_input_by_id(fmt).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Config(format!(
                "Unknown input format '{}'. Available formats: {}",
                fmt,
                available.join(", ")
            ))
        })?
    } else {
        registry.resolve_input(&bytes).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Parse(format!(
                "No input adapter recognized '{}'. Available adapters: {}",
                input_path,
                if available.is_empty() {
                    "(none registered — check build configuration)".to_string()
                } else {
                    available.join(", ")
                }
            ))
        })?
    };

    tracing::info!(
        "Input '{}' resolved by adapter '{}'",
        input_path,
        adapter.id()
    );

    let mut scenario = adapter.parse_with_path(&bytes, Some(input_p))?;
    for (key, val) in base_env {
        scenario
            .variables
            .entry(key.clone())
            .or_insert_with(|| serde_json::Value::String(val.clone()));
    }

    Ok(ResolvedInput::Scenario(Arc::new(scenario)))
}

// ══════════════════════════════════════════════════════════════════
// Scenario VU runner
// ══════════════════════════════════════════════════════════════════

async fn run_scenario_vus(
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
    thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
    data_rows: Vec<HashMap<String, serde_json::Value>>,
    test_start: Instant,
) {
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
    let stop_signal = executor.stop_signal();

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
    let metrics_c = metrics.clone();
    let scenario_c = scenario.clone();
    let stop_c = stop_signal.clone();
    let data_rows_c = data_rows.clone();
    let pool_c = pool.clone();
    let http_cfg_c = http_cfg.clone();
    let tls_cfg_c = tls_cfg.clone();
    let thresholds_c = thresholds.clone();
    let vu_env_c = vu_env.clone();
    let sc_name_c = sc_name.clone();
    let sc_tags_c = sc_tags.clone();

    executor.run(move |sched, vu_id| {
        let metrics = metrics_c.clone();
        let scenario = scenario_c.clone();
        let stop = stop_c.clone();
        let total_iters = total_iterations;
        let vu_env = vu_env_c.clone();
        let data_rows = data_rows_c.clone();
        let http_cfg = http_cfg_c.clone();
        let tls_cfg = tls_cfg_c.clone();
        let thresholds = thresholds_c.clone();
        let has_abort_thresholds = has_abort_thresholds;
        let pool = pool_c.clone();
        let think_time = think_time_cfg.clone();
        let sc_name_vu = sc_name_c.clone();
        let is_per_vu_iterations = is_per_vu_iterations;
        let sc_tags = sc_tags_c.clone();
        let executor_name = executor_name.clone();

        let (_, handle) = pool.spawn(async move {
            sched.add_active_vu(1).await;

            let client = match HttpClient::with_tls(&http_cfg, &tls_cfg) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                    sched.remove_active_vu(1).await;
                    return;
                }
            };

            let bridge_client = Arc::new(client.clone());
            let mut runner = VURunner::new(scenario, client, vu_id, sc_name_vu.clone())
                .with_expected_statuses(http_cfg.expected_statuses.clone())
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

            let mut iteration_index = 0u64;
            let mut vu_sample_counter: u64 = 0;

            loop {
                if sched.is_force_stop_requested() || sched.is_stop_requested() { break; }
                {
                    let active = sched.active_vus().await;
                    if sched.try_claim_ramp_down(active).await { break; }
                }

                vu_sample_counter += 1;
                if vu_sample_counter % 100 == 0 {
                    let active = sched.active_vus().await;
                    utils_emit_vus_metrics(&metrics, active, &sc_tags).await;
                }

                let iter_start = Instant::now();

                // Arrival-rate mode: wait for an iteration token. The wait is
                // ALSO woken by the stop signal so an idle VU observes the
                // level-triggered stop flag and exits promptly instead of
                // waiting forever for a token the ended run will never add.
                // The idle/busy marks are balanced on every exit path.
                if sched.is_arrival_rate() {
                    sched.mark_idle();
                    let arrival_notify = sched.arrival_notify();
                    let mut got_token = false;
                    loop {
                        if sched.is_stop_requested() || sched.is_force_stop_requested() { break; }
                        if sched.try_acquire_arrival_token() { got_token = true; break; }
                        tokio::select! {
                            _ = arrival_notify.notified() => {}
                            _ = stop.notified() => {}
                        }
                    }
                    sched.mark_busy();
                    if !got_token { break; }
                }

                // Run ONE full iteration to completion. Deliberately no
                // `stop.notified()` select here — gracefulStop must DRAIN
                // in-flight iterations (let them finish), not cancel them.
                // The level-triggered stop flag ends the VU between
                // iterations; the scheduler's wait_for_drain enforces the
                // grace window with a force-stop backstop.
                let result = async {
                    let data_row = if data_rows.is_empty() { None }
                        else { Some(data_rows[(iteration_index as usize + vu_id as usize) % data_rows.len()].clone()) };

                    let iter_start_time = Instant::now();
                    let iter_result = runner.run_iteration(iteration_index, data_row, &vu_env).await;
                    let iter_dur = iter_start_time.elapsed();
                    let mut iter_samples = iter_result.samples;
                    let now = std::time::SystemTime::now();
                    let empty_tags = TagMap::new();
                    iter_samples.push(Sample { metric: "iterations".into(), value: 1.0, tags: empty_tags.clone(), timestamp: now, sample_type: tropel_core::types::SampleType::Counter });
                    iter_samples.push(Sample { metric: "iteration_duration".into(), value: iter_dur.as_micros() as f64, tags: empty_tags, timestamp: now, sample_type: tropel_core::types::SampleType::Trend });
                    // Merge per-scenario tags into every sample so tag-scoped
                    // thresholds (e.g. {scenario=load}) work end-to-end.
                    merge_scenario_tags(&mut iter_samples, &sc_tags);
                    metrics.record_batch(&iter_samples).await;

                    if has_abort_thresholds {
                        let elapsed = test_start.elapsed();
                        if elapsed > Duration::from_secs(1) {
                            let slot = elapsed.as_secs() / 2;
                            let prev = (elapsed - Duration::from_millis(100)).as_secs() / 2;
                            if slot != prev {
                                let results = metrics.results().await;
                                if check_abort_on_fail(&thresholds, &results, elapsed) {
                                    sched.request_stop();
                                }
                            }
                        }
                    }

                    {
                        let state = pm_state.lock().unwrap();
                        if state.abort_requested {
                            let msg = state.abort_message.clone().unwrap_or_else(|| "Test aborted by script".to_string());
                            tracing::warn!("test.abort(): {} — stopping", msg);
                            drop(state);
                            sched.request_stop();
                        }
                    }

                    sched.increment_iterations().await;
                    iteration_index
                }.await;
                iteration_index = result + 1;

                // Skip pacing when stop/force-stop is already requested — a
                // drained VU should exit promptly instead of sleeping out a
                // full pacing period during graceful shutdown.
                if !sched.is_arrival_rate()
                    && !sched.is_stop_requested()
                    && !sched.is_force_stop_requested()
                {
                    apply_think_time(&think_time, Some(iter_start.elapsed())).await;
                }

                if total_iters != u64::MAX {
                    if is_per_vu_iterations {
                        if iteration_index >= total_iters { break; }
                    } else {
                        if sched.total_iterations().await >= total_iters { break; }
                    }
                }
            }

            sched.remove_active_vu(1).await;
        });
        handle
    }).await.ok();

    // Record dropped iterations (carries the scenario tags like every other
    // sample this scenario emits)
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
                    tags: dropped_tags,
                    timestamp: std::time::SystemTime::now(),
                    sample_type: tropel_core::types::SampleType::Counter,
                })
                .await;
        }
    }

    loop {
        let active = executor.active_vus().await;
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ══════════════════════════════════════════════════════════════════
// Driver VU runner
// ══════════════════════════════════════════════════════════════════

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

async fn run_driver_vus(
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
    thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
    data_rows: Vec<HashMap<String, serde_json::Value>>,
    test_start: Instant,
    input_path: &str,
    registry: Arc<ExtensionRegistry>,
) {
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
    let stop_signal = executor.stop_signal();
    let has_abort_thresholds = thresholds.values().any(|t| t.abort_on_fail);
    let think_time_cfg = extract_think_time(&exec_cfg);
    let driver_id = driver.id().to_string();

    let input_bytes = match std::fs::read(input_path) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!("Scenario '{}': failed to read input: {}", sc_name, e);
            return;
        }
    };
    let input_p = std::path::Path::new(input_path).to_path_buf();

    let total_iterations = match &exec_cfg {
        ExecutionConfig::SharedIterations { iterations, .. } => *iterations,
        ExecutionConfig::PerVUIterations { iterations, .. } => *iterations,
        _ => u64::MAX,
    };
    let is_per_vu_iterations = matches!(exec_cfg, ExecutionConfig::PerVUIterations { .. });

    let metrics_c = metrics.clone();
    let stop_c = stop_signal.clone();
    let data_rows_c = data_rows.clone();
    let pool_c = pool.clone();
    let http_cfg_c = http_cfg.clone();
    let tls_cfg_c = tls_cfg.clone();
    let thresholds_c = thresholds.clone();
    let vu_env_c = vu_env.clone();
    let sc_name_c = sc_name.clone();
    let sc_tags_c = sc_tags.clone();
    let sc_exec_c = sc_exec.clone();
    let driver_id_c = driver_id.clone();
    let input_bytes_c = input_bytes.clone();
    let input_p_c = input_p.clone();
    let registry_c = registry.clone();

    executor.run(move |sched, vu_id| {
        let metrics = metrics_c.clone();
        let stop = stop_c.clone();
        let total_iters = total_iterations;
        let vu_env = vu_env_c.clone();
        let data_rows = data_rows_c.clone();
        let http_cfg = http_cfg_c.clone();
        let tls_cfg = tls_cfg_c.clone();
        let thresholds = thresholds_c.clone();
        let has_abort_thresholds = has_abort_thresholds;
        let pool = pool_c.clone();
        let think_time = think_time_cfg.clone();
        let sc_name_vu = sc_name_c.clone();
        let is_per_vu_iterations = is_per_vu_iterations;
        let driver_id = driver_id_c.clone();
        let input_bytes = input_bytes_c.clone();
        let input_p = input_p_c.clone();
        let registry = registry_c.clone();
        let sc_tags = sc_tags_c.clone();
        let sc_exec = sc_exec_c.clone();

        let (_, handle) = pool.spawn(async move {
            sched.add_active_vu(1).await;

            // Re-resolve driver from registry so each VU gets a fresh instance
            let driver = match registry.resolve_driver_by_id(&driver_id) {
                Some(d) => d,
                None => {
                    tracing::error!("VU {}: Driver '{}' not found in registry", vu_id, driver_id);
                    sched.remove_active_vu(1).await;
                    return;
                }
            };

            let mut driver_instance = match driver
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
                    sched.remove_active_vu(1).await;
                    return;
                }
            };

            let client = match HttpClient::with_tls(&http_cfg, &tls_cfg) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                    sched.remove_active_vu(1).await;
                    return;
                }
            };
            let http_client_handle: Arc<dyn DriverHttpClient + Send + Sync> = Arc::new(DriverHttpClientImpl { client });

            let mut iteration_index = 0u64;
            let mut vu_sample_counter: u64 = 0;

            loop {
                if sched.is_force_stop_requested() || sched.is_stop_requested() { break; }
                {
                    let active = sched.active_vus().await;
                    if sched.try_claim_ramp_down(active).await { break; }
                }

                vu_sample_counter += 1;
                if vu_sample_counter % 100 == 0 {
                    let active = sched.active_vus().await;
                    utils_emit_vus_metrics(&metrics, active, &sc_tags).await;
                }

                let iter_start = Instant::now();

                // Arrival-rate mode: wait for an iteration token. The wait is
                // ALSO woken by the stop signal so an idle VU observes the
                // level-triggered stop flag and exits promptly instead of
                // waiting forever for a token the ended run will never add.
                // The idle/busy marks are balanced on every exit path.
                if sched.is_arrival_rate() {
                    sched.mark_idle();
                    let arrival_notify = sched.arrival_notify();
                    let mut got_token = false;
                    loop {
                        if sched.is_stop_requested() || sched.is_force_stop_requested() { break; }
                        if sched.try_acquire_arrival_token() { got_token = true; break; }
                        tokio::select! {
                            _ = arrival_notify.notified() => {}
                            _ = stop.notified() => {}
                        }
                    }
                    sched.mark_busy();
                    if !got_token { break; }
                }

                // Run ONE full iteration to completion. Deliberately no
                // `stop.notified()` select here — gracefulStop must DRAIN
                // in-flight iterations (let them finish), not cancel them.
                // The level-triggered stop flag ends the VU between
                // iterations; the scheduler's wait_for_drain enforces the
                // grace window with a force-stop backstop.
                let result = async {
                    let data_row = if data_rows.is_empty() { None }
                        else { Some(data_rows[(iteration_index as usize + vu_id as usize) % data_rows.len()].clone()) };

                    let iter_start_time = Instant::now();
                    let mut ctx = VuContext::new(vu_id, iteration_index, sc_name_vu.clone());
                    ctx.env = vu_env.clone();
                    ctx.data_row = data_row;
                    ctx.http_client = Some(http_client_handle.clone());

                    let result = driver_instance.run_iteration(&mut ctx).await;
                    let mut ctx_samples = std::mem::take(&mut ctx.samples);
                    if !ctx_samples.is_empty() {
                        // Merge per-scenario tags into every sample.
                        merge_scenario_tags(&mut ctx_samples, &sc_tags);
                        metrics.record_batch(&ctx_samples).await;
                    }

                    if ctx.abort_requested {
                        let msg = ctx.abort_message.unwrap_or_else(|| "Test aborted by driver".to_string());
                        tracing::warn!("Driver '{}' requested abort: {} — stopping", driver_id, msg);
                        sched.request_stop();
                    }

                    if let Err(e) = result {
                        tracing::warn!("VU {} iteration {} failed: {}", vu_id, iteration_index, e);
                    }

                    let iter_dur = iter_start_time.elapsed();
                    let now = std::time::SystemTime::now();
                    let empty_tags = TagMap::new();
                    let mut iter_samples = vec![
                        Sample { metric: "iterations".into(), value: 1.0, tags: empty_tags.clone(), timestamp: now, sample_type: tropel_core::types::SampleType::Counter },
                        Sample { metric: "iteration_duration".into(), value: iter_dur.as_micros() as f64, tags: empty_tags, timestamp: now, sample_type: tropel_core::types::SampleType::Trend },
                    ];
                    merge_scenario_tags(&mut iter_samples, &sc_tags);
                    metrics.record_batch(&iter_samples).await;

                    if has_abort_thresholds {
                        let elapsed = test_start.elapsed();
                        if elapsed > Duration::from_secs(1) {
                            let slot = elapsed.as_secs() / 2;
                            let prev = (elapsed - Duration::from_millis(100)).as_secs() / 2;
                            if slot != prev {
                                let results = metrics.results().await;
                                if check_abort_on_fail(&thresholds, &results, elapsed) {
                                    sched.request_stop();
                                }
                            }
                        }
                    }

                    sched.increment_iterations().await;
                    iteration_index
                }.await;
                iteration_index = result + 1;

                // Skip pacing when stop/force-stop is already requested — a
                // drained VU should exit promptly instead of sleeping out a
                // full pacing period during graceful shutdown.
                if !sched.is_arrival_rate()
                    && !sched.is_stop_requested()
                    && !sched.is_force_stop_requested()
                {
                    apply_think_time(&think_time, Some(iter_start.elapsed())).await;
                }

                if total_iters != u64::MAX {
                    if is_per_vu_iterations {
                        if iteration_index >= total_iters { break; }
                    } else {
                        if sched.total_iterations().await >= total_iters { break; }
                    }
                }
            }

            sched.remove_active_vu(1).await;
        });
        handle
    }).await.ok();

    // Record dropped iterations (carries the scenario tags like every other
    // sample this scenario emits)
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
                    tags: dropped_tags,
                    timestamp: std::time::SystemTime::now(),
                    sample_type: tropel_core::types::SampleType::Counter,
                })
                .await;
        }
    }

    loop {
        let active = executor.active_vus().await;
        if active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

// ══════════════════════════════════════════════════════════════════
// Helpers
// ══════════════════════════════════════════════════════════════════

/// Merge per-scenario tags into a batch of samples (k6 semantics: scenario
/// tags apply to every metric the scenario emits). Scenario tags win over a
/// sample's own tags on key collision.
fn merge_scenario_tags(samples: &mut [Sample], tags: &HashMap<String, String>) {
    if tags.is_empty() {
        return;
    }
    for sample in samples.iter_mut() {
        for (k, v) in tags {
            sample.tags.insert(k.clone(), v.clone());
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
    }
}

async fn utils_emit_vus_metrics(metrics: &MetricsCollector, active: u32, sc_tags: &HashMap<String, String>) {
    let now = std::time::SystemTime::now();
    let mut vus_tags = TagMap::new();
    // k6 tags vus/vus_max per scenario; carry the scenario tags along.
    for (k, v) in sc_tags {
        vus_tags.insert(k.clone(), v.clone());
    }
    metrics
        .record_batch(&[
            Sample {
                metric: "vus".into(),
                value: active as f64,
                tags: vus_tags.clone(),
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Point,
            },
            Sample {
                metric: "vus_max".into(),
                value: active as f64,
                tags: vus_tags,
                timestamp: now,
                sample_type: tropel_core::types::SampleType::Point,
            },
        ])
        .await;
}

// ── Duration parsing (from old engine.rs) ──

fn parse_duration_str(s: &str) -> Result<Duration> {
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

async fn create_vu_js_context(
    vu_id: u32,
    pm_state: &tropel_pm::bridge::SharedPmState,
    http_client: &Arc<tropel_http::client::HttpClient>,
) -> Option<tropel_js::JsContext> {
    let ctx = match tropel_js::JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            tracing::warn!(
                "VU {}: Failed to create JS context: {} (scripts will be skipped)",
                vu_id,
                e
            );
            return None;
        }
    };

    let js_libraries: [(&str, &str); 2] = [
        ("pm-api", include_str!("../../../js/pm-api/pm.js")),
        ("chai-shim", include_str!("../../../js/chai/chai-shim.js")),
    ];

    let lodash_code: &str = include_str!("../../../js/lodash/lodash-shim.js");
    let cryptojs_code: &str = include_str!("../../../js/cryptojs-shim/cryptojs.js");
    let exec_code: &str = include_str!("../../../js/exec/exec.js");

    for (name, code) in &js_libraries {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!(
                "VU {}: Failed to bootstrap JS library '{}': {}",
                vu_id,
                name,
                e
            );
        }
    }

    for (name, code) in &[
        ("lodash-shim", lodash_code),
        ("cryptojs-shim", cryptojs_code),
        ("exec-shim", exec_code),
    ] {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!(
                "VU {}: Failed to bootstrap JS library '{}': {}",
                vu_id,
                name,
                e
            );
        }
    }

    if let Err(e) = tropel_native::install_all(&ctx).await {
        tracing::warn!("VU {}: Failed to install native modules: {}", vu_id, e);
    }

    let bridge = tropel_pm::bridge_fns::PmBridge::new(pm_state.clone(), http_client.clone());
    if let Err(e) = bridge.install(&ctx) {
        tracing::warn!("VU {}: Failed to install PM bridge functions: {}", vu_id, e);
    }

    ctx.with_ctx(|rq_ctx| {
        let globals = rq_ctx.globals();
        let _ = globals.set(
            "__tropel_native_sleep",
            rquickjs::function::Func::from(move |ms: f64| {
                if ms > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(ms / 1000.0));
                }
            }),
        );
    });

    let sleep_code = [
        "if (typeof sleep === 'undefined') {",
        "  function sleep(seconds) {",
        "    if (typeof __tropel_native_sleep === 'function') {",
        "      __tropel_native_sleep(seconds * 1000);",
        "    }",
        "  }",
        "}",
    ]
    .join("\n");
    let _ = ctx.eval(&sleep_code).await;

    Some(ctx)
}
