use crate::worker::VUWorkerPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tropel_collection::{collection_to_scenario, parse_collection_file};
use tropel_core::config::{ExecutionConfig, JobConfig, OutputConfig};
use tropel_core::scenario::Scenario;
use tropel_core::{Result, TropelError};
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::VUScheduler;
use tropel_ext::registry::ExtensionRegistry;
use tropel_http::client::HttpClient;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::check_abort_on_fail;
use tropel_report::{create_reporter, Reporter};

/// The engine orchestrates a complete load test job.
pub struct Engine {
    extension_registry: ExtensionRegistry,
}

impl Engine {
    /// Create a new engine with the given extension registry.
    pub fn new(registry: ExtensionRegistry) -> Self {
        Self {
            extension_registry: registry,
        }
    }

    /// Run a complete load test job.
    ///
    /// Supports two modes:
    /// - **Single-scenario**: uses `config.execution` directly (backward compatible).
    /// - **Multi-scenario**: uses `config.scenarios`, each with its own executor, env,
    ///   tags, and optional staggered `startTime`. All scenarios share the same metrics
    ///   collector and VU worker pool.
    pub async fn run(&self, config: &JobConfig) -> Result<EngineResult> {
        let metrics = Arc::new(MetricsCollector::new());

        // Create thread-per-core worker pool for VU sharding
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pool = Arc::new(VUWorkerPool::new(num_workers));
        tracing::info!("VU worker pool: {} threads (available cores: {})", num_workers, num_workers);

        let http_config = config.http.clone();
        let thresholds = config.thresholds.clone();
        let data_rows = config.iteration_data.clone();
        let test_start = Instant::now();        // Build the list of scenarios to execute.
        // If config specifies named scenarios, use those; otherwise synthesise a
        // single anonymous scenario from the top-level execution config.
        let scenario_configs: Vec<(String, ExecutionConfig, HashMap<String, String>, Duration, String)> =
            if !config.scenarios.is_empty() {
                config.scenarios.iter().map(|(name, sc)| {
                    let start_delay = parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                    let input_path = sc.input.clone().unwrap_or_else(|| config.input.clone());
                    (name.clone(), sc.execution.clone(), sc.env.clone(), start_delay, input_path)
                }).collect()
            } else {
                vec![(
                    "default".to_string(),
                    config.execution.clone(),
                    HashMap::new(),
                    Duration::ZERO,
                    config.input.clone(),
                )]
            };

        tracing::info!(
            "Starting Tropel load test: {} scenario(s)",
            scenario_configs.len()
        );

        let mut scenario_handles = Vec::new();

        for (scenario_name, exec_cfg, sc_env, start_delay, input_path) in scenario_configs {
            let sc_name = scenario_name.clone();
            let metrics = metrics.clone();
            let pool = pool.clone();
            let http_cfg = http_config.clone();
            let thresholds = thresholds.clone();
            let data_rows = data_rows.clone();
            let base_env = config.env.clone();
            let test_start = test_start;

            let handle = tokio::spawn(async move {
                // Parse the input file for this scenario
                let scenario = parse_scenario_input(&input_path, &base_env).await;
                let scenario = match scenario {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        tracing::error!("Scenario '{}': failed to parse input '{}': {}", sc_name, input_path, e);
                        return;
                    }
                };

                // Staggered start: wait for start_time before beginning
                if start_delay > Duration::ZERO {
                    tokio::time::sleep(start_delay).await;
                    tracing::info!("Scenario '{}' started after {:?} delay", sc_name, start_delay);
                }

                // Build merged env: base env + scenario-specific env
                let mut vu_env = base_env;
                vu_env.extend(sc_env);

                // Create per-scenario executor
                let executor = VUScheduler::new(&exec_cfg);
                let stop_signal = executor.stop_signal();

                let total_iterations = match &exec_cfg {
                    ExecutionConfig::SharedIterations { iterations, .. } => *iterations,
                    _ => u64::MAX,
                };

                let vus = match &exec_cfg {
                    ExecutionConfig::ConstantVus { vus, .. } => *vus,
                    ExecutionConfig::RampingVus { start_vus, .. } => *start_vus,
                    ExecutionConfig::SharedIterations { vus, .. } => *vus,
                    ExecutionConfig::ConstantArrivalRate { pre_alloc_vus, .. } => *pre_alloc_vus,
                };
                let _vus = vus.max(1);

                let has_abort_thresholds = thresholds.values().any(|t| t.abort_on_fail);

                let metrics_clone = metrics.clone();
                let scenario_clone = scenario.clone();
                let stop_signal_clone = stop_signal.clone();
                let data_rows_clone = data_rows.clone();
                let pool_clone = pool.clone();
                let http_cfg_clone = http_cfg.clone();
                let thresholds_clone = thresholds.clone();
                let vu_env_clone = vu_env.clone();

                executor.run(move |sched, vu_id| {
                    let metrics = metrics_clone.clone();
                    let scenario = scenario_clone.clone();
                    let stop = stop_signal_clone.clone();
                    let total_iters = total_iterations;
                    let vu_env = vu_env_clone.clone();
                    let data_rows = data_rows_clone.clone();
                    let http_cfg = http_cfg_clone.clone();
                    let thresholds = thresholds_clone.clone();
                    let test_start = test_start;
                    let has_abort_thresholds = has_abort_thresholds;
                    let pool = pool_clone.clone();

                    let (_, handle) = pool.spawn(async move {
                        sched.add_active_vu(1).await;

                        let client = match HttpClient::new(&http_cfg) {
                            Ok(c) => c,
                            Err(e) => {
                                tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                                sched.remove_active_vu(1).await;
                                return;
                            }
                        };

                        let bridge_client = Arc::new(client.clone());
                        let mut runner = VURunner::new(scenario, client);
                        let pm_state = runner.state_handle();

                        let js_ctx = create_vu_js_context(vu_id, &pm_state, &bridge_client).await;
                        if let Some(ctx) = js_ctx {
                            runner = runner.with_js_context(Box::new(ctx));
                        }

                        let mut iteration_index = 0u64;

                        loop {
                            if sched.is_force_stop_requested() || sched.is_stop_requested() {
                                break;
                            }

                            // Check ramp-down target before starting a new iteration.
                            // If active VUs exceed the target, this VU self-selects
                            // to exit (level-triggered, so surplus VUs drain naturally).
                            {
                                let active = sched.active_vus().await;
                                if sched.should_ramp_down(active).await {
                                    break;
                                }
                            }

                            tokio::select! {
                                _ = stop.notified() => { break; }
                                result = async {
                                    let data_row = if data_rows.is_empty() {
                                        None
                                    } else {
                                        let row_idx = (iteration_index as usize + vu_id as usize) % data_rows.len();
                                        Some(data_rows[row_idx].clone())
                                    };

                                    if sched.is_arrival_rate() {
                                        sched.mark_idle();
                                        loop {
                                            if sched.try_acquire_arrival_token() {
                                                sched.mark_busy();
                                                break;
                                            }
                                            sched.arrival_notify().notified().await;
                                        }
                                    }

                                    let iter_result = runner.run_iteration(iteration_index, data_row, &vu_env).await;

                                    if !iter_result.samples.is_empty() {
                                        metrics.record_batch(&iter_result.samples).await;
                                    }

                                    if has_abort_thresholds {
                                        let elapsed = test_start.elapsed();
                                        if elapsed > Duration::from_secs(1) {
                                            let current_slot = elapsed.as_secs() / 2;
                                            let prev_slot = (elapsed - Duration::from_millis(100)).as_secs() / 2;
                                            if current_slot != prev_slot {
                                                let results = metrics.results().await;
                                                if check_abort_on_fail(&thresholds, &results, elapsed) {
                                                    sched.request_stop();
                                                }
                                            }
                                        }
                                    }

                                    sched.increment_iterations().await;
                                    iteration_index
                                } => { iteration_index = result + 1; }
                            }

                            if total_iters != u64::MAX {
                                let current_iters = sched.total_iterations().await;
                                if current_iters >= total_iters {
                                    break;
                                }
                            }
                        }

                        sched.remove_active_vu(1).await;
                    });
                    handle
                }).await.ok();

                // Wait for active VUs in this scenario to reach zero
                loop {
                    let active = executor.active_vus().await;
                    if active == 0 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                tracing::info!("Scenario '{}': completed", sc_name);
            });

            scenario_handles.push(handle);
        }

        // Wait for all scenarios to complete
        for handle in scenario_handles {
            handle.await.ok();
        }

        // Collect and aggregate metrics across all scenarios
        let results = metrics.results().await;

        // Report results
        let reporters = self.create_reporters(&config.output);
        for reporter in &reporters {
            reporter.report(&results).await?;
        }

        Ok(EngineResult {
            metrics: results,
        })
    }

    /// Create reporters based on config.
    fn create_reporters(&self, config: &OutputConfig) -> Vec<Box<dyn Reporter>> {
        let mut reporters: Vec<Box<dyn Reporter>> = Vec::new();

        for name in &config.reporters {
            if let Some(reporter) = create_reporter(name) {
                reporters.push(reporter);
            } else if let Some(_ext) = self.extension_registry.get_output(name) {
                tracing::warn!("Extension reporter '{}' not yet supported in engine runner", name);
            } else {
                tracing::warn!("Unknown reporter: {}", name);
            }
        }

        reporters
    }

    /// Get a reference to the extension registry.
    pub fn extension_registry(&self) -> &ExtensionRegistry {
        &self.extension_registry
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new(ExtensionRegistry::new())
    }
}

/// Result of a full engine run.
#[derive(Debug)]
pub struct EngineResult {
    pub metrics: tropel_metrics::collector::MetricsResult,
}

/// Parse a scenario input file and return a Scenario.
/// The scenario's env is merged at runtime by the caller.
async fn parse_scenario_input(
    input_path: &str,
    base_env: &std::collections::HashMap<String, String>,
) -> Result<Scenario> {
    let collection = parse_collection_file(input_path)
        .map_err(|e| TropelError::Parse(format!("Failed to parse collection: {}", e)))?;

    let scenario = collection_to_scenario(collection, base_env.clone());
    Ok(scenario)
}

/// Parse a duration string (e.g. "5s", "30s", "10m") into a Duration.
fn parse_duration_str(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "0s" {
        return Ok(Duration::ZERO);
    }
    if let Some(num_str) = s.strip_suffix("ms") {
        let ms: u64 = num_str.parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(num_str) = s.strip_suffix('s') {
        let secs: f64 = num_str.parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    } else if let Some(num_str) = s.strip_suffix('m') {
        let mins: f64 = num_str.parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(mins * 60.0))
    } else if let Some(num_str) = s.strip_suffix('h') {
        let hours: f64 = num_str.parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(hours * 3600.0))
    } else {
        // Default to seconds
        let secs: f64 = s.parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Create a JS context for a VU and bootstrap the vendored JS libraries.
async fn create_vu_js_context(
    vu_id: u32,
    pm_state: &tropel_pm::bridge::SharedPmState,
    http_client: &Arc<tropel_http::client::HttpClient>,
) -> Option<tropel_js::JsContext> {
    // Create the JS context with a 10 MB memory limit and 10s execution timeout
    let ctx = match tropel_js::JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10))).await {
        Ok(ctx) => ctx,
        Err(e) => {
            tracing::warn!("VU {}: Failed to create JS context: {} (scripts will be skipped)", vu_id, e);
            return None;
        }
    };

    // Bootstrap the vendored JS libraries
    let js_libraries: [(&str, &str); 2] = [
        ("pm-api", include_str!("../../../js/pm-api/pm.js")),
        ("chai-shim", include_str!("../../../js/chai/chai-shim.js")),
    ];

    // To keep things practical, we also include lodash and cryptojs shims
    let lodash_code: &str = include_str!("../../../js/lodash/lodash-shim.js");
    let cryptojs_code: &str = include_str!("../../../js/cryptojs-shim/cryptojs.js");

    // Bootstrap all libraries
    for (name, code) in &js_libraries {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!("VU {}: Failed to bootstrap JS library '{}': {}", vu_id, name, e);
        }
    }

    // Bootstrap lodash and cryptojs
    for (name, code) in &[
        ("lodash-shim", lodash_code),
        ("cryptojs-shim", cryptojs_code),
    ] {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!("VU {}: Failed to bootstrap JS library '{}': {}", vu_id, name, e);
        }
    }

    // Install native module functions (crypto, hash, encoding, assert, json, fn)
    if let Err(e) = tropel_native::install_all(&ctx).await {
        tracing::warn!("VU {}: Failed to install native modules: {}", vu_id, e);
    }

    // Install pm.* bridge functions so JS shims can call __tropel_pm_* functions.
    // Pass the per-VU HTTP client for pm.sendRequest to work synchronously.
    let bridge = tropel_pm::bridge_fns::PmBridge::new(pm_state.clone(), http_client.clone());
    if let Err(e) = bridge.install(&ctx) {
        tracing::warn!("VU {}: Failed to install PM bridge functions: {}", vu_id, e);
    }

    Some(ctx)
}
