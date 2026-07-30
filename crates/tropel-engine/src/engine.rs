use crate::worker::VUWorkerPool;
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
    pub async fn run(&self, config: &JobConfig) -> Result<EngineResult> {
        tracing::info!("Starting Tropel load test: {}", config.input);

        // Parse the input file
        let scenario = self.parse_input(&config.input, config).await?;
        let scenario = Arc::new(scenario);

        // Create metrics collector
        let metrics = Arc::new(MetricsCollector::new());

        // Create the executor
        let executor = VUScheduler::new(&config.execution);
        let stop_signal = executor.stop_signal();

        // Create thread-per-core worker pool for VU sharding
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pool = Arc::new(VUWorkerPool::new(num_workers));
        tracing::info!("VU worker pool: {} threads (available cores: {})", num_workers, num_workers);

        let total_iterations = match &config.execution {
            ExecutionConfig::SharedIterations { iterations, .. } => *iterations,
            _ => u64::MAX,
        };

        // Count VUs for iteration data sharding
        let vus = match &config.execution {
            ExecutionConfig::ConstantVus { vus, .. } => *vus,
            ExecutionConfig::RampingVus { start_vus, .. } => *start_vus,
            ExecutionConfig::SharedIterations { vus, .. } => *vus,
            ExecutionConfig::ConstantArrivalRate { pre_alloc_vus, .. } => *pre_alloc_vus,
        };
        let vus = vus.max(1);

        let test_start = Instant::now();
        let has_abort_thresholds = config.thresholds.values().any(|t| t.abort_on_fail);
        let thresholds = config.thresholds.clone();

        let metrics_clone = metrics.clone();
        let scenario_clone = scenario.clone();
        let stop_signal_clone = stop_signal.clone();
        let env_clone = config.env.clone();
        let data_rows = config.iteration_data.clone();
        let pool_clone = pool.clone();
        let http_config = config.http.clone();

        executor.run(move |sched, vu_id| {
            let metrics = metrics_clone.clone();
            let scenario = scenario_clone.clone();
            let stop = stop_signal_clone.clone();
            let total_iters = total_iterations;
            let vu_env = env_clone.clone();
            let data_rows = data_rows.clone();
            let http_cfg = http_config.clone();
            let _vus_count = vus;
            let thresholds = thresholds.clone();
            let test_start = test_start;
            let has_abort_thresholds = has_abort_thresholds;
            let pool = pool_clone.clone();

            let (_, handle) = pool.spawn(async move {
                // Increment active VU count on start
                sched.add_active_vu(1).await;

                // Create per-VU HTTP client — own connection pool + cookie jar
                let client = match HttpClient::new(&http_cfg) {
                    Ok(c) => c,
                    Err(e) => {
                        tracing::error!("VU {}: Failed to create HTTP client: {}", vu_id, e);
                        sched.remove_active_vu(1).await;
                        return;
                    }
                };

                // Clone the HTTP client for the PM bridge (sendRequest needs it)
                let bridge_client = Arc::new(client.clone());

                // Create VU runner with its own dedicated HTTP client
                let mut runner = VURunner::new(scenario, client);
                let pm_state = runner.state_handle();

                // Create JS context and attach to runner
                let js_ctx = create_vu_js_context(vu_id, &pm_state, &bridge_client).await;
                if let Some(ctx) = js_ctx {
                    runner = runner.with_js_context(Box::new(ctx));
                    tracing::debug!("VU {}: JS context attached for script execution", vu_id);
                }

                let mut iteration_index = 0u64;
                tracing::debug!("VU {} starting iteration loop at index 0", vu_id);

                loop {
                    // Level-triggered stop check — catches signals that arrived
                    // between iterations when no VU was awaiting on .notified().
                    if sched.is_stop_requested() {
                        tracing::debug!("VU {} stopping (level-triggered check)", vu_id);
                        break;
                    }

                    // Check if we should stop
                    tokio::select! {
                        _ = stop.notified() => {
                            tracing::debug!("VU {} stopping (signal)", vu_id);
                            break;
                        }
                        result = async {
                            // Get iteration data row (sharded by VU and iteration)
                            let data_row = if data_rows.is_empty() {
                                None
                            } else {
                                let row_idx = (iteration_index as usize + vu_id as usize) % data_rows.len();
                                Some(data_rows[row_idx].clone())
                            };

                            // Arrival-rate gate: consume a token before each iteration.
                            // Only applies in arrival-rate mode — VUs in constant/ramping/
                            // shared-iterations modes skip this gate entirely.
                            if sched.is_arrival_rate() {
                                sched.mark_idle();
                                loop {
                                    if sched.try_acquire_arrival_token() {
                                        sched.mark_busy();
                                        break;
                                    }
                                    // No token available — wait for ticker to add one
                                    sched.arrival_notify().notified().await;
                                }
                            }

                            tracing::debug!("VU {} running iteration {}", vu_id, iteration_index);
                            let iter_result = runner.run_iteration(iteration_index, data_row, &vu_env).await;
                            tracing::debug!("VU {} iteration {} completed with {} samples", vu_id, iteration_index, iter_result.samples.len());

                            // Record all samples from this iteration
                            if !iter_result.samples.is_empty() {
                                metrics.record_batch(&iter_result.samples).await;
                            }

                            // Mid-run threshold check: abort if any abortOnFail threshold breached
                            // Check at most once per 2-second slot to avoid hammering metrics.results()
                            if has_abort_thresholds {
                                let elapsed = test_start.elapsed();
                                // Only check if at least the shortest delay_abort_eval has passed
                                if elapsed > Duration::from_secs(1) {
                                    // 2-second slot throttle — distributes checks across VUs
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

                    // Check iteration limit for shared-iterations mode
                    if total_iters != u64::MAX {
                        let current_iters = sched.total_iterations().await;
                        if current_iters >= total_iters {
                            break;
                        }
                    }
                }

                // Decrement active VU count on exit
                sched.remove_active_vu(1).await;
                tracing::debug!("VU {} finished ({} iterations)", vu_id, iteration_index);
            });
            handle
        }).await?;

        // Wait for active VUs to reach zero (all finished)
        loop {
            let active = executor.active_vus().await;
            if active == 0 {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }

        // Collect and aggregate metrics
        let mut results = metrics.results().await;

        // Include dropped iterations from arrival-rate scheduler
        results.dropped_iterations = executor.take_dropped_iterations();

        // Report results
        let reporters = self.create_reporters(&config.output);
        for reporter in &reporters {
            reporter.report(&results).await?;
        }

        Ok(EngineResult {
            metrics: results,
        })
    }

    /// Parse an input file into a Scenario.
    async fn parse_input(&self, input_path: &str, config: &JobConfig) -> Result<Scenario> {
        let collection = parse_collection_file(input_path)
            .map_err(|e| TropelError::Parse(format!("Failed to parse collection: {}", e)))?;

        let scenario = collection_to_scenario(collection, config.env.clone());
        Ok(scenario)
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
