use crate::worker::VUWorkerPool;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use rand::Rng;
use tokio::sync::broadcast;
use tropel_core::config::{ExecutionConfig, JobConfig, OutputConfig, ThinkTimeConfig};
use tropel_core::scenario::Scenario;
use tropel_core::{Result, TropelError};
use tropel_core::types::Sample;
use tropel_executor::runner::VURunner;
use tropel_executor::scheduler::VUScheduler;
use tropel_ext::registry::ExtensionRegistry;
use tropel_http::client::HttpClient;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::check_abort_on_fail;
use tropel_report::{create_reporter, Reporter, StreamingStdoutOutput};

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
        // Collect inventory-registered extension and wrap registry in Arc for
        // passing across spawned task boundaries.
        let registry = Arc::new(self.extension_registry.clone());
        // Clone the format hint string before the spawn loop to avoid
        // lifetime issues (tokio::spawn requires 'static).
        let format_hint = config.input_type.clone();

        let metrics = Arc::new(MetricsCollector::new());

        // Create thread-per-core worker pool for VU sharding
        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pool = Arc::new(VUWorkerPool::new(num_workers));
        tracing::info!("VU worker pool: {} threads (available cores: {})", num_workers, num_workers);

        // Create a shared multi-thread tokio runtime for blocking bridge calls.
        // The VU thread-per-core workers use current-thread runtimes where
        // Handle::current().block_on() panics. Bridge functions like
        // pm.sendRequest that need to await async HTTP calls use this
        // independent multi-thread runtime via block_on() instead.
        let blocking_rt = Arc::new(tokio::runtime::Runtime::new()
            .expect("Failed to create blocking tokio runtime"));

        let http_config = config.http.clone();
        let thresholds = config.thresholds.clone();
        let data_rows = config.iteration_data.clone();
        let test_start = Instant::now();

        // ═══════════════════════════════════════════════════════════
        // Streaming outputs — live progress during the run
        // ═══════════════════════════════════════════════════════════
        // Create a broadcast sample stream. The metrics collector forwards
        // each sample to all subscribed outputs (best-effort, non-blocking).
        // Each output consumer subscribes independently via `subscribe()`.
        // When a consumer lags, it skips missed samples via RecvError::Lagged.
        //
        // Broadcast buffer capacity: 10_000 samples. At ~10 samples/req × 10k req/s
        // this provides ~100ms of burst buffer before oldest samples are evicted.
        // This is more than adequate for a 2s-interval progress display.
        // Higher capacity (100_000+) would allocate ~50 MB of pre-allocated buffer.
        let mut output_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let (sample_tx, _) = broadcast::channel::<Sample>(10_000);
        metrics.set_sample_sink(Some(sample_tx.clone()));

        // Start streaming output consumers based on config
        let has_stdout = config.output.reporters.iter().any(|r| r == "stdout");
        if has_stdout {
            let rx = sample_tx.subscribe();
            let handle = StreamingStdoutOutput::spawn(rx);
            output_handles.push(handle);
        }
        // If no streaming outputs are configured, clear the sink
        // so the broadcast channel can be garbage collected.
        if output_handles.is_empty() {
            metrics.set_sample_sink(None);
        }

        // Build the list of scenarios to execute.
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

        for (scenario_name, exec_cfg, sc_env, start_delay, input_path) in &scenario_configs {
            let sc_name = scenario_name.clone();
            let exec_cfg = exec_cfg.clone();
            let sc_env = sc_env.clone();
            let input_path = input_path.clone();
            let start_delay = *start_delay;
            let metrics = metrics.clone();
            let pool = pool.clone();
            let http_cfg = http_config.clone();
            let thresholds = thresholds.clone();
            let data_rows = data_rows.clone();
            let base_env = config.env.clone();
            let test_start = test_start;
            let blocking_rt_sc = blocking_rt.clone();

            let registry_sc = registry.clone();
            let fmt_hint = format_hint.clone();
            let handle = tokio::spawn(async move {
                // Parse the input file for this scenario using the
                // registry-driven input adapter dispatch.
                let scenario = parse_scenario_input(
                    &input_path,
                    fmt_hint.as_deref(),
                    &registry_sc,
                    &base_env,
                );
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
                    ExecutionConfig::PerVUIterations { iterations, .. } => *iterations,
                    _ => u64::MAX,
                };

                let vus = match &exec_cfg {
                    ExecutionConfig::ConstantVus { vus, .. } => *vus,
                    ExecutionConfig::RampingVus { start_vus, .. } => *start_vus,
                    ExecutionConfig::SharedIterations { vus, .. } => *vus,
                    ExecutionConfig::ConstantArrivalRate { pre_alloc_vus, .. } => *pre_alloc_vus,
                    ExecutionConfig::PerVUIterations { vus, .. } => *vus,
                    ExecutionConfig::RampingArrivalRate { pre_alloc_vus, .. } => *pre_alloc_vus,
                };
                let _vus = vus.max(1);

                let has_abort_thresholds = thresholds.values().any(|t| t.abort_on_fail);

                // Distinguish PerVUIterations from other executor types.
                // For PerVUIterations, each VU tracks its own iteration count
                // locally; the exit check uses the local `iteration_index`
                // instead of the scheduler's global counter.
                let is_per_vu_iterations = matches!(exec_cfg, ExecutionConfig::PerVUIterations { .. });

                let metrics_clone = metrics.clone();
                let scenario_clone = scenario.clone();
                let stop_signal_clone = stop_signal.clone();
                let data_rows_clone = data_rows.clone();
                let pool_clone = pool.clone();
                let http_cfg_clone = http_cfg.clone();
                let thresholds_clone = thresholds.clone();
                let vu_env_clone = vu_env.clone();

                // Extract think time config for this scenario
                let think_time_cfg = match &exec_cfg {
                    ExecutionConfig::ConstantVus { think_time, .. } => think_time.clone(),
                    ExecutionConfig::RampingVus { think_time, .. } => think_time.clone(),
                    ExecutionConfig::ConstantArrivalRate { think_time, .. } => think_time.clone(),
                    ExecutionConfig::SharedIterations { think_time, .. } => think_time.clone(),
                    ExecutionConfig::PerVUIterations { think_time, .. } => think_time.clone(),
                    ExecutionConfig::RampingArrivalRate { think_time, .. } => think_time.clone(),
                };

                // Clone sc_name for the closure to avoid move-ownership conflicts
                let sc_name_for_vu = sc_name.clone();
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
                    let think_time = think_time_cfg.clone();

                    let blocking_rt_vu = blocking_rt_sc.clone();
                    let sc_name_vu = sc_name_for_vu.clone();
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
                        let mut runner = VURunner::new(scenario, client, vu_id, sc_name_vu.clone())
                            .with_expected_statuses(http_cfg.expected_statuses.clone());
                        let pm_state = runner.state_handle();

                        let js_ctx = create_vu_js_context(vu_id, &pm_state, &bridge_client, blocking_rt_vu.clone()).await;
                        if let Some(ctx) = js_ctx {
                            runner = runner.with_js_context(Box::new(ctx));
                        }

                        let mut iteration_index = 0u64;
                        // Periodic VU gauge sampling: every N iterations, emit vus/vus_max
                        let mut vu_sample_counter: u64 = 0;

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

                            // Emit vus gauge sample periodically (every ~100 iterations)
                            vu_sample_counter += 1;
                            if vu_sample_counter % 100 == 0 {
                                let active = sched.active_vus().await;
                                let now = std::time::SystemTime::now();
                                let vus_tags = tropel_core::types::TagMap::new();
                                metrics.record_batch(&[
                                    tropel_core::types::Sample {
                                        metric: "vus".to_string(),
                                        value: active as f64,
                                        tags: vus_tags.clone(),
                                        timestamp: now,
                                        sample_type: tropel_core::types::SampleType::Point,
                                    },
                                    tropel_core::types::Sample {
                                        metric: "vus_max".to_string(),
                                        value: active as f64,
                                        tags: vus_tags,
                                        timestamp: now,
                                        sample_type: tropel_core::types::SampleType::Point,
                                    },
                                ]).await;
                            }

                            let iter_start = Instant::now();

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

                                    let iter_start_time = Instant::now();
                                    let iter_result = runner.run_iteration(iteration_index, data_row, &vu_env).await;
                                    let iter_dur = iter_start_time.elapsed();

                                    // Emit iteration metrics
                                    {
                                        let mut iter_samples = iter_result.samples;
                                        let now = std::time::SystemTime::now();
                                        let empty_tags = tropel_core::types::TagMap::new();

                                        // iterations (Counter)
                                        iter_samples.push(tropel_core::types::Sample {
                                            metric: "iterations".to_string(),
                                            value: 1.0,
                                            tags: empty_tags.clone(),
                                            timestamp: now,
                                            sample_type: tropel_core::types::SampleType::Counter,
                                        });

                                        // iteration_duration (Trend) in microseconds
                                        iter_samples.push(tropel_core::types::Sample {
                                            metric: "iteration_duration".to_string(),
                                            value: iter_dur.as_micros() as f64,
                                            tags: empty_tags,
                                            timestamp: now,
                                            sample_type: tropel_core::types::SampleType::Trend,
                                        });

                                        metrics.record_batch(&iter_samples).await;
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

                                    // Check if test.abort() was called from the script.
                                    // If so, request a clean stop so all VUs drain.
                                    {
                                        let state = pm_state.lock().unwrap();
                                        if state.abort_requested {
                                            let msg = state.abort_message.clone()
                                                .unwrap_or_else(|| "Test aborted by script".to_string());
                                            tracing::warn!("test.abort(): {} — stopping", msg);
                                            drop(state);
                                            sched.request_stop();
                                        }
                                    }

                                    sched.increment_iterations().await;
                                    iteration_index
                                } => { iteration_index = result + 1; }
                            }

                            // Apply iteration pacing AFTER the iteration:
                            // if iteration_pacing is set, wait to hit the target duration.
                            if !sched.is_arrival_rate() {
                                let iter_duration = iter_start.elapsed();
                                apply_think_time(&think_time, Some(iter_duration)).await;
                            }

                            // Check iteration limit.
                            // For PerVUIterations, each VU tracks its own count via
                            // the local `iteration_index`. For SharedIterations, VUs
                            // share a global counter checked through the scheduler.
                            if total_iters != u64::MAX {
                                if is_per_vu_iterations {
                                    // Per-VU count: each VU exits after running its
                                    // assigned iterations independently.
                                    if iteration_index >= total_iters {
                                        break;
                                    }
                                } else {
                                    // Global count: all VUs share the iteration budget.
                                    let current_iters = sched.total_iterations().await;
                                    if current_iters >= total_iters {
                                        break;
                                    }
                                }
                            }
                        }

                        sched.remove_active_vu(1).await;
                    });
                    handle
                }).await.ok();

                // Record dropped iterations from this scenario's scheduler as a counter metric
                {
                    let dropped = executor.take_dropped_iterations();
                    if dropped > 0 {
                        metrics.record(&tropel_core::types::Sample {
                            metric: "dropped_iterations".to_string(),
                            value: dropped as f64,
                            tags: tropel_core::types::TagMap::new(),
                            timestamp: std::time::SystemTime::now(),
                            sample_type: tropel_core::types::SampleType::Counter,
                        }).await;
                    }
                }

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

        // Drain remaining streaming output samples and wait for consumer tasks.
        // The metrics collector is still alive at this point, so the broadcast
        // channel is still open. Drop the sample sink first to signal outputs
        // that no more samples will arrive.
        metrics.set_sample_sink(None);
        for handle in output_handles {
            let _ = handle.await;
        }

        // Collect and aggregate metrics across all scenarios
        // dropped_iterations is populated via counter metric samples
        // recorded by each scenario's spawn after executor.run().
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

/// Parse a scenario input file and return a Scenario using the extension
/// registry's input adapters.
///
/// Resolution order:
/// 1. If `format_hint` is provided (`--format` flag), use that adapter directly.
/// 2. Otherwise, iterate all registered adapters and call `detect(bytes)`.
/// 3. If no adapter claims the file, return an error listing available adapters.
///
/// The registry is populated at startup via `collect_inventory()`, which gathers
/// all `inventory::submit!`-registered adapters from linked crates.
fn parse_scenario_input(
    input_path: &str,
    format_hint: Option<&str>,
    registry: &tropel_ext::registry::ExtensionRegistry,
    base_env: &std::collections::HashMap<String, String>,
) -> Result<Scenario> {
    let input_p = std::path::Path::new(input_path);

    // Read the file bytes — all adapters work from raw bytes.
    // Using std::fs::read (synchronous) because this runs once at startup,
    // not on the hot path. Tokio's fs feature would add unnecessary deps.
    let bytes = std::fs::read(input_path)
        .map_err(|e| TropelError::Parse(format!("Failed to read '{}': {}", input_path, e)))?;

    // Resolve the adapter: explicitly by format ID, or auto-detect from content.
    let adapter: Box<dyn tropel_ext::traits::InputAdapter> = if let Some(fmt) = format_hint {
        registry.resolve_input_by_id(fmt)
            .ok_or_else(|| {
                let available = registry.list_inputs();
                TropelError::Config(format!(
                    "Unknown input format '{}'. Available formats: {}",
                    fmt, available.join(", ")
                ))
            })?
    } else {
        registry.resolve_input(&bytes)
            .ok_or_else(|| {
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

    let adapter_id = adapter.id().to_string();
    tracing::info!(
        "Input '{}' resolved by adapter '{}'",
        input_path, adapter_id
    );

    // Parse with the source path hint so adapters that need file-system access
    // (e.g. k6 for module resolution) can use it.
    let mut scenario = adapter.parse_with_path(&bytes, Some(input_p))?;

    // Merge base environment variables into the scenario's variables.
    // The old code did this via collection_to_scenario(collection, base_env);
    // now we do it generically for any adapter output.
    for (key, val) in base_env {
        scenario.variables.entry(key.clone())
            .or_insert_with(|| serde_json::Value::String(val.clone()));
    }

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

/// Apply think time / pacing delay based on configuration.
///
/// - `iteration_pacing`: if set and `iter_duration` is provided, wait for the
///   remaining time to hit the target pacing duration.
/// - `delay`: fixed delay after each iteration.
/// - `min_delay` / `max_delay`: random delay in range [min, max].
/// - If both `delay` and min/max are set, `delay` wins.
/// - If neither is set and no pacing is configured, this is a no-op.
/// - In arrival-rate mode, this is skipped (rate control handles pacing).
async fn apply_think_time(config: &ThinkTimeConfig, iter_duration: Option<Duration>) {
    // If iteration_pacing is set, wait only to hit the target duration.
    // Pacing replaces think time — no additional delay after pacing.
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
            return; // pacing replaces think time
        }
    }

    // No pacing — apply fixed delay or random range.
    // These are mutually exclusive: fixed delay wins if set.
    if let Some(delay_str) = &config.delay {
        if let Ok(delay) = parse_duration_str(delay_str) {
            if delay > Duration::from_millis(1) {
                tokio::time::sleep(delay).await;
                return;
            }
        }
    }

    // Random range [min_delay, max_delay]
    if let (Some(min_str), Some(max_str)) = (&config.min_delay, &config.max_delay) {
        if let (Ok(min), Ok(max)) = (parse_duration_str(min_str), parse_duration_str(max_str)) {
            if max > Duration::ZERO && max > min {
                let range_ms = (max - min).as_millis() as u64;
                let rand_ms = rand::thread_rng().gen_range(0..=range_ms);
                let delay = min + Duration::from_millis(rand_ms);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Create a JS context for a VU and bootstrap the vendored JS libraries.
///
/// `blocking_rt` is a shared multi-thread tokio runtime used by bridge functions
/// like pm.sendRequest that need to await async operations from within a
/// synchronous FFI bridge closure (which runs on a current-thread VU worker
/// where Handle::current().block_on() would panic).
async fn create_vu_js_context(
    vu_id: u32,
    pm_state: &tropel_pm::bridge::SharedPmState,
    http_client: &Arc<tropel_http::client::HttpClient>,
    blocking_rt: Arc<tokio::runtime::Runtime>,
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

    // Bootstrap lodash, cryptojs, and exec shims
    let exec_code: &str = include_str!("../../../js/exec/exec.js");
    for (name, code) in &[
        ("lodash-shim", lodash_code),
        ("cryptojs-shim", cryptojs_code),
        ("exec-shim", exec_code),
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
    // Pass the per-VU HTTP client + blocking runtime for pm.sendRequest.
    // The blocking runtime allows synchronous bridge closures to await
    // async HTTP calls without panicking on a current-thread VU worker.
    let bridge = tropel_pm::bridge_fns::PmBridge::new(
        pm_state.clone(),
        http_client.clone(),
        blocking_rt,
    );
    if let Err(e) = bridge.install(&ctx) {
        tracing::warn!("VU {}: Failed to install PM bridge functions: {}", vu_id, e);
    }

    // Install __tropel_native_sleep for script-level sleep(ms).
    // Blocks the current thread via std::thread::sleep. In the thread-per-core
    // architecture each VU runs on its own tokio runtime thread, so blocking
    // the OS thread only pauses this VU — other VUs on other cores are unaffected.
    // This matches k6's behavior where sleep() blocks the goroutine.
    ctx.with_ctx(|rq_ctx| {
        let globals = rq_ctx.globals();
        let _ = globals.set(
            "__tropel_native_sleep",
            rquickjs::function::Func::from(move |ms: f64| {
                if ms > 0.0 {
                    let dur = Duration::from_secs_f64(ms / 1000.0);
                    std::thread::sleep(dur);
                }
            }),
        );
    });

    // Register a user-facing sleep(seconds) wrapper.
    // Users call `sleep(1)` in their scripts (seconds), the wrapper
    // converts to milliseconds and delegates to the native bridge.
    let sleep_code = [
        "if (typeof sleep === 'undefined') {",
        "  function sleep(seconds) {",
        "    if (typeof __tropel_native_sleep === 'function') {",
        "      __tropel_native_sleep(seconds * 1000);",
        "    }",
        "  }",
        "}",
    ].join("\n");
    let _ = ctx.eval(&sleep_code).await;

    Some(ctx)
}
