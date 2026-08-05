use crate::input::{resolve_input_or_driver, ResolvedInput};
use crate::outputs::spawn_extension_output;
use crate::summary::build_summary_data;
use crate::vu_loop::{parse_duration_str, run_driver_vus, run_scenario_vus};
use crate::worker::VUWorkerPool;

/// One entry of the per-scenario config list threaded into `run_scenario_vus`.
struct ScenarioConfigTuple {
    name: String,
    execution: ExecutionConfig,
    env: HashMap<String, String>,
    tags: HashMap<String, String>,
    start_delay: Duration,
    input_path: String,
    exec: Option<String>,
}
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tropel_core::config::{ExecutionConfig, JobConfig, OutputConfig, ScenarioConfig};
use tropel_core::types::Sample;
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::Protocol;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::validate_thresholds;
use tropel_report::{create_reporter, InfluxdbOutput, JsonStreamOutput, OtlpOutput, PrometheusRemoteWriteOutput, Reporter, StatsdOutput, StreamingStdoutOutput, TagPolicy};

/// Capacity of the streaming-output broadcast ring. Sized for ~2.5s of
/// samples at ~100k samples/s so consumer stalls don't drop live data
/// (see the ring construction in `Engine::run`).
const SAMPLE_STREAM_CAPACITY: usize = 1 << 18; // 262_144

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

        // Latency histogram ceiling (None = auto-resize, no clipping). Applied
        // before any samples are recorded so every MetricSet uses it.
        metrics.set_histogram_max(config.http.histogram_max_micros).await;

        let num_workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
        let pool = Arc::new(VUWorkerPool::new(num_workers));
        tracing::info!(
            "VU worker pool: {} threads (available cores: {})",
            num_workers,
            num_workers
        );

        let mut http_config = config.http.clone();
        let tls_config = config.tls.clone();
        let mut thresholds = config.thresholds.clone();
        // One shared read-only copy of the iteration dataset; every VU clones
        // the Arc (not the Vec of rows), so memory is O(dataset) not
        // O(VUs × dataset). Rows are only cloned individually when an
        // iteration actually consumes one.
        let data_rows = std::sync::Arc::new(config.iteration_data.clone());
        let test_start = Instant::now();

        // Script-declared load profile (k6 `export const options`). Applied only
        // when the user did not set an explicit load profile — i.e. no
        // vus/duration/mode/stages/iterations CLI flags (or a config file that
        // marked execution_explicit). This is what makes a k6 script's own
        // vus/duration/stages/scenarios/thresholds drive the run instead of
        // being silently ignored.
        let mut declared_scenarios: Option<HashMap<String, ScenarioConfig>> = None;
        let mut declared_execution: Option<ExecutionConfig> = None;
        let mut declared_trend_stats: Option<Vec<String>> = None;
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
                    // Script-declared global body handling (k6
                    // `options.discardResponseBodies`) applies to the HTTP
                    // client when the job didn't set one explicitly.
                    if let Some(discard) = decl.discard_response_bodies {
                        http_config.discard_response_bodies = discard;
                        tracing::info!(
                            "Script-declared discardResponseBodies={} applied to HTTP client",
                            discard
                        );
                    }
                    // Script-declared summary trend stats (k6
                    // `options.summaryTrendStats`) configure the summary.
                    if let Some(stats) = decl.summary_trend_stats {
                        if !stats.is_empty() {
                            tracing::info!(
                                "Script-declared summaryTrendStats applied: {:?}",
                                stats
                            );
                            declared_trend_stats = Some(stats);
                        }
                    }
                    // Script-declared HTTP/DNS options (k6 `options.dns`,
                    // `noConnectionReuse`, `rps`, `hosts`, `blacklistIPs`)
                    // fold into the HTTP client config.
                    if let Some(ttl) = decl.dns_ttl {
                        http_config.dns_ttl = Some(ttl);
                    }
                    if let Some(sel) = decl.dns_select {
                        http_config.dns_select = Some(sel);
                    }
                    if let Some(pol) = decl.dns_policy {
                        http_config.dns_policy = Some(pol);
                    }
                    if let Some(no) = decl.no_connection_reuse {
                        http_config.no_connection_reuse = no;
                    }
                    if let Some(no) = decl.no_vu_connection_reuse {
                        http_config.no_vu_connection_reuse = no;
                    }
                    if let Some(rps) = decl.rps {
                        http_config.rps = Some(rps);
                    }
                    if let Some(hosts) = decl.hosts {
                        http_config.hosts = hosts;
                    }
                    if let Some(bl) = decl.blacklist_ips {
                        http_config.blacklist_ips = bl;
                    }
                    if http_config.dns_ttl.is_some()
                        || http_config.dns_select.is_some()
                        || http_config.dns_policy.is_some()
                        || http_config.no_connection_reuse
                        || http_config.rps.is_some()
                        || !http_config.hosts.is_empty()
                        || !http_config.blacklist_ips.is_empty()
                    {
                        tracing::info!("Script-declared DNS/HTTP options applied");
                    }
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

        // Fail closed at startup (k6 behavior): a malformed threshold
        // expression must abort the run with a clear config error BEFORE any
        // load is generated — never silently pass at the end (the old
        // evaluator returned `(true, …)` for unparseable input, so a typo'd
        // metric or a bogus operator reported green).
        validate_thresholds(&thresholds).map_err(TropelError::Config)?;

        // Global RPS limiter (k6 `options.rps`): created ONCE per run and
        // shared by every VU across every scenario, so the cap is global.
        let rps_limiter: Option<Arc<tropel_http::RpsLimiter>> =
            http_config.rps.map(|r| Arc::new(tropel_http::RpsLimiter::new(r)));
        if rps_limiter.is_some() {
            tracing::info!(
                "Global RPS cap: {} req/s (shared across all VUs)",
                http_config.rps.unwrap_or(0.0)
            );
        }

        // Streaming outputs. Size the broadcast ring to comfortably cover the
        // expected sample rate: a 10k buffer held only ~100ms of samples at
        // ~100k samples/s, so ANY consumer stall longer than that dropped
        // live samples (streaming outputs were lossy by design). 2^18 slots
        // hold ~2.5s of peak-rate samples — short consumer hiccups (GC, a
        // flush that batches 10k) no longer lose data. Each slot is a small
        // fixed-size struct (the tags HashMap is heap-allocated), so the
        // preallocated ring is a few tens of MB worst case.
        let mut output_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        let (sample_tx, _) = broadcast::channel::<Sample>(SAMPLE_STREAM_CAPACITY);
        metrics.set_sample_sink(Some(sample_tx.clone()));

        // Planned wall-clock length of the run (incl. grace) for the live
        // progress bar's 100% target. Resolved with the SAME precedence the
        // scenario_configs below use (declared scenarios → config scenarios →
        // declared/single execution), so the bar fills to the right total.
        let progress_total: Option<Duration> = {
            let consider = |exec: &ExecutionConfig, start: Duration| -> Option<Duration> {
                exec.total_duration().map(|d| d + start)
            };
            // Note: `.flatten().max()` skips unbounded scenarios (None) — a
            // run mixing a bounded and an externally-controlled scenario
            // targets the longest bounded end while the unbounded one keeps
            // running (the bar then just stays at 100% elapsed-only).
            if let Some(scs) = &declared_scenarios {
                scs.values()
                    .map(|sc| {
                        let start =
                            parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                        consider(&sc.execution, start)
                    })
                    .flatten()
                    .max()
            } else if !config.scenarios.is_empty() {
                config
                    .scenarios
                    .values()
                    .map(|sc| {
                        let start =
                            parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                        consider(&sc.execution, start)
                    })
                    .flatten()
                    .max()
            } else {
                consider(declared_execution.as_ref().unwrap_or(&config.execution), Duration::ZERO)
            }
        };

        let has_stdout = config.output.reporters.iter().any(|r| r == "stdout");
        if has_stdout {
            let rx = sample_tx.subscribe();
            let handle = StreamingStdoutOutput::spawn(rx, progress_total);
            output_handles.push(handle);
        }
        // Shared tag-forwarding policy for the network outputs: bounds label
        // cardinality at the backend (allowlist + per-sample cap).
        let tag_policy = TagPolicy {
            allowlist: config.output.tag_allowlist.clone(),
            max_tags: config.output.max_tags_per_sample,
        };

        // Prometheus remote-write and OTLP outputs (streaming, best-effort).
        // When the user drives prometheus through the extension output
        // (`--reporter prometheus`), the extension handles it — skip the
        // built-in path so samples are not pushed twice. Only skip when the
        // extension output is actually registered: a custom binary built
        // without tropel-x-prometheus must not silently lose its stream.
        let prometheus_via_extension = config
            .output
            .reporters
            .iter()
            .any(|r| r == "prometheus")
            && self
                .extension_registry
                .list_outputs()
                .iter()
                .any(|o| o == "prometheus");
        if let Some(url) = &config.output.prometheus_remote_write_url {
            if !prometheus_via_extension {
                let rx = sample_tx.subscribe();
                let handle =
                    PrometheusRemoteWriteOutput::spawn(rx, url.clone(), tag_policy.clone());
                output_handles.push(handle);
            }
        }
        if let Some(endpoint) = &config.output.otlp_endpoint {
            let rx = sample_tx.subscribe();
            let handle = OtlpOutput::spawn(rx, endpoint.clone(), tag_policy.clone());
            output_handles.push(handle);
        }
        // JSON-stream (NDJSON file) output.
        if let Some(path) = &config.output.json_stream {
            let rx = sample_tx.subscribe();
            let handle = JsonStreamOutput::spawn(rx, path.clone());
            output_handles.push(handle);
        }
        // StatsD / Datadog output (UDP datagrams).
        if let Some(addr) = &config.output.statsd_addr {
            let rx = sample_tx.subscribe();
            let handle = StatsdOutput::spawn(rx, addr.clone(), tag_policy.clone());
            output_handles.push(handle);
        }
        // InfluxDB output (line protocol over UDP).
        if let Some(addr) = &config.output.influxdb_addr {
            let rx = sample_tx.subscribe();
            let handle = InfluxdbOutput::spawn(rx, addr.clone(), tag_policy.clone());
            output_handles.push(handle);
        }
        // Registered extension outputs: any configured reporter name that
        // resolves to an extension output (e.g. the `prometheus` reference
        // extension) is driven from the sample stream — emit() per batch,
        // flush() when the stream closes. This replaces the old
        // "extension reporter not supported" dead end.
        for name in &config.output.reporters {
            if create_reporter(name).is_some() {
                continue; // built-in reporters handled above / at the end
            }
            if let Some(mut ext) = self.extension_registry.get_output(name) {
                ext.configure(&config.output);
                let rx = sample_tx.subscribe();
                let handle = spawn_extension_output(rx, ext);
                output_handles.push(handle);
            }
        }
        if output_handles.is_empty() {
            metrics.set_sample_sink(None);
        }        // Build scenario configs. Script-declared scenarios/execution (from a
        // k6 `export const options`) take precedence over the default profile
        // but not over explicit user config (execution_explicit check above).
        let scenario_configs: Vec<ScenarioConfigTuple> = if let Some(scs) = declared_scenarios {
            scs.iter()
                .map(|(name, sc)| {
                    let start_delay = parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                    let input_path = sc.input.clone().unwrap_or_else(|| config.input.clone());
                    ScenarioConfigTuple {
                        name: name.clone(),
                        execution: sc.execution.clone(),
                        env: sc.env.clone(),
                        tags: sc.tags.clone(),
                        start_delay,
                        input_path,
                        exec: sc.exec.clone(),
                    }
                })
                .collect()
        } else if !config.scenarios.is_empty() {
            config
                .scenarios
                .iter()
                .map(|(name, sc)| {
                    let start_delay = parse_duration_str(&sc.start_time).unwrap_or(Duration::ZERO);
                    let input_path = sc.input.clone().unwrap_or_else(|| config.input.clone());
                    ScenarioConfigTuple {
                        name: name.clone(),
                        execution: sc.execution.clone(),
                        env: sc.env.clone(),
                        tags: sc.tags.clone(),
                        start_delay,
                        input_path,
                        exec: sc.exec.clone(),
                    }
                })
                .collect()
        } else {
            let exec = declared_execution.unwrap_or_else(|| config.execution.clone());
            vec![
                ScenarioConfigTuple {
                    name: "default".to_string(),
                    execution: exec,
                    env: HashMap::new(),
                    tags: HashMap::new(),
                    start_delay: Duration::ZERO,
                    input_path: config.input.clone(),
                    exec: None,
                }
            ]
        };

        // Apply the execution segment (k6 executionSegment /
        // executionSegmentSequence): each scenario's workload is scaled
        // deterministically to this node's share [from, to). An invalid
        // segment spec is a hard config error — better to fail before any
        // VU starts than to run the full workload on every node.
        let segment = match &config.execution_segment {
            Some(spec) => match tropel_core::segment::ExecutionSegment::parse(
                spec,
                config.execution_segment_sequence.as_deref(),
            ) {
                Ok(seg) => {
                    tracing::info!(
                        "Execution segment [{:.3}, {:.3}) — running {:.1}% of the workload",
                        seg.from(),
                        seg.to(),
                        seg.fraction() * 100.0
                    );
                    Some(seg)
                }
                Err(e) => return Err(e),
            },
            None => None,
        };
        let scenario_configs: Vec<ScenarioConfigTuple> = scenario_configs
            .into_iter()
            .map(|mut sc| {
                if let Some(seg) = &segment {
                    sc.execution = seg.apply(&sc.execution);
                }
                sc
            })
            .collect();

        tracing::info!(
            "Starting Tropel load test: {} scenario(s)",
            scenario_configs.len()
        );
        let mut scenario_handles = Vec::new();

        for sc in &scenario_configs {
            let sc_name = sc.name.clone();
            let exec_cfg = sc.execution.clone();
            let sc_env = sc.env.clone();
            let sc_tags = sc.tags.clone();
            let input_path = sc.input_path.clone();
            let sc_exec = sc.exec.clone();
            let start_delay = sc.start_delay;
            let metrics = metrics.clone();
            let pool = pool.clone();
            let http_cfg = http_config.clone();
            let tls_cfg = tls_config.clone();
            let thresholds = thresholds.clone();
            let data_rows = data_rows.clone();
            let base_env = config.env.clone();
            let registry_sc = registry.clone();
            let fmt_hint = format_hint.clone();
            let control_port = config.control_port;
            let rps_limiter_sc = rps_limiter.clone();

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
                // Instantiate every registered protocol once per scenario and
                // share the scheme-keyed map across VUs so ANY non-HTTP URL
                // scheme (`grpc://`, `ws://`, or a third-party one) dispatches
                // to its registered protocol (the runner's scheme lookup).
                let protocols: Arc<HashMap<String, Arc<dyn Protocol>>> =
                    Arc::new(registry_sc.instantiate_protocols());
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
                            protocols,
                            control_port,
                            rps_limiter_sc,
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
                            control_port,
                            rps_limiter_sc,
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

        // Apply summary presentation config (trend stats + effective
        // thresholds) to the collector so reporters see them.
        let summary_trend_stats = declared_trend_stats
            .unwrap_or_else(tropel_metrics::collector::k6_default_trend_stats);
        metrics
            .set_summary_config(summary_trend_stats, thresholds.clone())
            .await;

        // Raw snapshot (build_results now clones the summary config into
        // every result, so ordering no longer matters — captured here only
        // for the lossless distributed merge). Distributed agents ship this
        // to a controller; single-node runs never consume it, so skip the
        // serialize cost.
        let snapshot = if config.distributed_worker {
            metrics.snapshot().await
        } else {
            tropel_metrics::collector::MetricsSnapshot::default()
        };
        let results = metrics.results().await;

        // Distributed workers (`tropel-agent`) skip ALL end-of-run output —
        // the controller owns the summary, handleSummary, and reporters —
        // and only ship their raw snapshot back for central merging.
        if !config.distributed_worker {
            let reporters = self.create_reporters(&config.output);
            for reporter in &reporters {
                reporter.report(&results).await?;
            }

            // k6 `handleSummary(data)`: let the script emit custom summaries
            // (JSON/HTML/JUnit). Runs after the run with the aggregated data;
            // returned files are written (`stdout` prints). Falls back to
            // `--summary-export` when the script declares no handleSummary.
            self.emit_handle_summary(config, &registry, &results, &thresholds, test_start)
                .await;
        }

        Ok(EngineResult {
            metrics: results,
            snapshot,
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
            } else if self
                .extension_registry
                .list_outputs()
                .iter()
                .any(|o| o == name)
            {
                // Extension outputs are driven from the sample stream during
                // the run (see Engine::run) — they are not end-of-run
                // reporters, so there is nothing to create here.
                tracing::debug!(
                    "Extension output '{}' driven as a streaming output",
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


// handleSummary(data) — script-emitted custom summaries


impl Engine {
    /// Invoke the script's `handleSummary(data)` (k6) after the run and
    /// write the returned files (`stdout` key prints to stdout). When the
    /// script declares no handleSummary, honor `--summary-export` by
    /// writing the default summary data object as JSON. Best-effort — a
    /// failing script summary never fails the run.
    async fn emit_handle_summary(
        &self,
        config: &JobConfig,
        registry: &ExtensionRegistry,
        results: &tropel_metrics::collector::MetricsResult,
        thresholds: &HashMap<String, tropel_core::config::ThresholdConfig>,
        test_start: Instant,
    ) {
        let summary_value = build_summary_data(results, thresholds, test_start);
        let summary_json = serde_json::to_string(&summary_value).unwrap_or_default();

        // Resolve a driver for the input (k6 scripts declare handleSummary).
        // If no driver resolves (e.g. a Postman/HAR declarative collection),
        // there is no script to call — fall through to --summary-export.
        let input_path = std::path::Path::new(&config.input);
        let bytes = std::fs::read(&config.input).ok();
        let driver = bytes.as_ref().and_then(|b| {
            if let Some(fmt) = &config.input_type {
                registry.resolve_driver_by_id(fmt)
            } else {
                registry.resolve_driver(b)
            }
        });

        let mut handled = false;
        if let (Some(driver), Some(bytes)) = (driver, bytes.as_deref()) {
            if let Some(files) = driver
                .handle_summary(bytes, Some(input_path), &summary_json, &config.env)
                .await
            {
                // k6 semantics: a script-defined handleSummary REPLACES the
                // default summary entirely — even a stdout-only map suppresses
                // the --summary-export fallback.
                handled = true;
                for (name, content) in files {
                    if name == "stdout" {
                        println!("{content}");
                    } else if let Err(e) = std::fs::write(&name, content) {
                        tracing::warn!("handleSummary failed to write '{name}': {e}");
                    } else {
                        tracing::info!("handleSummary wrote '{name}'");
                    }
                }
            }
        }

        // Fallback: --summary-export writes the default JSON summary when no
        // script handleSummary produced any output.
        if !handled {
            if let Some(path) = &config.output.summary_export {
                let pretty = serde_json::to_string_pretty(&summary_value).unwrap_or_default();
                if let Err(e) = std::fs::write(path, pretty) {
                    tracing::warn!("Failed to write summary export to '{:?}': {}", path, e);
                } else {
                    tracing::info!("Summary exported to '{:?}'", path);
                }
            }
        }
    }
}
// Result type


#[derive(Debug)]
pub struct EngineResult {
    pub metrics: tropel_metrics::collector::MetricsResult,
    /// Raw serializable snapshot of the aggregated series (hdr-histogram V2
    /// bytes for Trend metrics). Distributed agents ship this to a
    /// controller; single-node runs can ignore it.
    pub snapshot: tropel_metrics::collector::MetricsSnapshot,
    /// The thresholds actually applied to the run: the job's own thresholds
    /// merged with any script-declared thresholds (e.g. from a k6
    /// `export const options`). Consumers should report/evaluate against this
    /// set rather than the raw `JobConfig.thresholds`.
    pub effective_thresholds: HashMap<String, tropel_core::config::ThresholdConfig>,
}


