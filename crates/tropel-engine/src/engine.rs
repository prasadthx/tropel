use crate::worker::VUWorkerPool;

/// One entry of the per-scenario config list threaded into `run_scenario_vus`.
/// (name, execution, env, tags, start_delay, input_path, exec_name)
type ScenarioConfigTuple = (
    String,
    ExecutionConfig,
    HashMap<String, String>,
    HashMap<String, String>,
    Duration,
    String,
    Option<String>,
);
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
use tropel_executor::scheduler::{VUScheduler, VuLease};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, DriverHttpClient, Output, Protocol, VuContext};
use tropel_http::client::HttpClient;
use tropel_http::AuthSigner;
use tropel_metrics::collector::MetricsCollector;
use tropel_metrics::thresholds::check_abort_on_fail;
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

        let has_stdout = config.output.reporters.iter().any(|r| r == "stdout");
        if has_stdout {
            let rx = sample_tx.subscribe();
            let handle = StreamingStdoutOutput::spawn(rx);
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
        // Tuple: (name, execution, env, tags, start_delay, input_path).
        let scenario_configs: Vec<ScenarioConfigTuple> = if let Some(scs) = declared_scenarios {
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
            .map(|(name, exec, env, tags, delay, input, exec_fn)| {
                let exec = match &segment {
                    Some(seg) => seg.apply(&exec),
                    None => exec,
                };
                (name, exec, env, tags, delay, input, exec_fn)
            })
            .collect();

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
                // Resolve the registered protocol extensions once per scenario
                // and share them across VUs so `grpc://` / `grpcs://` and
                // `ws://` / `wss://` URLs dispatch to them (the runner's
                // scheme checks).
                let grpc_protocol: Option<Arc<dyn Protocol>> = registry_sc
                    .get_protocol("grpc")
                    .map(Arc::from);
                let ws_protocol: Option<Arc<dyn Protocol>> = registry_sc
                    .get_protocol("ws")
                    .map(Arc::from);
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
                            grpc_protocol,
                            ws_protocol,
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

// ══════════════════════════════════════════════════════════════════
// handleSummary(data) — script-emitted custom summaries
// ══════════════════════════════════════════════════════════════════

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

/// Build the k6-style summary data object (`handleSummary(data)` argument)
/// from the aggregated results: per-metric values typed like k6 plus a
/// top-level `thresholds` map (expression → pass/fail) and run state.
fn build_summary_data(
    results: &tropel_metrics::collector::MetricsResult,
    thresholds: &HashMap<String, tropel_core::config::ThresholdConfig>,
    test_start: Instant,
) -> serde_json::Value {
    use serde_json::{json, Map};
    use tropel_metrics::collector::MetricType;
    use tropel_metrics::thresholds::evaluate_thresholds;

    let mut metrics = Map::new();
    for m in &results.metrics {
        let (typ, contains, values) = match m.metric_type {
            MetricType::Counter => (
                "counter",
                "default",
                json!({ "count": m.count }),
            ),
            MetricType::Gauge => (
                "gauge",
                "default",
                json!({ "value": m.last, "min": m.min, "max": m.max, "avg": m.mean }),
            ),
            MetricType::Rate => (
                "rate",
                "default",
                json!({ "rate": m.rate, "count": m.count }),
            ),
            MetricType::Trend => (
                "trend",
                "time",
                json!({
                    "avg": m.mean,
                    "min": m.min,
                    "med": m.p50,
                    "max": m.max,
                    "p(90)": m.p90,
                    "p(95)": m.p95,
                    "p(99)": m.p99,
                    "count": m.count,
                }),
            ),
        };
        metrics.insert(
            m.key.clone(),
            json!({
                "type": typ,
                "contains": contains,
                "values": values,
            }),
        );
    }

    let mut thresholds_map = Map::new();
    for t in evaluate_thresholds(thresholds, results) {
        thresholds_map.insert(t.expression.clone(), json!(t.passed));
    }

    json!({
        "metrics": metrics,
        "root_group": { "name": "", "path": "", "id": "", "groups": [], "checks": [] },
        "options": {},
        "thresholds": thresholds_map,
        "state": {
            "testRunDurationMs": test_start.elapsed().as_millis() as u64,
            "iterations": results.iterations,
            "vusMax": results.vus_max,
            "http_reqs": results.http_reqs,
            "checksTotal": results.checks_total,
            "checksPassed": results.checks_passed,
            "checksFailed": results.checks_failed,
        },
    })
}

// ══════════════════════════════════════════════════════════════════
// Extension output driver
// ══════════════════════════════════════════════════════════════════

/// Drive a registered extension output from the sample stream.
///
/// Subscribes to the metrics broadcast channel, batches samples, and calls
/// the output's `emit()` every `FLUSH_INTERVAL` (or when the batch exceeds
/// `MAX_BATCH`), then `flush()` once when the stream closes (test end).
/// Best-effort: `emit`/`flush` failures are logged, never fatal.
fn spawn_extension_output(
    mut rx: broadcast::Receiver<Sample>,
    output: Box<dyn Output>,
) -> tokio::task::JoinHandle<()> {
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
    const MAX_BATCH: usize = 10_000;

    tokio::spawn(async move {
        let mut batch: Vec<Sample> = Vec::with_capacity(1024);
        let mut tick = tokio::time::interval(FLUSH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                res = rx.recv() => match res {
                    Ok(sample) => {
                        batch.push(sample);
                        if batch.len() >= MAX_BATCH {
                            let b = std::mem::take(&mut batch);
                            if let Err(e) = output.emit(&b).await {
                                tracing::warn!("extension output '{}' emit failed: {e}", output.name());
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::trace!("extension output dropped {n} samples (consumer lag)");
                    }
                },
                _ = tick.tick() => {
                    if !batch.is_empty() {
                        let b = std::mem::take(&mut batch);
                        if let Err(e) = output.emit(&b).await {
                            tracing::warn!("extension output '{}' emit failed: {e}", output.name());
                        }
                    }
                }
            }
        }

        // Final flush on stream close.
        if !batch.is_empty() {
            if let Err(e) = output.emit(&batch).await {
                tracing::warn!("extension output '{}' final emit failed: {e}", output.name());
            }
        }
        if let Err(e) = output.flush().await {
            tracing::warn!("extension output '{}' flush failed: {e}", output.name());
        }
    })
}

// ══════════════════════════════════════════════════════════════════
// Result type
// ══════════════════════════════════════════════════════════════════

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
#[allow(clippy::too_many_arguments)]
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
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,

    test_start: Instant,
    grpc_protocol: Option<Arc<dyn Protocol>>,
    ws_protocol: Option<Arc<dyn Protocol>>,
    control_port: Option<u16>,
    rps_limiter: Option<Arc<tropel_http::RpsLimiter>>,
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
    let rps_limiter_c = rps_limiter.clone();
    let metrics_c = metrics.clone();
    let scenario_c = scenario.clone();
    let stop_c = stop_signal.clone();
    let data_rows_c = data_rows.clone();
    let pool_c = pool.clone();
    let http_cfg_c = http_cfg.clone();
    let tls_cfg_c = tls_cfg.clone();
    let vu_env_c = vu_env.clone();
    let sc_name_c = sc_name.clone();
    let sc_tags_c = sc_tags.clone();

    let abort_monitor = spawn_abort_coordinator(
        has_abort_thresholds,
        metrics.clone(),
        executor.control_handle(),
        thresholds.clone(),
        test_start,
    );

    executor.run(move |sched, vu_id| {
        let metrics = metrics_c.clone();
        let scenario = scenario_c.clone();
        let stop = stop_c.clone();
        let total_iters = total_iterations;
        let vu_env = vu_env_c.clone();
        let data_rows = data_rows_c.clone();
        let http_cfg = http_cfg_c.clone();
        let rps_vu = rps_limiter_c.clone();
        let tls_cfg = tls_cfg_c.clone();
        let pool = pool_c.clone();
        let think_time = think_time_cfg.clone();
        let sc_name_vu = sc_name_c.clone();
        let is_per_vu_iterations = is_per_vu_iterations;
        let sc_tags = sc_tags_c.clone();
        let executor_name = executor_name.clone();
        let grpc_protocol_vu = grpc_protocol.clone();
        let ws_protocol_vu = ws_protocol.clone();

        // 1-VU-per-task: pin this VU to its own dedicated worker thread so a
        // blocking script `sleep()` (std::thread::sleep) never freezes a
        // co-located VU — there is no co-located VU.
        let handle = pool.spawn_vu(vu_id, async move {
            // Panic-safe lease: increments `active_vus` now and decrements on
            // drop — even if the task panics mid-flight, the counter can't
            // leak and the engine's drain loop can't hang forever.
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
                .with_grpc_protocol(grpc_protocol_vu.clone())
                .with_ws_protocol(ws_protocol_vu.clone())
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

                // Externally-controlled pause gate: while paused, hold here
                // instead of starting the next iteration. Level-triggered —
                // the loop re-checks is_paused each wake, so an edge-triggered
                // resume notify can't be missed.
                while sched.is_paused()
                    && !sched.is_stop_requested()
                    && !sched.is_force_stop_requested()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                if sched.is_stop_requested() || sched.is_force_stop_requested() { break; }

                // Shared-iterations mode: PRE-CLAIM this iteration slot
                // atomically (lock-free CAS) before running, so concurrent
                // VUs can never overshoot the budget by up to vus−1 — the
                // old run-then-check let every VU read the same
                // under-budget snapshot and start one more iteration.
                if !is_per_vu_iterations && total_iters != u64::MAX {
                    if !sched.try_claim_shared_iteration(total_iters) { break; }
                }

                vu_sample_counter += 1;
                if vu_sample_counter % 100 == 0 {
                    let active = sched.active_vus().await;
                    let peak = sched.peak_vus();
                    utils_emit_vus_metrics(&metrics, active, peak, &sc_tags).await;
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
                    let empty_tags = Arc::new(TagMap::new());
                    iter_samples.push(Sample { metric: "iterations".into(), value: 1.0, tags: empty_tags.clone(), timestamp: now, sample_type: tropel_core::types::SampleType::Counter });
                    iter_samples.push(Sample { metric: "iteration_duration".into(), value: iter_dur.as_micros() as f64, tags: empty_tags, timestamp: now, sample_type: tropel_core::types::SampleType::Trend });
                    // Merge per-scenario tags into every sample so tag-scoped
                    // thresholds (e.g. {scenario=load}) work end-to-end.
                    merge_scenario_tags(&mut iter_samples, &sc_tags);
                    metrics.record_batch(&iter_samples).await;

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
                    }
                }
            }
        });
        handle
    }).await.ok();

    // Stop the single abort coordinator — the run has finished, so a
    // lingering 2s poller would otherwise keep the metrics aggregator alive.
    if let Some(monitor) = abort_monitor {
        monitor.abort();
    }

    // Emit a guaranteed final vus/vus_max sample. The periodic sampler only
    // fires every 100 iterations per VU, so a short run would otherwise emit
    // NO vus/vus_max samples and the summary would read vus_max: 0. The peak
    // is the pre-allocated config peak (k6 semantics); active is deliberately
    // 0 here because all VUs have joined — vus_max is what the summary needs,
    // and vus is only consumed through the collector's max-of-gauges, so the
    // trailing 0 is harmless.
    utils_emit_vus_metrics(&metrics, 0, executor.peak_vus(), &sc_tags).await;

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
                    tags: Arc::new(dropped_tags),
                    timestamp: std::time::SystemTime::now(),
                    sample_type: tropel_core::types::SampleType::Counter,
                })
                .await;
        }
    }

    // Bound the drain: a panicked VU (leaked `active_vus`) or timeout-less I/O
    // must not hang the run forever. Wait up to 30s for stragglers, then warn
    // and proceed to shutdown. The panic-safe VuLease guard plus the global
    // request timeout (HttpConfig.request_timeout) make the 30s bound a
    // backstop rather than the expected path.
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

#[allow(clippy::too_many_arguments)]
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
    data_rows: std::sync::Arc<Vec<HashMap<String, serde_json::Value>>>,

    test_start: Instant,
    input_path: &str,
    registry: Arc<ExtensionRegistry>,
    control_port: Option<u16>,
    rps_limiter: Option<Arc<tropel_http::RpsLimiter>>,
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

    // Runtime control API — same wiring as run_scenario_vus.
    let control_server = if matches!(exec_cfg, ExecutionConfig::ExternallyControlled { .. }) {
        control_port.map(|port| {
            let sched_handle = executor.control_handle();
            tokio::spawn(crate::control_api::serve_control_api(port, sched_handle))
        })
    } else {
        None
    };

    let has_abort_thresholds = thresholds.values().any(|t| t.abort_on_fail);
    let think_time_cfg = extract_think_time(&exec_cfg);
    let driver_id = driver.id().to_string();
    // k6-style executor name (e.g. "constant-vus") — backs exec.scenario.executor().
    let executor_name = exec_cfg.executor_name().to_string();

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
    let vu_env_c = vu_env.clone();
    let sc_name_c = sc_name.clone();
    let sc_tags_c = sc_tags.clone();
    let sc_exec_c = sc_exec.clone();
    let driver_id_c = driver_id.clone();
    let input_bytes_c = input_bytes.clone();
    let input_p_c = input_p.clone();
    let registry_c = registry.clone();
    let rps_limiter_c = rps_limiter.clone();

    let abort_monitor = spawn_abort_coordinator(
        has_abort_thresholds,
        metrics.clone(),
        executor.control_handle(),
        thresholds.clone(),
        test_start,
    );

    executor.run(move |sched, vu_id| {
        let metrics = metrics_c.clone();
        let stop = stop_c.clone();
        let total_iters = total_iterations;
        let vu_env = vu_env_c.clone();
        let data_rows = data_rows_c.clone();
        let http_cfg = http_cfg_c.clone();
        let tls_cfg = tls_cfg_c.clone();
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
        let rps_vu = rps_limiter_c.clone();
        let executor_name = executor_name.clone();

        // 1-VU-per-task: pin this VU to its own dedicated worker thread (see
        // run_scenario_vus for the rationale — blocking sleep() must never
        // freeze a co-located VU).
        let handle = pool.spawn_vu(vu_id, async move {
            // Panic-safe lease: increments `active_vus` now and decrements on
            // drop — even if the task panics mid-flight, the counter can't
            // leak and the engine's drain loop can't hang forever.
            let _lease = VuLease::acquire(&sched);

            // Re-resolve driver from registry so each VU gets a fresh instance
            let driver = match registry.resolve_driver_by_id(&driver_id) {
                Some(d) => d,
                None => {
                    tracing::error!("VU {}: Driver '{}' not found in registry", vu_id, driver_id);
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
            let http_client_handle: Arc<dyn DriverHttpClient + Send + Sync> = Arc::new(DriverHttpClientImpl { client });

            let mut iteration_index = 0u64;
            let mut vu_sample_counter: u64 = 0;

            loop {
                if sched.is_force_stop_requested() || sched.is_stop_requested() { break; }
                {
                    let active = sched.active_vus().await;
                    if sched.try_claim_ramp_down(active).await { break; }
                }

                // Externally-controlled pause gate: while paused, hold here
                // instead of starting the next iteration. Level-triggered —
                // the loop re-checks is_paused each wake, so an edge-triggered
                // resume notify can't be missed.
                while sched.is_paused()
                    && !sched.is_stop_requested()
                    && !sched.is_force_stop_requested()
                {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                if sched.is_stop_requested() || sched.is_force_stop_requested() { break; }

                // Shared-iterations mode: PRE-CLAIM this iteration slot
                // atomically (lock-free CAS) before running, so concurrent
                // VUs can never overshoot the budget by up to vus−1 — the
                // old run-then-check let every VU read the same
                // under-budget snapshot and start one more iteration.
                if !is_per_vu_iterations && total_iters != u64::MAX {
                    if !sched.try_claim_shared_iteration(total_iters) { break; }
                }

                vu_sample_counter += 1;
                if vu_sample_counter % 100 == 0 {
                    let active = sched.active_vus().await;
                    let peak = sched.peak_vus();
                    utils_emit_vus_metrics(&metrics, active, peak, &sc_tags).await;
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
                    // Populate exec.* context so drivers can expose
                    // exec.scenario/executor/vu/instance to scripts.
                    ctx.set_exec_context(
                        executor_name.clone(),
                        sched.total_iterations().await,
                        sched.active_vus().await,
                    );

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
                    let empty_tags = Arc::new(TagMap::new());
                    let mut iter_samples = vec![
                        Sample { metric: "iterations".into(), value: 1.0, tags: empty_tags.clone(), timestamp: now, sample_type: tropel_core::types::SampleType::Counter },
                        Sample { metric: "iteration_duration".into(), value: iter_dur.as_micros() as f64, tags: empty_tags, timestamp: now, sample_type: tropel_core::types::SampleType::Trend },
                    ];
                    merge_scenario_tags(&mut iter_samples, &sc_tags);
                    metrics.record_batch(&iter_samples).await;

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
                    }
                }
            }
        });
        handle
    }).await.ok();

    // Stop the single abort coordinator — the run has finished, so a
    // lingering 2s poller would otherwise keep the metrics aggregator alive.
    if let Some(monitor) = abort_monitor {
        monitor.abort();
    }

    // Emit a guaranteed final vus/vus_max sample. The periodic sampler only
    // fires every 100 iterations per VU, so a short run would otherwise emit
    // NO vus/vus_max samples and the summary would read vus_max: 0. The peak
    // is the pre-allocated config peak (k6 semantics); active is deliberately
    // 0 here because all VUs have joined — vus_max is what the summary needs,
    // and vus is only consumed through the collector's max-of-gauges, so the
    // trailing 0 is harmless.
    utils_emit_vus_metrics(&metrics, 0, executor.peak_vus(), &sc_tags).await;

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
                    tags: Arc::new(dropped_tags),
                    timestamp: std::time::SystemTime::now(),
                    sample_type: tropel_core::types::SampleType::Counter,
                })
                .await;
        }
    }

    // Bound the drain: a panicked VU (leaked `active_vus`) or timeout-less I/O
    // must not hang the run forever. Wait up to 30s for stragglers, then warn
    // and proceed to shutdown. The panic-safe VuLease guard plus the global
    // request timeout (HttpConfig.request_timeout) make the 30s bound a
    // backstop rather than the expected path.
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

// ══════════════════════════════════════════════════════════════════
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

    // All shim libraries are concatenated at COMPILE TIME (concat!) into a
    // single &'static str and evaluated with ONE bootstrap eval per VU. Each
    // separate bootstrap_library() call resets the JS interrupt timer, parses
    // the source, and pumps the promise queue, so N calls cost N × that
    // overhead. The shim sources are static include_str! strings byte-identical
    // for every VU, so one combined eval is semantically equivalent while
    // cutting the per-VU bootstrap cost ~5× and allocating nothing at runtime.
    // (rquickjs 0.12 exposes no public script-bytecode API to share compiled
    // shims across VU contexts — only Module bytecode, which doesn't apply to
    // plain-script shims — so a single compile-time-bundled eval is the safe,
    // verifiable win.)
    const JS_SHIM_BUNDLE: &str = concat!(
        "// ==== shim: pm-api ====\n",
        include_str!("../../../js/pm-api/pm.js"),
        "\n",
        "// ==== shim: chai-shim ====\n",
        include_str!("../../../js/chai/chai-shim.js"),
        "\n",
        "// ==== shim: lodash-shim ====\n",
        include_str!("../../../js/lodash/lodash-shim.js"),
        "\n",
        "// ==== shim: cryptojs-shim ====\n",
        include_str!("../../../js/cryptojs-shim/cryptojs.js"),
        "\n",
        "// ==== shim: exec-shim ====\n",
        include_str!("../../../js/exec/exec.js"),
    );
    if let Err(e) = ctx.bootstrap_library(JS_SHIM_BUNDLE).await {
        tracing::warn!(
            "VU {}: Failed to bootstrap JS shim bundle: {}",
            vu_id,
            e
        );
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
