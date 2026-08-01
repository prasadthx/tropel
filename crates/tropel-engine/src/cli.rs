//! # CLI entry point
//!
//! Reusable CLI logic that is called by both the standard `tropel` binary
//! and custom binaries built with `tropel build --with <ext>`.
//!
//! This module handles argument parsing, tracing initialization, and
//! dispatching to the appropriate engine subcommand. Custom binaries
//! simply call `tropel_engine::cli::run_cli()` from their `fn main()`.

use std::collections::HashMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use std::sync::Arc;
use tropel_core::config::*;
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;
use tropel_metrics::thresholds::evaluate_thresholds;

use crate::config_file::PartialConfig;
use crate::engine::Engine;

/// Tropel — A high-performance load-testing framework.
#[derive(Parser, Debug)]
#[command(name = "tropel", version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Run a load test
    Run {
        /// Path to the input file (collection, HAR, script, etc.)
        input: PathBuf,

        /// Input format (auto-detect if not specified).
        /// Use `tropel extensions` to list available formats.
        #[arg(long = "format")]
        format: Option<String>,

        /// Number of virtual users (overrides collection config)
        #[arg(short = 'u', long = "vus")]
        vus: Option<u32>,

        /// Test duration (e.g. "30s", "5m")
        #[arg(short = 'd', long = "duration")]
        duration: Option<String>,

        /// Environment variable (can be specified multiple times: -e KEY=VALUE)
        #[arg(short = 'e', long = "env")]
        env: Vec<String>,

        /// Environment file (JSON)
        #[arg(short = 'E', long = "env-file")]
        env_file: Option<PathBuf>,

        /// Data file (CSV or JSON)
        #[arg(short = 'D', long = "data-file")]
        data_file: Option<PathBuf>,

        /// JSON config file (partial JobConfig overlay). Merged with
        /// precedence: explicit CLI flags > config file > K6_* env > defaults.
        #[arg(long = "config")]
        config: Option<PathBuf>,

        /// Report format (stdout, json, csv)
        #[arg(short = 'r', long = "reporter", default_value = "stdout")]
        reporter: Vec<String>,

        /// Output file path (for json/csv reporters)
        #[arg(short = 'o', long = "output")]
        output: Option<PathBuf>,

        /// Prometheus remote-write endpoint (e.g. http://localhost:9090)
        #[arg(long = "prometheus-url")]
        prometheus_url: Option<String>,

        /// OTLP/HTTP collector endpoint (e.g. http://localhost:4318)
        #[arg(long = "otlp-endpoint")]
        otlp_endpoint: Option<String>,

        /// k6-style summary export path: writes the aggregated summary data
        /// object as JSON (when no script handleSummary overrides output)
        #[arg(long = "summary-export")]
        summary_export: Option<PathBuf>,

        /// NDJSON streaming output file (k6 `--out json=file` equivalent):
        /// every sample is appended as one JSON line during the run
        #[arg(long = "json-stream")]
        json_stream: Option<PathBuf>,

        /// StatsD / Datadog agent address (host:port, e.g. localhost:8125)
        /// for streaming datagram output
        #[arg(long = "statsd-addr")]
        statsd_addr: Option<String>,

        /// InfluxDB line-protocol UDP address (host:port, e.g. localhost:8089)
        /// for streaming line-protocol datagrams
        #[arg(long = "influxdb-addr")]
        influxdb_addr: Option<String>,

        /// Deterministic workload partition for this node, as "from:to"
        /// (e.g. "0:1/3") — k6 `executionSegment`. Combined with
        /// --execution-segment-sequence this node runs only its fraction of
        /// the workload (VUs/iterations/rate), scaled deterministically.
        #[arg(long = "execution-segment")]
        execution_segment: Option<String>,

        /// Full sequence of segment boundaries shared by all cooperating
        /// nodes, e.g. "0,1/3,2/3,1" — k6 `executionSegmentSequence`.
        #[arg(long = "execution-segment-sequence")]
        execution_segment_sequence: Option<String>,

        /// Threshold expression (can be specified multiple times)
        #[arg(short = 't', long = "threshold")]
        threshold: Vec<String>,

        /// Insecure TLS (skip certificate verification)
        #[arg(short = 'k', long = "insecure")]
        insecure: bool,

        /// Show verbose output
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        /// Run mode: constant-vus, ramping-vus, shared-iterations, arrival-rate
        /// (optional — when absent, a k6 script's own `export const options`
        /// drives the load profile; passing this flag makes the CLI profile win)
        #[arg(short = 'm', long = "mode")]
        mode: Option<String>,

        /// Ramping stages (JSON array, for ramping-vus mode)
        #[arg(long = "stages")]
        stages: Option<String>,

        /// Iterations (for shared-iterations mode)
        #[arg(long = "iterations")]
        iterations: Option<u64>,

        /// Subprocess adapter command (e.g. `--subprocess-adapter "python3 my-adapter.py"`).
        /// Runs the command for each detect/parse call, passing bytes on stdin
        /// and reading a JSON Scenario from stdout.
        /// The adapter is registered as `subprocess:<cmd>` (use with `--format`)
        /// and is also probed during content auto-detection, like WASM plugins.
        /// Each call is bounded by a 30s timeout and a 16 MiB output cap.
        #[arg(long = "subprocess-adapter")]
        subprocess_adapter: Vec<String>,

        /// Directory of WASM plugins (`.wasm`) to load as input adapters.
        /// Modules are AOT-precompiled to `.cwasm` next to the source and
        /// registered under `wasm:<plugin_id>`; content auto-detection probes
        /// them too. Example: `--plugins-dir ./plugins`.
        #[arg(long = "plugins-dir")]
        plugins_dir: Option<PathBuf>,

        /// Build extensions (for `tropel build` — used here for uniform parsing)
        #[arg(long = "with")]
        with_extensions: Vec<String>,
    },

    /// List available input formats and their capabilities
    Extensions {
        /// Optional directory of WASM plugins to include in the listing.
        #[arg(long = "plugins-dir")]
        plugins_dir: Option<PathBuf>,
    },

    /// Build a custom Tropel binary with extensions
    Build {
        /// Extension crates to include.
        /// Forms: `name` or `name@1.2.3` (crates.io), `./path` (local dir),
        /// `https://host/user/repo` or `git@host:user/repo.git` (git),
        /// and git refs: `git-url@main` (branch), `git-url@v1.2.3` (tag),
        /// `git-url@<sha>` (rev).
        /// Example: `--with tropel-x-grpc --with ./my-ext --with https://github.com/u/r@v0.2.0`
        #[arg(long = "with", required = true)]
        with: Vec<String>,

        /// Output binary path
        #[arg(short = 'o', long = "output", default_value = "./tropel-custom")]
        output: Option<PathBuf>,

        /// Build in debug mode (default: release)
        #[arg(long = "debug")]
        debug: bool,
    },

    /// Print the version and build information
    Version,
}

impl Cli {
    pub fn verbose(&self) -> bool {
        match &self.command {
            Commands::Run { verbose, .. } => *verbose,
            _ => false,
        }
    }
}

/// Run the CLI — parses args, initializes tracing, dispatches to engine.
///
/// This is the single entry point that both the standard `tropel` binary
/// and custom `tropel build` binaries call from their `fn main()`.
pub async fn run_cli() -> Result<()> {
    // Force-link built-in adapters/drivers so their `inventory::submit!`
    // registrations survive linker dead-stripping (see `builtins` module).
    crate::builtins::register_builtins();

    let cli = Cli::parse();

    // Initialize tracing
    let filter = if cli.verbose() {
        "tropel=debug,tropel_engine=debug"
    } else {
        "tropel=info"
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .init();

    match cli.command {
        Commands::Run { .. } => run_command(cli).await,
        Commands::Extensions { plugins_dir } => list_extensions(plugins_dir.as_deref()).await,
        Commands::Build {
            ref with,
            ref output,
            debug,
        } => {
            build_custom(
                with,
                output
                    .as_deref()
                    .unwrap_or(&PathBuf::from("./tropel-custom")),
                !debug,
            )
            .await
        }
        Commands::Version => print_version(),
    }
}

async fn run_command(cli: Cli) -> Result<()> {
    let Commands::Run {
        input,
        format,
        vus,
        duration,
        env,
        env_file,
        data_file,
        config,
        reporter,
        output,
        threshold,
        insecure,
        verbose: _,
        mode,
        stages,
        iterations,
        prometheus_url,
        otlp_endpoint,
        summary_export,
        json_stream,
        statsd_addr,
        influxdb_addr,
        execution_segment,
        execution_segment_sequence,
        subprocess_adapter,
        plugins_dir,
        ..
    } = &cli.command
    else {
        return Err(TropelError::Other("Not a Run command".into()));
    };

    let input = input.clone();
    let format = format.clone();
    let vus = *vus;
    let duration = duration.clone();
    let env = env.clone();
    let env_file = env_file.clone();
    let data_file = data_file.clone();
    let reporters = reporter.clone();
    let output = output.clone();
    let prometheus_url = prometheus_url.clone();
    let otlp_endpoint = otlp_endpoint.clone();
    let summary_export = summary_export.clone();
    let json_stream = json_stream.clone();
    let statsd_addr = statsd_addr.clone();
    let influxdb_addr = influxdb_addr.clone();
    let execution_segment = execution_segment.clone();
    let execution_segment_sequence = execution_segment_sequence.clone();
    let thresholds = threshold.clone();
    let insecure = *insecure;
    // `mode` is now optional so we can tell whether the user explicitly chose
    // a load profile (mode/vus/duration/stages/iterations flags). When none of
    // them are set, a k6 script's own `export const options` may drive the run.
    let mode_explicit = mode.is_some();
    let mode = mode.clone().unwrap_or_else(|| "constant-vus".to_string());
    let stages = stages.clone();
    let iterations = *iterations;

    tracing::info!("Tropel v{}", env!("CARGO_PKG_VERSION"));
    tracing::info!("Input: {}", input.display());

    // Parse environment variables
    let mut env_map: HashMap<String, String> = HashMap::new();
    for e in &env {
        if let Some((key, value)) = e.split_once('=') {
            env_map.insert(key.to_string(), value.to_string());
        }
    }

    // Load environment file if provided
    if let Some(env_path) = &env_file {
        match std::fs::read_to_string(env_path) {
            Ok(content) => {
                if let Ok(postman_env) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(values) = postman_env.get("values").and_then(|v| v.as_array()) {
                        for entry in values {
                            if let (Some(key), Some(value)) = (
                                entry.get("key").and_then(|k| k.as_str()),
                                entry.get("value").and_then(|v| v.as_str()),
                            ) {
                                let enabled = entry
                                    .get("enabled")
                                    .and_then(|e| e.as_bool())
                                    .unwrap_or(true);
                                if enabled {
                                    env_map.insert(key.to_string(), value.to_string());
                                }
                            }
                        }
                    } else if let Ok(flat_env) =
                        serde_json::from_value::<HashMap<String, String>>(postman_env.clone())
                    {
                        env_map.extend(flat_env);
                    } else {
                        tracing::warn!("Unrecognized env-file format in '{}': expected Postman env or flat JSON", env_path.display());
                    }
                }
            }
            Err(e) => {
                tracing::warn!("Failed to read env-file '{}': {}", env_path.display(), e);
            }
        }
    }

    // Build execution config
    let execution = match mode.as_str() {
        "ramping-vus" => {
            let start_vus = vus.unwrap_or(1);
            let stages_list = stages
                .as_ref()
                .and_then(|s| serde_json::from_str::<Vec<Stage>>(s).ok())
                .unwrap_or_else(|| {
                    vec![Stage {
                        duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
                        target: vus.unwrap_or(10),
                    }]
                });
            ExecutionConfig::RampingVus {
                stages: stages_list,
                start_vus,
                graceful_ramp_down: Some("30s".to_string()),
                graceful_stop: Some("30s".to_string()),
                think_time: ThinkTimeConfig::default(),
            }
        }
        "shared-iterations" => ExecutionConfig::SharedIterations {
            iterations: iterations.unwrap_or(100),
            max_duration: duration.clone(),
            vus: vus.unwrap_or(1),
            graceful_stop: Some("30s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
        "arrival-rate" | "constant-arrival-rate" => ExecutionConfig::ConstantArrivalRate {
            rate: vus.unwrap_or(1) as f64,
            time_unit: "1s".to_string(),
            duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
            pre_alloc_vus: 1,
            max_vus: vus.unwrap_or(10).max(10),
            graceful_stop: Some("30s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
        _ => ExecutionConfig::ConstantVus {
            vus: vus.unwrap_or(1),
            duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
            graceful_stop: Some("30s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
    };

    // Parse thresholds
    let mut threshold_map: HashMap<String, ThresholdConfig> = HashMap::new();
    for t in &thresholds {
        let name = format!("threshold_{}", threshold_map.len());
        threshold_map.insert(
            name,
            ThresholdConfig {
                expression: t.clone(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
    }

    // Load data file if provided
    let iteration_data = if let Some(data_path) = &data_file {
        match load_data_file(data_path) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!("Failed to load data-file '{}': {}", data_path.display(), e);
                vec![]
            }
        }
    } else {
        vec![]
    };

    // The user provided a load profile when they passed any of the load
    // flags. Otherwise (bare `tropel run script.js`) a k6 script's own
    // `export const options` is allowed to drive the run.
    let load_profile_explicit = vus.is_some()
        || duration.is_some()
        || mode_explicit
        || stages.is_some()
        || iterations.is_some();

    // Load config overlays: `K6_*` env vars first, then the `--config` JSON
    // file (file wins over env, explicit CLI flags win over both).
    let mut overlay = PartialConfig::from_env();
    if let Some(config_path) = config {
        let file_cfg = PartialConfig::load_from_file(config_path)?;
        overlay = merge_partial(overlay, file_cfg);
    }

    // Build the full job config
    let mut config = JobConfig {
        input: input.to_string_lossy().to_string(),
        input_type: format.clone(),
        execution,
        execution_explicit: load_profile_explicit,
        execution_segment,
        execution_segment_sequence,
        env: env_map,
        iteration_data,
        output: OutputConfig {
            reporters: reporters.clone(),
            output_file: output.map(|p| p.to_string_lossy().to_string()),
            prometheus_remote_write_url: prometheus_url,
            otlp_endpoint,
            summary_export: summary_export.map(|p| p.to_string_lossy().to_string()),
            json_stream: json_stream.map(|p| p.to_string_lossy().to_string()),
            statsd_addr,
            influxdb_addr,
            ..Default::default()
        },
        thresholds: threshold_map,
        tls: TlsConfig {
            insecure_skip_verify: insecure,
            ..Default::default()
        },
        ..Default::default()
    };

    // ── Apply the overlay (CLI flags win; overlay fills gaps) ──
    // Compute BEFORE the &mut borrow (CLI --data-file already loaded
    // iteration_data above).
    let cli_iteration_data_empty = config.iteration_data.is_empty();
    apply_overlay(
        &mut config,
        overlay,
        &reporters,
        insecure,
        load_profile_explicit,
        cli_iteration_data_empty,
    );

    tracing::info!("Execution config: {:?}", config.execution); // Create the engine with extension registry
    let mut registry = ExtensionRegistry::new();

    // Register any subprocess adapters specified via --subprocess-adapter
    for cmd in subprocess_adapter {
        let id = format!("subprocess:{}", cmd);
        tracing::info!("Registering subprocess adapter (command: {})", cmd);
        let cmd_clone = cmd.clone();
        registry.register_adapter_factory(
            &id,
            Arc::new(move || Box::new(tropel_input_subprocess::SubprocessAdapter::new(&cmd_clone))),
        );
    }

    // Register WASM plugins from --plugins-dir (Tier 2 no-recompile adapters).
    if let Some(dir) = plugins_dir {
        let adapters = tropel_wasm::discover_plugins(dir);
        tracing::info!(
            "Loaded {} WASM plugin(s) from {}",
            adapters.len(),
            dir.display()
        );
        for adapter in adapters {
            let id = format!("wasm:{}", adapter.plugin_id());
            let adapter = adapter.clone();
            registry.register_adapter_factory(&id, Arc::new(move || Box::new(adapter.clone())));
        }
    }

    let engine = Engine::new(registry);
    let result = engine.run(&config).await?;

    tracing::info!(
        "Load test completed: {} total requests",
        result.metrics.http_reqs
    );
    tracing::info!(
        "Checks: {}/{} passed",
        result.metrics.checks_passed,
        result.metrics.checks_total
    );

    // Evaluate thresholds and drive exit code. Uses the engine's EFFECTIVE
    // threshold set (job thresholds merged with script-declared ones, e.g.
    // k6 `export const options` thresholds) so k6 SLOs are reported too.
    let threshold_results = evaluate_thresholds(&result.effective_thresholds, &result.metrics);
    let mut any_failed = false;
    for tr in &threshold_results {
        if tr.passed {
            tracing::info!(
                "  ✓ Threshold '{}': {:.2} {} {:.2} (PASS)",
                tr.name,
                tr.actual,
                tr.expression.split_whitespace().nth(1).unwrap_or("<?>"),
                tr.threshold
            );
        } else {
            tracing::error!(
                "  ✗ Threshold '{}': {:.2} {} {:.2} (FAIL)",
                tr.name,
                tr.actual,
                tr.expression.split_whitespace().nth(1).unwrap_or("<?>"),
                tr.threshold
            );
            any_failed = true;
            if tr.abort_on_fail {
                tracing::error!("Aborting due to threshold '{}'", tr.name);
                return Err(TropelError::Other(format!(
                    "Threshold '{}' failed (abort-on-fail)",
                    tr.name
                )));
            }
        }
    }

    if any_failed {
        Err(TropelError::Other("One or more thresholds failed".into()))
    } else {
        Ok(())
    }
}

/// Apply the merged overlay onto the CLI-built `JobConfig`.
///
/// Precedence: explicit CLI flags > config file > K6_* env > defaults.
/// `cli_reporters` is the CLI `--reporter` value (default `["stdout"]` —
/// treated as "not explicitly set" so a config file or `K6_REPORTER` can
/// replace it). `cli_load_profile_explicit` is true when the user passed any
/// load-profile flag (`-u`/`-d`/`-m`/`--stages`/`--iterations`) — in that
/// case the overlay's execution is ignored (CLI wins).
fn apply_overlay(
    config: &mut JobConfig,
    overlay: PartialConfig,
    cli_reporters: &[String],
    cli_insecure: bool,
    cli_load_profile_explicit: bool,
    iteration_data_is_empty: bool,
) {
    // input_type: CLI --format wins; else overlay.
    if config.input_type.is_none() {
        config.input_type = overlay.input_type.clone();
    }
    // Load profile: only when the user passed no explicit load flags. This
    // mirrors the k6 `export const options` behavior — a config file or
    // K6_* env declaring an execution marks the profile explicit so scripts
    // don't silently override it.
    if !cli_load_profile_explicit {
        if let Some(exec) = overlay.execution {
            tracing::info!("Using execution from config file / K6_* env: {:?}", exec);
            config.execution = exec;
            config.execution_explicit = true;
        }
    }
    // Execution segments: CLI flags win; overlay fills gaps. Applied later
    // by the engine to scale each scenario's workload deterministically.
    if config.execution_segment.is_none() {
        config.execution_segment = overlay.execution_segment.clone();
    }
    if config.execution_segment_sequence.is_none() {
        config.execution_segment_sequence = overlay.execution_segment_sequence.clone();
    }
    // Env: overlay vars fill in, CLI -e already present wins (insert only
    // keys the overlay has that the CLI env doesn't).
    for (k, v) in &overlay.env {
        config.env.entry(k.clone()).or_insert_with(|| v.clone());
    }
    // Thresholds: overlay adds; CLI keys (threshold_N) don't collide.
    for (k, v) in &overlay.thresholds {
        config.thresholds.entry(k.clone()).or_insert_with(|| v.clone());
    }
    // Output: CLI flags win; overlay fills reporters/file/urls. The CLI's
    // --reporter has a "stdout" default on a Vec, so `is_empty()` would never
    // fire — instead treat the default ["stdout"] as "not explicitly set" so
    // a config file or K6_REPORTER can replace it.
    if let Some(out) = &overlay.output {
        let cli_reporters_default = cli_reporters.len() == 1 && cli_reporters[0] == "stdout";
        if cli_reporters_default && !out.reporters.is_empty() {
            config.output.reporters = out.reporters.clone();
        }
        if config.output.output_file.is_none() {
            config.output.output_file = out.output_file.clone();
        }
        if config.output.prometheus_remote_write_url.is_none() {
            config.output.prometheus_remote_write_url = out.prometheus_remote_write_url.clone();
        }
        if config.output.otlp_endpoint.is_none() {
            config.output.otlp_endpoint = out.otlp_endpoint.clone();
        }
        if config.output.summary_export.is_none() {
            config.output.summary_export = out.summary_export.clone();
        }
        if config.output.json_stream.is_none() {
            config.output.json_stream = out.json_stream.clone();
        }
        if config.output.statsd_addr.is_none() {
            config.output.statsd_addr = out.statsd_addr.clone();
        }
        if config.output.influxdb_addr.is_none() {
            config.output.influxdb_addr = out.influxdb_addr.clone();
        }
        // The CLI never sets summary/trends (both default false), so the
        // overlay's explicit values apply directly.
        config.output.summary = out.summary;
        config.output.trends = out.trends;
    }
    // Named scenarios from the overlay (CLI has no scenarios flag, so the
    // overlay always wins here).
    if let Some(scenarios) = &overlay.scenarios {
        if !scenarios.is_empty() {
            config.scenarios = scenarios.clone();
        }
    }
    // HTTP: overlay applies only when the user didn't set flags (the CLI
    // exposes no direct http flags, so overlay always wins here).
    if let Some(http) = &overlay.http {
        config.http = http.clone();
    }
    // TLS: CLI --insecure wins for skip-verify; overlay fills the rest.
    if let Some(tls) = &overlay.tls {
        let mut merged = tls.clone();
        if cli_insecure {
            merged.insecure_skip_verify = true;
        }
        config.tls = merged;
    }
    // Globals / collection vars / iteration data / extensions.
    if !overlay.globals.is_empty() {
        config.globals.extend(overlay.globals.clone());
    }
    if !overlay.collection_vars.is_empty() {
        config.collection_vars.extend(overlay.collection_vars.clone());
    }
    // Data file: `--data-file` (CLI) already loaded into iteration_data; a
    // config-file data_file is run through the same loader so it isn't dead.
    // When the overlay sets BOTH a data_file path and inline iteration_data,
    // inline data wins — warn so the surprise is explicit in every path.
    if overlay.data_file.is_some() && !overlay.iteration_data.is_empty() {
        tracing::warn!(
            "Config file sets both data_file and iteration_data — inline iteration_data wins"
        );
    } else if iteration_data_is_empty {
        if let Some(data_path) = overlay.data_file {
            match load_data_file(&std::path::PathBuf::from(&data_path)) {
                Ok(data) => config.iteration_data = data,
                Err(e) => {
                    tracing::warn!("Failed to load config-file data_file '{}': {}", data_path, e);
                }
            }
        }
    }
    if !overlay.iteration_data.is_empty() {
        config.iteration_data = overlay.iteration_data.clone();
    }
    if !overlay.extensions.is_empty() {
        config.extensions.extend(overlay.extensions.clone());
    }
}

/// Merge two partial configs: `file` wins over `base` (env) on a per-field
/// basis. Explicit CLI flags are applied AFTER this in `run_command`, so the
/// final precedence is: CLI flags > config file > K6_* env > defaults.
fn merge_partial(base: PartialConfig, file: PartialConfig) -> PartialConfig {
    PartialConfig {
        input_type: file.input_type.or(base.input_type),
        execution: file.execution.or(base.execution),
        scenarios: file.scenarios.or(base.scenarios),
        env: base.env.into_iter().chain(file.env).collect(),
        globals: base.globals.into_iter().chain(file.globals).collect(),
        collection_vars: base
            .collection_vars
            .into_iter()
            .chain(file.collection_vars)
            .collect(),
        data_file: file.data_file.or(base.data_file),
        iteration_data: if file.iteration_data.is_empty() {
            base.iteration_data
        } else {
            file.iteration_data
        },
        thresholds: base.thresholds.into_iter().chain(file.thresholds).collect(),
        output: file.output.or(base.output),
        http: file.http.or(base.http),
        tls: file.tls.or(base.tls),
        extensions: base.extensions.into_iter().chain(file.extensions).collect(),
        execution_segment: file.execution_segment.or(base.execution_segment),
        execution_segment_sequence: file
            .execution_segment_sequence
            .or(base.execution_segment_sequence),
    }
}

#[cfg(test)]
mod overlay_tests {
    use super::*;

    fn base_config() -> JobConfig {
        JobConfig {
            input: "test.js".to_string(),
            execution: ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "30s".to_string(),
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_overlay_execution_when_no_cli_load_flags() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            execution: Some(ExecutionConfig::SharedIterations {
                iterations: 50,
                max_duration: None,
                vus: 5,
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["stdout".to_string()], false, false, true);
        match cfg.execution {
            ExecutionConfig::SharedIterations { iterations, vus, .. } => {
                assert_eq!(iterations, 50);
                assert_eq!(vus, 5);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
        assert!(cfg.execution_explicit);
    }

    #[test]
    fn test_overlay_execution_ignored_when_cli_flags_explicit() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            execution: Some(ExecutionConfig::SharedIterations {
                iterations: 999,
                max_duration: None,
                vus: 5,
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["stdout".to_string()], false, true, true);
        // CLI load profile explicit → overlay execution ignored.
        match cfg.execution {
            ExecutionConfig::ConstantVus { vus, .. } => assert_eq!(vus, 1),
            other => panic!("expected ConstantVus, got {other:?}"),
        }
        assert!(!cfg.execution_explicit);
    }

    #[test]
    fn test_overlay_reporters_replace_default_stdout() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            output: Some(OutputConfig {
                reporters: vec!["json".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["stdout".to_string()], false, false, true);
        assert_eq!(cfg.output.reporters, vec!["json".to_string()]);
    }

    #[test]
    fn test_overlay_reporters_do_not_override_cli_flag() {
        let mut cfg = base_config();
        cfg.output.reporters = vec!["csv".to_string()];
        let overlay = PartialConfig {
            output: Some(OutputConfig {
                reporters: vec!["json".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["csv".to_string()], false, false, true);
        assert_eq!(cfg.output.reporters, vec!["csv".to_string()]);
    }

    #[test]
    fn test_overlay_env_thresholds_fill_not_override() {
        let mut cfg = base_config();
        cfg.env.insert("CLI_ONLY".to_string(), "1".to_string());
        let overlay = PartialConfig {
            env: [("CLI_ONLY".to_string(), "overridden".to_string()),
                ("NEW".to_string(), "2".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["stdout".to_string()], false, false, true);
        assert_eq!(cfg.env.get("CLI_ONLY").map(|s| s.as_str()), Some("1"));
        assert_eq!(cfg.env.get("NEW").map(|s| s.as_str()), Some("2"));
    }

    #[test]
    fn test_overlay_tls_insecure_cli_wins() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            tls: Some(TlsConfig {
                insecure_skip_verify: false,
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["stdout".to_string()], true, false, true);
        assert!(cfg.tls.insecure_skip_verify);
    }

    #[test]
    fn test_merge_partial_file_wins_over_env() {
        let base = PartialConfig {
            env: [("K".to_string(), "base".to_string())].into_iter().collect(),
            execution: Some(ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "10s".to_string(),
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        let file = PartialConfig {
            env: [("K".to_string(), "file".to_string())].into_iter().collect(),
            ..Default::default()
        };
        let merged = merge_partial(base, file);
        assert_eq!(merged.env.get("K").map(|s| s.as_str()), Some("file"));
        // File doesn't set execution → base (env) execution retained.
        assert!(merged.execution.is_some());
    }
}

async fn list_extensions(plugins_dir: Option<&std::path::Path>) -> Result<()> {
    let mut registry = ExtensionRegistry::new();

    // Include WASM plugins from --plugins-dir in the listing.
    if let Some(dir) = plugins_dir {
        let adapters = tropel_wasm::discover_plugins(dir);
        tracing::info!(
            "Loaded {} WASM plugin(s) from {}",
            adapters.len(),
            dir.display()
        );
        for adapter in adapters {
            let id = format!("wasm:{}", adapter.plugin_id());
            let adapter = adapter.clone();
            registry.register_adapter_factory(&id, Arc::new(move || Box::new(adapter.clone())));
        }
    }

    let inputs = registry.list_inputs();

    println!("Tropel Extensions — v{}", env!("CARGO_PKG_VERSION"));
    println!();

    if inputs.is_empty() {
        println!("  No input adapters registered.");
        println!("  Use `tropel build --with <crate>` to build a custom binary with extensions.");
    } else {
        println!("  Input formats:");
        for fmt in &inputs {
            println!(
                "    - {}  (use: `tropel run input.{} --format {})",
                fmt, fmt, fmt
            );
        }
        println!();
        println!("  Use `tropel run <file> --format <name>` to select a specific format.");
        println!("  Without `--format`, the engine auto-detects from file content.");
    }

    let protocols = registry.list_protocols();
    if !protocols.is_empty() {
        println!();
        println!("  Protocols:");
        for p in &protocols {
            println!("    - {}", p);
        }
    }

    let outputs = registry.list_outputs();
    if !outputs.is_empty() {
        println!();
        println!("  Outputs:");
        for o in &outputs {
            println!("    - {}", o);
        }
    }

    Ok(())
}

async fn build_custom(with: &[String], output: &std::path::Path, release: bool) -> Result<()> {
    use tropel_build::{build, BuildConfig};

    // Each `--with` spec is parsed AND validated here — names/versions/URLs
    // are injected into the generated Cargo.toml, so a malformed or hostile
    // value must fail before any file is written (build-time code injection).
    let mut extensions = Vec::with_capacity(with.len());
    for spec in with {
        extensions.push(tropel_build::parse_dep_spec(spec)?);
    }

    let config = BuildConfig {
        extensions,
        output: output.to_path_buf(),
        release,
    };

    build(&config)
}

fn print_version() -> Result<()> {
    println!("Tropel v{}", env!("CARGO_PKG_VERSION"));
    println!("Repository: https://github.com/prasadthx/tropel");
    println!("License: MIT OR Apache-2.0");
    Ok(())
}

/// Load iteration data from a CSV or JSON file.
fn load_data_file(path: &PathBuf) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    let content = std::fs::read_to_string(path).map_err(|e| TropelError::Io(e))?;

    let trimmed = content.trim();

    if trimmed.starts_with('[') {
        let data: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(trimmed)
            .map_err(|e| TropelError::Parse(format!("JSON data-file parse error: {}", e)))?;
        return Ok(data);
    }

    if trimmed.contains(',') || trimmed.starts_with('"') {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(content.as_bytes());

        let headers: Vec<String> = reader
            .headers()
            .map_err(|e| TropelError::Parse(format!("CSV header error: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows = Vec::new();
        for result in reader.records() {
            let record =
                result.map_err(|e| TropelError::Parse(format!("CSV record error: {}", e)))?;
            let mut map = HashMap::new();
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    map.insert(
                        headers[i].clone(),
                        serde_json::Value::String(field.to_string()),
                    );
                }
            }
            rows.push(map);
        }
        return Ok(rows);
    }

    Ok(vec![])
}
