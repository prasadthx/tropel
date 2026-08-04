//! # CLI entry point
//!
//! Reusable CLI logic that is called by both the standard `tropel` binary
//! and custom binaries built with `tropel build --with <ext>`.
//!
//! This module handles argument parsing, tracing initialization, and
//! dispatching to the appropriate engine subcommand. Custom binaries
//! simply call `tropel_engine::cli::run_cli()` from their `fn main()`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use std::sync::Arc;
use tropel_core::config::*;
use tropel_core::scenario::{Scenario, ScenarioItem};
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, InputAdapter};
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
#[allow(clippy::large_enum_variant)]
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

        /// Port for the runtime control API (k6 REST parity). When set with
        /// an `externally-controlled` executor, binds 127.0.0.1:<port> and
        /// serves GET/PATCH /v1/status so VUs can be adjusted mid-run.
        #[arg(long = "control-port")]
        control_port: Option<u16>,

        /// Insecure TLS (skip certificate verification)
        #[arg(short = 'k', long = "insecure")]
        insecure: bool,

        /// Show verbose output
        #[arg(short = 'v', long = "verbose")]
        verbose: bool,

        /// Log every HTTP request/response at debug level (method, URL,
        /// status, timing). Equivalent to `HttpConfig.http_debug`.
        #[arg(long = "http-debug")]
        http_debug: bool,

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

    /// Inspect an input file without running it: shows how Tropel resolves
    /// it (driver or adapter), the parsed scenario summary (name, request
    /// count, methods, variables, auth), and any script-declared options.
    Inspect {
        /// Path to the input file (collection, HAR, script, etc.)
        input: PathBuf,

        /// Input format (auto-detect if not specified)
        #[arg(long = "format")]
        format: Option<String>,

        /// Directory of WASM plugins to include in resolution
        #[arg(long = "plugins-dir")]
        plugins_dir: Option<PathBuf>,

        /// Subprocess adapter command (same semantics as `run`)
        #[arg(long = "subprocess-adapter")]
        subprocess_adapter: Vec<String>,
    },

    /// Bundle a test into a self-contained directory: the input file plus its
    /// referenced dependencies (data file, env file, config file) and a
    /// manifest, so the test can be replayed on another machine without the
    /// original paths.
    Archive {
        /// Path to the input file (collection, HAR, script, etc.)
        input: PathBuf,

        /// Input format (auto-detect if not specified)
        #[arg(long = "format")]
        format: Option<String>,

        /// Output directory for the bundle (default: ./tropel-archive)
        #[arg(short = 'o', long = "output")]
        output: Option<PathBuf>,

        /// Data file (CSV/JSON) to bundle
        #[arg(long = "data-file")]
        data_file: Option<PathBuf>,

        /// Environment file (JSON) to bundle
        #[arg(long = "env-file")]
        env_file: Option<PathBuf>,

        /// Config file (JSON) to bundle
        #[arg(long = "config")]
        config: Option<PathBuf>,
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
        Commands::Inspect {
            input,
            format,
            plugins_dir,
            subprocess_adapter,
        } => {
            inspect_command(
                &input,
                format.as_deref(),
                plugins_dir.as_deref(),
                &subprocess_adapter,
            )
            .await
        }
        Commands::Archive {
            input,
            format,
            output,
            data_file,
            env_file,
            config,
        } => {
            archive_command(
                &input,
                format.as_deref(),
                output.as_deref(),
                data_file.as_deref(),
                env_file.as_deref(),
                config.as_deref(),
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
        http_debug,
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
        control_port,
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
    let control_port = *control_port;
    let thresholds = threshold.clone();
    let insecure = *insecure;
    let http_debug = *http_debug;
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

    // The user provided a load profile when they passed any of the load
    // flags. Otherwise (bare `tropel run script.js`) a k6 script's own
    // `export const options` is allowed to drive the run. Computed before
    // `from_mode` so duration/stages can be moved into it without clones.
    let load_profile_explicit = vus.is_some()
        || duration.is_some()
        || mode_explicit
        || stages.is_some()
        || iterations.is_some();

    // Build execution config — canonical mode→executor mapping lives in
    // tropel-core (shared with the k6 env-file builder).
    let execution = ExecutionConfig::from_mode(&mode, vus, duration, iterations, stages);

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
        control_port,
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

    // The config-file / K6_* overlay may replace `config.http` wholesale, so
    // the explicit CLI --http-debug flag is applied AFTER the overlay to make
    // sure it always wins (regardless of what the overlay set).
    config.http.http_debug = http_debug;

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

    tracing::info!("Execution config: {:?}", config.execution);    // Create the engine with extension registry (subprocess + WASM plugins
    // registered the same way `inspect`/`list` do — one shared builder).
    let registry = build_registry(subprocess_adapter, plugins_dir.as_deref())?;
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
    // Control API port: CLI flag wins; overlay fills the gap.
    if config.control_port.is_none() {
        config.control_port = overlay.control_port;
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
        control_port: file.control_port.or(base.control_port),
    }
}

/// Build the extension registry exactly like `run_command` does: built-in
/// adapters/drivers from `inventory` plus any `--subprocess-adapter` and
/// `--plugins-dir` extras. Shared by `run`, `inspect` and `list` so resolution
/// is always identical.
fn build_registry(
    subprocess_adapter: &[String],
    plugins_dir: Option<&Path>,
) -> Result<ExtensionRegistry> {
    let mut registry = ExtensionRegistry::new();

    // Register any subprocess adapters specified via --subprocess-adapter
    for cmd in subprocess_adapter {
        // `SubprocessAdapter::new` rejects empty commands with a TropelError
        // (the old `parts[1..]` panicked) — surface that here, at CLI parse
        // time, instead of letting the factory panic later.
        if cmd.trim().is_empty() {
            return Err(TropelError::Other(format!(
                "--subprocess-adapter requires a non-empty command (got {cmd:?})"
            )));
        }
        let id = format!("subprocess:{}", cmd);
        tracing::info!("Registering subprocess adapter (command: {})", cmd);
        let cmd_clone = cmd.clone();
        registry.register_adapter_factory(
            &id,
            Arc::new(move || {
                Box::new(
                    tropel_input_subprocess::SubprocessAdapter::new(&cmd_clone)
                        .expect("command validated non-empty above"),
                )
            }),
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

    Ok(registry)
}

/// `tropel inspect <input>` — show how an input resolves and what it contains
/// WITHOUT running a load test. Useful to verify a collection/HAR/script
/// parses correctly, which adapter/driver handles it, and what load profile a
/// k6 script declares.
async fn inspect_command(
    input: &Path,
    format: Option<&str>,
    plugins_dir: Option<&Path>,
    subprocess_adapter: &[String],
) -> Result<()> {
    let registry = build_registry(subprocess_adapter, plugins_dir)?;
    let bytes = std::fs::read(input)
        .map_err(|e| TropelError::Parse(format!("Failed to read '{}': {}", input.display(), e)))?;

    println!("Tropel Inspect — v{}", env!("CARGO_PKG_VERSION"));
    println!("Input: {}", input.display());

    // 1. Drivers first (same precedence as the engine).
    let driver: Option<Box<dyn Driver>> = if let Some(fmt) = format {
        registry.resolve_driver_by_id(fmt)
    } else {
        registry.resolve_driver(&bytes)
    };
    if let Some(driver) = driver {
        println!("Resolved by driver: {}", driver.id());
        println!("Kind: imperative (runs JS per iteration)");
        // Note: an empty env is passed here — scripts that derive their
        // options from `__ENV` will show the defaults rather than what a
        // configured `run` would apply. `inspect` is a dry-run verification
        // tool; threading real env/`-e` values here is a future enhancement.
        if let Some(opts) = driver
            .declared_options(&bytes, Some(input), &HashMap::new())
            .await
        {
            println!("Declared options:");
            if let Some(exec) = &opts.execution {
                println!("  execution: {} ({:?})", exec.executor_name(), exec);
            }
            if let Some(scenarios) = &opts.scenarios {
                println!("  scenarios: {}", scenarios.len());
                for (name, sc) in scenarios {
                    println!(
                        "    - {}: {} ({:?})",
                        name,
                        sc.execution.executor_name(),
                        sc.execution
                    );
                }
            }
            if !opts.thresholds.is_empty() {
                println!("  thresholds: {}", opts.thresholds.len());
                for (name, t) in &opts.thresholds {
                    println!("    - {}: {}", name, t.expression);
                }
            }
        } else {
            println!("Declared options: (none)");
        }
        return Ok(());
    }

    // 2. Fall back to input adapters (declarative).
    let adapter: Box<dyn InputAdapter> = if let Some(fmt) = format {
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
                input.display(),
                if available.is_empty() {
                    "(none registered — check build configuration)".to_string()
                } else {
                    available.join(", ")
                }
            ))
        })?
    };

    println!("Resolved by adapter: {}", adapter.id());
    println!("Kind: declarative (static request list)");
    let scenario = adapter.parse_with_path(&bytes, Some(input))?;
    print_scenario_summary(&scenario);
    Ok(())
}

/// Recursively print a scenario's request tree plus totals.
fn print_scenario_summary(scenario: &Scenario) {
    println!("Scenario: {}", scenario.info.name);
    if let Some(desc) = &scenario.info.description {
        println!("  description: {}", desc);
    }
    if let Some(auth) = &scenario.auth {
        println!("  global auth: {:?}", auth);
    }
    println!("  variables: {} defined", scenario.variables.len());
    for (k, v) in &scenario.variables {
        println!("    {} = {}", k, v);
    }

    fn walk(items: &[ScenarioItem], depth: usize, out: &mut (usize, usize)) {
        for item in items {
            let indent = "  ".repeat(depth);
            match &item.request {
                Some(req) => {
                    out.0 += 1;
                    let scripted = item.test.is_some() || item.prerequest.is_some();
                    if scripted {
                        out.1 += 1;
                    }
                    println!(
                        "{}• {} — {} {}{}",
                        indent,
                        item.name,
                        req.method,
                        req.url,
                        if scripted { " (scripted)" } else { "" }
                    );
                }
                None => {
                    println!("{}▸ {} (folder)", indent, item.name);
                    walk(&item.items, depth + 1, out);
                }
            }
        }
    }

    let mut counts = (0usize, 0usize);
    walk(&scenario.items, 1, &mut counts);
    println!("Totals: {} requests ({} with scripts)", counts.0, counts.1);
}

/// `tropel archive <input> -o <dir>` — bundle a test into a self-contained
/// directory so it can be replayed on another machine (or after the original
/// files move). Copies the input plus any referenced data/env/config files and
/// writes a `tropel-archive.json` manifest describing how to re-run it.
async fn archive_command(
    input: &Path,
    format: Option<&str>,
    output: Option<&Path>,
    data_file: Option<&Path>,
    env_file: Option<&Path>,
    config: Option<&Path>,
) -> Result<()> {
    let out_dir = output.unwrap_or_else(|| Path::new("./tropel-archive"));
    std::fs::create_dir_all(out_dir).map_err(TropelError::Io)?;

    // Input file — the core of the bundle.
    let input_name = input
        .file_name()
        .ok_or_else(|| TropelError::Config(format!("Input '{}' has no file name", input.display())))?
        .to_string_lossy()
        .to_string();
    let bundled_input = out_dir.join(&input_name);
    std::fs::copy(input, &bundled_input).map_err(TropelError::Io)?;

    // Referenced dependency files, each copied next to the input. All files
    // land in ONE flat directory keyed by file name, so a dep sharing the
    // input's name (or another dep's name) would silently overwrite — guard
    // against collisions so the bundle stays deterministic.
    let mut deps: Vec<(String, PathBuf, PathBuf)> = Vec::new(); // (role, src, dest)
    let mut used_names: HashMap<String, String> = HashMap::new(); // name -> role
    used_names.insert(input_name.clone(), "input".to_string());
    let mut copy_dep = |role: &str, src: &Path, out: &Path| -> Result<()> {
        let name = src
            .file_name()
            .ok_or_else(|| {
                TropelError::Config(format!("{} '{}' has no file name", role, src.display()))
            })?
            .to_string_lossy()
            .to_string();
        if let Some(existing) = used_names.get(&name) {
            return Err(TropelError::Config(format!(
                "archive: '{}' (from {}) collides with {} — all bundled files \
                 share one directory, rename it or bundle separately",
                name,
                src.display(),
                existing
            )));
        }
        used_names.insert(name.clone(), role.to_string());
        let dest = out.join(&name);
        std::fs::copy(src, &dest).map_err(TropelError::Io)?;
        deps.push((role.to_string(), src.to_path_buf(), dest));
        Ok(())
    };
    if let Some(d) = data_file {
        copy_dep("data_file", d, out_dir)?;
    }
    if let Some(e) = env_file {
        copy_dep("env_file", e, out_dir)?;
    }
    if let Some(c) = config {
        copy_dep("config", c, out_dir)?;
    }

    // Manifest: how to re-run this bundle.
    let mut manifest = serde_json::Map::new();
    manifest.insert("version".into(), serde_json::Value::String(env!("CARGO_PKG_VERSION").into()));
    manifest.insert("input".into(), serde_json::Value::String(input_name.clone()));
    if let Some(fmt) = format {
        manifest.insert("format".into(), serde_json::Value::String(fmt.to_string()));
    }
    let mut dep_map = serde_json::Map::new();
    for (role, _src, dest) in &deps {
        dep_map.insert(
            role.clone(),
            serde_json::Value::String(
                dest.file_name().unwrap_or_default().to_string_lossy().to_string(),
            ),
        );
    }
    manifest.insert("bundled_files".into(), serde_json::Value::Object(dep_map));

    // Build the suggested re-run command relative to the bundle directory.
    let mut run_cmd = format!("tropel run {}", input_name);
    if let Some(fmt) = format {
        run_cmd.push_str(&format!(" --format {}", fmt));
    }
    for (role, _src, dest) in &deps {
        let flag = match role.as_str() {
            "data_file" => "--data-file",
            "env_file" => "--env-file",
            "config" => "--config",
            _ => continue,
        };
        run_cmd.push_str(&format!(
            " {} {}",
            flag,
            dest.file_name().unwrap_or_default().to_string_lossy()
        ));
    }
    manifest.insert("run".into(), serde_json::Value::String(run_cmd.clone()));

    let manifest_path = out_dir.join("tropel-archive.json");
    let manifest_json = serde_json::Value::Object(manifest);
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest_json)
            .map_err(|e| TropelError::Other(format!("manifest serialize: {}", e)))?,
    )
    .map_err(TropelError::Io)?;

    println!("Tropel Archive — v{}", env!("CARGO_PKG_VERSION"));
    println!("Bundle created in: {}", out_dir.display());
    println!("  input:  {} (from {})", bundled_input.display(), input.display());
    for (role, src, dest) in &deps {
        println!("  {}: {} (from {})", role, dest.display(), src.display());
    }
    println!("  manifest: {}", manifest_path.display());
    println!("Re-run from the bundle directory:");
    println!("  cd {} && {}", out_dir.display(), run_cmd);
    Ok(())
}

async fn list_extensions(plugins_dir: Option<&std::path::Path>) -> Result<()> {
    let registry = build_registry(&[], plugins_dir)?;

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
    let content = std::fs::read_to_string(path).map_err(TropelError::Io)?;

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
