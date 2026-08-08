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
use tropel_core::config::*;
use tropel_core::{Result, TropelError};
use tropel_metrics::thresholds::evaluate_thresholds;

use crate::cli_commands::{
    archive_command, build_custom, inspect_command, list_extensions, load_data_file, print_version,
};
use crate::cli_overlay::{apply_overlay, merge_partial};
use crate::cli_registry::build_registry;
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

        /// Never follow redirects: each 3xx response is returned to the
        /// script as-is and every redirect hop is counted as a request
        /// (default) — with this flag the 3xx itself IS the final response.
        /// k6 always follows redirects (up to maxRedirects); this opt-out
        /// is a Tropel extra.
        #[arg(long = "no-redirects")]
        no_redirects: bool,

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
        no_redirects,
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
    let no_redirects = *no_redirects;
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
    // the explicit CLI --http-debug / --no-redirects flags are applied AFTER
    // the overlay to make sure they always win (regardless of what the
    // overlay set).
    config.http.http_debug = http_debug;
    config.http.no_redirects = no_redirects;

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

    tracing::info!("Execution config: {:?}", config.execution); // Create the engine with extension registry (subprocess + WASM plugins
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

    // VUs that failed to START (e.g. WASM driver pool exhaustion) mean the
    // requested load was not delivered — the summary has already printed, so
    // fail loudly here with a non-zero exit instead of silently reporting the
    // requested VU count as if the run succeeded.
    if result.vu_init_failures > 0 {
        tracing::error!(
            "{} VU(s) failed to start — requested load was NOT delivered (see errors above)",
            result.vu_init_failures
        );
        return Err(TropelError::Other(format!(
            "{} VU(s) failed to start — requested load was not delivered",
            result.vu_init_failures
        )));
    }

    // Script failures (backlog line 98): a run where every prerequest/test
    // script (or driver iteration) threw used to exit 0 with a clean summary.
    // Now each failure is a failed check AND this counter — exit non-zero so
    // CI pipelines see the failure. The summary has already printed.
    //
    // Deliberate semantics: ANY script failure (even a single transient one)
    // makes the run exit non-zero, NOT gated behind a `checks` threshold — a
    // script that throws is a broken test artifact, not an SLO outcome. A
    // flaky script therefore fails CI loudly, which is the point.
    if result.script_failures > 0 {
        tracing::error!(
            "{} script execution(s) failed during the run (see errors above) — exiting non-zero",
            result.script_failures
        );
        return Err(TropelError::Other(format!(
            "{} script execution(s) failed during the run",
            result.script_failures
        )));
    }

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
