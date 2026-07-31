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

        /// Report format (stdout, json, csv)
        #[arg(short = 'r', long = "reporter", default_value = "stdout")]
        reporter: Vec<String>,

        /// Output file path (for json/csv reporters)
        #[arg(short = 'o', long = "output")]
        output: Option<PathBuf>,

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
        #[arg(short = 'm', long = "mode", default_value = "constant-vus")]
        mode: String,

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
        Commands::Build { ref with, ref output, debug } => {
            build_custom(with, output.as_deref().unwrap_or(&PathBuf::from("./tropel-custom")), !debug).await
        }
        Commands::Version => print_version(),
    }
}

async fn run_command(cli: Cli) -> Result<()> {
    let Commands::Run {
        input, format, vus, duration, env, env_file, data_file,
        reporter, output, threshold, insecure, verbose: _,
        mode, stages, iterations, subprocess_adapter, plugins_dir, ..
    } = &cli.command else {
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
    let thresholds = threshold.clone();
    let insecure = *insecure;
    let mode = mode.clone();
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
                                let enabled = entry.get("enabled").and_then(|e| e.as_bool()).unwrap_or(true);
                                if enabled {
                                    env_map.insert(key.to_string(), value.to_string());
                                }
                            }
                        }
                    } else if let Ok(flat_env) = serde_json::from_value::<HashMap<String, String>>(postman_env.clone()) {
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
        "shared-iterations" => {
            ExecutionConfig::SharedIterations {
                iterations: iterations.unwrap_or(100),
                max_duration: duration.clone(),
                vus: vus.unwrap_or(1),
                graceful_stop: Some("30s".to_string()),
                think_time: ThinkTimeConfig::default(),
            }
        }
        "arrival-rate" | "constant-arrival-rate" => {
            ExecutionConfig::ConstantArrivalRate {
                rate: vus.unwrap_or(1) as f64,
                time_unit: "1s".to_string(),
                duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
                pre_alloc_vus: 1,
                max_vus: vus.unwrap_or(10).max(10),
                graceful_stop: Some("30s".to_string()),
                think_time: ThinkTimeConfig::default(),
            }
        }
        _ => {
            ExecutionConfig::ConstantVus {
                vus: vus.unwrap_or(1),
                duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
                graceful_stop: Some("30s".to_string()),
                think_time: ThinkTimeConfig::default(),
            }
        }
    };

    // Parse thresholds
    let mut threshold_map: HashMap<String, ThresholdConfig> = HashMap::new();
    for t in &thresholds {
        let name = format!("threshold_{}", threshold_map.len());
        threshold_map.insert(name, ThresholdConfig {
            expression: t.clone(),
            abort_on_fail: false,
            delay_abort_eval: None,
        });
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

    // Build the full job config
    let config = JobConfig {
        input: input.to_string_lossy().to_string(),
        input_type: format,
        execution,
        env: env_map,
        iteration_data,
        output: OutputConfig {
            reporters: reporters.clone(),
            output_file: output.map(|p| p.to_string_lossy().to_string()),
            ..Default::default()
        },
        thresholds: threshold_map,
        tls: TlsConfig {
            insecure_skip_verify: insecure,
            ..Default::default()
        },
        ..Default::default()
    };

    tracing::info!("Execution config: {:?}", config.execution);    // Create the engine with extension registry
    let mut registry = ExtensionRegistry::new();

    // Register any subprocess adapters specified via --subprocess-adapter
    for cmd in subprocess_adapter {
        let id = format!("subprocess:{}", cmd);
        tracing::info!("Registering subprocess adapter (command: {})", cmd);
        let cmd_clone = cmd.clone();
        registry.register_adapter_factory(
            &id,
            Arc::new(move || {
                Box::new(tropel_input_subprocess::SubprocessAdapter::new(&cmd_clone))
            }),
        );
    }

    // Register WASM plugins from --plugins-dir (Tier 2 no-recompile adapters).
    if let Some(dir) = plugins_dir {
        let adapters = tropel_wasm::discover_plugins(dir);
        tracing::info!("Loaded {} WASM plugin(s) from {}", adapters.len(), dir.display());
        for adapter in adapters {
            let id = format!("wasm:{}", adapter.plugin_id());
            let adapter = adapter.clone();
            registry.register_adapter_factory(
                &id,
                Arc::new(move || Box::new(adapter.clone())),
            );
        }
    }

    let engine = Engine::new(registry);
    let result = engine.run(&config).await?;

    tracing::info!("Load test completed: {} total requests", result.metrics.http_reqs);
    tracing::info!("Checks: {}/{} passed", result.metrics.checks_passed, result.metrics.checks_total);

    // Evaluate thresholds and drive exit code
    let threshold_results = evaluate_thresholds(&config.thresholds, &result.metrics);
    let mut any_failed = false;
    for tr in &threshold_results {
        if tr.passed {
            tracing::info!("  ✓ Threshold '{}': {:.2} {} {:.2} (PASS)", tr.name, tr.actual, tr.expression.split_whitespace().nth(1).unwrap_or("<?>"), tr.threshold);
        } else {
            tracing::error!("  ✗ Threshold '{}': {:.2} {} {:.2} (FAIL)", tr.name, tr.actual, tr.expression.split_whitespace().nth(1).unwrap_or("<?>"), tr.threshold);
            any_failed = true;
            if tr.abort_on_fail {
                tracing::error!("Aborting due to threshold '{}'", tr.name);
                return Err(TropelError::Other(format!("Threshold '{}' failed (abort-on-fail)", tr.name)));
            }
        }
    }

    if any_failed {
        Err(TropelError::Other("One or more thresholds failed".into()))
    } else {
        Ok(())
    }
}

async fn list_extensions(plugins_dir: Option<&std::path::Path>) -> Result<()> {
    let mut registry = ExtensionRegistry::new();

    // Include WASM plugins from --plugins-dir in the listing.
    if let Some(dir) = plugins_dir {
        let adapters = tropel_wasm::discover_plugins(dir);
        tracing::info!("Loaded {} WASM plugin(s) from {}", adapters.len(), dir.display());
        for adapter in adapters {
            let id = format!("wasm:{}", adapter.plugin_id());
            let adapter = adapter.clone();
            registry.register_adapter_factory(
                &id,
                Arc::new(move || Box::new(adapter.clone())),
            );
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
            println!("    - {}  (use: `tropel run input.{} --format {})", fmt, fmt, fmt);
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
    let content = std::fs::read_to_string(path)
        .map_err(|e| TropelError::Io(e))?;

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

        let headers: Vec<String> = reader.headers()
            .map_err(|e| TropelError::Parse(format!("CSV header error: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows = Vec::new();
        for result in reader.records() {
            let record = result
                .map_err(|e| TropelError::Parse(format!("CSV record error: {}", e)))?;
            let mut map = HashMap::new();
            for (i, field) in record.iter().enumerate() {
                if i < headers.len() {
                    map.insert(headers[i].clone(), serde_json::Value::String(field.to_string()));
                }
            }
            rows.push(map);
        }
        return Ok(rows);
    }

    Ok(vec![])
}
