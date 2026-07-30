//! # Tropel CLI
//!
//! The main entry point for the Tropel load testing tool.
//! Run `tropel run <collection>` to execute a load test.

// Select the global allocator at compile time via feature flags.
// - Default: mimalloc (fast, low fragmentation, well-tested across workloads)
// - Feature `alloc-jemalloc`: tikv-jemallocator (better peak-RSS behavior for
//   very large heap sizes, matching k6's Go runtime GC profile)
#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;
use tropel_core::config::*;
use tropel_core::Result;
use tropel_engine::engine::Engine;
use tropel_ext::registry::ExtensionRegistry;
use tropel_metrics::thresholds::evaluate_thresholds;

/// Tropel — A high-performance load-testing framework.
#[derive(Parser, Debug)]
#[command(name = "tropel", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
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
    },

    /// List available input formats and their capabilities
    Extensions,

    /// Print the version and build information
    Version,
}

// Outer tokio runtime: 2 workers. VUs run on the thread-per-core VUWorkerPool
// (current-thread runtimes, one per CPU core), so the orchestrator only needs
// minimal worker threads for scenario coordination and final metric collection.
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
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
        Commands::Run { .. } => run(cli).await,
        Commands::Extensions => list_extensions().await,
        Commands::Version => print_version(),
    }
}

impl Cli {
    fn verbose(&self) -> bool {
        match &self.command {
            Commands::Run { verbose, .. } => *verbose,
            _ => false,
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    // Extract fields from the Run command
    let Commands::Run {
        input, format, vus, duration, env, env_file, data_file,
        reporter, output, threshold, insecure, verbose: _,
        mode, stages, iterations,
    } = &cli.command else {
        return Err(tropel_core::TropelError::Other("Not a Run command".into()));
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
                // Try Postman environment format first: {"values":[{"key":...,"value":...,"enabled":true}]}
                if let Ok(postman_env) = serde_json::from_str::<serde_json::Value>(&content) {
                    if let Some(values) = postman_env.get("values").and_then(|v| v.as_array()) {
                        for entry in values {
                            if let (Some(key), Some(value)) = (
                                entry.get("key").and_then(|k| k.as_str()),
                                entry.get("value").and_then(|v| v.as_str()),
                            ) {
                                // Only add if enabled (default: enabled if not specified)
                                let enabled = entry.get("enabled").and_then(|e| e.as_bool()).unwrap_or(true);
                                if enabled {
                                    env_map.insert(key.to_string(), value.to_string());
                                }
                            }
                        }
                    } else if let Ok(flat_env) = serde_json::from_value::<HashMap<String, String>>(postman_env.clone()) {
                        // Fallback: flat JSON format
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
            // constant-vus (default)
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

    tracing::info!("Execution config: {:?}", config.execution);

    // Create the engine and run
    let engine = Engine::new(ExtensionRegistry::new());
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
                return Err(tropel_core::TropelError::Other(format!("Threshold '{}' failed (abort-on-fail)", tr.name)));
            }
        }
    }

    if any_failed {
        Err(tropel_core::TropelError::Other("One or more thresholds failed".into()))
    } else {
        Ok(())
    }
}

async fn list_extensions() -> Result<()> {
    let registry = ExtensionRegistry::new();
    let inputs = registry.list_inputs();

    println!("Tropel Extensions — v{}", env!("CARGO_PKG_VERSION"));
    println!();

    if inputs.is_empty() {
        println!("  No input adapters registered.");
        println!("  Use `tropel build --with <crate>` to build a custom binary with extensions.");
    } else {
        println!("  Input formats:");
        for fmt in &inputs {
            println!("    - {}  (use: `tropel run input.{} --format {}", fmt, fmt, fmt);
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

fn print_version() -> Result<()> {
    println!("Tropel v{}", env!("CARGO_PKG_VERSION"));
    println!("Repository: https://github.com/prasadthx/tropel");
    println!("License: MIT OR Apache-2.0");
    Ok(())
}

/// Load iteration data from a CSV or JSON file.
fn load_data_file(path: &PathBuf) -> Result<Vec<HashMap<String, serde_json::Value>>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| tropel_core::TropelError::Io(e))?;

    let trimmed = content.trim();

    // Try JSON array first: [{"key":"val"}, ...]
    if trimmed.starts_with('[') {
        let data: Vec<HashMap<String, serde_json::Value>> = serde_json::from_str(trimmed)
            .map_err(|e| tropel_core::TropelError::Parse(format!("JSON data-file parse error: {}", e)))?;
        return Ok(data);
    }

    // Try CSV: header row + data rows
    if trimmed.contains(',') || trimmed.starts_with('"') {
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .flexible(true)
            .from_reader(content.as_bytes());

        let headers: Vec<String> = reader.headers()
            .map_err(|e| tropel_core::TropelError::Parse(format!("CSV header error: {}", e)))?
            .iter()
            .map(|h| h.to_string())
            .collect();

        let mut rows = Vec::new();
        for result in reader.records() {
            let record = result
                .map_err(|e| tropel_core::TropelError::Parse(format!("CSV record error: {}", e)))?;
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
