//! # Tropel CLI
//!
//! The main entry point for the Tropel load testing tool.
//! Run `tropel run <collection>` to execute a load test.

use clap::{Parser, Subcommand};
use std::collections::HashMap;
use std::path::PathBuf;
use tropel_core::config::*;
use tropel_core::Result;
use tropel_engine::engine::Engine;
use tropel_ext::registry::ExtensionRegistry;

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Tropel — A high-performance load-testing framework.
#[derive(Parser, Debug)]
#[command(name = "tropel", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a load test from a Postman Collection
    Run {
        /// Path to the Postman Collection JSON file
        input: PathBuf,

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

    /// List available extensions and their capabilities
    Extensions,

    /// Print the version and build information
    Version,
}

#[tokio::main]
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
        input, vus, duration, env, env_file, data_file,
        reporter, output, threshold, insecure, verbose: _,
        mode, stages, iterations,
    } = &cli.command else {
        return Err(tropel_core::TropelError::Other("Not a Run command".into()));
    };

    let input = input.clone();
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
        if let Ok(content) = std::fs::read_to_string(env_path) {
            if let Ok(json_env) = serde_json::from_str::<HashMap<String, String>>(&content) {
                env_map.extend(json_env);
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
            }
        }
        "shared-iterations" => {
            ExecutionConfig::SharedIterations {
                iterations: iterations.unwrap_or(100),
                max_duration: duration.clone(),
                vus: vus.unwrap_or(1),
            }
        }
        "arrival-rate" | "constant-arrival-rate" => {
            ExecutionConfig::ConstantArrivalRate {
                rate: vus.unwrap_or(1) as f64,
                time_unit: "1s".to_string(),
                duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
                pre_alloc_vus: 1,
                max_vus: vus.unwrap_or(10).max(10),
            }
        }
        _ => {
            // constant-vus (default)
            ExecutionConfig::ConstantVus {
                vus: vus.unwrap_or(1),
                duration: duration.clone().unwrap_or_else(|| "30s".to_string()),
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
        });
    }

    // Build the full job config
    let config = JobConfig {
        input: input.to_string_lossy().to_string(),
        execution,
        env: env_map,
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

    Ok(())
}

async fn list_extensions() -> Result<()> {
    println!("Tropel Extensions");
    println!("  No extensions currently registered.");
    println!("  Use `tropel build --with <ext>` to build a custom binary with extensions.");
    Ok(())
}

fn print_version() -> Result<()> {
    println!("Tropel v{}", env!("CARGO_PKG_VERSION"));
    println!("Repository: https://github.com/tropel/tropel");
    println!("License: MIT OR Apache-2.0");
    Ok(())
}
