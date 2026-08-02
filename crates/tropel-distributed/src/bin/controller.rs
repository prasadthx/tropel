//! `tropel-controller` — distribute a load test across N `tropel-agent`
//! workers and merge their hdr-histogram snapshots losslessly.
//!
//! Usage:
//!   tropel-controller --config <job.json> --agents <N> [--listen host:port]

use clap::Parser;
use std::path::PathBuf;
use tokio::net::TcpListener;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_metrics::thresholds::evaluate_thresholds;
use tropel_report::create_reporter;

#[derive(Parser)]
#[command(name = "tropel-controller", about = "Distributed load-test controller")]
struct Args {
    /// Job config JSON (a full JobConfig).
    #[arg(long, short = 'c')]
    config: PathBuf,
    /// Number of agent workers to expect.
    #[arg(long, default_value_t = 1)]
    agents: u32,
    /// Listen address for agents.
    #[arg(long, default_value = "127.0.0.1:17890")]
    listen: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    let raw = std::fs::read_to_string(&args.config)
        .map_err(TropelError::Io)?;
    let config: JobConfig = serde_json::from_str(&raw)
        .map_err(|e| TropelError::Parse(format!("invalid job config: {e}")))?;

    let listener = TcpListener::bind(&args.listen)
        .await
        .map_err(TropelError::Io)?;
    tracing::info!("Controller listening on {}. Waiting for {} agent(s)...", args.listen, args.agents);

    let result = tropel_distributed::run_controller(listener, &config, args.agents).await?;

    // Report the merged result through the configured reporters.
    let mut reporters = Vec::new();
    for name in &config.output.reporters {
        if let Some(r) = create_reporter(name) {
            reporters.push(r);
        } else {
            tracing::warn!("Unknown reporter: {name}");
        }
    }
    for reporter in &reporters {
        reporter.report(&result).await?;
    }

    // Thresholds → exit code (mirrors the single-node CLI tail).
    let threshold_results = evaluate_thresholds(&result.effective_thresholds, &result);
    let mut any_failed = false;
    for tr in &threshold_results {
        if tr.passed {
            tracing::info!("  ✓ Threshold '{}': {:.2} (PASS)", tr.name, tr.actual);
        } else {
            tracing::error!("  ✗ Threshold '{}': {:.2} (FAIL)", tr.name, tr.actual);
            any_failed = true;
        }
    }
    if any_failed {
        Err(TropelError::Other("One or more thresholds failed".into()))
    } else {
        Ok(())
    }
}
