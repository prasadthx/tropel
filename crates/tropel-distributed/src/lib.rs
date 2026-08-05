//! # tropel-distributed
//!
//! Multi-node load testing: a `tropel-controller` partitions a job across N
//! `tropel-agent` workers using execution segments, then merges their
//! serialized hdr-histogram snapshots **losslessly** (🦀 Rust-opt: the
//! hdr-histogram V2 binary merge is exact — no percentile estimation, no
//! sampling — so the controller's p95/p99 are precisely the merged buckets).
//!
//! # Protocol
//!
//! TCP with length-prefixed JSON frames (u32 BE length + JSON bytes):
//!
//! - Agent → Controller (first): `Hello { token }` — the shared-secret
//!   authentication preamble. The controller refuses the connection unless
//!   the token matches (constant-time), so anything that can reach the
//!   ClusterIP service never sees the credential-bearing job config.
//! - Controller → Agent: `Assign { config, segment, sequence, index, token }`
//!   — the token is echoed so the agent can authenticate the controller
//!   (mutual auth on a plaintext channel; the token gates connectivity, TLS
//!   would additionally hide it from passive sniffers).
//! - Agent → Controller: `Snapshot { snapshot }`
//!
//! The controller computes N equal execution segments (`0:1/N`,
//! `1/N:2/N`, ... against sequence `0,1/N,...,1`) and dispatches one per
//! worker; each agent applies its segment (scaling VUs/iterations/rates
//! deterministically — see `tropel-core`'s `ExecutionSegment`), runs the
//! engine as a `distributed_worker` (no local reporting), and ships its raw
//! `MetricsSnapshot` back. The controller merges and reports.

use std::path::PathBuf;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_metrics::thresholds::evaluate_thresholds;
use tropel_report::create_reporter;

pub mod agent;
pub mod cloud;
pub mod controller;
pub mod protocol;
pub mod yaml;

pub use agent::run_agent;
pub use cloud::{generate_k8s_manifests, run_cloud};
pub use controller::run_controller;
pub use protocol::{AssignMsg, HelloMsg, SnapshotMsg, generate_token};

/// Whether any token source was provided (`--token`, `--token-file`, or the
/// `TROPEL_TOKEN` env var). Callers use this to decide between resolving a
/// real token and auto-generating one — auto-generation must only happen
/// when NO source exists, so a typo'd `--token-file` path surfaces as an
/// error instead of being silently masked.
pub fn has_token_source(cli: &Option<String>, file: &Option<PathBuf>) -> bool {
    cli.is_some() || file.is_some() || std::env::var("TROPEL_TOKEN").is_ok()
}

/// Resolve the shared auth token from the CLI `--token` value, a
/// `--token-file` path, or the `TROPEL_TOKEN` env var, in that order.
pub fn resolve_token(cli: Option<String>, file: Option<PathBuf>) -> Result<String> {
    if let Some(t) = cli {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    if let Some(path) = file {
        let raw = std::fs::read_to_string(&path).map_err(TropelError::Io)?;
        let t = raw.trim();
        if t.is_empty() {
            return Err(TropelError::Config(format!(
                "token file {} is empty",
                path.display()
            )));
        }
        return Ok(t.to_string());
    }
    if let Ok(t) = std::env::var("TROPEL_TOKEN") {
        if !t.is_empty() {
            return Ok(t);
        }
    }
    Err(TropelError::Config(
        "no auth token: pass --token <secret>, --token-file <path>, or set TROPEL_TOKEN".into(),
    ))
}

/// Run the configured reporters over a merged result, then evaluate
/// thresholds and return an error if any failed (exit-code contract shared
/// by the `tropel-controller` and `tropel-cloud-run local/controller` bins).
pub async fn report_and_thresholds(
    config: &JobConfig,
    result: &tropel_metrics::collector::MetricsResult,
) -> Result<()> {
    let mut reporters = Vec::new();
    for name in &config.output.reporters {
        if let Some(r) = create_reporter(name) {
            reporters.push(r);
        } else {
            tracing::warn!("Unknown reporter: {name}");
        }
    }
    for reporter in &reporters {
        reporter.report(result).await?;
    }

    let threshold_results = evaluate_thresholds(&result.effective_thresholds, result);
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
