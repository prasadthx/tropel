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
//! - Controller → Agent: `Assign { config, segment, sequence, index }`
//! - Agent → Controller: `Snapshot { snapshot }`
//!
//! The controller computes N equal execution segments (`0:1/N`,
//! `1/N:2/N`, ... against sequence `0,1/N,...,1`) and dispatches one per
//! worker; each agent applies its segment (scaling VUs/iterations/rates
//! deterministically — see `tropel-core`'s `ExecutionSegment`), runs the
//! engine as a `distributed_worker` (no local reporting), and ships its raw
//! `MetricsSnapshot` back. The controller merges and reports.

use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_metrics::thresholds::evaluate_thresholds;
use tropel_report::create_reporter;

pub mod agent;
pub mod cloud;
pub mod controller;
pub mod protocol;

pub use agent::run_agent;
pub use cloud::{generate_k8s_manifests, run_cloud};
pub use controller::run_controller;
pub use protocol::{AssignMsg, SnapshotMsg};

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
