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
use std::time::Instant;
use tropel_core::config::JobConfig;
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;
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

/// Build the tokio runtime for the distributed binaries.
///
/// `tropel-cloud-run local --agents N` runs the controller AND N in-process
/// agent engines in one process, and `tropel-agent` runs a full engine too —
/// a hardcoded `worker_threads = 2` (the old `#[tokio::main]` in both bins)
/// starved that: at `--agents 4` there were 2 async workers juggling the
/// controller, 4 agent loops, and 4 engines' orchestration. Default scales
/// with available parallelism (the same default `#[tokio::main(flavor =
/// "multi_thread")]` would pick); `TROPEL_TOKIO_WORKERS` overrides (clamped
/// to [2, 256] — a floor of 2 keeps controller+agent I/O responsive on
/// 1-core CI, and beyond 256 only adds contention). VU threads run on their
/// own thread-per-core pool, so this outer runtime only ever multiplexes
/// async orchestration.
pub fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let workers = std::env::var("TROPEL_TOKIO_WORKERS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(4)
        })
        .clamp(2, 256);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
}

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
///
/// `test_start` is the controller-side run start, used for the summary
/// export's duration field (the merged result carries no wall clock).
pub async fn report_and_thresholds(
    config: &JobConfig,
    result: &tropel_metrics::collector::MetricsResult,
    test_start: Instant,
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

    // Streaming outputs (Prometheus/OTLP/StatsD/Influx/json-stream) consume
    // a LIVE sample stream during the run. In distributed mode agents run
    // with outputs nulled (OutputConfig::into_worker) and only ship merged
    // snapshots at the end — there is no sample stream on the controller to
    // feed them, so they cannot be honored. Warn loudly instead of silently
    // dropping them (the previous behavior: no warning at all).
    let mut unstreamable: Vec<&str> = Vec::new();
    if config.output.prometheus_remote_write_url.is_some() {
        unstreamable.push("prometheus_remote_write_url");
    }
    if config.output.otlp_endpoint.is_some() {
        unstreamable.push("otlp_endpoint");
    }
    if config.output.json_stream.is_some() {
        unstreamable.push("json_stream");
    }
    if config.output.statsd_addr.is_some() {
        unstreamable.push("statsd_addr");
    }
    if config.output.influxdb_addr.is_some() {
        unstreamable.push("influxdb_addr");
    }
    if !unstreamable.is_empty() {
        tracing::warn!(
            "Distributed mode cannot stream samples live to: {} — agents run with outputs \
             nulled (into_worker) and the controller merges end-of-run snapshots only, so \
             there is no sample stream to push. Configure these on a local run instead.",
            unstreamable.join(", ")
        );
    }

    // Honor summary_export / script handleSummary from the MERGED result —
    // the engine's emit_handle_summary is public for exactly this. Previously
    // the controller never called it, silently dropping summary_export.
    let registry = ExtensionRegistry::new();
    tropel_engine::emit_handle_summary(
        config,
        &registry,
        result,
        &result.effective_thresholds,
        test_start,
    )
    .await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};
    use tropel_core::config::JobConfig;
    use tropel_metrics::collector::MetricsResult;

    #[tokio::test]
    async fn report_and_thresholds_honors_summary_export() {
        // P1 regression: report_and_thresholds only built stdout/json/csv
        // reporters and NEVER called emit_handle_summary, so summary_export
        // was silently dropped on distributed runs. It must write the file.
        let dir = std::env::temp_dir().join(format!(
            "tropel-summary-export-test-{}",
            std::process::id()
        ));
        let path = dir.with_extension("json");
        let _ = std::fs::remove_file(&path);

        let mut config = JobConfig::default();
        config.output.reporters = Vec::new();
        config.output.summary_export = Some(path.to_string_lossy().to_string());
        // The input path doesn't exist — emit_handle_summary must fall through
        // to the --summary-export write instead of failing.
        config.input = "/nonexistent/input.json".into();

        let result = MetricsResult::default();
        let start = Instant::now() - Duration::from_secs(3);
        let outcome = report_and_thresholds(&config, &result, start).await;
        assert!(outcome.is_ok(), "report_and_thresholds failed: {:?}", outcome.err());

        let written = std::fs::read_to_string(&path).expect("summary_export must be written");
        let value: serde_json::Value =
            serde_json::from_str(&written).expect("summary_export must be valid JSON");
        // The merged result feeds the summary data (metrics map present).
        assert!(value.get("metrics").is_some(), "summary JSON has metrics");
        assert!(value.get("state").is_some(), "summary JSON has state");
        // testRunDurationMs reflects the passed test_start (at least 3s —
        // elapsed() includes the setup time between the two calls, so an
        // exact-equality assert would flake under load).
        let state = value.get("state").unwrap();
        assert!(
            state
                .get("testRunDurationMs")
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                >= 3000,
            "testRunDurationMs must reflect the passed test_start"
        );

        let _ = std::fs::remove_file(&path);
    }
}
