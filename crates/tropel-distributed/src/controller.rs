//! Controller orchestration: accept N agents, dispatch segments, merge.

use crate::protocol::{write_frame, read_frame, AssignMsg, SnapshotMsg};
use std::time::Duration;
use tokio::net::TcpListener;
use tropel_core::config::JobConfig;
use tropel_core::segment::ExecutionSegment;
use tropel_core::{Result, TropelError};
use tropel_metrics::collector::{merge_snapshots, MetricsResult, MetricsSnapshot};

/// Base timeout for a single agent to connect+run and ship its snapshot.
/// The job's own duration (max_duration / longest stage) is added on top,
/// plus a grace window — a flat 24h constant would let a dead agent hang
/// the whole run long past the test's real bounds.
const AGENT_BASE_TIMEOUT: Duration = Duration::from_secs(60);
/// Grace added over the job's declared duration for in-flight iterations.
const AGENT_GRACE: Duration = Duration::from_secs(120);

/// Run a distributed load test as the controller.
///
/// Computes N equal execution segments over `[0, 1)` (unless the job config
/// declares its own `execution_segment_sequence`), accepts `num_agents`
/// connections on `listener`, dispatches one segment per agent, collects
/// their raw snapshots, and returns the centrally merged `MetricsResult`.
///
/// The caller (CLI) reports the merged result and evaluates thresholds.
pub async fn run_controller(
    listener: TcpListener,
    config: &JobConfig,
    num_agents: u32,
) -> Result<MetricsResult> {
    if num_agents == 0 {
        return Err(TropelError::Config("--agents must be >= 1".into()));
    }

    // The controller owns ALL output — agents must not stream to the same
    // endpoints/files the controller or other agents use (a shared NDJSON
    // file written by N processes, or N parallel remote-write pushes).
    // Null out the streaming output fields on the dispatched config.
    let mut worker_config = config.clone();
    worker_config.output.reporters = vec![];
    worker_config.output.output_file = None;
    worker_config.output.prometheus_remote_write_url = None;
    worker_config.output.otlp_endpoint = None;
    worker_config.output.summary_export = None;
    worker_config.output.json_stream = None;
    worker_config.output.statsd_addr = None;
    worker_config.output.influxdb_addr = None;

    // Compute the segment dispatch: if the job declares a sequence, use it;
    // otherwise split [0,1) into num_agents equal segments.
    let (segments, sequence) = if let Some(seq) = &config.execution_segment_sequence {
        let bounds = ExecutionSegment::parse_sequence(seq)?;
        if bounds.len() as u32 != num_agents + 1 {
            return Err(TropelError::Config(format!(
                "execution_segment_sequence '{}' has {} boundaries but --agents is {num_agents}",
                seq,
                bounds.len()
            )));
        }
        let segs: Vec<String> = bounds
            .windows(2)
            .map(|w| format!("{}:{}", w[0], w[1]))
            .collect();
        (segs, Some(seq.clone()))
    } else {
        // Equal split: "0:1/N", "1/N:2/N", ... with sequence "0,1/N,...,1".
        let seq = (0..=num_agents)
            .map(|i| format!("{i}/{num_agents}"))
            .collect::<Vec<_>>()
            .join(",");
        let segs = (0..num_agents)
            .map(|i| format!("{i}/{num_agents}:{}/{}", i + 1, num_agents))
            .collect::<Vec<_>>();
        (segs, Some(seq))
    };

    tracing::info!(
        "Controller: partitioning into {num_agents} segment(s) against sequence '{}'",
        sequence.as_deref().unwrap_or("")
    );

    // Spawn ALL agent handlers concurrently: each task accepts its own
    // connection on the shared listener, dispatches its segment, and reads
    // that agent's snapshot. All agents therefore run SIMULTANEOUSLY (the
    // whole point of distributed load) instead of serially — before this
    // change the controller blocked on agent N's full run before even
    // accepting agent N+1, so wall-clock ≈ N × duration and the target
    // never saw aggregate load.
    let listener = std::sync::Arc::new(listener);
    let per_agent_timeout = agent_timeout(config);
    let mut agent_tasks = Vec::with_capacity(num_agents as usize);

    for (i, segment) in segments.iter().enumerate() {
        let listener = listener.clone();
        let worker_config = worker_config.clone();
        let segment = segment.clone();
        let sequence = sequence.clone();
        agent_tasks.push(tokio::spawn(async move {
            tracing::info!("Controller: waiting for agent {}/{}...", i + 1, num_agents);
            let (mut stream, peer) = tokio::time::timeout(
                per_agent_timeout,
                listener.accept(),
            )
            .await
            .map_err(|_| TropelError::Execution("timed out waiting for an agent".into()))?
            .map_err(TropelError::Io)?;
            tracing::info!("Controller: agent {i} connected from {peer}");

            let assign = AssignMsg {
                config: worker_config,
                segment,
                sequence,
                index: i as u32,
                total: num_agents,
            };
            write_frame(&mut stream, &assign).await?;

            let snapshot = tokio::time::timeout(
                per_agent_timeout,
                read_agent_snapshot(&mut stream),
            )
            .await
            .map_err(|_| {
                TropelError::Execution(format!(
                    "agent {i} timed out before shipping its snapshot"
                ))
            })??;
            Ok::<_, TropelError>((i as u32, snapshot))
        }));
    }

    // Join all agent tasks. Results are placed back at their agent index so
    // the merged snapshot ordering matches the original deterministic order.
    let mut snapshots: Vec<Option<MetricsSnapshot>> =
        vec![None; num_agents as usize];
    for task in agent_tasks {
        match task.await {
            Ok(Ok((i, snapshot))) => {
                tracing::info!(
                    "Controller: agent {i} shipped {} series ({} events)",
                    snapshot.series.len(),
                    snapshot.series.iter().map(|s| s.count as u64).sum::<u64>()
                );
                snapshots[i as usize] = Some(snapshot);
            }
            Ok(Err(e)) => {
                tracing::error!("Controller: agent failed: {e}");
                return Err(e);
            }
            Err(e) => {
                return Err(TropelError::Execution(format!(
                    "agent task panicked: {e}"
                )));
            }
        }
    }

    let snapshots: Vec<MetricsSnapshot> = snapshots.into_iter().flatten().collect();
    tracing::info!("Controller: all {num_agents} agents done — merging losslessly");
    Ok(merge_snapshots(snapshots, config.thresholds.clone()))
}

/// Read an agent's snapshot frame (drain any prior frames defensively).
async fn read_agent_snapshot(stream: &mut tokio::net::TcpStream) -> Result<MetricsSnapshot> {
    let msg = read_frame::<_, SnapshotMsg>(stream).await?;
    Ok(msg.snapshot)
}

/// A per-agent timeout bounded by the job's own declared duration: base
/// window + the declared run time + grace. A dead agent therefore fails
/// the run shortly after the test would have finished, not 24h later.
///
/// Ramping executors run their stages *sequentially*, so the declared time
/// is the SUM of stage durations (max would under-budget a long ramp).
fn agent_timeout(config: &JobConfig) -> Duration {
    use tropel_core::config::ExecutionConfig;

    let declared = match &config.execution {
        ExecutionConfig::ConstantVus { duration, .. } => parse_duration(duration),
        ExecutionConfig::RampingVus { stages, .. } => stages
            .iter()
            .map(|s| parse_duration(&s.duration))
            .sum::<Duration>(),
        ExecutionConfig::ConstantArrivalRate { duration, .. } => parse_duration(duration),
        ExecutionConfig::SharedIterations { max_duration, .. } => {
            max_duration.as_deref().map(parse_duration).unwrap_or(Duration::ZERO)
        }
        ExecutionConfig::RampingArrivalRate { stages, .. } => stages
            .iter()
            .map(|s| parse_duration(&s.duration))
            .sum::<Duration>(),
        ExecutionConfig::PerVUIterations { max_duration, .. } => {
            max_duration.as_deref().map(parse_duration).unwrap_or(Duration::ZERO)
        }
        ExecutionConfig::ExternallyControlled { duration, .. } => {
            duration.as_deref().map(parse_duration).unwrap_or(Duration::ZERO)
        }
    };
    AGENT_BASE_TIMEOUT + declared + AGENT_GRACE
}

/// Parse a k6-style duration string; invalid values degrade to zero rather
/// than panicking the controller.
fn parse_duration(s: &str) -> Duration {
    let s = s.trim();
    if let Some(num) = s.strip_suffix("ms") {
        num.parse::<u64>().map(Duration::from_millis).unwrap_or(Duration::ZERO)
    } else if let Some(num) = s.strip_suffix('s') {
        num.parse::<f64>().map(Duration::from_secs_f64).unwrap_or(Duration::ZERO)
    } else if let Some(num) = s.strip_suffix('m') {
        num.parse::<f64>().map(|m| Duration::from_secs_f64(m * 60.0)).unwrap_or(Duration::ZERO)
    } else if let Some(num) = s.strip_suffix('h') {
        num.parse::<f64>().map(|h| Duration::from_secs_f64(h * 3600.0)).unwrap_or(Duration::ZERO)
    } else {
        s.parse::<f64>().map(Duration::from_secs_f64).unwrap_or(Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener as TokioListener;
    use tropel_core::config::{ExecutionConfig, ThinkTimeConfig};
    use tropel_core::Result;

    /// Start a minimal HTTP/1.1 server that answers every request with 200.
    async fn start_http_server() -> std::net::SocketAddr {
        let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        addr
    }

    /// Write a minimal Postman collection hitting `base` and return its path.
    fn write_collection(base: &str) -> String {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("tropel-distributed-e2e-{}.json", std::process::id()));
        let json = format!(
            r#"{{"info":{{"_postman_id":"e2e","name":"dist","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"}},"item":[{{"name":"r1","request":{{"method":"GET","url":"{base}/","header":[]}},"response":[]}}]}}"#
        );
        std::fs::File::create(&path).unwrap().write_all(json.as_bytes()).unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distributed_two_agents_merge_losslessly() -> Result<()> {
        let srv = start_http_server().await;
        let coll = write_collection(&format!("http://{srv}"));

        let config = JobConfig {
            input: coll.clone(),
            input_type: Some("postman".into()),
            execution: ExecutionConfig::SharedIterations {
                iterations: 4,
                max_duration: Some("30s".into()),
                vus: 2,
                graceful_stop: Some("10s".into()),
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        };

        let listener = TokioListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let addr_str = addr.to_string();
        let cfg = config.clone();

        let controller = tokio::spawn(async move { run_controller(listener, &cfg, 2).await });
        let mut agents = Vec::new();
        for _ in 0..2 {
            let a = addr_str.clone();
            agents.push(tokio::spawn(async move { crate::agent::run_agent(&a).await }));
        }
        for h in agents {
            h.await.unwrap()?;
        }
        let merged = controller.await.unwrap()?;

        // 4 iterations split across 2 agents → 4 total requests, merged
        // histogram holds all 4 samples.
        assert_eq!(merged.http_reqs, 4, "merged http_reqs = 4: {}", merged.http_reqs);
        let dur = merged.http_req_duration.expect("merged http_req_duration");
        assert_eq!(dur.count, 4, "merged histogram count = 4");
        assert!(dur.max > 0, "merged max latency recorded");
        assert_eq!(merged.iterations, 4, "merged iterations = 4");

        let _ = std::fs::remove_file(&coll);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn controller_errors_on_bad_sequence() {
        let config = JobConfig {
            execution_segment_sequence: Some("0,1/3,2/3,1".into()),
            ..Default::default()
        };
        let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
        // 3 boundaries in the sequence vs --agents 2 → hard error, no hang.
        let err = run_controller(listener, &config, 2).await.unwrap_err();
        assert!(err.to_string().contains("boundaries"));
    }
}

