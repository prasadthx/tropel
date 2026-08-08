//! Controller orchestration: accept N agents, dispatch segments, merge.

use crate::protocol::{read_frame, token_matches, write_frame, AssignMsg, HelloMsg, SnapshotMsg};
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
/// `token` is the shared secret: every agent must present it in its
/// [`HelloMsg`] before the controller sends its assignment (the ClusterIP
/// service is reachable by anything in the cluster).
///
/// The caller (CLI) reports the merged result and evaluates thresholds.
pub async fn run_controller(
    listener: TcpListener,
    config: &JobConfig,
    num_agents: u32,
    token: &str,
) -> Result<MetricsResult> {
    if num_agents == 0 {
        return Err(TropelError::Config("--agents must be >= 1".into()));
    }

    // The controller owns ALL output — agents must not stream to the same
    // endpoints/files the controller or other agents use (a shared NDJSON
    // file written by N processes, or N parallel remote-write pushes).
    // OutputConfig::into_worker() nulls every streaming field in one place.
    let mut worker_config = config.clone();
    worker_config.output = std::mem::take(&mut worker_config.output).into_worker();

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
        let token = token.to_string();
        agent_tasks.push(tokio::spawn(async move {
            tracing::info!("Controller: waiting for agent {}/{}...", i + 1, num_agents);
            let (mut stream, peer) = tokio::time::timeout(per_agent_timeout, listener.accept())
                .await
                .map_err(|_| TropelError::Execution("timed out waiting for an agent".into()))?
                .map_err(TropelError::Io)?;
            tracing::info!("Controller: agent {i} connected from {peer}");

            // Authentication gate: the agent MUST present the shared token
            // before any assignment is sent. Anything else in the cluster
            // (the listener is a ClusterIP service) is refused — it never
            // sees the JobConfig (which carries env credentials) and can't
            // forge a SnapshotMsg. Timeout-wrapped like the connect so a
            // hostile stream that connects and stays silent cannot hang.
            let hello: HelloMsg = tokio::time::timeout(per_agent_timeout, read_frame(&mut stream))
                .await
                .map_err(|_| {
                    TropelError::Execution(format!(
                        "agent {i} did not authenticate within {per_agent_timeout:?}"
                    ))
                })??;
            if !token_matches(&token, &hello.token) {
                tracing::warn!("Controller: refusing agent {i} from {peer} — bad auth token");
                return Err(TropelError::Execution(
                    "agent authentication failed: token mismatch".into(),
                ));
            }
            tracing::debug!("Controller: agent {i} from {peer} authenticated");

            let assign = AssignMsg {
                config: worker_config,
                segment,
                sequence,
                index: i as u32,
                total: num_agents,
                // Echo the token so the agent can authenticate the
                // controller (mutual auth on a plaintext channel).
                token: token.clone(),
            };
            write_frame(&mut stream, &assign).await?;

            let snapshot =
                tokio::time::timeout(per_agent_timeout, read_agent_snapshot(&mut stream))
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
    let mut snapshots: Vec<Option<MetricsSnapshot>> = vec![None; num_agents as usize];
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
                return Err(TropelError::Execution(format!("agent task panicked: {e}")));
            }
        }
    }

    let snapshots: Vec<MetricsSnapshot> = snapshots.into_iter().flatten().collect();
    tracing::info!("Controller: all {num_agents} agents done — merging losslessly");
    // A corrupt histogram (bad base64 / truncated V2 bytes) fails the merge
    // loudly instead of silently fabricating results from a partial set.
    merge_snapshots(snapshots, config.thresholds.clone())
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
        ExecutionConfig::SharedIterations { max_duration, .. } => max_duration
            .as_deref()
            .map(parse_duration)
            .unwrap_or(Duration::ZERO),
        ExecutionConfig::RampingArrivalRate { stages, .. } => stages
            .iter()
            .map(|s| parse_duration(&s.duration))
            .sum::<Duration>(),
        ExecutionConfig::PerVUIterations { max_duration, .. } => max_duration
            .as_deref()
            .map(parse_duration)
            .unwrap_or(Duration::ZERO),
        ExecutionConfig::ExternallyControlled { duration, .. } => duration
            .as_deref()
            .map(parse_duration)
            .unwrap_or(Duration::ZERO),
    };
    AGENT_BASE_TIMEOUT + declared + AGENT_GRACE
}

/// Parse a k6-style duration string; invalid values degrade to zero rather
/// than panicking the controller.
fn parse_duration(s: &str) -> Duration {
    tropel_core::parse_duration(s).unwrap_or(Duration::ZERO)
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
                let Ok((mut sock, _)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    // Connection: close — this handler serves ONE request then
                    // drops the socket. Without it, reqwest pools the connection
                    // and reusing it on a fast host (Linux/macOS CI) races the
                    // close, causing a spurious transport error that drops a
                    // sample and flakes the merge assertions.
                    let resp = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        addr
    }

    /// Write a minimal Postman collection hitting `base` and return its path.
    ///
    /// The temp file is keyed on a per-test `tag` IN ADDITION to the pid:
    /// two e2e tests run in parallel in the same process (same pid) and used
    /// to clobber each other's collection mid-write — one test's engine then
    /// read the other's config (or a truncated file) and reported 0 requests.
    fn write_collection(base: &str, tag: &str) -> String {
        let dir = std::env::temp_dir();
        let path = dir.join(format!(
            "tropel-distributed-e2e-{tag}-{}.json",
            std::process::id()
        ));
        let json = format!(
            r#"{{"info":{{"_postman_id":"e2e","name":"dist","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"}},"item":[{{"name":"r1","request":{{"method":"GET","url":"{base}/","header":[]}},"response":[]}}]}}"#
        );
        std::fs::File::create(&path)
            .unwrap()
            .write_all(json.as_bytes())
            .unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn distributed_two_agents_merge_losslessly() -> Result<()> {
        let srv = start_http_server().await;
        let coll = write_collection(&format!("http://{srv}"), "merge");

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

        let controller =
            tokio::spawn(async move { run_controller(listener, &cfg, 2, "test-token").await });
        let mut agents = Vec::new();
        for _ in 0..2 {
            let a = addr_str.clone();
            agents.push(tokio::spawn(async move {
                crate::agent::run_agent(&a, "test-token").await
            }));
        }
        for h in agents {
            h.await.unwrap()?;
        }
        let merged = controller.await.unwrap()?;

        // 4 iterations split across 2 agents → 4 total requests, merged
        // histogram holds all 4 samples.
        assert_eq!(
            merged.http_reqs, 4,
            "merged http_reqs = 4: {}",
            merged.http_reqs
        );
        let dur = merged.http_req_duration.expect("merged http_req_duration");
        assert_eq!(dur.count, 4, "merged histogram count = 4");
        assert!(dur.max > 0, "merged max latency recorded");
        assert_eq!(merged.iterations, 4, "merged iterations = 4");

        let _ = std::fs::remove_file(&coll);
        Ok(())
    }

    #[test]
    fn into_worker_disables_all_streaming_output() {
        let mut config = JobConfig::default();
        config.output.reporters = vec!["stdout".into(), "json".into()];
        config.output.output_file = Some("out.json".into());
        config.output.prometheus_remote_write_url = Some("http://prom:9090".into());
        config.output.otlp_endpoint = Some("http://otlp:4318".into());
        config.output.summary_export = Some("summary.json".into());
        config.output.json_stream = Some("stream.ndjson".into());
        config.output.statsd_addr = Some("localhost:8125".into());
        config.output.influxdb_addr = Some("localhost:8089".into());

        let worker = config.output.clone().into_worker();
        assert!(worker.reporters.is_empty());
        assert!(worker.output_file.is_none());
        assert!(worker.prometheus_remote_write_url.is_none());
        assert!(worker.otlp_endpoint.is_none());
        assert!(worker.summary_export.is_none());
        assert!(worker.json_stream.is_none());
        assert!(worker.statsd_addr.is_none());
        assert!(worker.influxdb_addr.is_none());
        // Non-streaming knobs survive (summary/trends/tag policy untouched).
        assert!(worker.summary);
        assert!(worker.trends);
        assert_eq!(worker.tag_allowlist, config.output.tag_allowlist);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn controller_errors_on_bad_sequence() {
        let config = JobConfig {
            execution_segment_sequence: Some("0,1/3,2/3,1".into()),
            ..Default::default()
        };
        let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
        // 3 boundaries in the sequence vs --agents 2 → hard error, no hang.
        let err = run_controller(listener, &config, 2, "test-token")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boundaries"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_rejects_bad_token() {
        // P2 regression: the ClusterIP service is reachable by anything in
        // the cluster. An agent presenting the wrong token must be refused
        // BEFORE the controller sends the credential-bearing JobConfig, and
        // the run must fail with an auth error — not dispatch anyway.
        let srv = start_http_server().await;
        let coll = write_collection(&format!("http://{srv}"), "badtoken");

        let config = JobConfig {
            input: coll.clone(),
            input_type: Some("postman".into()),
            execution: ExecutionConfig::SharedIterations {
                iterations: 1,
                max_duration: Some("30s".into()),
                vus: 1,
                graceful_stop: Some("10s".into()),
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        };

        let listener = TokioListener::bind("127.0.0.1:0").await.unwrap();
        let addr_str = listener.local_addr().unwrap().to_string();
        let cfg = config.clone();

        let controller =
            tokio::spawn(async move { run_controller(listener, &cfg, 1, "right-token").await });
        let agent =
            tokio::spawn(async move { crate::agent::run_agent(&addr_str, "wrong-token").await });

        let merged = controller.await.unwrap();
        let agent_res = agent.await.unwrap();
        assert!(merged.is_err(), "controller must reject bad token");
        assert!(
            merged.unwrap_err().to_string().contains("token"),
            "error should mention the auth failure"
        );
        assert!(agent_res.is_err(), "agent must fail on controller mismatch");

        let _ = std::fs::remove_file(&coll);
    }
}
