//! Agent worker logic: connect to the controller, receive a segment, run.

use crate::protocol::{read_frame, token_matches, write_frame, AssignMsg, HelloMsg, SnapshotMsg};
use tokio::net::TcpStream;
use tropel_core::{Result, TropelError};
use tropel_engine::Engine;
use tropel_ext::registry::ExtensionRegistry;

/// Total time an agent keeps retrying the controller connection before
/// giving up (the controller pod may still be scheduling — agents must not
/// fail instantly in a Job where `backoffLimit: 0` means no kubelet retry).
const CONNECT_TOTAL_BOUND: std::time::Duration = std::time::Duration::from_secs(300);
/// Per-attempt connect timeout: a single hanging `TcpStream::connect` (e.g.
/// a black-holed address) must not exceed the total bound — without this,
/// the OS-level connect timeout (often minutes) would silently blow past
/// `CONNECT_TOTAL_BOUND` and defeat the Job-safety rationale.
const CONNECT_ATTEMPT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Initial connect-retry delay, doubling each attempt up to the cap.
const CONNECT_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(250);
/// Cap on the per-attempt backoff delay.
const CONNECT_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(5);

/// Connect to a controller, run this worker's segment of the job, and ship
/// the raw metrics snapshot back for central lossless merging.
///
/// `token` is the shared secret the controller requires: the agent presents
/// it in a [`HelloMsg`] as the first frame and verifies the controller
/// echoes it back in the [`AssignMsg`] (mutual auth on a plaintext channel).
///
/// The initial connect retries with exponential backoff (bounded by
/// [`CONNECT_TOTAL_BOUND`]). In a Kubernetes Indexed Job the controller pod
/// may still be scheduling when this agent starts, and `restartPolicy:
/// Never` + `backoffLimit: 0` means a single failed attempt would fail the
/// whole Job — so a short-lived refusal must not be fatal.
pub async fn run_agent(controller_addr: &str, token: &str) -> Result<()> {
    let mut stream = connect_with_retry(controller_addr).await?;
    tracing::info!("Agent: connected to controller {controller_addr}");

    // Authenticate BEFORE any assignment is sent — the controller never
    // dispatches to an unauthenticated peer.
    write_frame(
        &mut stream,
        &HelloMsg {
            token: token.to_string(),
        },
    )
    .await?;

    let assign: AssignMsg = read_frame(&mut stream).await?;
    if !token_matches(token, &assign.token) {
        return Err(TropelError::Execution(
            "controller authentication failed: token mismatch".into(),
        ));
    }
    let index = assign.index;
    let total = assign.total;
    tracing::info!(
        "Agent: received assignment (segment {} of {}) — segment '{}'",
        index + 1,
        total,
        assign.segment
    );

    // Build the worker config: mark this process a distributed worker
    // (the controller owns all end-of-run output) and apply the segment.
    let mut config = assign.config;
    config.distributed_worker = true;
    config.execution_segment = Some(assign.segment);
    config.execution_segment_sequence = assign.sequence;

    // Run the engine with the applied segment. The engine scales the
    // workload deterministically to this node's share.
    let registry = ExtensionRegistry::new();
    let engine = Engine::new(registry);
    let result = engine.run(&config).await?;

    tracing::info!(
        "Agent: finished — {}/{} iterations, {} reqs — shipping snapshot",
        result.metrics.iterations,
        total,
        result.metrics.http_reqs
    );

    let msg = SnapshotMsg {
        snapshot: result.snapshot,
    };
    write_frame(&mut stream, &msg).await?;
    tracing::info!("Agent: snapshot shipped");
    Ok(())
}

/// Attempt `TcpStream::connect` with bounded exponential backoff so a
/// controller that is still being scheduled does not fail the run.
/// Fails only after [`CONNECT_TOTAL_BOUND`] elapses.
async fn connect_with_retry(controller_addr: &str) -> Result<TcpStream> {
    let deadline = std::time::Instant::now() + CONNECT_TOTAL_BOUND;
    let mut backoff = CONNECT_INITIAL_BACKOFF;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        // Timeout each attempt so a single hanging connect cannot extend the
        // run past CONNECT_TOTAL_BOUND (deadline is only checked on failure).
        match tokio::time::timeout(CONNECT_ATTEMPT_TIMEOUT, TcpStream::connect(controller_addr))
            .await
        {
            Ok(Ok(stream)) => return Ok(stream),
            Ok(Err(e)) => {
                if std::time::Instant::now() >= deadline {
                    return Err(TropelError::Io(e));
                }
                tracing::warn!(
                    "Agent: controller connect attempt {attempt} failed ({e}) — retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CONNECT_MAX_BACKOFF);
            }
            Err(_timeout) => {
                // The attempt itself timed out (e.g. black-holed address) —
                // there is no real error to propagate from tokio::time::timeout.
                if std::time::Instant::now() >= deadline {
                    return Err(TropelError::Io(std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        format!(
                            "controller {controller_addr} unreachable after {attempt} attempts"
                        ),
                    )));
                }
                tracing::warn!(
                    "Agent: controller connect attempt {attempt} timed out — retrying in {:?}",
                    backoff
                );
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(CONNECT_MAX_BACKOFF);
            }
        }
    }
}
