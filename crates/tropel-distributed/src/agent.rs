//! Agent worker logic: connect to the controller, receive a segment, run.

use crate::protocol::{read_frame, write_frame, AssignMsg, SnapshotMsg};
use tokio::net::TcpStream;
use tropel_core::{Result, TropelError};
use tropel_engine::Engine;
use tropel_ext::registry::ExtensionRegistry;

/// Connect to a controller, run this worker's segment of the job, and ship
/// the raw metrics snapshot back for central lossless merging.
pub async fn run_agent(controller_addr: &str) -> Result<()> {
    let mut stream = TcpStream::connect(controller_addr)
        .await
        .map_err(|e| TropelError::Io(e))?;
    tracing::info!("Agent: connected to controller {controller_addr}");

    let assign: AssignMsg = read_frame(&mut stream).await?;
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
