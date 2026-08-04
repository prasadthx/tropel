//! Streaming extension output driver.
//!
//! Subscribes to the metrics broadcast channel, batches samples, and calls
//! the output's `emit()`/`flush()`. Moved out of the former `engine.rs`
//! god-file.

use std::time::Duration;
use tokio::sync::broadcast;
use tropel_core::types::Sample;
use tropel_ext::traits::Output;

/// Drive a registered extension output from the sample stream.
///
/// Subscribes to the metrics broadcast channel, batches samples, and calls
/// the output's `emit()` every `FLUSH_INTERVAL` (or when the batch exceeds
/// `MAX_BATCH`), then `flush()` once when the stream closes (test end).
/// Best-effort: `emit`/`flush` failures are logged, never fatal.
pub(crate) fn spawn_extension_output(
    mut rx: broadcast::Receiver<Sample>,
    output: Box<dyn Output>,
) -> tokio::task::JoinHandle<()> {
    const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
    const MAX_BATCH: usize = 10_000;

    tokio::spawn(async move {
        let mut batch: Vec<Sample> = Vec::with_capacity(1024);
        let mut tick = tokio::time::interval(FLUSH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                res = rx.recv() => match res {
                    Ok(sample) => {
                        batch.push(sample);
                        if batch.len() >= MAX_BATCH {
                            let b = std::mem::take(&mut batch);
                            if let Err(e) = output.emit(&b).await {
                                tracing::warn!("extension output '{}' emit failed: {e}", output.name());
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::trace!("extension output dropped {n} samples (consumer lag)");
                    }
                },
                _ = tick.tick() => {
                    if !batch.is_empty() {
                        let b = std::mem::take(&mut batch);
                        if let Err(e) = output.emit(&b).await {
                            tracing::warn!("extension output '{}' emit failed: {e}", output.name());
                        }
                    }
                }
            }
        }

        // Final flush on stream close.
        if !batch.is_empty() {
            if let Err(e) = output.emit(&batch).await {
                tracing::warn!("extension output '{}' final emit failed: {e}", output.name());
            }
        }
        if let Err(e) = output.flush().await {
            tracing::warn!("extension output '{}' flush failed: {e}", output.name());
        }
    })
}
