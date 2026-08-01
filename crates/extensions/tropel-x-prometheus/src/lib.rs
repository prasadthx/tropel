//! # tropel-x-prometheus
//!
//! Prometheus remote-write output extension for Tropel.
//! This is a reference output extension implementing the SDK `Output` trait.
//!
//! It delegates to `tropel_report::PrometheusRemoteWriteOutput` — the same
//! implementation behind the built-in `--prometheus-url` flag — so the wire
//! format is identical (snappy-compressed protobuf `WriteRequest` to
//! `{url}/api/v1/write`, ms timestamps, `__name__` = metric + sorted tags).
//!
//! Configure the endpoint with `--prometheus-url` (the engine passes the
//! `OutputConfig` through [`Output::configure`]) or the `TROPEL_PROMETHEUS_URL`
//! environment variable. Without an endpoint, `emit` is a no-op that warns
//! (the run must never fail because a dashboard is unreachable).
//!
//! Buffered samples are pushed to the endpoint every `FLUSH_INTERVAL`
//! during the run (from inside `emit`) plus a final push on `flush` — so
//! the dashboard receives data while the test is running, mirroring the
//! built-in `--prometheus-url` path.

use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tropel_report::PrometheusRemoteWriteOutput;
use tropel_report::Output as _; // bring `sample`/`stop` into scope (anonymous, avoids the name clash with sdk::Output)
use tropel_sdk::{Output, OutputConfig, OutputRegistration, Result, Sample};

/// How often buffered samples are pushed to the endpoint while the run is
/// in progress (the engine driver only calls `flush` once, at stream close;
/// the reference output pushes on a time basis from inside `emit` so the
/// dashboard sees data during the run, matching the built-in path).
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

/// Environment variable supplying the remote-write endpoint when the output
/// is constructed without an explicit URL (e.g. via the inventory factory).
pub const PROMETHEUS_URL_ENV: &str = "TROPEL_PROMETHEUS_URL";

/// Prometheus remote-write output extension.
pub struct PrometheusOutput {
    inner: Option<PrometheusRemoteWriteOutput>,
    /// Warn about the missing endpoint only once per output instance.
    warned_no_url: AtomicBool,
    /// When the buffer was last pushed to the endpoint (periodic streaming).
    last_flush: Mutex<Instant>,
}

impl PrometheusOutput {
    /// Create an output pushing to a remote-write endpoint.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            inner: Some(PrometheusRemoteWriteOutput::new(url)),
            warned_no_url: AtomicBool::new(false),
            last_flush: Mutex::new(Instant::now()),
        }
    }

    /// Create from `TROPEL_PROMETHEUS_URL`; unconfigured when unset.
    pub fn from_env() -> Self {
        match std::env::var(PROMETHEUS_URL_ENV) {
            Ok(url) if !url.trim().is_empty() => Self::new(url),
            _ => Self {
                inner: None,
                warned_no_url: AtomicBool::new(false),
                last_flush: Mutex::new(Instant::now()),
            },
        }
    }
}

impl Default for PrometheusOutput {
    fn default() -> Self {
        Self::from_env()
    }
}

#[async_trait]
impl Output for PrometheusOutput {
    fn name(&self) -> &str {
        "prometheus"
    }

    fn configure(&mut self, config: &OutputConfig) {
        // CLI/config wins over env (project convention). Log which endpoint
        // is in effect so a stale shell env var can't silently redirect.
        if let Some(url) = &config.prometheus_remote_write_url {
            self.inner = Some(PrometheusRemoteWriteOutput::new(url.clone()));
            tracing::debug!("prometheus extension: endpoint from --prometheus-url: {url}");
        } else if self.inner.is_none() {
            tracing::debug!(
                "prometheus extension: no --prometheus-url; using {} if set",
                PROMETHEUS_URL_ENV
            );
        }
    }

    async fn emit(&self, batch: &[Sample]) -> Result<()> {
        match &self.inner {
            Some(inner) => {
                inner.sample(batch).await?;
                // Push buffered samples periodically so the dashboard sees
                // data during the run, not only at the final flush. The
                // engine driver only calls `flush` on stream close; the
                // periodic push happens here (best-effort, like the built-in
                // path's 5s tick).
                let should_flush = {
                    let last = self.last_flush.lock().unwrap();
                    last.elapsed() >= FLUSH_INTERVAL
                };
                if should_flush {
                    inner.stop().await?;
                    *self.last_flush.lock().unwrap() = Instant::now();
                }
                Ok(())
            }
            None => {
                // No endpoint: warn once, drop, keep the run alive.
                if !self.warned_no_url.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "prometheus extension: no endpoint configured (set --prometheus-url or {PROMETHEUS_URL_ENV}); samples dropped"
                    );
                }
                Ok(())
            }
        }
    }

    async fn flush(&self) -> Result<()> {
        match &self.inner {
            Some(inner) => inner.stop().await,
            None => Ok(()),
        }
    }
}

/// Inventory factory — must be a `fn` pointer for `inventory::submit!`.
fn prometheus_factory() -> Box<dyn Output> {
    Box::new(PrometheusOutput::default())
}

// Register for compile-time discovery by the engine's ExtensionRegistry.
// The binary links this crate, so the registration is always present.
inventory::submit!(OutputRegistration::new("prometheus", prometheus_factory));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registers_prometheus_output() {
        let registry = tropel_sdk::ExtensionRegistry::new();
        let output = registry
            .get_output("prometheus")
            .expect("prometheus output must be registered via inventory");
        assert_eq!(output.name(), "prometheus");
    }

    #[test]
    fn unconfigured_emit_is_safe() {
        let output = PrometheusOutput::default();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            output.emit(&[]).await.expect("emit must not fail");
            output.flush().await.expect("flush must not fail");
        });
    }

    #[test]
    fn configure_adopts_job_url() {
        let mut output = PrometheusOutput::default();
        let config = OutputConfig {
            prometheus_remote_write_url: Some("http://localhost:9090".into()),
            ..Default::default()
        };
        output.configure(&config);
        assert!(output.inner.is_some(), "configure must set the endpoint");
    }
}
