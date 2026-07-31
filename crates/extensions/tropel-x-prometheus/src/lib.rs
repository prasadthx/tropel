//! # tropel-x-prometheus
//!
//! Prometheus/OTLP output extension for Tropel.
//! This is a reference output extension implementing Output trait.

use async_trait::async_trait;
use tropel_sdk::{Output, Result, Sample};

/// Prometheus output (stub — requires prometheus-http-metrics or OTLP library).
pub struct PrometheusOutput;

#[async_trait]
impl Output for PrometheusOutput {
    fn name(&self) -> &str {
        "prometheus"
    }

    async fn emit(&self, _batch: &[Sample]) -> Result<()> {
        // Not yet implemented
        // Would push metrics to a Prometheus push gateway or expose an endpoint
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}
