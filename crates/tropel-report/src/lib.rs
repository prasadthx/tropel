//! # tropel-report
//!
//! Reporters consuming aggregated metrics: stdout summary, JSON, CSV.
//! Streaming outputs consuming individual samples during the run.

pub mod csv_reporter;
pub mod influxdb;
pub mod json_reporter;
pub mod json_stream;
pub mod otlp;
pub mod output;
pub mod prometheus;
pub mod statsd;
pub mod stdout;

pub use csv_reporter::*;
pub use influxdb::*;
pub use json_reporter::*;
pub use json_stream::*;
pub use otlp::*;
pub use output::*;
pub use prometheus::*;
pub use statsd::*;
pub use stdout::*;

use async_trait::async_trait;
use tropel_core::Result;
use tropel_metrics::collector::MetricsResult;

/// A reporter that outputs test results.
#[async_trait]
pub trait Reporter: Send + Sync {
    fn name(&self) -> &str;
    async fn report(&self, result: &MetricsResult) -> Result<()>;
}

/// Create reporters by name.
pub fn create_reporter(name: &str) -> Option<Box<dyn Reporter>> {
    match name {
        "stdout" => Some(Box::new(StdoutReporter)),
        "json" => Some(Box::new(JsonReporter::new(None))),
        "csv" => Some(Box::new(CsvReporter::new(None))),
        _ => None,
    }
}
