//! # tropel-report
//!
//! Reporters consuming aggregated metrics: stdout summary, JSON, CSV.
//! Streaming outputs consuming individual samples during the run.

pub mod stdout;
pub mod json_reporter;
pub mod csv_reporter;
pub mod output;

pub use stdout::*;
pub use json_reporter::*;
pub use csv_reporter::*;
pub use output::*;

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

/// Create a streaming output by name.
pub fn create_output(name: &str) -> Option<Box<dyn Output>> {
    match name {
        "stdout" => Some(Box::new(StreamingStdoutOutput::new())),
        _ => None,
    }
}
