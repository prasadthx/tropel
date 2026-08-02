use crate::Reporter;
use async_trait::async_trait;
use std::path::PathBuf;
use tropel_core::Result;
use tropel_metrics::collector::MetricsResult;

/// Writes metrics to a CSV file.
pub struct CsvReporter {
    output_path: Option<PathBuf>,
}

impl CsvReporter {
    pub fn new(output_path: Option<String>) -> Self {
        Self {
            output_path: output_path.map(PathBuf::from),
        }
    }

    /// Render the full report as CSV text (no I/O). Exposed for tests and
    /// programmatic consumers; `report()` writes it.
    pub fn render(&self, result: &MetricsResult) -> String {
        let mut csv_output = String::from("key,count,sum,mean,min,max,p50,p90,p95,p99\n");

        for metric in &result.metrics {
            csv_output.push_str(&format!(
                "{},{},{},{:.2},{},{},{},{},{},{}\n",
                metric.key,
                metric.count,
                metric.sum,
                metric.mean,
                metric.min,
                metric.max,
                metric.p50,
                metric.p90,
                metric.p95,
                metric.p99
            ));
        }

        csv_output
    }
}

#[async_trait]
impl Reporter for CsvReporter {
    fn name(&self) -> &str {
        "csv"
    }

    async fn report(&self, result: &MetricsResult) -> Result<()> {
        let csv_output = self.render(result);

        if let Some(path) = &self.output_path {
            tokio::fs::write(path, &csv_output)
                .await
                .map_err(tropel_core::TropelError::Io)?;
        } else {
            // Print to stdout
            println!("{}", csv_output);
        }

        Ok(())
    }
}
