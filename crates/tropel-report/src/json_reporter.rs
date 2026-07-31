use crate::Reporter;
use async_trait::async_trait;
use serde_json::json;
use std::path::PathBuf;
use tropel_core::Result;
use tropel_metrics::collector::MetricsResult;

/// Writes metrics to a JSON file.
pub struct JsonReporter {
    output_path: Option<PathBuf>,
}

impl JsonReporter {
    pub fn new(output_path: Option<String>) -> Self {
        Self {
            output_path: output_path.map(PathBuf::from),
        }
    }
}

#[async_trait]
impl Reporter for JsonReporter {
    fn name(&self) -> &str {
        "json"
    }

    async fn report(&self, result: &MetricsResult) -> Result<()> {
        let mut metrics_map = serde_json::Map::new();

        for metric in &result.metrics {
            metrics_map.insert(
                metric.key.clone(),
                json!({
                    "count": metric.count,
                    "sum": metric.sum,
                    "mean": metric.mean,
                    "min": metric.min,
                    "max": metric.max,
                    "p50": metric.p50,
                    "p90": metric.p90,
                    "p95": metric.p95,
                    "p99": metric.p99,
                }),
            );
        }

        let output = json!({
            "metrics": metrics_map,
            "http_reqs": result.http_reqs,
            "checks": {
                "total": result.checks_total,
                "passed": result.checks_passed,
                "failed": result.checks_failed,
            },
            "errors": result.errors,
            "data_received": result.data_received,
            "data_sent": result.data_sent,
        });

        let json_str = serde_json::to_string_pretty(&output).map_err(|e| {
            tropel_core::TropelError::Report(format!("JSON serialization error: {}", e))
        })?;

        if let Some(path) = &self.output_path {
            tokio::fs::write(path, &json_str)
                .await
                .map_err(|e| tropel_core::TropelError::Io(e))?;
        } else {
            // Print to stdout
            println!("{}", json_str);
        }

        Ok(())
    }
}
