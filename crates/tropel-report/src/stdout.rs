use crate::Reporter;
use async_trait::async_trait;
use tropel_core::Result;
use tropel_metrics::collector::MetricsResult;

/// Prints a summary report to stdout.
pub struct StdoutReporter;

#[async_trait]
impl Reporter for StdoutReporter {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn report(&self, result: &MetricsResult) -> Result<()> {
        println!("\n╔══════════════════════════════════════════════════╗");
        println!("║          Tropel Load Test Summary               ║");
        println!("╚══════════════════════════════════════════════════╝\n");

        // Execution overview
        println!("  Execution:");
        println!("    Iterations:     {}", result.iterations);
        println!("    Max VUs:        {}", result.vus_max);
        println!("    Dropped:        {}", result.dropped_iterations);

        // HTTP requests
        println!("\n  HTTP requests:");
        println!("    Total:     {}", result.http_reqs);
        println!("    Failed:    {} ({:.1}%)",
            (result.http_req_failed * result.http_reqs as f64) as u64,
            result.http_req_failed * 100.0);
        println!("    Data received: {:.2} MB", result.data_received / 1_000_000.0);
        println!("    Data sent:     {:.2} MB", result.data_sent / 1_000_000.0);

        if let Some(duration) = &result.http_req_duration {
            println!("\n  HTTP request duration:");
            println!("    avg:    {:.2}ms", duration.mean / 1000.0);
            println!("    min:    {}μs", duration.min);
            println!("    max:    {}ms", duration.max / 1000);
            println!("    p50:    {}ms", duration.p50 / 1000);
            println!("    p90:    {}ms", duration.p90 / 1000);
            println!("    p95:    {}ms", duration.p95 / 1000);
            println!("    p99:    {}ms", duration.p99 / 1000);
        }

        // Iteration duration
        if let Some(dur) = &result.iteration_duration {
            println!("\n  Iteration duration:");
            println!("    avg:    {:.2}ms", dur.mean / 1000.0);
            println!("    min:    {}μs", dur.min);
            println!("    max:    {}ms", dur.max / 1000);
        }

        // Checks/assertions
        if result.checks_total > 0 {
            println!("\n  Checks:");
            println!("    Total:  {}", result.checks_total);
            println!("    Passed: {} ({}%)", result.checks_passed,
                     (result.checks_passed as f64 / result.checks_total as f64 * 100.0) as u64);
            println!("    Failed: {} ({}%)", result.checks_failed,
                     (result.checks_failed as f64 / result.checks_total as f64 * 100.0) as u64);
        }

        // Custom / other metrics (type-aware display)
        if !result.metrics.is_empty() {
            println!("\n  All metrics:");
            for metric in &result.metrics {
                if metric.key.starts_with("http_req_duration") || metric.key.starts_with("http_reqs") || metric.key.starts_with("checks") || metric.key.starts_with("iteration_duration") || metric.key.starts_with("iterations") || metric.key.starts_with("http_req_failed") || metric.key.starts_with("data_") {
                    continue; // Already shown above
                }
                print!("    {}  ", metric.key);
                match metric.metric_type {
                    tropel_metrics::collector::MetricType::Counter => {
                        println!("[Counter]  total: {:.0}", metric.sum);
                    }
                    tropel_metrics::collector::MetricType::Rate => {
                        println!("[Rate]  events: {}  rate: {:.4}", metric.count, metric.rate);
                    }
                    tropel_metrics::collector::MetricType::Gauge => {
                        println!("[Gauge]  last: {:.0}  min: {}  max: {}  avg: {:.2}",
                            metric.last, metric.min, metric.max, metric.mean);
                    }
                    tropel_metrics::collector::MetricType::Trend => {
                        println!("[Trend]  count: {}  avg: {:.2}  min: {}  max: {}  p50: {}  p90: {}  p95: {}  p99: {}",
                            metric.count, metric.mean, metric.min, metric.max,
                            metric.p50, metric.p90, metric.p95, metric.p99);
                    }
                }
            }
        }

        println!();

        Ok(())
    }
}
