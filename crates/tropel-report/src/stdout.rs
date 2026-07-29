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

        // HTTP requests
        println!("  HTTP requests:");
        println!("    Total:     {}", result.http_reqs);
        println!("    Failed:    {}", result.errors);
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

        // Checks/assertions
        if result.checks_total > 0 {
            println!("\n  Checks:");
            println!("    Total:  {}", result.checks_total);
            println!("    Passed: {} ({}%)", result.checks_passed,
                     (result.checks_passed as f64 / result.checks_total as f64 * 100.0) as u64);
            println!("    Failed: {} ({}%)", result.checks_failed,
                     (result.checks_failed as f64 / result.checks_total as f64 * 100.0) as u64);
        }

        // Other metrics
        if !result.metrics.is_empty() {
            println!("\n  All metrics:");
            for metric in &result.metrics {
                if metric.key.starts_with("http_req_duration") || metric.key.starts_with("http_reqs") || metric.key.starts_with("checks") {
                    continue; // Already shown above
                }
                println!("    {}:", metric.key);
                println!("      count: {}", metric.count);
                println!("      avg:   {:.2}", metric.mean);
                println!("      min:   {}", metric.min);
                println!("      max:   {}", metric.max);
            }
        }

        println!();

        Ok(())
    }
}
