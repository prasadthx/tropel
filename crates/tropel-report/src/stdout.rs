use crate::Reporter;
use async_trait::async_trait;
use tropel_core::Result;
use tropel_metrics::collector::{trend_stat_value, MetricSummary, MetricsResult};
use tropel_metrics::thresholds::evaluate_thresholds;

/// Prints a summary report to stdout.
pub struct StdoutReporter;

impl StdoutReporter {
    /// Render a Trend metric using the configured `summaryTrendStats` list.
    fn render_trend(line: &str, m: &MetricSummary, stats: &[String]) {
        let mut parts: Vec<String> = Vec::new();
        for stat in stats {
            if let Some(v) = trend_stat_value(stat, m) {
                match stat.trim() {
                    s if s.starts_with("p(") => {
                        parts.push(format!("{}={:.0}ms", stat.trim(), v / 1000.0));
                    }
                    "avg" | "mean" => parts.push(format!("avg={:.2}ms", v / 1000.0)),
                    "min" => parts.push(format!("min={:.0}ms", v / 1000.0)),
                    "max" => parts.push(format!("max={:.0}ms", v / 1000.0)),
                    "count" => parts.push(format!("count={:.0}", v)),
                    "sum" => parts.push(format!("sum={:.2}ms", v / 1000.0)),
                    "rate" => parts.push(format!("rate={:.4}", v)),
                    "med" | "median" => parts.push(format!("med={:.0}ms", v / 1000.0)),
                    _ => parts.push(format!("{}={:.2}", stat.trim(), v)),
                }
            }
        }
        println!("    {}{}", line, parts.join("  "));
    }
}

#[async_trait]
impl Reporter for StdoutReporter {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn report(&self, result: &MetricsResult) -> Result<()> {
        let stats = if result.summary_trend_stats.is_empty() {
            vec![
                "avg".to_string(),
                "min".to_string(),
                "med".to_string(),
                "max".to_string(),
                "p(90)".to_string(),
                "p(95)".to_string(),
                "p(99)".to_string(),
            ]
        } else {
            result.summary_trend_stats.clone()
        };

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
        println!(
            "    Failed:    {} ({:.1}%)",
            (result.http_req_failed * result.http_reqs as f64) as u64,
            result.http_req_failed * 100.0
        );
        println!(
            "    Data received: {:.2} MB",
            result.data_received / 1_000_000.0
        );
        println!(
            "    Data sent:     {:.2} MB",
            result.data_sent / 1_000_000.0
        );

        if let Some(duration) = &result.http_req_duration {
            println!("\n  HTTP request duration:");
            Self::render_trend("", duration, &stats);
        }

        // Iteration duration
        if let Some(dur) = &result.iteration_duration {
            println!("\n  Iteration duration:");
            Self::render_trend("", dur, &stats);
        }

        // Checks/assertions
        if result.checks_total > 0 {
            println!("\n  Checks:");
            println!("    Total:  {}", result.checks_total);
            println!(
                "    Passed: {} ({}%)",
                result.checks_passed,
                (result.checks_passed as f64 / result.checks_total as f64 * 100.0) as u64
            );
            println!(
                "    Failed: {} ({}%)",
                result.checks_failed,
                (result.checks_failed as f64 / result.checks_total as f64 * 100.0) as u64
            );
        }

        // Custom / other metrics (type-aware display)
        if !result.metrics.is_empty() {
            println!("\n  All metrics:");
            for metric in &result.metrics {
                if metric.key.starts_with("http_req_duration")
                    || metric.key.starts_with("http_reqs")
                    || metric.key.starts_with("checks")
                    || metric.key.starts_with("iteration_duration")
                    || metric.key.starts_with("iterations")
                    || metric.key.starts_with("http_req_failed")
                    || metric.key.starts_with("data_")
                {
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
                        println!(
                            "[Gauge]  last: {:.0}  min: {}  max: {}  avg: {:.2}",
                            metric.last, metric.min, metric.max, metric.mean
                        );
                    }
                    tropel_metrics::collector::MetricType::Trend => {
                        println!("[Trend]");
                        Self::render_trend("", metric, &stats);
                    }
                }
            }
        }

        // Thresholds — pass/fail against the effective threshold set.
        if !result.effective_thresholds.is_empty() {
            println!("\n  Thresholds:");
            let threshold_results = evaluate_thresholds(&result.effective_thresholds, result);
            for tr in &threshold_results {
                let op = tr.expression.split_whitespace().nth(1).unwrap_or("<?>");
                if tr.passed {
                    println!(
                        "    ✓ {}: {:.2} {} {:.2} (PASS)",
                        tr.name, tr.actual, op, tr.threshold
                    );
                } else {
                    println!(
                        "    ✗ {}: {:.2} {} {:.2} (FAIL)",
                        tr.name, tr.actual, op, tr.threshold
                    );
                }
            }
        }

        println!();

        Ok(())
    }
}
