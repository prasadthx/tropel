use crate::Reporter;
use async_trait::async_trait;
use tropel_core::Result;
use tropel_metrics::collector::{trend_stat_value, MetricSummary, MetricsResult};
use tropel_metrics::thresholds::evaluate_thresholds;

/// Prints a summary report to stdout.
pub struct StdoutReporter;

impl StdoutReporter {
    /// Render a Trend metric using the configured `summaryTrendStats` list,
    /// appending to `out` (single trailing newline).
    fn render_trend(out: &mut String, line: &str, m: &MetricSummary, stats: &[String]) {
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
        out.push_str(&format!("    {}{}\n", line, parts.join("  ")));
    }

    /// Render the full summary to a String (no I/O). Exposed for tests and
    /// programmatic consumers; `report()` just prints it.
    pub fn render(&self, result: &MetricsResult) -> String {
        let mut out = String::new();
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

        out.push_str("\n╔══════════════════════════════════════════════════╗\n");
        out.push_str("║          Tropel Load Test Summary               ║\n");
        out.push_str("╚══════════════════════════════════════════════════╝\n\n");

        // Execution overview
        out.push_str("  Execution:\n");
        out.push_str(&format!("    Iterations:     {}\n", result.iterations));
        out.push_str(&format!("    Max VUs:        {}\n", result.vus_max));
        out.push_str(&format!("    Dropped:        {}\n", result.dropped_iterations));

        // HTTP requests
        out.push_str("\n  HTTP requests:\n");
        out.push_str(&format!("    Total:     {}\n", result.http_reqs));
        out.push_str(&format!(
            "    Failed:    {} ({:.1}%)\n",
            (result.http_req_failed * result.http_reqs as f64) as u64,
            result.http_req_failed * 100.0
        ));
        out.push_str(&format!(
            "    Data received: {:.2} MB\n",
            result.data_received / 1_000_000.0
        ));
        out.push_str(&format!(
            "    Data sent:     {:.2} MB\n",
            result.data_sent / 1_000_000.0
        ));

        if let Some(duration) = &result.http_req_duration {
            out.push_str("\n  HTTP request duration:\n");
            Self::render_trend(&mut out, "", duration, &stats);
        }

        // Iteration duration
        if let Some(dur) = &result.iteration_duration {
            out.push_str("\n  Iteration duration:\n");
            Self::render_trend(&mut out, "", dur, &stats);
        }

        // Checks/assertions
        if result.checks_total > 0 {
            out.push_str("\n  Checks:\n");
            out.push_str(&format!("    Total:  {}\n", result.checks_total));
            out.push_str(&format!(
                "    Passed: {} ({}%)\n",
                result.checks_passed,
                (result.checks_passed as f64 / result.checks_total as f64 * 100.0) as u64
            ));
            out.push_str(&format!(
                "    Failed: {} ({}%)\n",
                result.checks_failed,
                (result.checks_failed as f64 / result.checks_total as f64 * 100.0) as u64
            ));
        }

        // Custom / other metrics (type-aware display)
        if !result.metrics.is_empty() {
            out.push_str("\n  All metrics:\n");
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
                out.push_str(&format!("    {}  ", metric.key));
                match metric.metric_type {
                    tropel_metrics::collector::MetricType::Counter => {
                        out.push_str(&format!("[Counter]  total: {:.0}\n", metric.sum));
                    }
                    tropel_metrics::collector::MetricType::Rate => {
                        out.push_str(&format!(
                            "[Rate]  events: {}  rate: {:.4}\n",
                            metric.count, metric.rate
                        ));
                    }
                    tropel_metrics::collector::MetricType::Gauge => {
                        out.push_str(&format!(
                            "[Gauge]  last: {:.0}  min: {}  max: {}  avg: {:.2}\n",
                            metric.last, metric.min, metric.max, metric.mean
                        ));
                    }
                    tropel_metrics::collector::MetricType::Trend => {
                        out.push_str("[Trend]\n");
                        Self::render_trend(&mut out, "", metric, &stats);
                    }
                }
            }
        }

        // Per-URL breakdown — the collector merges all http_req_duration
        // series per distinct `url` tag into exact per-URL summaries stored
        // in the dedicated `result.per_url` field (kept out of `metrics` so
        // threshold evaluation can't double-count). One row per URL with true
        // merged percentiles.
        if result.per_url.len() > 1 {
            out.push_str("\n  Per-URL (http_req_duration):\n");
            for m in &result.per_url {
                let url = m.tags.iter().find(|(k, _)| k == "url").map(|(_, v)| v.as_str());
                let url = url.unwrap_or(&m.key);
                out.push_str(&format!("    {}  (reqs: {})\n", url, m.count));
                Self::render_trend(&mut out, "  ", m, &stats);
            }
        }

        // Per-group breakdown — series carrying a `group` tag. The runner
        // tags every request `group=http` by default, so exclude that
        // constant (the headline already covers overall HTTP); named groups
        // from `group()`/`pm.group` produce the meaningful rows.
        let grouped_series: Vec<&MetricSummary> = result
            .metrics
            .iter()
            .filter(|m| m.tags.iter().any(|(k, v)| k == "group" && v != "http"))
            .collect();
        if !grouped_series.is_empty() {
            out.push_str("\n  Per-group breakdown:\n");
            for m in &grouped_series {
                let group = m
                    .tags
                    .iter()
                    .find(|(k, _)| k == "group")
                    .map(|(_, v)| v.as_str())
                    .unwrap_or("");
                let metric = m.key.split('{').next().unwrap_or("");
                out.push_str(&format!("    {}", metric));
                match m.metric_type {
                    tropel_metrics::collector::MetricType::Rate => {
                        out.push_str(&format!("  [group={}]  rate: {:.4}\n", group, m.rate));
                    }
                    tropel_metrics::collector::MetricType::Counter => {
                        out.push_str(&format!("  [group={}]  total: {:.0}\n", group, m.sum));
                    }
                    tropel_metrics::collector::MetricType::Gauge => {
                        out.push_str(&format!(
                            "  [group={}]  last: {:.0}  min: {}  max: {}\n",
                            group, m.last, m.min, m.max
                        ));
                    }
                    tropel_metrics::collector::MetricType::Trend => {
                        out.push_str(&format!("  [group={}]\n", group));
                        Self::render_trend(&mut out, "      ", m, &stats);
                    }
                }
            }
        }

        // Thresholds — pass/fail against the effective threshold set.
        if !result.effective_thresholds.is_empty() {
            out.push_str("\n  Thresholds:\n");
            let threshold_results = evaluate_thresholds(&result.effective_thresholds, result);
            for tr in &threshold_results {
                let op = tr.expression.split_whitespace().nth(1).unwrap_or("<?>");
                if tr.passed {
                    out.push_str(&format!(
                        "    ✓ {}: {:.2} {} {:.2} (PASS)\n",
                        tr.name, tr.actual, op, tr.threshold
                    ));
                } else {
                    out.push_str(&format!(
                        "    ✗ {}: {:.2} {} {:.2} (FAIL)\n",
                        tr.name, tr.actual, op, tr.threshold
                    ));
                }
            }
        }

        out.push('\n');
        out
    }
}

#[async_trait]
impl Reporter for StdoutReporter {
    fn name(&self) -> &str {
        "stdout"
    }

    async fn report(&self, result: &MetricsResult) -> Result<()> {
        print!("{}", self.render(result));
        Ok(())
    }
}
