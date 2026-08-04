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
    ///
    /// Time-based trends (k6 `contains: "time"`) render in `ms` (values are
    /// microseconds internally); non-duration trends (byte counts, custom
    /// metrics, `contains: "default"`) render raw — the old code stamped
    /// `ms` on every trend, mislabeling non-duration metrics.
    fn render_trend(out: &mut String, line: &str, m: &MetricSummary, stats: &[String]) {
        let base = m.key.split('{').next().unwrap_or(&m.key);
        let is_time = crate::json_stream::is_time_metric(base);
        let unit = if is_time { "ms" } else { "" };

        let mut parts: Vec<String> = Vec::new();
        for stat in stats {
            if let Some(v) = trend_stat_value(stat, m) {
                match stat.trim() {
                    s if s.starts_with("p(") => parts.push(format!(
                        "{}={:.0}{unit}",
                        stat.trim(),
                        if is_time { v / 1000.0 } else { v }
                    )),
                    "avg" | "mean" => parts.push(format!(
                        "avg={:.2}{unit}",
                        if is_time { v / 1000.0 } else { v }
                    )),
                    "min" => parts.push(format!(
                        "min={:.0}{unit}",
                        if is_time { v / 1000.0 } else { v }
                    )),
                    "max" => parts.push(format!(
                        "max={:.0}{unit}",
                        if is_time { v / 1000.0 } else { v }
                    )),
                    "count" => parts.push(format!("count={:.0}", v)),
                    "sum" => parts.push(format!(
                        "sum={:.2}{unit}",
                        if is_time { v / 1000.0 } else { v }
                    )),
                    "rate" => parts.push(format!("rate={:.4}", v)),
                    "med" | "median" => parts.push(format!(
                        "med={:.0}{unit}",
                        if is_time { v / 1000.0 } else { v }
                    )),
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

        // ── Dynamic-width centered header box ──
        const BOX_W: usize = 66;
        let title = "Tropel Load Test Summary";
        let pad = (BOX_W - 2 - title.chars().count()) / 2;
        let left_pad = " ".repeat(pad);
        let right_pad = " ".repeat(BOX_W - 2 - title.chars().count() - pad);
        out.push_str(&format!("\n╔{}╗\n", "═".repeat(BOX_W - 2)));
        out.push_str(&format!("║{}{}{}║\n", left_pad, title, right_pad));
        out.push_str(&format!("╠{}╣\n", "═".repeat(BOX_W - 2)));

        // Execution overview — aligned two-column block
        out.push_str("  ── Execution ─────────────────────────────────────────────\n");
        let exec_rows = [
            ("Iterations", result.iterations.to_string()),
            ("Max VUs", result.vus_max.to_string()),
            ("Dropped", result.dropped_iterations.to_string()),
        ];
        for (label, value) in exec_rows {
            out.push_str(&format!("    {:<14}{}\n", label, value));
        }

        // HTTP requests — aligned two-column block
        out.push_str("\n  ── HTTP requests ─────────────────────────────────────────\n");
        out.push_str(&format!(
            "    {:<14}{}\n",
            "Total",
            result.http_reqs
        ));
        out.push_str(&format!(
            "    {:<14}{} ({:.1}%)\n",
            "Failed",
            (result.http_req_failed * result.http_reqs as f64) as u64,
            result.http_req_failed * 100.0
        ));
        out.push_str(&format!(
            "    {:<14}{:.2} MB\n",
            "Data received",
            result.data_received / 1_000_000.0
        ));
        out.push_str(&format!(
            "    {:<14}{:.2} MB\n",
            "Data sent",
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
                    || metric.tags.iter().any(|(k, _)| k == "group")
                {
                    continue; // Already shown above or in the per-group breakdown
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
                    tropel_metrics::collector::                    MetricType::Trend => {
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

        // Per-group breakdown — the collector merges all series carrying a
        // `group` tag into exact per-(metric, group) summaries stored in the
        // dedicated `result.per_group` field (kept out of `metrics` so
        // thresholds can't double-count). The runner tags every request
        // `group=http` by default, so exclude that constant — the headline
        // already covers overall HTTP; named groups from `group()`/`pm.group`
        // produce the meaningful rows.
        let grouped_series: Vec<&MetricSummary> = result
            .per_group
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
            out.push_str("\n  ── Thresholds ──────────────────────────────────────────\n");
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

        // ── Status footer: green PASS / red FAIL ──
        // ANSI colors only when stdout is a TTY so piped output stays clean.
        // FAIL is driven by THRESHOLDS ONLY — matching k6 semantics and the
        // CLI exit code (cli.rs returns Err on threshold failure, not on
        // ordinary request failures like a single 404 in thousands).
        let thresholds_failed = evaluate_thresholds(&result.effective_thresholds, result)
            .iter()
            .any(|t| !t.passed);
        let (status, color) = if thresholds_failed {
            ("✗ FAIL — one or more thresholds crossed", "\x1b[31m") // red
        } else {
            ("✓ PASS — test completed successfully", "\x1b[32m") // green
        };
        if Self::stdout_is_tty() {
            out.push_str(&format!(
                "\n  {}{}\x1b[0m\n",
                color, status
            ));
        } else {
            out.push_str(&format!("\n  {}\n", status));
        }
        out.push_str(&format!("╚{}╝\n", "═".repeat(BOX_W - 2)));

        out.push('\n');
        out
    }

    /// True when stdout is an interactive terminal (ANSI colors safe).
    fn stdout_is_tty() -> bool {
        use std::io::IsTerminal;
        std::io::stdout().is_terminal()
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
