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
    /// All durations are stored in MILLISECONDS end-to-end (backlog §0), so
    /// time-based trends (k6 `contains: "time"`) render their value with an
    /// `ms` suffix and NO scaling; non-duration trends (byte counts, custom
    /// metrics, `contains: "default"`) render raw with no suffix. The old
    /// name-heuristic `/1000` is gone — values are already in the public unit.
    fn render_trend(out: &mut String, line: &str, m: &MetricSummary, stats: &[String]) {
        let base = m.key.split('{').next().unwrap_or(&m.key);
        let is_time = crate::json_stream::is_time_metric(base);
        let unit = if is_time { "ms" } else { "" };

        let mut parts: Vec<String> = Vec::new();
        for stat in stats {
            if let Some(v) = trend_stat_value(stat, m) {
                match stat.trim() {
                    s if s.starts_with("p(") => {
                        parts.push(format!("{}={:.0}{unit}", stat.trim(), v))
                    }
                    "avg" | "mean" => parts.push(format!("avg={:.2}{unit}", v)),
                    "min" => parts.push(format!("min={:.0}{unit}", v)),
                    "max" => parts.push(format!("max={:.0}{unit}", v)),
                    "count" => parts.push(format!("count={:.0}", v)),
                    "sum" => parts.push(format!("sum={:.2}{unit}", v)),
                    "rate" => parts.push(format!("rate={:.4}", v)),
                    "med" | "median" => parts.push(format!("med={:.0}{unit}", v)),
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
        // k6-style per-second rate (`http_reqs: 136 13.56/s`) — backlog 154.
        let run_secs = result.run_duration.as_secs_f64();
        let per_sec = |n: f64| if run_secs > 0.0 { n / run_secs } else { 0.0 };

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
            "    {:<14}{} ({:.2}/s)\n",
            "Total",
            result.http_reqs,
            per_sec(result.http_reqs as f64)
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
            // One decimal place, matching the HTTP failure line above
            // ("Failed 15 (1.5%)") — integer rounding would under-report
            // small rates (0.2% -> 0%) and make 1996/2000 read as a
            // contradictory "100% passed, 0% failed".
            out.push_str(&format!(
                "    Passed: {} ({:.1}%)\n",
                result.checks_passed,
                result.checks_passed as f64 / result.checks_total as f64 * 100.0
            ));
            out.push_str(&format!(
                "    Failed: {} ({:.1}%)\n",
                result.checks_failed,
                result.checks_failed as f64 / result.checks_total as f64 * 100.0
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
                        out.push_str(&format!(
                            "[Counter]  total: {:.0}  rate: {:.2}/s\n",
                            metric.sum,
                            per_sec(metric.sum)
                        ));
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
                let url = m
                    .tags
                    .iter()
                    .find(|(k, _)| k == "url")
                    .map(|(_, v)| v.as_str());
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
            out.push_str(&format!("\n  {}{}\x1b[0m\n", color, status));
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use tropel_core::config::ThresholdConfig;
    use tropel_metrics::collector::{MetricSummary, MetricType};

    fn trend(key: &str, mean: f64, p50: u64, p90: u64, p95: u64, p99: u64) -> MetricSummary {
        MetricSummary {
            key: key.to_string(),
            tags: vec![],
            metric_type: MetricType::Trend,
            count: 10,
            sum: mean * 10.0,
            mean,
            min: (mean * 0.5) as u64,
            max: (mean * 1.5) as u64,
            p50,
            p90,
            p95,
            p99,
            last: 0.0,
            rate: 0.0,
            histogram: None,
        }
    }

    fn result_with() -> MetricsResult {
        MetricsResult {
            iterations: 68,
            http_reqs: 136,
            vus_max: 2,
            data_received: 266_000.0,
            data_sent: 13_000.0,
            http_req_failed: 0.0,
            dropped_iterations: 0,
            checks_total: 2,
            checks_passed: 2,
            checks_failed: 0,
            run_duration: Duration::from_secs(10),
            http_req_duration: Some(trend("http_req_duration", 134.89, 150, 268, 272, 338)),
            iteration_duration: Some(trend("iteration_duration", 294.58, 262, 279, 321, 1_150)),
            summary_trend_stats: vec![],
            effective_thresholds: HashMap::new(),
            ..Default::default()
        }
    }

    #[test]
    fn render_shows_execution_and_http_blocks() {
        let out = StdoutReporter.render(&result_with());
        assert!(out.contains("Tropel Load Test Summary"), "header box");
        assert!(out.contains("Iterations"), "execution block");
        assert!(out.contains("Max VUs"));
        assert!(out.contains("Dropped"));
        assert!(out.contains("HTTP requests"));
        // k6-style per-second rate: 136 reqs / 10s = 13.60/s.
        assert!(out.contains("13.60/s"), "per-second rate: {out}");
        assert!(out.contains("Total"), "request totals");
    }

    #[test]
    fn render_trend_uses_ms_for_time_metrics_only() {
        let mut out = String::new();
        let m = trend("http_req_duration", 134.89, 150, 268, 272, 338);
        let stats = vec![
            "avg".to_string(),
            "min".to_string(),
            "med".to_string(),
            "max".to_string(),
            "p(90)".to_string(),
        ];
        StdoutReporter::render_trend(&mut out, "", &m, &stats);
        // Values are ms end-to-end (backlog §0): avg=134.89ms, med=150ms,
        // p(90)=268ms — no /1000 anywhere.
        assert!(out.contains("avg=134.89ms"), "{out}");
        assert!(out.contains("med=150ms"), "{out}");
        assert!(out.contains("p(90)=268ms"), "{out}");
        assert!(out.contains("min=67ms"), "{out}");
    }

    #[test]
    fn render_trend_keeps_raw_values_for_non_time_metrics() {
        // A custom byte-count trend is NOT a time metric — values render raw
        // with no ms suffix (regression: old code stamped ms on everything).
        let mut out = String::new();
        let m = trend(
            "http_response_body_size",
            2_500_000.0,
            2_400_000,
            3_000_000,
            3_200_000,
            3_500_000,
        );
        let stats = vec!["avg".to_string(), "med".to_string()];
        StdoutReporter::render_trend(&mut out, "", &m, &stats);
        assert!(out.contains("avg=2500000.00"), "{out}");
        assert!(out.contains("med=2400000"), "{out}");
        assert!(!out.contains("ms"), "no ms suffix on non-time trend: {out}");
    }

    #[test]
    fn render_thresholds_pass_fail_and_status_footer() {
        // PASS case.
        let r = result_with();
        let out = StdoutReporter.render(&r);
        assert!(
            out.contains("✓ PASS — test completed successfully"),
            "{out}"
        );

        // FAIL case: threshold breached.
        let mut failed = result_with();
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "http_req_duration".to_string(),
            ThresholdConfig {
                expression: "http_req_duration avg < 100".to_string(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
        failed.effective_thresholds = thresholds;
        let out = StdoutReporter.render(&failed);
        assert!(out.contains("── Thresholds ──"), "{out}");
        assert!(out.contains("✗"), "failed threshold mark");
        assert!(out.contains("FAIL"), "{out}");
    }

    #[test]
    fn render_per_url_breakdown_when_multiple_urls() {
        let mut r = result_with();
        let mut a = trend("http_req_duration{url=/a}", 100.0, 100, 150, 160, 200);
        let mut b = trend("http_req_duration{url=/b}", 200.0, 200, 250, 260, 300);
        a.tags = vec![("url".to_string(), "/a".to_string())];
        b.tags = vec![("url".to_string(), "/b".to_string())];
        r.per_url = vec![a, b];
        let out = StdoutReporter.render(&r);
        assert!(out.contains("Per-URL (http_req_duration)"), "{out}");
        assert!(out.contains("/a"), "{out}");
        assert!(out.contains("/b"), "{out}");
    }
}
