//! # Reporter snapshot tests (insta)
//!
//! Golden-file regression tests for the three text reporters. A fixed
//! `MetricsResult` fixture is rendered by each reporter and compared
//! byte-for-byte against a stored snapshot — any accidental change to
//! summary layout, units, or rounding shows up as a snapshot diff.

use std::collections::HashMap;
use tropel_core::config::ThresholdConfig;
use tropel_metrics::collector::{
    k6_default_trend_stats, MetricSummary, MetricType, MetricsResult,
};
use tropel_report::{CsvReporter, JsonReporter, StdoutReporter};

/// A fully-populated `MetricsResult` that exercises every summary section:
/// execution, HTTP, trend stats, checks, custom metrics of each type,
/// per-URL breakdown, per-group breakdown, and thresholds (pass + fail).
fn fixture() -> MetricsResult {
    let trend = |key: &str, tags: Vec<(&str, &str)>, count: u64, max: u64| MetricSummary {
        key: key.to_string(),
        tags: tags
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        metric_type: MetricType::Trend,
        count,
        sum: 250_000_000.0,
        mean: 250_000.0,
        min: 10_000,
        max,
        p50: 180_000,
        p90: 420_000,
        p95: 620_000,
        p99: 900_000,
        last: 0.0,
        rate: 0.0,
        histogram: None,
    };

    let mut result = MetricsResult {
        iterations: 1_024,
        vus_max: 4,
        dropped_iterations: 2,
        http_reqs: 1_000,
        http_req_failed: 0.015,
        data_received: 12_500_000.0,
        data_sent: 2_000_000.0,
        checks_total: 2_000,
        checks_passed: 1_996,
        checks_failed: 4,
        errors: 0,
        summary_trend_stats: k6_default_trend_stats(),
        effective_thresholds: HashMap::from([
            (
                "http_req_duration".to_string(),
                ThresholdConfig {
                    expression: "http_req_duration.p95 < 1000".to_string(),
                    abort_on_fail: false,
                    delay_abort_eval: None,
                },
            ),
            (
                "checks".to_string(),
                ThresholdConfig {
                    expression: "checks.pass_rate > 0.99".to_string(),
                    abort_on_fail: false,
                    delay_abort_eval: None,
                },
            ),
        ]),
        http_req_duration: Some(trend("http_req_duration", vec![], 1_000, 950_000)),
        iteration_duration: Some(trend("iteration_duration", vec![], 1_024, 1_100_000)),
        metrics: vec![
            MetricSummary {
                key: "custom_counter".to_string(),
                tags: vec![],
                metric_type: MetricType::Counter,
                count: 42,
                sum: 42.0,
                mean: 1.0,
                min: 1,
                max: 1,
                p50: 1,
                p90: 1,
                p95: 1,
                p99: 1,
                last: 0.0,
                rate: 0.0,
                histogram: None,
            },
            MetricSummary {
                key: "custom_gauge".to_string(),
                tags: vec![],
                metric_type: MetricType::Gauge,
                count: 9,
                sum: 45.0,
                mean: 5.0,
                min: 1,
                max: 9,
                p50: 5,
                p90: 9,
                p95: 9,
                p99: 9,
                last: 7.0,
                rate: 0.0,
                histogram: None,
            },
        ],
        per_url: vec![
            trend(
                "http_req_duration{url=/api/a}",
                vec![("url", "/api/a")],
                600,
                800_000,
            ),
            trend(
                "http_req_duration{url=/api/b}",
                vec![("url", "/api/b")],
                400,
                950_000,
            ),
        ],
        // Per-group breakdown — the collector merges group-tagged series
        // into this dedicated field (k6 parity: aggregated per group, not
        // raw per-(url,status) series).
        per_group: vec![trend(
            "group_duration{group=checkout}",
            vec![("group", "checkout")],
            500,
            700_000,
        )],
        // Wall-clock run duration — backs k6-style per-second rates
        // (`http_reqs: 1000 33.33/s`) in the stdout summary (backlog 154).
        run_duration: std::time::Duration::from_secs(30),
    };
    result.metrics.extend(result.per_url.clone());
    result
}

#[test]
fn stdout_summary_snapshot() {
    let rendered = StdoutReporter.render(&fixture());
    insta::assert_snapshot!("stdout_summary", rendered);
}

#[test]
fn json_report_snapshot() {
    let rendered = JsonReporter::new(None).render(&fixture()).unwrap();
    insta::assert_snapshot!("json_report", rendered);
}

#[test]
fn csv_report_snapshot() {
    let rendered = CsvReporter::new(None).render(&fixture());
    insta::assert_snapshot!("csv_report", rendered);
}
