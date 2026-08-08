//! k6-style summary data builder.
//!
//! Turns the aggregated results into the `handleSummary(data)` argument
//! object (per-metric values typed like k6, a top-level `thresholds` map,
//! and run state). Moved out of the former `engine.rs` god-file.

use serde_json::{json, Map};
use std::collections::HashMap;
use std::time::Instant;
use tropel_core::config::ThresholdConfig;
use tropel_metrics::collector::MetricType;
use tropel_metrics::collector::MetricsResult;
use tropel_metrics::thresholds::evaluate_thresholds;

/// Build the k6-style summary data object (`handleSummary(data)` argument)
/// from the aggregated results: per-metric values typed like k6 plus a
/// top-level `thresholds` map (expression → pass/fail) and run state.
pub(crate) fn build_summary_data(
    results: &MetricsResult,
    thresholds: &HashMap<String, ThresholdConfig>,
    test_start: Instant,
) -> serde_json::Value {
    let mut metrics = Map::new();
    for m in &results.metrics {
        let (typ, contains, values) = match m.metric_type {
            MetricType::Counter => {
                // k6 handleSummary Counter values: `count` (accumulated) +
                // `rate` = count / elapsed seconds (backlog line 154).
                let secs = results.run_duration.as_secs_f64();
                let rate = if secs > 0.0 {
                    m.count as f64 / secs
                } else {
                    0.0
                };
                (
                    "counter",
                    "default",
                    json!({ "count": m.count, "rate": rate }),
                )
            }
            MetricType::Gauge => (
                "gauge",
                "default",
                json!({ "value": m.last, "min": m.min, "max": m.max, "avg": m.mean }),
            ),
            MetricType::Rate => (
                "rate",
                "default",
                json!({ "rate": m.rate, "count": m.count }),
            ),
            MetricType::Trend => {
                // Values are already in ms end-to-end (backlog §0); k6's
                // `contains` is `"time"` only for duration metrics — a custom
                // byte-count Trend must NOT be labelled time (the old code
                // hardcoded "time" for every Trend). The classification
                // delegates to tropel-metrics (single source of truth shared
                // with json-stream/stdout) so outputs never disagree.
                let base = m.key.split('{').next().unwrap_or(&m.key);
                let contains = if tropel_metrics::time_metrics::is_time_metric(base) {
                    "time"
                } else {
                    "default"
                };
                (
                    "trend",
                    contains,
                    json!({
                        "avg": m.mean,
                        "min": m.min,
                        "med": m.p50,
                        "max": m.max,
                        "p(90)": m.p90,
                        "p(95)": m.p95,
                        "p(99)": m.p99,
                        "count": m.count,
                    }),
                )
            }
        };
        metrics.insert(
            m.key.clone(),
            json!({
                "type": typ,
                "contains": contains,
                "values": values,
            }),
        );
    }

    let mut thresholds_map = Map::new();
    for t in evaluate_thresholds(thresholds, results) {
        thresholds_map.insert(t.expression.clone(), json!(t.passed));
    }

    json!({
        "metrics": metrics,
        "root_group": { "name": "", "path": "", "id": "", "groups": [], "checks": [] },
        "options": {},
        "thresholds": thresholds_map,
        "state": {
            "testRunDurationMs": test_start.elapsed().as_millis() as u64,
            "iterations": results.iterations,
            "vusMax": results.vus_max,
            "http_reqs": results.http_reqs,
            "checksTotal": results.checks_total,
            "checksPassed": results.checks_passed,
            "checksFailed": results.checks_failed,
        },
    })
}
