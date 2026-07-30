use crate::collector::MetricsResult;
use std::collections::HashMap;
use std::time::Duration;
use tropel_core::config::ThresholdConfig;

/// Result of threshold evaluation.
#[derive(Debug, Clone)]
pub struct ThresholdResult {
    pub name: String,
    pub expression: String,
    pub passed: bool,
    pub actual: f64,
    pub threshold: f64,
    pub abort_on_fail: bool,
    pub delay_abort_eval: Option<Duration>,
}

/// Evaluate thresholds against aggregated metrics.
pub fn evaluate_thresholds(
    thresholds: &HashMap<String, ThresholdConfig>,
    metrics: &MetricsResult,
) -> Vec<ThresholdResult> {
    let mut results = Vec::new();

    for (name, config) in thresholds {
        let result = evaluate_single_threshold(&config.expression, metrics);
        results.push(ThresholdResult {
            name: name.clone(),
            expression: config.expression.clone(),
            passed: result.0,
            actual: result.1,
            threshold: result.2,
            abort_on_fail: config.abort_on_fail,
            delay_abort_eval: config.delay_abort_eval.as_ref()
                .and_then(|s| parse_duration(s).ok()),
        });
    }

    results
}

/// Check if any abort-on-fail threshold has been breached (mid-run evaluation).
/// Returns `true` if the test should be aborted immediately.
/// Respects `delay_abort_eval` — thresholds in their grace period won't abort.
pub fn check_abort_on_fail(
    thresholds: &HashMap<String, ThresholdConfig>,
    metrics: &MetricsResult,
    elapsed: Duration,
) -> bool {
    for (name, config) in thresholds {
        if !config.abort_on_fail {
            continue;
        }

        // Check if delayAbortEval grace period is still active
        if let Some(ref delay_str) = config.delay_abort_eval {
            if let Ok(delay) = parse_duration(delay_str) {
                if elapsed < delay {
                    continue; // Still in grace period — don't abort yet
                }
            }
        }

        let (passed, _, _) = evaluate_single_threshold(&config.expression, metrics);
        if !passed {
            tracing::error!(
                "Threshold '{}' ({}) breached with abortOnFail — aborting test",
                name, config.expression
            );
            return true;
        }
    }
    false
}

/// Parse a duration string like "30s", "1m", "500ms" into a Duration.
fn parse_duration(s: &str) -> std::result::Result<Duration, ()> {
    let s = s.trim();
    if let Some(num_str) = s.strip_suffix("ms") {
        let ms: u64 = num_str.parse().map_err(|_| ())?;
        Ok(Duration::from_millis(ms))
    } else if let Some(num_str) = s.strip_suffix('s') {
        let secs: f64 = num_str.parse().map_err(|_| ())?;
        Ok(Duration::from_secs_f64(secs))
    } else if let Some(num_str) = s.strip_suffix('m') {
        let mins: f64 = num_str.parse().map_err(|_| ())?;
        Ok(Duration::from_secs_f64(mins * 60.0))
    } else if let Some(num_str) = s.strip_suffix('h') {
        let hours: f64 = num_str.parse().map_err(|_| ())?;
        Ok(Duration::from_secs_f64(hours * 3600.0))
    } else {
        // Default to seconds
        let secs: f64 = s.parse().map_err(|_| ())?;
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Parse a tag-scoped metric reference like `"http_req_duration{status=200}.p95"`
/// into its components: (metric_name, tags, stat).
///
/// Returns:
/// - `metric_name`: the base metric name before any `{...}` or `.stat` suffix
/// - `tags`: vector of `(key, value)` pairs extracted from `{key=value,...}` (empty if none)
/// - `stat`: the statistic part after `.` (None if absent)
///
/// Examples:
///   "http_req_duration{status=200}.p95" → ("http_req_duration", [(status, 200)], "p95")
///   "http_reqs"                        → ("http_reqs", [], None)
///   "checks.pass_rate"                 → ("checks", [], "pass_rate")
fn parse_metric_ref(metric_ref: &str) -> (&str, Vec<(&str, &str)>, Option<&str>) {
    // Step 1: Find the tag block boundaries
    let (brace_start, brace_close) = {
        let start = metric_ref.find('{');
        let end = start.and_then(|s| metric_ref[s..].find('}').map(|i| s + i));
        (start, end)
    };

    // Step 2: Extract tags from inside `{...}`
    let tags = if let (Some(bs), Some(bc)) = (brace_start, brace_close) {
        metric_ref[bs+1..bc]
            .split(',')
            .filter_map(|pair| {
                let pair = pair.trim();
                if pair.is_empty() {
                    return None;
                }
                // Support both `:` and `=` as key=value separators
                let sep = if pair.contains(':') { ':' } else { '=' };
                pair.split_once(sep).map(|(k, v)| (k.trim(), v.trim()))
            })
            .collect()
    } else {
        vec![]
    };

    // Step 3: Extract metric name and stat suffix
    // The metric name is the text before `{` (or the whole string if no tags).
    // The stat suffix can be after `}` (e.g. `{status=200}.p95`) or
    // after the name but before `{` (e.g. `.p95{status=200}`).
    let before = &metric_ref[..brace_start.unwrap_or(metric_ref.len())];
    let after = &metric_ref[brace_close.map(|bc| bc + 1).unwrap_or(metric_ref.len())..];

    // Find stat: prefer after `}` (more common), fall back to before `{`
    let (name, stat) = if let Some(dot) = after.rfind('.') {
        // Stat is after `}` — name is the part before `{`
        (before, Some(&after[dot + 1..]))
    } else if let Some(dot) = before.rfind('.') {
        // Stat is before `{` — strip it from the name
        (&before[..dot], Some(&before[dot + 1..]))
    } else {
        // No stat suffix
        (before, None)
    };

    (name, tags, stat)
}

/// Evaluate a single threshold expression against real metrics.
/// Supports expressions like:
///   "http_req_duration.p95 < 500"
///   "http_req_duration{status=200}.p95 < 500"
///   "http_reqs > 100"
///   "checks.pass_rate > 0.99"
///   "errors < 10"
fn evaluate_single_threshold(expression: &str, metrics: &MetricsResult) -> (bool, f64, f64) {
    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() < 3 {
        tracing::warn!("Invalid threshold expression: '{}'", expression);
        return (true, 0.0, 0.0);
    }

    // Format: "metric_ref operator value"
    // metric_ref can be "http_req_duration.p95", "http_req_duration{status=200}.p95",
    // or just "http_reqs"
    let metric_ref = parts[0];
    let operator = parts[1];
    let threshold: f64 = match parts[2].parse() {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!("Invalid threshold value in '{}': '{}'", expression, parts[2]);
            return (true, 0.0, 0.0);
        }
    };

    // Parse metric reference into (metric_name, tags, stat)
    let (metric_name, tag_filters, stat) = parse_metric_ref(metric_ref);

    // Look up the actual metric value
    let actual = if !tag_filters.is_empty() {
        // Tag-scoped threshold: search metrics.metrics for matching entries
        get_tag_scoped_metric_value(metrics, metric_name, &tag_filters, stat)
    } else {
        // No tag filter — use the existing top-level lookup
        get_metric_value(metrics, metric_name, stat)
    };

    let passed = match operator {
        "<" => actual < threshold,
        "<=" => actual <= threshold,
        ">" => actual > threshold,
        ">=" => actual >= threshold,
        "==" => (actual - threshold).abs() < f64::EPSILON,
        "!=" => (actual - threshold).abs() > f64::EPSILON,
        _ => {
            tracing::warn!("Unknown operator '{}' in threshold '{}'", operator, expression);
            true
        }
    };

    (passed, actual, threshold)
}

/// Get a metric value for a tag-scoped threshold by searching the metrics list.
/// Looks for entries whose key starts with the metric name and contains all
/// the specified tag key=value pairs.
///
/// When MULTIPLE entries match (e.g. `http_req_duration{status=200}{method=GET}`
/// and `http_req_duration{status=200}{method=POST}` both match `{status=200}`),
/// the function aggregates:
/// - **Percentiles** (p50/p90/p95/p99): returns the WORST (highest) value
/// - **Avg/mean**: returns the WORST (highest) value across matches
/// - **Min**: returns the MINIMUM value across matches
/// - **Max**: returns the MAXIMUM value across matches
/// - **Count**: returns the SUM of counts
/// - **Sum**: returns the SUM of sums
/// - **Rate**: recomputes sum/count from totals
///
/// If no entry matches, returns 0.0.
fn get_tag_scoped_metric_value(
    metrics: &MetricsResult,
    metric_name: &str,
    tag_filters: &[(&str, &str)],
    stat: Option<&str>,
) -> f64 {
    let mut matched = Vec::new();

    for m in &metrics.metrics {
        // Check if this entry's key starts with the metric name
        if !m.key.starts_with(metric_name) {
            continue;
        }
        // Check if all tag filters are present in the key string
        // The key format is like "http_req_duration{status=200}{method=GET}"
        let all_tags_match = tag_filters.iter().all(|(key, val)| {
            let pattern = format!("{{{}={}}}", key, val);
            m.key.contains(&pattern)
        });
        if !all_tags_match {
            continue;
        }
        matched.push(m);
    }

    if matched.is_empty() {
        return 0.0;
    }

    // Aggregate all matching entries
    match stat {
        Some("avg") => {
            // Return the WORST (highest) mean across all matches
            matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max)
        }
        Some("min") => {
            // Return the MINIMUM min across all matches
            matched.iter().map(|m| m.min as f64).fold(f64::MAX, f64::min)
        }
        Some("max") => {
            // Return the MAXIMUM max across all matches
            matched.iter().map(|m| m.max as f64).fold(0.0_f64, f64::max)
        }
        Some("p50") | Some("median") => {
            matched.iter().map(|m| m.p50 as f64).fold(0.0_f64, f64::max)
        }
        Some("p90") => {
            matched.iter().map(|m| m.p90 as f64).fold(0.0_f64, f64::max)
        }
        Some("p95") => {
            matched.iter().map(|m| m.p95 as f64).fold(0.0_f64, f64::max)
        }
        Some("p99") => {
            matched.iter().map(|m| m.p99 as f64).fold(0.0_f64, f64::max)
        }
        Some("count") => {
            matched.iter().map(|m| m.count as f64).sum()
        }
        Some("rate") => {
            let total_sum: f64 = matched.iter().map(|m| m.sum).sum();
            let total_count: f64 = matched.iter().map(|m| m.count as f64).sum();
            if total_count > 0.0 { total_sum / total_count } else { 0.0 }
        }
        Some("sum") => {
            matched.iter().map(|m| m.sum).sum()
        }
        Some("last") => {
            matched.last().map(|m| m.last).unwrap_or(0.0)
        }
        // Default (no stat or unknown stat) — return WORST mean
        _ => {
            matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max)
        }
    }
}

/// Extract a metric value from the MetricsResult by name and optional statistic.
fn get_metric_value(metrics: &MetricsResult, name: &str, stat: Option<&str>) -> f64 {
    match name {
        "http_reqs" => metrics.http_reqs as f64,
        "errors" => metrics.errors as f64,
        "checks" | "checks.total" => metrics.checks_total as f64,
        "checks.passed" => metrics.checks_passed as f64,
        "checks.failed" => metrics.checks_failed as f64,
        "checks.pass_rate" => {
            if metrics.checks_total > 0 {
                metrics.checks_passed as f64 / metrics.checks_total as f64
            } else {
                0.0
            }
        }
        "http_req_duration" => {
            if let Some(d) = &metrics.http_req_duration {
                match stat {
                    Some("avg") => d.mean,
                    Some("min") => d.min as f64,
                    Some("max") => d.max as f64,
                    Some("p50") | Some("median") => d.p50 as f64,
                    Some("p90") => d.p90 as f64,
                    Some("p95") => d.p95 as f64,
                    Some("p99") => d.p99 as f64,
                    Some("count") => d.count as f64,
                    _ => d.mean, // default to mean if no stat specified
                }
            } else {
                0.0
            }
        }
        _ => {
            // Try custom metric from the metrics vector
            for m in &metrics.metrics {
                if m.key.starts_with(name) {
                    return match stat {
                        Some("avg") => m.mean,
                        Some("min") => m.min as f64,
                        Some("max") => m.max as f64,
                        Some("p50") | Some("median") => m.p50 as f64,
                        Some("p90") => m.p90 as f64,
                        Some("p95") => m.p95 as f64,
                        Some("p99") => m.p99 as f64,
                        Some("count") => m.count as f64,
                        _ => m.mean,
                    };
                }
            }
            0.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::{MetricSummary, MetricsResult, MetricType};

    fn make_metrics() -> MetricsResult {
        MetricsResult {
            http_reqs: 100,
            errors: 2,
            checks_total: 50,
            checks_passed: 45,
            checks_failed: 5,
            http_req_duration: Some(MetricSummary {
                key: "http_req_duration".into(),
                metric_type: MetricType::Trend,
                count: 100,
                sum: 50000.0,
                mean: 500.0,
                min: 50,
                max: 2000,
                p50: 450,
                p90: 900,
                p95: 1200,
                p99: 1800,
                last: 0.0,
                rate: 0.0,
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_threshold_p95_under() {
        let metrics = make_metrics();
        let result = evaluate_single_threshold("http_req_duration.p95 < 1500", &metrics);
        assert!(result.0, "p95 1200 should be < 1500");
        assert_eq!(result.1, 1200.0);
        assert_eq!(result.2, 1500.0);
    }

    #[test]
    fn test_threshold_p95_over() {
        let metrics = make_metrics();
        let result = evaluate_single_threshold("http_req_duration.p95 < 1000", &metrics);
        assert!(!result.0, "p95 1200 should NOT be < 1000");
        assert_eq!(result.1, 1200.0);
        assert_eq!(result.2, 1000.0);
    }

    #[test]
    fn test_threshold_errors() {
        let metrics = make_metrics();
        let result = evaluate_single_threshold("errors < 10", &metrics);
        assert!(result.0);
        assert_eq!(result.1, 2.0);
    }

    #[test]
    fn test_threshold_http_reqs() {
        let metrics = make_metrics();
        let result = evaluate_single_threshold("http_reqs > 50", &metrics);
        assert!(result.0);
        assert_eq!(result.1, 100.0);
    }

    #[test]
    fn test_threshold_pass_rate() {
        let metrics = make_metrics();
        let result = evaluate_single_threshold("checks.pass_rate > 0.8", &metrics);
        assert!(result.0, "pass rate 0.9 should be > 0.8");
    }

    // ── Tag-scoped threshold tests ──

    fn make_tag_scoped_metrics() -> MetricsResult {
        // Build MetricsResult with per-tag http_req_duration entries
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "http_req_duration{status=200}".into(),
            metric_type: MetricType::Trend,
            count: 80,
            sum: 32000.0,
            mean: 400.0,
            min: 50,
            max: 1500,
            p50: 350,
            p90: 700,
            p95: 900,
            p99: 1400,
            last: 0.0,
            rate: 0.0,
        });
        metrics.metrics.push(MetricSummary {
            key: "http_req_duration{status=500}".into(),
            metric_type: MetricType::Trend,
            count: 10,
            sum: 15000.0,
            mean: 1500.0,
            min: 500,
            max: 3000,
            p50: 1200,
            p90: 2500,
            p95: 2800,
            p99: 3000,
            last: 0.0,
            rate: 0.0,
        });
        metrics
    }

    #[test]
    fn test_tag_scoped_p95_under() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=200}.p95 < 1000
        let result = evaluate_single_threshold("http_req_duration{status=200}.p95 < 1000", &metrics);
        assert!(result.0, "p95 900 should be < 1000");
        assert_eq!(result.1, 900.0);
        assert_eq!(result.2, 1000.0);
    }

    #[test]
    fn test_tag_scoped_p95_over() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=500}.p95 < 2000
        let result = evaluate_single_threshold("http_req_duration{status=500}.p95 < 2000", &metrics);
        assert!(!result.0, "p95 2800 should NOT be < 2000");
        assert_eq!(result.1, 2800.0);
        assert_eq!(result.2, 2000.0);
    }

    #[test]
    fn test_tag_scoped_mean() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=200}.avg < 500
        let result = evaluate_single_threshold("http_req_duration{status=200}.avg < 500", &metrics);
        assert!(result.0, "mean 400 should be < 500");
        assert!((result.1 - 400.0).abs() < 0.01);
    }

    #[test]
    fn test_tag_scoped_no_stat_defaults_to_mean() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=200} < 500 — no stat, defaults to mean
        let result = evaluate_single_threshold("http_req_duration{status=200} < 500", &metrics);
        assert!(result.0, "mean 400 should be < 500");
    }

    #[test]
    fn test_tag_scoped_colon_syntax() {
        let metrics = make_tag_scoped_metrics();
        // Use colon syntax: {status:200}
        let result = evaluate_single_threshold("http_req_duration{status:200}.p95 < 1000", &metrics);
        assert!(result.0, "colon syntax should work");
        assert_eq!(result.1, 900.0);
    }

    #[test]
    fn test_tag_scoped_nonexistent_tag() {
        let metrics = make_tag_scoped_metrics();
        // Tag that doesn't exist in the metrics — should return 0.0
        let result = evaluate_single_threshold("http_req_duration{status=404}.p95 < 100", &metrics);
        assert!(result.0, "missing tag should return 0.0, which is < 100");
        assert_eq!(result.1, 0.0);
    }

    #[test]
    fn test_parse_metric_ref_no_tags() {
        let (name, tags, stat) = parse_metric_ref("http_req_duration.p95");
        assert_eq!(name, "http_req_duration");
        assert!(tags.is_empty());
        assert_eq!(stat, Some("p95"));
    }

    #[test]
    fn test_parse_metric_ref_with_tags() {
        let (name, tags, stat) = parse_metric_ref("http_req_duration{status=200}.p95");
        assert_eq!(name, "http_req_duration");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0], ("status", "200"));
        assert_eq!(stat, Some("p95"));
    }

    #[test]
    fn test_parse_metric_ref_colon_tags() {
        let (name, tags, stat) = parse_metric_ref("http_req_duration{status:200}.p95");
        assert_eq!(name, "http_req_duration");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0], ("status", "200"));
        assert_eq!(stat, Some("p95"));
    }

    #[test]
    fn test_parse_metric_ref_no_stat() {
        let (name, tags, stat) = parse_metric_ref("http_reqs");
        assert_eq!(name, "http_reqs");
        assert!(tags.is_empty());
        assert_eq!(stat, None);
    }

    #[test]
    fn test_parse_metric_ref_stat_only() {
        let (name, tags, stat) = parse_metric_ref("checks.pass_rate");
        assert_eq!(name, "checks");
        assert!(tags.is_empty());
        assert_eq!(stat, Some("pass_rate"));
    }
}

