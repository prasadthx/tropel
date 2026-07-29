use crate::collector::MetricsResult;
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
}

/// Evaluate thresholds against aggregated metrics.
pub fn evaluate_thresholds(
    thresholds: &std::collections::HashMap<String, ThresholdConfig>,
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
        });
    }

    results
}

/// Evaluate a single threshold expression against real metrics.
/// Supports expressions like:
///   "http_req_duration.p95 < 500"
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
    // metric_ref can be "http_req_duration.p95" or just "http_reqs"
    let metric_ref = parts[0];
    let operator = parts[1];
    let threshold: f64 = match parts[2].parse() {
        Ok(v) => v,
        Err(_) => {
            tracing::warn!("Invalid threshold value in '{}': '{}'", expression, parts[2]);
            return (true, 0.0, 0.0);
        }
    };

    // Extract metric name and optional statistic
    let (metric_name, stat) = if let Some(dot) = metric_ref.rfind('.') {
        let name = &metric_ref[..dot];
        let s = &metric_ref[dot+1..];
        (name, Some(s))
    } else {
        (metric_ref, None)
    };

    // Look up the actual metric value
    let actual = get_metric_value(metrics, metric_name, stat);

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
    use crate::collector::{MetricSummary, MetricsResult};

    fn make_metrics() -> MetricsResult {
        MetricsResult {
            http_reqs: 100,
            errors: 2,
            checks_total: 50,
            checks_passed: 45,
            checks_failed: 5,
            http_req_duration: Some(MetricSummary {
                key: "http_req_duration".into(),
                count: 100,
                sum: 50000.0,
                mean: 500.0,
                min: 50,
                max: 2000,
                p50: 450,
                p90: 900,
                p95: 1200,
                p99: 1800,
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
}

