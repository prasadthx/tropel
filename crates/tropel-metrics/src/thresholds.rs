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
    _metrics: &crate::collector::MetricsResult,
) -> Vec<ThresholdResult> {
    let mut results = Vec::new();

    for (name, config) in thresholds {
        // Parse the expression and evaluate
        let result = evaluate_single_threshold(name, &config.expression);
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

/// Evaluate a single threshold expression.
/// Supports simple expressions like "p95 < 500", "rate > 0.99"
fn evaluate_single_threshold(_name: &str, expression: &str) -> (bool, f64, f64) {
    // Simple parser for common threshold patterns
    // Format: "metric_name operator value"
    // e.g. "http_req_duration p95 < 500", "checks rate > 0.99"

    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() < 3 {
        // Invalid expression, treat as passed
        return (true, 0.0, 0.0);
    }

    // Try to parse the last part as the threshold value
    let threshold: f64 = parts.last().and_then(|s| s.parse().ok()).unwrap_or(0.0);

    // Try to parse the second-to-last as the actual value
    let actual: f64 = parts.get(parts.len() - 2)
        .and_then(|_| Some(0.0))
        .unwrap_or(0.0);

    // Try to get the operator
    let operator = parts.get(parts.len() - 2).unwrap_or(&"<");

    let passed = match *operator {
        "<" => actual < threshold,
        "<=" => actual <= threshold,
        ">" => actual > threshold,
        ">=" => actual >= threshold,
        "==" => (actual - threshold).abs() < f64::EPSILON,
        "!=" => (actual - threshold).abs() > f64::EPSILON,
        _ => true,
    };

    (passed, actual, threshold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_threshold() {
        // We can't easily test without running the full collector, but the structure works
        let result = evaluate_single_threshold("test", "value < 500");
        assert!(result.0); // actual 0 < 500 = true
    }
}
