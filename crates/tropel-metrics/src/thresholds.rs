use crate::collector::{
    parse_percentile, percentile_value, MetricSummary, MetricType, MetricsResult,
};
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
            delay_abort_eval: config
                .delay_abort_eval
                .as_ref()
                .and_then(|s| parse_duration(s).ok()),
        });
    }

    // Sort by name so the summary output is deterministic — the input is a
    // HashMap whose iteration order is random per process, which made the
    // rendered Thresholds block (and the insta snapshot of it) flaky.
    results.sort_by(|a, b| a.name.cmp(&b.name));

    results
}

/// Validate every configured threshold expression BEFORE the run starts.
/// Returns an error naming the first malformed expression so the run aborts
/// at startup with a clear message — k6 rejects bad threshold syntax at init
/// rather than silently passing it at the end.
///
/// Rejects: fewer/greater than 3 whitespace tokens per CLAUSE, an unknown
/// operator, or a non-numeric RHS. Backlog line 154: compound `&&`/`||`
/// expressions are now ACCEPTED (k6 supports them) — each clause is validated
/// independently instead of the whole expression being rejected.
pub fn validate_thresholds(thresholds: &HashMap<String, ThresholdConfig>) -> Result<(), String> {
    for (name, config) in thresholds {
        let expr = config.expression.trim();
        // Compound AND/OR: validate every clause the same way a single
        // threshold is validated.
        for clause in compound_clauses(expr) {
            let parts: Vec<&str> = clause.split_whitespace().collect();
            if parts.len() != 3 {
                return Err(format!(
                    "threshold '{}': clause '{}' in '{}' — expected '<metric> <op> <value>' \
                     (3 tokens), got {}",
                    name,
                    clause,
                    expr,
                    parts.len()
                ));
            }
            if !matches!(parts[1], "<" | "<=" | ">" | ">=" | "==" | "!=") {
                return Err(format!(
                    "threshold '{}': unknown operator '{}' in clause '{}' of '{}'",
                    name, parts[1], clause, expr
                ));
            }
            if parts[2].parse::<f64>().is_err() {
                return Err(format!(
                    "threshold '{}': value '{}' is not a number",
                    name, parts[2]
                ));
            }
        }
    }
    Ok(())
}

/// Split a (possibly compound) threshold expression into its individual
/// clauses on `&&` and `||`. A simple expression returns itself.
fn compound_clauses(expression: &str) -> Vec<&str> {
    if !expression.contains("&&") && !expression.contains("||") {
        return vec![expression];
    }
    expression
        .split("||")
        .flat_map(|group| group.split("&&"))
        .map(|c| c.trim())
        .filter(|c| !c.is_empty())
        .collect()
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

        // Use the `_opt` variant: a metric with no samples YET (mid-run)
        // returns None and must NOT abort — data may simply not have
        // arrived. Only a definite numerical breach aborts.
        if let Some((passed, _, _)) = evaluate_single_threshold_opt(&config.expression, metrics) {
            if !passed {
                tracing::error!(
                    "Threshold '{}' ({}) breached with abortOnFail — aborting test",
                    name,
                    config.expression
                );
                return true;
            }
        }
    }
    false
}

/// Parse a duration string like "30s", "1m", "500ms" into a Duration.
fn parse_duration(s: &str) -> std::result::Result<Duration, ()> {
    tropel_core::parse_duration(s).map_err(|_| ())
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
pub(crate) fn parse_metric_ref(metric_ref: &str) -> (&str, Vec<(&str, &str)>, Option<&str>) {
    // Step 1: Find the tag block boundaries
    let (brace_start, brace_close) = {
        let start = metric_ref.find('{');
        let end = start.and_then(|s| metric_ref[s..].find('}').map(|i| s + i));
        (start, end)
    };

    // Step 2: Extract tags from inside `{...}`
    let tags = if let (Some(bs), Some(bc)) = (brace_start, brace_close) {
        metric_ref[bs + 1..bc]
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

    // Find stat: prefer after `}` (more common), fall back to before `{`.
    // Must be the last dot OUTSIDE a `(...)` pair — a decimal percentile
    // like `.p(99.9)` contains a dot inside the parens, and a naive
    // `rfind('.')` would split `http_req_duration.p(99.9)` at the decimal
    // point (name "http_req_duration.p(99", stat "9"), silently degrading
    // p(99.9) thresholds to the mean / a non-existent metric.
    let (name, stat) = if let Some(dot) = rfind_dot_outside_parens(after) {
        // Stat is after `}` — name is the part before `{`
        (before, Some(&after[dot + 1..]))
    } else if let Some(dot) = rfind_dot_outside_parens(before) {
        // Stat is before `{` — strip it from the name
        (&before[..dot], Some(&before[dot + 1..]))
    } else {
        // No stat suffix
        (before, None)
    };

    (name, tags, stat)
}

/// Index of the last `'.'` in `s` that is NOT inside a `(...)` pair, or
/// `None` when every dot is parenthesized. Backs [`parse_metric_ref`]: the
/// stat suffix is `.p(99.9)`, whose decimal point sits inside the parens —
/// a plain `rfind('.')` would treat that decimal as the stat separator and
/// mangle the metric reference.
fn rfind_dot_outside_parens(s: &str) -> Option<usize> {
    let mut depth = 0i32;
    for (i, b) in s.bytes().enumerate().rev() {
        match b {
            b')' => depth += 1,
            b'(' => depth -= 1,
            b'.' if depth <= 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Evaluate a single threshold expression against real metrics.
/// Supports expressions like:
///   "http_req_duration.p95 < 500"
///   "http_req_duration{status=200}.p95 < 500"
///   "http_reqs > 100"
///   "checks.pass_rate > 0.99"
///   "errors < 10"
/// Evaluate a single threshold expression, returning `None` when the metric
/// has NO samples yet (mid-run this must NOT abort — data may simply not have
/// arrived). Parse errors and unknown operators fail closed (see
/// [`validate_thresholds`], which rejects them at startup).
fn evaluate_single_threshold_opt(
    expression: &str,
    metrics: &MetricsResult,
) -> Option<(bool, f64, f64)> {
    // Backlog line 154: compound AND/OR expressions (k6 supports them,
    // e.g. `p(95) < 500 && p(99) < 1000`). `&&` binds tighter than `||`:
    // split on `||` first, every group must contain only AND-passing
    // clauses, and any passing group passes the whole threshold. The
    // reported (actual, threshold) pair comes from the LAST clause that
    // determined the outcome (the group's final clause).
    if expression.contains("&&") || expression.contains("||") {
        let groups: Vec<Vec<&str>> = expression
            .split("||")
            .map(|g| {
                g.split("&&")
                    .map(|c| c.trim())
                    .filter(|c| !c.is_empty())
                    .collect()
            })
            .filter(|g: &Vec<&str>| !g.is_empty())
            .collect();
        let mut last_pair = (0.0f64, 0.0f64);
        let mut any_group_passed = false;
        for group in groups {
            let mut group_passed = true;
            for clause in group {
                let (passed, actual, threshold) = evaluate_single_threshold_opt(clause, metrics)?;
                last_pair = (actual, threshold);
                if !passed {
                    group_passed = false;
                }
            }
            if group_passed {
                any_group_passed = true;
            }
        }
        return Some((any_group_passed, last_pair.0, last_pair.1));
    }

    // Fail CLOSED: any parse error or unknown operator must FAIL the
    // threshold (and, via `validate_thresholds` at startup, abort the run).
    // The old code returned `(true, …)` on malformed input, so a typo'd
    // metric or a bogus operator silently reported green. k6 rejects bad
    // threshold syntax at startup instead.
    let parts: Vec<&str> = expression.split_whitespace().collect();
    if parts.len() != 3 {
        tracing::error!(
            "Invalid threshold expression '{}' — expected '<metric> <op> <value>' (3 tokens), got {}",
            expression,
            parts.len()
        );
        return Some((false, 0.0, 0.0));
    }

    // Format: "metric_ref operator value"
    // metric_ref can be "http_req_duration.p95", "http_req_duration{status=200}.p95",
    // or just "http_reqs"
    let metric_ref = parts[0];
    let operator = parts[1];
    let threshold: f64 = match parts[2].parse() {
        Ok(v) => v,
        Err(_) => {
            tracing::error!(
                "Invalid threshold value in '{}': '{}'",
                expression,
                parts[2]
            );
            return Some((false, 0.0, 0.0));
        }
    };

    // Parse metric reference into (metric_name, tags, stat)
    let (metric_name, tag_filters, stat) = parse_metric_ref(metric_ref);

    // Look up the actual metric value. `None` means the metric has NO
    // samples at all — distinguish that from a real measured 0.0: a missing
    // metric must fail the threshold (k6 marks no-data thresholds as failed),
    // never pass a `<` comparison against an invented 0.0.
    let actual = if !tag_filters.is_empty() {
        // Tag-scoped threshold: search metrics.metrics for matching entries
        get_tag_scoped_metric_value(metrics, metric_name, &tag_filters, stat)
    } else {
        // No tag filter — use the existing top-level lookup
        get_metric_value(metrics, metric_name, stat)
    };

    let actual = actual?; // None → metric has no samples yet — no data.

    let passed = match operator {
        "<" => actual < threshold,
        "<=" => actual <= threshold,
        ">" => actual > threshold,
        ">=" => actual >= threshold,
        "==" => (actual - threshold).abs() < f64::EPSILON,
        "!=" => (actual - threshold).abs() > f64::EPSILON,
        _ => {
            tracing::error!(
                "Unknown operator '{}' in threshold '{}'",
                operator,
                expression
            );
            return Some((false, 0.0, threshold));
        }
    };

    Some((passed, actual, threshold))
}

/// Fail-closed wrapper used by the final summary: no data → FAILED (k6 marks
/// no-data thresholds as failed).
fn evaluate_single_threshold(expression: &str, metrics: &MetricsResult) -> (bool, f64, f64) {
    evaluate_single_threshold_opt(expression, metrics).unwrap_or((false, 0.0, 0.0))
}

/// Extract the base metric name from a series key string, stripping any
/// `{tag=value}` suffix: `"http_req_duration{status=200}"` → `"http_req_duration"`.
/// Exact base-name matching (not prefix) keeps a threshold on `login` from
/// aggregating `login_errors` + `login_duration`.
fn metric_base_name(key: &str) -> &str {
    match key.find('{') {
        Some(i) => &key[..i],
        None => key,
    }
}

/// Get a metric value for a tag-scoped threshold by searching the metrics list.
/// Looks for entries whose BASE name equals the metric name and that contain
/// all the specified tag key=value pairs.
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
/// If no entry matches, returns `None` (no data for this tag set → the
/// evaluator fails the threshold closed, matching k6's "no data" behavior
/// instead of inventing a passing 0.0).
fn get_tag_scoped_metric_value(
    metrics: &MetricsResult,
    metric_name: &str,
    tag_filters: &[(&str, &str)],
    stat: Option<&str>,
) -> Option<f64> {
    let mut matched = Vec::new();

    for m in &metrics.metrics {
        // Exact base-name match: `login` must not aggregate `login_errors`.
        if metric_base_name(&m.key) != metric_name {
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
        return None;
    }

    // Aggregate all matching entries
    Some(match stat {
        Some("avg") => {
            // Return the WORST (highest) mean across all matches
            matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max)
        }
        Some("min") => {
            // Return the MINIMUM min across all matches
            matched
                .iter()
                .map(|m| m.min as f64)
                .fold(f64::MAX, f64::min)
        }
        Some("max") => {
            // Return the MAXIMUM max across all matches
            matched.iter().map(|m| m.max as f64).fold(0.0_f64, f64::max)
        }
        Some("p50") | Some("median") | Some("med") => {
            matched.iter().map(|m| m.p50 as f64).fold(0.0_f64, f64::max)
        }
        Some("p90") => matched.iter().map(|m| m.p90 as f64).fold(0.0_f64, f64::max),
        Some("p95") => matched.iter().map(|m| m.p95 as f64).fold(0.0_f64, f64::max),
        Some("p99") => matched.iter().map(|m| m.p99 as f64).fold(0.0_f64, f64::max),
        // Any other pNN / p(NN) percentile — exact from the retained
        // histogram of each matching series; worst (highest) wins across
        // matches (consistent with the tracked buckets above).
        Some(s) if parse_percentile(s).is_some() => {
            let pct = parse_percentile(s).expect("guarded");
            matched
                .iter()
                .map(|m| percentile_value(m, pct))
                .fold(0.0_f64, f64::max)
        }
        Some("count") => matched.iter().map(|m| m.count as f64).sum(),
        Some("rate") => {
            let total_sum: f64 = matched.iter().map(|m| m.sum).sum();
            let total_count: f64 = matched.iter().map(|m| m.count as f64).sum();
            if total_count > 0.0 {
                total_sum / total_count
            } else {
                0.0
            }
        }
        Some("sum") => matched.iter().map(|m| m.sum).sum(),
        Some("last") => matched.last().map(|m| m.last).unwrap_or(0.0),
        // Default (no stat or unknown stat) — return WORST mean
        _ => matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max),
    })
}

/// Aggregate a statistic across ALL series in `metrics.metrics` whose BASE
/// name equals `name` (k6 merges tagged sub-series for the unscoped metric).
/// Exact match — never prefix: a threshold on `login` must not aggregate
/// `login_errors` / `login_duration`.
/// Returns `None` when no series match, so callers can fall back to a
/// top-level field or 0.0.
fn aggregate_series(metrics: &MetricsResult, name: &str, stat: Option<&str>) -> Option<f64> {
    let matched: Vec<&MetricSummary> = metrics
        .metrics
        .iter()
        .filter(|m| metric_base_name(&m.key) == name)
        .collect();
    if matched.is_empty() {
        return None;
    }
    Some(match stat {
        // Percentiles: worst (highest) across matched series, mirroring
        // the tag-scoped path.
        Some("min") => matched
            .iter()
            .map(|m| m.min as f64)
            .fold(f64::MAX, f64::min),
        Some("max") => matched.iter().map(|m| m.max as f64).fold(0.0_f64, f64::max),
        Some("p50") | Some("median") | Some("med") => {
            matched.iter().map(|m| m.p50 as f64).fold(0.0_f64, f64::max)
        }
        Some("p90") => matched.iter().map(|m| m.p90 as f64).fold(0.0_f64, f64::max),
        Some("p95") => matched.iter().map(|m| m.p95 as f64).fold(0.0_f64, f64::max),
        Some("p99") => matched.iter().map(|m| m.p99 as f64).fold(0.0_f64, f64::max),
        Some(s) if parse_percentile(s).is_some() => {
            let pct = parse_percentile(s).expect("guarded");
            matched
                .iter()
                .map(|m| percentile_value(m, pct))
                .fold(0.0_f64, f64::max)
        }
        // Rate = total sum / total count across ALL series (k6 merges tagged
        // sub-series for the unscoped metric).
        Some("rate") => {
            if matched.iter().all(|m| m.metric_type == MetricType::Counter) {
                // Counter `count` IS the accumulated value (k6 semantics), so
                // sum/count would degenerate to 1.0. There is no per-event
                // rate for a merged counter — fall back to the per-series
                // mean (worst across matches, mirroring the default arm).
                matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max)
            } else {
                let total_sum: f64 = matched.iter().map(|m| m.sum).sum();
                let total_count: f64 = matched.iter().map(|m| m.count as f64).sum();
                if total_count > 0.0 {
                    total_sum / total_count
                } else {
                    0.0
                }
            }
        }
        Some("count") => matched.iter().map(|m| m.count as f64).sum(),
        Some("sum") => matched.iter().map(|m| m.sum).sum(),
        Some("avg") => {
            if matched.iter().all(|m| m.metric_type == MetricType::Counter) {
                // Counter: count == accumulated value, so sum/count is always
                // 1.0 — use the preserved per-series mean instead.
                matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max)
            } else {
                let total_sum: f64 = matched.iter().map(|m| m.sum).sum();
                let total_count: f64 = matched.iter().map(|m| m.count as f64).sum();
                if total_count > 0.0 {
                    total_sum / total_count
                } else {
                    0.0
                }
            }
        }
        // Default (no stat or unknown stat) — worst mean across matches.
        _ => matched.iter().map(|m| m.mean).fold(0.0_f64, f64::max),
    })
}

/// Extract a metric value from the MetricsResult by name and optional
/// statistic. Returns `None` when the metric has NO samples at all, so the
/// evaluator can distinguish "no data" (fails closed, like k6) from a real
/// measured 0.0.
fn get_metric_value(metrics: &MetricsResult, name: &str, stat: Option<&str>) -> Option<f64> {
    match name {
        "http_reqs" => Some(metrics.http_reqs as f64),
        "errors" => {
            // Per-tag series (errors{url=…}) may exist; aggregate per stat
            // across them (k6 merges tagged sub-series for the unscoped
            // metric). Only when a stat is present though — for a bare
            // `errors < N` the top-level counter IS the merged total, while
            // the helper's default arm would return the worst per-series
            // mean (1.0 for value-1 Counter samples) and pass spuriously.
            if let Some(v) = stat.and_then(|_| aggregate_series(metrics, "errors", stat)) {
                Some(v)
            } else {
                Some(metrics.errors as f64)
            }
        }
        // NOTE: `parse_metric_ref` strips the stat at the last dot, so a
        // `checks.rate` / `checks.pass_rate` reference arrives here as
        // name="checks", stat=Some("rate"/"pass_rate") — the dotted-name
        // arms below would be unreachable. Match on (name, stat) instead of
        // on a dot-stripped name, otherwise `checks: ['rate>0.99']` (the
        // stock k6 gate) compares the check COUNT against the rate and
        // always passes.
        "checks" | "checks.total" => Some(match stat {
            Some("passed") => metrics.checks_passed as f64,
            Some("failed") => metrics.checks_failed as f64,
            Some("rate") | Some("pass_rate") => {
                if metrics.checks_total > 0 {
                    metrics.checks_passed as f64 / metrics.checks_total as f64
                } else {
                    0.0
                }
            }
            _ => metrics.checks_total as f64,
        }),
        "http_req_duration" => metrics.http_req_duration.as_ref().map(|d| {
            match stat {
                Some("avg") => d.mean,
                Some("min") => d.min as f64,
                Some("max") => d.max as f64,
                Some("p50") | Some("median") | Some("med") => d.p50 as f64,
                Some("p90") => d.p90 as f64,
                Some("p95") => d.p95 as f64,
                Some("p99") => d.p99 as f64,
                Some("count") => d.count as f64,
                // Rate = sum/count (mirrors the custom-metric loop).
                Some("rate") => {
                    if d.count > 0 {
                        d.sum / d.count as f64
                    } else {
                        0.0
                    }
                }
                // Any other pNN / p(NN) percentile — exact from the
                // retained histogram (not the mean, not a bucket guess).
                Some(s) if parse_percentile(s).is_some() => {
                    percentile_value(d, parse_percentile(s).expect("guarded"))
                }
                _ => d.mean, // default to mean if no stat specified
            }
        }),
        _ => {
            // Custom metric (e.g. http_req_failed, user metrics): aggregate
            // across ALL series whose key starts with the name. The naive
            // first-match returned an arbitrary tagged series (e.g.
            // http_req_failed{url=…} picked one URL's rate — 1.00 when that
            // series was all-failed) instead of the merged value k6 reports
            // for the unscoped metric. `None` = no series at all → no data.
            aggregate_series(metrics, name, stat)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collector::{MetricSummary, MetricType, MetricsResult};

    fn make_metrics() -> MetricsResult {
        MetricsResult {
            http_reqs: 100,
            errors: 2,
            checks_total: 50,
            checks_passed: 45,
            checks_failed: 5,
            http_req_duration: Some(MetricSummary {
                key: "http_req_duration".into(),
                tags: vec![],
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
                histogram: None,
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

    // ── compound && / || (backlog line 154) ──

    #[test]
    fn compound_and_requires_all_clauses() {
        // p95=1200 < 1500 AND p90=900 < 1000 → both pass.
        let metrics = make_metrics();
        let result = evaluate_single_threshold(
            "http_req_duration.p95 < 1500 && http_req_duration.p90 < 1000",
            &metrics,
        );
        assert!(result.0, "&& passes when ALL clauses pass");
        assert_eq!(result.1, 900.0, "reports the last clause's actual");
        assert_eq!(result.2, 1000.0, "reports the last clause's threshold");

        // p95=1200 < 1500 (true) AND p90=900 < 500 (false) → fails.
        let result = evaluate_single_threshold(
            "http_req_duration.p95 < 1500 && http_req_duration.p90 < 500",
            &metrics,
        );
        assert!(!result.0, "&& requires ALL clauses");
    }

    #[test]
    fn compound_or_passes_on_any_clause() {
        // p95=1200 > 1500 (false) OR p90=900 < 1000 (true) → passes.
        let metrics = make_metrics();
        let result = evaluate_single_threshold(
            "http_req_duration.p95 > 1500 || http_req_duration.p90 < 1000",
            &metrics,
        );
        assert!(result.0, "|| passes if ANY clause passes");

        // Both false → fails.
        let result = evaluate_single_threshold(
            "http_req_duration.p95 > 1500 || http_req_duration.p90 > 1000",
            &metrics,
        );
        assert!(!result.0, "|| fails when ALL clauses fail");
    }

    #[test]
    fn compound_and_binds_tighter_than_or() {
        // `a && b || c` — && binds tighter: (a AND b) OR c. With make_metrics
        // p95=1200, p90=900, p99=1800: a = (p95>1500)=false, b = (p90<1000)=true,
        // so the && group is false; c = (p99>1500)=true saves the || group.
        let metrics = make_metrics();
        let result = evaluate_single_threshold(
            "http_req_duration.p95 > 1500 && http_req_duration.p90 < 1000 || \
             http_req_duration.p99 > 1500",
            &metrics,
        );
        assert!(result.0, "c saves the OR group: {result:?}");

        // c=false too → fails.
        let result = evaluate_single_threshold(
            "http_req_duration.p95 > 1500 && http_req_duration.p90 < 1000 || \
             http_req_duration.p99 < 1500",
            &metrics,
        );
        assert!(!result.0, "&& group fails and || group fails: {result:?}");
    }

    #[test]
    fn validate_accepts_compound_rejects_bad_clause() {
        let mut ok = HashMap::new();
        ok.insert(
            "compound".to_string(),
            ThresholdConfig {
                expression: "p(95) < 500 && p(99) < 1000".into(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
        assert!(validate_thresholds(&ok).is_ok(), "&& thresholds validate");

        let mut bad = HashMap::new();
        bad.insert(
            "bad".to_string(),
            ThresholdConfig {
                expression: "p(95) < 500 && p(99) < not-a-number".into(),
                abort_on_fail: false,
                delay_abort_eval: None,
            },
        );
        let err = validate_thresholds(&bad).unwrap_err();
        assert!(
            err.contains("not-a-number"),
            "bad clause names the offending value: {err}"
        );
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

    // ── Arbitrary-percentile tests ──

    /// Build a MetricsResult whose http_req_duration carries a real retained
    /// histogram with values 100..=1000 µs (10 samples, mean 550 µs).
    /// p75 ≈ 775-800 µs — far above the mean — so a `.p75 < 600` threshold
    /// MUST fail. Before the histogram was retained, any non-{p50,p90,p95,p99}
    /// stat silently fell back to the mean (550) → false PASS.
    fn make_histogram_metrics() -> MetricsResult {
        use crate::histogram::LatencyHistogram;
        let mut h = LatencyHistogram::new();
        for i in 1..=10u64 {
            h.record_micros(i * 100);
        }
        MetricsResult {
            http_req_duration: Some(MetricSummary {
                key: "http_req_duration".into(),
                tags: vec![],
                metric_type: MetricType::Trend,
                count: 10,
                sum: 5500.0,
                mean: 550.0,
                min: 100,
                max: 1000,
                p50: 500,
                p90: 900,
                p95: 950,
                p99: 990,
                last: 0.0,
                rate: 0.0,
                histogram: Some(h),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_arbitrary_percentile_exact_not_mean() {
        let metrics = make_histogram_metrics();
        // p75 of 100..1000 (10 values) is ~775-800 µs. The mean is 550.
        let result = evaluate_single_threshold("http_req_duration.p75 < 600", &metrics);
        assert!(
            !result.0,
            "p75 must be > 600 (mean fallback would false-PASS)"
        );
        assert!(
            result.1 > 600.0 && result.1 < 850.0,
            "p75 should be an exact histogram percentile, got {}",
            result.1
        );
        assert_ne!(result.1, 550.0, "must not fall back to the mean");
    }

    #[test]
    fn test_arbitrary_percentile_paren_syntax() {
        let metrics = make_histogram_metrics();
        // k6-style p(90) syntax
        let result = evaluate_single_threshold("http_req_duration.p(90) < 2000", &metrics);
        assert!(result.0, "p90 of 100..1000 is ~900, should be < 2000");
        assert!(
            result.1 >= 850.0 && result.1 <= 1000.0,
            "p(90) should be an exact histogram percentile, got {}",
            result.1
        );
    }

    #[test]
    fn test_decimal_percentile_exact_from_histogram() {
        // Backlog line 135 regression: p(99.9) must resolve EXACTLY from the
        // retained histogram, never the mean. parse_metric_ref splits the
        // stat at the last dot OUTSIDE parens — a naive rfind('.') split
        // `.p(99.9)` at the decimal point (stat "9", name "…p(99"), which
        // would fall back to the mean / a non-existent metric.
        let metrics = make_histogram_metrics();
        let result = evaluate_single_threshold("http_req_duration.p(99.9) < 2000", &metrics);
        assert!(result.0, "p99.9 of 100..1000 is ~1000, should be < 2000");
        assert!(
            result.1 > 950.0,
            "p(99.9) must be an exact high percentile (~1000), got {}",
            result.1
        );
        assert_ne!(result.1, 550.0, "must not fall back to the mean");

        // Tag-scoped decimal percentile resolves through the after-`}` path.
        use crate::histogram::LatencyHistogram;
        let mut h = LatencyHistogram::new();
        for i in 1..=10u64 {
            h.record_micros(i * 100);
        }
        let mut m = MetricsResult::default();
        m.metrics.push(MetricSummary {
            key: "http_req_duration{status=200}".into(),
            tags: vec![("status".into(), "200".into())],
            metric_type: MetricType::Trend,
            count: 10,
            sum: 5500.0,
            mean: 550.0,
            min: 100,
            max: 1000,
            p50: 500,
            p90: 900,
            p95: 950,
            p99: 990,
            last: 0.0,
            rate: 0.0,
            histogram: Some(h),
        });
        let result = evaluate_single_threshold("http_req_duration{status=200}.p(99.9) < 2000", &m);
        assert!(result.0);
        assert!(
            result.1 > 950.0,
            "tag-scoped p(99.9) must be exact, got {}",
            result.1
        );
    }

    #[test]
    fn test_arbitrary_percentile_tag_scoped() {
        // Build a tag-scoped series carrying a histogram, then assert the
        // tag-scoped path also resolves non-standard percentiles exactly.
        use crate::histogram::LatencyHistogram;
        let mut h = LatencyHistogram::new();
        for i in 1..=10u64 {
            h.record_micros(i * 100);
        }
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "http_req_duration{status=200}".into(),
            tags: vec![("status".into(), "200".into())],
            metric_type: MetricType::Trend,
            count: 10,
            sum: 5500.0,
            mean: 550.0,
            min: 100,
            max: 1000,
            p50: 500,
            p90: 900,
            p95: 950,
            p99: 990,
            last: 0.0,
            rate: 0.0,
            histogram: Some(h),
        });
        let result = evaluate_single_threshold("http_req_duration{status=200}.p75 < 600", &metrics);
        assert!(!result.0, "tag-scoped p75 must also be exact, not the mean");
        assert_ne!(result.1, 550.0);
    }

    // ── Tag-scoped threshold tests ──

    fn make_tag_scoped_metrics() -> MetricsResult {
        // Build MetricsResult with per-tag http_req_duration entries
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "http_req_duration{status=200}".into(),
            tags: vec![("status".into(), "200".into())],
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
            histogram: None,
        });
        metrics.metrics.push(MetricSummary {
            key: "http_req_duration{status=500}".into(),
            tags: vec![("status".into(), "500".into())],
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
            histogram: None,
        });
        metrics
    }

    #[test]
    fn test_tag_scoped_p95_under() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=200}.p95 < 1000
        let result =
            evaluate_single_threshold("http_req_duration{status=200}.p95 < 1000", &metrics);
        assert!(result.0, "p95 900 should be < 1000");
        assert_eq!(result.1, 900.0);
        assert_eq!(result.2, 1000.0);
    }

    #[test]
    fn test_tag_scoped_p95_over() {
        let metrics = make_tag_scoped_metrics();
        // http_req_duration{status=500}.p95 < 2000
        let result =
            evaluate_single_threshold("http_req_duration{status=500}.p95 < 2000", &metrics);
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
        let result =
            evaluate_single_threshold("http_req_duration{status:200}.p95 < 1000", &metrics);
        assert!(result.0, "colon syntax should work");
        assert_eq!(result.1, 900.0);
    }

    #[test]
    fn test_tag_scoped_nonexistent_tag() {
        let metrics = make_tag_scoped_metrics();
        // Tag that doesn't exist in the metrics = no samples for that series.
        // Fail CLOSED: a missing series must FAIL the threshold (k6 marks
        // no-data thresholds as failed), never pass a `<` against an invented
        // 0.0 (the old behavior silently reported green).
        let result = evaluate_single_threshold("http_req_duration{status=404}.p95 < 100", &metrics);
        assert!(!result.0, "missing tag must fail closed, not pass with 0.0");
        assert_eq!(result.1, 0.0);
    }

    #[test]
    fn test_unscoped_custom_metric_aggregates_all_series() {
        // http_req_failed is emitted as one series per (url,status) tag set;
        // the unscoped threshold must merge ALL of them (k6 semantics), not
        // return one arbitrary series' rate.
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "http_req_failed{url=a}".into(),
            tags: vec![("url".into(), "a".into())],
            metric_type: MetricType::Rate,
            count: 100,
            sum: 20.0, // 20% failed
            mean: 0.2,
            min: 0,
            max: 1,
            p50: 0,
            p90: 1,
            p95: 1,
            p99: 1,
            last: 0.0,
            rate: 0.2,
            histogram: None,
        });
        metrics.metrics.push(MetricSummary {
            key: "http_req_failed{url=b}".into(),
            tags: vec![("url".into(), "b".into())],
            metric_type: MetricType::Rate,
            count: 100,
            sum: 100.0, // 100% failed
            mean: 1.0,
            min: 0,
            max: 1,
            p50: 1,
            p90: 1,
            p95: 1,
            p99: 1,
            last: 0.0,
            rate: 1.0,
            histogram: None,
        });
        // Merged: 120 failures / 200 samples = 0.60 — not the 1.00 that a
        // first-match lookup would have returned for series b.
        let (passed, actual, _) = evaluate_single_threshold("http_req_failed.rate < 0.5", &metrics);
        assert!(!passed, "merged rate 0.60 must not pass < 0.5");
        assert!(
            (actual - 0.6).abs() < 1e-9,
            "merged rate should be 0.6, got {actual}"
        );

        let (passed, _, _) = evaluate_single_threshold("http_req_failed.rate < 0.7", &metrics);
        assert!(passed, "merged rate 0.60 should pass < 0.7");
    }

    #[test]
    fn test_unscoped_count_sums_all_series() {
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "errors{url=a}".into(),
            tags: vec![("url".into(), "a".into())],
            metric_type: MetricType::Counter,
            count: 7,
            sum: 7.0,
            mean: 1.0,
            min: 0,
            max: 1,
            p50: 0,
            p90: 1,
            p95: 1,
            p99: 1,
            last: 0.0,
            rate: 0.0,
            histogram: None,
        });
        metrics.metrics.push(MetricSummary {
            key: "errors{url=b}".into(),
            tags: vec![("url".into(), "b".into())],
            metric_type: MetricType::Counter,
            count: 3,
            sum: 3.0,
            mean: 1.0,
            min: 0,
            max: 1,
            p50: 0,
            p90: 1,
            p95: 1,
            p99: 1,
            last: 0.0,
            rate: 0.0,
            histogram: None,
        });
        let (_, actual, _) = evaluate_single_threshold("errors.count > 5", &metrics);
        assert_eq!(actual, 10.0, "count must sum across all series");
    }

    #[test]
    fn test_threshold_exact_name_not_prefix_aggregated() {
        // Regression (backlog line 80): the unscoped series lookup used
        // `starts_with`, so a threshold on `login` aggregated `login_errors`
        // and `login_duration` too. Series must match the base name exactly.
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "login".into(),
            tags: vec![],
            metric_type: MetricType::Trend,
            count: 5,
            sum: 1000.0,
            mean: 200.0,
            min: 50,
            max: 400,
            p50: 200,
            p90: 300,
            p95: 350,
            p99: 390,
            last: 0.0,
            rate: 0.0,
            histogram: None,
        });
        metrics.metrics.push(MetricSummary {
            key: "login_errors".into(),
            tags: vec![],
            metric_type: MetricType::Trend,
            count: 500, // high count — would swamp `login` if prefix-folded
            sum: 1.0,
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
        });
        metrics.metrics.push(MetricSummary {
            key: "login_duration".into(),
            tags: vec![],
            metric_type: MetricType::Trend,
            count: 500,
            sum: 1.0,
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
        });

        // `login.count < 10` must evaluate against ONLY the `login` series
        // (count 5). Prefix folding would have summed all three → 1005 → fail.
        let (passed, actual, _) = evaluate_single_threshold("login.count < 10", &metrics);
        assert!(
            passed,
            "login.count must be 5 (only the exact series), not 1005"
        );
        assert_eq!(actual, 5.0);

        // A threshold on login_errors itself still resolves its own series.
        let (passed, actual, _) = evaluate_single_threshold("login_errors.count < 1000", &metrics);
        assert!(passed);
        assert_eq!(actual, 500.0);
    }

    #[test]
    fn test_tag_scoped_exact_name_not_prefix() {
        // Tag-scoped lookup had the same starts_with bug: `{status=200}` on
        // `login` must not match `login_errors{status=200}`.
        let mut metrics = MetricsResult::default();
        metrics.metrics.push(MetricSummary {
            key: "login{status=200}".into(),
            tags: vec![("status".into(), "200".into())],
            metric_type: MetricType::Trend,
            count: 2,
            sum: 400.0,
            mean: 200.0,
            min: 100,
            max: 300,
            p50: 200,
            p90: 250,
            p95: 280,
            p99: 290,
            last: 0.0,
            rate: 0.0,
            histogram: None,
        });
        metrics.metrics.push(MetricSummary {
            key: "login_errors{status=200}".into(),
            tags: vec![("status".into(), "200".into())],
            metric_type: MetricType::Trend,
            count: 900,
            sum: 1.0,
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
        });

        let (passed, actual, _) =
            evaluate_single_threshold("login{status=200}.count < 10", &metrics);
        assert!(
            passed,
            "must match only the login{{status=200}} series (count 2)"
        );
        assert_eq!(actual, 2.0);
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

    // ── check_abort_on_fail (backlog §6 P1: zero coverage before) ──

    fn abort_config(expression: &str, abort_on_fail: bool, delay: Option<&str>) -> ThresholdConfig {
        ThresholdConfig {
            expression: expression.to_string(),
            abort_on_fail,
            delay_abort_eval: delay.map(|s| s.to_string()),
        }
    }

    #[test]
    fn abort_fires_on_breach_with_abort_on_fail() {
        // p95 = 1200, threshold 1000 → breached, abort_on_fail → true.
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "dur".to_string(),
            abort_config("http_req_duration.p95 < 1000", true, None),
        );
        assert!(check_abort_on_fail(
            &thresholds,
            &make_metrics(),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn abort_ignores_non_abort_thresholds() {
        // Breached but abort_on_fail = false → must NOT abort (the run
        // continues and reports the failure at the end).
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "dur".to_string(),
            abort_config("http_req_duration.p95 < 1000", false, None),
        );
        assert!(!check_abort_on_fail(
            &thresholds,
            &make_metrics(),
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn abort_respects_delay_abort_eval_grace_period() {
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "dur".to_string(),
            abort_config("http_req_duration.p95 < 1000", true, Some("30s")),
        );
        // Inside the 30s grace period: breach must NOT abort yet.
        assert!(!check_abort_on_fail(
            &thresholds,
            &make_metrics(),
            Duration::from_secs(10)
        ));
        // After the grace period: breach aborts.
        assert!(check_abort_on_fail(
            &thresholds,
            &make_metrics(),
            Duration::from_secs(31)
        ));
    }

    #[test]
    fn abort_does_not_fire_when_metric_has_no_samples_yet() {
        // Mid-run, the metric series may not have arrived yet (the `_opt`
        // variant returns None). A threshold on it must NOT abort — data
        // may simply not have been recorded in the first instant.
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "dur".to_string(),
            abort_config("http_req_duration.p95 < 1000", true, None),
        );
        let empty = MetricsResult::default(); // no http_req_duration series
        assert!(!check_abort_on_fail(
            &thresholds,
            &empty,
            Duration::from_secs(5)
        ));
    }

    #[test]
    fn abort_only_fires_for_actually_breached_thresholds() {
        // p95 = 1200 < 1500 → passes → no abort even with abort_on_fail.
        let mut thresholds = HashMap::new();
        thresholds.insert(
            "dur".to_string(),
            abort_config("http_req_duration.p95 < 1500", true, None),
        );
        assert!(!check_abort_on_fail(
            &thresholds,
            &make_metrics(),
            Duration::from_secs(5)
        ));
    }
}
