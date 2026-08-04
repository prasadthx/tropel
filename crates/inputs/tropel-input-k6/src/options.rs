//! # k6 options model
//!
//! Serde models for k6's `export const options = { … }` object, plus
//! conversion into Tropel's `ExecutionConfig` / `ScenarioConfig` /
//! `ThresholdConfig` types.
//!
//! k6 field names are camelCase (`gracefulStop`, `preAllocatedVUs`, …), so
//! every k6 name carries a `#[serde(alias)]`. The structs are intentionally
//! lenient (`#[serde(default)]`, all `Option`) — a k6 script that declares
//! only a subset of the fields must still parse.

use regex::Regex;
use serde::Deserialize;
use std::collections::HashMap;
use tropel_sdk::{
    ArrivalRateStage, DriverDeclaredOptions, ExecutionConfig, ScenarioConfig, Stage,
    ThinkTimeConfig, ThresholdConfig,
};

/// k6 `export const options = { … }` — top level.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct K6Options {
    /// Number of VUs (constant-vus / start of ramping-vus).
    pub vus: Option<u32>,
    /// Test duration (e.g. `"30s"`, `"5m"`).
    pub duration: Option<String>,
    /// Total iteration count (shared-iterations).
    pub iterations: Option<u64>,
    /// Ramping stages (`[{ duration, target }]`) → ramping-vus.
    pub stages: Option<Vec<K6Stage>>,
    /// Named scenarios — each has its own executor.
    pub scenarios: Option<HashMap<String, K6Scenario>>,
    /// Thresholds keyed by metric name.
    pub thresholds: Option<HashMap<String, K6ThresholdSpec>>,
    #[serde(alias = "gracefulStop")]
    pub graceful_stop: Option<String>,
    #[serde(alias = "gracefulRampDown")]
    pub graceful_ramp_down: Option<String>,
    #[serde(alias = "maxDuration")]
    pub max_duration: Option<String>,
    /// Global body-handling: when true, response bodies are discarded for ALL
    /// requests (k6 `options.discardResponseBodies`). Overrides per-request
    /// `responseType` defaults; pairs with the lazy-body work.
    #[serde(alias = "discardResponseBodies")]
    pub discard_response_bodies: Option<bool>,
    /// Which trend statistics the summary shows (k6 `options.summaryTrendStats`,
    /// e.g. `["avg","min","med","max","p(90)","p(95)","p(99)"]`). When
    /// absent, the k6 default set is used.
    #[serde(alias = "summaryTrendStats")]
    pub summary_trend_stats: Option<Vec<String>>,
    /// DNS configuration (k6 `options.dns`): `{ ttl, select, policy }`.
    #[serde(default)]
    pub dns: Option<K6Dns>,
    /// Close the connection after every request (k6 `noConnectionReuse`).
    #[serde(alias = "noConnectionReuse")]
    pub no_connection_reuse: Option<bool>,
    /// k6 `noVUConnectionReuse` — accepted for compatibility; Tropel already
    /// gives each VU its own client/pool, so it is effectively always on.
    #[serde(alias = "noVUConnectionReuse")]
    pub no_vu_connection_reuse: Option<bool>,
    /// Global request-rate cap in requests/second (k6 `rps`).
    #[serde(default)]
    pub rps: Option<f64>,
    /// Static hostname → IP mapping (k6 `hosts`).
    #[serde(default)]
    pub hosts: Option<HashMap<String, String>>,
    /// Blocked IPs / CIDRs (k6 `blacklistIPs`).
    #[serde(alias = "blacklistIPs")]
    pub blacklist_ips: Option<Vec<String>>,
}

/// k6 `options.dns` — DNS cache TTL, address selection and family policy.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct K6Dns {
    /// Cache TTL: `"5m"`, `"inf"`, `"0"`. Absent = no caching.
    pub ttl: Option<String>,
    /// Address selection: `"first"`, `"roundRobin"`, `"random"`.
    pub select: Option<String>,
    /// Address-family policy: `"preferIPv4"`, `"preferIPv6"`,
    /// `"onlyIPv4"`, `"onlyIPv6"`, `"any"`.
    pub policy: Option<String>,
}

/// A ramping stage. `target` is `f64` so a single struct serves both
/// ramping-vus (integer VU counts) and ramping-arrival-rate (fractional
/// iterations/sec) stages.
#[derive(Debug, Clone, Deserialize)]
pub struct K6Stage {
    pub duration: String,
    pub target: f64,
}

/// One entry of `options.scenarios`.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct K6Scenario {
    /// k6 executor name: `constant-vus`, `ramping-vus`, `shared-iterations`,
    /// `per-vu-iterations`, `constant-arrival-rate`, `ramping-arrival-rate`.
    pub executor: String,
    // constant-vus / shared-iterations / per-vu-iterations
    pub vus: Option<u32>,
    pub duration: Option<String>,
    pub iterations: Option<u64>,
    // ramping-vus
    #[serde(alias = "startVus")]
    pub start_vus: Option<u32>,
    pub stages: Option<Vec<K6Stage>>,
    // arrival rate
    pub rate: Option<f64>,
    #[serde(alias = "timeUnit")]
    pub time_unit: Option<String>,
    #[serde(alias = "preAllocatedVUs")]
    pub pre_allocated_vus: Option<u32>,
    #[serde(alias = "maxVUs")]
    pub max_vus: Option<u32>,
    #[serde(alias = "startRate")]
    pub start_rate: Option<f64>,
    // shared / per-vu-iterations
    #[serde(alias = "maxDuration")]
    pub max_duration: Option<String>,
    // common
    #[serde(alias = "gracefulStop")]
    pub graceful_stop: Option<String>,
    #[serde(alias = "gracefulRampDown")]
    pub graceful_ramp_down: Option<String>,
    #[serde(alias = "startTime")]
    pub start_time: Option<String>,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    /// Named export the scenario executes. Tropel currently always runs the
    /// script's `export default`, so a non-default `exec` is logged and the
    /// default function is used instead.
    pub exec: Option<String>,
}

/// k6 thresholds value: either a bare string (`"p(95)<500"`), or an array of
/// strings and/or `{ threshold, abortOnFail, delayAbortEval }` objects.
///
/// The `Other` catch-all keeps an unusual threshold shape from failing the
/// *whole* options parse (which would silently drop vus/duration too). An
/// unrecognized shape yields no thresholds rather than killing the load
/// profile.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum K6ThresholdSpec {
    Single(String),
    Array(Vec<serde_json::Value>),
    /// Any other shape (e.g. a bare `{threshold:...}` object) — ignored.
    /// The payload exists only so the untagged fallback succeeds; it is never
    /// read.
    #[allow(dead_code)]
    Other(serde_json::Value),
}

impl K6Options {
    /// Convert into the engine-facing declared-options struct.
    ///
    /// Named scenarios take precedence over the top-level executor, matching
    /// k6 semantics. Returns `None` when nothing usable is declared.
    pub fn to_declared(&self) -> Option<DriverDeclaredOptions> {
        let thresholds = self.convert_thresholds();

        if let Some(scenarios) = &self.scenarios {
            if !scenarios.is_empty() {
                let mut map = HashMap::new();
                for (name, sc) in scenarios {
                    let Some(exec) = sc.to_execution() else {
                        tracing::warn!(
                            "k6 scenario '{name}' (executor '{}') is missing required \
                             fields — skipped",
                            sc.executor
                        );
                        continue;
                    };
                    map.insert(
                        name.clone(),
                        ScenarioConfig {
                            execution: exec,
                            input: None,
                            env: sc.env.clone(),
                            tags: sc.tags.clone(),
                            start_time: sc.start_time.clone().unwrap_or_else(|| "0s".to_string()),
                            // k6 `exec` names which exported function runs for
                            // this scenario — threaded through to the driver
                            // so it installs that export as __tropel_iteration.
                            exec: sc.exec.clone(),
                        },
                    );
                }
                if !map.is_empty() {
                    return Some(DriverDeclaredOptions {
                        execution: None,
                        scenarios: Some(map),
                        thresholds,
                        discard_response_bodies: self.discard_response_bodies,
                        summary_trend_stats: self.summary_trend_stats.clone(),
                        dns_ttl: self.dns.as_ref().and_then(|d| d.ttl.clone()),
                        dns_select: self.dns.as_ref().and_then(|d| d.select.clone()),
                        dns_policy: self.dns.as_ref().and_then(|d| d.policy.clone()),
                        no_connection_reuse: self.no_connection_reuse,
                        no_vu_connection_reuse: self.no_vu_connection_reuse,
                        rps: self.rps,
                        hosts: self.hosts.clone(),
                        blacklist_ips: self.blacklist_ips.clone(),
                    });
                }
            }
        }

        let execution = self.to_execution()?;
        Some(DriverDeclaredOptions {
            execution: Some(execution),
            scenarios: None,
            thresholds,
            discard_response_bodies: self.discard_response_bodies,
            summary_trend_stats: self.summary_trend_stats.clone(),
            dns_ttl: self.dns.as_ref().and_then(|d| d.ttl.clone()),
            dns_select: self.dns.as_ref().and_then(|d| d.select.clone()),
            dns_policy: self.dns.as_ref().and_then(|d| d.policy.clone()),
            no_connection_reuse: self.no_connection_reuse,
            no_vu_connection_reuse: self.no_vu_connection_reuse,
            rps: self.rps,
            hosts: self.hosts.clone(),
            blacklist_ips: self.blacklist_ips.clone(),
        })
    }

    /// Build the top-level executor from `vus`/`duration`/`iterations`/`stages`.
    /// Precedence mirrors k6: stages → ramping-vus, iterations →
    /// shared-iterations, vus+duration → constant-vus.
    fn to_execution(&self) -> Option<ExecutionConfig> {
        let think_time = ThinkTimeConfig::default();

        if let Some(stages) = &self.stages {
            if !stages.is_empty() {
                return Some(ExecutionConfig::RampingVus {
                    stages: stages
                        .iter()
                        .map(|s| Stage {
                            duration: s.duration.clone(),
                            target: s.target as u32,
                        })
                        .collect(),
                    start_vus: self.vus.unwrap_or(1),
                    graceful_ramp_down: self.graceful_ramp_down.clone(),
                    graceful_stop: self.graceful_stop.clone(),
                    think_time,
                });
            }
        }

        if let Some(iterations) = self.iterations {
            return Some(ExecutionConfig::SharedIterations {
                iterations,
                max_duration: self.max_duration.clone(),
                vus: self.vus.unwrap_or(1),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            });
        }

        if let (Some(vus), Some(duration)) = (self.vus, &self.duration) {
            return Some(ExecutionConfig::ConstantVus {
                vus,
                duration: duration.clone(),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            });
        }

        None
    }

    /// Convert k6 thresholds (metric → spec) into Tropel `ThresholdConfig`s.
    fn convert_thresholds(&self) -> HashMap<String, ThresholdConfig> {
        let mut out = HashMap::new();
        if let Some(thresholds) = &self.thresholds {
            for (metric, spec) in thresholds {
                let configs = spec_to_configs(metric, spec);
                for (i, cfg) in configs.into_iter().enumerate() {
                    let key = if i == 0 {
                        metric.clone()
                    } else {
                        format!("{}#{}", metric, i)
                    };
                    out.insert(key, cfg);
                }
            }
        }
        out
    }
}

impl K6Scenario {
    /// Convert one named scenario into an `ExecutionConfig` by executor name.
    fn to_execution(&self) -> Option<ExecutionConfig> {
        let think_time = ThinkTimeConfig::default();
        match self.executor.as_str() {
            "constant-vus" => Some(ExecutionConfig::ConstantVus {
                vus: self.vus?,
                duration: self.duration.clone()?,
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "ramping-vus" => Some(ExecutionConfig::RampingVus {
                stages: self
                    .stages
                    .as_ref()
                    .map(|s| {
                        s.iter()
                            .map(|st| Stage {
                                duration: st.duration.clone(),
                                target: st.target as u32,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                start_vus: self.start_vus.or(self.vus).unwrap_or(1),
                graceful_ramp_down: self.graceful_ramp_down.clone(),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "shared-iterations" => Some(ExecutionConfig::SharedIterations {
                iterations: self.iterations?,
                max_duration: self.max_duration.clone(),
                vus: self.vus.unwrap_or(1),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "per-vu-iterations" => Some(ExecutionConfig::PerVUIterations {
                vus: self.vus.unwrap_or(1),
                iterations: self.iterations?,
                max_duration: self.max_duration.clone(),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "constant-arrival-rate" => Some(ExecutionConfig::ConstantArrivalRate {
                rate: self.rate?,
                time_unit: self.time_unit.clone().unwrap_or_else(|| "1s".to_string()),
                duration: self.duration.clone()?,
                pre_alloc_vus: self.pre_allocated_vus.unwrap_or(1),
                max_vus: self.max_vus.unwrap_or(10),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "ramping-arrival-rate" => Some(ExecutionConfig::RampingArrivalRate {
                start_rate: self.start_rate.unwrap_or(0.0),
                stages: self
                    .stages
                    .as_ref()
                    .map(|s| {
                        s.iter()
                            .map(|st| ArrivalRateStage {
                                duration: st.duration.clone(),
                                target: st.target,
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                time_unit: self.time_unit.clone().unwrap_or_else(|| "1s".to_string()),
                pre_alloc_vus: self.pre_allocated_vus.unwrap_or(1),
                max_vus: self.max_vus.unwrap_or(10),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            "externally-controlled" => Some(ExecutionConfig::ExternallyControlled {
                vus: self.vus.unwrap_or(1),
                max_vus: self.max_vus.unwrap_or(10),
                duration: self.duration.clone(),
                graceful_stop: self.graceful_stop.clone(),
                think_time,
            }),
            other => {
                tracing::warn!("k6 scenario executor '{other}' is not supported — skipping");
                None
            }
        }
    }
}

fn spec_to_configs(metric: &str, spec: &K6ThresholdSpec) -> Vec<ThresholdConfig> {
    match spec {            K6ThresholdSpec::Single(s) => vec![build_threshold(metric, s, false, None)],
        K6ThresholdSpec::Other(_) => {
            tracing::warn!(
                "k6 threshold '{metric}' has an unsupported shape — ignored"
            );
            vec![]
        }
        K6ThresholdSpec::Array(items) => {
            let mut out = Vec::new();
            for item in items {
                if let Some(expr) = item.as_str() {
                    out.push(build_threshold(metric, expr, false, None));
                } else if let Some(obj) = item.as_object() {
                    let expr = obj
                        .get("threshold")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    let abort = obj
                        .get("abortOnFail")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    let delay = obj
                        .get("delayAbortEval")
                        .and_then(|v| v.as_str())
                        .map(|s| s.to_string());
                    if let Some(expr) = expr {
                        out.push(build_threshold(metric, &expr, abort, delay));
                    }
                }
            }
            out
        }
    }
}

fn build_threshold(
    metric: &str,
    expr: &str,
    abort_on_fail: bool,
    delay_abort_eval: Option<String>,
) -> ThresholdConfig {
    ThresholdConfig {
        expression: translate_k6_expression(metric, expr),
        abort_on_fail,
        delay_abort_eval,
    }
}

/// Duration metrics recorded by Tropel in MICROSECONDS (µs). k6 thresholds
/// on these are in MILLISECONDS (k6's native unit), so a `p(95)<500` means
/// 500 ms = 500 000 µs. Without scaling, `http_req_duration.p95 < 500`
/// compared 500 against µs values (e.g. 292 607 µs) and every duration
/// threshold failed ~1000×.
const DURATION_METRICS: [&str; 6] = [
    "http_req_duration",
    "iteration_duration",
    "group_duration",
    "ws_req_duration",
    "ws_connecting",
    "ws_session_duration",
];

/// Translate a k6 threshold expression (`p(95)<500`, `avg<200`, `rate<0.01`)
/// into Tropel's `metric.stat op value` form (e.g. `http_req_duration.p95 < 500000`).
///
/// k6 expresses the metric via the map key; Tropel's evaluator wants a fully
/// qualified reference. Duration-metric values are scaled ms → µs to match
/// Tropel's internal unit. Expressions that already carry a metric name (or
/// use syntax we don't recognize) are passed through unchanged — compound
/// expressions (`&&`/`||`) are logged loudly because the evaluator cannot
/// parse them and would otherwise report them as silently passing.
fn translate_k6_expression(metric: &str, expr: &str) -> String {
    let re = Regex::new(
        r"^\s*(p\(\d+(?:\.\d+)?\)|avg|median|min|max|count|sum|rate)\s*(<=|>=|==|!=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$",
    )
    .expect("threshold translation regex is valid");
    if let Some(caps) = re.captures(expr) {
        let stat = &caps[1];
        let op = &caps[2];
        let val: f64 = caps[3].parse().unwrap_or(0.0);
        // ms → µs for duration metrics (k6 writes these thresholds in ms).
        // EXCEPT `count` — the sample count is unitless, so `count>100` must
        // stay `> 100`, not `> 100000`.
        //
        // The metric key may carry a tag filter
        // (`http_req_duration{scenario:api_load}`) — strip `{…}` before the
        // membership check, otherwise scoped duration thresholds were never
        // scaled and compared ms values against µs samples (~1000× off).
        let is_count = stat == "count";
        let base = metric.split('{').next().unwrap_or(metric);
        let val = if DURATION_METRICS.contains(&base) && !is_count {
            val * 1000.0
        } else {
            val
        };
        // Trim trailing ".0" so integers stay readable (500000.0 → "500000").
        let val_str = if val.fract() == 0.0 {
            format!("{:.0}", val)
        } else {
            val.to_string()
        };
        let suffix = match stat {
            s if s.starts_with("p(") => {
                // p(95) → .p95; map unsupported buckets to the nearest supported
                // (MetricsResult tracks p50/p90/p95/p99).
                let pct: f64 = s[2..s.len() - 1].parse().unwrap_or(95.0);
                match pct {
                    x if x <= 50.0 => ".p50".to_string(),
                    x if x <= 90.0 => ".p90".to_string(),
                    x if x <= 95.0 => ".p95".to_string(),
                    _ => ".p99".to_string(),
                }
            }
            "median" => ".p50".to_string(),
            // avg / min / max / count / sum / rate map 1:1 onto evaluator stats
            other => format!(".{other}"),
        };
        return format!("{metric}{suffix} {op} {val_str}");
    }
    if expr.contains("&&") || expr.contains("||") {
        tracing::warn!(
            "k6 threshold '{}' uses compound expression '{}' which Tropel cannot \
             evaluate — it will always report PASS",
            metric,
            expr.trim()
        );
    }
    expr.trim().to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(json: &str) -> K6Options {
        serde_json::from_str(json).expect("options JSON must parse")
    }

    #[test]
    fn test_constant_vus() {
        let opts = parse(r#"{"vus": 10, "duration": "30s"}"#);
        let decl = opts.to_declared().expect("declared options");
        match decl.execution {
            Some(ExecutionConfig::ConstantVus { vus, duration, .. }) => {
                assert_eq!(vus, 10);
                assert_eq!(duration, "30s");
            }
            other => panic!("expected ConstantVus, got {other:?}"),
        }
    }

    #[test]
    fn test_ramping_vus_from_stages() {
        let opts = parse(r#"{"vus": 2, "stages": [{"duration": "10s", "target": 20}]}"#);
        let decl = opts.to_declared().unwrap();
        match decl.execution {
            Some(ExecutionConfig::RampingVus {
                start_vus,
                stages,
                ..
            }) => {
                assert_eq!(start_vus, 2);
                assert_eq!(stages.len(), 1);
                assert_eq!(stages[0].target, 20);
            }
            other => panic!("expected RampingVus, got {other:?}"),
        }
    }

    #[test]
    fn test_shared_iterations() {
        let opts = parse(r#"{"vus": 5, "iterations": 100}"#);
        let decl = opts.to_declared().unwrap();
        match decl.execution {
            Some(ExecutionConfig::SharedIterations {
                iterations, vus, ..
            }) => {
                assert_eq!(iterations, 100);
                assert_eq!(vus, 5);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }

    #[test]
    fn test_named_scenarios_take_precedence() {
        let opts = parse(
            r#"{
                "vus": 1,
                "duration": "10s",
                "scenarios": {
                    "load": { "executor": "constant-vus", "vus": 25, "duration": "1m", "startTime": "5s", "env": {"K": "V"}, "tags": {"scenario": "load"} }
                }
            }"#,
        );
        let decl = opts.to_declared().unwrap();
        let scenarios = decl.scenarios.expect("scenarios");
        let load = scenarios.get("load").expect("load scenario");
        assert_eq!(load.start_time, "5s");
        assert_eq!(load.env.get("K").map(|s| s.as_str()), Some("V"));
        match &load.execution {
            ExecutionConfig::ConstantVus { vus, duration, .. } => {
                assert_eq!(*vus, 25);
                assert_eq!(duration, "1m");
            }
            other => panic!("expected ConstantVus, got {other:?}"),
        }
    }

    #[test]
    fn test_arrival_rate_scenario() {
        let opts = parse(
            r#"{
                "scenarios": {
                    "spam": { "executor": "constant-arrival-rate", "rate": 50, "timeUnit": "1s", "duration": "30s", "preAllocatedVUs": 5, "maxVUs": 20 }
                }
            }"#,
        );
        let decl = opts.to_declared().unwrap();
        let sc = decl.scenarios.unwrap().remove("spam").unwrap();
        match sc.execution {
            ExecutionConfig::ConstantArrivalRate {
                rate,
                pre_alloc_vus,
                max_vus,
                ..
            } => {
                assert_eq!(rate, 50.0);
                assert_eq!(pre_alloc_vus, 5);
                assert_eq!(max_vus, 20);
            }
            other => panic!("expected ConstantArrivalRate, got {other:?}"),
        }
    }

    #[test]
    fn test_threshold_string_form() {
        let opts = parse(r#"{"thresholds": {"http_req_duration": ["p(95)<500", "avg<200"]}}"#);
        let thresholds = opts.convert_thresholds();
        assert_eq!(thresholds.len(), 2);
        // Duration metrics are recorded in µs internally; k6 thresholds are
        // ms, so they are scaled ×1000 during translation.
        assert_eq!(
            thresholds.get("http_req_duration").unwrap().expression,
            "http_req_duration.p95 < 500000"
        );
        assert_eq!(
            thresholds.get("http_req_duration#1").unwrap().expression,
            "http_req_duration.avg < 200000"
        );
    }

    #[test]
    fn test_threshold_non_duration_not_scaled() {
        // Rate/counter thresholds keep their raw value.
        let opts = parse(
            r#"{"thresholds": {"http_req_failed": ["rate<0.01"], "http_reqs": ["count>100"]}}"#,
        );
        let thresholds = opts.convert_thresholds();
        assert_eq!(
            thresholds.get("http_req_failed").unwrap().expression,
            "http_req_failed.rate < 0.01"
        );
        assert_eq!(
            thresholds.get("http_reqs").unwrap().expression,
            "http_reqs.count > 100"
        );
    }

    #[test]
    fn test_duration_threshold_fraction_scaled() {
        let expr = translate_k6_expression("http_req_duration", "avg<1.5");
        assert_eq!(expr, "http_req_duration.avg < 1500");
    }

    #[test]
    fn test_duration_threshold_tag_scoped_scaled() {
        // Tag-scoped duration thresholds must be scaled too — the key
        // carries a `{scenario:…}` filter that must not defeat the µs check.
        let expr = translate_k6_expression(
            "http_req_duration{scenario:api_load}",
            "p(95)<300",
        );
        assert_eq!(expr, "http_req_duration{scenario:api_load}.p95 < 300000");
    }

    #[test]
    fn test_duration_count_not_scaled() {
        // `count` is unitless even on duration metrics — must not be ×1000.
        let expr = translate_k6_expression("http_req_duration", "count>100");
        assert_eq!(expr, "http_req_duration.count > 100");
        // Non-duration metrics unaffected.
        let expr = translate_k6_expression("http_reqs", "count>100");
        assert_eq!(expr, "http_reqs.count > 100");
    }

    #[test]
    fn test_threshold_config_object_form() {
        let opts = parse(
            r#"{"thresholds": {"http_req_duration": [{"threshold": "p(99)<1000", "abortOnFail": true, "delayAbortEval": "30s"}]}}"#,
        );
        let thresholds = opts.convert_thresholds();
        let cfg = thresholds.get("http_req_duration").unwrap();
        // p(99)<1000 ms → 1 000 000 µs.
        assert_eq!(cfg.expression, "http_req_duration.p99 < 1000000");
        assert!(cfg.abort_on_fail);
        assert_eq!(cfg.delay_abort_eval.as_deref(), Some("30s"));
    }

    #[test]
    fn test_threshold_fully_qualified_passthrough() {
        // Expressions that already carry a full metric ref are not rewritten.
        let expr = translate_k6_expression("http_req_duration", "http_req_duration.p95 < 500");
        assert_eq!(expr, "http_req_duration.p95 < 500");
    }

    #[test]
    fn test_threshold_p_99_9_maps_to_p99() {
        // p(99.9) maps to the nearest supported bucket AND is ms→µs scaled.
        let expr = translate_k6_expression("http_req_duration", "p(99.9)<1000");
        assert_eq!(expr, "http_req_duration.p99 < 1000000");
    }

    #[test]
    fn test_empty_options_is_none() {
        let opts = parse(r#"{}"#);
        assert!(opts.to_declared().is_none());
    }

    #[test]
    fn test_camel_case_aliases() {
        let opts = parse(
            r#"{"vus": 3, "duration": "1m", "gracefulStop": "45s", "gracefulRampDown": "20s"}"#,
        );
        assert_eq!(opts.graceful_stop.as_deref(), Some("45s"));
        assert_eq!(opts.graceful_ramp_down.as_deref(), Some("20s"));
    }

    #[test]
    fn test_dns_and_http_options_map() {
        let opts = parse(
            r#"{
                "vus": 1,
                "duration": "10s",
                "dns": { "ttl": "1m", "select": "roundRobin", "policy": "preferIPv4" },
                "noConnectionReuse": true,
                "noVUConnectionReuse": true,
                "rps": 50,
                "hosts": { "api.example.com": "10.0.0.1" },
                "blacklistIPs": ["10.0.0.0/8", "192.168.1.5"]
            }"#,
        );
        assert_eq!(opts.dns.as_ref().and_then(|d| d.ttl.as_deref()), Some("1m"));
        assert_eq!(
            opts.dns.as_ref().and_then(|d| d.select.as_deref()),
            Some("roundRobin")
        );
        assert_eq!(
            opts.dns.as_ref().and_then(|d| d.policy.as_deref()),
            Some("preferIPv4")
        );
        assert_eq!(opts.no_connection_reuse, Some(true));
        assert_eq!(opts.no_vu_connection_reuse, Some(true));
        assert_eq!(opts.rps, Some(50.0));
        assert_eq!(
            opts.hosts.as_ref().and_then(|h| h.get("api.example.com")).map(|s| s.as_str()),
            Some("10.0.0.1")
        );
        assert_eq!(opts.blacklist_ips.as_ref().map(|b| b.len()), Some(2));

        let decl = opts.to_declared().expect("declared options");
        assert_eq!(decl.dns_ttl.as_deref(), Some("1m"));
        assert_eq!(decl.dns_select.as_deref(), Some("roundRobin"));
        assert_eq!(decl.dns_policy.as_deref(), Some("preferIPv4"));
        assert_eq!(decl.no_connection_reuse, Some(true));
        assert_eq!(decl.no_vu_connection_reuse, Some(true));
        assert_eq!(decl.rps, Some(50.0));
        assert_eq!(decl.hosts.as_ref().map(|h| h.len()), Some(1));
        assert_eq!(decl.blacklist_ips.as_ref().map(|b| b.len()), Some(2));
    }
}
