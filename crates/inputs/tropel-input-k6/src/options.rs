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
    /// Skip TLS certificate verification (k6 `insecureSkipTLSVerify`) — the
    /// most common staging idiom, and security-relevant. When `Some(true)`,
    /// the engine disables certificate validation on the HTTP client's TLS
    /// config. Previously unmodelled, so it was silently dropped.
    #[serde(alias = "insecureSkipTLSVerify")]
    pub insecure_skip_tls_verify: Option<bool>,
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
    // k6's real spelling is `startVUs` (serde aliases are byte-exact — the
    // old `startVus` alias silently dropped the field, falling back to
    // `vus`/1). Backlog line 128.
    #[serde(alias = "startVUs")]
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
                        insecure_skip_tls_verify: self.insecure_skip_tls_verify,
                    });
                }
            }
        }

        let execution = self.to_execution();
        // Return None only when NOTHING usable is declared (an empty script).
        // When the script declares only safety options — thresholds, DNS,
        // hosts, blacklist, rps, discardResponseBodies, summaryTrendStats —
        // with no load profile (the standard CI shape where --vus/--duration
        // come from the CLI), those options MUST survive. The old `?` on
        // to_execution() short-circuited the whole method to None and
        // silently discarded every one of them.
        let has_anything = execution.is_some()
            || !thresholds.is_empty()
            || self.discard_response_bodies.is_some()
            || self
                .summary_trend_stats
                .as_ref()
                .is_some_and(|s| !s.is_empty())
            || self.dns.is_some()
            || self.no_connection_reuse.is_some()
            || self.no_vu_connection_reuse.is_some()
            || self.rps.is_some()
            || self.hosts.as_ref().is_some_and(|h| !h.is_empty())
            || self.blacklist_ips.as_ref().is_some_and(|b| !b.is_empty())
            || self.insecure_skip_tls_verify.is_some();
        if !has_anything {
            return None;
        }
        if execution.is_none() {
            tracing::debug!(
                "k6 script declares no load profile — keeping {} declared \
                 safety option(s) (thresholds/DNS/hosts/rps); load profile \
                 comes from the CLI/config",
                thresholds.len()
                    + usize::from(self.dns.is_some())
                    + usize::from(self.rps.is_some())
                    + usize::from(self.hosts.is_some())
                    + usize::from(self.blacklist_ips.is_some())
                    + usize::from(self.no_connection_reuse.is_some())
                    + usize::from(self.no_vu_connection_reuse.is_some())
                    + usize::from(self.discard_response_bodies.is_some())
                    + usize::from(self.insecure_skip_tls_verify.is_some())
            );
        }
        Some(DriverDeclaredOptions {
            execution,
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
            insecure_skip_tls_verify: self.insecure_skip_tls_verify,
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

        // Backlog line 152: `duration`-only options previously yielded NO
        // profile at all (this arm required BOTH `vus` and `duration`). k6
        // defaults `vus` to 1, so `options: { duration: "30s" }` must produce
        // a constant-vus profile, not silently run the CLI profile.
        if let Some(duration) = &self.duration {
            return Some(ExecutionConfig::ConstantVus {
                vus: self.vus.unwrap_or(1),
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
            // Backlog line 152: k6 REQUIRES `preAllocatedVUs` for arrival-rate
            // executors (a missing value is a hard error there; here the
            // scenario is skipped with a warning instead of silently running
            // 1 VU serving the whole rate), and `maxVUs` DEFAULTS to
            // `preAllocatedVUs` and can never be below it (the old invented
            // `10` could under-provision `{rate:2000, preAllocatedVUs:400}`
            // to 10 VUs, or worse if maxVUs was set lower than preAlloc).
            "constant-arrival-rate" => {
                let pre_alloc_vus = self.pre_allocated_vus?;
                Some(ExecutionConfig::ConstantArrivalRate {
                    rate: self.rate?,
                    time_unit: self.time_unit.clone().unwrap_or_else(|| "1s".to_string()),
                    duration: self.duration.clone()?,
                    pre_alloc_vus,
                    max_vus: self.max_vus.unwrap_or(pre_alloc_vus).max(pre_alloc_vus),
                    graceful_stop: self.graceful_stop.clone(),
                    think_time,
                })
            }
            "ramping-arrival-rate" => {
                // Same k6 parity as constant-arrival-rate (line 152):
                // preAllocatedVUs required, maxVUs defaults to it and can
                // never be below it.
                let pre_alloc_vus = self.pre_allocated_vus?;
                Some(ExecutionConfig::RampingArrivalRate {
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
                    pre_alloc_vus,
                    max_vus: self.max_vus.unwrap_or(pre_alloc_vus).max(pre_alloc_vus),
                    graceful_stop: self.graceful_stop.clone(),
                    think_time,
                })
            }
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
    match spec {
        K6ThresholdSpec::Single(s) => vec![build_threshold(metric, s, false, None)],
        K6ThresholdSpec::Other(_) => {
            tracing::warn!("k6 threshold '{metric}' has an unsupported shape — ignored");
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

/// Translate a k6 threshold expression (`p(95)<500`, `avg<200`, `rate<0.01`)
/// into Tropel's `metric.stat op value` form (e.g. `http_req_duration.p95 < 500`).
///
/// k6 expresses the metric via the map key; Tropel's evaluator wants a fully
/// qualified reference. Since backlog §0, ALL durations are stored in
/// MILLISECONDS end-to-end (emitters push ms, the histogram buckets ms), so
/// k6's ms-denominated threshold values pass through UNSCALED — the old
/// ms→µs ×1000 (and the DURATION_METRICS list it depended on) is deleted.
/// This is what makes `--threshold 'http_req_duration.p95 < 500'` on the CLI
/// agree with an identical k6-script threshold. Expressions that already
/// carry a metric name (or use syntax we don't recognize) are passed through
/// unchanged — compound expressions (`&&`/`||`) are logged loudly because
/// the evaluator cannot parse them and would otherwise report them as
/// silently passing.
fn translate_k6_expression(metric: &str, expr: &str) -> String {
    // k6's real median stat is `med` (not `median`). Accepting `median` meant
    // `med<400` fell through to the evaluator as a 1-token expression and
    // silently passed (fail-open gate).
    let re = Regex::new(
        r"^\s*(p\(\d+(?:\.\d+)?\)|avg|med|min|max|count|sum|rate)\s*(<=|>=|==|!=|<|>)\s*(-?\d+(?:\.\d+)?)\s*$",
    )
    .expect("threshold translation regex is valid");
    if let Some(caps) = re.captures(expr) {
        let stat = &caps[1];
        let op = &caps[2];
        let val: f64 = caps[3].parse().unwrap_or(0.0);
        // Duration thresholds are written in ms and samples are stored in ms
        // (backlog §0) — no scaling. `count` is unitless and also passes
        // through unchanged.
        //
        // Trim trailing ".0" so integers stay readable (500.0 → "500").
        let val_str = if val.fract() == 0.0 {
            format!("{:.0}", val)
        } else {
            val.to_string()
        };
        let suffix = match stat {
            // Preserve the EXACT percentile — the evaluator resolves any
            // p(N) from the retained histogram (parse_percentile /
            // percentile_value, thresholds.rs / collector.rs). The old
            // bucket-snap (p(99.9)→p99, p(75)→p90, p(10)→p50) discarded that
            // precision one layer above the layer that handles it, producing
            // false green (looser bucket) and false red (stricter bucket).
            // The parenthesized form is REQUIRED: `.p99.9` would split on the
            // second dot in parse_metric_ref and yield stat "9".
            s if s.starts_with("p(") => format!(".{s}"),
            "med" | "median" => ".p50".to_string(),
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
                start_vus, stages, ..
            }) => {
                assert_eq!(start_vus, 2);
                assert_eq!(stages.len(), 1);
                assert_eq!(stages[0].target, 20);
            }
            other => panic!("expected RampingVus, got {other:?}"),
        }
    }

    #[test]
    fn test_scenario_start_vus_k6_spelling() {
        // Backlog line 128 regression: k6's real spelling is `startVUs` — the
        // old `#[serde(alias = "startVus")]` was byte-exact, so the field was
        // silently dropped and start_vus fell back to vus/1. The repo's own
        // examples/k6/k6_sample_scenarios.js uses `startVUs: 0`.
        let opts = parse(
            r#"{"scenarios": {"s": {"executor": "ramping-vus", "startVUs": 0, "stages": [{"duration": "10s", "target": 10}]}}}"#,
        );
        let decl = opts.to_declared().expect("declared options");
        let scenarios = decl.scenarios.expect("scenarios present");
        let sc = scenarios.get("s").expect("scenario s");
        match &sc.execution {
            tropel_sdk::ExecutionConfig::RampingVus {
                start_vus, stages, ..
            } => {
                assert_eq!(
                    *start_vus, 0,
                    "startVUs must be honored (not fall back to vus/1)"
                );
                assert_eq!(stages.len(), 1);
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
    fn test_arrival_rate_max_vus_defaults_and_clamps() {
        // Backlog line 152: maxVUs previously defaulted to an invented 10 and
        // could be BELOW preAllocatedVUs. k6 defaults maxVUs to
        // preAllocatedVUs and never lets it under-provision.
        let opts = parse(
            r#"{
                "scenarios": {
                    "spam": { "executor": "constant-arrival-rate", "rate": 2000, "timeUnit": "1s", "duration": "30s", "preAllocatedVUs": 400 }
                }
            }"#,
        );
        let decl = opts.to_declared().unwrap();
        let sc = decl.scenarios.unwrap().remove("spam").unwrap();
        match sc.execution {
            ExecutionConfig::ConstantArrivalRate {
                pre_alloc_vus,
                max_vus,
                ..
            } => {
                assert_eq!(pre_alloc_vus, 400);
                assert_eq!(
                    max_vus, 400,
                    "maxVUs must default to preAllocatedVUs (k6 parity), not 10"
                );
            }
            other => panic!("expected ConstantArrivalRate, got {other:?}"),
        }

        // maxVUs explicitly BELOW preAllocatedVUs must clamp up.
        let opts = parse(
            r#"{
                "scenarios": {
                    "spam": { "executor": "constant-arrival-rate", "rate": 50, "duration": "30s", "preAllocatedVUs": 20, "maxVUs": 5 }
                }
            }"#,
        );
        let decl = opts.to_declared().unwrap();
        let sc = decl.scenarios.unwrap().remove("spam").unwrap();
        match sc.execution {
            ExecutionConfig::ConstantArrivalRate { max_vus, .. } => {
                assert_eq!(max_vus, 20, "maxVUs below preAllocatedVUs must clamp up");
            }
            other => panic!("expected ConstantArrivalRate, got {other:?}"),
        }
    }

    #[test]
    fn test_arrival_rate_requires_pre_allocated_vus() {
        // Backlog line 152: omitting preAllocatedVUs made 1 VU serve the whole
        // rate (k6 hard-errors). The scenario must be skipped, never silently
        // under-provisioned.
        let opts = parse(
            r#"{
                "scenarios": {
                    "spam": { "executor": "constant-arrival-rate", "rate": 2000, "duration": "30s" }
                }
            }"#,
        );
        // When the ONLY scenario is skipped and no top-level profile exists,
        // to_declared() returns None (nothing usable declared) — both are
        // correct outcomes; the scenario must never silently run 1 VU.
        let declared = opts.to_declared();
        assert!(
            declared
                .as_ref()
                .and_then(|d| d.scenarios.as_ref())
                .is_none(),
            "arrival-rate scenario without preAllocatedVUs must be skipped (k6 rejects it)"
        );
    }

    #[test]
    fn test_duration_only_options_produce_constant_vus() {
        // Backlog line 152: `duration`-only options previously yielded NO
        // profile (the top-level executor required BOTH vus and duration). k6
        // defaults vus to 1, so a profile must be produced.
        let opts = parse(r#"{ "duration": "30s" }"#);
        let decl = opts.to_declared().unwrap();
        match decl.execution {
            Some(ExecutionConfig::ConstantVus { vus, duration, .. }) => {
                assert_eq!(vus, 1, "duration-only options must default vus to 1");
                assert_eq!(duration, "30s");
            }
            other => panic!("expected ConstantVus, got {other:?}"),
        }
    }

    #[test]
    fn test_threshold_string_form() {
        let opts = parse(r#"{"thresholds": {"http_req_duration": ["p(95)<500", "avg<200"]}}"#);
        let thresholds = opts.convert_thresholds();
        assert_eq!(thresholds.len(), 2);
        // Durations are stored in ms end-to-end (backlog §0) — k6's
        // ms-denominated threshold values pass through unscaled.
        assert_eq!(
            thresholds.get("http_req_duration").unwrap().expression,
            "http_req_duration.p(95) < 500"
        );
        assert_eq!(
            thresholds.get("http_req_duration#1").unwrap().expression,
            "http_req_duration.avg < 200"
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
    fn test_duration_threshold_fraction_unscaled() {
        // Durations are ms end-to-end (backlog §0): 1.5 ms stays 1.5.
        let expr = translate_k6_expression("http_req_duration", "avg<1.5");
        assert_eq!(expr, "http_req_duration.avg < 1.5");
    }

    #[test]
    fn test_duration_threshold_tag_scoped_unscaled() {
        // Tag-scoped duration thresholds pass through in ms too — the key
        // carries a `{scenario:…}` filter that must not trigger any scaling
        // (there is none anymore; the value is already the public unit).
        let expr = translate_k6_expression("http_req_duration{scenario:api_load}", "p(95)<300");
        assert_eq!(expr, "http_req_duration{scenario:api_load}.p(95) < 300");
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
        // p(99)<1000 ms stays 1000 ms (backlog §0).
        assert_eq!(cfg.expression, "http_req_duration.p(99) < 1000");
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
    fn test_threshold_p_99_9_preserved_exact() {
        // Backlog line 135: p(99.9) must NOT be snapped to the looser p99
        // bucket (false green) — the exact percentile is preserved and the
        // evaluator resolves it from the retained histogram.
        let expr = translate_k6_expression("http_req_duration", "p(99.9)<1000");
        assert_eq!(expr, "http_req_duration.p(99.9) < 1000");
        // p(75) must not become the stricter p90 (false red); p(10) must not
        // become p50 (false green).
        assert_eq!(
            translate_k6_expression("http_req_duration", "p(75)<800"),
            "http_req_duration.p(75) < 800"
        );
        assert_eq!(
            translate_k6_expression("http_req_duration", "p(10)<300"),
            "http_req_duration.p(10) < 300"
        );
    }

    #[test]
    fn test_empty_options_is_none() {
        let opts = parse(r#"{}"#);
        assert!(opts.to_declared().is_none());
    }

    #[test]
    fn test_safety_options_survive_without_load_profile() {
        // The standard CI shape: thresholds/DNS/hosts/rps live in the script,
        // --vus/--duration come from the CLI. to_declared() must NOT return
        // None just because no load profile is declared — every safety
        // control must survive with execution: None.
        let opts = parse(
            r#"{
                "thresholds": {"http_req_duration": ["p(95)<500"]},
                "dns": {"ttl": "1m", "select": "roundRobin", "policy": "any"},
                "hosts": {"test.k6.io": "1.2.3.4"},
                "blacklistIPs": ["10.0.0.0/8"],
                "rps": 100,
                "noConnectionReuse": true,
                "discardResponseBodies": true,
                "summaryTrendStats": ["avg", "p(99)"]
            }"#,
        );
        let decl = opts
            .to_declared()
            .expect("safety options must not be discarded");
        assert!(decl.execution.is_none());
        assert!(decl.scenarios.is_none());
        assert_eq!(decl.thresholds.len(), 1);
        assert_eq!(decl.dns_ttl.as_deref(), Some("1m"));
        assert_eq!(decl.dns_select.as_deref(), Some("roundRobin"));
        assert_eq!(decl.dns_policy.as_deref(), Some("any"));
        assert_eq!(
            decl.hosts.as_ref().and_then(|h| h.get("test.k6.io")),
            Some(&"1.2.3.4".to_string())
        );
        assert_eq!(
            decl.blacklist_ips.as_deref(),
            Some(&["10.0.0.0/8".to_string()][..])
        );
        assert_eq!(decl.rps, Some(100.0));
        assert_eq!(decl.no_connection_reuse, Some(true));
        assert_eq!(decl.discard_response_bodies, Some(true));
        assert_eq!(
            decl.summary_trend_stats.as_deref(),
            Some(&["avg".to_string(), "p(99)".to_string()][..])
        );
    }

    #[test]
    fn test_sub_timing_thresholds_unscaled() {
        // Backlog §0: durations are stored in ms end-to-end, so a k6
        // `http_req_waiting: ['p(95)<400']` (400 ms) compares 400 against ms
        // samples directly — the old ms→µs ×1000 is gone.
        let expr = translate_k6_expression("http_req_waiting", "p(95)<400");
        assert_eq!(expr, "http_req_waiting.p(95) < 400");
        let expr = translate_k6_expression("http_req_tls_handshaking", "avg<100");
        assert_eq!(expr, "http_req_tls_handshaking.avg < 100");
        let expr = translate_k6_expression("http_req_receiving", "med<50");
        assert_eq!(expr, "http_req_receiving.p50 < 50");
        let expr = translate_k6_expression("http_req_blocked", "p(90)<200");
        assert_eq!(expr, "http_req_blocked.p(90) < 200");
    }

    #[test]
    fn test_insecure_skip_tls_verify_survives() {
        // Backlog line 132: insecureSkipTLSVerify was unmodelled (zero repo
        // hits), so the most common staging idiom was silently dropped — a
        // script declaring ONLY this option must still yield Some(decl) with
        // the flag set, even without any load profile.
        let opts = parse(r#"{"insecureSkipTLSVerify": true}"#);
        let decl = opts
            .to_declared()
            .expect("insecureSkipTLSVerify alone must survive");
        assert!(decl.execution.is_none());
        assert_eq!(decl.insecure_skip_tls_verify, Some(true));

        // Explicitly false must also survive (not collapse into None).
        let opts = parse(r#"{"insecureSkipTLSVerify": false}"#);
        let decl = opts
            .to_declared()
            .expect("insecureSkipTLSVerify=false must survive");
        assert_eq!(decl.insecure_skip_tls_verify, Some(false));

        // And it rides along with a normal load profile.
        let opts = parse(r#"{"vus": 2, "duration": "10s", "insecureSkipTLSVerify": true}"#);
        let decl = opts.to_declared().expect("declared options");
        assert_eq!(decl.insecure_skip_tls_verify, Some(true));
        assert!(decl.execution.is_some());
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
            opts.hosts
                .as_ref()
                .and_then(|h| h.get("api.example.com"))
                .map(|s| s.as_str()),
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
