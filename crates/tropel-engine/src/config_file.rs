//! # Config-file & `K6_*` environment overlays
//!
//! Tropel can be configured three ways, in increasing precedence:
//!
//! 1. `K6_*` environment variables (k6-compatible names)
//! 2. a `--config <file.json>` partial job-config overlay
//! 3. explicit CLI flags (always win)
//!
//! The merge lives in [`crate::cli`]; this module only provides the partial
//! model (`PartialConfig`), the JSON loader, and the env-var parser.

use serde::Deserialize;
use std::collections::HashMap;
use std::path::Path;
use tropel_core::config::*;
use tropel_core::{Result, TropelError};

/// A partial `JobConfig` — every field optional so a config file (or the
/// env parser) can override only what it sets. `None`/empty means "not
/// specified; leave the CLI/default value in place".
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
pub struct PartialConfig {
    pub input_type: Option<String>,
    pub execution: Option<ExecutionConfig>,
    pub scenarios: Option<HashMap<String, ScenarioConfig>>,
    pub env: HashMap<String, String>,
    pub globals: HashMap<String, serde_json::Value>,
    pub collection_vars: HashMap<String, serde_json::Value>,
    pub data_file: Option<String>,
    pub iteration_data: Vec<HashMap<String, serde_json::Value>>,
    pub thresholds: HashMap<String, ThresholdConfig>,
    pub output: Option<OutputConfig>,
    pub http: Option<HttpConfig>,
    pub tls: Option<TlsConfig>,
    pub extensions: HashMap<String, serde_json::Value>,
    /// k6 `executionSegment` — "from:to" workload partition for this node.
    #[serde(default, alias = "executionSegment")]
    pub execution_segment: Option<String>,
    /// k6 `executionSegmentSequence` — full sequence of boundaries.
    #[serde(default, alias = "executionSegmentSequence")]
    pub execution_segment_sequence: Option<String>,
    /// Port for the runtime control API (k6 REST parity) for
    /// `externally-controlled` executors.
    #[serde(default, alias = "controlPort")]
    pub control_port: Option<u16>,
}

impl PartialConfig {
    /// Load a partial config from a JSON file.
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            TropelError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to read config file '{}': {e}", path.display()),
            ))
        })?;
        serde_json::from_str(&content).map_err(|e| {
            TropelError::Parse(format!(
                "Failed to parse config file '{}': {e}",
                path.display()
            ))
        })
    }

    /// Build a partial config from `K6_*` environment variables
    /// (k6-compatible names: `K6_VUS`, `K6_DURATION`, `K6_ITERATIONS`,
    /// `K6_MODE`, `K6_STAGES`, `K6_THRESHOLDS`, `K6_INSECURE_SKIP_TLS_VERIFY`,
    /// `K6_REPORTER`, `K6_OUTPUT`, ...). Invalid values are logged and
    /// skipped — a typo in an env var must not abort the run.
    ///
    /// The load-profile vars (`K6_MODE`/`K6_VUS`/`K6_DURATION`/
    /// `K6_ITERATIONS`/`K6_STAGES`) build an `ExecutionConfig` using the same
    /// precedence as the CLI (stages → ramping-vus, iterations →
    /// shared-iterations, mode → that executor, else vus+duration →
    /// constant-vus). The raw values are also copied into `env` so scripts
    /// see them via `__ENV` (k6 behavior).
    pub fn from_env() -> Self {
        let mut cfg = Self::default();

        let k6_mode = env_str("K6_MODE");
        let k6_vus = env_num::<u32>("K6_VUS");
        let k6_duration = env_str("K6_DURATION");
        let k6_iterations = env_num::<u64>("K6_ITERATIONS");
        let k6_stages = env_str("K6_STAGES");

        if let Some(v) = k6_mode.clone() {
            cfg.env.insert("K6_MODE".into(), v);
        }
        if let Some(v) = k6_vus {
            cfg.env.insert("K6_VUS".into(), v.to_string());
        }
        if let Some(v) = k6_duration.clone() {
            cfg.env.insert("K6_DURATION".into(), v);
        }
        if let Some(v) = k6_iterations {
            cfg.env.insert("K6_ITERATIONS".into(), v.to_string());
        }
        if let Some(v) = k6_stages.clone() {
            // JSON array of {duration, target} — parsed below.
            cfg.env.insert("K6_STAGES".into(), v);
        }

        cfg.execution = env_execution(
            k6_mode.as_deref(),
            k6_vus,
            k6_duration.as_deref(),
            k6_iterations,
            k6_stages.as_deref(),
        );

        if let Some(v) = env_str("K6_THRESHOLDS") {
            if let Ok(map) = serde_json::from_str::<HashMap<String, ThresholdConfig>>(&v) {
                cfg.thresholds.extend(map);
            } else if let Ok(map) =
                serde_json::from_str::<HashMap<String, Vec<ThresholdConfig>>>(&v)
            {
                for (metric, list) in map {
                    for (i, t) in list.into_iter().enumerate() {
                        let key = if i == 0 {
                            metric.clone()
                        } else {
                            format!("{metric}#{i}")
                        };
                        cfg.thresholds.insert(key, t);
                    }
                }
            } else {
                tracing::warn!("K6_THRESHOLDS is not valid JSON — ignored");
            }
        }
        if let Some(v) = env_str("K6_INSECURE_SKIP_TLS_VERIFY") {
            if let Ok(b) = v.parse::<bool>() {
                let mut tls = cfg.tls.take().unwrap_or_default();
                tls.insecure_skip_verify = b;
                cfg.tls = Some(tls);
            }
        }
        if let Some(v) = env_str("K6_REPORTER") {
            let mut out = cfg.output.take().unwrap_or_default();
            out.reporters = v.split(',').map(|s| s.trim().to_string()).collect();
            cfg.output = Some(out);
        }
        if let Some(v) = env_str("K6_OUTPUT") {
            let mut out = cfg.output.take().unwrap_or_default();
            out.output_file = Some(v);
            cfg.output = Some(out);
        }
        if let Some(v) = env_str("K6_PROMETHEUS_URL") {
            let mut out = cfg.output.take().unwrap_or_default();
            out.prometheus_remote_write_url = Some(v);
            cfg.output = Some(out);
        }
        if let Some(v) = env_str("K6_OTLP_ENDPOINT") {
            let mut out = cfg.output.take().unwrap_or_default();
            out.otlp_endpoint = Some(v);
            cfg.output = Some(out);
        }
        cfg.execution_segment = env_str("K6_EXECUTION_SEGMENT");
        cfg.execution_segment_sequence = env_str("K6_EXECUTION_SEGMENT_SEQUENCE");
        if let Some(v) = env_str("K6_DISCARD_RESPONSE_BODIES") {
            if let Ok(b) = v.parse::<bool>() {
                let mut http = cfg.http.take().unwrap_or_default();
                http.discard_response_bodies = b;
                cfg.http = Some(http);
            }
        }

        cfg
    }
}

fn env_str(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

fn env_num<T: std::str::FromStr>(key: &str) -> Option<T> {
    std::env::var(key).ok().and_then(|v| match v.parse() {
        Ok(val) => Some(val),
        Err(_) => {
            tracing::debug!("K6_* env var '{key}' value '{v}' is not valid — ignored");
            None
        }
    })
}

/// Build an `ExecutionConfig` from k6-style env load-profile vars,
/// mirroring the CLI's precedence (stages → ramping-vus, iterations →
/// shared-iterations, mode → explicit executor, else vus+duration →
/// constant-vus). Returns `None` when nothing usable is set.
fn env_execution(
    mode: Option<&str>,
    vus: Option<u32>,
    duration: Option<&str>,
    iterations: Option<u64>,
    stages: Option<&str>,
) -> Option<ExecutionConfig> {
    let think_time = ThinkTimeConfig::default();

    if let Some(mode) = mode {
        // Canonical mode→executor mapping lives in tropel-core (shared with
        // the CLI), so the precedence rules exist in exactly one place.
        return Some(ExecutionConfig::from_mode(
            mode,
            vus,
            duration.map(|s| s.to_string()),
            iterations,
            stages.map(|s| s.to_string()),
        ));
    }

    if stages.is_some() {
        let stage_list = stages
            .and_then(|s| serde_json::from_str::<Vec<Stage>>(s).ok())?;
        if !stage_list.is_empty() {
            return Some(ExecutionConfig::RampingVus {
                stages: stage_list,
                start_vus: vus.unwrap_or(1),
                graceful_ramp_down: Some("30s".to_string()),
                graceful_stop: Some("30s".to_string()),
                think_time,
            });
        }
    }

    if let Some(iterations) = iterations {
        return Some(ExecutionConfig::SharedIterations {
            iterations,
            max_duration: duration.map(|s| s.to_string()),
            vus: vus.unwrap_or(1),
            graceful_stop: Some("30s".to_string()),
            think_time,
        });
    }

    if let (Some(vus), Some(duration)) = (vus, duration) {
        return Some(ExecutionConfig::ConstantVus {
            vus,
            duration: duration.to_string(),
            graceful_stop: Some("30s".to_string()),
            think_time,
        });
    }

    None
}

#[cfg(test)]
mod env_tests {
    use super::*;

    #[test]
    fn test_env_vus_duration_constant() {
        let exec = env_execution(None, Some(5), Some("10s"), None, None);
        match exec {
            Some(ExecutionConfig::ConstantVus { vus, duration, .. }) => {
                assert_eq!(vus, 5);
                assert_eq!(duration, "10s");
            }
            other => panic!("expected ConstantVus, got {other:?}"),
        }
    }

    #[test]
    fn test_env_iterations_shared() {
        let exec = env_execution(None, Some(3), None, Some(50), None);
        match exec {
            Some(ExecutionConfig::SharedIterations {
                iterations, vus, ..
            }) => {
                assert_eq!(iterations, 50);
                assert_eq!(vus, 3);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }

    #[test]
    fn test_env_stages_ramping() {
        let exec = env_execution(
            None,
            Some(2),
            None,
            None,
            Some(r#"[{"duration":"10s","target":20}]"#),
        );
        match exec {
            Some(ExecutionConfig::RampingVus {
                start_vus, stages, ..
            }) => {
                assert_eq!(start_vus, 2);
                assert_eq!(stages[0].target, 20);
            }
            other => panic!("expected RampingVus, got {other:?}"),
        }
    }

    #[test]
    fn test_env_mode_wins() {
        let exec = env_execution(
            Some("shared-iterations"),
            Some(4),
            Some("10s"),
            Some(99),
            None,
        );
        match exec {
            Some(ExecutionConfig::SharedIterations {
                iterations, vus, ..
            }) => {
                assert_eq!(iterations, 99);
                assert_eq!(vus, 4);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }

    #[test]
    fn test_env_nothing() {
        assert!(env_execution(None, None, None, None, None).is_none());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_load_config_file_roundtrip() {
        let dir = std::env::temp_dir();
        let path = dir.join("tropel_test_config.json");
        std::fs::write(
            &path,
            r#"{
                "input_type": "postman",
                "thresholds": {"http_req_duration": {"expression": "http_req_duration.p95 < 500"}},
                "tls": {"insecure_skip_verify": true}
            }"#,
        )
        .unwrap();
        let cfg = PartialConfig::load_from_file(&path).unwrap();
        assert_eq!(cfg.input_type.as_deref(), Some("postman"));
        assert_eq!(
            cfg.thresholds.get("http_req_duration").unwrap().expression,
            "http_req_duration.p95 < 500"
        );
        assert!(cfg.tls.unwrap().insecure_skip_verify);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn test_config_file_missing_is_error() {
        let err = PartialConfig::load_from_file(Path::new("/nonexistent/nope.json"));
        assert!(err.is_err());
    }
}
