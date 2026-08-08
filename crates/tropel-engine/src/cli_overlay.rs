//! Config-overlay merging for the CLI: applies a merged `PartialConfig`
//! (config file / `K6_*` env) onto the CLI-built `JobConfig` with the
//! precedence CLI flags > config file > `K6_*` env > defaults. Split out of
//! the former `cli.rs` god-file.

use crate::cli_commands::load_data_file;
use crate::config_file::PartialConfig;
use tropel_core::config::*;

/// Apply the merged overlay onto the CLI-built `JobConfig`.
///
/// Precedence: explicit CLI flags > config file > K6_* env > defaults.
/// `cli_reporters` is the CLI `--reporter` value (default `["stdout"]` —
/// treated as "not explicitly set" so a config file or `K6_REPORTER` can
/// replace it). `cli_load_profile_explicit` is true when the user passed any
/// load-profile flag (`-u`/`-d`/`-m`/`--stages`/`--iterations`) — in that
/// case the overlay's execution is ignored (CLI wins).
pub(crate) fn apply_overlay(
    config: &mut JobConfig,
    overlay: PartialConfig,
    cli_reporters: &[String],
    cli_insecure: bool,
    cli_load_profile_explicit: bool,
    iteration_data_is_empty: bool,
) {
    // input_type: CLI --format wins; else overlay.
    if config.input_type.is_none() {
        config.input_type = overlay.input_type.clone();
    }
    // Load profile: only when the user passed no explicit load flags. This
    // mirrors the k6 `export const options` behavior — a config file or
    // K6_* env declaring an execution marks the profile explicit so scripts
    // don't silently override it.
    if !cli_load_profile_explicit {
        if let Some(exec) = overlay.execution {
            tracing::info!("Using execution from config file / K6_* env: {:?}", exec);
            config.execution = exec;
            config.execution_explicit = true;
        }
    }
    // Execution segments: CLI flags win; overlay fills gaps. Applied later
    // by the engine to scale each scenario's workload deterministically.
    if config.execution_segment.is_none() {
        config.execution_segment = overlay.execution_segment.clone();
    }
    if config.execution_segment_sequence.is_none() {
        config.execution_segment_sequence = overlay.execution_segment_sequence.clone();
    }
    // Control API port: CLI flag wins; overlay fills the gap.
    if config.control_port.is_none() {
        config.control_port = overlay.control_port;
    }
    // Env: overlay vars fill in, CLI -e already present wins (insert only
    // keys the overlay has that the CLI env doesn't).
    for (k, v) in &overlay.env {
        config.env.entry(k.clone()).or_insert_with(|| v.clone());
    }
    // Thresholds: overlay adds; CLI keys (threshold_N) don't collide.
    for (k, v) in &overlay.thresholds {
        config
            .thresholds
            .entry(k.clone())
            .or_insert_with(|| v.clone());
    }
    // Output: CLI flags win; overlay fills reporters/file/urls. The CLI's
    // --reporter has a "stdout" default on a Vec, so `is_empty()` would never
    // fire — instead treat the default ["stdout"] as "not explicitly set" so
    // a config file or K6_REPORTER can replace it.
    if let Some(out) = &overlay.output {
        let cli_reporters_default = cli_reporters.len() == 1 && cli_reporters[0] == "stdout";
        if cli_reporters_default && !out.reporters.is_empty() {
            config.output.reporters = out.reporters.clone();
        }
        if config.output.output_file.is_none() {
            config.output.output_file = out.output_file.clone();
        }
        if config.output.prometheus_remote_write_url.is_none() {
            config.output.prometheus_remote_write_url = out.prometheus_remote_write_url.clone();
        }
        if config.output.otlp_endpoint.is_none() {
            config.output.otlp_endpoint = out.otlp_endpoint.clone();
        }
        if config.output.summary_export.is_none() {
            config.output.summary_export = out.summary_export.clone();
        }
        if config.output.json_stream.is_none() {
            config.output.json_stream = out.json_stream.clone();
        }
        if config.output.statsd_addr.is_none() {
            config.output.statsd_addr = out.statsd_addr.clone();
        }
        if config.output.influxdb_addr.is_none() {
            config.output.influxdb_addr = out.influxdb_addr.clone();
        }
        // The CLI never sets summary/trends (both default false), so the
        // overlay's explicit values apply directly.
        config.output.summary = out.summary;
        config.output.trends = out.trends;
    }
    // Named scenarios from the overlay (CLI has no scenarios flag, so the
    // overlay always wins here).
    if let Some(scenarios) = &overlay.scenarios {
        if !scenarios.is_empty() {
            config.scenarios = scenarios.clone();
        }
    }
    // HTTP: overlay applies only when the user didn't set flags (the CLI
    // exposes no direct http flags, so overlay always wins here).
    if let Some(http) = &overlay.http {
        config.http = http.clone();
    }
    // TLS: CLI --insecure wins for skip-verify; overlay fills the rest.
    if let Some(tls) = &overlay.tls {
        let mut merged = tls.clone();
        if cli_insecure {
            merged.insecure_skip_verify = true;
        }
        config.tls = merged;
    }
    // Globals / collection vars / iteration data / extensions.
    if !overlay.globals.is_empty() {
        config.globals.extend(overlay.globals.clone());
    }
    if !overlay.collection_vars.is_empty() {
        config
            .collection_vars
            .extend(overlay.collection_vars.clone());
    }
    // Data file: `--data-file` (CLI) already loaded into iteration_data; a
    // config-file data_file is run through the same loader so it isn't dead.
    // When the overlay sets BOTH a data_file path and inline iteration_data,
    // inline data wins — warn so the surprise is explicit in every path.
    if overlay.data_file.is_some() && !overlay.iteration_data.is_empty() {
        tracing::warn!(
            "Config file sets both data_file and iteration_data — inline iteration_data wins"
        );
    } else if iteration_data_is_empty {
        if let Some(data_path) = overlay.data_file {
            match load_data_file(&std::path::PathBuf::from(&data_path)) {
                Ok(data) => config.iteration_data = data,
                Err(e) => {
                    tracing::warn!(
                        "Failed to load config-file data_file '{}': {}",
                        data_path,
                        e
                    );
                }
            }
        }
    }
    if !overlay.iteration_data.is_empty() {
        config.iteration_data = overlay.iteration_data.clone();
    }
    if !overlay.extensions.is_empty() {
        config.extensions.extend(overlay.extensions.clone());
    }
}

/// Merge two partial configs: `file` wins over `base` (env) on a per-field
/// basis. Explicit CLI flags are applied AFTER this in `run_command`, so the
/// final precedence is: CLI flags > config file > K6_* env > defaults.
pub(crate) fn merge_partial(base: PartialConfig, file: PartialConfig) -> PartialConfig {
    PartialConfig {
        input_type: file.input_type.or(base.input_type),
        execution: file.execution.or(base.execution),
        scenarios: file.scenarios.or(base.scenarios),
        env: base.env.into_iter().chain(file.env).collect(),
        globals: base.globals.into_iter().chain(file.globals).collect(),
        collection_vars: base
            .collection_vars
            .into_iter()
            .chain(file.collection_vars)
            .collect(),
        data_file: file.data_file.or(base.data_file),
        iteration_data: if file.iteration_data.is_empty() {
            base.iteration_data
        } else {
            file.iteration_data
        },
        thresholds: base.thresholds.into_iter().chain(file.thresholds).collect(),
        output: file.output.or(base.output),
        http: file.http.or(base.http),
        tls: file.tls.or(base.tls),
        extensions: base.extensions.into_iter().chain(file.extensions).collect(),
        execution_segment: file.execution_segment.or(base.execution_segment),
        execution_segment_sequence: file
            .execution_segment_sequence
            .or(base.execution_segment_sequence),
        control_port: file.control_port.or(base.control_port),
    }
}
#[cfg(test)]
mod overlay_tests {
    use super::*;

    fn base_config() -> JobConfig {
        JobConfig {
            input: "test.js".to_string(),
            execution: ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "30s".to_string(),
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_overlay_execution_when_no_cli_load_flags() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            execution: Some(ExecutionConfig::SharedIterations {
                iterations: 50,
                max_duration: None,
                vus: 5,
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        apply_overlay(
            &mut cfg,
            overlay,
            &["stdout".to_string()],
            false,
            false,
            true,
        );
        match cfg.execution {
            ExecutionConfig::SharedIterations {
                iterations, vus, ..
            } => {
                assert_eq!(iterations, 50);
                assert_eq!(vus, 5);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
        assert!(cfg.execution_explicit);
    }

    #[test]
    fn test_overlay_execution_ignored_when_cli_flags_explicit() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            execution: Some(ExecutionConfig::SharedIterations {
                iterations: 999,
                max_duration: None,
                vus: 5,
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        apply_overlay(
            &mut cfg,
            overlay,
            &["stdout".to_string()],
            false,
            true,
            true,
        );
        // CLI load profile explicit → overlay execution ignored.
        match cfg.execution {
            ExecutionConfig::ConstantVus { vus, .. } => assert_eq!(vus, 1),
            other => panic!("expected ConstantVus, got {other:?}"),
        }
        assert!(!cfg.execution_explicit);
    }

    #[test]
    fn test_overlay_reporters_replace_default_stdout() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            output: Some(OutputConfig {
                reporters: vec!["json".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(
            &mut cfg,
            overlay,
            &["stdout".to_string()],
            false,
            false,
            true,
        );
        assert_eq!(cfg.output.reporters, vec!["json".to_string()]);
    }

    #[test]
    fn test_overlay_reporters_do_not_override_cli_flag() {
        let mut cfg = base_config();
        cfg.output.reporters = vec!["csv".to_string()];
        let overlay = PartialConfig {
            output: Some(OutputConfig {
                reporters: vec!["json".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(&mut cfg, overlay, &["csv".to_string()], false, false, true);
        assert_eq!(cfg.output.reporters, vec!["csv".to_string()]);
    }

    #[test]
    fn test_overlay_env_thresholds_fill_not_override() {
        let mut cfg = base_config();
        cfg.env.insert("CLI_ONLY".to_string(), "1".to_string());
        let overlay = PartialConfig {
            env: [
                ("CLI_ONLY".to_string(), "overridden".to_string()),
                ("NEW".to_string(), "2".to_string()),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        apply_overlay(
            &mut cfg,
            overlay,
            &["stdout".to_string()],
            false,
            false,
            true,
        );
        assert_eq!(cfg.env.get("CLI_ONLY").map(|s| s.as_str()), Some("1"));
        assert_eq!(cfg.env.get("NEW").map(|s| s.as_str()), Some("2"));
    }

    #[test]
    fn test_overlay_tls_insecure_cli_wins() {
        let mut cfg = base_config();
        let overlay = PartialConfig {
            tls: Some(TlsConfig {
                insecure_skip_verify: false,
                ..Default::default()
            }),
            ..Default::default()
        };
        apply_overlay(
            &mut cfg,
            overlay,
            &["stdout".to_string()],
            true,
            false,
            true,
        );
        assert!(cfg.tls.insecure_skip_verify);
    }

    #[test]
    fn test_merge_partial_file_wins_over_env() {
        let base = PartialConfig {
            env: [("K".to_string(), "base".to_string())]
                .into_iter()
                .collect(),
            execution: Some(ExecutionConfig::ConstantVus {
                vus: 1,
                duration: "10s".to_string(),
                graceful_stop: None,
                think_time: ThinkTimeConfig::default(),
            }),
            ..Default::default()
        };
        let file = PartialConfig {
            env: [("K".to_string(), "file".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };
        let merged = merge_partial(base, file);
        assert_eq!(merged.env.get("K").map(|s| s.as_str()), Some("file"));
        // File doesn't set execution → base (env) execution retained.
        assert!(merged.execution.is_some());
    }
}
