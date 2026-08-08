//! Think-time and duration-parsing helpers shared by the VU loop: pacing
//! between iterations (`apply_think_time`), execution-config extraction
//! (`extract_think_time`) and k6-style duration parsing (`parse_duration_str`).
//! Split out of the former `vu_loop.rs` god-file.

use rand::RngExt;
use std::time::Duration;
use tropel_core::config::{ExecutionConfig, ThinkTimeConfig};
use tropel_core::Result;

pub(crate) fn extract_think_time(exec_cfg: &ExecutionConfig) -> ThinkTimeConfig {
    match exec_cfg {
        ExecutionConfig::ConstantVus { think_time, .. } => think_time.clone(),
        ExecutionConfig::RampingVus { think_time, .. } => think_time.clone(),
        ExecutionConfig::ConstantArrivalRate { think_time, .. } => think_time.clone(),
        ExecutionConfig::SharedIterations { think_time, .. } => think_time.clone(),
        ExecutionConfig::PerVUIterations { think_time, .. } => think_time.clone(),
        ExecutionConfig::RampingArrivalRate { think_time, .. } => think_time.clone(),
        ExecutionConfig::ExternallyControlled { think_time, .. } => think_time.clone(),
    }
}

// ── Duration parsing (from old engine.rs) ──

pub(crate) fn parse_duration_str(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() || s == "0" || s == "0s" {
        return Ok(Duration::ZERO);
    }
    tropel_core::parse_duration(s)
}

// ── Think time ──

pub(crate) async fn apply_think_time(config: &ThinkTimeConfig, iter_duration: Option<Duration>) {
    if let Some(pacing_str) = &config.iteration_pacing {
        if let Ok(pacing) = parse_duration_str(pacing_str) {
            if let Some(actual_dur) = iter_duration {
                if actual_dur < pacing {
                    let remaining = pacing - actual_dur;
                    if remaining > Duration::from_millis(1) {
                        tokio::time::sleep(remaining).await;
                    }
                }
            }
            return;
        }
    }

    if let Some(delay_str) = &config.delay {
        if let Ok(delay) = parse_duration_str(delay_str) {
            if delay > Duration::from_millis(1) {
                tokio::time::sleep(delay).await;
                return;
            }
        }
    }

    if let (Some(min_str), Some(max_str)) = (&config.min_delay, &config.max_delay) {
        if let (Ok(min), Ok(max)) = (parse_duration_str(min_str), parse_duration_str(max_str)) {
            if max > Duration::ZERO && max > min {
                let range_ms = (max - min).as_millis() as u64;
                let rand_ms = rand::rng().random_range(0..=range_ms);
                tokio::time::sleep(min + Duration::from_millis(rand_ms)).await;
            }
        }
    }
}
