//! # tropel-core
//!
//! Protocol-agnostic domain types shared across all Tropel crates.
//! This crate is a leaf — it depends on nothing in the workspace.

pub mod config;
pub mod error;
pub mod scenario;
pub mod segment;
pub mod types;

pub use config::*;
pub use error::*;
pub use scenario::*;
pub use segment::*;
pub use types::*;

use std::time::Duration;

/// Parse a human duration string (`"500ms"`, `"30s"`, `"5m"`, `"2h"`, or a
/// bare number of seconds like `"10"` → 10s) into a [`Duration`].
///
/// Canonical implementation, hoisted from duplicated copies in
/// `tropel-http`, `tropel-executor`, `tropel-metrics`, and
/// `tropel-distributed` so duration parsing lives in one place.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(num_str) = s.strip_suffix("ms") {
        let ms: u64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(num_str) = s.strip_suffix('s') {
        let secs: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    } else if let Some(num_str) = s.strip_suffix('m') {
        let mins: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(mins * 60.0))
    } else if let Some(num_str) = s.strip_suffix('h') {
        let hours: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(hours * 3600.0))
    } else {
        // Default to seconds
        let secs: f64 = s
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    }
}
