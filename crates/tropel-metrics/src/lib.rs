//! # tropel-metrics
//!
//! Sample ingestion, hdr histogram aggregation, tag/label aggregation,
//! and threshold evaluation.

pub mod collector;
pub mod histogram;
pub mod thresholds;

pub use collector::*;
pub use histogram::*;
pub use thresholds::*;

/// Registry of custom metrics explicitly declared as time metrics.
///
/// k6's `new Trend(name, isTime)` (and the older `metric(name, type,
/// isTime)`) let a script mark a custom metric as containing time so the
/// JSON output stamps `contains: "time"` and summaries render it in ms.
/// The k6 driver registers names here when `isTime` is true; reporters
/// (json-stream's `is_time_metric`, the stdout ms unit) consult it in
/// addition to the name-suffix heuristic, so a custom `my_timer` renders
/// as time even though its name doesn't end in `_duration`/`_time`.
pub mod time_metrics {
    use std::collections::HashSet;
    use std::sync::{Mutex, OnceLock};

    static TIME_METRICS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();

    /// Register a custom metric name as a time metric (`isTime: true`).
    pub fn register(name: &str) {
        TIME_METRICS
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(name.to_string());
    }

    /// Whether the metric was explicitly declared as a time metric.
    pub fn is_time_metric(name: &str) -> bool {
        TIME_METRICS
            .get()
            .is_some_and(|m| m.lock().unwrap().contains(name))
    }
}
