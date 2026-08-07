//! Shared clocks.
//!
//! - [`monotonic_now_nanos`] — a process-monotonic nanosecond clock used for
//!   interrupt deadlines. Immune to NTP steps: a wall-clock jump must never
//!   kill a running script (backlog: "interrupt deadline uses `SystemTime`").
//! - [`monotonic_wall_now`] — a wall-clock-aligned `SystemTime` that is
//!   monotonic by construction (anchored to the real clock on first use, then
//!   advanced via the monotonic clock). Keeps k6-style outputs on real time
//!   while guaranteeing sample timestamps never go backwards.
//!
//! Both clocks share one anchor so their epochs are consistent.

use std::sync::OnceLock;
use std::time::{Instant, SystemTime};

/// Anchor: the monotonic instant and the wall-clock time observed at first
/// use. Everything else is derived from `Instant::elapsed()`, so no
/// wall-clock read ever happens after startup.
static BASE: OnceLock<(Instant, SystemTime)> = OnceLock::new();

fn base() -> (Instant, SystemTime) {
    *BASE.get_or_init(|| (Instant::now(), SystemTime::now()))
}

/// Monotonic nanoseconds since the clock's first use.
///
/// Safe for deadline arithmetic (the interrupt handler compares against this):
/// a system clock step cannot move it backwards or forwards.
pub fn monotonic_now_nanos() -> u64 {
    base().0.elapsed().as_nanos() as u64
}

/// Wall-clock-aligned, monotonic `SystemTime`.
///
/// Equal to the real wall clock at the moment of first use, then advances at
/// real time via the monotonic clock — so a backward NTP step never yields a
/// timestamp earlier than a previous one, and a forward step never jumps
/// ahead of elapsed real time. k6-compatible outputs (`json_stream`,
/// `influxdb`, `otlp`) stay on real time; sample ordering stays monotonic.
pub fn monotonic_wall_now() -> SystemTime {
    let (instant, wall) = base();
    wall + instant.elapsed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wall_now_is_monotonic() {
        let a = monotonic_wall_now();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = monotonic_wall_now();
        assert!(b >= a, "timestamps must never go backwards");
    }

    #[test]
    fn now_nanos_is_monotonic() {
        let a = monotonic_now_nanos();
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = monotonic_now_nanos();
        assert!(b >= a, "deadline clock must never go backwards");
    }

    #[test]
    fn wall_now_is_aligned_to_real_clock() {
        // Anchored at first use, so it must be within a few minutes of the
        // real wall clock (not relative-to-process-start).
        let diff = SystemTime::now()
            .duration_since(monotonic_wall_now())
            .unwrap_or_default();
        assert!(
            diff < std::time::Duration::from_secs(300),
            "wall_now drifted from the real clock by {diff:?}"
        );
    }
}
