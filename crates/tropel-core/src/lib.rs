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

/// Parse a human duration string into a [`Duration`].
///
/// Accepts k6-style compound unit strings (`"1m30s"`, `"1h30m"`,
/// `"100us"`, `"1.5ms"`, `"2h30m10s"`), single units (`"500ms"`, `"30s"`,
/// `"5m"`, `"2h"`), and a bare number of seconds (`"10"` → 10s). Supported
/// units: `ns`, `us`/`µs`/`μs`, `ms`, `s`, `m`, `h`.
///
/// NEVER panics: unlike a naive `Duration::from_secs_f64`, malformed or
/// out-of-range input (`"-30s"`, `"nans"`, `"1e400s"`) returns an `Err`
/// instead of aborting the process — a misconfigured duration can't kill a
/// run that is supposed to surface config errors.
///
/// Canonical implementation, hoisted from duplicated copies in
/// `tropel-http`, `tropel-executor`, `tropel-metrics`, and
/// `tropel-distributed` so duration parsing lives in one place.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return Err(TropelError::Config("Invalid duration: (empty)".into()));
    }

    let mut total = Duration::ZERO;
    let mut rest = s;
    // `s` is non-empty (checked above), so the loop runs at least once.
    while !rest.is_empty() {
        // Number part: digits with at most one '.'. Any other char ends it.
        let num_end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '.'))
            .unwrap_or(rest.len());
        if num_end == 0 {
            return Err(TropelError::Config(format!("Invalid duration: '{s}'")));
        }
        let num: f64 = rest[..num_end]
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: '{s}'")))?;
        rest = &rest[num_end..];

        // Unit part: an alphabetic run (including the micro-sign variants).
        let unit_end = rest
            .char_indices()
            .find(|(_, c)| !(c.is_ascii_alphabetic() || *c == 'µ' || *c == 'μ'))
            .map(|(idx, _)| idx)
            .unwrap_or(rest.len());
        let unit = &rest[..unit_end];
        rest = &rest[unit_end..];

        let factor: f64 = match unit {
            "ns" => 1e-9,
            "us" | "µs" | "μs" => 1e-6,
            "ms" => 1e-3,
            "s" => 1.0,
            "m" => 60.0,
            "h" => 3600.0,
            // Bare number → seconds (k6 semantics).
            "" => 1.0,
            _ => return Err(TropelError::Config(format!("Invalid duration: '{s}'"))),
        };

        let secs = num * factor;
        // Reject NaN / ±inf / negative — Duration::from_secs_f64 would panic.
        if !secs.is_finite() || secs < 0.0 {
            return Err(TropelError::Config(format!("Invalid duration: '{s}'")));
        }
        let part = Duration::try_from_secs_f64(secs)
            .map_err(|_| TropelError::Config(format!("Invalid duration: '{s}'")))?;
        total = total
            .checked_add(part)
            .ok_or_else(|| TropelError::Config(format!("Invalid duration: '{s}'")))?;
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dur(s: &str) -> Duration {
        parse_duration(s).expect("duration must parse")
    }

    #[test]
    fn test_single_units() {
        assert_eq!(dur("500ms"), Duration::from_millis(500));
        assert_eq!(dur("30s"), Duration::from_secs(30));
        assert_eq!(dur("5m"), Duration::from_secs(300));
        assert_eq!(dur("2h"), Duration::from_secs(7200));
        assert_eq!(dur("10"), Duration::from_secs(10));
        assert_eq!(dur("1.5s"), Duration::from_millis(1500));
    }

    #[test]
    fn test_compound_units() {
        // k6-documented compound strings that previously returned Err.
        assert_eq!(dur("1m30s"), Duration::from_secs(90));
        assert_eq!(dur("1h30m"), Duration::from_secs(5400));
        assert_eq!(dur("1h2m3s"), Duration::from_secs(3723));
        assert_eq!(dur("100us"), Duration::from_micros(100));
        assert_eq!(dur("1.5ms"), Duration::from_micros(1500));
        assert_eq!(dur("2m30.5s"), Duration::from_millis(150500));
    }

    #[test]
    fn test_micro_sign_variants() {
        assert_eq!(dur("100µs"), Duration::from_micros(100));
        assert_eq!(dur("100μs"), Duration::from_micros(100));
    }

    #[test]
    fn test_whitespace_trimmed() {
        assert_eq!(dur(" 30s "), Duration::from_secs(30));
    }

    #[test]
    fn test_invalid_inputs_error_not_panic() {
        // All of these previously PANICKED inside Duration::from_secs_f64.
        for bad in ["-30s", "nans", "1e400s", "abc", "", "s", "1xs"] {
            assert!(parse_duration(bad).is_err(), "'{bad}' must error, not panic");
        }
    }
}
