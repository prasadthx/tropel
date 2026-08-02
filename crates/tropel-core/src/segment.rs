//! # Execution segments
//!
//! Deterministic workload partitioning across N cooperating nodes — the
//! primitive for horizontal scale (k6's `executionSegment` /
//! `executionSegmentSequence` options).
//!
//! A segment is a half-open interval `[from, to)` of the unit workload.
//! Node `i` runs the fraction of the workload that falls inside its
//! segment: VU counts, arrival rates, and iteration budgets are scaled by
//! the segment's length, deterministically — every node computes its own
//! share from the same inputs, with no coordination required.

use crate::config::{ArrivalRateStage, ExecutionConfig, Stage};
use crate::{Result, TropelError};

/// A deterministic workload partition: `[from, to)` of the unit interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExecutionSegment {
    from: f64,
    to: f64,
}

impl ExecutionSegment {
    /// Create a segment from raw fractions, validating `0 <= from < to <= 1`.
    pub fn new(from: f64, to: f64) -> Result<Self> {
        if !(0.0..=1.0).contains(&from) || !(0.0..=1.0).contains(&to) {
            return Err(TropelError::Config(format!(
                "execution segment bounds must be within [0, 1]: {from}..{to}"
            )));
        }
        if from >= to {
            return Err(TropelError::Config(format!(
                "execution segment must be non-empty (from < to): {from}..{to}"
            )));
        }
        Ok(Self { from, to })
    }

    /// Parse a single fraction like `"1/3"`, `"0.5"`, or `"0"`.
    fn parse_fraction(s: &str) -> Result<f64> {
        let s = s.trim();
        if s.is_empty() {
            return Err(TropelError::Config("empty execution segment fraction".into()));
        }
        if let Some((num, den)) = s.split_once('/') {
            let n: f64 = num
                .trim()
                .parse()
                .map_err(|_| TropelError::Config(format!("invalid segment numerator '{num}'")))?;
            let d: f64 = den
                .trim()
                .parse()
                .map_err(|_| TropelError::Config(format!("invalid segment denominator '{den}'")))?;
            if d == 0.0 {
                return Err(TropelError::Config("division by zero in segment".into()));
            }
            Ok(n / d)
        } else {
            s.parse::<f64>()
                .map_err(|_| TropelError::Config(format!("invalid segment fraction '{s}'")))
        }
    }

    /// Parse a segment spec like `"0:1/3"` (or `"1/3:2/3"`, `"0:1"`).
    /// The optional `sequence` (e.g. `"0,1/3,2/3,1"`) is validated when
    /// provided: the segment must be one of the consecutive pairs formed by
    /// the (sorted) sequence boundaries, and the sequence must start at 0 and
    /// end at 1.
    pub fn parse(segment: &str, sequence: Option<&str>) -> Result<Self> {
        let (from_s, to_s) = segment
            .split_once(':')
            .ok_or_else(|| TropelError::Config(format!(
                "invalid execution segment '{segment}' — expected 'from:to' e.g. '0:1/3'"
            )))?;
        let from = Self::parse_fraction(from_s)?;
        let to = Self::parse_fraction(to_s)?;
        let seg = Self::new(from, to)?;

        if let Some(seq) = sequence {
            let bounds = Self::parse_sequence(seq)?;
            // The segment's bounds must be consecutive entries of the sequence.
            let from_idx = bounds.iter().position(|b| (*b - from).abs() < 1e-9);
            let to_idx = bounds.iter().position(|b| (*b - to).abs() < 1e-9);
            match (from_idx, to_idx) {
                (Some(i), Some(j)) if j == i + 1 => Ok(seg),
                (Some(i), Some(_)) => Err(TropelError::Config(format!(
                    "execution segment {from}..{to} skips sequence boundary '{}' — segments must be consecutive pairs of the sequence '{seq}'",
                    bounds[i + 1]
                ))),
                _ => Err(TropelError::Config(format!(
                    "execution segment bounds {from}..{to} not found in sequence '{seq}'"
                ))),
            }
        } else {
            Ok(seg)
        }
    }

    /// Parse and validate a sequence spec like `"0,1/3,2/3,1"`.
    /// Rules: non-empty, strictly ascending, starts at 0, ends at 1.
    pub fn parse_sequence(sequence: &str) -> Result<Vec<f64>> {
        let parts: Vec<&str> = sequence.split(',').map(str::trim).collect();
        if parts.len() < 2 {
            return Err(TropelError::Config(format!(
                "execution segment sequence needs at least 2 boundaries: '{sequence}'"
            )));
        }
        let mut bounds = Vec::with_capacity(parts.len());
        for p in parts {
            bounds.push(Self::parse_fraction(p)?);
        }
        if (bounds[0]).abs() > 1e-9 {
            return Err(TropelError::Config(format!(
                "execution segment sequence must start at 0: '{sequence}'"
            )));
        }
        if (bounds[bounds.len() - 1] - 1.0).abs() > 1e-9 {
            return Err(TropelError::Config(format!(
                "execution segment sequence must end at 1: '{sequence}'"
            )));
        }
        for w in bounds.windows(2) {
            if w[1] <= w[0] {
                return Err(TropelError::Config(format!(
                    "execution segment sequence must be strictly ascending: '{sequence}'"
                )));
            }
        }
        Ok(bounds)
    }

    /// The segment's lower bound.
    pub fn from(&self) -> f64 {
        self.from
    }

    /// The segment's upper bound.
    pub fn to(&self) -> f64 {
        self.to
    }

    /// The fraction of the workload this segment covers (`to - from`).
    pub fn fraction(&self) -> f64 {
        self.to - self.from
    }

    /// Scale a VU count deterministically: at least 1 VU when the segment
    /// covers a nonzero share and the workload is nonzero (a node must have
    /// at least one worker to run anything).
    pub fn scale_vus(&self, vus: u32) -> u32 {
        if vus == 0 {
            return 0;
        }
        let scaled = (vus as f64 * self.fraction()).floor() as u32;
        scaled.max(1).min(vus)
    }

    /// Scale an iteration budget deterministically (floor). A nonzero budget
    /// on a nonzero segment stays at least 1.
    pub fn scale_iterations(&self, iterations: u64) -> u64 {
        if iterations == 0 {
            return 0;
        }
        let scaled = (iterations as f64 * self.fraction()).floor() as u64;
        scaled.max(1).min(iterations)
    }

    /// Scale a rate (iterations/sec) by the segment's fraction.
    pub fn scale_rate(&self, rate: f64) -> f64 {
        rate * self.fraction()
    }

    /// Apply the segment to an execution config, returning a scaled copy
    /// that runs only this node's share of the workload. All scaling is
    /// deterministic — each node derives the same result from the same
    /// segment spec, so N nodes partition the workload with no coordination.
    pub fn apply(&self, exec: &ExecutionConfig) -> ExecutionConfig {
        match exec {
            ExecutionConfig::ConstantVus {
                vus,
                duration,
                graceful_stop,
                think_time,
            } => ExecutionConfig::ConstantVus {
                vus: self.scale_vus(*vus),
                duration: duration.clone(),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::RampingVus {
                stages,
                start_vus,
                graceful_ramp_down,
                graceful_stop,
                think_time,
            } => ExecutionConfig::RampingVus {
                stages: stages
                    .iter()
                    .map(|s| Stage {
                        duration: s.duration.clone(),
                        target: self.scale_vus(s.target),
                    })
                    .collect(),
                start_vus: self.scale_vus(*start_vus),
                graceful_ramp_down: graceful_ramp_down.clone(),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::ConstantArrivalRate {
                rate,
                time_unit,
                duration,
                pre_alloc_vus,
                max_vus,
                graceful_stop,
                think_time,
            } => ExecutionConfig::ConstantArrivalRate {
                rate: self.scale_rate(*rate),
                time_unit: time_unit.clone(),
                duration: duration.clone(),
                pre_alloc_vus: self.scale_vus(*pre_alloc_vus),
                max_vus: self.scale_vus(*max_vus),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::SharedIterations {
                iterations,
                max_duration,
                vus,
                graceful_stop,
                think_time,
            } => ExecutionConfig::SharedIterations {
                iterations: self.scale_iterations(*iterations),
                max_duration: max_duration.clone(),
                vus: self.scale_vus(*vus),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::RampingArrivalRate {
                start_rate,
                stages,
                time_unit,
                pre_alloc_vus,
                max_vus,
                graceful_stop,
                think_time,
            } => ExecutionConfig::RampingArrivalRate {
                start_rate: self.scale_rate(*start_rate),
                stages: stages
                    .iter()
                    .map(|s| ArrivalRateStage {
                        duration: s.duration.clone(),
                        target: self.scale_rate(s.target),
                    })
                    .collect(),
                time_unit: time_unit.clone(),
                pre_alloc_vus: self.scale_vus(*pre_alloc_vus),
                max_vus: self.scale_vus(*max_vus),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::PerVUIterations {
                vus,
                iterations,
                max_duration,
                graceful_stop,
                think_time,
            } => ExecutionConfig::PerVUIterations {
                vus: self.scale_vus(*vus),
                iterations: self.scale_iterations(*iterations),
                max_duration: max_duration.clone(),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
            ExecutionConfig::ExternallyControlled {
                vus,
                max_vus,
                duration,
                graceful_stop,
                think_time,
            } => ExecutionConfig::ExternallyControlled {
                vus: self.scale_vus(*vus),
                max_vus: self.scale_vus(*max_vus),
                duration: duration.clone(),
                graceful_stop: graceful_stop.clone(),
                think_time: think_time.clone(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fraction_forms() {
        assert_eq!(
            ExecutionSegment::parse_fraction("1/3").unwrap(),
            1.0 / 3.0
        );
        assert_eq!(ExecutionSegment::parse_fraction("0.5").unwrap(), 0.5);
        assert_eq!(ExecutionSegment::parse_fraction("0").unwrap(), 0.0);
        assert_eq!(ExecutionSegment::parse_fraction("2/3").unwrap(), 2.0 / 3.0);
        assert!(ExecutionSegment::parse_fraction("x/3").is_err());
        assert!(ExecutionSegment::parse_fraction("1/0").is_err());
    }

    #[test]
    fn parses_segment_spec() {
        let seg = ExecutionSegment::parse("0:1/3", None).unwrap();
        assert!((seg.from() - 0.0).abs() < 1e-9);
        assert!((seg.to() - 1.0 / 3.0).abs() < 1e-9);
        assert!((seg.fraction() - 1.0 / 3.0).abs() < 1e-9);

        let seg = ExecutionSegment::parse("1/3:2/3", None).unwrap();
        assert!((seg.from() - 1.0 / 3.0).abs() < 1e-9);
        assert!((seg.to() - 2.0 / 3.0).abs() < 1e-9);

        // Full workload
        let seg = ExecutionSegment::parse("0:1", None).unwrap();
        assert_eq!(seg.fraction(), 1.0);

        // Invalid: from >= to, out of range, malformed
        assert!(ExecutionSegment::parse("1/3:1/3", None).is_err());
        assert!(ExecutionSegment::parse("1:0", None).is_err());
        assert!(ExecutionSegment::parse("1/3", None).is_err());
        assert!(ExecutionSegment::parse("-1:1", None).is_err());
        assert!(ExecutionSegment::parse("0:2", None).is_err());
    }

    #[test]
    fn validates_against_sequence() {
        let seq = "0,1/3,2/3,1";
        // Consecutive pair → OK
        assert!(ExecutionSegment::parse("0:1/3", Some(seq)).is_ok());
        assert!(ExecutionSegment::parse("1/3:2/3", Some(seq)).is_ok());
        assert!(ExecutionSegment::parse("2/3:1", Some(seq)).is_ok());
        // Non-consecutive pair → Err
        assert!(ExecutionSegment::parse("0:2/3", Some(seq)).is_err());
        // Bound not in sequence → Err
        assert!(ExecutionSegment::parse("0:0.25", Some(seq)).is_err());
    }

    #[test]
    fn validates_sequence_shape() {
        assert!(ExecutionSegment::parse_sequence("0,1/3,2/3,1").is_ok());
        assert!(ExecutionSegment::parse_sequence("0,1").is_ok());
        // Must start at 0
        assert!(ExecutionSegment::parse_sequence("1/3,1").is_err());
        // Must end at 1
        assert!(ExecutionSegment::parse_sequence("0,1/3").is_err());
        // Must be ascending
        assert!(ExecutionSegment::parse_sequence("0,2/3,1/3,1").is_err());
        // At least 2 boundaries
        assert!(ExecutionSegment::parse_sequence("0").is_err());
    }

    #[test]
    fn scales_vus_and_iterations() {
        let third = ExecutionSegment::parse("0:1/3", None).unwrap();
        // 9 VUs × 1/3 = 3
        assert_eq!(third.scale_vus(9), 3);
        // 90 iterations × 1/3 = 30
        assert_eq!(third.scale_iterations(90), 30);
        // Small values clamp to 1 (a node must have a worker)
        assert_eq!(third.scale_vus(1), 1);
        assert_eq!(third.scale_iterations(1), 1);
        // Zero stays zero
        assert_eq!(third.scale_vus(0), 0);
        // Rate is fractional, not floored
        assert!((third.scale_rate(6.0) - 2.0).abs() < 1e-9);

        let full = ExecutionSegment::parse("0:1", None).unwrap();
        assert_eq!(full.scale_vus(9), 9);
        assert_eq!(full.scale_iterations(90), 90);
    }

    #[test]
    fn apply_scales_execution_config() {
        let third = ExecutionSegment::parse("0:1/3", None).unwrap();

        let exec = ExecutionConfig::ConstantVus {
            vus: 9,
            duration: "30s".into(),
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::ConstantVus { vus, .. } => assert_eq!(vus, 3),
            _ => panic!("wrong variant"),
        }

        let exec = ExecutionConfig::SharedIterations {
            iterations: 90,
            max_duration: Some("60s".into()),
            vus: 9,
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::SharedIterations {
                iterations, vus, ..
            } => {
                assert_eq!(iterations, 30);
                assert_eq!(vus, 3);
            }
            _ => panic!("wrong variant"),
        }

        let exec = ExecutionConfig::RampingVus {
            stages: vec![
                Stage { duration: "1m".into(), target: 10 },
                Stage { duration: "1m".into(), target: 20 },
            ],
            start_vus: 3,
            graceful_ramp_down: None,
            graceful_stop: None,
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::RampingVus { stages, start_vus, .. } => {
                assert_eq!(start_vus, 1);
                assert_eq!(stages[0].target, 3); // 10/3 = 3.33 → 3
                assert_eq!(stages[1].target, 6); // 20/3 = 6.67 → 6
            }
            _ => panic!("wrong variant"),
        }

        let exec = ExecutionConfig::ConstantArrivalRate {
            rate: 9.0,
            time_unit: "1s".into(),
            duration: "30s".into(),
            pre_alloc_vus: 3,
            max_vus: 9,
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::ConstantArrivalRate {
                rate,
                pre_alloc_vus,
                max_vus,
                ..
            } => {
                assert!((rate - 3.0).abs() < 1e-9); // 9/3 = 3
                assert_eq!(pre_alloc_vus, 1);
                assert_eq!(max_vus, 3);
            }
            _ => panic!("wrong variant"),
        }

        let exec = ExecutionConfig::RampingArrivalRate {
            start_rate: 3.0,
            stages: vec![ArrivalRateStage {
                duration: "1m".into(),
                target: 9.0,
            }],
            time_unit: "1s".into(),
            pre_alloc_vus: 3,
            max_vus: 9,
            graceful_stop: None,
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::RampingArrivalRate {
                start_rate,
                stages,
                pre_alloc_vus,
                max_vus,
                ..
            } => {
                assert!((start_rate - 1.0).abs() < 1e-9); // 3/3 = 1
                assert!((stages[0].target - 3.0).abs() < 1e-9); // 9/3 = 3
                assert_eq!(pre_alloc_vus, 1);
                assert_eq!(max_vus, 3);
            }
            _ => panic!("wrong variant"),
        }

        let exec = ExecutionConfig::PerVUIterations {
            vus: 9,
            iterations: 90,
            max_duration: Some("60s".into()),
            graceful_stop: Some("30s".into()),
            think_time: Default::default(),
        };
        let scaled = third.apply(&exec);
        match scaled {
            ExecutionConfig::PerVUIterations {
                vus, iterations, ..
            } => {
                assert_eq!(vus, 3);
                assert_eq!(iterations, 30);
            }
            _ => panic!("wrong variant"),
        }
    }
}
