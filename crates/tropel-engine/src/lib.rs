//! # tropel-engine
//!
//! Orchestration facade: wires adapters → executor → protocols/pm → metrics → reporters.

pub mod engine;
pub use engine::*;
