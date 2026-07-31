//! # tropel-engine
//!
//! Orchestration facade: wires adapters → executor → protocols/pm → metrics → reporters.

pub mod builtins;
pub mod cli;
pub mod engine;
pub mod worker;
pub use engine::*;
