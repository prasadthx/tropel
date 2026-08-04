//! # tropel-engine
//!
//! Orchestration facade: wires adapters → executor → protocols/pm → metrics → reporters.

pub mod builtins;
pub mod cli;
pub mod config_file;
pub mod control_api;
pub mod engine;
pub mod input;
pub mod js_bootstrap;
pub mod outputs;
pub mod summary;
pub mod vu_loop;
pub mod worker;
pub use engine::*;
