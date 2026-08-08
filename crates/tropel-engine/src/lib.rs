//! # tropel-engine
//!
//! Orchestration facade: wires adapters → executor → protocols/pm → metrics → reporters.

pub mod builtins;
pub mod cli;
mod cli_commands;
mod cli_overlay;
mod cli_registry;
pub mod config_file;
pub mod control_api;
pub mod engine;
pub mod input;
pub mod js_bootstrap;
pub mod outputs;
mod pacing;
pub mod summary;
mod vu_loop;
mod vu_sources;
pub mod worker;
pub use engine::*;
