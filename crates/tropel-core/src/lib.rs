//! # tropel-core
//!
//! Protocol-agnostic domain types shared across all Tropel crates.
//! This crate is a leaf — it depends on nothing in the workspace.

pub mod error;
pub mod types;
pub mod config;
pub mod scenario;

pub use error::*;
pub use types::*;
pub use config::*;
pub use scenario::*;
