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
