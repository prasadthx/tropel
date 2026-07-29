//! # tropel-ext
//!
//! The Extension SDK: extension-point traits + the registry.
//! Everything pluggable depends on this crate.

pub mod registry;
pub mod traits;

pub use registry::*;
pub use traits::*;
