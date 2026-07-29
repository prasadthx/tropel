//! # tropel-variables
//!
//! {{var}} resolution with scope precedence and dynamic-variable catalog.

pub mod resolver;
pub mod catalog;

pub use resolver::*;
pub use catalog::*;
