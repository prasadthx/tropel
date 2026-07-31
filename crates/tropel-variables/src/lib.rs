//! # tropel-variables
//!
//! {{var}} resolution with scope precedence and dynamic-variable catalog.

pub mod catalog;
pub mod resolver;

pub use catalog::*;
pub use resolver::*;
