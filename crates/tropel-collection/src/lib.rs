//! # tropel-collection
//!
//! Postman Collection v2.1 (and v2.0) model + parser.
//! Deserializes Collection JSON into typed models and converts to
//! protocol-agnostic `Scenario`.

pub mod model;
pub mod parser;
pub mod error;

pub use model::*;
pub use parser::*;
pub use error::*;
