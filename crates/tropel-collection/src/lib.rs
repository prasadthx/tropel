//! # tropel-collection
//!
//! Postman Collection v2.1 (and v2.0) model + parser.
//! Deserializes Collection JSON into typed models and converts to
//! protocol-agnostic `Scenario`.

pub mod error;
pub mod model;
pub mod parser;

pub use error::*;
pub use model::*;
pub use parser::*;
