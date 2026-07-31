//! # tropel-http
//!
//! HTTP Protocol implementation: reqwest client, connection pooling,
//! redirects, per-VU cookie jar, and auth signers.

pub mod client;
pub mod auth;
pub mod protocol;
pub mod blocking;

pub use client::*;
pub use auth::*;
pub use protocol::*;
