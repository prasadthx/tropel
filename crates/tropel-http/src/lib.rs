//! # tropel-http
//!
//! HTTP Protocol implementation: reqwest client, connection pooling,
//! redirects, per-VU cookie jar, and auth signers.

pub mod auth;
pub mod blocking;
pub mod client;
pub mod protocol;

pub use auth::*;
pub use client::*;
pub use protocol::*;
