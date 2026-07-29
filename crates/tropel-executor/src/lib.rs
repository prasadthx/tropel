//! # tropel-executor
//!
//! VU scheduler (constant/ramping/arrival-rate/shared-iters) and the
//! per-VU iteration loop with setNextRequest flow control.

pub mod scheduler;
pub mod runner;

pub use scheduler::*;
pub use runner::*;
