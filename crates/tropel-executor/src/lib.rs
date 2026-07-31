//! # tropel-executor
//!
//! VU scheduler (constant/ramping/arrival-rate/shared-iters) and the
//! per-VU iteration loop with setNextRequest flow control.

pub mod runner;
pub mod scheduler;

pub use runner::*;
pub use scheduler::*;
