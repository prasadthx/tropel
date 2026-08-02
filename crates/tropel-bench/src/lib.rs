//! # tropel-bench
//!
//! Criterion benchmark suite for Tropel (see `benches/perf.rs`).
//!
//! This crate intentionally has no library code — it exists so the benchmark
//! targets have a package to live in and link against the workspace crates.
//! Run with:
//!
//! ```text
//! cargo bench -p tropel-bench --bench perf
//! ```
//!
//! On disk-constrained machines, add `--profile dev` to reuse the already-built
//! debug rlibs instead of triggering a full release rebuild of the dep tree.
//!
//! Benchmarks: context bootstrap, per-iteration overhead (compile-once vs
//! cold eval), native-vs-JS bridge speedup, VUWorkerPool dispatch (VUs/sec),
//! and memory-per-VU (RSS growth per live JS context).
