//! # Tropel CLI
//!
//! The main entry point for the Tropel load testing tool.
//! Delegates all logic to `tropel_engine::cli::run_cli()`.
//! This ensures that custom binaries built with `tropel build` have
//! identical CLI behavior.

// Select the global allocator at compile time via feature flags.
#[cfg(feature = "alloc-mimalloc")]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "alloc-jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// Outer tokio runtime: 2 workers. VUs run on the thread-per-core VUWorkerPool
// (current-thread runtimes, one per CPU core), so the orchestrator only needs
// minimal worker threads for scenario coordination and final metric collection.
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> tropel_core::Result<()> {
    tropel_engine::cli::run_cli().await
}
