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

/// Outer tokio runtime worker count.
///
/// VUs run on the thread-per-core `VUWorkerPool` (current-thread runtimes,
/// one per CPU core), so the outer orchestrator only needs minimal workers
/// for scenario coordination and final metric collection. Default 2; override
/// with `TROPEL_TOKIO_WORKERS` (clamped to [1, 128] — more than the core
/// count would only add contention).
fn outer_worker_threads() -> usize {
    std::env::var("TROPEL_TOKIO_WORKERS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(2)
        .clamp(1, 128)
}

fn main() -> tropel_core::Result<()> {
    let workers = outer_worker_threads();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
        .expect("failed to build outer tokio runtime");
    rt.block_on(tropel_engine::cli::run_cli())
}
