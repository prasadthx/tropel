//! Blocking HTTP execution for host functions (pm.sendRequest, k6 http.*).
//!
//! The constraint: VUs run on current-thread runtimes that multiplex many VUs
//! (thread-per-core). From inside QuickJS `ctx.with` you must call HTTP
//! **synchronously** and return the result to JS. You therefore **cannot**:
//! - `Runtime::block_on(...)` (any runtime) — panics, the caller thread is
//!   already in a runtime;
//! - `futures::executor::block_on(reqwest_fut)` — deadlocks, the future needs
//!   the caller's blocked reactor.
//!
//! The correct primitive: offload the future to a dedicated I/O runtime on its
//! own threads, and block the caller on a plain `std` channel (a thread park —
//! no tokio runtime is entered on the caller, so no panic; the future runs on
//! the I/O runtime's reactor, so no deadlock).

use std::sync::{mpsc::sync_channel, OnceLock};
use tokio::runtime::{Builder, Runtime};
use tropel_core::{Result, TropelError};

/// A dedicated multi-thread runtime that ONLY drives host I/O futures.
/// It is separate from the per-core VU worker runtimes, so blocking a VU
/// thread on its result never touches (or deadlocks) a VU runtime's reactor.
static IO_RT: OnceLock<Runtime> = OnceLock::new();

/// Default I/O worker count: scale to the host's cores so TLS handshakes,
/// response decode and reactor work aren't capped at an arbitrary 4 threads
/// on a 16-core box. Override with `TROPEL_IO_WORKERS` (clamped to [1, 512];
/// values beyond the core count only add scheduler contention).
fn default_io_workers() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
}

/// Pure sizing logic — separate from env access so tests exercise it without
/// mutating process-global state (which would race under parallel test runs).
fn workers_from_override(override_str: Option<&str>) -> usize {
    override_str
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or_else(default_io_workers)
        .clamp(1, 512)
}

fn io_worker_threads() -> usize {
    workers_from_override(std::env::var("TROPEL_IO_WORKERS").ok().as_deref())
}

fn io_rt() -> &'static Runtime {
    IO_RT.get_or_init(|| {
        Builder::new_multi_thread()
            .enable_all()
            .worker_threads(io_worker_threads())
            .thread_name("tropel-io")
            .build()
            .expect("build tropel-io runtime")
    })
}

/// Run a host-I/O future to completion synchronously.
///
/// Safe to call from inside QuickJS `ctx.with` on a current-thread VU runtime:
/// the caller parks on a plain `std` channel (no tokio runtime is entered on
/// the caller → no "runtime within runtime" panic), while the future runs on
/// the dedicated multi-thread I/O runtime's reactor → no deadlock with the
/// caller's blocked reactor. reqwest's own per-request timeout fires normally.
///
/// This is the **single source of truth** for host functions that do async I/O:
/// never hand-roll a `block_on` at a call site — the bug recurred precisely
/// because the logic was duplicated.
pub fn execute_blocking<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = sync_channel::<Result<T>>(1);
    io_rt().spawn(async move {
        let _ = tx.send(fut.await); // ignore if receiver dropped
    });
    // Plain thread park: NO tokio runtime entered here → no panic; future runs
    // on io_rt's reactor → no deadlock.
    rx.recv()
        .map_err(|_| TropelError::Http("io task dropped".into()))?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workers_from_override_defaults_to_cores() {
        // No override → host core count (no env mutation, deterministic).
        let expected = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        assert_eq!(workers_from_override(None), expected);
        // Unparseable override → same default.
        assert_eq!(workers_from_override(Some("bogus")), expected);
    }

    #[test]
    fn workers_from_override_clamps_bounds() {
        assert_eq!(workers_from_override(Some("999")), 512);
        assert_eq!(workers_from_override(Some("0")), 1);
        assert_eq!(workers_from_override(Some(" 8 ")), 8);
    }

    #[test]
    fn execute_blocking_resolves_future() {
        let result = execute_blocking(async { Ok::<i32, TropelError>(42) }).unwrap();
        assert_eq!(result, 42);
    }

    #[test]
    fn execute_blocking_propagates_error() {
        let result: tropel_core::Result<i32> =
            execute_blocking(async { Err::<i32, _>(TropelError::Http("boom".into())) });
        let err = result.unwrap_err();
        assert_eq!(format!("{}", err), "HTTP error: boom");
    }

    #[test]
    fn execute_blocking_works_from_inside_current_thread_runtime() {
        // Regression test for issue #1: calling the helper from inside a
        // current-thread runtime must NOT panic with "Cannot start a runtime
        // from within a runtime". The helper parks on a std channel instead of
        // entering any tokio runtime on the caller thread.
        //
        // We call it DIRECTLY inside `rt.block_on` (no spawn_blocking): a
        // reintroduced `Runtime::block_on`/`Handle::current().block_on` would
        // panic right here, exactly where the original bug panicked. It does
        // not deadlock because the future runs on the separate `io_rt` reactor,
        // not on this current-thread runtime.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let out =
            rt.block_on(async { execute_blocking(async { Ok::<i32, TropelError>(7) }).unwrap() });
        assert_eq!(out, 7);
    }
}
