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
use std::time::{Duration, Instant};
use tokio::runtime::{Builder, Runtime};
use tropel_core::{Result, TropelError};

/// How long the caller spins (polling the rendezvous channel with `try_recv`,
/// a lock-free check with no syscall) before parking on `recv`.
///
/// Rationale: `execute_blocking` is the hot path for every k6/pm/ws host call
/// (~1 per request). For fast responses — the common case in high-RPS local
/// and synthetic runs — the spawned future often completes within this window,
/// so the caller never issues the park/wake futex pair: zero syscalls instead
/// of two per request (~200 k syscalls/s at 100 k req/s).
///
/// The spin must be short enough that a slow request doesn't waste much CPU:
/// the window only overlaps the tail of the handoff (spawn + reactor wake),
/// not the full network latency, and the multi-thread `io_rt` runs the future
/// on OTHER threads while we spin, so the request always progresses.
const SPIN_WINDOW: Duration = Duration::from_micros(40);

// Per-thread spin heuristic: once a request on this thread takes longer than
// the spin window, spinning is a losing bet (the next request likely will too
// — latency is stable per endpoint) — skip the spin and park directly, so a
// slow endpoint never burns the full window on every request. The flag is
// re-armed when a request completes within the window.
thread_local! {
    static SKIP_SPIN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

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

/// Error message when the spawned I/O task dies before producing a result.
const IO_TASK_DROPPED: &str = "io task dropped";

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

    // Adaptive spin-then-park: for fast responses the result lands in the
    // channel before a park syscall would be worth it. `try_recv` is lock-free
    // (no syscall), so the spin window costs CPU only; a slow request parks
    // exactly like before, and once a thread observes a slow request it skips
    // spinning until a fast one re-arms it (latency is stable per endpoint, so
    // spinning after a slow response is a losing bet). NO tokio runtime is
    // entered on the caller either way → no "runtime within runtime" panic;
    // the future runs on io_rt's reactor → no deadlock.
    let spin_deadline = Instant::now() + SPIN_WINDOW;
    let mut spun = false;
    if !SKIP_SPIN.with(|s| s.get()) {
        loop {
            match rx.try_recv() {
                Ok(r) => {
                    SKIP_SPIN.with(|s| s.set(false)); // fast → keep spinning
                    return r;
                }
                Err(std::sync::mpsc::TryRecvError::Empty) => {
                    if Instant::now() < spin_deadline {
                        spun = true;
                        std::hint::spin_loop();
                        continue;
                    }
                }
                Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                    return Err(TropelError::Http(IO_TASK_DROPPED.into()));
                }
            }
            break;
        }
        if spun {
            // The request outlasted the window — record that so the next one
            // on this thread parks immediately instead of re-spinning.
            SKIP_SPIN.with(|s| s.set(true));
        }
    }
    // Plain thread park: no tokio runtime entered here → no panic; future runs
    // on io_rt's reactor → no deadlock. Time the park: if a previously-slow
    // thread completes within the window (endpoint got fast again), re-arm the
    // spin — otherwise the flag would latch true forever and the "re-arm on
    // fast completion" promise in the doc comment would be unreachable.
    let park_start = Instant::now();
    let result = rx
        .recv()
        .map_err(|_| TropelError::Http(IO_TASK_DROPPED.into()))?;
    if park_start.elapsed() < SPIN_WINDOW {
        SKIP_SPIN.with(|s| s.set(false));
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workers_from_override_defaults_to_cores() {
        // No override → host core count (no env mutation, deterministic).
        let expected = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4);
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
    fn execute_blocking_tight_loop_no_starvation() {
        // The spin window must never starve or reorder results: run a tight
        // loop of trivial futures and assert every result comes back intact
        // (a buggy spin would hang or drop values here).
        for i in 0..2_000 {
            let v = execute_blocking(async move { Ok::<i32, TropelError>(i) }).unwrap();
            assert_eq!(v, i, "result mismatch at iteration {i}");
        }
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
