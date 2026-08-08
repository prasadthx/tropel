//! # VUWorkerPool — Thread-per-core VU sharding (1 VU per dedicated thread)
//!
//! Distributes VUs across a pool of current-thread tokio runtimes, each
//! pinned to a dedicated OS thread. This gives each VU core-level isolation:
//!
//! - **No shared work-stealing** — VUs on one core never steal work from another
//! - **Better cache locality** — each core's VU data stays in its L1/L2 cache
//! - **JS execution isolation** — blocking JS on core 0 doesn't stall VUs on core 1
//! - **`sleep()` safety** — each VU owns its OS thread, so a blocking script
//!   `sleep()` (implemented with `std::thread::sleep`) pauses *only* that VU.
//!   The pool grows on demand (`spawn_vu`), so no two VUs ever share a
//!   current-thread runtime — otherwise a `sleep()` in one VU would freeze
//!   every VU co-located on the same worker.
//!
//! # Scalability tradeoff
//!
//! 1 VU per OS thread is the closest Rust analog to k6's goroutine-per-VU
//! model (without a GC), and it is what makes blocking `sleep()` safe. The
//! cost is one OS thread (plus a current-thread runtime) per VU, so very high
//! VU counts (e.g. 10k) are thread-heavy. That is the accepted tradeoff of
//! the 1-VU-per-task design; a future refinement could cap growth when a
//! script never calls `sleep()`.
//!
//! **Hard ceiling:** the pool never grows past `MAX_WORKERS` (4096). For a
//! bounded executor with `vus > 4096` (an extreme 10k-VU constant test), VU
//! `n` and VU `n+4096` would silently share a worker, so a blocking
//! `sleep()` in one could freeze the other — the cap trades strict isolation
//! away at extreme VU counts to avoid exhausting the OS with one thread per
//! VU. Realistic tests stay far below the cap, where isolation is exact.
//! - **Future safety** — each JsContext is only used by its pinned thread, so we
//!   could drop the `rquickjs` `parallel` feature (and its per-`ctx.with` mutex)
//!   if `JsContext` were made `!Send`

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// How long `Drop` waits for a worker thread to exit before detaching it.
/// Matches the engine's VU-drain bound (see `vu_loop.rs`: "VU drain timed
/// out after 30s") so a worker wedged in a blocking eval or
/// `std::thread::sleep` can never hang teardown past the drain bound.
const JOIN_BOUND: Duration = Duration::from_secs(30);

/// A pool of dedicated worker threads, each running a current-thread tokio
/// runtime. VU tasks are pinned to their own worker (`spawn_vu`), so a VU is
/// never co-located with another VU on the same runtime.
pub struct VUWorkerPool {
    /// Workers created so far. Grown on demand by `spawn_vu` so each live VU
    /// gets its own OS thread. Mutex: growth is rare (once per concurrent
    /// slot) and cheap.
    workers: Mutex<Vec<WorkerInner>>,
    /// Parallel to `workers`: true while that worker hosts a LIVE VU task.
    /// `spawn_vu` reuses a slot whose flag is false (the finished VU freed
    /// it), so the pool sizes to PEAK CONCURRENCY — never to the cumulative
    /// monotonic vu_id count, which never resets and used to leak one OS
    /// thread + runtime + ~2 fds per id across ramp cycles (P0: fd
    /// exhaustion at tiny peak concurrency → swallowed panic → green run).
    busy: Mutex<Vec<Arc<AtomicBool>>>,
    next_idx: AtomicUsize,
    /// How long `Drop` waits for a worker to exit before detaching it. 30s by
    /// default (matching the engine's drain bound); short in tests.
    join_bound: Duration,
}

struct WorkerInner {
    /// Runtime handle — lets us spawn tasks onto this worker from any thread.
    handle: tokio::runtime::Handle,
    /// Signalled in `Drop` to unblock the worker thread's `block_on` call.
    shutdown: Arc<tokio::sync::Notify>,
    /// The dedicated OS thread that polls this runtime's task queue.
    thread: Option<thread::JoinHandle<()>>,
    /// Receives `()` once the worker thread has returned from `block_on` (i.e.
    /// it is about to exit). Lets `Drop` wait on a *bounded* join instead of
    /// blocking forever on a wedged worker.
    exited: Option<mpsc::Receiver<()>>,
}

/// How a `spawn_vu` slot was acquired.
enum Slot {
    /// Reused an existing worker whose previous VU had finished.
    Idle(usize, Arc<AtomicBool>),
    /// Grew the pool with a brand-new worker (busy from birth).
    Grown(usize, Arc<AtomicBool>),
    /// Past the hard cap — co-scheduled on a busy worker (isolation
    /// traded away at extreme VU counts, as documented).
    Wrapped(usize),
}

/// Clears a worker's busy flag on drop. The VU task's completion (or panic)
/// releases the slot back to the pool for reuse — this is what keeps the pool
/// sized to peak concurrency instead of cumulative ids.
struct BusyGuard(Arc<AtomicBool>);

impl Drop for BusyGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl VUWorkerPool {
    /// Create a new pool with `count` workers (one per core).
    ///
    /// Each worker runs a current-thread tokio runtime on a dedicated OS thread.
    /// Panics if `count` is 0.
    pub fn new(count: usize) -> Self {
        Self::with_join_bound(count, JOIN_BOUND)
    }

    /// Create a pool with a custom join bound (tests use a short one).
    fn with_join_bound(count: usize, join_bound: Duration) -> Self {
        assert!(count > 0, "VUWorkerPool requires at least 1 worker");

        let workers = (0..count).map(Self::make_worker).collect();
        let busy = (0..count).map(|_| Arc::new(AtomicBool::new(false))).collect();
        Self {
            workers: Mutex::new(workers),
            busy: Mutex::new(busy),
            next_idx: AtomicUsize::new(0),
            join_bound,
        }
    }

    /// Create a single worker (current-thread runtime + pinned OS thread).
    fn make_worker(i: usize) -> WorkerInner {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("Failed to create current-thread tokio runtime");

        let handle = runtime.handle().clone();
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let sig = shutdown.clone();
        let (exited_tx, exited_rx) = mpsc::channel::<()>();

        let thread = thread::Builder::new()
            .name(format!("tropel-worker-{}", i))
            .spawn(move || {
                // Block on the runtime, waiting for shutdown signal.
                // While blocked, the runtime processes spawned tasks.
                runtime.block_on(async {
                    sig.notified().await;
                });
                // The worker is exiting — signal the pool so `Drop` can join
                // it within the join bound. If the pool is gone (detached),
                // the send fails silently.
                let _ = exited_tx.send(());
            })
            .expect("Failed to spawn worker thread");

        WorkerInner {
            handle,
            shutdown,
            thread: Some(thread),
            exited: Some(exited_rx),
        }
    }

    /// Find a worker slot with no live VU (its flag is false) and mark it
    /// busy. Returns the slot and the flag (cloned) so the spawned task can
    /// clear it on completion. `None` when every slot is busy.
    fn find_idle_slot(&self) -> Option<(usize, Arc<AtomicBool>)> {
        let busy = self.busy.lock().unwrap();
        for (i, flag) in busy.iter().enumerate() {
            if flag
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Some((i, flag.clone()));
            }
        }
        None
    }

    /// Claim a worker slot for one VU. Reuses an idle slot when one exists
    /// (pool sizes to peak concurrency); grows by one only when every slot
    /// is busy; wraps onto an existing worker only past `MAX_WORKERS`.
    ///
    /// Workers (runtime and OS thread) are created OUTSIDE the mutex: this is
    /// called from the async ramp loop, and creating a current-thread runtime
    /// and spawning a thread can take milliseconds — holding the mutex across
    /// that would stall the ramp loop and every other pool operation. The
    /// lock is only held for the final insert.
    fn acquire_slot(&self, vu_id: u32) -> Slot {
        if let Some((idx, flag)) = self.find_idle_slot() {
            return Slot::Idle(idx, flag);
        }
        loop {
            let current = self.workers.lock().unwrap().len();
            if current >= Self::MAX_WORKERS {
                return Slot::Wrapped((vu_id as usize) % current);
            }
            // Build the worker outside the lock (runtime + thread creation).
            let worker = Self::make_worker(current);
            let flag = Arc::new(AtomicBool::new(true)); // busy from birth
            let mut workers = self.workers.lock().unwrap();
            if workers.len() == current {
                // No concurrent growth — commit.
                workers.push(worker);
                self.busy.lock().unwrap().push(flag.clone());
                return Slot::Grown(current, flag);
            }
            // Another thread grew the pool between our snapshot and lock
            // acquisition; the freshly-built worker is surplus. Signal it to
            // stop and reap it, then re-check. The surplus worker is
            // GUARANTEED idle (never inserted, so `spawn_on` can't reach it),
            // so the notify is consumed promptly and `recv()` can't deadlock.
            drop(workers);
            worker.shutdown.notify_one();
            if let Some(exited) = &worker.exited {
                let _ = exited.recv();
            }
            if let Some(thread) = worker.thread {
                let _ = thread.join();
            }
        }
    }

    /// Return the number of workers in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.lock().unwrap().len()
    }

    /// Spawn a future on the worker at `idx` (must be < worker_count).
    /// Returns a `JoinHandle` that can be awaited from any runtime.
    pub fn spawn_on<F>(&self, idx: usize, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let handle = self.workers.lock().unwrap()[idx].handle.clone();
        handle.spawn(future)
    }

    /// Maximum number of worker threads the pool will ever create. `spawn_vu`
    /// reuses idle slots (slots freed by finished/panicked VUs) before
    /// growing, so for any realistic test the pool sizes to PEAK CONCURRENCY
    /// and stays far below this cap — strict 1-VU-per-thread isolation is
    /// preserved. The cap only bites when a single run is concurrently busy
    /// beyond 4096 VUs; once reached, additional VUs wrap onto existing
    /// workers (isolation traded away at extreme VU counts, as documented).
    /// The vu_id passed to `run_vu` is unaffected (naming stays unique) —
    /// only the worker slot may be shared.
    const MAX_WORKERS: usize = 4096;

    /// Spawn a VU on a dedicated worker thread. Reuses an idle slot (a
    /// finished VU's worker) when one exists, grows the pool only when every
    /// slot is busy, and wraps onto an existing worker only past
    /// `MAX_WORKERS`. No two LIVE VUs ever share a worker, so a blocking
    /// script `sleep()` still only blocks its own VU — while the pool sizes
    /// to PEAK CONCURRENCY instead of the cumulative monotonic id count
    /// (P0: ids grew the pool to thousands of threads/runtimes/fds at tiny
    /// peak concurrency).
    ///
    /// Returns a `JoinHandle` that can be awaited from any runtime.
    pub fn spawn_vu<F>(&self, vu_id: u32, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        match self.acquire_slot(vu_id) {
            // Panic-safe release: the guard clears the busy flag on drop, so
            // the slot returns to the pool even if the VU task panics
            // mid-flight. The Slot pattern owns the only Arc — moved straight
            // into the task, no clone.
            Slot::Idle(idx, flag) | Slot::Grown(idx, flag) => self.spawn_on(idx, async move {
                let _release = BusyGuard(flag);
                future.await
            }),
            Slot::Wrapped(idx) => self.spawn_on(idx, future),
        }
    }

    /// Spawn a future on the next worker (round-robin distribution).
    /// Returns a tuple of (worker_index, JoinHandle).
    pub fn spawn<F>(&self, future: F) -> (usize, tokio::task::JoinHandle<F::Output>)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let len = self.worker_count();
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % len;
        let handle = self.spawn_on(idx, future);
        (idx, handle)
    }
}

impl Drop for VUWorkerPool {
    fn drop(&mut self) {
        // `get_mut` is sound here: Drop runs only when the last Arc is dropped,
        // so no other thread can hold the lock. Recover from a poisoned mutex
        // (a panic in any earlier lock guard) instead of aborting teardown.
        let workers = self
            .workers
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Signal each worker to stop. Each worker has its OWN Notify, and we
        // use `notify_one()` (not `notify_waiters()`): notify_waiters stores
        // no permit, so a notification fired before the worker thread has
        // registered its `notified().await` waiter would be LOST and the
        // worker would hang. `notify_one()` stores a permit when no waiter is
        // present yet, so the wake can never be missed — race-free whether
        // the worker is starting, parked, or wedged in a blocking call.
        for worker in workers.iter() {
            worker.shutdown.notify_one();
        }
        // Join the worker threads within a BOUNDED window. A worker whose VU
        // is wedged in a blocking eval or `std::thread::sleep` cannot poll the
        // shutdown notify until that blocking call returns, so an unbounded
        // `join()` would hang teardown past the engine's 30s drain bound.
        // Wait up to `join_bound` for each worker; a straggler is DETACHED
        // (its handle dropped without joining) so the run finishes on time —
        // the abandoned VU keeps running in the background, its late samples
        // land after the summary snapshot (so they can't corrupt it), and the
        // OS thread is reclaimed whenever its blocking call finally returns.
        let deadline = Instant::now() + self.join_bound;
        for worker in workers.iter_mut() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let exited = match &worker.exited {
                Some(rx) => matches!(
                    rx.recv_timeout(remaining),
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected)
                ),
                None => false,
            };
            if exited {
                // Worker returned from `block_on` — it is about to exit, so
                // `join` returns promptly.
                if let Some(thread) = worker.thread.take() {
                    let _ = thread.join();
                }
            } else if let Some(thread) = worker.thread.take() {
                // Wedged past the bound: detach. The OS thread keeps running
                // (the VU's blocking call is stuck) and exits whenever that
                // call finally returns; we simply stop waiting so teardown is
                // bounded. The `exited` sender is dropped with the thread, so
                // nothing leaks — the detached thread is reclaimed by the OS
                // when its task completes.
                tracing::warn!(
                    "VU worker {} did not exit within the {}s join bound — detaching (its VU is wedged in a blocking call)",
                    thread.thread().name().unwrap_or("?"),
                    self.join_bound.as_secs()
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn spawn_vu_pins_each_vu_to_its_own_thread() {
        // Two VUs spawned via spawn_vu must land on DIFFERENT OS threads —
        // the whole point of the 1-VU-per-task design. If they shared a
        // current-thread runtime, a blocking sleep() in one would freeze the
        // other.
        let pool = VUWorkerPool::new(1);

        let (t0, t1) = tokio::join!(
            async {
                let h = pool.spawn_vu(0, async {
                    std::thread::current().name().map(|s| s.to_string())
                });
                h.await.unwrap()
            },
            async {
                let h = pool.spawn_vu(1, async {
                    std::thread::current().name().map(|s| s.to_string())
                });
                h.await.unwrap()
            },
        );

        assert_ne!(t0, t1, "VUs must run on distinct worker threads");
        assert_eq!(t0.as_deref(), Some("tropel-worker-0"));
        assert_eq!(t1.as_deref(), Some("tropel-worker-1"));
    }

    #[tokio::test]
    async fn sleep_in_one_vu_does_not_block_another() {
        // Regression test for the sleep()-blocks-the-core bug: with 1 VU per
        // task, a blocking std::thread::sleep in VU 0 must not delay VU 1.
        let pool = VUWorkerPool::new(1);

        // The slow VU blocks its OS thread for 200ms (exactly what a script
        // `sleep(0.2)` does via the native bridge).
        let slow = pool.spawn_vu(0, async {
            std::thread::sleep(Duration::from_millis(200));
            "slow"
        });

        // The fast VU must finish well within the slow VU's sleep window —
        // if VUs shared a current-thread runtime, the fast VU would be stuck
        // behind the blocking sleep and this timeout would fire.
        let fast = tokio::time::timeout(
            Duration::from_millis(100),
            pool.spawn_vu(1, async { "fast" }),
        )
        .await
        .expect("fast VU was blocked behind another VU's sleep")
        .unwrap();

        assert_eq!(fast, "fast");
        let _ = slow.await.unwrap();
    }

    #[tokio::test]
    async fn worker_pool_grows_only_for_concurrent_vus() {
        // P0 (backlog): the pool sized on the process-wide MONOTONIC vu_id
        // counter — spawn_vu(10) grew the pool to 11 workers even though a
        // single VU at a time never needs more than one. Across 20 ramp
        // cycles (ids ~2000) that leaked ~2000 OS threads + runtimes + ~4000
        // fds at peak concurrency 100 → fd exhaustion → a swallowed panic
        // inside the scenario task → green run on partial data. The pool now
        // REUSES idle slots: sequential VUs (whatever their id) must not grow
        // it; only genuinely CONCURRENT VUs do.
        let pool = VUWorkerPool::new(2);
        assert_eq!(pool.worker_count(), 2);

        // 2000 sequential VUs with monotonic ids (simulating many ramp
        // cycles) must not grow the pool at all — each reuses a freed slot.
        for vu_id in 0..2000u32 {
            let h = pool.spawn_vu(vu_id, async {});
            assert!(h.await.is_ok());
            assert_eq!(pool.worker_count(), 2, "sequential VU {vu_id} grew the pool");
        }

        // Concurrent VUs DO grow it: 3 simultaneously-live VUs on a 2-worker
        // pool must grow to 3 workers, then free slots back when they finish.
        let (a, b, c) = tokio::join!(
            pool.spawn_vu(0, async { std::thread::sleep(std::time::Duration::from_millis(50)); }),
            pool.spawn_vu(1, async { std::thread::sleep(std::time::Duration::from_millis(50)); }),
            pool.spawn_vu(2, async { std::thread::sleep(std::time::Duration::from_millis(50)); }),
        );
        assert!(a.is_ok() && b.is_ok() && c.is_ok());
        assert_eq!(pool.worker_count(), 3, "3 concurrent VUs must grow the pool to 3");

        // After they finish, the 3rd worker slot is idle again but the pool
        // never shrinks below the peak — a later sequential VU reuses it.
        let h = pool.spawn_vu(3, async {});
        assert!(h.await.is_ok());
        assert_eq!(pool.worker_count(), 3);

        // spawn (round-robin) still works and does not shrink anything.
        let (idx, h) = pool.spawn(async {});
        assert!(h.await.is_ok());
        assert!(idx < pool.worker_count());
    }

    /// Backlog line 168: `Drop` used to `join()` every worker unconditionally,
    /// so a VU wedged in a blocking eval / `std::thread::sleep` hung teardown
    /// past the engine's 30s drain bound. Drop must now return within the
    /// join bound by DETACHING the wedged worker instead of waiting for it.
    #[test]
    fn drop_detaches_wedged_worker_within_join_bound() {
        // Short join bound so the test is fast; the worker is wedged for far
        // longer than the bound.
        let pool = VUWorkerPool::with_join_bound(1, Duration::from_millis(150));

        // A VU that blocks its OS thread for 2s (what a script `sleep(2.0)`
        // does via the native bridge). While wedged, the worker cannot poll
        // the shutdown notify, so an unbounded join would hang ~2s here —
        // and with a truly stuck eval, forever.
        let _h = pool.spawn_vu(0, async {
            std::thread::sleep(Duration::from_secs(2));
        });

        let start = Instant::now();
        drop(pool);
        let elapsed = start.elapsed();
        // Must return near the join bound (detaching), NOT after the 2s sleep.
        assert!(
            elapsed < Duration::from_millis(800),
            "drop blocked for {elapsed:?} on a wedged worker instead of detaching"
        );
    }

    /// A healthy pool (no wedged VUs) must still tear down cleanly and
    /// promptly — the bounded join must not regress the fast path.
    #[test]
    fn drop_joins_healthy_workers_promptly() {
        let pool = VUWorkerPool::with_join_bound(2, Duration::from_secs(5));
        let _h = pool.spawn_vu(0, async {});
        let _h2 = pool.spawn_vu(1, async {});
        let start = Instant::now();
        drop(pool);
        // Both workers exited on the shutdown notify immediately.
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "healthy teardown took {:?}",
            start.elapsed()
        );
    }
}
