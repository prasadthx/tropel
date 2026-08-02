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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

/// A pool of dedicated worker threads, each running a current-thread tokio
/// runtime. VU tasks are pinned to their own worker (`spawn_vu`), so a VU is
/// never co-located with another VU on the same runtime.
pub struct VUWorkerPool {
    /// Workers created so far. Grown on demand by `spawn_vu` so each VU gets
    /// its own OS thread. Mutex: growth is rare (once per VU id) and cheap.
    workers: Mutex<Vec<WorkerInner>>,
    next_idx: AtomicUsize,
}

struct WorkerInner {
    /// Runtime handle — lets us spawn tasks onto this worker from any thread.
    handle: tokio::runtime::Handle,
    /// Signalled in `Drop` to unblock the worker thread's `block_on` call.
    shutdown: Arc<tokio::sync::Notify>,
    /// The dedicated OS thread that polls this runtime's task queue.
    thread: Option<thread::JoinHandle<()>>,
}

impl VUWorkerPool {
    /// Create a new pool with `count` workers (one per core).
    ///
    /// Each worker runs a current-thread tokio runtime on a dedicated OS thread.
    /// Panics if `count` is 0.
    pub fn new(count: usize) -> Self {
        assert!(count > 0, "VUWorkerPool requires at least 1 worker");

        let workers = (0..count).map(|i| Self::make_worker(i)).collect();
        Self {
            workers: Mutex::new(workers),
            next_idx: AtomicUsize::new(0),
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

        let thread = thread::Builder::new()
            .name(format!("tropel-worker-{}", i))
            .spawn(move || {
                // Block on the runtime, waiting for shutdown signal.
                // While blocked, the runtime processes spawned tasks.
                runtime.block_on(async {
                    sig.notified().await;
                });
            })
            .expect("Failed to spawn worker thread");

        WorkerInner {
            handle,
            shutdown,
            thread: Some(thread),
        }
    }

    /// Ensure at least `n` workers exist, growing the pool on demand.
    fn ensure_workers(&self, n: usize) {
        let mut workers = self.workers.lock().unwrap();
        while workers.len() < n {
            let i = workers.len();
            workers.push(Self::make_worker(i));
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

    /// Maximum number of worker threads the pool will ever create. All
    /// bounded executors (constant/ramping/shared/arrival/per-vu-iterations)
    /// hand out vu_ids < max_vus and therefore stay far below this cap, so
    /// `spawn_vu` is identity for them — strict 1-VU-per-thread isolation is
    /// preserved. Only `externally_controlled` hands out a *monotonic* id
    /// counter (ids are never reused while an old VU with the same id is
    /// still exiting, to keep data-row rotation / JS-context naming unique);
    /// a long churning run could otherwise leak one thread per regrow. Once
    /// the cap is reached, ids wrap onto existing workers. The id passed to
    /// `run_vu` is unaffected (naming stays unique) — only the worker slot
    /// wraps.
    const MAX_WORKERS: usize = 4096;

    /// Spawn a VU on its own dedicated worker thread, growing the pool on
    /// demand so VU `vu_id` always gets worker `vu_id` — a 1-VU-per-task
    /// mapping. This is what makes a blocking script `sleep()` safe: it only
    /// blocks this VU's OS thread, never a co-located VU (there is none).
    ///
    /// Returns a `JoinHandle` that can be awaited from any runtime.
    pub fn spawn_vu<F>(&self, vu_id: u32, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        // Cap growth (see MAX_WORKERS): monotonic externally-controlled ids
        // must not leak an OS thread per regrow. For any realistic test the
        // id is below the cap, so this is the identity mapping.
        let idx = (vu_id as usize) % Self::MAX_WORKERS;
        self.ensure_workers(idx + 1);
        self.spawn_on(idx, future)
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
        // Signal each worker to stop, which unblocks the `notified().await` call.
        for worker in workers.iter() {
            worker.shutdown.notify_waiters();
        }
        // Join the worker threads.
        for worker in workers.iter_mut() {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
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
                let h = pool.spawn_vu(
                    0,
                    async { std::thread::current().name().map(|s| s.to_string()) },
                );
                h.await.unwrap()
            },
            async {
                let h = pool.spawn_vu(
                    1,
                    async { std::thread::current().name().map(|s| s.to_string()) },
                );
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
        let slow = pool.spawn_vu(
            0,
            async {
                std::thread::sleep(Duration::from_millis(200));
                "slow"
            },
        );

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
    async fn worker_pool_grows_on_demand() {
        let pool = VUWorkerPool::new(2);
        assert_eq!(pool.worker_count(), 2);
        // spawn_vu(10) forces the pool to grow to 11 workers.
        let h = pool.spawn_vu(10, async {});
        assert!(h.await.is_ok());
        assert_eq!(pool.worker_count(), 11);
        // spawn (round-robin) still works and does not shrink anything.
        let (idx, h) = pool.spawn(async {});
        assert!(h.await.is_ok());
        assert!(idx < pool.worker_count());
    }
}
