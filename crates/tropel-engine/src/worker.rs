//! # VUWorkerPool — Thread-per-core VU sharding
//!
//! Distributes VUs across a pool of current-thread tokio runtimes, each
//! pinned to a dedicated OS thread. This gives each VU core-level isolation:
//!
//! - **No shared work-stealing** — VUs on one core never steal work from another
//! - **Better cache locality** — each core's VU data stays in its L1/L2 cache
//! - **JS execution isolation** — blocking JS on core 0 doesn't stall VUs on core 1
//! - **Future safety** — each JsContext is only used by its pinned thread, so we
//!   could drop the `rquickjs` `parallel` feature (and its per-`ctx.with` mutex)
//!   if `JsContext` were made `!Send`

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;

/// A pool of dedicated worker threads, each running a current-thread tokio
/// runtime. VU tasks are distributed round-robin across workers.
pub struct VUWorkerPool {
    workers: Vec<WorkerInner>,
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

        let workers: Vec<WorkerInner> = (0..count)
            .map(|i| {
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
            })
            .collect();

        Self {
            workers,
            next_idx: AtomicUsize::new(0),
        }
    }

    /// Return the number of workers in the pool.
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Spawn a future on the worker at `idx` (must be < worker_count).
    /// Returns a `JoinHandle` that can be awaited from any runtime.
    pub fn spawn_on<F>(&self, idx: usize, future: F) -> tokio::task::JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        self.workers[idx].handle.spawn(future)
    }

    /// Spawn a future on the next worker (round-robin distribution).
    /// Returns a tuple of (worker_index, JoinHandle).
    pub fn spawn<F>(&self, future: F) -> (usize, tokio::task::JoinHandle<F::Output>)
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let handle = self.workers[idx].handle.spawn(future);
        (idx, handle)
    }
}

impl Drop for VUWorkerPool {
    fn drop(&mut self) {
        // Signal each worker to stop, which unblocks the `notified().await` call.
        for worker in &self.workers {
            worker.shutdown.notify_waiters();
        }
        // Join the worker threads.
        for worker in &mut self.workers {
            if let Some(thread) = worker.thread.take() {
                let _ = thread.join();
            }
        }
    }
}

unsafe impl Send for VUWorkerPool {}
unsafe impl Sync for VUWorkerPool {}
