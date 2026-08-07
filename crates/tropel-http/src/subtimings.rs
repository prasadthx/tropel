//! Real sub-timing measurement via reqwest extension hooks.
//!
//! reqwest 0.13 does not expose connection-phase timings (blocked / DNS /
//! TCP connect / TLS) through its stable API. It does, however, provide two
//! extension hooks that let us measure them:
//!
//! 1. [`ClientBuilder::dns_resolver`](reqwest::ClientBuilder::dns_resolver) —
//!    a custom [`reqwest::dns::Resolve`] implementation that times DNS lookups.
//! 2. [`ClientBuilder::connector_layer`](reqwest::ClientBuilder::connector_layer) —
//!    a generic tower layer that wraps the connector service and times each
//!    connection attempt (DNS + TCP + TLS for a fresh connection).
//!
//! # Phase attribution (per-request slots)
//!
//! Requests are NOT one-per-thread: `http.batch` interleaves many requests on
//! one thread via `join_all`, and `execute()` runs on the shared multi-thread
//! `io_rt`, so a single request future can even migrate threads between polls.
//! A single thread-local slot would be clobbered by concurrent requests — the
//! old design cross-attributed phases between batch entries (one entry
//! reported everyone's `blocked`/`dns`/`connecting`, the rest got zeros).
//!
//! Instead every request gets its OWN slot (`Arc<Mutex<PhaseSlot>>`, created
//! by [`begin_request`]). Attribution happens through a thread-local *current
//! slot* that the [`TimedRequest`] wrapper installs at the start of **each
//! poll** of the request future and restores when the poll returns. The
//! DNS/connector hooks fire *inside* a poll of that request future (reqwest
//! drives them as part of it), so they observe exactly their request's slot —
//! even with N requests interleaved on one thread. As belt-and-braces the
//! hooks additionally capture the slot at `resolve()` / `call()` entry and
//! thread it through their async completions, so a completion that is polled
//! outside the originating poll (e.g. by hyper on another thread) still
//! writes to the correct slot.
//!
//! # Phase semantics
//!
//! - **blocked**: request start → connector `call()` begins (connection-pool
//!   wait / queueing). Zero when a pooled keep-alive connection is reused.
//! - **dns**: real DNS resolution time, from the resolver hook.
//! - **connecting**: connector call duration − DNS. For `http` this is pure
//!   TCP connect time. For `https`, reqwest folds the TLS handshake into the
//!   same connector call, so TLS time is included here — see
//!   [`Timings::tls_handshaking`](crate::Timings::tls_handshaking).
//! - **tls_handshaking** / **sending**: reqwest seals these inside the
//!   connector / request future; they stay zero. A hyper-based custom
//!   connector would be required to split them out of the connector call.

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// One request's worth of connection-phase timing, recorded on the VU thread.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct PhaseSlot {
    /// Set by `execute()` at the very start of the request.
    pub request_start: Option<Instant>,
    /// Real DNS resolution time (from the resolver hook).
    pub dns_elapsed: Option<Duration>,
    /// When the connector's `call()` began (first connection attempt).
    pub connect_start: Option<Instant>,
    /// Duration of the connector call (DNS + TCP + TLS).
    pub connect_elapsed: Option<Duration>,
}

// The slot of the request whose future is currently being polled on this
// thread. Installed by [`TimedRequest`] at the start of every poll and
// restored when the poll returns; the DNS resolver and connector layer read
// it to discover which request they belong to. `None` outside a request poll
// — hooks then no-op (or, for async completions, use the slot captured at
// entry instead).
//
// (A plain comment, not `///`: rustdoc does not generate docs for the
// `thread_local!` macro invocation, so a doc comment here triggers
// `unused_doc_comments`.)
thread_local! {
    static CURRENT_SLOT: RefCell<Option<Arc<Mutex<PhaseSlot>>>> = const { RefCell::new(None) };
}

/// Create a fresh per-request slot and stamp the request start.
///
/// Returns the slot handle. The caller must wrap the request future in
/// [`TimedRequest`] (which installs the slot as the poll-scoped current slot)
/// and later call [`take_slot`] on the same handle.
pub(crate) fn begin_request(now: Instant) -> Arc<Mutex<PhaseSlot>> {
    Arc::new(Mutex::new(PhaseSlot {
        request_start: Some(now),
        ..PhaseSlot::default()
    }))
}

/// The slot of the request whose future is currently being polled on this
/// thread, if any. Hooks call this at `call()` / `resolve()` entry — i.e.
/// during a poll of the request future — to discover their request's slot.
pub(crate) fn current_slot() -> Option<Arc<Mutex<PhaseSlot>>> {
    CURRENT_SLOT.with(|s| s.borrow().clone())
}

/// Record real DNS resolution time into the given request's slot.
pub(crate) fn record_dns(slot: &Arc<Mutex<PhaseSlot>>, elapsed: Duration) {
    slot.lock().unwrap().dns_elapsed = Some(elapsed);
}

/// Record when a connector `call()` began. First attempt wins (redirects
/// that open a second connection do not clobber the initial measurement).
///
/// Note: under a redirect/retry the first (possibly failed) attempt is the
/// one measured — pairing first-start with first-elapsed keeps the two
/// consistent. This is intentional and matches the "first connection"
/// semantics k6 reports for the redirecting request.
pub(crate) fn record_connect_start(slot: &Arc<Mutex<PhaseSlot>>, now: Instant) {
    let mut s = slot.lock().unwrap();
    if s.connect_start.is_none() {
        s.connect_start = Some(now);
    }
}

/// Record the duration of a connector call. First completion wins.
pub(crate) fn record_connect_elapsed(slot: &Arc<Mutex<PhaseSlot>>, elapsed: Duration) {
    let mut s = slot.lock().unwrap();
    if s.connect_elapsed.is_none() {
        s.connect_elapsed = Some(elapsed);
    }
}

/// Read the recorded phases of the given request and reset its slot.
pub(crate) fn take_slot(slot: &Arc<Mutex<PhaseSlot>>) -> PhaseSlot {
    let mut s = slot.lock().unwrap();
    let taken = *s;
    *s = PhaseSlot::default();
    taken
}

/// Wraps a request future so its connection-phase hooks attribute to the
/// right per-request slot.
///
/// Every poll installs the request's slot as the thread-local *current slot*
/// and restores the previous value when the poll returns. The DNS resolver
/// and connector layer callbacks fire *inside* one of these polls, so — even
/// with N requests interleaved on one thread (`http.batch`) or a future
/// migrating threads on the multi-thread `io_rt` — each request's phases land
/// in its own slot and never leak into another request's.
pub(crate) struct TimedRequest<F> {
    inner: F,
    slot: Arc<Mutex<PhaseSlot>>,
}

impl<F> TimedRequest<F> {
    pub(crate) fn new(inner: F, slot: Arc<Mutex<PhaseSlot>>) -> Self {
        Self { inner, slot }
    }
}

impl<F: Future> Future for TimedRequest<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // Install this request's slot as the current one for the duration of
        // this poll, then restore whatever was current before (normally None
        // — polls never nest, but restoring is cheap and defensive).
        let prev = CURRENT_SLOT.with(|s| s.borrow_mut().replace(self.slot.clone()));
        // SAFETY: `inner` is a field of the pinned struct and is never moved
        // out of it — we only project the Pin down to the field (the standard
        // pin-projection pattern; `Pin::map_unchecked_mut` is stable since
        // 1.75).
        let inner = unsafe { self.map_unchecked_mut(|s| &mut s.inner) };
        let out = inner.poll(cx);
        CURRENT_SLOT.with(|s| *s.borrow_mut() = prev);
        out
    }
}

/// Tower layer that times each connector call.
///
/// Fully generic over the request/response types (reqwest's connector service
/// uses sealed `Unnameable`/`Conn` types that we must never name — the same
/// trick reqwest's own `TimeoutLayer` example uses).
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TimingConnectorLayer;

impl<S> tower::Layer<S> for TimingConnectorLayer {
    type Service = TimingConnectorService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TimingConnectorService { inner }
    }
}

/// Tower service produced by [`TimingConnectorLayer`].
#[derive(Debug, Clone)]
pub(crate) struct TimingConnectorService<S> {
    inner: S,
}

impl<S, Req> tower::Service<Req> for TimingConnectorService<S>
where
    S: tower::Service<Req> + Clone + Send + Sync + 'static,
    S::Response: Send + 'static,
    S::Error: Send + 'static,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send + 'static>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let start = Instant::now();
        // Capture the slot at entry: `call()` runs during a poll of the
        // request future (TimedRequest has installed the current slot), and
        // the completion below may be polled outside that poll, so the
        // explicit capture — not a second `current_slot()` read — is what
        // keeps the elapsed write attributed correctly.
        let slot = current_slot();
        if let Some(slot) = &slot {
            record_connect_start(slot, start);
        }
        let inner = self.inner.call(req);
        Box::pin(async move {
            let out = inner.await;
            if let Some(slot) = &slot {
                record_connect_elapsed(slot, start.elapsed());
            }
            out
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slot_records_and_resets() {
        let now = Instant::now();
        let slot = begin_request(now);
        record_dns(&slot, Duration::from_millis(5));
        record_connect_start(&slot, now + Duration::from_millis(2));
        record_connect_elapsed(&slot, Duration::from_millis(20));

        let s = take_slot(&slot);
        assert_eq!(s.request_start, Some(now));
        assert_eq!(s.dns_elapsed, Some(Duration::from_millis(5)));
        assert_eq!(s.connect_start, Some(now + Duration::from_millis(2)));
        assert_eq!(s.connect_elapsed, Some(Duration::from_millis(20)));

        // Second read sees a clean slot (and other requests' slots are
        // unaffected — each request owns its own handle).
        let s2 = take_slot(&slot);
        assert!(s2.connect_start.is_none());
        assert!(s2.connect_elapsed.is_none());
    }

    #[test]
    fn first_connect_wins() {
        let now = Instant::now();
        let slot = begin_request(now);
        record_connect_start(&slot, now + Duration::from_millis(1));
        record_connect_start(&slot, now + Duration::from_millis(50));
        record_connect_elapsed(&slot, Duration::from_millis(10));
        record_connect_elapsed(&slot, Duration::from_millis(99));

        let s = take_slot(&slot);
        assert_eq!(s.connect_start, Some(now + Duration::from_millis(1)));
        assert_eq!(s.connect_elapsed, Some(Duration::from_millis(10)));
    }

    #[test]
    fn pooled_connection_records_no_connect_phases() {
        let now = Instant::now();
        let slot = begin_request(now);
        // No connector call — pooled keep-alive reuse.
        let s = take_slot(&slot);
        assert!(s.connect_start.is_none());
        assert!(s.dns_elapsed.is_none());
    }

    #[test]
    fn slots_are_isolated_per_request() {
        // Backlog line 166: two concurrent requests must never share phases.
        // Each `begin_request` returns an independent slot, so recording into
        // one can never be read by the other.
        let now = Instant::now();
        let a = begin_request(now);
        let b = begin_request(now + Duration::from_millis(10));
        record_connect_start(&a, now + Duration::from_millis(1));
        record_connect_elapsed(&a, Duration::from_millis(5));
        // B records nothing — its slot must stay empty regardless of A.
        let sb = take_slot(&b);
        assert!(sb.connect_start.is_none());
        assert!(sb.connect_elapsed.is_none());
        // And A still sees its own phases.
        let sa = take_slot(&a);
        assert_eq!(sa.connect_start, Some(now + Duration::from_millis(1)));
        assert_eq!(sa.connect_elapsed, Some(Duration::from_millis(5)));
    }

    #[tokio::test]
    async fn timed_request_installs_current_slot_during_poll() {
        // The wrapper must make `current_slot()` return THIS request's slot
        // while the inner future is being polled, and clear it afterwards.
        let slot = begin_request(Instant::now());
        let seen: Arc<Mutex<Option<Arc<Mutex<PhaseSlot>>>>> = Arc::new(Mutex::new(None));
        let seen_clone = seen.clone();
        TimedRequest::new(
            async move {
                *seen_clone.lock().unwrap() = current_slot();
            },
            slot.clone(),
        )
        .await;

        let got = seen.lock().unwrap().clone();
        assert!(
            Arc::ptr_eq(got.as_ref().unwrap(), &slot),
            "inner future must see its own request's slot"
        );
        assert!(
            current_slot().is_none(),
            "current slot must be cleared after the request completes"
        );
    }

    /// End-to-end: a fresh connection records real connect phases; a pooled
    /// keep-alive reuse records none. Uses a live tokio TCP server so the
    /// whole `dns_resolver` + `connector_layer` + `execute()` chain is
    /// exercised, not just the slot primitives.
    ///
    /// Uses the **current-thread** runtime flavor to mirror the engine's
    /// thread-per-core model: every VU runs on its own OS thread with a
    /// current-thread tokio runtime, so all DNS/connect work happens on the
    /// VU thread and the per-request recorder is exact. A multi-thread
    /// runtime would let reqwest poll the connector on a different worker
    /// thread, and the slot written there would be invisible to `take_slot()`.
    #[tokio::test(flavor = "current_thread")]
    async fn fresh_connection_records_real_connect_phases() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Keep-alive server: accepts ONE connection and serves a read-loop so
        // the second request reuses the same (pooled) socket. Exits on EOF,
        // which happens when the client is dropped and the pool closes it.
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 4096];
            // Tolerate Err: when the client is dropped the pooled socket may
            // close with RST (abortive close is common on Windows) instead of
            // a clean FIN, so read() can error rather than return Ok(0).
            // Either way the server task should just exit.
            while let Ok(n) = sock.read(&mut buf).await {
                if n == 0 {
                    break;
                }
                sock.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                )
                .await
                .unwrap();
            }
        });

        let cfg = tropel_core::config::HttpConfig::default();
        let client = super::super::client::HttpClient::new(&cfg).unwrap();
        let req = tropel_core::types::Request {
            url: format!("http://{}/", addr),
            method: tropel_core::types::Method::GET,
            ..Default::default()
        };

        // First request: fresh connection → connector call recorded.
        let resp1 = client.execute(&req, None).await.unwrap();
        let t1 = resp1.timings.as_ref().unwrap();
        assert!(
            t1.blocked + t1.dns + t1.connecting > std::time::Duration::ZERO,
            "fresh connection should record real connect phases: {:?}",
            t1
        );
        assert!(t1.waiting + t1.receiving > std::time::Duration::ZERO);
        assert!(t1.total >= t1.waiting + t1.receiving);
        // k6 breakdown invariant: the phases sum to (at most) the total. The
        // waiting/TTFB phase excludes the connect phases (blocked + dns +
        // connecting are subtracted from the raw elapsed), so a fresh
        // connection's breakdown closes the gap to total.
        let sum1 = t1.blocked + t1.dns + t1.connecting + t1.waiting + t1.receiving;
        assert!(
            sum1 <= t1.total,
            "phase sum must not exceed total: {sum1:?} vs {:?}",
            t1.total
        );
        assert!(
            t1.total - sum1 < std::time::Duration::from_millis(50),
            "fresh-connection phases should sum close to total: {sum1:?} vs {:?}",
            t1.total
        );

        // Second request: pooled keep-alive reuse → no connector call, so the
        // connect phases are exactly zero (matching k6 for reused connections).
        let resp2 = client.execute(&req, None).await.unwrap();
        let t2 = resp2.timings.as_ref().unwrap();
        assert_eq!(t2.blocked + t2.dns + t2.connecting, std::time::Duration::ZERO);
        assert!(t2.waiting + t2.receiving > std::time::Duration::ZERO);
        let sum2 = t2.blocked + t2.dns + t2.connecting + t2.waiting + t2.receiving;
        assert!(sum2 <= t2.total, "phase sum must not exceed total");

        // Dropping the client closes the pooled socket → server read-loop gets
        // EOF and the task can be awaited without hanging.
        drop(client);
        server.await.unwrap();
    }

    /// Backlog line 166 regression: `http.batch` runs N requests through
    /// `join_all`, so several requests are in flight on ONE thread at once.
    /// The old single thread-local slot cross-attributed phases (the first
    /// `take_slot` drained everything, the rest reported zeros). Every
    /// concurrent request must report its OWN connect phases.
    #[tokio::test(flavor = "current_thread")]
    async fn concurrent_requests_each_report_own_connect_phases() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Server accepts 4 connections and answers each after a short delay
        // so the client futures overlap on the one test thread — the exact
        // batch interleaving the bug was about.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            for _ in 0..4 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = [0u8; 4096];
                let _ = sock.read(&mut buf).await;
                tokio::time::sleep(Duration::from_millis(30)).await;
                sock.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok",
                )
                .await
                .ok();
            }
        });

        // `no_connection_reuse` forces a FRESH connection per request — every
        // one of the 4 concurrent requests must perform a real TCP connect,
        // so each must record non-zero connect phases.
        let cfg = tropel_core::config::HttpConfig {
            no_connection_reuse: true,
            ..Default::default()
        };
        let client = super::super::client::HttpClient::new(&cfg).unwrap();
        let req = tropel_core::types::Request {
            url: format!("http://{}/", addr),
            method: tropel_core::types::Method::GET,
            ..Default::default()
        };

        let f1 = client.execute(&req, None);
        let f2 = client.execute(&req, None);
        let f3 = client.execute(&req, None);
        let f4 = client.execute(&req, None);
        let (r1, r2, r3, r4) = tokio::join!(f1, f2, f3, f4);

        drop(client);
        server.await.unwrap();

        let responses = [r1, r2, r3, r4];
        for (i, resp) in responses.iter().enumerate() {
            let resp = resp.as_ref().expect("request succeeded");
            let t = resp.timings.as_ref().expect("timings present");
            assert!(
                t.blocked + t.dns + t.connecting > Duration::ZERO,
                "request {i}: a fresh connection must record its own connect \
                 phases (cross-attribution regression), got {:?}",
                t
            );
            let sum = t.blocked + t.dns + t.connecting + t.waiting + t.receiving;
            assert!(
                sum <= t.total,
                "request {i}: phase sum must not exceed total: {sum:?} vs {:?}",
                t.total
            );
        }
    }
}
