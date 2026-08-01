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
//! Results are recorded into a thread-local slot and consumed by
//! [`HttpClient::execute`](crate::client::HttpClient::execute) after each
//! request. This is safe because Tropel's thread-per-core executor gives each
//! VU its own OS thread and its own `HttpClient`; all request work (including
//! DNS and connection establishment) happens on that one thread. If a client
//! is ever shared across threads, the slot degrades gracefully (phases report
//! zero rather than crashing).
//!
//! # Phase attribution
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

thread_local! {
    static SLOT: RefCell<PhaseSlot> = RefCell::new(PhaseSlot::default());
}

/// Begin timing a new request: resets the slot and stamps the request start.
pub(crate) fn begin_request(now: Instant) {
    SLOT.with(|slot| {
        *slot.borrow_mut() = PhaseSlot {
            request_start: Some(now),
            ..PhaseSlot::default()
        };
    });
}

/// Record real DNS resolution time (called from the resolver hook).
pub(crate) fn record_dns(elapsed: Duration) {
    SLOT.with(|slot| {
        slot.borrow_mut().dns_elapsed = Some(elapsed);
    });
}

/// Record when a connector `call()` began. First attempt wins (redirects
/// that open a second connection do not clobber the initial measurement).
///
/// Note: under a redirect/retry the first (possibly failed) attempt is the
/// one measured — pairing first-start with first-elapsed keeps the two
/// consistent. This is intentional and matches the "first connection"
/// semantics k6 reports for the redirecting request.
pub(crate) fn record_connect_start(now: Instant) {
    SLOT.with(|slot| {
        let mut s = slot.borrow_mut();
        if s.connect_start.is_none() {
            s.connect_start = Some(now);
        }
    });
}

/// Record the duration of a connector call. First completion wins.
pub(crate) fn record_connect_elapsed(elapsed: Duration) {
    SLOT.with(|slot| {
        let mut s = slot.borrow_mut();
        if s.connect_elapsed.is_none() {
            s.connect_elapsed = Some(elapsed);
        }
    });
}

/// Read the recorded phases and reset the slot for the next request.
pub(crate) fn take_slot() -> PhaseSlot {
    SLOT.with(|slot| {
        let mut s = slot.borrow_mut();
        let taken = *s;
        *s = PhaseSlot::default();
        taken
    })
}

/// Custom DNS resolver that times real lookups.
///
/// Delegates to tokio's `lookup_host` (the same getaddrinfo-based resolution
/// reqwest's default GaiResolver uses) and records the elapsed time into the
/// thread-local slot.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct TimingDnsResolver;

impl reqwest::dns::Resolve for TimingDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let start = Instant::now();
            // Port 0: hyper-util applies the request's port to each resolved
            // address afterward (see HttpConnector::call_async → set_port).
            // `(String, u16)` implements ToSocketAddrs; owning the host makes
            // the future 'static for the boxed Resolving type.
            let result = tokio::net::lookup_host((host, 0)).await;
            record_dns(start.elapsed());
            result
                .map(|addrs| -> reqwest::dns::Addrs { Box::new(addrs) })
                .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })
        })
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
        record_connect_start(start);
        let inner = self.inner.call(req);
        Box::pin(async move {
            let out = inner.await;
            record_connect_elapsed(start.elapsed());
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
        begin_request(now);
        record_dns(Duration::from_millis(5));
        record_connect_start(now + Duration::from_millis(2));
        record_connect_elapsed(Duration::from_millis(20));

        let s = take_slot();
        assert_eq!(s.request_start, Some(now));
        assert_eq!(s.dns_elapsed, Some(Duration::from_millis(5)));
        assert_eq!(s.connect_start, Some(now + Duration::from_millis(2)));
        assert_eq!(s.connect_elapsed, Some(Duration::from_millis(20)));

        // Second read sees a clean slot.
        let s2 = take_slot();
        assert!(s2.connect_start.is_none());
        assert!(s2.connect_elapsed.is_none());
    }

    #[test]
    fn first_connect_wins() {
        let now = Instant::now();
        begin_request(now);
        record_connect_start(now + Duration::from_millis(1));
        record_connect_start(now + Duration::from_millis(50));
        record_connect_elapsed(Duration::from_millis(10));
        record_connect_elapsed(Duration::from_millis(99));

        let s = take_slot();
        assert_eq!(s.connect_start, Some(now + Duration::from_millis(1)));
        assert_eq!(s.connect_elapsed, Some(Duration::from_millis(10)));
    }

    #[test]
    fn pooled_connection_records_no_connect_phases() {
        let now = Instant::now();
        begin_request(now);
        // No connector call — pooled keep-alive reuse.
        let s = take_slot();
        assert!(s.connect_start.is_none());
        assert!(s.dns_elapsed.is_none());
    }

    /// End-to-end: a fresh connection records real connect phases; a pooled
    /// keep-alive reuse records none. Uses a live tokio TCP server so the
    /// whole `dns_resolver` + `connector_layer` + `execute()` chain is
    /// exercised, not just the slot primitives.
    ///
    /// Uses the **current-thread** runtime flavor to mirror the engine's
    /// thread-per-core model: every VU runs on its own OS thread with a
    /// current-thread tokio runtime, so all DNS/connect work happens on the
    /// VU thread and the thread-local recorder is exact. A multi-thread
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
            loop {
                // Tolerate Err: when the client is dropped the pooled socket
                // may close with RST (abortive close is common on Windows)
                // instead of a clean FIN, so read() can error rather than
                // return Ok(0). Either way the server task should just exit.
                let n = match sock.read(&mut buf).await {
                    Ok(n) => n,
                    Err(_) => break,
                };
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

        // Second request: pooled keep-alive reuse → no connector call, so the
        // connect phases are exactly zero (matching k6 for reused connections).
        let resp2 = client.execute(&req, None).await.unwrap();
        let t2 = resp2.timings.as_ref().unwrap();
        assert_eq!(t2.blocked + t2.dns + t2.connecting, std::time::Duration::ZERO);
        assert!(t2.waiting + t2.receiving > std::time::Duration::ZERO);

        // Dropping the client closes the pooled socket → server read-loop gets
        // EOF and the task can be awaited without hanging.
        drop(client);
        server.await.unwrap();
    }
}
