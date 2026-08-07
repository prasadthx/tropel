//! # tropel-x-websocket
//!
//! Real WebSocket protocol extension for Tropel: connects to `ws://` /
//! `wss://` endpoints, sends messages (from the request body or config),
//! collects responses, and emits k6-style `ws_*` metrics.
//!
//! ## Request contract
//!
//! - URL: `ws://host:port/path` or `wss://host:port/path` (TLS is handled
//!   automatically by tokio-tungstenite's rustls connector).
//! - Request headers are passed through as WebSocket handshake headers.
//! - Messages to send (first match wins):
//!   1. `config["messages"]` — a JSON array of strings.
//!   2. The request body (`Body::Raw`) — a single message.
//! - Config keys:
//!   - `messages`: array of strings to send after connecting.
//!   - `wait`: how long to keep reading responses (e.g. `"1s"`, `"500ms"`;
//!     default `1s`).
//!   - `binary`: send the messages as binary frames (default `false`).
//!
//! The response lands in `pm.response`: status 101 (Switching Protocols) on
//! success, body = JSON array of the received text messages (binary payloads
//! are summarized as `<binary N bytes>`).
//!
//! ## Metrics (k6-style)
//!
//! `ws_connecting` (Trend), `ws_msgs_sent` / `ws_msgs_received` /
//! `ws_bytes_sent` / `ws_bytes_received` / `ws_sessions` (Counter),
//! `ws_req_duration` (Trend), `ws_req_failed` (Rate), plus `data_sent` /
//! `data_received` for parity with the HTTP path.

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tropel_sdk::{
    Body, Protocol, ProtocolOutcome, ProtocolRegistration, Request, Response, Result, Sample,
    SampleType, TagMap, TropelError,
};

/// Default time to wait for responses after sending messages.
const DEFAULT_WAIT: Duration = Duration::from_secs(1);
/// Cap on bytes captured into `pm.response` (1 MiB) — protects the bridge
/// from unbounded buffers on chatty servers.
const MAX_BODY_BYTES: usize = 1024 * 1024;

/// WebSocket protocol executor.
#[derive(Default)]
pub struct WebSocketProtocol;

#[async_trait]
impl Protocol for WebSocketProtocol {
    fn scheme(&self) -> &str {
        "ws"
    }

    async fn execute(
        &self,
        req: &Request,
        config: Option<&serde_json::Value>,
    ) -> Result<ProtocolOutcome> {
        let start = Instant::now();

        // ── Validate scheme ──
        let is_tls = req.url.starts_with("wss://");
        if !req.url.starts_with("ws://") && !is_tls {
            return Err(TropelError::Config(format!(
                "not a WebSocket URL: '{}'",
                req.url
            )));
        }

        // ── Build the handshake request, carrying request headers ──
        let mut handshake = req.url.clone().into_client_request().map_err(|e| {
            TropelError::Config(format!("invalid WebSocket URL '{}': {}", req.url, e))
        })?;
        for (k, v) in &req.headers {
            // http 1.x `IntoHeaderName` is only implemented for `&'static
            // str` and `HeaderName` — convert the (borrowed, method-scoped)
            // request header key into an owned `HeaderName`.
            if let (Ok(hname), Ok(hv)) = (
                http::HeaderName::from_bytes(k.as_bytes()),
                http::HeaderValue::from_str(v),
            ) {
                handshake.headers_mut().insert(hname, hv);
            }
        }

        // ── Connect (auto-TLS on wss://) ──
        let connect_start = Instant::now();
        let (mut ws, handshake_resp) =
            tokio_tungstenite::connect_async(handshake)
                .await
                .map_err(|e| {
                    TropelError::Extension(format!("WebSocket connect to '{}': {}", req.url, e))
                })?;
        let connecting = connect_start.elapsed();
        let session_status = handshake_resp.status().as_u16();

        // ── Messages to send: config["messages"] > request body ──
        let messages: Vec<String> = match config
            .and_then(|c| c.get("messages"))
            .and_then(|m| m.as_array())
        {
            Some(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            None => match &req.body {
                Some(Body::Raw(s)) => vec![s.clone()],
                _ => Vec::new(),
            },
        };
        let send_binary = config
            .and_then(|c| c.get("binary"))
            .and_then(|b| b.as_bool())
            .unwrap_or(false);

        let mut bytes_sent: u64 = 0;
        for m in &messages {
            bytes_sent += m.len() as u64;
            let msg = if send_binary {
                Message::Binary(m.clone().into_bytes().into())
            } else {
                Message::Text(m.as_str().into())
            };
            ws.send(msg)
                .await
                .map_err(|e| TropelError::Extension(format!("WebSocket send: {}", e)))?;
        }
        let msgs_sent = messages.len() as u64;

        // ── Read responses until the peer closes or the window elapses ──
        //
        // Event-driven: the session ends the moment the server sends a close
        // frame, closes the stream, or errors — not when an arbitrary timer
        // expires. `wait` is an optional *ceiling* (default 1s); `"until-close"`
        // (or an absent `wait`) removes the ceiling entirely, so a chatty or
        // long-lived server drives the session length. The request timeout and
        // MAX_BODY_BYTES remain as hard safety caps so an unresponsive peer
        // can never hang the VU.
        let wait: Option<Duration> =
            match config.and_then(|c| c.get("wait")).and_then(|w| w.as_str()) {
                Some("until-close") => None,
                Some(s) => parse_duration(s).or(Some(DEFAULT_WAIT)),
                // Absent `wait` keeps the historical 1s default — only the
                // explicit "until-close" opts into an unbounded (request-timeout
                // capped) session.
                None => Some(DEFAULT_WAIT),
            };
        // Hard ceiling when the wait window is unbounded: the request timeout.
        let hard_cap = req.timeout.unwrap_or(Duration::from_secs(30));
        let session_deadline = tokio::time::Instant::now() + wait.unwrap_or(hard_cap);
        let mut received: Vec<String> = Vec::new();
        let mut bytes_received: u64 = 0;
        loop {
            if tokio::time::Instant::now() >= session_deadline {
                break;
            }
            let remaining = session_deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, ws.next()).await {
                Ok(Some(Ok(Message::Text(t)))) => {
                    let s = t.to_string();
                    bytes_received += s.len() as u64;
                    received.push(s);
                }
                Ok(Some(Ok(Message::Binary(b)))) => {
                    bytes_received += b.len() as u64;
                    received.push(format!("<binary {} bytes>", b.len()));
                }
                Ok(Some(Ok(Message::Close(_)))) => break, // server closed — event-driven end
                Ok(Some(Ok(_))) => {}                     // ping/pong frames
                Ok(Some(Err(e))) => {
                    tracing::debug!("WebSocket read error: {}", e);
                    break;
                }
                Ok(None) => break, // stream ended — event-driven end
                Err(_) => break,   // window elapsed
            }
            if bytes_received >= MAX_BODY_BYTES as u64 {
                break;
            }
        }
        let msgs_received = received.len() as u64;

        // ── Close handshake (best-effort) ──
        let _ = ws.close(None).await;
        let duration = start.elapsed();
        let ok = session_status == 101;

        // ── Build the response for pm.response ──
        let body = serde_json::to_vec(&received).unwrap_or_default();
        let response = Response {
            url: req.url.clone(),
            status_code: session_status,
            status_text: if ok {
                "Switching Protocols".into()
            } else {
                "ERROR".into()
            },
            headers: handshake_resp
                .headers()
                .iter()
                .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
                .collect(),
            body: body.clone(),
            text_cache: std::cell::OnceCell::new(),
            json_cache: std::cell::OnceCell::new(),
            response_time: duration,
            timings: None,
            cookies: vec![],
            size: body.len() as u64,
            redirects: vec![],
        };

        // ── Metrics ──
        let now = std::time::SystemTime::now();
        let mut tags = TagMap::with_capacity(5);
        tags.insert("url", req.url.clone());
        tags.insert("method", "GET");
        tags.insert("status", session_status.to_string());
        tags.insert("name", req.url.clone());
        tags.insert("group", "ws");
        let tags = std::sync::Arc::new(tags);

        let samples = vec![
            Sample {
                metric: "ws_connecting".into(),
                value: connecting.as_micros() as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Trend,
            },
            Sample {
                metric: "ws_msgs_sent".into(),
                value: msgs_sent as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "ws_msgs_received".into(),
                value: msgs_received as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "ws_bytes_sent".into(),
                value: bytes_sent as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "ws_bytes_received".into(),
                value: bytes_received as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "ws_sessions".into(),
                value: 1.0,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "ws_req_duration".into(),
                value: duration.as_micros() as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Trend,
            },
            Sample {
                metric: "ws_req_failed".into(),
                value: if ok { 0.0 } else { 1.0 },
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Rate,
            },
            Sample {
                metric: "data_sent".into(),
                value: bytes_sent as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "data_received".into(),
                value: bytes_received as f64,
                tags,
                timestamp: now,
                sample_type: SampleType::Counter,
            },
        ];

        Ok(ProtocolOutcome {
            samples,
            response: Some(response),
        })
    }
}

/// Parse a duration string (`"1s"`, `"500ms"`, `"1m30s"`, `"100"` = 100 s)
/// into a `Duration`. Delegates to the canonical `tropel_core::parse_duration`
/// so every consumer shares ONE implementation and the same unit semantics
/// (the old local copy treated a bare number as MILLISECONDS — a third
/// divergent impl, backlog line 136).
fn parse_duration(s: &str) -> Option<Duration> {
    tropel_sdk::parse_duration(s).ok()
}

/// Inventory factory — must be a `fn` pointer for `inventory::submit!`.
fn ws_factory() -> Box<dyn Protocol> {
    Box::new(WebSocketProtocol)
}

inventory::submit!(ProtocolRegistration::new("ws", ws_factory));
