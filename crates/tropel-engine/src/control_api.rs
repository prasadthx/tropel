//! Minimal runtime control API — k6 REST `/v1/status` parity.
//!
//! Binds `127.0.0.1:<port>` and serves:
//! - `GET  /v1/status`  → k6 JSON:API shape
//!   `{"data":{"type":"status","id":"default","attributes":{...}}}`
//!   (vus / vus-max / paused / running / stopped / tainted)
//! - `PATCH /v1/status` → k6 envelope `{"data":{"attributes":{...}}}` or a
//!   flat `{"vus":N,"max":M,"vus-max":N,"paused":bool}` — adjusts the
//!   externally-controlled scheduler's VU pool / pause state at runtime.
//!   `max` is clamped to the configured `max_vus` ceiling, so a client can
//!   never grow the pool past the run's cap. Both `max` (legacy) and
//!   `vus-max` (k6's JSON:API field name) are accepted, and the status doc
//!   emits BOTH so old and new clients work.
//!
//! Everything else returns 404. This is intentionally dependency-free: a
//! hand-rolled HTTP/1.1 reader keeps the control surface small and avoids
//! pulling a web framework into the engine for one endpoint.

use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tropel_core::Result;
use tropel_executor::scheduler::VUScheduler;

/// Handle the control server task. Runs until the listener errors or the
/// task is aborted by the scenario finishing.
pub async fn serve_control_api(port: u16, scheduler: Arc<VUScheduler>) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port);
    // Bind failure must be visible: the spawned task's JoinHandle is only
    // aborted (never awaited) by the engine, so a port conflict would
    // otherwise leave the run silently without a control API.
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("control API: failed to bind {}: {}", addr, e);
            return Err(tropel_core::TropelError::Config(format!(
                "control API: failed to bind {}: {}",
                addr, e
            )));
        }
    };
    tracing::info!("Control API listening on http://{addr}");

    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(x) => x,
            Err(e) => {
                tracing::debug!("control API: accept error: {}", e);
                continue;
            }
        };
        let sched = scheduler.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_conn(stream, &sched).await {
                tracing::debug!("control API: connection error: {}", e);
            }
        });
    }
}

/// Serve one HTTP connection. Reads the request line + headers + body
/// (Content-Length), routes it, writes the response, closes.
async fn handle_conn(stream: TcpStream, sched: &Arc<VUScheduler>) -> Result<()> {
    let mut reader = BufReader::new(stream);
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).await? == 0 {
        return Ok(());
    }
    let request_line = request_line.trim_end().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    // Read headers, discover Content-Length.
    let mut content_length: usize = 0;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            break;
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some(v) = line.to_ascii_lowercase().strip_prefix("content-length:") {
            content_length = v.trim().parse().unwrap_or(0);
        }
    }

    // Read the body.
    let mut body = Vec::new();
    if content_length > 0 {
        body.resize(content_length, 0);
        reader.read_exact(&mut body).await?;
    }

    let (status, response_body) = route(&method, &path, &body, sched);

    let response = format!(
        "HTTP/1.1 {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        status,
        response_body.len(),
        response_body
    );
    let mut out = reader.into_inner();
    out.write_all(response.as_bytes()).await?;
    out.flush().await?;
    Ok(())
}

/// Route a control request and return (status line, JSON body).
fn route(method: &str, path: &str, body: &[u8], sched: &Arc<VUScheduler>) -> (String, String) {
    match (method, path) {
        ("GET", "/v1/status") => ("200 OK".to_string(), status_json(sched)),
        ("PATCH", "/v1/status") => match parse_status_body(body) {
            Some(patch) => {
                // Apply only the fields the client sent (k6 allows partial
                // PATCHes: just vus, just max, just paused, or any combo).
                if patch.vus.is_some() || patch.max.is_some() {
                    let vus = patch.vus.unwrap_or_else(|| sched.control_target());
                    let max = patch.max.unwrap_or_else(|| sched.control_max());
                    sched.set_control_target(vus, max);
                    tracing::info!("Control API: set VUs target={} max={}", vus, max);
                }
                if let Some(paused) = patch.paused {
                    sched.set_paused(paused);
                    tracing::info!("Control API: paused={}", paused);
                }
                ("200 OK".to_string(), status_json(sched))
            }
            None => (
                "400 Bad Request".to_string(),
                "{\"error\":\"expected {\\\"vus\\\":N,\\\"max\\\":M,\\\"paused\\\":bool}\"}"
                    .to_string(),
            ),
        },
        ("POST", "/v1/stop") => {
            sched.request_stop();
            ("200 OK".to_string(), status_json(sched))
        }
        _ => (
            "404 Not Found".to_string(),
            r#"{"error":"not found"}"#.to_string(),
        ),
    }
}

/// Render the k6 JSON:API status document. `running` is false once a stop
/// has been requested; `tainted` reflects real threshold failures
/// (backlog line 154 — was hardcoded null).
fn status_json(sched: &Arc<VUScheduler>) -> String {
    let vus = sched.control_target();
    let max = sched.control_max();
    let paused = sched.is_paused();
    let stopped = sched.is_stop_requested();
    let running = !stopped;
    let tainted = sched.is_tainted();
    format!(
        r#"{{"data":{{"type":"status","id":"default","attributes":{{"vus":{},"vus-max":{},"max":{},"paused":{},"running":{},"stopped":{},"tainted":{}}}}}}}"#,
        vus, max, max, paused, running, stopped, tainted
    )
}

/// A parsed PATCH /v1/status body. All fields optional — k6 allows partial
/// patches (`{"paused":true}` alone is valid).
#[derive(Debug, Clone, Copy, PartialEq)]
struct StatusPatch {
    vus: Option<u32>,
    max: Option<u32>,
    paused: Option<bool>,
}

/// Parse a PATCH /v1/status body. Accepts the flat form
/// `{"vus":5,"vus-max":10}`, the k6 envelope
/// `{"data":{"attributes":{"vus":5,"vus-max":10}}}` — and the legacy
/// `max` key (backlog line 154: k6's JSON:API field is `vus-max`; Tropel's
/// old `max` stays accepted). `vus-max` wins over `max` when both are sent.
/// Returns `None` when the body is unparseable or carries none of the known
/// fields (so a garbage body can't be silently swallowed).
fn parse_status_body(body: &[u8]) -> Option<StatusPatch> {
    let text = std::str::from_utf8(body).ok()?;
    let json: serde_json::Value = serde_json::from_str(text).ok()?;

    // k6 envelope: {"data":{"attributes":{...}}}
    let attrs = json
        .get("data")
        .and_then(|d| d.get("attributes"))
        .or(Some(&json))?;

    let vus = attrs.get("vus").and_then(|v| v.as_u64()).map(|v| v as u32);
    let max = attrs
        .get("vus-max")
        .and_then(|v| v.as_u64())
        .map(|v| v as u32)
        .or_else(|| attrs.get("max").and_then(|v| v.as_u64()).map(|v| v as u32));
    let paused = attrs.get("paused").and_then(|v| v.as_bool());

    if vus.is_none() && max.is_none() && paused.is_none() {
        return None;
    }
    Some(StatusPatch { vus, max, paused })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_body() {
        assert_eq!(
            parse_status_body(br#"{"vus":5,"max":20}"#),
            Some(StatusPatch {
                vus: Some(5),
                max: Some(20),
                paused: None
            })
        );
    }

    #[test]
    fn parses_k6_envelope() {
        assert_eq!(
            parse_status_body(br#"{"data":{"attributes":{"vus":3,"max":9}}}"#),
            Some(StatusPatch {
                vus: Some(3),
                max: Some(9),
                paused: None
            })
        );
    }

    #[test]
    fn parses_paused_only() {
        assert_eq!(
            parse_status_body(br#"{"paused":true}"#),
            Some(StatusPatch {
                vus: None,
                max: None,
                paused: Some(true)
            })
        );
    }

    #[test]
    fn partial_patch_with_only_vus_is_valid() {
        assert_eq!(
            parse_status_body(br#"{"vus":5}"#),
            Some(StatusPatch {
                vus: Some(5),
                max: None,
                paused: None
            })
        );
    }

    #[test]
    fn rejects_garbage_and_unknown_only() {
        assert_eq!(parse_status_body(br#"{"foo":1}"#), None); // no known field
        assert_eq!(parse_status_body(b"garbage"), None);
        assert_eq!(parse_status_body(b"{}"), None);
    }

    #[test]
    fn status_json_is_k6_shape() {
        let sched = VUScheduler::new(
            &tropel_core::config::ExecutionConfig::ExternallyControlled {
                vus: 2,
                max_vus: 10,
                duration: None,
                graceful_stop: None,
                think_time: Default::default(),
            },
        );
        let sched = Arc::new(sched);
        sched.set_control_target(4, 10);
        let body = status_json(&sched);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        let attrs = &v["data"]["attributes"];
        assert_eq!(attrs["type"], serde_json::Value::Null); // type/id live on data, not attributes
        assert_eq!(v["data"]["type"], "status");
        assert_eq!(v["data"]["id"], "default");
        assert_eq!(attrs["vus"], 4);
        // Backlog line 154: k6's JSON:API field is `vus-max`; Tropel also
        // keeps emitting `max` for back-compat.
        assert_eq!(attrs["vus-max"], 10);
        assert_eq!(attrs["max"], 10);
        assert_eq!(attrs["paused"], false);
        assert_eq!(attrs["running"], true);
        assert_eq!(attrs["stopped"], false);
        assert_eq!(attrs["tainted"], false);

        // Taint is real: once a threshold fails, the status shows it.
        sched.set_tainted();
        let body = status_json(&sched);
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["data"]["attributes"]["tainted"], true);
    }
}
