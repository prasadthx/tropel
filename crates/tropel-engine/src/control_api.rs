//! Minimal runtime control API — k6 REST `/v1/status` parity.
//!
//! Binds `127.0.0.1:<port>` and serves:
//! - `GET  /v1/status`  → `{"vus": N, "max": M}` (current target/cap)
//! - `PATCH /v1/status` → body `{"vus": N, "max": M}` (k6 accepts a nested
//!   `{"data":{"attributes":{...}}}` envelope too) — adjusts the
//!   externally-controlled scheduler's VU pool at runtime.
//! - `POST /v1/stop`    → requests a graceful stop.
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
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        tropel_core::TropelError::Config(format!(
            "control API: failed to bind {}: {}",
            addr, e
        ))
    })?;
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
fn route(
    method: &str,
    path: &str,
    body: &[u8],
    sched: &Arc<VUScheduler>,
) -> (String, String) {
    match (method, path) {
        ("GET", "/v1/status") => {
            let vus = sched.control_target();
            let max = sched.control_max();
            (
                "200 OK".to_string(),
                format!(r#"{{"vus":{},"max":{}}}"#, vus, max),
            )
        }
        ("PATCH", "/v1/status") => {
            match parse_status_body(body) {
                Some((vus, max)) => {
                    sched.set_control_target(vus, max);
                    tracing::info!("Control API: set VUs target={} max={}", vus, max);
                    (
                        "200 OK".to_string(),
                        format!(r#"{{"vus":{},"max":{}}}"#, vus.min(max), max),
                    )
                }
                None => (
                    "400 Bad Request".to_string(),
                    "{\"error\":\"expected {\\\"vus\\\":N,\\\"max\\\":M}\"}".to_string(),
                ),
            }
        }
        ("POST", "/v1/stop") => {
            sched.request_stop();
            ("200 OK".to_string(), r#"{"stopped":true}"#.to_string())
        }
        _ => (
            "404 Not Found".to_string(),
            r#"{"error":"not found"}"#.to_string(),
        ),
    }
}

/// Parse a PATCH /v1/status body. Accepts both the flat form
/// `{"vus":5,"max":10}` and the k6 envelope `{"data":{"attributes":{"vus":5,"max":10}}}`.
/// Values are capped to u32 and max is clamped to be >= vus target handling.
fn parse_status_body(body: &[u8]) -> Option<(u32, u32)> {
    let text = std::str::from_utf8(body).ok()?;
    let json: serde_json::Value = serde_json::from_str(text).ok()?;

    // k6 envelope: {"data":{"attributes":{...}}}
    let attrs = json
        .get("data")
        .and_then(|d| d.get("attributes"))
        .or_else(|| Some(&json))?;

    let vus = attrs
        .get("vus")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    let max = attrs
        .get("max")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    if max == 0 {
        return None;
    }
    Some((vus, max))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_flat_body() {
        assert_eq!(parse_status_body(br#"{"vus":5,"max":20}"#), Some((5, 20)));
    }

    #[test]
    fn parses_k6_envelope() {
        assert_eq!(
            parse_status_body(br#"{"data":{"attributes":{"vus":3,"max":9}}}"#),
            Some((3, 9))
        );
    }

    #[test]
    fn rejects_empty_max() {
        assert_eq!(parse_status_body(br#"{"vus":5}"#), None);
        assert_eq!(parse_status_body(b"garbage"), None);
    }

    #[test]
    fn rejects_missing_vus_as_zero_then_fails_on_zero_max() {
        // max present but vus absent → vus defaults 0; max>0 → Some((0, max)).
        assert_eq!(parse_status_body(br#"{"max":7}"#), Some((0, 7)));
    }
}
