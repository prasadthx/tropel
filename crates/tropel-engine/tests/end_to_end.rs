//! # End-to-end test
//!
//! Exercises the FULL pipeline the way a user would drive it: a Postman
//! collection file is parsed by the postman adapter, executed by 2 VUs
//! against a real local HTTP server, with a `{{header}}` variable, a
//! prerequest script that sets a variable, a `pm.test`, and a threshold.
//!
//! Two historical regressions are covered elsewhere by dedicated unit
//! tests: N2 (the script interrupt timer keyed off context-creation time,
//! which killed every eval ~10s in) is pinned by
//! `tropel_js::context::reset_interrupt_keeps_evals_alive_past_original_deadline`;
//! N1 (a shared response slot letting one VU read another's response) is
//! covered by the per-VU response assertions. Keeping this e2e short (3s)
//! keeps `cargo test` fast.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tropel_core::config::{
    ExecutionConfig, JobConfig, OutputConfig, ThinkTimeConfig, ThresholdConfig,
};
use tropel_core::Result;
use tropel_engine::Engine;
use tropel_ext::registry::ExtensionRegistry;
use tropel_metrics::thresholds::evaluate_thresholds;

/// Minimal HTTP/1.1 server that records the `X-E2E` header value it sees on
/// each request, then answers `200 {"ok":true}`. Header values are pushed
/// into the shared `seen` list so the test can assert the resolved
/// `{{header}}` variable actually reached the wire.
///
/// Also tracks the PEAK number of simultaneously-open connections via an
/// `AtomicUsize` guard (returned alongside the address) so the test can
/// assert REAL concurrency — `http_reqs >= vus` only proves requests fired,
/// not that `vus` were actually concurrent.
async fn start_echo_server(seen: Arc<Mutex<Vec<String>>>) -> (std::net::SocketAddr, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let peak_out = peak.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let seen = seen.clone();
            let active = active.clone();
            let peak = peak.clone();
            tokio::spawn(async move {
                let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                let mut buf = [0u8; 4096];
                // Keep-alive loop (like behavior_parity's echo server): the
                // client pools HTTP/1.1 connections, and a server that drops
                // the socket after one response races the client's reuse —
                // occasionally failing a pooled request and flaking
                // `checks_failed == 0` assertions. Run the per-connection
                // body in a block so the active-count decrement is guaranteed
                // on every exit path (clean close, read error, write error).
                let conn_result: std::io::Result<()> = async {
                    loop {
                        // Read until the request-head terminator (headers end
                        // at CRLF CRLF). A fresh head per request so a split
                        // packet never loses the header across requests.
                        let mut head = Vec::new();
                        loop {
                            let n = sock.read(&mut buf).await?;
                            if n == 0 {
                                return Ok(());
                            }
                            head.extend_from_slice(&buf[..n]);
                            if head.windows(4).any(|w| w == b"\r\n\r\n") {
                                break;
                            }
                        }
                        let text = String::from_utf8_lossy(&head).to_string();
                        for line in text.lines() {
                            let lower = line.to_ascii_lowercase();
                            if let Some(v) = lower.strip_prefix("x-e2e:") {
                                // Poison-tolerant: a panicked test thread must
                                // not let this connection task die before the
                                // active-count decrement (which would inflate
                                // peak forever).
                                seen.lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .push(v.trim().to_string());
                            }
                        }
                        // Hold the connection open so concurrent VUs overlap —
                        // the same determinism trick as behavior_parity's
                        // peak server.
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        let body = r#"{"ok":true}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        sock.write_all(resp.as_bytes()).await?;
                    }
                }
                .await;
                let _ = conn_result;
                active.fetch_sub(1, Ordering::SeqCst);
            });
        }
    });
    (addr, peak_out)
}

/// Server that records the `Authorization` header value it sees on each
/// request (used to prove prerequest-added pm.request headers reach the wire).
async fn start_auth_capture_server(seen: Arc<Mutex<Vec<String>>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            let seen = seen.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0u8; 4096];
                loop {
                    let n = match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    head.extend_from_slice(&buf[..n]);
                    if head.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let text = String::from_utf8_lossy(&head).to_string();
                for line in text.lines() {
                    // Match the header name case-insensitively but capture the
                    // VALUE in its ORIGINAL case (lowercasing the whole line
                    // would corrupt "Bearer s3cret" into "bearer s3cret").
                    if line.to_ascii_lowercase().starts_with("authorization:") {
                        if let Some(idx) = line.find(':') {
                            seen.lock()
                                .unwrap()
                                .push(line[idx + 1..].trim().to_string());
                        }
                    }
                }
                let body = r#"{"ok":true}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    addr
}

/// Write a minimal Postman collection to a temp file and return its path.
///
/// The single request sends `X-E2E: {{header}}`; its prerequest script sets
/// a variable (`pm.variables.set`), and its test script asserts both the
/// HTTP status and that the prerequest-set variable is still visible — the
/// exact seam where a broken prerequest→test bridge or a broken response
/// slot would surface.
fn write_collection(base: &str, tag: &str) -> String {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("tropel-e2e-{}-{}.json", std::process::id(), tag));
    let url = format!("{base}/");
    let collection = serde_json::json!({
        "info": {
            "name": "e2e",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        },
        "item": [{
            "name": "req1",
            "request": {
                "method": "GET",
                "url": url,
                "header": [{"key": "X-E2E", "value": "{{header}}"}]
            },
            "event": [
                {
                    "listen": "prerequest",
                    "script": {
                        "exec": ["pm.variables.set('pre_req_var', 'set-in-prerequest');"],
                        "type": "text/javascript"
                    }
                },
                {
                    "listen": "test",
                    "script": {
                        "exec": [
                            "pm.test('status is 200', function () { pm.response.to.have.status(200); });",
                            "pm.test('prereq var visible', function () { return pm.variables.get('pre_req_var') === 'set-in-prerequest'; });"
                        ],
                        "type": "text/javascript"
                    }
                }
            ]
        }]
    });
    let json = serde_json::to_string(&collection).unwrap();
    std::fs::write(&path, json).unwrap();
    path.to_string_lossy().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_to_end_two_vu_with_header_check_and_threshold() -> Result<()> {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let (srv, peak) = start_echo_server(seen.clone()).await;
    let coll = write_collection(&format!("http://{srv}"), "two-vu");

    // Threshold on http_req_duration (samples are MICROSECONDS): a generous
    // 5 s ceiling that still reflects REAL latency — a hardcoded pass or an
    // empty series (actual = 0) must fail the "> 0 real samples" asserts.
    let mut thresholds = HashMap::new();
    thresholds.insert(
        "http_req_duration".to_string(),
        ThresholdConfig {
            expression: "http_req_duration.p95 < 5000000".to_string(),
            abort_on_fail: false,
            delay_abort_eval: None,
        },
    );

    let mut env = HashMap::new();
    env.insert("header".to_string(), "e2e-header-value".to_string());

    let config = JobConfig {
        input: coll.clone(),
        input_type: Some("postman".to_string()),
        execution: ExecutionConfig::ConstantVus {
            vus: 2,
            // Short run to keep cargo test fast — the ~10s interrupt-timer
            // regression has dedicated unit coverage in tropel-js.
            duration: "3s".to_string(),
            graceful_stop: Some("5s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
        env,
        thresholds,
        // Keep the test output clean: no stdout summary stream.
        output: OutputConfig {
            reporters: vec![],
            ..Default::default()
        },
        ..Default::default()
    };

    let engine = Engine::new(ExtensionRegistry::new());
    let result = engine.run(&config).await?;
    let m = &result.metrics;

    // 1. The {{header}} variable was resolved and the header sent.
    {
        let seen_guard = seen.lock().unwrap();
        assert!(
            seen_guard.iter().any(|v| v == "e2e-header-value"),
            "server saw headers {:?}, expected the resolved 'e2e-header-value'",
            *seen_guard
        );
    }

    // 2. The pm.test checks ran (and the prerequest→test var bridge worked).
    //    checks_failed == 0 is essential: the test script has TWO pm.tests —
    //    a status check (passes regardless) and a prerequest-var check. If
    //    `pm.variables.get` were broken, only the second would fail, leaving
    //    checks_passed > 0 true. Zero failures pins the bridge down.
    assert!(
        m.checks_total > 0,
        "checks_total > 0, got {}",
        m.checks_total
    );
    assert_eq!(
        m.checks_failed, 0,
        "all checks passed, got {} failed of {} total",
        m.checks_failed, m.checks_total
    );

    // 3. The threshold reflected REAL latency: the http_req_duration series
    //    has samples with a measured max, and the evaluation passes.
    let dur = m
        .http_req_duration
        .as_ref()
        .expect("http_req_duration summary present");
    assert!(dur.count > 0, "http_req_duration has samples");
    assert!(
        dur.max > 0,
        "http_req_duration max > 0 (real latency measured)"
    );

    let threshold_results = evaluate_thresholds(&result.effective_thresholds, m);
    let t = threshold_results
        .iter()
        .find(|t| t.name == "http_req_duration")
        .expect("threshold evaluated");
    assert!(
        t.passed,
        "threshold '{}' passed (actual={} < {})",
        t.expression, t.actual, t.threshold
    );

    // Sanity: both VUs actually made requests.
    // Real concurrency: the server observed >= 2 simultaneously-open
    // connections. `http_reqs >= 2` only proves requests fired — a
    // sequential runner would pass it; two overlapping connections prove
    // the 2 configured VUs actually ran in parallel.
    let peak_obs = peak.load(Ordering::SeqCst);
    assert!(
        peak_obs >= 2,
        "peak concurrent connections >= 2, got {} (vus actually parallel)",
        peak_obs
    );

    let _ = std::fs::remove_file(&coll);
    Ok(())
}

/// Backlog line 145: a prerequest script using `pm.request.headers.add(...)`
/// to attach an Authorization header must ACTUALLY send that header — the
/// primary purpose of pm.request. The runner used to rebuild the wire request
/// from the static collection item, silently discarding every mutation; this
/// test pins the fix by asserting the server received the prerequest header.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prerequest_pm_request_header_reaches_the_wire() -> Result<()> {
    let seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let srv = start_auth_capture_server(seen.clone()).await;

    let dir = std::env::temp_dir();
    let path = dir.join(format!("tropel-e2e-prereq-hdr-{}-prereq.json", std::process::id()));
    let url = format!("http://{srv}/");
    let collection = serde_json::json!({
        "info": {
            "name": "prereq-hdr",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        },
        "item": [{
            "name": "req1",
            "request": {
                "method": "GET",
                "url": url,
                "header": []
            },
            "event": [
                {
                    "listen": "prerequest",
                    "script": {
                        "exec": [
                            // Literal token: the point of this test is that the
                            // prerequest pm.request mutation REACHES THE WIRE,
                            // not env resolution (pm.environment.get reads
                            // script-set state, not JobConfig env).
                            "pm.request.headers.add({ key: 'Authorization', value: 'Bearer fixed-token' });"
                        ],
                        "type": "text/javascript"
                    }
                },
                {
                    "listen": "test",
                    "script": {
                        "exec": [
                            "pm.test('status is 200', function () { pm.response.to.have.status(200); });"
                        ],
                        "type": "text/javascript"
                    }
                }
            ]
        }]
    });
    let coll = path.to_string_lossy().to_string();
    std::fs::write(&path, serde_json::to_string(&collection).unwrap()).unwrap();

    let config = JobConfig {
        input: coll.clone(),
        input_type: Some("postman".to_string()),
        execution: ExecutionConfig::ConstantVus {
            vus: 2,
            duration: "3s".to_string(),
            graceful_stop: Some("10s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
        env: HashMap::new(),
        thresholds: HashMap::new(),
        output: OutputConfig {
            reporters: vec![],
            ..Default::default()
        },
        ..Default::default()
    };

    let engine = Engine::new(ExtensionRegistry::new());
    let result = engine.run(&config).await?;

    {
        let seen_guard = seen.lock().unwrap();
        assert!(
            seen_guard.iter().any(|v| v == "Bearer fixed-token"),
            "server saw Authorization headers {:?}; the prerequest pm.request.headers.add was dropped by the runner",
            *seen_guard
        );
    }
    assert!(
        result.metrics.checks_failed == 0,
        "all checks passed, got {} failed of {} total",
        result.metrics.checks_failed,
        result.metrics.checks_total
    );

    let _ = std::fs::remove_file(&coll);
    Ok(())
}
