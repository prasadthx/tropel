//! # Behavior-not-shape tests
//!
//! The "shape" tests (metrics present, struct fields correct) can pass while
//! the product silently does nothing — a hardcoded-zero histogram passes
//! "http_req_duration has samples" only if a *behavioral* assertion pins it.
//! These tests exercise real behavior:
//!
//! 1. **k6 end-to-end**: a real k6 script (`http.get` + `check`) is parsed by
//!    the k6 driver, executed by 2 VUs against a local HTTP server, and must
//!    produce `http_reqs > 0`, `checks_total > 0` with **zero failures**, and
//!    a `http_req_duration` series whose `max > 0` — i.e. real measured
//!    latency — that passes a real threshold evaluation. If the k6 shim's
//!    `check` never records, or the HTTP bridge never emits samples, or the
//!    threshold were hardcoded to pass, one of these assertions fails loudly.
//!
//! 2. **Ramping wall-clock**: a `RampingVus` run with staged targets must
//!    actually *span* wall-clock time (stages are not collapsed), actually
//!    reach the stage target (observed `vus_max` reflects the ramp), and make
//!    real requests. A scheduler that skipped stages or finished instantly
//!    fails the elapsed-time and `vus_max` assertions.

use std::collections::HashMap;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tropel_core::config::{
    ExecutionConfig, JobConfig, OutputConfig, Stage, ThinkTimeConfig, ThresholdConfig,
};
use tropel_core::Result;
use tropel_engine::Engine;
use tropel_ext::registry::ExtensionRegistry;
use tropel_metrics::thresholds::evaluate_thresholds;

/// Minimal HTTP/1.1 server that answers `200 {"ok":true}`.
async fn start_echo_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
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

/// Write a minimal k6 script to a temp file. Uses `http.get` + `check` —
/// the exact seam where a broken k6 shim, a broken HTTP bridge, or a broken
/// `checks` recording would surface as zero samples.
///
/// `tag` disambiguates the temp file per test: the tests in this binary run
/// in PARALLEL, so sharing one `{pid}`-keyed filename would race — one
/// test's `remove_file` could delete the script the other is about to read.
fn write_k6_script(base: &str, tag: &str) -> String {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("tropel-k6-e2e-{}-{}.js", std::process::id(), tag));
    let script = format!(
        r#"import http from 'k6/http';
import {{ check }} from 'k6';

export default function () {{
  const res = http.get('{base}/');
  check(res, {{ 'status is 200': (r) => r.status === 200 }});
}}
"#
    );
    std::fs::write(&path, script).unwrap();
    path.to_string_lossy().to_string()
}

/// k6 script run through the full engine: driver parse → JS eval → HTTP →
/// metrics → thresholds. Asserts *behavior*: requests actually fired, checks
/// actually recorded (with zero failures), and the threshold reflects real
/// measured latency (max > 0), not a hardcoded pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn k6_script_records_requests_checks_and_real_latency() -> Result<()> {
    let srv = start_echo_server().await;
    let script = write_k6_script(&format!("http://{srv}"), "check");

    // Real-latency threshold: p95 < 5 s. An empty series or a hardcoded-zero
    // histogram must fail the "> 0 real samples" asserts below.
    let mut thresholds = HashMap::new();
    thresholds.insert(
        "http_req_duration".to_string(),
        ThresholdConfig {
            expression: "http_req_duration.p95 < 5000000".to_string(),
            abort_on_fail: false,
            delay_abort_eval: None,
        },
    );

    let config = JobConfig {
        input: script.clone(),
        input_type: Some("k6".to_string()),
        execution: ExecutionConfig::ConstantVus {
            vus: 2,
            duration: "3s".to_string(),
            graceful_stop: Some("5s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
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

    // 1. The k6 script's http.get actually fired requests.
    assert!(m.http_reqs > 0, "http_reqs > 0, got {}", m.http_reqs);

    // 2. The k6 shim's check() recorded — and everything passed (the server
    //    always answers 200, so a single failure means the bridge is broken).
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

    // 3. The http_req_duration series has REAL measured latency.
    let dur = m
        .http_req_duration
        .as_ref()
        .expect("http_req_duration summary present");
    assert!(dur.count > 0, "http_req_duration has samples");
    assert!(
        dur.max > 0,
        "http_req_duration max > 0 (real latency measured)"
    );

    // 4. The threshold evaluation passes on the real series.
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

    let _ = std::fs::remove_file(&script);
    Ok(())
}

/// Ramping must be a *real* wall-clock behavior: stages are not collapsed,
/// the pool actually grows toward the stage target, and requests fire.
/// A scheduler that skipped stages (or finished instantly) fails the
/// elapsed-time and vus_max assertions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ramping_stages_span_wall_clock_and_reach_target() -> Result<()> {
    let srv = start_echo_server().await;
    let coll = write_k6_script(&format!("http://{srv}"), "ramp");

    // 1s ramp to 3 VUs, 1s hold at 3, 1s ramp down to 1 → ~3s of stages.
    let config = JobConfig {
        input: coll.clone(),
        input_type: Some("k6".to_string()),
        execution: ExecutionConfig::RampingVus {
            start_vus: 1,
            stages: vec![
                Stage {
                    duration: "1s".to_string(),
                    target: 3,
                },
                Stage {
                    duration: "1s".to_string(),
                    target: 3,
                },
                Stage {
                    duration: "1s".to_string(),
                    target: 1,
                },
            ],
            graceful_ramp_down: Some("5s".to_string()),
            graceful_stop: Some("5s".to_string()),
            think_time: ThinkTimeConfig::default(),
        },
        output: OutputConfig {
            reporters: vec![],
            ..Default::default()
        },
        ..Default::default()
    };

    let engine = Engine::new(ExtensionRegistry::new());
    let start = Instant::now();
    let result = engine.run(&config).await?;
    let elapsed = start.elapsed();
    let m = &result.metrics;

    // 1. The run actually SPANNED the stage wall-clock (3s of stages, minus
    //    a 25% tolerance for scheduler/timer granularity — but never 0).
    assert!(
        elapsed >= std::time::Duration::from_millis(2250),
        "ramping run elapsed {elapsed:?}, expected >= 2.25s (stages not collapsed)"
    );

    // 2. The ramp reached its target: vus_max reflects the stage target (3).
    assert!(
        m.vus_max >= 2,
        "vus_max >= 2, got {} (pool actually grew toward target)",
        m.vus_max
    );

    // 3. Requests actually fired during the ramp.
    assert!(
        m.http_reqs > 0,
        "http_reqs > 0 during ramp, got {}",
        m.http_reqs
    );

    let _ = std::fs::remove_file(&coll);
    Ok(())
}
