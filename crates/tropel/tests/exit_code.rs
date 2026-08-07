//! # Exit-code tests
//!
//! The real `tropel` binary must exit NON-ZERO when a threshold fails (CI
//! pipelines depend on the exit code to gate deploys) and ZERO when all
//! thresholds pass. Backlog §6 P1: nothing verified this — `cli.rs`'s tests
//! are config-overlay merges and `main.rs` has zero tests.
//!
//! These tests spawn the ACTUAL built binary (`CARGO_BIN_EXE_tropel`, set by
//! cargo for integration tests in the same package as the binary) against a
//! tiny local HTTP server, covering the whole path: CLI parsing → engine run
//! → threshold evaluation → process exit code.

use std::process::Command;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Minimal HTTP/1.1 server answering `200 {"ok":true}`. Serves keep-alive
/// (loops per connection) so the client's pooled-connection reuse never
/// races a closed socket — the same flake we fixed in the engine tests.
async fn start_echo_server() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    let mut head = Vec::new();
                    loop {
                        let n = match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
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
                    if sock.write_all(resp.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// Write a minimal k6 script to a temp file and return its path. `tag`
/// disambiguates per test — the tests run in parallel.
fn write_k6_script(base: &str, tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir();
    let path = dir.join(format!("tropel-exitcode-{}-{}.js", std::process::id(), tag));
    let script = format!(
        r#"import http from 'k6/http';
export default function () {{
  http.get('{base}/');
}}
"#
    );
    std::fs::write(&path, script).unwrap();
    path
}

/// Run the real binary: `tropel run <script> -u 2 -d 1s -t <threshold>`.
fn run_tropel(script: &std::path::Path, threshold: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_tropel"))
        .arg("run")
        .arg(script)
        .arg("--format")
        .arg("k6")
        .arg("-u")
        .arg("2")
        .arg("-d")
        .arg("1s")
        .arg("-t")
        .arg(threshold)
        .output()
        .expect("failed to spawn the tropel binary")
}

/// A failed threshold must yield a non-zero process exit code — CI gates on
/// it. `http_reqs > 1000000` is impossible for a 1s/2-VU run, so the
/// threshold is guaranteed to fail if requests actually fired.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_threshold_exits_nonzero() {
    let srv = start_echo_server().await;
    let script = write_k6_script(&format!("http://{srv}"), "fail");
    let out = run_tropel(&script, "http_reqs > 1000000");
    let stderr = String::from_utf8_lossy(&out.stderr);
    // Pin the FAILURE to the threshold path: a bare `!success` would pass
    // for a k6 parse error, a VU-init failure, or a panic — all exit
    // non-zero too. Require exit code 1 AND the threshold-failure marker so
    // the test can't silently stop testing thresholds.
    assert_eq!(
        out.status.code(),
        Some(1),
        "failed threshold must exit with code 1, exited {:?}\nstderr: {}",
        out.status.code(),
        stderr
    );
    assert!(
        stderr.contains("One or more thresholds failed"),
        "stderr must name the threshold failure, got:\n{}",
        stderr
    );
    let _ = std::fs::remove_file(&script);
}

/// A passing threshold must yield a zero exit code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn passing_threshold_exits_zero() {
    let srv = start_echo_server().await;
    let script = write_k6_script(&format!("http://{srv}"), "pass");
    let out = run_tropel(&script, "http_reqs > 0");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "passing threshold must exit zero, exited {:?}\nstderr: {}",
        out.status.code(),
        stderr
    );
    let _ = std::fs::remove_file(&script);
}
