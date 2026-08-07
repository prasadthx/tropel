//! Integration tests for the WebSocket protocol extension.
//!
//! Spins up a real tokio-tungstenite echo server (codegen-free — just
//! `accept_async` + echo loop) and drives `WebSocketProtocol` against it:
//! text echo roundtrip, binary messages, multiple messages via config,
//! and a connection-refused error path. Metrics (`ws_*`) are asserted on
//! the returned samples.

use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tropel_sdk::{Body, Method, Protocol, Request, ResponseType};
use tropel_x_websocket::WebSocketProtocol;

fn make_request(url: &str, body: Option<Body>) -> Request {
    Request {
        url: url.to_string(),
        method: Method::GET,
        headers: HashMap::new(),
        query_params: HashMap::new(),
        body,
        auth: None,
        certificate: None,
        follow_redirects: true,
        timeout: Some(Duration::from_secs(5)),
        response_type: ResponseType::Text,
    }
}

/// Echo server: echoes every message (text or binary) back verbatim until a
/// close frame arrives.
async fn spawn_echo_server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                while let Some(Ok(msg)) = ws.next().await {
                    if matches!(msg, Message::Close(_)) {
                        let _ = ws.send(Message::Close(None)).await;
                        break;
                    }
                    if ws.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn text_echo_roundtrip() {
    let addr = spawn_echo_server().await;
    let req = make_request(
        &format!("ws://{addr}/echo"),
        Some(Body::Raw("hello".into())),
    );
    let outcome = WebSocketProtocol.execute(&req, None).await.unwrap();
    let resp = outcome.response.expect("response");
    assert_eq!(resp.status_code, 101, "handshake must switch protocols");
    let received: Vec<String> = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(received, vec!["hello"]);

    // Metrics: one session, one message sent, one received, failed=0.
    let sample = |name: &str| -> f64 {
        outcome
            .samples
            .iter()
            .find(|s| s.metric == name)
            .unwrap_or_else(|| panic!("missing metric {name}"))
            .value
    };
    assert_eq!(sample("ws_sessions"), 1.0);
    assert_eq!(sample("ws_msgs_sent"), 1.0);
    assert_eq!(sample("ws_msgs_received"), 1.0);
    assert!(sample("ws_bytes_received") >= 5.0);
    assert_eq!(sample("ws_req_failed"), 0.0);
    assert!(sample("ws_req_duration") > 0.0);
    assert!(sample("ws_connecting") > 0.0);
}

#[tokio::test]
async fn binary_messages() {
    let addr = spawn_echo_server().await;
    let req = make_request(&format!("ws://{addr}/bin"), None);
    let config = serde_json::json!({
        "messages": ["ping"],
        "binary": true,
        "wait": "500ms",
    });
    let outcome = WebSocketProtocol
        .execute(&req, Some(&config))
        .await
        .unwrap();
    let resp = outcome.response.expect("response");
    assert_eq!(resp.status_code, 101);
    // Binary payloads are summarized as `<binary N bytes>`.
    let received: Vec<String> = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(received, vec!["<binary 4 bytes>"]);
}

#[tokio::test]
async fn multiple_messages_via_config() {
    let addr = spawn_echo_server().await;
    let req = make_request(&format!("ws://{addr}/multi"), None);
    let config = serde_json::json!({
        "messages": ["a", "b", "c"],
        "wait": "500ms",
    });
    let outcome = WebSocketProtocol
        .execute(&req, Some(&config))
        .await
        .unwrap();
    let received: Vec<String> = serde_json::from_slice(&outcome.response.unwrap().body).unwrap();
    assert_eq!(received, vec!["a", "b", "c"]);
    let msgs_sent = outcome
        .samples
        .iter()
        .find(|s| s.metric == "ws_msgs_sent")
        .unwrap()
        .value;
    assert_eq!(msgs_sent, 3.0);
}

/// Server that echoes one message then sends a close frame — used to prove
/// event-driven (until-close) termination without a fixed-wait drain.
async fn spawn_echo_then_close_server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else {
                    return;
                };
                if let Some(Ok(msg)) = ws.next().await {
                    if !matches!(msg, Message::Close(_)) {
                        let _ = ws.send(msg).await; // echo it back
                    }
                    // Then close the connection so the client session ends
                    // promptly (event-driven) instead of waiting out a cap.
                    let _ = ws.send(Message::Close(None)).await;
                }
            });
        }
    });
    addr
}

#[tokio::test]
async fn until_close_ends_session_when_server_closes() {
    // Event-driven termination: with `wait: "until-close"` the session must
    // end the moment the server closes the connection — no fixed-wait drain.
    let addr = spawn_echo_then_close_server().await;
    // The request body is the message the ext sends; the server echoes it
    // then closes, which must end the session promptly (event-driven).
    let req = make_request(
        &format!("ws://{addr}/until-close"),
        Some(Body::Raw("ping".into())),
    );
    let config = serde_json::json!({ "wait": "until-close" });
    let start = std::time::Instant::now();
    let outcome = WebSocketProtocol
        .execute(&req, Some(&config))
        .await
        .unwrap();
    let elapsed = start.elapsed();
    let resp = outcome.response.unwrap();
    assert_eq!(resp.status_code, 101);
    // The server closes right after echoing, so the session ends promptly —
    // far below the 5s request-timeout cap. This proves no fixed-wait drain.
    assert!(
        elapsed < Duration::from_secs(1),
        "until-close took {elapsed:?}"
    );
    let received: Vec<String> = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(received, vec!["ping"]);
}

#[tokio::test]
async fn connection_refused_is_error() {
    // Bind then drop to free the port, guaranteeing ECONNREFUSED.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    let req = make_request(&format!("ws://{addr}/nope"), None);
    let err = WebSocketProtocol
        .execute(&req, None)
        .await
        .err()
        .expect("expected error");
    assert!(
        err.to_string().contains("connect"),
        "expected connect error, got: {err}"
    );
}
