//! # Integration test: real gRPC end-to-end
//!
//! Spins up a **codegen-free** tonic test server (no `tonic-build` generated
//! code): a hand-written `tower::Service` dispatches on the gRPC method path
//! and delegates to `tonic::server::Grpc` with the same `DynamicCodec` the
//! client uses. This proves proto loading, unary calls, server streaming,
//! and the `grpc_req_duration` metric all work against a live wire.

use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};
use std::convert::Infallible;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::transport::server::TcpIncoming;
use tonic::{Request, Response, Status};
use tower::Service;
use tropel_sdk::{Body as ReqBody, Method, Protocol, Request as TpRequest, ResponseType};

const TEST_PROTO: &str = r#"
syntax = "proto3";
package test;

message HelloRequest {
  string name = 1;
}

message HelloReply {
  string message = 1;
}

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc StreamHello(HelloRequest) returns (stream HelloReply);
}
"#;

// ══════════════════════════════════════════════════════════════════════
// Server-side handlers (blanket-implement `UnaryService` / `ServerStreamingService`)
// ══════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct UnaryHandler {
    out_desc: MessageDescriptor,
}

impl Service<Request<DynamicMessage>> for UnaryHandler {
    type Response = Response<DynamicMessage>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<DynamicMessage>) -> Self::Future {
        let out_desc = self.out_desc.clone();
        Box::pin(async move {
            let input = req.into_inner();
            let name = input
                .get_field_by_name("name")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "world".to_string());
            let mut reply = DynamicMessage::new(out_desc);
            reply.set_field_by_name("message", Value::String(format!("Hello, {name}!")));
            Ok(Response::new(reply))
        })
    }
}

#[derive(Clone)]
struct StreamHandler {
    out_desc: MessageDescriptor,
}

impl Service<Request<DynamicMessage>> for StreamHandler {
    type Response =
        Response<tokio_stream::Iter<std::vec::IntoIter<Result<DynamicMessage, Status>>>>;
    type Error = Status;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _req: Request<DynamicMessage>) -> Self::Future {
        let out_desc = self.out_desc.clone();
        Box::pin(async move {
            let mut m1 = DynamicMessage::new(out_desc.clone());
            m1.set_field_by_name("message", Value::String("one".into()));
            let mut m2 = DynamicMessage::new(out_desc);
            m2.set_field_by_name("message", Value::String("two".into()));
            let stream = tokio_stream::iter(vec![Ok(m1), Ok(m2)]);
            Ok(Response::new(stream))
        })
    }
}

// ══════════════════════════════════════════════════════════════════════
// Dispatcher service: routes /test.Greeter/{Method} to the right handler
// ══════════════════════════════════════════════════════════════════════

#[derive(Clone)]
struct GreeterService {
    pool: Arc<DescriptorPool>,
}

impl Service<http::Request<Body>> for GreeterService {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: http::Request<Body>) -> Self::Future {
        let path = req.uri().path().to_string();
        let pool = self.pool.clone();
        Box::pin(async move {
            let service = pool.get_service_by_name("test.Greeter").unwrap();
            let resp = match path.as_str() {
                "/test.Greeter/SayHello" => {
                    let method = service.methods().find(|m| m.name() == "SayHello").unwrap();
                    // Server codec: decoder = request message (method.input),
                    // encoder = response message (method.output).
                    let codec = tropel_x_grpc::DynamicCodec::new(method.output(), method.input());
                    let mut grpc = tonic::server::Grpc::new(codec);
                    grpc.unary(UnaryHandler { out_desc: method.output() }, req).await
                }
                "/test.Greeter/StreamHello" => {
                    let method = service.methods().find(|m| m.name() == "StreamHello").unwrap();
                    let codec = tropel_x_grpc::DynamicCodec::new(method.output(), method.input());
                    let mut grpc = tonic::server::Grpc::new(codec);
                    grpc.server_streaming(StreamHandler { out_desc: method.output() }, req).await
                }
                _ => {
                    let mut resp = http::Response::new(Body::empty());
                    *resp.status_mut() = http::StatusCode::NOT_FOUND;
                    resp
                }
            };
            Ok(resp)
        })
    }
}

/// Bind a listener, spawn the codegen-free server, return the addr.
async fn spawn_server(pool: DescriptorPool) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let incoming = TcpIncoming::from(listener);
    let svc = GreeterService {
        pool: Arc::new(pool),
    };
    // Never signal shutdown — the server lives until the test process exits.
    // (A oneshot sender dropped at the end of this fn would close the channel
    // and fire the signal immediately, killing the server before any call.)
    let shutdown = std::future::pending::<()>();
    let server = tonic::transport::Server::builder().serve_with_incoming_shutdown(svc, incoming, shutdown);
    tokio::spawn(async move {
        match server.await {
            Ok(()) => eprintln!("[tropel-x-grpc test] server exited cleanly (unexpected)"),
            Err(e) => eprintln!("[tropel-x-grpc test] server error: {e}"),
        }
    });
    addr
}

fn make_req(url: String) -> TpRequest {
    TpRequest {
        url,
        method: Method::POST,
        headers: Default::default(),
        query_params: Default::default(),
        body: Some(ReqBody::Json(serde_json::json!({"name": "tropel"}))),
        auth: None,
        certificate: None,
        follow_redirects: true,
        timeout: None,
        response_type: ResponseType::Text,
    }
}

#[tokio::test]
async fn unary_roundtrip() {
    let pool = tropel_x_grpc::compile_proto(TEST_PROTO, None).unwrap();
    let addr = spawn_server(pool).await;

    let proto = tropel_x_grpc::GrpcProtocol;
    let outcome = proto
        .execute(
            &make_req(format!("grpc://{addr}/test.Greeter/SayHello")),
            Some(&serde_json::json!({"proto": TEST_PROTO})),
        )
        .await
        .unwrap();

    let resp = outcome.response.unwrap();
    assert_eq!(resp.status_code, 200, "unary should return HTTP 200");
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    assert_eq!(body["message"], "Hello, tropel!");

    // grpc_req_duration (Trend) + grpc_reqs (Counter) samples must exist.
    assert!(outcome
        .samples
        .iter()
        .any(|s| s.metric == "grpc_req_duration"));
    assert!(outcome.samples.iter().any(|s| s.metric == "grpc_reqs"));
    assert!(outcome
        .samples
        .iter()
        .any(|s| s.metric == "grpc_req_failed" && s.value == 0.0));
}

#[tokio::test]
async fn server_streaming_roundtrip() {
    let pool = tropel_x_grpc::compile_proto(TEST_PROTO, None).unwrap();
    let addr = spawn_server(pool).await;

    let proto = tropel_x_grpc::GrpcProtocol;
    let outcome = proto
        .execute(
            &make_req(format!("grpc://{addr}/test.Greeter/StreamHello")),
            Some(&serde_json::json!({"proto": TEST_PROTO})),
        )
        .await
        .unwrap();

    let resp = outcome.response.unwrap();
    assert_eq!(resp.status_code, 200, "streaming should return HTTP 200");
    let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
    let arr = body.as_array().expect("streaming body is a JSON array");
    assert_eq!(arr.len(), 2, "server should stream exactly 2 messages");
    assert_eq!(arr[0]["message"], "one");
    assert_eq!(arr[1]["message"], "two");

    assert!(outcome
        .samples
        .iter()
        .any(|s| s.metric == "grpc_req_duration"));
}

#[tokio::test]
async fn unknown_method_returns_error_status() {
    let pool = tropel_x_grpc::compile_proto(TEST_PROTO, None).unwrap();
    let addr = spawn_server(pool).await;

    let proto = tropel_x_grpc::GrpcProtocol;
    // A method that does not exist in the proto → config error, no network call.
    let err = proto
        .execute(
            &make_req(format!("grpc://{addr}/test.Greeter/Nope")),
            Some(&serde_json::json!({"proto": TEST_PROTO})),
        )
        .await
        .err()
        .expect("unknown method must be an error");
    assert!(
        err.to_string().contains("not found on service"),
        "expected method-not-found, got: {err}"
    );
}
