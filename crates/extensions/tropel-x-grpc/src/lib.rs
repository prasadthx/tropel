//! # tropel-x-grpc
//!
//! Real gRPC protocol extension for Tropel: loads `.proto` files at runtime
//! (protox + prost-reflect), executes unary, server-streaming,
//! client-streaming, and bidi-streaming calls via tonic with a fully dynamic
//! codec (no codegen), and emits the k6-compatible `grpc_reqs` and
//! `grpc_req_duration` metrics.
//!
//! ## Request contract
//!
//! The URL encodes the gRPC method: `grpc://host:port/package.Service/Method`
//! (or `grpcs://` for TLS). The request body is the input message as JSON.
//!
//! The proto source is resolved from (first match wins):
//! 1. `config["proto"]` — a `.proto` file path or inline proto source text,
//!    with optional `config["proto_dir"]` for imports.
//! 2. The `x-grpc-proto` request header (path or inline source), with
//!    optional `x-grpc-proto-dir`.
//! 3. The `TROPEL_GRPC_PROTO` env var (path), with `TROPEL_GRPC_PROTO_DIR`.
//!
//! The response is stored in `pm.response`: status 200 on OK (or a mapped
//! HTTP status on gRPC error), body = response message(s) as JSON (an array
//! for server-streaming).

use async_trait::async_trait;
use bytes::{Buf, BufMut};
use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use serde::de::DeserializeSeed;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code as GrpcCode, Request as TonicRequest, Status};
use tropel_sdk::{
    Body, Protocol, ProtocolOutcome, ProtocolRegistration, Request, Response, Result, Sample,
    SampleType, TagMap, TropelError,
};

/// Default gRPC port when the URL omits it.
const DEFAULT_PORT: u16 = 50051;
/// Max proto source size accepted as inline text (1 MiB).
const MAX_INLINE_PROTO: usize = 1024 * 1024;

/// gRPC protocol executor with per-scenario caches.
///
/// Compiling the `.proto` (protox) and establishing a tonic channel are the
/// two dominant per-request costs — both are re-done on EVERY request in the
/// naive implementation, so `grpc_req_duration` included compile+connect and
/// high-rate tests spent tens of ms per call before the actual RPC. These
/// caches make both one-time per (proto, authority) pair, shared across all
/// VUs of a scenario (the engine resolves the protocol once and shares the
/// Compiled descriptor pools, keyed by `(proto source, include dir)`.
type PoolKey = (String, Option<String>);

/// Max compiled proto pools and tonic channels kept per scenario protocol.
///
/// Both caches are keyed by inputs a caller controls (`(proto, dir)` and
/// authority). Without a cap, a hostile/adversarial scenario that varies the
/// inline proto text or authority per request would grow them without bound
/// (each unique inline proto = a full protox compile retained forever). The
/// hot path never has more than a handful of entries (one proto, one or few
/// hosts), so the caps are far above real use — they exist purely as a memory
/// safety valve, evicting the oldest entry FIFO when full.
const MAX_CACHED_POOLS: usize = 64;
const MAX_CACHED_CHANNELS: usize = 128;

/// `Arc<dyn Protocol>`).
#[derive(Default)]
pub struct GrpcProtocol {
    /// Compiled descriptor pools, keyed by `(proto source, include dir)`.
    pools: Mutex<HashMap<PoolKey, Arc<DescriptorPool>>>,
    /// Insertion order for the pool cache (FIFO eviction key).
    pool_order: Mutex<VecDeque<PoolKey>>,
    /// Tonic channels pooled by authority (`scheme://host:port`).
    channels: Mutex<HashMap<String, Channel>>,
    /// Insertion order for the channel cache (FIFO eviction key).
    channel_order: Mutex<VecDeque<String>>,
}

/// Insert into a bounded cache: when the map is at capacity, evict the oldest
/// entry (FIFO, tracked by `order`) before inserting the new one. The cache
/// is keyed by caller-controlled inputs, so this is the memory-safety valve
/// against unbounded growth — a scenario that varies the key per request can
/// never retain more than `max` entries.
fn cache_insert_bounded<K, V>(
    map: &mut HashMap<K, V>,
    order: &mut VecDeque<K>,
    key: K,
    value: V,
    max: usize,
) where
    K: Eq + std::hash::Hash + Clone,
{
    // A zero-capacity cache retains nothing (the eviction path below needs an
    // entry in `order` to pop; with max == 0 there is never room).
    if max == 0 {
        return;
    }
    if !map.contains_key(&key) && map.len() >= max {
        if let Some(oldest) = order.pop_front() {
            map.remove(&oldest);
        }
    }
    if map.insert(key.clone(), value).is_none() {
        order.push_back(key);
    }
}

/// A tonic `Codec` that encodes/decodes prost-reflect `DynamicMessage`s.
///
/// tonic's `Codec` trait is unsealed, so a fully dynamic codec (no generated
/// code) is possible: the input/output `MessageDescriptor`s come from a
/// runtime-compiled `DescriptorPool`.
///
/// `input` is the message type the **encoder** produces (on a client: the
/// request message; on a server: the response message). `output` is the
/// message type the **decoder** yields (on a client: the response message;
/// on a server: the request message). For a server-side codec pass
/// `(method.output(), method.input())`.
pub struct DynamicCodec {
    /// Descriptor for the message type the **decoder** yields (on a client:
    /// the response message; on a server: the request message).
    output: MessageDescriptor,
}

impl DynamicCodec {
    /// Create a codec from an encoder message descriptor and a decoder
    /// message descriptor. See the struct docs for the client/server swap.
    ///
    /// Only `output` is stored: `DynamicEncoder` is a unit struct because a
    /// [`DynamicMessage`] self-describes and needs no external descriptor to
    /// encode. The `_input` parameter is kept so call sites can express the
    /// orientation (`(method.input(), method.output())` client-side,
    /// swapped server-side).
    pub fn new(_input: MessageDescriptor, output: MessageDescriptor) -> Self {
        Self { output }
    }
}

impl Codec for DynamicCodec {
    type Encode = DynamicMessage;
    type Decode = DynamicMessage;
    type Encoder = DynamicEncoder;
    type Decoder = DynamicDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        DynamicEncoder
    }

    fn decoder(&mut self) -> Self::Decoder {
        DynamicDecoder {
            desc: self.output.clone(),
        }
    }
}

/// Encoder half of [`DynamicCodec`] — delegates to prost's binary encoding.
pub struct DynamicEncoder;

impl Encoder for DynamicEncoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn encode(
        &mut self,
        item: Self::Item,
        dst: &mut EncodeBuf<'_>,
    ) -> std::result::Result<(), Self::Error> {
        let bytes = item.encode_to_vec();
        dst.put_slice(&bytes);
        Ok(())
    }
}

/// Decoder half of [`DynamicCodec`] — delegates to prost-reflect's binary
/// decoding against the message descriptor.
pub struct DynamicDecoder {
    desc: MessageDescriptor,
}

impl Decoder for DynamicDecoder {
    type Item = DynamicMessage;
    type Error = Status;

    fn decode(
        &mut self,
        src: &mut DecodeBuf<'_>,
    ) -> std::result::Result<Option<Self::Item>, Self::Error> {
        if src.remaining() == 0 {
            return Ok(None);
        }
        let bytes = src.copy_to_bytes(src.remaining());
        let msg = DynamicMessage::decode(self.desc.clone(), bytes)
            .map_err(|e| Status::internal(format!("decode {}: {e}", self.desc.full_name())))?;
        Ok(Some(msg))
    }
}

#[async_trait]
impl Protocol for GrpcProtocol {
    fn scheme(&self) -> &str {
        "grpc"
    }

    async fn execute(&self, req: &Request, config: Option<&serde_json::Value>) -> Result<ProtocolOutcome> {
        // ── Parse the URL: grpc://host:port/package.Service/Method ──
        let url = url::Url::parse(&req.url)
            .map_err(|e| TropelError::Config(format!("invalid gRPC URL '{}': {}", req.url, e)))?;
        let is_tls = url.scheme() == "grpcs";
        if !matches!(url.scheme(), "grpc" | "grpcs") {
            return Err(TropelError::Config(format!(
                "not a gRPC URL: '{}'",
                req.url
            )));
        }
        let host = url
            .host_str()
            .ok_or_else(|| TropelError::Config(format!("gRPC URL has no host: '{}'", req.url)))?
            .to_string();
        let port = url.port().unwrap_or(DEFAULT_PORT);
        let path = url.path().trim_start_matches('/');
        let mut segments = path.split('/');
        let service_full = segments.next().unwrap_or("").trim();
        let method_name = segments.next().unwrap_or("").trim();
        if service_full.is_empty() || method_name.is_empty() {
            return Err(TropelError::Config(format!(
                "gRPC URL must be grpc(s)://host:port/package.Service/Method, got '{}'",
                req.url
            )));
        }

        // ── Resolve the proto source (config → header → env) ──
        let (proto_src, proto_dir) = resolve_proto(req, config)?;

        // ── Compile the proto ONCE per (source, dir) and cache the pool ──
        // Compilation (protox) is tens of ms — the dominant per-request cost
        // before the actual RPC. The double-checked cache shares one compiled
        // pool across every VU of the scenario (the engine hands the same
        // `Arc<dyn Protocol>` to all VUs).
        let pool = {
            let key = (proto_src.clone(), proto_dir.clone());
            let cached = self.pools.lock().unwrap().get(&key).cloned();
            match cached {
                Some(p) => p,
                None => {
                    let compiled = Arc::new(compile_proto(&proto_src, proto_dir.as_deref())?);
                    let mut pools = self.pools.lock().unwrap();
                    let mut order = self.pool_order.lock().unwrap();
                    // Bounded: a caller that varies the inline proto per
                    // request can never retain more than MAX_CACHED_POOLS
                    // compiled pools (FIFO eviction of the oldest).
                    cache_insert_bounded(
                        &mut pools,
                        &mut order,
                        key,
                        compiled.clone(),
                        MAX_CACHED_POOLS,
                    );
                    compiled
                }
            }
        };
        let service = pool
            .get_service_by_name(service_full)
            .ok_or_else(|| {
                TropelError::Config(format!(
                    "service '{}' not found in proto (have: {})",
                    service_full,
                    list_services(&pool)
                ))
            })?;
        let method = service
            .methods()
            .find(|m| m.name() == method_name)
            .ok_or_else(|| {
                TropelError::Config(format!(
                    "method '{}' not found on service '{}'",
                    method_name, service_full
                ))
            })?;

        // ── Build the input message(s) from the JSON body ──
        // prost-reflect's canonical JSON support is the serde path:
        // `MessageDescriptor: DeserializeSeed` (JSON → DynamicMessage) and
        // `DynamicMessage: Serialize` (DynamicMessage → JSON). The
        // `transcode_from`/`transcode_to` helpers take concrete `prost::Message`
        // impls, which a dynamic protocol does not have.
        //
        // Client-streaming / bidi methods take a JSON **array** of messages
        // (each element deserialized against the input descriptor); unary and
        // server-streaming take a single JSON object.
        let input_desc = method.input();
        let stream_in = method.is_client_streaming();
        let mut input_messages: Vec<DynamicMessage> = Vec::new();
        if let Some(json) = body_to_json(req) {
            let parse_json = |v: &serde_json::Value| -> Result<DynamicMessage> {
                let json_str = serde_json::to_string(v)
                    .map_err(|e| TropelError::Parse(format!("request body JSON: {e}")))?;
                let mut de = serde_json::Deserializer::from_str(&json_str);
                input_desc.clone().deserialize(&mut de).map_err(|e| {
                    TropelError::Parse(format!(
                        "request body is not a valid {}: {}",
                        input_desc.full_name(),
                        e
                    ))
                })
            };
            if stream_in {
                match json {
                    serde_json::Value::Array(items) => {
                        for item in items {
                            input_messages.push(parse_json(&item)?);
                        }
                    }
                    // Explicit `null` behaves like an absent body: empty stream
                    // (immediate half-close), consistent with the unary case.
                    serde_json::Value::Null => {}
                    other => input_messages.push(parse_json(&other)?),
                }
            } else {
                input_messages.push(parse_json(&json)?);
            }
        }
        if input_messages.is_empty() && !stream_in {
            // Empty body → all-default message (matches the pre-streaming
            // behaviour of sending a single empty DynamicMessage).
            input_messages.push(DynamicMessage::new(input_desc.clone()));
        }

        // ── Pool the channel by authority — connect ONCE per endpoint ──
        // Tonic's `Channel` is cheaply cloneable (an Arc-backed handle), so
        // pooling by `scheme://host:port` reuses the established HTTP/2
        // connection instead of reconnecting per request. The endpoint is
        // rebuilt (with TLS config) only on the first request for an
        // authority.
        let scheme = if is_tls { "https" } else { "http" };
        let authority = format!("{scheme}://{host}:{port}");
        let channel = {
            let cached = self.channels.lock().unwrap().get(&authority).cloned();
            match cached {
                Some(c) => c,
                None => {
                    let mut endpoint = Endpoint::from_shared(authority.clone())
                        .map_err(|e| TropelError::Extension(format!("bad gRPC endpoint: {e}")))?;
                    if is_tls {
                        endpoint = endpoint
                            .tls_config(ClientTlsConfig::new().domain_name(&host))
                            .map_err(|e| {
                                TropelError::Extension(format!("gRPC TLS config: {e}"))
                            })?;
                    }
                    let connected = endpoint
                        .connect()
                        .await
                        .map_err(|e| {
                            TropelError::Extension(format!(
                                "gRPC connect to {host}:{port}: {e}"
                            ))
                        })?;
                    let mut channels = self.channels.lock().unwrap();
                    let mut order = self.channel_order.lock().unwrap();
                    // Bounded: a caller that varies the authority per request
                    // can never retain more than MAX_CACHED_CHANNELS live
                    // connections (FIFO eviction of the oldest).
                    cache_insert_bounded(
                        &mut channels,
                        &mut order,
                        authority,
                        connected.clone(),
                        MAX_CACHED_CHANNELS,
                    );
                    connected
                }
            }
        };

        // ── Start the RPC timer AFTER connect ──
        // `grpc_req_duration` must measure the RPC itself, not the one-time
        // proto compile + channel connect (which the caches above amortize to
        // zero on the hot path).
        let start = Instant::now();

        // ── Build the request metadata from request headers ──
        // Pseudo-headers (`:...`) and the internal `x-grpc-proto*` headers
        // (used for proto resolution) are never forwarded as gRPC metadata.
        let mut metadata = tonic::metadata::MetadataMap::new();
        for (k, v) in &req.headers {
            let lower = k.to_ascii_lowercase();
            if lower.starts_with(':') || lower.starts_with("x-grpc-proto") {
                continue;
            }
            if let (Ok(mv), Ok(key)) = (
                v.parse::<AsciiMetadataValue>(),
                AsciiMetadataKey::from_bytes(lower.as_bytes()),
            ) {
                metadata.insert(key, mv);
            }
        }

        let path_str = format!("/{service_full}/{method_name}");
        let path = parse_path(&path_str)?;
        let codec = DynamicCodec::new(method.input(), method.output());
        let deadline = req.timeout;
        let mut client = tonic::client::Grpc::new(channel);
        // Reserve the channel before the call: the transport `Channel` is
        // backed by a tower `Buffer`, whose `call` panics if `poll_ready`
        // was never polled (`send_item called without first calling
        // poll_reserve`). Generated tonic clients do this too.
        client
            .ready()
            .await
            .map_err(|e| TropelError::Extension(format!("gRPC channel not ready: {e}")))?;

        // ── Execute: unary / server-streaming / client-streaming / bidi ──
        let mut status_override: Option<GrpcCode> = None;
        let is_server_streaming = method.is_server_streaming();
        // prost-reflect has no `is_bidi_streaming()`; bidi means both directions
        // stream. The client-streaming branch below is therefore only reached
        // for client-streaming-only methods (is_bidi is checked first).
        let is_bidi = method.is_client_streaming() && is_server_streaming;
        let is_client_streaming = method.is_client_streaming() && !is_server_streaming;

        // The response HEADER is bounded by `deadline` via with_timeout, but the
        // DRAIN loop of a streaming method is NOT — a server that trickles or
        // never closes would hold the VU forever. drain_bounded caps the whole
        // drain with the same deadline so `req.timeout` bounds the full stream.
        let response_value: serde_json::Value = if is_bidi {
            let mut tonic_req = TonicRequest::new(tokio_stream::iter(input_messages));
            *tonic_req.metadata_mut() = metadata.clone();
            let fut = client.streaming(tonic_req, path, codec);
            let result = with_timeout(deadline, fut).await;
            match result {
                Ok(stream) => drain_bounded(deadline, stream.into_inner(), &mut status_override).await,
                Err(e) => {
                    status_override = Some(e.code());
                    serde_json::Value::Null
                }
            }
        } else if is_client_streaming {
            let mut tonic_req = TonicRequest::new(tokio_stream::iter(input_messages));
            *tonic_req.metadata_mut() = metadata.clone();
            let fut = client.client_streaming(tonic_req, path, codec);
            let result = with_timeout(deadline, fut).await;
            match result {
                Ok(resp) => serde_json::to_value(resp.into_inner())
                    .unwrap_or(serde_json::Value::Null),
                Err(e) => {
                    status_override = Some(e.code());
                    serde_json::Value::Null
                }
            }
        } else if is_server_streaming {
            let msg = input_messages
                .into_iter()
                .next()
                .unwrap_or_else(|| DynamicMessage::new(input_desc.clone()));
            let mut tonic_req = TonicRequest::new(msg);
            *tonic_req.metadata_mut() = metadata.clone();
            let fut = client.server_streaming(tonic_req, path, codec);
            let result = with_timeout(deadline, fut).await;
            match result {
                Ok(stream) => drain_bounded(deadline, stream.into_inner(), &mut status_override).await,
                Err(e) => {
                    status_override = Some(e.code());
                    serde_json::Value::Null
                }
            }
        } else {
            let msg = input_messages
                .into_iter()
                .next()
                .unwrap_or_else(|| DynamicMessage::new(input_desc.clone()));
            let mut tonic_req = TonicRequest::new(msg);
            *tonic_req.metadata_mut() = metadata.clone();
            let fut = client.unary(tonic_req, path, codec);
            let result = with_timeout(deadline, fut).await;
            match result {
                Ok(resp) => serde_json::to_value(resp.into_inner())
                    .unwrap_or(serde_json::Value::Null),
                Err(e) => {
                    status_override = Some(e.code());
                    serde_json::Value::Null
                }
            }
        };

        let duration = start.elapsed();
        // k6 tags gRPC samples with the numeric gRPC code (0 = OK), not an
        // HTTP status; keep the HTTP mapping only on the response object.
        let grpc_status = status_override.unwrap_or(GrpcCode::Ok) as i32;
        let (http_status, ok) = match status_override {
            None => (200u16, true),
            Some(code) => (grpc_code_to_http(code), false),
        };

        // ── Build the outcome: response + samples ──
        let body_bytes = serde_json::to_vec(&response_value).unwrap_or_default();
        let response = Response {
            url: req.url.clone(),
            status_code: http_status,
            status_text: if ok { "OK".into() } else { "ERROR".into() },
            headers: HashMap::new(),
            body: body_bytes.clone(),
            text_cache: std::cell::OnceCell::new(),
            json_cache: std::cell::OnceCell::new(),
            response_time: duration,
            timings: None,
            cookies: vec![],
            size: body_bytes.len() as u64,
            redirects: vec![],
        };

        let now = std::time::SystemTime::now();
        let mut tags = TagMap::with_capacity(5);
        tags.insert("url", req.url.clone());
        tags.insert("method", format!("{service_full}/{method_name}"));
        tags.insert("status", grpc_status.to_string());
        tags.insert("name", format!("{service_full}/{method_name}"));
        tags.insert("group", "grpc");
        let tags = std::sync::Arc::new(tags);

        let sent = body_to_json(req)
            .map(|v| serde_json::to_vec(&v).map(|b| b.len() as f64).unwrap_or(0.0))
            .unwrap_or(0.0);
        let samples = vec![
            Sample {
                metric: "grpc_req_duration".into(),
                value: duration.as_micros() as f64,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Trend,
            },
            Sample {
                metric: "grpc_reqs".into(),
                value: 1.0,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "data_sent".into(),
                value: sent,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Counter,
            },
            Sample {
                metric: "data_received".into(),
                value: body_bytes.len() as f64,
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

/// Await a gRPC call with an optional deadline, mapping timeout to
/// `DEADLINE_EXCEEDED` (k6's gRPC module times out with that code).
async fn with_timeout<T>(
    deadline: Option<std::time::Duration>,
    fut: impl std::future::Future<Output = std::result::Result<T, Status>>,
) -> std::result::Result<T, Status> {
    match deadline {
        Some(t) => match tokio::time::timeout(t, fut).await {
            Ok(r) => r,
            Err(_) => Err(Status::deadline_exceeded("gRPC call timed out")),
        },
        None => fut.await,
    }
}

/// Drain a tonic response stream into a JSON array, surfacing any terminal
/// status via `status_override`. The caller bounds the whole drain with the
/// request deadline (see `drain_bounded`), so a never-closing server can't
/// hold the VU.
async fn collect_messages(
    stream: &mut tonic::Streaming<DynamicMessage>,
    status_override: &mut Option<GrpcCode>,
) -> std::result::Result<serde_json::Value, Status> {
    let mut msgs = Vec::new();
    loop {
        match stream.message().await {
            Ok(Some(msg)) => {
                if let Ok(v) = serde_json::to_value(&msg) {
                    msgs.push(v);
                }
            }
            Ok(None) => break,
            Err(e) => {
                *status_override = Some(e.code());
                break;
            }
        }
    }
    Ok(serde_json::Value::Array(msgs))
}

/// Drain a streaming response bounded by the request deadline. On timeout the
/// stream is abandoned (dropped, half-closing it) and the status is set to
/// `DEADLINE_EXCEEDED` — consistent with the header-phase timeout.
async fn drain_bounded(
    deadline: Option<std::time::Duration>,
    mut stream: tonic::Streaming<DynamicMessage>,
    status_override: &mut Option<GrpcCode>,
) -> serde_json::Value {
    match with_timeout(deadline, collect_messages(&mut stream, status_override)).await {
        Ok(v) => v,
        Err(e) => {
            *status_override = Some(e.code());
            serde_json::Value::Null
        }
    }
}

/// Parse a `/package.Service/Method` path into a `PathAndQuery`.
fn parse_path(path: &str) -> Result<http::uri::PathAndQuery> {
    http::uri::PathAndQuery::try_from(path)
        .map_err(|e| TropelError::Config(format!("invalid gRPC path '{path}': {e}")))
}

/// Convert a gRPC status code to an approximate HTTP status.
fn grpc_code_to_http(code: GrpcCode) -> u16 {
    match code {
        GrpcCode::Ok => 200,
        GrpcCode::Cancelled => 499,
        GrpcCode::Unknown => 500,
        GrpcCode::InvalidArgument => 400,
        GrpcCode::DeadlineExceeded => 504,
        GrpcCode::NotFound => 404,
        GrpcCode::AlreadyExists => 409,
        GrpcCode::PermissionDenied => 403,
        GrpcCode::ResourceExhausted => 429,
        GrpcCode::FailedPrecondition => 400,
        GrpcCode::Aborted => 409,
        GrpcCode::OutOfRange => 400,
        GrpcCode::Unimplemented => 501,
        GrpcCode::Internal => 500,
        GrpcCode::Unavailable => 503,
        GrpcCode::DataLoss => 500,
        GrpcCode::Unauthenticated => 401,
    }
}

/// Extract a JSON value from the request body, if any.
fn body_to_json(req: &Request) -> Option<serde_json::Value> {
    match req.body.as_ref()? {
        Body::Raw(s) => serde_json::from_str(s).ok(),
        Body::Json(v) => Some(v.clone()),
        _ => None,
    }
}

/// Resolve the proto source from config → headers → env.
/// Returns `(source_or_path, include_dir)`.
fn resolve_proto(
    req: &Request,
    config: Option<&serde_json::Value>,
) -> Result<(String, Option<String>)> {
    // 1. config
    if let Some(cfg) = config {
        if let Some(p) = cfg.get("proto").and_then(|v| v.as_str()) {
            let dir = cfg
                .get("proto_dir")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            return Ok((p.to_string(), dir));
        }
    }
    // 2. request headers
    if let Some(p) = req.headers.get("x-grpc-proto") {
        let dir = req.headers.get("x-grpc-proto-dir").cloned();
        return Ok((p.clone(), dir));
    }
    // 3. env
    if let Ok(p) = std::env::var("TROPEL_GRPC_PROTO") {
        let dir = std::env::var("TROPEL_GRPC_PROTO_DIR").ok();
        return Ok((p, dir));
    }
    Err(TropelError::Config(
        "no proto source: pass config {\"proto\": ...} / {\"proto_dir\": ...}, \
         the x-grpc-proto request header, or set TROPEL_GRPC_PROTO"
            .into(),
    ))
}

/// Compile proto source (path or inline text) into a `DescriptorPool`.
///
/// Public so extensions/tests can compile the same pool used at runtime.
/// Inline source is detected by the presence of a `syntax` directive or a
/// newline; file paths pass through unchanged.
pub fn compile_proto(source: &str, include_dir: Option<&str>) -> Result<DescriptorPool> {
    let tmp;
    let proto_path = if source.contains('\n') || source.contains("syntax") || source.trim().is_empty()
    {
        // Inline source → write to a temp file.
        if source.len() > MAX_INLINE_PROTO {
            return Err(TropelError::Config(format!(
                "inline proto source exceeds {} bytes",
                MAX_INLINE_PROTO
            )));
        }
        tmp = tempfile::Builder::new()
            .prefix("tropel-grpc-")
            .suffix(".proto")
            .tempfile()
            .map_err(|e| TropelError::Extension(format!("temp proto file: {e}")))?;
        std::fs::write(tmp.path(), source)
            .map_err(|e| TropelError::Extension(format!("write temp proto: {e}")))?;
        tmp.path().to_path_buf()
    } else {
        std::path::PathBuf::from(source)
    };

    let mut includes: Vec<std::path::PathBuf> = Vec::new();
    if let Some(dir) = include_dir {
        includes.push(std::path::PathBuf::from(dir));
    }
    if let Some(parent) = proto_path.parent() {
        if !includes.iter().any(|i| i == parent) {
            includes.push(parent.to_path_buf());
        }
    }

    let fds = protox::compile([proto_path.as_path()], includes)
        .map_err(|e| TropelError::Extension(format!("proto compile: {e}")))?;
    // Decode from bytes rather than passing the FileDescriptorSet by value —
    // sidesteps prost-types version unification between protox and
    // prost-reflect (both use prost-types 0.14, but the bytes round-trip is
    // immune to any future drift). `Vec<u8>` does not impl `bytes::Buf`, so
    // hand out a slice.
    let bytes = prost::Message::encode_to_vec(&fds);
    let pool = DescriptorPool::decode(bytes.as_slice())
        .map_err(|e| TropelError::Extension(format!("proto pool: {e}")))?;
    Ok(pool)
}

fn list_services(pool: &DescriptorPool) -> String {
    pool.services()
        .map(|s| s.name().to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Inventory factory — must be a `fn` pointer for `inventory::submit!`.
fn grpc_factory() -> Box<dyn Protocol> {
    Box::new(GrpcProtocol::default())
}

inventory::submit!(ProtocolRegistration::new("grpc", grpc_factory));

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_cache_evicts_oldest_fifo() {
        let mut map: HashMap<i32, &str> = HashMap::new();
        let mut order: VecDeque<i32> = VecDeque::new();

        // Fill to capacity.
        cache_insert_bounded(&mut map, &mut order, 1, "a", 3);
        cache_insert_bounded(&mut map, &mut order, 2, "b", 3);
        cache_insert_bounded(&mut map, &mut order, 3, "c", 3);
        assert_eq!(map.len(), 3);
        assert_eq!(order.len(), 3);

        // Over capacity: the OLDEST (1) is evicted, the new key is kept.
        cache_insert_bounded(&mut map, &mut order, 4, "d", 3);
        assert_eq!(map.len(), 3);
        assert!(!map.contains_key(&1), "oldest entry must be evicted");
        assert_eq!(map.get(&4), Some(&"d"));
        assert_eq!(order.front(), Some(&2), "eviction order must advance");

        // Re-inserting an EXISTING key updates the value but not the order
        // (no duplicate in the FIFO, no spurious eviction).
        cache_insert_bounded(&mut map, &mut order, 2, "b2", 3);
        assert_eq!(map.len(), 3);
        assert_eq!(order.len(), 3);
        assert_eq!(map.get(&2), Some(&"b2"));
        assert_eq!(order.front(), Some(&2));

        // Steady-state: after two more inserts, the oldest entries are gone.
        // Order before this point is [2, 3, 4] (key 2 was re-inserted but its
        // FIFO position did NOT refresh). Insert 5 evicts 2, insert 6 evicts
        // 3 — survivors are {4, 5, 6}.
        cache_insert_bounded(&mut map, &mut order, 5, "e", 3);
        cache_insert_bounded(&mut map, &mut order, 6, "f", 3);
        assert_eq!(map.len(), 3);
        assert!(!map.contains_key(&2), "re-inserted key 2 is still the FIFO front and must be evicted first");
        assert!(!map.contains_key(&3));
        assert_eq!(map.get(&4), Some(&"d"));
        assert_eq!(map.get(&5), Some(&"e"));
        assert_eq!(map.get(&6), Some(&"f"));
    }

    #[test]
    fn bounded_cache_zero_capacity_evicts_on_every_insert() {
        let mut map: HashMap<i32, i32> = HashMap::new();
        let mut order: VecDeque<i32> = VecDeque::new();
        cache_insert_bounded(&mut map, &mut order, 1, 10, 0);
        assert!(map.is_empty(), "zero capacity must never retain entries");
        assert!(order.is_empty());
    }
}
