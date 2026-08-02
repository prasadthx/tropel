use crate::auth::AuthSigner;
use crate::dns::DnsResolver;
use crate::rps::RpsLimiter;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tropel_core::config::{HttpConfig, TlsConfig};
use tropel_core::types::*;
use tropel_core::Result;
use tropel_core::TropelError;

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const MULTIPART_BOUNDARY: &str = "------------------------tropel-boundary-7a2f24b9";

/// Per-VU HTTP client with auth and response tracking.
#[derive(Clone)]
pub struct HttpClient {
    inner: reqwest::Client,
    /// When true, response bodies are discarded entirely.
    /// The body field will be empty, saving memory and bandwidth.
    discard_bodies: bool,
    /// Optional global RPS limiter (k6 `options.rps`), shared across all
    /// VUs of the run. `None` = unlimited.
    rps: Option<Arc<RpsLimiter>>,
    /// Log every HTTP request/response (method, URL, status, timing) at
    /// debug level — the `--http-debug` flag / `HttpConfig.http_debug`.
    http_debug: bool,
}

impl HttpClient {
    /// Create a new HTTP client from config (default TLS settings).
    pub fn new(config: &HttpConfig) -> Result<Self> {
        Self::with_tls(config, &TlsConfig::default())
    }

    /// Create a client with an optional global RPS limiter (no TLS overrides).
    pub fn new_with_rps(config: &HttpConfig, rps: Option<Arc<RpsLimiter>>) -> Result<Self> {
        Self::with_tls_and_rps(config, &TlsConfig::default(), rps)
    }

    /// Create a new HTTP client from config, applying the TLS settings:
    /// - `insecure_skip_verify`: disable certificate verification
    ///   (`danger_accept_invalid_certs`)
    /// - `min_version` / `max_version`: TLS protocol version bounds
    /// - `client_cert` + `client_key`: mTLS client identity — an unencrypted
    ///   PEM cert + key pair, concatenated into one buffer for
    ///   `Identity::from_pem` (accepts PKCS#8, PKCS#1 and SEC1 keys).
    ///
    /// `client_passphrase` and `allowed_ciphers` are deliberately not applied:
    /// PKCS#12/encrypted-key support requires the native-tls backend, and
    /// per-client cipher selection is not exposed through reqwest's
    /// `ClientBuilder` (custom cipher suites would need
    /// `use_preconfigured_tls(rustls::ClientConfig)`). This build uses
    /// rustls, which negotiates a safe default cipher set; a supplied
    /// passphrase logs a warning (its value is never logged).
    pub fn with_tls(config: &HttpConfig, tls: &TlsConfig) -> Result<Self> {
        Self::with_tls_and_rps(config, tls, None)
    }

    /// Full constructor: TLS overrides plus an optional shared global RPS
    /// limiter (k6 `options.rps`). The limiter is created once per run and
    /// cloned into every per-VU client so the cap is global across VUs.
    pub fn with_tls_and_rps(
        config: &HttpConfig,
        tls: &TlsConfig,
        rps: Option<Arc<RpsLimiter>>,
    ) -> Result<Self> {
        // k6 `noConnectionReuse`: close the connection after every request.
        // reqwest has no direct "reuse off" switch — setting the idle pool to
        // 0 causes every returned connection to be closed instead of pooled,
        // so each request opens a fresh connection.
        let max_idle = if config.no_connection_reuse {
            0
        } else {
            config.max_idle_connections
        };
        // Global request timeout: configurable via `HttpConfig.request_timeout`
        // (k6 `timeout`); falls back to the 10s engine default. A per-request
        // `timeout` (Request.timeout) can still override it shorter.
        let request_timeout = config
            .request_timeout
            .as_deref()
            .and_then(|s| parse_duration(s).ok())
            .unwrap_or(DEFAULT_REQUEST_TIMEOUT);
        let mut builder = reqwest::Client::builder()
            .cookie_store(true)
            .user_agent(&config.user_agent)
            .pool_max_idle_per_host(max_idle)
            .timeout(request_timeout);

        // ── TLS: insecure_skip_verify ──
        if tls.insecure_skip_verify {
            builder = builder.danger_accept_invalid_certs(true);
        }

        // ── TLS: min/max protocol version ──
        if let Some(version) = parse_tls_version(&tls.min_version) {
            builder = builder.min_tls_version(version);
        }
        if let Some(version) = parse_tls_version(&tls.max_version) {
            builder = builder.max_tls_version(version);
        }

        // ── TLS: mTLS client identity ──
        if let Some(cert_path) = &tls.client_cert {
            let cert_bytes = std::fs::read(cert_path).map_err(|e| {
                TropelError::Config(format!("Failed to read client cert '{}': {}", cert_path, e))
            })?;

            // PEM cert + key pair — client_key is REQUIRED here: an identity
            // without key material is meaningless, so fail fast with a clear
            // message instead of a confusing parse error.
            let key_path = tls.client_key.as_deref().ok_or_else(|| {
                TropelError::Config(format!(
                    "TLS client_cert '{}' set but no client_key: a client \
                     identity requires both a certificate and its private key",
                    cert_path
                ))
            })?;
            let key_bytes = std::fs::read(key_path).map_err(|e| {
                TropelError::Config(format!("Failed to read client key '{}': {}", key_path, e))
            })?;

            // PKCS#12 bundles and encrypted PEM keys require the native-tls
            // backend, which this build does not enable (rustls only). Warn
            // when a passphrase was supplied so users aren't surprised that
            // it's ignored; the key must be unencrypted PEM. The passphrase
            // VALUE is never logged (it is a secret).
            if tls.client_passphrase.is_some() {
                tracing::warn!(
                    "client_passphrase is only honored with the native-tls backend; \
                     this rustls build uses unencrypted PEM keys, so the supplied \
                     passphrase will be ignored"
                );
            }

            // Concatenate cert + key into ONE PEM buffer and use
            // `Identity::from_pem` (the only identity constructor available
            // under the rustls feature). It parses mixed PEM sections and
            // accepts PKCS#8 (`BEGIN PRIVATE KEY`), PKCS#1
            // (`BEGIN RSA PRIVATE KEY`) and SEC1 (`BEGIN EC PRIVATE KEY`)
            // keys. `from_pkcs8_pem`/`from_pkcs12_der` exist but are gated
            // behind native-tls.
            let mut combined = cert_bytes;
            combined.extend_from_slice(b"\n");
            combined.extend_from_slice(&key_bytes);
            let identity = reqwest::Identity::from_pem(&combined).map_err(|e| {
                TropelError::Config(format!(
                    "Failed to load PEM client identity (cert '{}', key '{}'): {}",
                    cert_path, key_path, e
                ))
            })?;
            builder = builder.identity(identity);
        } else if tls.client_key.is_some() {
            // Asymmetry guard: a key without a cert is meaningless and would
            // otherwise be silently ignored (the block above is cert-gated).
            tracing::warn!(
                "client_key is set without client_cert — the key will be ignored"
            );
        }

        if !config.decompress {
            builder = builder.no_deflate();
            builder = builder.no_gzip();
            builder = builder.no_brotli();
        }

        if config.max_redirects > 0 {
            builder = builder.redirect(reqwest::redirect::Policy::limited(
                config.max_redirects as usize,
            ));
        } else {
            builder = builder.redirect(reqwest::redirect::Policy::none());
        }

        if let Some(timeout_str) = &config.keep_alive {
            if let Ok(timeout) = parse_duration(timeout_str) {
                builder = builder.pool_idle_timeout(timeout);
            }
        }

        // TCP keep-alive: `idle_connection_timeout` controls the socket-level
        // keep-alive idle period (probes sent after this much idle). This is
        // distinct from the connection-pool idle timeout above.
        if let Some(timeout_str) = &config.idle_connection_timeout {
            if let Ok(timeout) = parse_duration(timeout_str) {
                builder = builder.tcp_keepalive(timeout);
            }
        }

        // HTTP/2 toggle: when disabled, force HTTP/1.1. When enabled (default)
        // reqwest negotiates HTTP/2 over TLS via ALPN (and h2c prior knowledge
        // for plaintext where the server supports it).
        if !config.http2 {
            builder = builder.http1_only();
        }

        // DNS resolver: k6-compatible options (hosts map, blacklist, TTL
        // cache, select/policy) on top of real timed lookups.
        let dns_resolver = DnsResolver::from_config(config);
        // - `connector_layer` times each connection attempt (DNS + TCP + TLS)
        //   via generic tower middleware that never names reqwest's sealed
        //   `Unnameable`/`Conn` types.
        // Results are recorded on the VU thread and consumed by `execute()`.
        builder = builder
            .dns_resolver(dns_resolver)
            .connector_layer(crate::subtimings::TimingConnectorLayer);

        let inner = builder
            .build()
            .map_err(|e| TropelError::Http(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            inner,
            discard_bodies: config.discard_response_bodies,
            rps,
            http_debug: config.http_debug,
        })
    }

    /// Execute an HTTP request with sub-timing instrumentation.
    ///
    /// Measures the full request lifecycle with real phase data captured via
    /// reqwest's `dns_resolver` and `connector_layer` hooks (see
    /// [`crate::subtimings`]):
    /// - **blocked**: request start → connector `call()` begins (pool wait /
    ///   queueing; zero when a pooled keep-alive connection is reused)
    /// - **dns**: real DNS resolution time
    /// - **connecting**: connector call minus DNS (pure TCP for http; for
    ///   https reqwest folds the TLS handshake into the connector call, so it
    ///   is included here)
/// - **waiting** (TTFB): from just before the request is sent to response
///   headers received. For fresh connections this includes the connect phases
///   (blocked + dns + connecting); k6's `http_req_waiting` excludes them
    /// - **receiving**: from response headers to full body bytes received
    /// - **total**: entire `execute()` duration
    ///
    /// `tls_handshaking` and `sending` remain `Duration::ZERO` — reqwest
    /// seals those phases inside the connector / request future. A
    /// hyper-based custom connector would be required to split them out.
    ///
    /// Returns the response along with the number of bytes sent in the request body.
    pub async fn execute(
        &self,
        request: &Request,
        signer: Option<&dyn AuthSigner>,
    ) -> Result<HttpResponse> {
        // Global RPS pacing happens BEFORE the request timer starts, so the
        // wait never inflates http_req_duration / TTFB.
        if let Some(limiter) = &self.rps {
            limiter.acquire().await;
        }
        let total_start = std::time::Instant::now();
        crate::subtimings::begin_request(total_start);

        // Calculate request body size for data_sent tracking
        let request_body_size: u64 = request
            .body
            .as_ref()
            .map(|b| body_size(b) as u64)
            .unwrap_or(0);

        if self.http_debug {
            // info! so the flag is self-sufficient: the default log filter is
            // WARN, and a debug-level line would only appear with RUST_LOG.
            tracing::info!(
                "HTTP >>> {:?} {} (body {} bytes, {} headers)",
                request.method,
                request.url,
                request_body_size,
                request.headers.len()
            );
        }

        // Build the reqwest request
        let multipart_content_type = if matches!(request.body, Some(Body::FormData(_))) {
            Some(format!("multipart/form-data; boundary={}", MULTIPART_BOUNDARY))
        } else {
            None
        };

        let mut req_builder = match request.method {
            Method::GET => self.inner.get(&request.url),
            Method::POST => {
                let rb = self.inner.post(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::PUT => {
                let rb = self.inner.put(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::PATCH => {
                let rb = self.inner.patch(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::DELETE => self.inner.delete(&request.url),
            Method::HEAD => self.inner.head(&request.url),
            Method::OPTIONS => self.inner.request(reqwest::Method::OPTIONS, &request.url),
            Method::TRACE => self.inner.request(reqwest::Method::TRACE, &request.url),
            Method::CONNECT => {
                return Err(TropelError::Http("CONNECT method not supported".into()));
            }
        };

        // Add headers
        if let Some(content_type) = multipart_content_type {
            if !request
                .headers
                .keys()
                .any(|k| k.eq_ignore_ascii_case("content-type"))
            {
                req_builder = req_builder.header("Content-Type", content_type);
            }
        }
        for (key, value) in &request.headers {
            req_builder = req_builder.header(key.as_str(), value.as_str());
        }

        // Add query parameters
        if !request.query_params.is_empty() {
            req_builder = req_builder.query(&request.query_params);
        }

        // Set timeout (client-level timeout is already set, request can override shorter)
        if let Some(timeout) = request.timeout {
            req_builder = req_builder.timeout(timeout);
        }

        // Build the request, then apply auth IN PLACE. Signers need the final
        // method/URL/body (SigV4, OAuth1, Hawk), which a RequestBuilder cannot
        // expose, so the auth happens on the built Request.
        let mut built_request = req_builder.build().map_err(|e| {
            TropelError::Http(format!("Failed to build request: {}", e))
        })?;
        if let Some(signer) = signer {
            signer
                .sign(&mut built_request)
                .map_err(|e| TropelError::Http(format!("Auth signing failed: {}", e)))?;
        }

        // Keep a clone for the Digest challenge-response retry below. For all
        // other signers `challenge_response` returns None and this is unused.
        let retry_request = built_request.try_clone();

        // ═══════════════════════════════════════════════════════
        // Phase 1: Send request → receive response head (TTFB)
        // ═══════════════════════════════════════════════════════
        // The response head (status line + headers) is received when this
        // resolves. The measured "waiting" time includes everything up to
        // this point: blocked + DNS + TCP connect + TLS handshake + sending +
        // server processing.
        let waiting_start = std::time::Instant::now();
        let mut response = self
            .inner
            .execute(built_request)
            .await
            .map_err(|e| TropelError::Http(format!("Request failed: {}", e)))?;
        let mut waiting_duration = waiting_start.elapsed();

        // HTTP Digest (RFC 7616) is challenge-response: the first request goes
        // out unauthenticated, and on a 401 with a `WWW-Authenticate: Digest`
        // header we compute the Authorization value and retry once. The
        // retried response replaces the 401 for all downstream processing.
        if response.status().as_u16() == 401 {
            if let Some(signer) = signer {
                let www = response
                    .headers()
                    .get(reqwest::header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string);
                if let Some(www) = www {
                    if let Some(mut retry) = retry_request {
                        if let Some(auth_value) = signer.challenge_response(&www, &retry) {
                            retry
                                .headers_mut()
                                .insert(
                                    reqwest::header::AUTHORIZATION,
                                    auth_value.parse().map_err(|_| {
                                        TropelError::Http(
                                            "Invalid digest Authorization header value".into(),
                                        )
                                    })?,
                                );
                            let retry_start = std::time::Instant::now();
                            response = self
                                .inner
                                .execute(retry)
                                .await
                                .map_err(|e| TropelError::Http(format!("Request failed: {}", e)))?;
                            waiting_duration = retry_start.elapsed();
                            tracing::debug!(
                                "Digest auth: retried after 401 challenge (status now {})",
                                response.status().as_u16()
                            );
                        }
                    }
                }
            }
        }

        let status_code = response.status().as_u16();
        let status_text = response
            .status()
            .canonical_reason()
            .unwrap_or("Unknown")
            .to_string();

        // Collect response headers
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();

        // ═══════════════════════════════════════════════════════
        // Phase 2: Receive response body
        // ═══════════════════════════════════════════════════════
        // The body is skipped when the GLOBAL discard_response_bodies flag is
        // set OR the per-request k6 `responseType: "none"` is requested — both
        // save bandwidth/memory; scripts just see an empty body.
        let receiving_start = std::time::Instant::now();
        let discard = self.discard_bodies
            || request.response_type == tropel_core::types::ResponseType::None;
        let body_vec = if discard {
            Vec::new()
        } else {
            response
                .bytes()
                .await
                .map_err(|e| TropelError::Http(format!("Failed to read response body: {}", e)))?
                .to_vec()
        };
        let receiving_duration = receiving_start.elapsed();
        let size = body_vec.len() as u64;

        let total_duration = total_start.elapsed();

        // Build sub-timings from the real phases recorded by the
        // `dns_resolver` and `connector_layer` hooks (thread-local slot).
        // When the request reused a pooled keep-alive connection no connector
        // call happened, so the connect phases are ZERO — matching k6, which
        // also reports ~0 blocked/connecting for pooled connections.
        //
        // Note: `dns` is optional on purpose — for IP-literal hosts (e.g.
        // "127.0.0.1") reqwest's HttpConnector skips DNS resolution entirely,
        // so only the connect phases exist.
        let phases = crate::subtimings::take_slot();
        let mut timings = Timings::from_measured(waiting_duration, receiving_duration, total_duration);
        if let (Some(request_start), Some(connect_start), Some(connect_elapsed)) = (
            phases.request_start,
            phases.connect_start,
            phases.connect_elapsed,
        ) {
            timings.blocked = connect_start.saturating_duration_since(request_start);
            timings.dns = phases.dns_elapsed.unwrap_or_default();
            // connect_elapsed spans DNS + TCP (+ TLS for https); subtract the
            // separately-measured DNS to leave the transport phases.
            timings.connecting = connect_elapsed.saturating_sub(timings.dns);
        }

        if self.http_debug {
            tracing::info!(
                "HTTP <<< {:?} {} -> {} ({} bytes in {:.2?})",
                request.method,
                request.url,
                status_code,
                size,
                total_duration
            );
        }

        let response = HttpResponse {
            status_code,
            status_text,
            headers,
            body: body_vec,
            response_time: total_duration,
            timings: Some(timings),
            cookies: vec![],
            size,
            request_body_size,
        };

        Ok(response)
    }

    /// Get an auth signer based on the auth config.
    ///
    /// Delegates to the single consolidated signer builder
    /// ([`crate::auth::build_auth_signer`]) shared with the executor runner,
    /// so every auth type (Bearer, Basic, ApiKey, OAuth2, SigV4, OAuth1,
    /// Hawk, Digest) is supported in exactly one place.
    pub fn get_signer(&self, auth: &AuthConfig) -> Option<Box<dyn AuthSigner>> {
        crate::auth::build_auth_signer(auth)
    }
}

/// HTTP response data (mirrors `tropel_core::Response` but from reqwest).
/// Body text and JSON are NOT eagerly parsed — see `body_text()` / `body_json()`.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub response_time: Duration,
    pub timings: Option<Timings>,
    pub cookies: Vec<Cookie>,
    pub size: u64,
    /// Number of bytes sent in the request body (for data_sent tracking).
    pub request_body_size: u64,
}

impl From<&HttpResponse> for tropel_core::types::Response {
    fn from(resp: &HttpResponse) -> Self {
        tropel_core::types::Response {
            status_code: resp.status_code,
            status_text: resp.status_text.clone(),
            headers: resp.headers.clone(),
            body: resp.body.clone(),
            response_time: resp.response_time,
            timings: resp.timings.clone(),
            cookies: resp.cookies.clone(),
            size: resp.size,
        }
    }
}

impl HttpResponse {
    /// Decode the body as UTF-8 text (lazy — parses on each call).
    pub fn body_text(&self) -> Option<String> {
        if self.body.is_empty() {
            None
        } else {
            String::from_utf8(self.body.clone()).ok()
        }
    }

    /// Parse the body as JSON using simd-json (lazy — parses on each call).
    ///
    /// Parses directly from raw bytes, skipping the `String::from_utf8`
    /// intermediate step. Uses `simd-json` for ~2-4x faster parsing.
    pub fn body_json(&self) -> Option<serde_json::Value> {
        if self.body.is_empty() {
            return None;
        }
        let mut body_bytes = self.body.clone();
        simd_json::serde::from_slice(&mut body_bytes).ok()
    }
}

/// Parse a TLS version string ("1.2", "tls1.2", "1.3", ...) into a reqwest
/// TLS version. Returns None for unrecognized/empty values (builder defaults).
fn parse_tls_version(s: &Option<String>) -> Option<reqwest::tls::Version> {
    let v = s.as_deref()?.trim().to_ascii_lowercase();
    match v.as_str() {
        "1.0" | "tls1.0" | "tlsv1.0" | "tls1" => Some(reqwest::tls::Version::TLS_1_0),
        "1.1" | "tls1.1" | "tlsv1.1" => Some(reqwest::tls::Version::TLS_1_1),
        "1.2" | "tls1.2" | "tlsv1.2" | "tls12" => Some(reqwest::tls::Version::TLS_1_2),
        "1.3" | "tls1.3" | "tlsv1.3" | "tls13" => Some(reqwest::tls::Version::TLS_1_3),
        _ => None,
    }
}

/// Calculate the byte size of a request body.
pub fn body_size(body: &Body) -> usize {
    match body {
        Body::Raw(s) => s.len(),
        Body::Json(val) => serde_json::to_string(val).unwrap_or_default().len(),
        Body::FormData(map) => multipart_form_data_bytes(map).len(),
        Body::UrlEncoded(map) => map
            .iter()
            .map(|(k, v)| k.len() + v.len() + 1)
            .sum::<usize>(),
        Body::Binary(data) => data.len(),
        Body::GraphQL { query, variables } => {
            // Exact wire size — the same serializer the client sends, so
            // data_sent accounting can't diverge from the actual body.
            Body::graphql_json_string(query, variables).len()
        }
    }
}

fn multipart_form_data_bytes(map: &HashMap<String, String>) -> Vec<u8> {
    let mut body = Vec::new();

    for (name, value) in map {
        body.extend_from_slice(format!("--{}\r\n", MULTIPART_BOUNDARY).as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{}\"\r\n\r\n",
                escape_multipart_field_name(name)
            )
            .as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{}--\r\n", MULTIPART_BOUNDARY).as_bytes());
    body
}

fn escape_multipart_field_name(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn body_to_reqwest(body: &Body) -> reqwest::Body {
    match body {
        Body::Raw(s) => s.clone().into(),
        Body::Json(val) => serde_json::to_string(val).unwrap_or_default().into(),
        Body::FormData(map) => reqwest::Body::from(multipart_form_data_bytes(map)),
        Body::UrlEncoded(map) => {
            let params: Vec<(String, String)> = map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone().to_string()))
                .collect();
            reqwest::Body::from(serde_urlencoded::to_string(params).unwrap_or_default())
        }
        Body::Binary(data) => data.clone().into(),
        Body::GraphQL { query, variables } => {
            // The single shared serializer — includes `variables` when
            // present (the old code dropped them entirely, so scripts that
            // relied on `variables` sent a query with NO variables).
            Body::graphql_json_string(query, variables).into()
        }
    }
}

#[cfg(test)]
mod multipart_tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn multipart_form_data_serializes_with_boundary() {
        let mut formdata = HashMap::new();
        formdata.insert("field1".to_string(), "value1".to_string());
        formdata.insert("field 2".to_string(), "two".to_string());

        let bytes = multipart_form_data_bytes(&formdata);
        let text = String::from_utf8(bytes.clone()).expect("multipart body must be UTF-8");

        assert!(text.contains("Content-Disposition: form-data; name=\"field1\""));
        assert!(text.contains("Content-Disposition: form-data; name=\"field 2\""));
        assert!(text.contains("value1"));
        assert!(text.contains("two"));
        assert!(text.ends_with("------------------------tropel-boundary-7a2f24b9--\r\n"));
        assert_eq!(body_size(&Body::FormData(formdata)), bytes.len());
    }

    #[test]
    fn graphql_body_includes_variables() {
        // Regression: `body_to_reqwest` destructured `variables: _` and sent
        // ONLY the query — a GraphQL request with variables silently dropped
        // them (the server would error or return wrong results).
        let mut vars: HashMap<String, serde_json::Value> = HashMap::new();
        vars.insert("id".to_string(), serde_json::json!("42"));
        let body = Body::GraphQL {
            query: "query($id: ID!) { user(id: $id) { name } }".to_string(),
            variables: Some(vars),
        };
        let req_body = body_to_reqwest(&body);
        let bytes = req_body.as_bytes().expect("reqwest body is bytes");
        let json: serde_json::Value =
            serde_json::from_slice(bytes).expect("GraphQL wire body is valid JSON");
        assert_eq!(json["query"], "query($id: ID!) { user(id: $id) { name } }");
        assert_eq!(json["variables"]["id"], "42");
        // body_size must account for the variables too (exact, not the old
        // `query.len() + 50` approximation).
        assert_eq!(body_size(&body), bytes.len());
    }

    #[test]
    fn graphql_body_omits_empty_variables() {
        // No variables map → the wire JSON has NO "variables" key at all
        // (strict servers reject an empty `variables: {}`).
        let body = Body::GraphQL {
            query: "{ hello }".to_string(),
            variables: None,
        };
        let req_body = body_to_reqwest(&body);
        let bytes = req_body.as_bytes().expect("reqwest body is bytes");
        let json: serde_json::Value =
            serde_json::from_slice(bytes).expect("GraphQL wire body is valid JSON");
        assert_eq!(json["query"], "{ hello }");
        assert!(json.get("variables").is_none());
        assert_eq!(body_size(&body), bytes.len());
    }
}

pub(crate) fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    if let Some(num_str) = s.strip_suffix("ms") {
        let ms: u64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_millis(ms))
    } else if let Some(num_str) = s.strip_suffix('s') {
        let secs: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    } else if let Some(num_str) = s.strip_suffix('m') {
        let mins: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(mins * 60.0))
    } else if let Some(num_str) = s.strip_suffix('h') {
        let hours: f64 = num_str
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(hours * 3600.0))
    } else {
        // Default to seconds
        let secs: f64 = s
            .parse()
            .map_err(|_| TropelError::Config(format!("Invalid duration: {}", s)))?;
        Ok(Duration::from_secs_f64(secs))
    }
}

/// Re-export serde_urlencoded for form body encoding.
mod serde_urlencoded {

    pub fn to_string(pairs: Vec<(String, String)>) -> Result<String, std::convert::Infallible> {
        let encoded: Vec<String> = pairs
            .iter()
            .map(|(k, v)| {
                let k = urlencoding(k);
                let v = urlencoding(v);
                format!("{}={}", k, v)
            })
            .collect();
        Ok(encoded.join("&"))
    }

    fn urlencoding(s: &str) -> String {
        s.chars()
            .map(|c| match c {
                'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
                ' ' => "+".to_string(),
                _ => format!("%{:02X}", c as u8),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[test]
    fn test_parse_duration() {
        assert_eq!(
            super::parse_duration("500ms").unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(super::parse_duration("5s").unwrap(), Duration::from_secs(5));
        assert_eq!(
            super::parse_duration("1.5s").unwrap(),
            Duration::from_millis(1500)
        );
        assert_eq!(
            super::parse_duration("2m").unwrap(),
            Duration::from_secs(120)
        );
        assert_eq!(
            super::parse_duration("1h").unwrap(),
            Duration::from_secs(3600)
        );
    }

    #[test]
    fn test_form_urlencoding() {
        let result = super::serde_urlencoded::to_string(vec![
            ("key".to_string(), "value".to_string()),
            ("name".to_string(), "hello world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, "key=value&name=hello+world");
    }
}

