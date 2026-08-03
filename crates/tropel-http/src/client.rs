use crate::auth::AuthSigner;
use crate::dns::DnsResolver;
use crate::rps::RpsLimiter;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
    /// Primary client: follows redirects per `HttpConfig.max_redirects`.
    inner: reqwest::Client,
    /// Twin client that never follows redirects (`Policy::none()`), used when
    /// a request sets `follow_redirects: false` (reqwest bakes the redirect
    /// policy into the client at build time, so per-request redirect control
    /// needs a second client). `None` when `max_redirects == 0` — the primary
    /// client already never follows.
    no_redirect: Option<reqwest::Client>,
    /// Lazily-built clients for per-request mTLS identities, keyed by
    /// `(cert_path, key_path, follow_redirects)`. The identity is baked into
    /// the client at build time, so a per-request `Request.certificate` needs
    /// its own client; built on first use and cached per distinct identity.
    /// `Arc<Mutex<…>>` because `std::sync::Mutex` is not `Clone` while
    /// `HttpClient` derives `Clone`.
    cert_clients: Arc<Mutex<HashMap<(String, String, bool), reqwest::Client>>>,
    /// Config snapshot used to lazily build per-certificate clients.
    config: HttpConfig,
    /// TLS snapshot used to lazily build per-certificate clients.
    tls: TlsConfig,
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
        let identity = Self::load_global_identity(tls)?;
        let redirect = if config.max_redirects > 0 {
            reqwest::redirect::Policy::limited(config.max_redirects as usize)
        } else {
            reqwest::redirect::Policy::none()
        };
        let inner = Self::build_client(config, tls, identity.clone(), redirect)?;
        // When the primary client follows redirects, a second client with
        // `Policy::none()` backs `follow_redirects: false` requests. When
        // `max_redirects == 0` the primary already never follows, so both
        // request shapes reuse `inner`.
        let no_redirect = if config.max_redirects > 0 {
            Some(Self::build_client(
                config,
                tls,
                identity,
                reqwest::redirect::Policy::none(),
            )?)
        } else {
            None
        };

        Ok(Self {
            inner,
            no_redirect,
            cert_clients: Arc::new(Mutex::new(HashMap::new())),
            config: config.clone(),
            tls: tls.clone(),
            discard_bodies: config.discard_response_bodies,
            rps,
            http_debug: config.http_debug,
        })
    }

    /// Build a `reqwest::Client` from the full HTTP/TLS configuration with an
    /// optional mTLS identity and an explicit redirect policy. This is the
    /// single builder shared by the primary client, the no-redirect twin, and
    /// the lazily-built per-request-certificate clients.
    fn build_client(
        config: &HttpConfig,
        tls: &TlsConfig,
        identity: Option<reqwest::Identity>,
        redirect: reqwest::redirect::Policy,
    ) -> Result<reqwest::Client> {
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
        if let Some(identity) = identity {
            builder = builder.identity(identity);
        }

        if !config.decompress {
            builder = builder.no_deflate();
            builder = builder.no_gzip();
            builder = builder.no_brotli();
        }

        builder = builder.redirect(redirect);

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

        builder
            .build()
            .map_err(|e| TropelError::Http(format!("Failed to create HTTP client: {}", e)))
    }

    /// Load the global mTLS identity from `TlsConfig` (if configured).
    fn load_global_identity(tls: &TlsConfig) -> Result<Option<reqwest::Identity>> {
        if let Some(cert_path) = &tls.client_cert {
            let key_path = tls.client_key.as_deref().ok_or_else(|| {
                TropelError::Config(format!(
                    "TLS client_cert '{}' set but no client_key: a client \
                     identity requires both a certificate and its private key",
                    cert_path
                ))
            })?;
            Ok(Some(Self::load_pem_identity(
                cert_path,
                key_path,
                tls.client_passphrase.as_deref(),
            )?))
        } else {
            if tls.client_key.is_some() {
                tracing::warn!("client_key is set without client_cert — the key will be ignored");
            }
            Ok(None)
        }
    }

    /// Read a PEM cert + key pair from disk and build a `reqwest::Identity`.
    ///
    /// Concatenates cert + key into ONE PEM buffer and uses
    /// `Identity::from_pem` (the only identity constructor available under the
    /// rustls feature). It parses mixed PEM sections and accepts PKCS#8
    /// (`BEGIN PRIVATE KEY`), PKCS#1 (`BEGIN RSA PRIVATE KEY`) and SEC1
    /// (`BEGIN EC PRIVATE KEY`) keys. PKCS#12 bundles and encrypted PEM keys
    /// require the native-tls backend; a supplied passphrase logs a warning
    /// (its value is never logged) and the key must be unencrypted PEM.
    fn load_pem_identity(
        cert_path: &str,
        key_path: &str,
        passphrase: Option<&str>,
    ) -> Result<reqwest::Identity> {
        if passphrase.is_some() {
            tracing::warn!(
                "client_passphrase is only honored with the native-tls backend; \
                 this rustls build uses unencrypted PEM keys, so the supplied \
                 passphrase will be ignored"
            );
        }
        let cert_bytes = std::fs::read(cert_path).map_err(|e| {
            TropelError::Config(format!("Failed to read client cert '{}': {}", cert_path, e))
        })?;
        let key_bytes = std::fs::read(key_path).map_err(|e| {
            TropelError::Config(format!("Failed to read client key '{}': {}", key_path, e))
        })?;
        let mut combined = cert_bytes;
        combined.extend_from_slice(b"\n");
        combined.extend_from_slice(&key_bytes);
        reqwest::Identity::from_pem(&combined).map_err(|e| {
            TropelError::Config(format!(
                "Failed to load PEM client identity (cert '{}', key '{}'): {}",
                cert_path, key_path, e
            ))
        })
    }

    /// Pick the `reqwest::Client` for a request, honoring the per-request
    /// `follow_redirects` and `certificate` overrides that reqwest bakes in at
    /// client-build time:
    /// - `follow_redirects: false` → the no-redirect twin client
    /// - `certificate` → a lazily-built client with that mTLS identity,
    ///   cached per (cert, key, follow_redirects) so each distinct identity
    ///   is loaded from disk exactly once
    ///
    /// Note: the `cert_clients` lock is held across `load_pem_identity` (file
    /// reads) and `build_client`. This is safe because clients are per-VU and
    /// run on single-threaded current-thread runtimes (no awaits under the
    /// lock, no concurrent callers for the same `HttpClient`). A future
    /// refactor that shares one client across threads must build outside the
    /// lock instead.
    fn select_client(&self, request: &Request) -> Result<reqwest::Client> {
        let follow = request.follow_redirects;
        match &request.certificate {
            Some(cert) => {
                let cert_path = cert.cert.as_deref().ok_or_else(|| {
                    TropelError::Config("per-request certificate requires a cert path".into())
                })?;
                let key_path = cert.key.as_deref().ok_or_else(|| {
                    TropelError::Config("per-request certificate requires a key path".into())
                })?;
                let cache_key = (cert_path.to_string(), key_path.to_string(), follow);
                let mut cache = self.cert_clients.lock().unwrap();
                if let Some(client) = cache.get(&cache_key) {
                    return Ok(client.clone());
                }
                let identity =
                    Self::load_pem_identity(cert_path, key_path, cert.passphrase.as_deref())?;
                let redirect = if follow && self.config.max_redirects > 0 {
                    reqwest::redirect::Policy::limited(self.config.max_redirects as usize)
                } else {
                    reqwest::redirect::Policy::none()
                };
                let client =
                    Self::build_client(&self.config, &self.tls, Some(identity), redirect)?;
                cache.insert(cache_key, client.clone());
                Ok(client)
            }
            None => {
                if follow {
                    Ok(self.inner.clone())
                } else if let Some(no_redirect) = &self.no_redirect {
                    Ok(no_redirect.clone())
                } else {
                    Ok(self.inner.clone())
                }
            }
        }
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

        // Per-request overrides (mTLS identity, follow_redirects) are baked
        // into the reqwest client at build time, so select the right client
        // for THIS request before building the RequestBuilder.
        let client = self.select_client(request)?;

        let mut req_builder = match request.method {
            Method::GET => client.get(&request.url),
            Method::POST => {
                let rb = client.post(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::PUT => {
                let rb = client.put(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::PATCH => {
                let rb = client.patch(&request.url);
                if let Some(body) = &request.body {
                    rb.body(body_to_reqwest(body))
                } else {
                    rb
                }
            }
            Method::DELETE => client.delete(&request.url),
            Method::HEAD => client.head(&request.url),
            Method::OPTIONS => client.request(reqwest::Method::OPTIONS, &request.url),
            Method::TRACE => client.request(reqwest::Method::TRACE, &request.url),
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
        let mut response = client
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
                            response = client
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
        // The body is drained-but-not-stored when the GLOBAL
        // discard_response_bodies flag is set OR the per-request k6
        // `responseType: "none"` is requested — scripts see an empty body,
        // but the bytes are still read so the pooled connection survives.
        let receiving_start = std::time::Instant::now();
        let discard = self.discard_bodies
            || request.response_type == tropel_core::types::ResponseType::None;
        // When the body is discarded (global `discardResponseBodies` or the
        // per-request k6 `responseType: "none"`), we must STILL read the body
        // off the wire so reqwest can return the connection to the pool.
        // Dropping the `Response` unread closes the socket — every request
        // then opens a fresh TCP connection, the exact opposite of the
        // pooling these flags are meant to preserve. We drain the body and
        // throw the bytes away; the drained byte count still feeds
        // `size`/`data_received` so accounting matches the wire.
        //
        // Behavior notes (intended): with discard, `http_req_receiving` and
        // `data_received` now reflect the real drain time / wire bytes instead
        // of ~0 — k6 still downloads the body and only skips storing it. And
        // a server that streams forever now fails at the request timeout
        // (chunk error) instead of silently succeeding with an empty body,
        // which is more correct.
        let (body_vec, size) = if discard {
            let mut drained: u64 = 0;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| TropelError::Http(format!("Failed to drain response body: {}", e)))?
            {
                drained += chunk.len() as u64;
            }
            (Vec::new(), drained)
        } else {
            let body = response
                .bytes()
                .await
                .map_err(|e| TropelError::Http(format!("Failed to read response body: {}", e)))?
                .to_vec();
            (body.clone(), body.len() as u64)
        };
        let receiving_duration = receiving_start.elapsed();

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
    use super::*;
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

    // ── per-request client selection (TROPEL_TODO_V2: "client cert and
    //    follow_redirects are ignored — fixed at client build") ──

    #[test]
    fn no_redirect_twin_built_only_when_following_enabled() {
        // max_redirects > 0 → a Policy::none() twin exists to serve
        // `follow_redirects: false` requests (the redirect policy is baked
        // into the client at build time).
        let cfg = HttpConfig::default(); // max_redirects = 10
        let client = HttpClient::new(&cfg).unwrap();
        assert!(client.no_redirect.is_some());

        // max_redirects == 0 → the primary client already never follows, so
        // no twin is needed and both request shapes share `inner`.
        let mut no_redirect_cfg = HttpConfig::default();
        no_redirect_cfg.max_redirects = 0;
        let client = HttpClient::new(&no_redirect_cfg).unwrap();
        assert!(client.no_redirect.is_none());
    }

    #[test]
    fn select_client_no_error_for_plain_requests() {
        // Both follow shapes resolve to a client (no panics / no errors);
        // behavioral proof of which one is used lives in the async redirect
        // test below.
        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        for follow in [true, false] {
            let req = Request {
                follow_redirects: follow,
                ..Default::default()
            };
            assert!(client.select_client(&req).is_ok(), "follow={}", follow);
        }
    }

    #[test]
    fn select_client_cert_missing_file_errors() {
        // Regression: Request.certificate was silently ignored at client
        // build. A missing cert file must now surface a Config error instead
        // of proceeding without the identity.
        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        let cert = CertificateConfig {
            cert: Some("missing.pem".to_string()),
            key: Some("missing.key".to_string()),
            passphrase: None,
        };
        // Both attempts fail on the missing files (proving the cert path IS
        // exercised) rather than being silently ignored.
        let follow_req = Request {
            certificate: Some(cert.clone()),
            follow_redirects: true,
            ..Default::default()
        };
        let no_follow_req = Request {
            certificate: Some(cert),
            follow_redirects: false,
            ..Default::default()
        };
        assert!(client.select_client(&follow_req).is_err());
        assert!(client.select_client(&no_follow_req).is_err());
    }

    #[test]
    fn select_client_certificate_requires_both_paths() {
        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        let req = Request {
            certificate: Some(CertificateConfig {
                cert: Some("cert.pem".to_string()),
                key: None,
                passphrase: None,
            }),
            ..Default::default()
        };
        let err = client.select_client(&req).unwrap_err();
        assert!(format!("{}", err).contains("key path"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn follow_redirects_false_returns_redirect_not_followed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // Tiny redirect server: /start → 302 → /final; /final → 200.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let req = String::from_utf8_lossy(&buf);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|l| l.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let resp = if path == "/start" {
                        "HTTP/1.1 302 Found\r\nLocation: /final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                    } else {
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_string()
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();

        // follow_redirects: true (default) → redirect is followed → 200.
        let follow_req = Request {
            url: format!("http://{}/start", addr),
            method: Method::GET,
            follow_redirects: true,
            ..Default::default()
        };
        let resp = client.execute(&follow_req, None).await.unwrap();
        assert_eq!(resp.status_code, 200, "redirect should be followed");

        // follow_redirects: false → the 302 is returned to the caller.
        let no_follow_req = Request {
            url: format!("http://{}/start", addr),
            method: Method::GET,
            follow_redirects: false,
            ..Default::default()
        };
        let resp = client.execute(&no_follow_req, None).await.unwrap();
        assert_eq!(resp.status_code, 302, "redirect must NOT be followed");

        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn discarded_body_still_reuses_pooled_connection() {
        // Regression for TROPEL_TODO_V2: when discardResponseBodies (or
        // responseType: "none") was set, execute() left the body unread and
        // dropped the Response — reqwest then closed the socket, so every
        // request opened a fresh TCP connection (the opposite of pooling).
        // With the drain fix, N sequential requests must ride ONE connection.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let connections = Arc::new(AtomicUsize::new(0));
        let connections_srv = connections.clone();

        // Keep-alive server that counts every accepted TCP connection and
        // serves a read-loop (like the subtimings test) so pooled requests
        // can reuse the same socket.
        let server = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else { break };
                connections_srv.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    while let Ok(n) = sock.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        sock.write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: keep-alive\r\n\r\nok",
                        )
                        .await
                        .unwrap();
                    }
                });
            }
        });

        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        let req = Request {
            url: format!("http://{}/", addr),
            method: Method::GET,
            response_type: tropel_core::types::ResponseType::None,
            ..Default::default()
        };

        // Three requests with discarded bodies. Each must return 200 with an
        // EMPTY body (the drain discards the bytes) — and because the body is
        // fully drained, reqwest returns the connection to the pool.
        for i in 0..3 {
            let resp = client.execute(&req, None).await.unwrap();
            assert_eq!(resp.status_code, 200, "request {} failed", i);
            assert!(resp.body.is_empty(), "discarded body must be empty");
            // The body is drained, not skipped: size/data_received still count
            // the wire bytes (Content-Length is 2 here) even though the body
            // is empty.
            assert_eq!(resp.size, 2, "drained bytes must still feed size");
        }

        // Give the server a moment to register any extra connects, then
        // assert the pool was reused: exactly ONE TCP connection for 3
        // requests. (Before the fix this was 3 — one reconnect per request.)
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        server.abort();
        assert_eq!(
            connections.load(Ordering::SeqCst),
            1,
            "discarded bodies must not tear down the pooled connection"
        );
    }
}

