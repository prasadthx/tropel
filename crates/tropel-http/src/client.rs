use crate::auth::AuthSigner;
use crate::dns::{parse_blacklist, DnsResolver, IpCidr};
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

/// Credential headers stripped on cross-origin redirect hops and carried
/// forward on same-origin hops. Single source of truth — the two `matches!`
/// sites below must not drift (drift in exactly this kind of list is what
/// caused the signed-Authorization-dropped-on-redirect bug).
const CREDENTIAL_HEADERS: [&str; 8] = [
    "authorization",
    "cookie",
    "cookie2",
    "proxy-authorization",
    "www-authenticate",
    "x-amz-date",
    "x-amz-content-sha256",
    "x-amz-security-token",
];

fn is_credential_header(key: &str) -> bool {
    CREDENTIAL_HEADERS.contains(&key)
}

/// Canonicalize an HTTP header name to Go's MIME canonical form
/// (uppercase first letter of each dash-separated word, lowercase the
/// rest): `content-type` → `Content-Type`, `x-request-id` →
/// `X-Request-Id`. The `http`/reqwest crate lowercases every `HeaderName`,
/// so without this every k6/Postman doc idiom (`res.headers['Content-Type']`,
/// `pm.response.header('Content-Type')`) would see `undefined`.
fn canonical_header_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = true;
    for c in name.chars() {
        if c == '-' {
            out.push('-');
            upper = true;
        } else if upper {
            out.push(c.to_ascii_uppercase());
            upper = false;
        } else {
            out.push(c.to_ascii_lowercase());
        }
    }
    out
}

/// Per-certificate-identity HTTP clients (`Arc<Mutex<…>>` because
/// `std::sync::Mutex` is not `Clone` while `HttpClient` derives `Clone`).
type CertClientMap = Arc<Mutex<HashMap<(String, String, bool), reqwest::Client>>>;

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
    cert_clients: CertClientMap,
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
    /// k6 `blacklistIPs` CIDRs, parsed once at client build. Hostnames are
    /// filtered by the DNS resolver, but an IP-literal URL (e.g.
    /// `http://127.0.0.1:8080`) never triggers a lookup — reqwest hands the
    /// literal straight to the connector. This list backs the per-hop
    /// literal check in `execute()` so a blacklisted literal (including a
    /// redirect target) is rejected before any connection attempt.
    blacklist: Vec<IpCidr>,
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
            blacklist: parse_blacklist(&config.blacklist_ips),
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
        self.select_client_with_follow(request, request.follow_redirects)
    }

    /// Like [`select_client`], but with an explicit follow decision. Used by
    /// `execute()` for manual redirect following: when Tropel follows
    /// redirects ITSELF (k6 parity — every hop counts as a request), it needs
    /// a `Policy::none()` client so reqwest never auto-follows behind its
    /// back; the no-redirect twin provides exactly that.
    fn select_client_with_follow(
        &self,
        request: &Request,
        follow: bool,
    ) -> Result<reqwest::Client> {
        // `--no-redirects` (HttpConfig.no_redirects) forces no-follow for
        // EVERY request, regardless of the per-request flag: the 3xx is
        // returned as-is (k6 always follows; this opt-out is a Tropel extra).
        let follow = follow && !self.config.no_redirects;
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
    ///   is included here)    /// - **waiting** (TTFB): from the request being sent to response headers
    ///   received, EXCLUDING the connection phases (blocked + dns + connecting)
    ///   — subtracted from the raw elapsed so the breakdown sums to `total`,
    ///   matching k6's `http_req_waiting` semantics.
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

        // Serialize the request body ONCE. The resulting bytes feed BOTH the
        // data_sent accounting (exact wire size) and the reqwest body — the
        // old code called body_size() and body_to_reqwest() separately, each
        // re-serializing JSON / urlencoded / multipart bodies (2× work per
        // request on the hot path). The bytes are memcpy'd into reqwest
        // (cheap); the expensive serialization happens exactly once.
        let body_bytes: Option<Vec<u8>> = request.body.as_ref().map(body_to_bytes);
        let request_body_size: u64 = body_bytes.as_ref().map_or(0, |b| b.len() as u64);

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

        // ── Redirect handling — k6 parity ──
        // k6 counts EVERY redirect hop as its own request (the test.k6.io 302
        // chain produced 136 http_reqs for 68 iterations; Tropel recorded only
        // the final 64). We therefore follow redirects MANUALLY: each 3xx hop
        // becomes its own HttpResponse collected into `redirects`, and the
        // returned response is the FINAL one (what scripts see via
        // pm.response / res), with `redirects` attached for per-hop sample
        // emission by callers.
        //
        // `--no-redirects` (HttpConfig.no_redirects) disables following
        // entirely: the 3xx response is returned as-is with no hops captured
        // — an option k6 itself lacks (it always follows up to maxRedirects).
        let manual_follow = request.follow_redirects
            && !self.config.no_redirects
            && self.config.max_redirects > 0;
        let client = if manual_follow {
            // Manual following needs a Policy::none() client so reqwest never
            // auto-follows behind our back (the no-redirect twin).
            self.select_client_with_follow(request, false)?
        } else {
            self.select_client(request)?
        };
        let max_hops = if manual_follow {
            self.config.max_redirects as usize
        } else {
            0
        };
        let mut redirects: Vec<HttpResponse> = Vec::new();
        let mut current_url = request.url.clone();
        let mut current_method = request.method.clone();
        let mut current_body: Option<Vec<u8>> = body_bytes.clone();
        let mut hop_index: usize = 0;
        // Set when the previous hop redirected to a DIFFERENT origin: the
        // next hop must not carry credentials (Authorization/Cookie/…),
        // matching reqwest's and k6's redirect policies.
        let mut strip_sensitive = false;
        // Headers the signer ADDED to the hop-0 request (Authorization,
        // x-amz-*, …). `request.headers` is never mutated by signers — they
        // sign the built reqwest::Request in place — so without this capture
        // the signed header dies with hop 0's consumed request and a
        // same-origin redirect 401s (reqwest's auto-follow used to forward
        // it for us).
        let mut signed_headers: Vec<(String, String)> = Vec::new();

        loop {
            // Each redirect hop is checked too — a Location header pointing
            // at a blacklisted literal must not slip past the resolver.
            check_literal_blacklist(&self.blacklist, &current_url)?;
            let hop_start = std::time::Instant::now();
            // Per-request slot: concurrent requests (http.batch) interleave on
            // one thread and futures migrate threads on the shared io_rt, so
            // phases can no longer live in a single thread-local. Each hop
            // gets its own slot; the TimedRequest wrapper below makes the
            // DNS/connector hooks attribute to THIS request during every poll.
            let slot = crate::subtimings::begin_request(hop_start);

            // Build the reqwest request for THIS hop (URL/method/body may
            // have been rewritten by a redirect). Match by reference: the
            // `Custom` arm binds `m: &String`, so `current_method` is NOT
            // moved out of — it is still needed by the redirect-rewrite
            // logic below (303 → GET etc.).
            let mut req_builder = match &current_method {
                Method::GET => client.get(&current_url),
                Method::POST => {
                    let rb = client.post(&current_url);
                    if let Some(bytes) = &current_body {
                        rb.body(reqwest::Body::from(bytes.clone()))
                    } else {
                        rb
                    }
                }
                Method::PUT => {
                    let rb = client.put(&current_url);
                    if let Some(bytes) = &current_body {
                        rb.body(reqwest::Body::from(bytes.clone()))
                    } else {
                        rb
                    }
                }
                Method::PATCH => {
                    let rb = client.patch(&current_url);
                    if let Some(bytes) = &current_body {
                        rb.body(reqwest::Body::from(bytes.clone()))
                    } else {
                        rb
                    }
                }
                Method::DELETE => client.delete(&current_url),
                Method::HEAD => client.head(&current_url),
                Method::OPTIONS => client.request(reqwest::Method::OPTIONS, &current_url),
                Method::TRACE => client.request(reqwest::Method::TRACE, &current_url),
                Method::CONNECT => {
                    return Err(TropelError::Http("CONNECT method not supported".into()));
                }
                // Custom token (PURGE, LINK, …): parse into a reqwest Method.
                // `from_bytes` validates the token charset, so a token that
                // slipped past Method::parse would surface as a request-build
                // error here instead of silently becoming GET.
                Method::Custom(m) => {
                    let method = reqwest::Method::from_bytes(m.as_bytes()).map_err(|e| {
                        TropelError::Http(format!("Invalid HTTP method '{}': {}", m, e))
                    })?;
                    client.request(method, &current_url)
                }
            };

            // Add headers
            if let Some(content_type) = &multipart_content_type {
                if !request
                    .headers
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case("content-type"))
                {
                    req_builder = req_builder.header("Content-Type", content_type);
                }
            }
            for (key, value) in &request.headers {
                // Cross-origin redirect hops must not leak credentials to
                // another origin (reqwest's redirect policy strips
                // Authorization/Cookie/etc on origin change).
                if strip_sensitive && is_credential_header(&key.to_ascii_lowercase()) {
                    continue;
                }
                req_builder = req_builder.header(key.as_str(), value.as_str());
            }
            // Same-origin hops re-apply the signer-added headers captured at
            // hop 0 (Authorization, x-amz-*, …). Cross-origin hops
            // (strip_sensitive) drop them — credentials never leak to another
            // origin. Applied AFTER the base headers so a signer that
            // overrode a header (e.g. replaced a user-supplied Authorization)
            // wins on every hop, not just hop 0.
            if !strip_sensitive {
                for (key, value) in &signed_headers {
                    req_builder = req_builder.header(key.as_str(), value.as_str());
                }
            }

            // Add query parameters. ONLY on hop 0: the original request's
            // query_params describe the ORIGINAL URL. A redirect target URL
            // (Location header) carries its own query — re-appending the
            // original params there turns /x?page=2 → 302 /y?token=z into
            // /y?token=z&page=2 (k6/reqwest don't do this).
            if hop_index == 0 && !request.query_params.is_empty() {
                req_builder = req_builder.query(&request.query_params);
            }

            // Set timeout (client-level timeout is already set, request can override shorter)
            if let Some(timeout) = request.timeout {
                req_builder = req_builder.timeout(timeout);
            }

            // Build the request, then apply auth IN PLACE. Signers need the
            // final method/URL/body (SigV4, OAuth1, Hawk), which a
            // RequestBuilder cannot expose, so the auth happens on the built
            // Request. Auth is applied ONLY to the first hop: signing the
            // redirect target would be wrong (the signature is for the
            // original URL). The signer-added headers are captured and
            // re-applied on same-origin hops (see above); cross-origin hops
            // strip them, matching reqwest's redirect policy.
            let mut built_request = req_builder.build().map_err(|e| {
                TropelError::Http(format!("Failed to build request: {}", e))
            })?;
            if hop_index == 0 {
                if let Some(signer) = signer {
                    signer
                        .sign(&mut built_request)
                        .map_err(|e| TropelError::Http(format!("Auth signing failed: {}", e)))?;
                    // Capture what the signer added/changed vs the original
                    // headers so same-origin redirect hops re-apply them.
                    // Filtered to the credential header names (the same set
                    // the strip_sensitive check uses) so hop-0-only headers
                    // like reqwest's injected `Accept: */*` or the code's own
                    // multipart Content-Type are NOT carried to hops where
                    // the body/method may have been rewritten.
                    for (name, value) in built_request.headers().iter() {
                        let key = name.as_str().to_ascii_lowercase();
                        if !is_credential_header(&key) {
                            continue;
                        }
                        let value_str = value.to_str().unwrap_or("");
                        let identical_in_original = request.headers.iter().any(|(k, v)| {
                            k.eq_ignore_ascii_case(&key) && v == value_str
                        });
                        if !identical_in_original {
                            signed_headers.push((key, value_str.to_string()));
                        }
                    }
                }
            }

            // Keep a clone for the Digest challenge-response retry below. For
            // all other signers `challenge_response` returns None and this is
            // unused.
            let retry_request = built_request.try_clone();

        // ═══════════════════════════════════════════════════════
        // Phase 1: Send request → receive response head (TTFB)
        // ═══════════════════════════════════════════════════════
        // The response head (status line + headers) is received when this
        // resolves. The measured "waiting" time includes everything up to
        // this point: blocked + DNS + TCP connect + TLS handshake + sending +
        // server processing.
        let waiting_start = std::time::Instant::now();
        let mut response = crate::subtimings::TimedRequest::new(
            client.execute(built_request),
            slot.clone(),
        )
        .await
        .map_err(|e| TropelError::Http(format!("Request failed: {}", e)))?;
        let mut waiting_duration = waiting_start.elapsed();

        // HTTP Digest (RFC 7616) is challenge-response: the first request goes
        // out unauthenticated, and on a 401 with a `WWW-Authenticate: Digest`
        // header we compute the Authorization value and retry once. The
        // retried response replaces the 401 for all downstream processing.
        if response.status().as_u16() == 401 {
            if let Some(signer) = signer {
                // A server may send several `WWW-Authenticate` header LINES
                // (one per scheme: `Basic realm=...` then `Digest realm=...`)
                // — the old `.get()` read only the first line, so a Digest
                // challenge on a later line was never seen (backlog line 176).
                let www: Vec<String> = response
                    .headers()
                    .get_all(reqwest::header::WWW_AUTHENTICATE)
                    .iter()
                    .filter_map(|v| v.to_str().ok())
                    .map(str::to_string)
                    .collect();
                let www = www.join(", ");
                if !www.is_empty() {
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
                            response = crate::subtimings::TimedRequest::new(
                                client.execute(retry),
                                slot.clone(),
                            )
                            .await
                            .map_err(|e| {
                                TropelError::Http(format!("Request failed: {}", e))
                            })?;
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

        // Collect response headers — canonicalized to Go's MIME form
        // (Content-Type, X-Request-Id) because reqwest's HeaderName is
        // always lowercase; k6/Postman scripts index headers by their
        // canonical spelling and every doc example would otherwise see
        // undefined.
        let headers: HashMap<String, String> = response
            .headers()
            .iter()
            .map(|(k, v)| {
                (
                    canonical_header_name(k.as_str()),
                    v.to_str().unwrap_or("").to_string(),
                )
            })
            .collect();

        // Parse Set-Cookie headers into structured cookies so scripts can
        // read `res.cookies` (pm.response.cookies / k6 res.cookies). The
        // header may appear multiple times (one per cookie); a HashMap would
        // collapse them, so we walk `get_all` on the raw header map.
        let cookies: Vec<Cookie> = response
            .headers()
            .get_all(reqwest::header::SET_COOKIE)
            .iter()
            .filter_map(|v| parse_set_cookie(v.to_str().ok()?))
            .collect();

        // ── Redirect hop? ──
        // k6 parity: every redirect hop is its own request. When the response
        // is a 3xx with a Location header (and hops remain), capture it as a
        // hop response, resolve the next URL, rewrite method/body per RFC
        // 7231, and loop to send the next hop.
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let is_redirect = matches!(status_code, 301 | 302 | 303 | 307 | 308);
        if manual_follow && redirects.len() < max_hops && is_redirect {
            if let Some(location) = location {
                // Capture the hop as its own response (own duration). The
                // body is the usually-tiny redirect body — drain it so the
                // connection returns to the pool.
                let mut hop_body: Vec<u8> = Vec::new();
                while let Some(chunk) = response
                    .chunk()
                    .await
                    .map_err(|e| TropelError::Http(format!("Failed to read redirect body: {}", e)))?
                {
                    hop_body.extend_from_slice(&chunk);
                }
                let hop_total = hop_start.elapsed();
                let hop_phases = crate::subtimings::take_slot(&slot);
                let mut hop_timings =
                    Timings::from_measured(waiting_duration, Duration::ZERO, hop_total);
                if let (Some(request_start), Some(connect_start), Some(connect_elapsed)) = (
                    hop_phases.request_start,
                    hop_phases.connect_start,
                    hop_phases.connect_elapsed,
                ) {
                    hop_timings.blocked = connect_start.saturating_duration_since(request_start);
                    hop_timings.dns = hop_phases.dns_elapsed.unwrap_or_default();
                    hop_timings.connecting = connect_elapsed.saturating_sub(hop_timings.dns);
                }
                let hop_connect = hop_timings.blocked + hop_timings.dns + hop_timings.connecting;
                hop_timings.waiting = hop_timings.waiting.saturating_sub(hop_connect);

                // `size` counts the drained hop body bytes so data_received
                // per hop matches the wire (k6 counts per-request
                // data_received).
                let hop_size = hop_body.len() as u64;
                redirects.push(HttpResponse {
                    url: current_url.clone(),
                    status_code,
                    status_text,
                    headers,
                    body: hop_body,
                    text_cache: std::cell::OnceCell::new(),
                    json_cache: std::cell::OnceCell::new(),
                    response_time: hop_total,
                    timings: Some(hop_timings),
                    cookies,
                    size: hop_size,
                    request_body_size: 0,
                    redirects: Vec::new(),
                });

                // Resolve the Location header against the current URL.
                let base = reqwest::Url::parse(&current_url).map_err(|e| {
                    TropelError::Http(format!("Invalid request URL '{}': {}", current_url, e))
                })?;
                let next = base.join(&location).map_err(|e| {
                    TropelError::Http(format!("Invalid redirect Location '{}': {}", location, e))
                })?;

                // Cross-origin redirect → drop credentials for the next hop.
                let cur = reqwest::Url::parse(&current_url).ok();
                let same_origin = match &cur {
                    Some(c) => {
                        c.scheme() == next.scheme()
                            && c.host_str() == next.host_str()
                            && c.port_or_known_default() == next.port_or_known_default()
                    }
                    None => false,
                };
                if !same_origin {
                    strip_sensitive = true;
                }

                // RFC 7231 method rewrite (matches reqwest/k6):
                //   303 → GET (drop body), except HEAD stays HEAD
                //   301/302 → GET only for POST (drop body)
                //   307/308 → keep method and body
                match status_code {
                    303 if current_method != Method::HEAD => {
                        current_method = Method::GET;
                        current_body = None;
                    }
                    301 | 302 if current_method == Method::POST => {
                        current_method = Method::GET;
                        current_body = None;
                    }
                    _ => {}
                }

                tracing::debug!("Redirect {}: {} -> {}", status_code, current_url, next);
                current_url = next.to_string();
                hop_index += 1;
                continue;
            }
        }

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

        // The FINAL hop's own duration (k6 reports the last hop's time in its
        // final http_req_duration sample; the whole-chain wall time lives in
        // iteration_duration, so don't double-count the redirect hops here).
        let total_duration = hop_start.elapsed();

        // Build sub-timings from the real phases recorded by the
        // `dns_resolver` and `connector_layer` hooks (thread-local slot).
        // When the request reused a pooled keep-alive connection no connector
        // call happened, so the connect phases are ZERO — matching k6, which
        // also reports ~0 blocked/connecting for pooled connections.
        //
        // Note: `dns` is optional on purpose — for IP-literal hosts (e.g.
        // "127.0.0.1") reqwest's HttpConnector skips DNS resolution entirely,
        // so only the connect phases exist.
        let phases = crate::subtimings::take_slot(&slot);
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

        // k6 phase semantics: `http_req_waiting` (TTFB) is measured from the
        // moment the request is fully sent, EXCLUDING the connection phases.
        // Our `waiting_duration` is stamped just before `client.execute()`,
        // so for a fresh connection it *includes* blocked + DNS + connecting.
        // Subtract them so the breakdown sums to `total`:
        //   total = blocked + dns + connecting + waiting + receiving
        // (tls_handshaking/sending stay zero — reqwest seals those inside the
        // connector/request future; see the module docs.) For pooled reuse the
        // connect phases are zero, so `waiting` is unchanged.
        let connect_phases = timings.blocked + timings.dns + timings.connecting;
        timings.waiting = timings.waiting.saturating_sub(connect_phases);

        if self.http_debug {
            tracing::info!(
                "HTTP <<< {:?} {} -> {} ({} bytes in {:.2?})",
                request.method,
                current_url,
                status_code,
                size,
                total_duration
            );
        }

        let response = HttpResponse {
            url: current_url,
            status_code,
            status_text,
            headers,
            body: body_vec,
            text_cache: std::cell::OnceCell::new(),
            json_cache: std::cell::OnceCell::new(),
            response_time: total_duration,
            timings: Some(timings),
            cookies,
            size,
            request_body_size,
            // Every intermediate redirect hop, in order — callers emit one
            // http_req_* sample set per hop (k6 parity: a 302 chain counts
            // as hops + 1 requests, not just the final).
            redirects,
        };

        return Ok(response);
        } // end redirect-follow loop
    }/// Get an auth signer based on the auth config.
///
/// Delegates to the single consolidated signer builder
/// ([`crate::auth::build_auth_signer`]) shared with the executor runner,
/// so every auth type (Bearer, Basic, ApiKey, OAuth2, SigV4, OAuth1,
/// Hawk, Digest) is supported in exactly one place.
pub fn get_signer(&self, auth: &AuthConfig) -> Option<Box<dyn AuthSigner>> {
    crate::auth::build_auth_signer(auth)
}
}

/// k6 `blacklistIPs` enforcement for IP-literal hosts.
///
/// The DNS resolver filters hostnames (every resolved address is checked),
/// but an IP-literal URL (`http://127.0.0.1:8080`, `http://[::1]/`) never
/// triggers a lookup — reqwest hands the literal straight to the connector.
/// This rejects a literal that falls inside any blacklisted CIDR BEFORE any
/// connection attempt. Hostnames pass through untouched (the resolver owns
/// them); a URL whose host does not parse as an IP is not our concern.
fn check_literal_blacklist(blacklist: &[IpCidr], url: &str) -> Result<()> {
    if blacklist.is_empty() {
        return Ok(());
    }
    let parsed = reqwest::Url::parse(url)
        .map_err(|e| TropelError::Http(format!("Invalid request URL '{}': {}", url, e)))?;
    let Some(host) = parsed.host_str() else {
        return Ok(());
    };
    // The url crate serializes IPv6 hosts WITH brackets and normalizes
    // v4-mapped forms to hex (`http://[::ffff:10.1.2.3]` → `[::ffff:a01:203]`),
    // so strip the brackets before parsing as an IP.
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let Ok(ip) = host.parse::<std::net::IpAddr>() else {
        // Hostname → the DNS resolver applies the blacklist.
        return Ok(());
    };
    if blacklist.iter().any(|c| c.contains(ip)) {
        return Err(TropelError::Http(format!(
            "request to blacklisted IP literal '{}' (blacklistIPs)",
            host
        )));
    }
    Ok(())
}

/// HTTP response data (mirrors `tropel_core::Response` but from reqwest).
/// Body text and JSON are lazily decoded ONCE and memoized — see
/// `body_text()` / `body_json()`.
#[derive(Debug, Clone)]
pub struct HttpResponse {
    /// The URL that produced THIS response. For a redirect chain, each hop
    /// carries its own URL; the final response carries the final URL (what
    /// scripts see via pm.response / res).
    pub url: String,
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    /// Memoized UTF-8 decode of `body` (see `body_text()`).
    pub text_cache: std::cell::OnceCell<Option<String>>,
    /// Memoized JSON parse of `body` (see `body_json()`).
    pub json_cache: std::cell::OnceCell<Option<serde_json::Value>>,
    pub response_time: Duration,
    pub timings: Option<Timings>,
    pub cookies: Vec<Cookie>,
    pub size: u64,
    /// Number of bytes sent in the request body (for data_sent tracking).
    pub request_body_size: u64,
    /// Intermediate redirect hops, in order, each captured as its own
    /// request (k6 parity). Empty when the request did not redirect (or
    /// `follow_redirects` / `--no-redirects` disabled following). Callers
    /// emit one http_req_* sample set PER hop plus the final response.
    pub redirects: Vec<HttpResponse>,
}

impl From<&HttpResponse> for tropel_core::types::Response {
    fn from(resp: &HttpResponse) -> Self {
        tropel_core::types::Response {
            url: resp.url.clone(),
            status_code: resp.status_code,
            status_text: resp.status_text.clone(),
            headers: resp.headers.clone(),
            body: resp.body.clone(),
            text_cache: std::cell::OnceCell::new(),
            json_cache: std::cell::OnceCell::new(),
            response_time: resp.response_time,
            timings: resp.timings.clone(),
            cookies: resp.cookies.clone(),
            size: resp.size,
            // Recursively convert the redirect chain so DriverHttpClient / k6
            // driver / pm.response all see per-hop requests (k6 parity).
            redirects: resp.redirects.iter().map(Response::from).collect(),
        }
    }
}

impl HttpResponse {
    /// Decode the body as UTF-8 text (lazy — decodes once, then memoized).
    ///
    /// Postman parity (backlog line 171): an EMPTY body yields `Some("")`
    /// (Postman's `pm.response.text()` returns `''`, not `undefined`), and a
    /// non-UTF-8 body is decoded LOSSILY instead of becoming `null` — so
    /// `res.body.includes(...)` on a binary/odd-encoding response doesn't
    /// throw `undefined` method errors.
    pub fn body_text(&self) -> Option<String> {
        self.text_cache
            .get_or_init(|| {
                if self.body.is_empty() {
                    Some(String::new())
                } else {
                    Some(String::from_utf8_lossy(&self.body).into_owned())
                }
            })
            .clone()
    }

    /// Parse the body as JSON using simd-json (lazy — parses once, then
    /// memoized).
    ///
    /// Parses directly from raw bytes, skipping the `String::from_utf8`
    /// intermediate step. Uses `simd-json` for ~2-4x faster parsing.
    pub fn body_json(&self) -> Option<serde_json::Value> {
        self.json_cache
            .get_or_init(|| {
                if self.body.is_empty() {
                    return None;
                }
                let mut body_bytes = self.body.clone();
                simd_json::serde::from_slice(&mut body_bytes).ok()
            })
            .clone()
    }
}

/// Parse a single `Set-Cookie` header value into a structured [`Cookie`].
///
/// Handles the standard `name=value; Attr=Val; Flag` grammar — name/value are
/// the bare pair, then optional `Domain`, `Path`, `Expires`, `SameSite`
/// attributes plus the boolean `HttpOnly` / `Secure` flags. Unknown attributes
/// are ignored. Returns `None` when the header has no `name=value` pair.
fn parse_set_cookie(header: &str) -> Option<Cookie> {
    let mut parts = header.split(';');
    let pair = parts.next()?.trim();
    let (name, value) = pair.split_once('=')?;

    let mut cookie = Cookie {
        name: name.trim().to_string(),
        value: value.trim().to_string(),
        domain: None,
        path: None,
        http_only: None,
        secure: None,
        same_site: None,
        expires: None,
    };

    for attr in parts {
        let attr = attr.trim();
        if attr.is_empty() {
            continue;
        }
        // Case-insensitive attribute NAMES (Set-Cookie attrs are
        // case-insensitive per RFC 6265) — but the VALUE keeps its original
        // case (e.g. SameSite=Lax must not become "lax"). Split the raw attr
        // on '=' and lowercase only the key for matching.
        match attr.split_once('=') {
            Some((key, val)) => match key.trim().to_ascii_lowercase().as_str() {
                "domain" => cookie.domain = Some(val.trim().trim_matches('"').to_string()),
                "path" => cookie.path = Some(val.trim().trim_matches('"').to_string()),
                "expires" => cookie.expires = Some(val.trim().trim_matches('"').to_string()),
                "samesite" => {
                    cookie.same_site = Some(val.trim().trim_matches('"').to_string())
                }
                _ => {}
            },
            None => match attr.to_ascii_lowercase().as_str() {
                "httponly" => cookie.http_only = Some(true),
                "secure" => cookie.secure = Some(true),
                _ => {}
            },
        }
    }
    Some(cookie)
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
/// Serialize a request body to its exact wire bytes — the SINGLE serializer
/// for both data_sent accounting and the reqwest body. Keeping one source of
/// truth guarantees `body_size` can never diverge from what's actually sent
/// (the old UrlEncoded branch used an un-encoded `k.len()+v.len()+1`
/// approximation that under-counted vs the serde_urlencoded wire bytes).
fn body_to_bytes(body: &Body) -> Vec<u8> {
    match body {
        Body::Raw(s) => s.as_bytes().to_vec(),
        Body::Json(val) => serde_json::to_string(val).unwrap_or_default().into_bytes(),
        Body::FormData(map) => multipart_form_data_bytes(map),
        Body::UrlEncoded(map) => {
            let params: Vec<(String, String)> = map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone().to_string()))
                .collect();
            serde_urlencoded::to_string(params).unwrap_or_default().into_bytes()
        }
        Body::Binary(data) => data.clone(),
        Body::GraphQL { query, variables } => {
            // The same serializer the client sends, so data_sent accounting
            // can't diverge from the actual body.
            Body::graphql_json_string(query, variables).into_bytes()
        }
    }
}

/// Exact wire size of a request body (delegates to the single serializer).
pub fn body_size(body: &Body) -> usize {
    body_to_bytes(body).len()
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

/// Build the reqwest body from the single wire serializer (used by tests;
/// the hot path in `execute` reuses `body_to_bytes` output directly).
#[cfg(test)]
fn body_to_reqwest(body: &Body) -> reqwest::Body {
    reqwest::Body::from(body_to_bytes(body))
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

/// Delegate to the canonical `tropel_core::parse_duration`.
pub(crate) fn parse_duration(s: &str) -> Result<Duration> {
    tropel_core::parse_duration(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn canonical_header_name_matches_go_mime_form() {
        // Regression (backlog line 139): reqwest lowercases HeaderName, so
        // response headers arrived as `content-type` and k6's canonical
        // `res.headers['Content-Type']` (and Postman's
        // `pm.response.header('Content-Type')`) returned undefined.
        assert_eq!(canonical_header_name("content-type"), "Content-Type");
        assert_eq!(canonical_header_name("x-request-id"), "X-Request-Id");
        assert_eq!(canonical_header_name("etag"), "Etag");
        assert_eq!(
            canonical_header_name("www-authenticate"),
            "Www-Authenticate"
        );
        // Already-canonical and single-word names are idempotent.
        assert_eq!(canonical_header_name("Content-Type"), "Content-Type");
        assert_eq!(canonical_header_name("location"), "Location");
        assert_eq!(canonical_header_name("set-cookie"), "Set-Cookie");
    }

    #[test]
    fn http_response_body_text_lossy_and_empty() {
        // Regression (backlog line 171) — same contract as the core
        // `Response::body_text()`: empty → Some("") (Postman `''`), and
        // non-UTF-8 decodes LOSSILY so `res.body.includes(...)` never sees
        // null/undefined. Fresh caches per response (OnceCell memoization
        // must not leak across the struct-update clones).
        fn resp_with(body: Vec<u8>) -> HttpResponse {
            HttpResponse {
                url: String::new(),
                status_code: 200,
                status_text: "OK".into(),
                headers: Default::default(),
                body,
                text_cache: Default::default(),
                json_cache: Default::default(),
                response_time: Duration::ZERO,
                timings: None,
                cookies: Vec::new(),
                size: 0,
                request_body_size: 0,
                redirects: Vec::new(),
            }
        }
        assert_eq!(resp_with(Vec::new()).body_text(), Some(String::new()));
        assert_eq!(
            resp_with(vec![0xC3, 0x28, 0x41]).body_text(),
            Some("\u{FFFD}(A".to_string())
        );
        assert_eq!(resp_with(b"ok".to_vec()).body_text(), Some("ok".to_string()));
    }

    #[test]
    fn parse_set_cookie_extracts_name_value_and_attrs() {
        let c = parse_set_cookie(
            "session=abc123; Path=/; Domain=example.com; HttpOnly; Secure; SameSite=Lax; Expires=Wed, 21 Oct 2026 07:28:00 GMT; Unknown=x",
        )
        .unwrap();
        assert_eq!(c.name, "session");
        assert_eq!(c.value, "abc123");
        assert_eq!(c.path.as_deref(), Some("/"));
        assert_eq!(c.domain.as_deref(), Some("example.com"));
        assert_eq!(c.http_only, Some(true));
        assert_eq!(c.secure, Some(true));
        assert_eq!(c.same_site.as_deref(), Some("Lax"));
        assert!(c.expires.as_deref().unwrap().starts_with("Wed, 21 Oct"));
    }

    #[test]
    fn parse_set_cookie_case_insensitive_attrs_and_quoted_values() {
        let c = parse_set_cookie("id=7; PATH=\"/app\"; HTTPONLY; sAmEsItE=Strict")
            .unwrap();
        assert_eq!(c.name, "id");
        assert_eq!(c.value, "7");
        assert_eq!(c.path.as_deref(), Some("/app"));
        assert_eq!(c.http_only, Some(true));
        assert_eq!(c.same_site.as_deref(), Some("Strict"));
    }

    #[test]
    fn parse_set_cookie_minimal_and_garbage() {
        // Minimal `name=value` only.
        let c = parse_set_cookie("token=x").unwrap();
        assert_eq!(c.name, "token");
        assert_eq!(c.value, "x");
        assert!(c.path.is_none() && c.domain.is_none() && c.http_only.is_none());
        // No `name=value` pair → None. (Note: a leading segment that LOOKS
        // like an attribute, e.g. "Path=/; HttpOnly", is parsed as the
        // cookie-pair name="Path" value="/" — RFC 6265 requires the first
        // segment to be the cookie-pair, so this is only reachable on
        // malformed headers; the None paths below are the truly invalid ones.)
        assert!(parse_set_cookie("garbage-without-equals").is_none());
        assert!(parse_set_cookie("").is_none());
    }

    #[test]
    fn body_size_counts_percent_encoding_and_multipart_framing() {
        // Regression (backlog line 90): the deleted `Body::encoded_len`
        // measured UrlEncoded/FormData as a RAW `k=v&k=v` concat — no
        // percent-encoding and no multipart framing — so the k6/WASM
        // drivers' `data_sent` undercounted the real wire bytes. The single
        // serializer `body_size` must count what is actually sent.
        //
        // "a&b" percent-encodes to "a%26b" — `&` (1 byte) becomes `%26`
        // (3 bytes) — so the encoded size is LARGER than the raw concat.
        let mut map = std::collections::HashMap::new();
        map.insert("k".to_string(), "a&b".to_string());
        let url = Body::UrlEncoded(map);
        let wire = serde_urlencoded::to_string(vec![("k".to_string(), "a&b".to_string())]).unwrap();
        assert_eq!(wire, "k=a%26b");
        // 5 raw bytes vs 7 encoded — the old function reported 5.
        assert_eq!("k=a&b".len(), 5);
        assert_eq!(body_size(&url), wire.len());
        assert_eq!(wire.len(), 7);

        // Multipart framing: the wire body includes boundaries and
        // Content-Disposition headers, far larger than `k=v&k=v`.
        let mut form = std::collections::HashMap::new();
        form.insert("a".to_string(), "b".to_string());
        let framed = Body::FormData(form);
        assert!(
            body_size(&framed) > 3,
            "multipart framing must exceed the raw k=v size"
        );
    }

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
        let result = serde_urlencoded::to_string(vec![
            ("key".to_string(), "value".to_string()),
            ("name".to_string(), "hello world".to_string()),
        ])
        .unwrap();
        assert_eq!(result, "key=value&name=hello+world");
    }

    #[test]
    fn blacklist_rejects_ip_literal_but_not_hostname() {
        let blacklist = parse_blacklist(&["127.0.0.1".to_string(), "10.0.0.0/8".to_string()]);
        // Literal host inside the blacklist → rejected before any connect.
        assert!(check_literal_blacklist(&blacklist, "http://127.0.0.1:8080/api").is_err());
        assert!(check_literal_blacklist(&blacklist, "http://10.1.2.3/x").is_err());
        // v4-mapped v6 literal is canonicalized before matching.
        assert!(check_literal_blacklist(&blacklist, "http://[::ffff:10.1.2.3]/x").is_err());
        // Hostname → DNS resolver owns the blacklist; literal outside → fine.
        assert!(check_literal_blacklist(&blacklist, "http://example.com/").is_ok());
        assert!(check_literal_blacklist(&blacklist, "http://8.8.8.8/").is_ok());
        // Invalid URL surfaces as an error (not a silent pass).
        assert!(check_literal_blacklist(&blacklist, "not a url").is_err());
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
        let no_redirect_cfg = HttpConfig {
            max_redirects: 0,
            ..Default::default()
        };
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

    /// Minimal signer: sets a fixed Authorization header, like a Bearer
    /// token signer. Lets the redirect tests observe whether the signed
    /// header survives to hop 1+.
    struct StaticSigner;

    impl AuthSigner for StaticSigner {
        fn name(&self) -> &str {
            "static"
        }

        fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
            request
                .headers_mut()
                .insert(reqwest::header::AUTHORIZATION, "Bearer s3cret".parse().unwrap());
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_origin_redirect_keeps_signed_authorization() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // P0 regression: hop-0's request was signed in place on the built
        // reqwest::Request, but hop 1+ rebuilt from request.headers (never
        // signed), so the Authorization died with hop 0 and any same-origin
        // authenticated redirect 401'd. Server: /start → 302 → /final;
        // /final echoes whether it saw the Authorization header.
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
                    } else if path == "/final" {
                        let saw_auth = req
                            .lines()
                            .any(|l| l.to_ascii_lowercase().starts_with("authorization:"));
                        let body = if saw_auth { "AUTH=YES" } else { "AUTH=NO" };
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        let signer = StaticSigner;
        let req = Request {
            url: format!("http://{}/start", addr),
            method: Method::GET,
            follow_redirects: true,
            ..Default::default()
        };
        let resp = client.execute(&req, Some(&signer)).await.unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            String::from_utf8_lossy(&resp.body),
            "AUTH=YES",
            "signed Authorization must be forwarded on a same-origin redirect hop"
        );
        // The 302 hop must also be captured as its own response (k6 parity).
        assert_eq!(resp.redirects.len(), 1);
        assert_eq!(resp.redirects[0].status_code, 302);

        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn redirect_hops_do_not_reappend_original_query() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // P1 regression: the original request's query_params were re-appended
        // on EVERY redirect hop — /x?page=2 → 302 /y?token=z fetched
        // /y?token=z&page=2. The redirect target's own query must win; the
        // original query belongs to hop 0 only (k6/reqwest behavior).
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
                    let resp = if path == "/start?page=2" {
                        "HTTP/1.1 302 Found\r\nLocation: /final?token=z\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_string()
                    } else if path == "/final?token=z" {
                        "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok".to_string()
                    } else {
                        // Any OTHER path/query means the original query leaked
                        // onto the redirect hop — report it in the body so
                        // the test can assert the exact request target.
                        let body = format!("GOT:{}", path);
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    };
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();

        // Redirect case: URL carries its own query; query_params empty. The
        // final hop must be exactly /final?token=z (no re-appended page=2).
        let req = Request {
            url: format!("http://{}/start?page=2", addr),
            method: Method::GET,
            follow_redirects: true,
            ..Default::default()
        };
        let resp = client.execute(&req, None).await.unwrap();
        assert_eq!(
            resp.status_code, 200,
            "final hop should be served; got body: {}",
            String::from_utf8_lossy(&resp.body)
        );
        assert_eq!(
            String::from_utf8_lossy(&resp.body),
            "ok",
            "original query must NOT be re-appended to the redirect target; got: {}",
            String::from_utf8_lossy(&resp.body)
        );
        assert_eq!(resp.redirects.len(), 1);
        assert_eq!(resp.redirects[0].status_code, 302);

        // Preserved hop-0 behavior: a populated query_params (URL without its
        // own query) must STILL be appended exactly once on hop 0 — the gate
        // must not break the common case.
        let mut query_params = std::collections::HashMap::new();
        query_params.insert("page".to_string(), "2".to_string());
        let req2 = Request {
            url: format!("http://{}/start", addr),
            method: Method::GET,
            follow_redirects: true,
            query_params,
            ..Default::default()
        };
        let resp2 = client.execute(&req2, None).await.unwrap();
        assert_eq!(
            resp2.status_code, 200,
            "hop 0 with populated query_params should be served; got: {}",
            String::from_utf8_lossy(&resp2.body)
        );
        assert_eq!(
            String::from_utf8_lossy(&resp2.body),
            "ok",
            "populated query_params must still be appended once on hop 0; got: {}",
            String::from_utf8_lossy(&resp2.body)
        );

        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cross_origin_redirect_strips_signed_authorization() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        // P0 corollary: a redirect to a DIFFERENT origin must NOT carry the
        // hop-0 signed Authorization (credential leak). /start on server A
        // → 302 → server B /final; B reports whether it saw the header.
        let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr_a = listener_a.local_addr().unwrap();
        let addr_b = listener_b.local_addr().unwrap();

        let server_b = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener_b.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let req = String::from_utf8_lossy(&buf);
                    let saw_auth = req
                        .lines()
                        .any(|l| l.to_ascii_lowercase().starts_with("authorization:"));
                    let body = if saw_auth { "AUTH=YES" } else { "AUTH=NO" };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let server_a = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener_a.accept().await else { break };
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = sock.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 302 Found\r\nLocation: http://{}/final\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        addr_b
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });

        let cfg = HttpConfig::default();
        let client = HttpClient::new(&cfg).unwrap();
        let signer = StaticSigner;
        let req = Request {
            url: format!("http://{}/start", addr_a),
            method: Method::GET,
            follow_redirects: true,
            ..Default::default()
        };
        let resp = client.execute(&req, Some(&signer)).await.unwrap();
        assert_eq!(resp.status_code, 200);
        assert_eq!(
            String::from_utf8_lossy(&resp.body),
            "AUTH=NO",
            "signed Authorization must NOT leak to a different origin"
        );

        server_a.abort();
        server_b.abort();
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

