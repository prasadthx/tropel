//! Auth signers — sign/modify an HTTP request before sending.
//!
//! Supports: Bearer, Basic, ApiKey, OAuth2, AWS SigV4, OAuth1 (RFC 5849
//! HMAC-SHA1), Hawk, and HTTP Digest (RFC 7616, challenge-response).
//!
//! Signers operate on a fully built [`reqwest::Request`] (not a builder) so
//! schemes like SigV4 / OAuth1 / Hawk can read the method, URL and body.
//! Digest is a two-phase scheme: the first request goes out unauthenticated,
//! and on a 401 the client calls [`AuthSigner::challenge_response`] to build
//! the Authorization header from the server's `WWW-Authenticate` challenge
//! and retries once.

use base64::Engine;
use hmac::digest::KeyInit;
use hmac::{Hmac, Mac};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use rand::RngExt;
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};
use tropel_core::types::{ApiKeyLocation, AuthConfig};
use tropel_core::Result;
use tropel_core::TropelError;

/// RFC 3986 unreserved characters: `A-Z a-z 0-9 - . _ ~` stay unencoded.
const UNRESERVED: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

type HmacSha256 = Hmac<Sha256>;
type HmacSha1 = Hmac<Sha1>;

/// Auth signer trait — signs/modifies a request before sending.
///
/// `sign()` mutates the built request in place (headers, URL query, ...).
/// For challenge-response schemes (Digest), `sign()` is a no-op; the engine
/// surfaces the 401 `WWW-Authenticate` challenge via [`Self::challenge_response`].
pub trait AuthSigner: Send + Sync {
    fn name(&self) -> &str;
    fn sign(&self, request: &mut reqwest::Request) -> Result<()>;

    /// For challenge-response schemes (Digest): given the server's
    /// `WWW-Authenticate` header value from a 401, produce the value for the
    /// `Authorization` header to retry with. The default returns `None`
    /// (no challenge handling). `request` is a freshly built copy of the
    /// original request (method + URI), so the signer can recompute per-URI
    /// components (e.g. the digest `uri`).
    fn challenge_response(
        &self,
        _www_authenticate: &str,
        _request: &reqwest::Request,
    ) -> Option<String> {
        None
    }
}

// ─────────────────────────── Simple schemes ───────────────────────────

/// Bearer token authentication.
pub struct BearerAuth {
    token: String,
}

impl BearerAuth {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
        }
    }
}

impl AuthSigner for BearerAuth {
    fn name(&self) -> &str {
        "bearer"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        set_auth_header(request, &format!("Bearer {}", self.token))
    }
}

/// Basic authentication (`Authorization: Basic base64(user:pass)`).
pub struct BasicAuth {
    username: String,
    password: String,
}

impl BasicAuth {
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl AuthSigner for BasicAuth {
    fn name(&self) -> &str {
        "basic"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        let credentials = format!("{}:{}", self.username, self.password);
        let encoded = base64::engine::general_purpose::STANDARD.encode(credentials);
        set_auth_header(request, &format!("Basic {}", encoded))
    }
}

/// API Key authentication (header or query).
pub struct ApiKeyAuth {
    key: String,
    value: String,
    location: ApiKeyLocation,
}

impl ApiKeyAuth {
    pub fn new(key: &str, value: &str, location: ApiKeyLocation) -> Self {
        Self {
            key: key.to_string(),
            value: value.to_string(),
            location,
        }
    }
}

impl AuthSigner for ApiKeyAuth {
    fn name(&self) -> &str {
        "apikey"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        match self.location {
            ApiKeyLocation::Header => {
                let key =
                    reqwest::header::HeaderName::from_bytes(self.key.as_bytes()).map_err(|_| {
                        TropelError::Http("API key name is not a valid header name".into())
                    })?;
                let value = self.value.parse().map_err(|_| {
                    TropelError::Http("API key value is not a valid header value".into())
                })?;
                request.headers_mut().insert(key, value);
            }
            ApiKeyLocation::Query => {
                request
                    .url_mut()
                    .query_pairs_mut()
                    .append_pair(&self.key, &self.value);
            }
        }
        Ok(())
    }
}

// ─────────────────────────── OAuth2 ───────────────────────────

/// OAuth2 bearer token (`Authorization: <token_type or Bearer> <access_token>`).
pub struct OAuth2Auth {
    access_token: String,
    token_type: Option<String>,
}

impl OAuth2Auth {
    pub fn new(access_token: &str, token_type: Option<String>) -> Self {
        Self {
            access_token: access_token.to_string(),
            token_type,
        }
    }
}

impl AuthSigner for OAuth2Auth {
    fn name(&self) -> &str {
        "oauth2"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        let scheme = self.token_type.as_deref().unwrap_or("Bearer");
        set_auth_header(request, &format!("{} {}", scheme, self.access_token))
    }
}

// ─────────────────────────── AWS SigV4 ───────────────────────────

/// AWS Signature Version 4 request signing.
///
/// Builds the canonical request, hashes the payload with SHA-256, derives
/// the signing key from the secret, and emits the `Authorization`,
/// `X-Amz-Date`, `X-Amz-Content-Sha256` and (when present)
/// `X-Amz-Security-Token` headers.
pub struct AwsSigV4Auth {
    access_key: String,
    secret_key: String,
    region: Option<String>,
    service: Option<String>,
    session_token: Option<String>,
}

impl AwsSigV4Auth {
    pub fn new(
        access_key: &str,
        secret_key: &str,
        region: Option<String>,
        service: Option<String>,
        session_token: Option<String>,
    ) -> Self {
        Self {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
            region,
            service,
            session_token,
        }
    }
}

impl AuthSigner for AwsSigV4Auth {
    fn name(&self) -> &str {
        "aws-sigv4"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        let url = request.url();
        let method = request.method().as_str();
        let now = chrono::Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let region = self
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string());
        let service = self
            .service
            .clone()
            .unwrap_or_else(|| default_service(url.host_str().unwrap_or("")));

        // Payload hash — body is always buffered by tropel, but fall back to
        // the empty-string hash for streaming bodies (never panic).
        let payload_hash = match request.body().and_then(|b| b.as_bytes()) {
            Some(bytes) => hex_sha256(bytes),
            None => EMPTY_SHA256.to_string(),
        };

        // Canonical URI. AWS requires DOUBLE URI-encoding of the path for
        // every service EXCEPT the S3 family (s3, s3control, s3-object-lambda,
        // …), which sign the single-encoded path exactly as sent. The url
        // crate already single-encodes `path()` with the RFC 3986 unreserved
        // rules, so re-encoding each segment yields the required second
        // encoding for non-S3 services.
        let path = url.path();
        let canonical_uri = sigv4_canonical_uri(path, &service);

        // Canonical query string: sorted by encoded key.
        let mut pairs: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (enc(k.as_ref()), enc(v.as_ref())))
            .collect();
        pairs.sort();
        let canonical_query = pairs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        // Canonical headers: host + x-amz-* AND every header already present
        // on the request (lowercased, deduped), including ALL values of
        // multi-value headers comma-joined (AWS canonicalization).
        let mut headers = sigv4_canonical_headers(
            request,
            &payload_hash,
            &amz_date,
            self.session_token.as_deref(),
        );
        headers.sort();
        let signed_headers = headers
            .iter()
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>()
            .join(";");
        let canonical_headers = headers
            .iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>();

        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
        );
        let scope = format!("{date_stamp}/{region}/{service}/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex_sha256(canonical_request.as_bytes())
        );

        let signing_key = derive_signing_key(&self.secret_key, &date_stamp, &region, &service);
        let signature = hex_hmac_sha256(&signing_key, string_to_sign.as_bytes());

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={signed_headers}, Signature={signature}",
            self.access_key, scope
        );

        insert_header(request, "x-amz-date", &amz_date)?;
        insert_header(request, "x-amz-content-sha256", &payload_hash)?;
        if let Some(token) = &self.session_token {
            insert_header(request, "x-amz-security-token", token)?;
        }
        set_auth_header(request, &authorization)?;
        Ok(())
    }
}

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

fn default_service(host: &str) -> String {
    // "s3.us-west-2.amazonaws.com" → "s3"; fall back to the whole host.
    host.split('.').next().unwrap_or(host).to_string()
}

/// `host[:port]` — omit the port when it is the scheme default. IPv6 hosts
/// are wrapped in brackets (`[::1]:443`) since `Url::host_str` returns the
/// bare address.
/// AWS "Trimall": trim leading/trailing whitespace AND collapse each run of
/// internal whitespace to a single space. SigV4 canonicalization requires
/// this — `.trim()` alone leaves `"a  b"` intact, which changes the
/// canonical hash and yields 403 SignatureDoesNotMatch.
fn trim_all(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn canonical_host(url: &reqwest::Url) -> String {
    let host = bracket_host(url.host_str().unwrap_or(""));
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host,
    }
}

/// Wrap IPv6 literal hosts in brackets; pass everything else through.
fn bracket_host(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn derive_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hex_sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac =
        <HmacSha256 as KeyInit>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hex_hmac_sha256(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

/// RFC 3986 percent-encode (unreserved chars pass through).
fn enc(s: &str) -> String {
    utf8_percent_encode(s, &UNRESERVED).to_string()
}

/// Canonical URI for SigV4.
///
/// AWS requires DOUBLE URI-encoding of the absolute path for every service
/// EXCEPT the S3 family (s3, s3control, s3-object-lambda, s3-outposts,
/// s3express), which sign the single-encoded path exactly as sent. The `url`
/// crate already single-encodes `Url::path()` with the RFC 3986 unreserved
/// rules, so re-encoding each segment yields the required second encoding
/// for non-S3 services (`%` is not unreserved → `%25`). Empty segments
/// (leading/trailing slash, `//`) are preserved; an empty path becomes `/`.
fn sigv4_canonical_uri(path: &str, service: &str) -> String {
    if is_s3_family(service) {
        return if path.is_empty() {
            "/".to_string()
        } else {
            path.to_string()
        };
    }
    if path.is_empty() {
        return "/".to_string();
    }
    let encoded: Vec<String> = path.split('/').map(enc).collect();
    encoded.join("/")
}

/// True for the S3 family of services, which sign the single-encoded path.
///
/// Matches both spellings of S3 Control's signing name ("s3-control" is what
/// `default_service` derives from `s3-control.<region>.amazonaws.com`; some
/// tools emit the un-hyphenated "s3control").
fn is_s3_family(service: &str) -> bool {
    matches!(
        service,
        "s3" | "s3control" | "s3-control" | "s3-object-lambda" | "s3-outposts" | "s3express"
    )
}

/// Canonical headers for SigV4.
///
/// Collects the `host` header plus every header already present on the
/// request (lowercased, deduped), joining the VALUES of multi-value headers
/// with a bare `","` (NO space, per AWS canonicalization) after applying
/// Trimall (ends trimmed, internal whitespace runs collapsed to one space),
/// then adds the `x-amz-*` signing headers. The `Authorization` header is
/// never signed (it is the output of this signer). Uses `get_all` so
/// duplicate header values are preserved — the old `headers().iter()`
/// collapsed multi-value headers to the first.
fn sigv4_canonical_headers(
    request: &reqwest::Request,
    payload_hash: &str,
    amz_date: &str,
    session_token: Option<&str>,
) -> Vec<(String, String)> {
    let mut out: HashMap<String, String> = HashMap::new();

    out.insert("host".to_string(), canonical_host(request.url()));

    for name in request.headers().keys() {
        let key = name.as_str().to_ascii_lowercase();
        if key == "authorization" {
            continue;
        }
        // Non-UTF8 header values cannot appear in a canonical request (the
        // sigstring is ASCII); such values are skipped rather than panicking.
        // HeaderMap preserves insertion order, so the comma-join is stable.
        let values: Vec<String> = request
            .headers()
            .get_all(name)
            .iter()
            .filter_map(|v| v.to_str().ok())
            .map(trim_all)
            .collect();
        if values.is_empty() {
            continue;
        }
        // AWS joins multi-value headers with a bare comma — NO space — and
        // requires Trimall (sequential spaces collapsed to one). The old
        // `", "` join produced a different canonical hash → 403
        // SignatureDoesNotMatch on any duplicated header.
        out.insert(key, values.join(","));
    }

    out.insert("x-amz-content-sha256".to_string(), payload_hash.to_string());
    out.insert("x-amz-date".to_string(), amz_date.to_string());
    if let Some(token) = session_token {
        out.insert("x-amz-security-token".to_string(), token.to_string());
    }
    out.into_iter().collect()
}

// ─────────────────────────── OAuth1 (RFC 5849) ───────────────────────────

/// OAuth1 HMAC-SHA1 request signing (RFC 5849).
///
/// Builds the signature base string from method, base URL, query + form body
/// params and the OAuth params, signs with the consumer/token secrets, and
/// emits the `Authorization: OAuth ...` header.
pub struct OAuth1Auth {
    consumer_key: String,
    consumer_secret: String,
    token: Option<String>,
    token_secret: Option<String>,
}

impl OAuth1Auth {
    pub fn new(
        consumer_key: &str,
        consumer_secret: &str,
        token: Option<String>,
        token_secret: Option<String>,
    ) -> Self {
        Self {
            consumer_key: consumer_key.to_string(),
            consumer_secret: consumer_secret.to_string(),
            token,
            token_secret,
        }
    }
}

impl AuthSigner for OAuth1Auth {
    fn name(&self) -> &str {
        "oauth1"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        let url = request.url();
        let method = request.method().as_str();
        let nonce = generate_nonce();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            .to_string();

        // Collect protocol params: query + form body (if urlencoded) + oauth.
        let mut params: Vec<(String, String)> = url
            .query_pairs()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        if is_form_urlencoded(request) {
            if let Some(bytes) = request.body().and_then(|b| b.as_bytes()) {
                params.extend(parse_form(bytes));
            }
        }
        let mut oauth: Vec<(String, String)> = vec![
            ("oauth_consumer_key".to_string(), self.consumer_key.clone()),
            ("oauth_nonce".to_string(), nonce.clone()),
            (
                "oauth_signature_method".to_string(),
                "HMAC-SHA1".to_string(),
            ),
            ("oauth_timestamp".to_string(), timestamp.clone()),
            ("oauth_version".to_string(), "1.0".to_string()),
        ];
        if let Some(token) = &self.token {
            oauth.push(("oauth_token".to_string(), token.clone()));
        }
        params.extend(oauth.clone());

        // RFC 5849 §3.4.1.1: signature base string = HTTPMethod & "&" &
        // baseURI & "&" & normalizedParams — the separators are literal `&`
        // characters (ASCII 38), NOT newlines. Verified against the oauth.net
        // canonical test vector (see tests).
        let base_uri = base_url(url);
        let base_string = oauth1_base_string(method, &base_uri, &params);
        let signature = oauth1_hmac_sha1(
            &base_string,
            &self.consumer_secret,
            self.token_secret.as_deref().unwrap_or(""),
        );

        // Header params are quoted, nonce/signature included.
        let mut header_params = oauth;
        header_params.push(("oauth_signature".to_string(), signature));
        header_params.sort();
        let header_value = header_params
            .iter()
            .map(|(k, v)| format!("{}=\"{}\"", enc(k), enc(v)))
            .collect::<Vec<_>>()
            .join(", ");
        set_auth_header(request, &format!("OAuth {header_value}"))
    }
}

/// RFC 5849 §3.4.1.1 signature base string.
///
/// `HTTPMethod & "&" & baseURI & "&" & normalizedParams` where the
/// separators are literal `&` characters (ASCII 38), NOT newlines. Each
/// component is percent-encoded per RFC 3986 (unreserved chars pass
/// through); the params are sorted by encoded name (then value).
fn oauth1_base_string(method: &str, base_uri: &str, params: &[(String, String)]) -> String {
    let mut encoded: Vec<(String, String)> = params.iter().map(|(k, v)| (enc(k), enc(v))).collect();
    encoded.sort();
    let param_string = encoded
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!(
        "{}&{}&{}",
        method.to_uppercase(),
        enc(base_uri),
        enc(&param_string)
    )
}

/// RFC 5849 §3.4.2 HMAC-SHA1 signature over the base string.
///
/// Signing key = `enc(consumer_secret) & enc(token_secret)`. HMAC accepts
/// any key length, so `new_from_slice` cannot fail here.
fn oauth1_hmac_sha1(base_string: &str, consumer_secret: &str, token_secret: &str) -> String {
    let key = format!("{}&{}", enc(consumer_secret), enc(token_secret));
    let mut mac = <HmacSha1 as KeyInit>::new_from_slice(key.as_bytes())
        .expect("HMAC-SHA1 accepts any key length");
    mac.update(base_string.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn is_form_urlencoded(request: &reqwest::Request) -> bool {
    request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.to_ascii_lowercase()
                .starts_with("application/x-www-form-urlencoded")
        })
        .unwrap_or(false)
}

fn parse_form(bytes: &[u8]) -> Vec<(String, String)> {
    let Ok(s) = std::str::from_utf8(bytes) else {
        return vec![];
    };
    s.split('&')
        .filter(|p| !p.is_empty())
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            Some((
                percent_decode(k).unwrap_or_else(|| k.to_string()),
                percent_decode(v).unwrap_or_else(|| v.to_string()),
            ))
        })
        .collect()
}

fn percent_decode(s: &str) -> Option<String> {
    use percent_encoding::percent_decode_str;
    percent_decode_str(s)
        .decode_utf8()
        .ok()
        .map(|c| c.to_string())
}

fn base_url(url: &reqwest::Url) -> String {
    let host = bracket_host(url.host_str().unwrap_or(""));
    let port = match url.port() {
        Some(port) => format!(":{port}"),
        None => String::new(),
    };
    format!("{}://{host}{port}{}", url.scheme(), url.path())
}

// ─────────────────────────── Hawk ───────────────────────────

/// Hawk header authentication (`Authorization: Hawk id=..., ts=..., nonce=...,
/// mac=...`), MAC computed with HMAC-SHA256 (or SHA-1 when configured).
pub struct HawkAuth {
    auth_id: String,
    auth_key: String,
    algorithm: Option<String>,
}

impl HawkAuth {
    pub fn new(auth_id: &str, auth_key: &str, algorithm: Option<String>) -> Self {
        Self {
            auth_id: auth_id.to_string(),
            auth_key: auth_key.to_string(),
            algorithm,
        }
    }
}

impl AuthSigner for HawkAuth {
    fn name(&self) -> &str {
        "hawk"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        let url = request.url();
        let method = request.method().as_str().to_uppercase();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            .to_string();
        let nonce = generate_nonce();

        // Resource = path + query.
        let resource = match url.query() {
            Some(q) => format!("{}?{}", url.path(), q),
            None => url.path().to_string(),
        };
        let host = bracket_host(url.host_str().unwrap_or(""));
        let port = url
            .port()
            .unwrap_or_else(|| if url.scheme() == "https" { 443 } else { 80 });

        // Normalized string per the Hawk spec (mozilla/hawk lib/crypto.js
        // generateNormalizedString): ts and nonce come FIRST, immediately
        // after the scheme line; method/resource/host/port follow; then hash
        // and ext (empty here — this shim doesn't do payload validation). The
        // previous ordering (method…port before ts/nonce) produced a MAC that
        // mismatches a Hawk server (verified against the Hawk API.md
        // reference vectors in the tests below).
        let normalized =
            hawk_normalized_string(&method, &ts, &nonce, &resource, &host, port, "", "");
        let mac = hawk_mac(&normalized, &self.auth_key, self.algorithm.as_deref());

        let header = format!(
            "Hawk id=\"{}\", ts=\"{}\", nonce=\"{}\", mac=\"{}\"",
            self.auth_id, ts, nonce, mac
        );
        set_auth_header(request, &header)
    }
}

/// Hawk normalized request string ("hawk.1.header" scheme).
///
/// Each value is followed by a newline, in the order: scheme, ts, nonce,
/// method (uppercased), resource, host (lowercased), port, hash, ext.
/// ts/nonce come FIRST per the Hawk spec — the earlier implementation put
/// method/resource/host/port first, producing a MAC any Hawk server rejects.
/// The normalized request string has exactly 8 spec-ordered fields (scheme,
/// ts, nonce, method, resource, host, port, hash, ext) — grouping them would
/// obscure the wire order the spec mandates, so allow the arity.
#[allow(clippy::too_many_arguments)]
fn hawk_normalized_string(
    method: &str,
    ts: &str,
    nonce: &str,
    resource: &str,
    host: &str,
    port: u16,
    hash: &str,
    ext: &str,
) -> String {
    let method = method.to_uppercase();
    let host = host.to_lowercase();
    format!("hawk.1.header\n{ts}\n{nonce}\n{method}\n{resource}\n{host}\n{port}\n{hash}\n{ext}\n")
}

/// Hawk request MAC = base64(HMAC(algorithm, key, normalized)).
fn hawk_mac(normalized: &str, key: &str, algorithm: Option<&str>) -> String {
    let sha1 = algorithm
        .map(|a| a.eq_ignore_ascii_case("sha1"))
        .unwrap_or(false);
    if sha1 {
        let mut mac = <HmacSha1 as KeyInit>::new_from_slice(key.as_bytes())
            .expect("HMAC-SHA1 accepts any key length");
        mac.update(normalized.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    } else {
        let mut mac = <HmacSha256 as KeyInit>::new_from_slice(key.as_bytes())
            .expect("HMAC-SHA256 accepts any key length");
        mac.update(normalized.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
    }
}

// ─────────────────────────── Digest (RFC 7616) ───────────────────────────

/// HTTP Digest authentication (RFC 7616) — challenge-response.
///
/// The first request is sent unauthenticated; on a 401 the engine calls
/// [`AuthSigner::challenge_response`] which parses `WWW-Authenticate`,
/// computes the digest response (MD5 or SHA-256, with or without qop) and
/// returns the `Authorization: Digest ...` header value for the retry.
///
/// The session (nonce + `nc` counter) is cached per host, so [`AuthSigner::sign`]
/// can pre-attach the Authorization header on subsequent requests to the same
/// host — no 401 round-trip per request (backlog line 176).
pub struct DigestAuth {
    username: String,
    password: String,
    /// Cached digest sessions keyed by `host[:port]` (backlog line 176).
    sessions: Mutex<HashMap<String, DigestSession>>,
}

/// One server challenge we're actively authenticating against: the realm /
/// nonce / qop / algorithm / opaque from the 401's `WWW-Authenticate`, plus
/// the per-nonce request counter (`nc`, RFC 7616 §3.4.1 — counts requests
/// sent with a given nonce, reset when the server rotates the nonce).
struct DigestSession {
    realm: String,
    nonce: String,
    nc: u64,
    qop: Option<String>,
    algorithm: Option<String>,
    opaque: Option<String>,
}

impl DigestAuth {
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
            sessions: Mutex::new(HashMap::new()),
        }
    }
}

impl AuthSigner for DigestAuth {
    fn name(&self) -> &str {
        "digest"
    }

    fn sign(&self, request: &mut reqwest::Request) -> Result<()> {
        // Backlog line 176: if a digest session is already cached for this
        // host (established by an earlier 401 → challenge_response), attach
        // the Authorization header NOW with the next `nc` — the request
        // skips the 401 round-trip entirely. The first request to a new host
        // has no session and still goes out unauthenticated.
        let key = digest_session_key(request.url());
        let header = {
            let mut sessions = self.sessions.lock().unwrap();
            match sessions.get_mut(&key) {
                Some(sess) => {
                    sess.nc += 1;
                    Some(build_digest_authorization(
                        &self.username,
                        &self.password,
                        sess,
                        request,
                    ))
                }
                None => None,
            }
        };
        if let Some(h) = header {
            set_auth_header(request, &h)?;
        }
        Ok(())
    }

    fn challenge_response(
        &self,
        www_authenticate: &str,
        request: &reqwest::Request,
    ) -> Option<String> {
        // RFC 7235 §4.1 allows several challenges in one header value
        // (`WWW-Authenticate: Basic realm=\"x\", Digest realm=\"y\", nonce=\"z\"`)
        // and the header may repeat across lines — the old parser only ever
        // looked at the first scheme, so Digest after Basic was skipped
        // (backlog line 176).
        let challenge = find_digest_challenge(www_authenticate)?;
        let realm = challenge.get("realm")?;
        let nonce = challenge.get("nonce")?;
        let key = digest_session_key(request.url());
        let mut sessions = self.sessions.lock().unwrap();
        let sess = sessions.entry(key).or_insert_with(|| DigestSession {
            realm: realm.clone(),
            nonce: nonce.clone(),
            nc: 0,
            qop: challenge.get("qop").cloned(),
            algorithm: challenge.get("algorithm").cloned(),
            opaque: challenge.get("opaque").cloned(),
        });
        // Server rotated the nonce (or first challenge for this host) → reset
        // the per-nonce counter; otherwise keep counting within this nonce.
        if sess.nonce != *nonce {
            sess.nonce = nonce.clone();
            sess.nc = 0;
            sess.realm = realm.clone();
            sess.qop = challenge.get("qop").cloned();
            sess.algorithm = challenge.get("algorithm").cloned();
            sess.opaque = challenge.get("opaque").cloned();
        }
        sess.nc += 1;
        Some(build_digest_authorization(
            &self.username,
            &self.password,
            sess,
            request,
        ))
    }
}

/// Compute the RFC 7616 §3.4.1 `response` digest for a session with a KNOWN
/// cnonce. Extracted from `build_digest_authorization` (which supplies a
/// fresh random cnonce) so the published RFC reference vectors can be pinned
/// EXACTLY — the RFC examples fix the cnonce, and a random one can never be
/// compared against the published answer (backlog line 210).
///
/// `qop = Some(...)` containing `auth` uses the qop form
/// `H(H(A1):nonce:nc:cnonce:auth:H(A2))`; any other combination falls back
/// to the no-qop form `H(H(A1):nonce:H(A2))` (with `-sess` folding the
/// nonce+cnonce into HA1 first).
#[allow(clippy::too_many_arguments)] // all 8 params are RFC-specified digest inputs
fn digest_response_value(
    base_ha1: &str,
    is_sess: bool,
    nonce: &str,
    nc: &str,
    cnonce: &str,
    qop: Option<&str>,
    ha2: &str,
    base_algorithm: &str,
) -> String {
    if let Some(qop) = qop {
        if qop.split(',').any(|q| q.trim() == "auth") {
            let ha1 = sess_fold_ha1(base_ha1, is_sess, nonce, cnonce, base_algorithm);
            return digest_with(
                &format!("{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}"),
                base_algorithm,
            );
        }
    }
    // No qop (or qop without `auth`) — the no-qop form; nc/cnonce unused.
    if is_sess {
        let ha1 = sess_fold_ha1(base_ha1, true, nonce, cnonce, base_algorithm);
        digest_with(&format!("{ha1}:{nonce}:{ha2}"), base_algorithm)
    } else {
        digest_with(&format!("{base_ha1}:{nonce}:{ha2}"), base_algorithm)
    }
}

/// RFC 7616 §3.4.4: the `-sess` HA1 folds nonce + cnonce into the base HA1
/// (`H(base_ha1:nonce:cnonce)`); non-sess algorithms use the base HA1 as-is.
fn sess_fold_ha1(
    base_ha1: &str,
    is_sess: bool,
    nonce: &str,
    cnonce: &str,
    base_algorithm: &str,
) -> String {
    if is_sess {
        digest_with(&format!("{base_ha1}:{nonce}:{cnonce}"), base_algorithm)
    } else {
        base_ha1.to_string()
    }
}

/// Cache key for a digest session: `host[:port]` from the request URL.
fn digest_session_key(url: &reqwest::Url) -> String {
    let host = url.host_str().unwrap_or("");
    match url.port() {
        Some(p) => format!("{host}:{p}"),
        None => host.to_string(),
    }
}

/// Build the full `Authorization: Digest ...` header value for a session and
/// a request (RFC 7616). Shared by [`AuthSigner::sign`] (pre-attached from a
/// cached session) and [`AuthSigner::challenge_response`] (fresh challenge).
fn build_digest_authorization(
    username: &str,
    password: &str,
    sess: &DigestSession,
    request: &reqwest::Request,
) -> String {
    let realm = &sess.realm;
    let nonce = &sess.nonce;
    // RFC 7616 defaults to MD5 when the server omits `algorithm`; only
    // emit the field when the challenge actually specified it (some
    // strict servers reject an explicit `algorithm=MD5`).
    let server_algorithm = sess.algorithm.as_ref().map(|s| s.to_ascii_uppercase());
    let algorithm = server_algorithm
        .clone()
        .unwrap_or_else(|| "MD5".to_string());
    // RFC 7616 §3.4.4: the "-sess" variants (MD5-sess / SHA-256-sess)
    // only change how HA1 is derived (nonce + cnonce are folded in); the
    // hash function itself is always the base algorithm (MD5 or SHA-256).
    let base_algorithm = if algorithm.starts_with("SHA-256") {
        "SHA-256"
    } else {
        "MD5"
    };
    let is_sess = algorithm.ends_with("-SESS");
    let qop = sess.qop.as_deref();

    let method = request.method().as_str();
    // The digest `uri` is the request-target: path + query (RFC 7616).
    let url = request.url();
    let uri = match url.query() {
        Some(q) => format!("{}?{}", url.path(), q),
        None => url.path().to_string(),
    };

    // Base HA1 = H(username:realm:password)
    let ha1_input = format!("{username}:{realm}:{password}");
    let base_ha1 = digest_with(&ha1_input, base_algorithm);
    // HA2 = H(method:uri)
    let ha2_input = format!("{method}:{uri}");
    let ha2 = digest_with(&ha2_input, base_algorithm);

    let mut fields: Vec<(String, String)> = vec![
        ("username".into(), username.to_string()),
        ("realm".into(), realm.clone()),
        ("nonce".into(), nonce.clone()),
        ("uri".into(), uri),
    ];

    // RFC 7616 §3.4.1: `nc` is an 8-digit hex counter of requests sent with
    // this nonce (00000001, 00000002, …) — NOT hardcoded (backlog line 176).
    let nc = format!("{:08x}", sess.nc);
    // -sess requires a cnonce even when qop is absent (RFC 7616 §3.4.4).
    let response = if let Some(qop) = qop {
        if qop.split(',').any(|q| q.trim() == "auth") {
            let c = generate_crypto_nonce();
            let response = digest_response_value(
                &base_ha1,
                is_sess,
                nonce,
                &nc,
                &c,
                Some(qop),
                &ha2,
                base_algorithm,
            );
            fields.push(("qop".into(), "auth".into()));
            fields.push(("nc".into(), nc));
            fields.push(("cnonce".into(), c));
            response
        } else {
            // qop present but no 'auth' — fall back to the no-qop form.
            if is_sess {
                let c = generate_crypto_nonce();
                fields.push(("cnonce".into(), c.clone()));
                digest_response_value(&base_ha1, true, nonce, &nc, &c, None, &ha2, base_algorithm)
            } else {
                digest_response_value(&base_ha1, false, nonce, &nc, "", None, &ha2, base_algorithm)
            }
        }
    } else if is_sess {
        let c = generate_crypto_nonce();
        fields.push(("cnonce".into(), c.clone()));
        digest_response_value(&base_ha1, true, nonce, &nc, &c, None, &ha2, base_algorithm)
    } else {
        digest_response_value(&base_ha1, false, nonce, &nc, "", None, &ha2, base_algorithm)
    };
    fields.push(("response".into(), response));
    if let Some(alg) = server_algorithm {
        fields.push(("algorithm".into(), alg));
    }
    if let Some(opaque) = &sess.opaque {
        fields.push(("opaque".into(), opaque.clone()));
    }

    // RFC 7616 §3.4.1: `qop`, `nc` and `algorithm` are `token` productions
    // and MUST NOT be quoted (strict servers reject `qop=\"auth\"` — the
    // old code quoted every field, so `nc`/`algorithm`/`qop` were wrong).
    // All other directives (username, realm, nonce, uri, response,
    // cnonce, opaque) are `quoted-string` and MUST be quoted.
    let header = fields
        .iter()
        .map(|(k, v)| match k.as_str() {
            "qop" | "nc" | "algorithm" => format!("{k}={v}"),
            _ => format!("{k}=\"{}\"", v.replace('"', "")),
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("Digest {header}")
}

/// Digest a string with the algorithm chosen by the server's challenge.
fn digest_with(input: &str, algorithm: &str) -> String {
    match algorithm {
        "SHA-256" => hex_sha256(input.as_bytes()),
        _ => hex_md5(input.as_bytes()),
    }
}

fn hex_md5(bytes: &[u8]) -> String {
    hex::encode(md5::Md5::digest(bytes))
}

/// Parse a `WWW-Authenticate` header value into a list of `(scheme, params)`
/// challenges (RFC 7235 §4.1). Handles MULTIPLE schemes in one header value
/// — `Basic realm=\"x\", Digest realm=\"y\", nonce=\"z\"` is two challenges, and
/// the old parser only ever looked at the first scheme, so a Digest challenge
/// listed after a Basic one was silently skipped (backlog line 176).
fn parse_challenges(header: &str) -> Vec<(String, HashMap<String, String>)> {
    let mut challenges: Vec<(String, HashMap<String, String>)> = Vec::new();
    let mut current: Option<(String, HashMap<String, String>)> = None;

    for part in split_challenge_parts(header) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        // A new challenge starts with a bare scheme token: a token NOT
        // followed by '=' (e.g. `Digest realm=\"y\"` — "Digest" then space).
        // A `key=value` part (no whitespace before the '=') belongs to the
        // current challenge.
        let first_space = part.find(char::is_whitespace).unwrap_or(usize::MAX);
        let first_eq = part.find('=').unwrap_or(usize::MAX);
        // New scheme iff the part's first token is NOT a `key=value`: either
        // there is no '=' at all (bare/token68 challenge) or the '=' comes
        // after the first whitespace (scheme then params).
        if first_eq == usize::MAX || first_eq > first_space {
            // Flush the previous challenge, start a new scheme.
            if let Some(c) = current.take() {
                challenges.push(c);
            }
            let (scheme, rest) = match part.find(char::is_whitespace) {
                Some(i) => (&part[..i], &part[i..]),
                None => (part, ""),
            };
            let mut params = HashMap::new();
            if let Some((k, v)) = parse_challenge_part(rest) {
                params.insert(k, v);
            }
            current = Some((scheme.to_string(), params));
        } else if let Some((_, params)) = current.as_mut() {
            if let Some((k, v)) = parse_challenge_part(part) {
                params.insert(k, v);
            }
        }
    }
    if let Some(c) = current {
        challenges.push(c);
    }
    challenges
}

/// Find the Digest challenge among possibly several schemes in a
/// `WWW-Authenticate` value (backlog line 176).
fn find_digest_challenge(header: &str) -> Option<HashMap<String, String>> {
    parse_challenges(header)
        .into_iter()
        .find(|(scheme, _)| scheme.eq_ignore_ascii_case("digest"))
        .map(|(_, params)| params)
}

/// Split a `WWW-Authenticate` value on commas, but NOT commas inside a
/// quoted-string — RFC 2617/7616 challenges frequently quote a list
/// (`qop=\"auth, auth-int\"`). The old naive `split(',')` split inside the
/// quotes, so a challenge advertising `auth` second would be mis-parsed as
/// only the first qop value.
fn split_challenge_parts(header: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut part_start = 0usize;
    let bytes = header.as_bytes();
    let mut in_quotes = false;
    for (i, b) in bytes.iter().enumerate() {
        match b {
            b'"' => in_quotes = !in_quotes,
            b',' if !in_quotes => {
                parts.push(&header[part_start..i]);
                part_start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&header[part_start..]);
    parts
}

/// Parse one `key=value` segment of a challenge (may be quoted or bare).
fn parse_challenge_part(part: &str) -> Option<(String, String)> {
    let part = part.trim();
    if part.is_empty() {
        return None;
    }
    let (k, v) = part.split_once('=')?;
    let v = v.trim().trim_matches('"').to_string();
    Some((k.trim().to_ascii_lowercase(), v))
}

// ─────────────────────────── Shared helpers ───────────────────────────

fn set_auth_header(request: &mut reqwest::Request, value: &str) -> Result<()> {
    insert_header(request, "authorization", value)
}

fn insert_header(request: &mut reqwest::Request, name: &str, value: &str) -> Result<()> {
    let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| TropelError::Http(format!("'{name}' is not a valid HTTP header name")))?;
    let header_value = value.parse().map_err(|_| {
        TropelError::Http(format!("'{name}' value is not a valid HTTP header value"))
    })?;
    request.headers_mut().insert(header_name, header_value);
    Ok(())
}

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Deterministic-but-unique per-process nonce: time-derived seed XORed with a
/// monotonic counter, hex-encoded. Suitable for signing nonces where
/// uniqueness is all that is required (OAuth1, Hawk) — not cryptographic
/// secrecy. See [`generate_crypto_nonce`] for the Digest `cnonce`, which MUST
/// be unpredictable — do not unify the two.
fn generate_nonce() -> String {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!(
        "{:016x}",
        seed ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15)
    )
}

/// Cryptographically secure random nonce (16 bytes → 32 hex chars) for the
/// HTTP Digest `cnonce`.
///
/// The Digest cnonce is folded into the auth response (and, for `-sess`
/// algorithms, into HA1), so it must be unpredictable to an attacker who can
/// observe traffic — a time-seeded counter would let them predict/replay
/// client nonces. `rand::rng()` is a CSPRNG (ChaCha12, OS-seeded); 128 bits
/// of entropy is the conventional crypto-nonce strength.
fn generate_crypto_nonce() -> String {
    let mut rng = rand::rng();
    let bytes: [u8; 16] = rng.random();
    hex::encode(bytes)
}

/// Build an auth signer from an `AuthConfig`.
///
/// This is the single signer-builder used by both the executor runner
/// (`runner.rs`) and the HTTP client (`HttpClient::get_signer`) — the two
/// previously duplicated builders were consolidated into this one function.
pub fn build_auth_signer(auth: &AuthConfig) -> Option<Box<dyn AuthSigner>> {
    match auth {
        // Explicit noauth: no signer — and crucially the RUNNER must not
        // fall back to scenario auth. The runner's `.or(scenario.auth)`
        // only falls through on `None`, so `Some(NoAuth)` reaching here
        // yields no signer while still blocking inheritance (Postman
        // semantics: noauth does NOT inherit collection/folder auth).
        AuthConfig::NoAuth => None,
        AuthConfig::Bearer { token } => Some(Box::new(BearerAuth::new(token))),
        AuthConfig::Basic { username, password } => {
            Some(Box::new(BasicAuth::new(username, password)))
        }
        AuthConfig::ApiKey {
            key,
            value,
            location,
        } => Some(Box::new(ApiKeyAuth::new(key, value, location.clone()))),
        AuthConfig::OAuth2 {
            access_token,
            token_type,
        } => Some(Box::new(OAuth2Auth::new(access_token, token_type.clone()))),
        AuthConfig::AwsSigV4 {
            access_key,
            secret_key,
            region,
            service,
            session_token,
        } => Some(Box::new(AwsSigV4Auth::new(
            access_key,
            secret_key,
            region.clone(),
            service.clone(),
            session_token.clone(),
        ))),
        AuthConfig::OAuth1 {
            consumer_key,
            consumer_secret,
            token,
            token_secret,
        } => Some(Box::new(OAuth1Auth::new(
            consumer_key,
            consumer_secret,
            token.clone(),
            token_secret.clone(),
        ))),
        AuthConfig::Hawk {
            auth_id,
            auth_key,
            algorithm,
        } => Some(Box::new(HawkAuth::new(
            auth_id,
            auth_key,
            algorithm.clone(),
        ))),
        AuthConfig::Digest { username, password } => {
            Some(Box::new(DigestAuth::new(username, password)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_request(method: &str, url: &str, body: Option<&str>) -> reqwest::Request {
        let mut req = reqwest::Request::new(
            reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
            url.parse().unwrap(),
        );
        if let Some(b) = body {
            let headers = req.headers_mut();
            headers.insert(
                "content-type",
                "application/x-www-form-urlencoded".parse().unwrap(),
            );
            let _ = headers;
            req.body_mut().replace(reqwest::Body::from(b.to_string()));
        }
        req
    }

    fn auth_header(req: &reqwest::Request) -> String {
        req.headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn bearer_sets_header() {
        let mut req = build_request("GET", "http://example.com/", None);
        BearerAuth::new("tok123").sign(&mut req).unwrap();
        assert_eq!(auth_header(&req), "Bearer tok123");
    }

    #[test]
    fn basic_base64s_credentials() {
        let mut req = build_request("GET", "http://example.com/", None);
        BasicAuth::new("user", "pass").sign(&mut req).unwrap();
        // base64("user:pass") = dXNlcjpwYXNz
        assert_eq!(auth_header(&req), "Basic dXNlcjpwYXNz");
    }

    #[test]
    fn apikey_header_and_query() {
        let mut req = build_request("GET", "http://example.com/", None);
        ApiKeyAuth::new("X-Key", "v", ApiKeyLocation::Header)
            .sign(&mut req)
            .unwrap();
        assert_eq!(req.headers().get("X-Key").unwrap(), "v");

        let mut req = build_request("GET", "http://example.com/", None);
        ApiKeyAuth::new("api_key", "sekret", ApiKeyLocation::Query)
            .sign(&mut req)
            .unwrap();
        assert_eq!(req.url().query(), Some("api_key=sekret"));
    }

    #[test]
    fn oauth2_defaults_to_bearer() {
        let mut req = build_request("GET", "http://example.com/", None);
        OAuth2Auth::new("acc", None).sign(&mut req).unwrap();
        assert_eq!(auth_header(&req), "Bearer acc");

        let mut req = build_request("GET", "http://example.com/", None);
        OAuth2Auth::new("acc", Some("MAC".to_string()))
            .sign(&mut req)
            .unwrap();
        assert_eq!(auth_header(&req), "MAC acc");
    }

    #[test]
    fn sigv4_sets_required_headers() {
        let mut req = build_request(
            "GET",
            "https://examplebucket.s3.amazonaws.com/test.txt",
            None,
        );
        AwsSigV4Auth::new(
            "AKID",
            "SECRET",
            Some("us-east-1".into()),
            Some("s3".into()),
            None,
        )
        .sign(&mut req)
        .unwrap();
        let h = auth_header(&req);
        assert!(h.starts_with("AWS4-HMAC-SHA256 Credential=AKID/"));
        assert!(h.contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert!(h.contains("Signature="));
        assert!(req.headers().contains_key("x-amz-date"));
        assert_eq!(
            req.headers().get("x-amz-content-sha256").unwrap(),
            EMPTY_SHA256
        );
    }

    #[test]
    fn sigv4_non_s3_path_is_double_encoded_s3_single() {
        // Non-S3 services double URI-encode the path (AWS SigV4 spec); the
        // url crate already single-encodes `path()`, so the canonical URI
        // must re-encode each segment (e.g. `%20` → `%2520`).
        let canonical = sigv4_canonical_uri("/my%20file%2Fname", "execute-api");
        assert_eq!(canonical, "/my%2520file%252Fname");
        // The S3 family signs the single-encoded path exactly as sent.
        assert_eq!(
            sigv4_canonical_uri("/my%20file%2Fname", "s3"),
            "/my%20file%2Fname"
        );
        assert_eq!(
            sigv4_canonical_uri("/my%20file%2Fname", "s3-object-lambda"),
            "/my%20file%2Fname"
        );
        // S3 Control's signing name is hyphenated ("s3-control") — must not
        // double-encode either.
        assert_eq!(
            sigv4_canonical_uri("/my%20file%2Fname", "s3-control"),
            "/my%20file%2Fname"
        );
        // Empty path → "/".
        assert_eq!(sigv4_canonical_uri("", "execute-api"), "/");
        // Empty segments (leading/trailing slash) are preserved.
        assert_eq!(sigv4_canonical_uri("/a//b/", "execute-api"), "/a//b/");
    }

    #[test]
    fn sigv4_multi_value_headers_joined_in_canonical_headers() {
        let mut req = build_request("GET", "https://example.com/thing", None);
        req.headers_mut().append("x-test", "one".parse().unwrap());
        req.headers_mut().append("x-test", "two".parse().unwrap());
        req.headers_mut().append("x-test", "three".parse().unwrap());
        let headers = sigv4_canonical_headers(&req, "HASH", "20260729T000000Z", None);
        // Multi-value header values are comma-joined with NO space per AWS
        // canonicalization; host + x-amz-* are also present.
        let map: std::collections::HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(map.get("x-test").map(|s| s.as_str()), Some("one,two,three"));
        assert_eq!(map.get("host").map(|s| s.as_str()), Some("example.com"));
        assert_eq!(
            map.get("x-amz-content-sha256").map(|s| s.as_str()),
            Some("HASH")
        );
        assert_eq!(
            map.get("x-amz-date").map(|s| s.as_str()),
            Some("20260729T000000Z")
        );
    }

    #[test]
    fn sigv4_trimall_collapses_internal_whitespace() {
        // AWS requires Trimall: trim ends AND collapse internal whitespace
        // runs to a single space. `.trim()` alone leaves "a  b" intact,
        // changing the canonical hash → 403 SignatureDoesNotMatch.
        let mut req = build_request("GET", "https://example.com/thing", None);
        req.headers_mut()
            .insert("x-test", "  one   two \t three  ".parse().unwrap());
        let headers = sigv4_canonical_headers(&req, "HASH", "20260729T000000Z", None);
        let map: std::collections::HashMap<String, String> = headers.into_iter().collect();
        assert_eq!(
            map.get("x-test").map(|s| s.as_str()),
            Some("one two three"),
            "internal whitespace runs must collapse to a single space"
        );
    }

    #[test]
    fn sigv4_session_token_adds_header_and_signed_headers() {
        let mut req = build_request("GET", "https://example.com/", None);
        AwsSigV4Auth::new(
            "AKID",
            "SECRET",
            Some("us-west-2".into()),
            Some("execute-api".into()),
            Some("tok".into()),
        )
        .sign(&mut req)
        .unwrap();
        assert_eq!(req.headers().get("x-amz-security-token").unwrap(), "tok");
        let h = auth_header(&req);
        assert!(h.contains("x-amz-security-token"));
        // deterministic signature (same input → same output)
        let mut req2 = build_request("GET", "https://example.com/", None);
        AwsSigV4Auth::new(
            "AKID",
            "SECRET",
            Some("us-west-2".into()),
            Some("execute-api".into()),
            Some("tok".into()),
        )
        .sign(&mut req2)
        .unwrap();
        // x-amz-date may differ across seconds; signature differs only if date
        // rolled over — instead compare shape.
        assert_eq!(
            req.headers().get("x-amz-content-sha256"),
            req2.headers().get("x-amz-content-sha256")
        );
    }

    /// Backlog line 210: SigV4 must reproduce the PUBLISHED AWS reference
    /// vector, not just `contains("Signature=")`. This is the canonical
    /// "Signature Calculations for the Authorization Header: Transferring a
    /// Payload in a Single Chunk" (ListUsers) example from the AWS docs:
    /// secret `wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY`, region us-east-1,
    /// service iam, date 20150830T123600Z. The canonical-request hash and
    /// the final signature below were re-derived with openssl and match the
    /// values published in the AWS documentation byte-for-byte.
    #[test]
    fn sigv4_matches_aws_docs_reference_vector() {
        // The canonical request exactly as the AWS docs example publishes it
        // (GET /?Action=ListUsers&Version=2010-05-08, host iam.amazonaws.com,
        // content-type, x-amz-date; empty payload hash).
        let canonical_request = concat!(
            "GET\n",
            "/\n",
            "Action=ListUsers&Version=2010-05-08\n",
            "content-type:application/x-www-form-urlencoded; charset=utf-8\n",
            "host:iam.amazonaws.com\n",
            "x-amz-date:20150830T123600Z\n",
            "\n",
            "content-type;host;x-amz-date\n",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        );
        assert_eq!(
            hex_sha256(canonical_request.as_bytes()),
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
            "canonical request must hash to the AWS-published value"
        );

        // String to sign (date + scope + canonical-request hash) → signature.
        let string_to_sign = concat!(
            "AWS4-HMAC-SHA256\n",
            "20150830T123600Z\n",
            "20150830/us-east-1/iam/aws4_request\n",
            "f536975d06c0309214f805bb90ccff089219ecd68b2577efef23edd43b7e1a59",
        );
        let key = derive_signing_key(
            "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY",
            "20150830",
            "us-east-1",
            "iam",
        );
        assert_eq!(
            hex_hmac_sha256(&key, string_to_sign.as_bytes()),
            "5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7",
            "signature must match the AWS-published value (kDate→kRegion→kService→kSigning chain)"
        );
    }

    #[test]
    fn oauth1_produces_authorization_header() {
        let mut req = build_request(
            "GET",
            "http://example.com/request?b5=%3D%253D&a3=a&c%40=&a2=r%20b",
            None,
        );
        OAuth1Auth::new(
            "dpf43f3p2l4k3l03",
            "kd94hf93k423kf44",
            Some("nnch734d00sl2jdk".into()),
            Some("pfkkdhi9sl3r4s00".into()),
        )
        .sign(&mut req)
        .unwrap();
        let h = auth_header(&req);
        assert!(h.starts_with("OAuth "));
        assert!(h.contains("oauth_consumer_key=\"dpf43f3p2l4k3l03\""));
        assert!(h.contains("oauth_signature_method=\"HMAC-SHA1\""));
        assert!(h.contains("oauth_signature=\""));
        // The header values are percent-encoded per RFC 5849 §3.5.2, so the
        // signature is percent-encoded base64; decode it before base64 check.
        let sig_enc = h
            .split("oauth_signature=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        let sig = percent_decode(sig_enc).expect("signature percent-decodes");
        assert!(base64::engine::general_purpose::STANDARD
            .decode(&sig)
            .is_ok());
        // Signature is deterministic for the same nonce/timestamp — re-sign
        // with a fixed nonce path isn't practical here, so just confirm the
        // signature is non-trivial and round-trips.
        assert!(!sig.is_empty());
    }

    #[test]
    fn hawk_produces_header() {
        let mut req = build_request("GET", "http://example.com:8000/resource/1?b=1&a=2", None);
        HawkAuth::new(
            "dh37fgj492je",
            "werxhqb98rpaxn39848xrunpaw3489ruxnpa98w4rxn",
            None,
        )
        .sign(&mut req)
        .unwrap();
        let h = auth_header(&req);
        assert!(h.starts_with("Hawk id=\"dh37fgj492je\""));
        assert!(h.contains("ts=\""));
        assert!(h.contains("nonce=\""));
        assert!(h.contains("mac=\""));
    }

    #[test]
    fn oauth1_base_string_uses_amp_separators() {
        // RFC 5849 §3.4.1.1: the three components are joined with literal `&`
        // characters (ASCII 38), never `\n` — the old implementation used
        // newlines, producing a signature any OAuth1 server rejects.
        let base = oauth1_base_string(
            "POST",
            "http://example.com/request",
            &[("a".to_string(), "1".to_string())],
        );
        assert!(base.starts_with("POST&http%3A%2F%2Fexample.com%2Frequest&a%3D1"));
        assert!(!base.contains('\n'));
    }

    #[test]
    fn oauth1_matches_oauth_net_reference_vector() {
        // Canonical oauth.net/core/1.0a example — reproduced verbatim in the
        // test suites of every OAuth 1.0a implementation. Verified against
        // openssl HMAC-SHA1.
        let params: Vec<(String, String)> = vec![
            ("file".into(), "vacation.jpg".into()),
            ("oauth_consumer_key".into(), "dpf43f3p2l4k3l03".into()),
            ("oauth_nonce".into(), "kllo9940pd9333jh".into()),
            ("oauth_signature_method".into(), "HMAC-SHA1".into()),
            ("oauth_timestamp".into(), "1191242096".into()),
            ("oauth_token".into(), "nnch734d00sl2jdk".into()),
            ("oauth_version".into(), "1.0".into()),
            ("size".into(), "original".into()),
        ];
        let base = oauth1_base_string("GET", "http://photos.example.net/photos", &params);
        assert_eq!(
            base,
            "GET&http%3A%2F%2Fphotos.example.net%2Fphotos&file%3Dvacation.jpg%26oauth_consumer_key%3Ddpf43f3p2l4k3l03%26oauth_nonce%3Dkllo9940pd9333jh%26oauth_signature_method%3DHMAC-SHA1%26oauth_timestamp%3D1191242096%26oauth_token%3Dnnch734d00sl2jdk%26oauth_version%3D1.0%26size%3Doriginal"
        );
        let sig = oauth1_hmac_sha1(&base, "kd94hf93k423kf44", "pfkkdhi9sl3r4s00");
        assert_eq!(sig, "tR3+Ty81lMeYAr/Fid0kMTYa/WM=");
    }

    #[test]
    fn hawk_normalized_orders_ts_nonce_first() {
        // Hawk spec: ts and nonce come FIRST, immediately after the scheme
        // line — the old implementation put method/resource/host/port first.
        let normalized = hawk_normalized_string(
            "GET",
            "1353832234",
            "j4h3g2",
            "/resource/1?b=1&a=2",
            "example.com",
            8000,
            "",
            "",
        );
        let lines: Vec<&str> = normalized.lines().collect();
        assert_eq!(lines[0], "hawk.1.header");
        assert_eq!(lines[1], "1353832234");
        assert_eq!(lines[2], "j4h3g2");
        assert_eq!(lines[3], "GET");
        assert_eq!(lines[4], "/resource/1?b=1&a=2");
        assert_eq!(lines[5], "example.com");
        assert_eq!(lines[6], "8000");
    }

    #[test]
    fn hawk_matches_api_reference_vector() {
        // Hawk API.md "Protocol Example": GET /resource/1?b=1&a=2 on
        // example.com:8000 with ext="some-app-ext-data". Published MAC:
        // 6R4rV5iE+NPoym+WwjeHzjAGXUtLNIxmo1vpMofpLAE=
        let normalized = hawk_normalized_string(
            "GET",
            "1353832234",
            "j4h3g2",
            "/resource/1?b=1&a=2",
            "example.com",
            8000,
            "",
            "some-app-ext-data",
        );
        assert_eq!(
            normalized,
            "hawk.1.header\n1353832234\nj4h3g2\nGET\n/resource/1?b=1&a=2\nexample.com\n8000\n\nsome-app-ext-data\n"
        );
        let mac = hawk_mac(
            &normalized,
            "werxhqb98rpaxn39848xrunpaw3489ruxnpa98w4rxn",
            None,
        );
        assert_eq!(mac, "6R4rV5iE+NPoym+WwjeHzjAGXUtLNIxmo1vpMofpLAE=");
    }

    #[test]
    fn hawk_matches_api_payload_hash_vector() {
        // Hawk API.md "Payload Validation": POST /resource/1?b=1&a=2 with a
        // payload hash and ext. Published MAC: aSe1DERmZuRl3pI36/9BdZmnErTw3sNzOOAUlfeKjVw=
        let normalized = hawk_normalized_string(
            "POST",
            "1353832234",
            "j4h3g2",
            "/resource/1?b=1&a=2",
            "example.com",
            8000,
            "Yi9LfIIFRtBEPt74PVmbTF/xVAwPn7ub15ePICfgnuY=",
            "some-app-ext-data",
        );
        let mac = hawk_mac(
            &normalized,
            "werxhqb98rpaxn39848xrunpaw3489ruxnpa98w4rxn",
            None,
        );
        assert_eq!(mac, "aSe1DERmZuRl3pI36/9BdZmnErTw3sNzOOAUlfeKjVw=");
    }

    #[test]
    fn digest_challenge_response_parses_and_computes() {
        let www = r#"Digest realm="testrealm@host.com", qop="auth, auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", opaque="5ccc069c403ebaf9f0171e9517f40e41""#;
        let req = build_request("GET", "http://www.example.com/dir/index.html", None);
        let auth = DigestAuth::new("Mufasa", "Circle Of Life");
        let header = auth
            .challenge_response(www, &req)
            .expect("challenge response");
        assert!(header.starts_with("Digest "));
        assert!(header.contains("username=\"Mufasa\""));
        assert!(header.contains("realm=\"testrealm@host.com\""));
        assert!(header.contains("nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\""));
        assert!(header.contains("uri=\"/dir/index.html\""));
        // qop/nc are bare tokens per RFC 7616 §3.4.1 — NOT quoted.
        assert!(header.contains("qop=auth"));
        assert!(!header.contains("qop=\"auth\""));
        assert!(header.contains("nc=00000001"));
        assert!(!header.contains("nc=\"00000001\""));
        assert!(header.contains("response=\""));
        assert!(header.contains("opaque=\"5ccc069c403ebaf9f0171e9517f40e41\""));
        // Deterministic response for MD5 no-cnonce variant — verify 32 hex chars.
        let response = header
            .split("response=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        assert_eq!(response.len(), 32);
        assert!(response.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Backlog line 210: the RFC 2617 §3.5 reference vector (reproduced in
    /// RFC 7616 §3.9.1). Username Mufasa / realm testrealm@host.com /
    /// password "Circle Of Life", nonce dcd98b7102dd2f0e8b11d0f600bfb0c093,
    /// uri /dir/index.html, qop=auth, nc=00000001, cnonce 0a4f113b. The
    /// published response `6629fae4…` was re-derived with openssl and the
    /// test now compares the ANSWER instead of just its length.
    #[test]
    fn digest_matches_rfc2617_md5_reference_vector() {
        let base_ha1 = digest_with("Mufasa:testrealm@host.com:Circle Of Life", "MD5");
        assert_eq!(base_ha1, "939e7578ed9e3c518a452acee763bce9");
        let ha2 = digest_with("GET:/dir/index.html", "MD5");
        assert_eq!(ha2, "39aff3a2bab6126f332b942af96d3366");
        let response = digest_response_value(
            &base_ha1,
            false,
            "dcd98b7102dd2f0e8b11d0f600bfb0c093",
            "00000001",
            "0a4f113b",
            Some("auth"),
            &ha2,
            "MD5",
        );
        assert_eq!(
            response, "6629fae49393a05397450978507c4ef1",
            "RFC 2617 §3.5 published response"
        );
    }

    /// Backlog line 210: RFC 7616 §3.9.1 reference vectors (MD5 + SHA-256).
    /// Realm http-auth@example.org, password "Circle of Life", nonce
    /// 7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v, uri /dir/index.html,
    /// nc=00000001, cnonce f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ.
    /// Both published responses were re-derived with openssl.
    #[test]
    fn digest_matches_rfc7616_reference_vectors() {
        // NOTE: RFC 7616 spells the password "Circle of Life" (lowercase o)
        // while RFC 2617 uses "Circle Of Life" — the two RFCs genuinely
        // differ; keep each test faithful to its own source.
        let base_ha1 = digest_with("Mufasa:http-auth@example.org:Circle of Life", "MD5");
        let ha2 = digest_with("GET:/dir/index.html", "MD5");
        assert_eq!(
            digest_response_value(
                &base_ha1,
                false,
                "7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v",
                "00000001",
                "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ",
                Some("auth"),
                &ha2,
                "MD5",
            ),
            "8ca523f5e9506fed4657c9700eebdbec",
            "RFC 7616 §3.9.1 published MD5 response"
        );

        let base_ha1 = digest_with("Mufasa:http-auth@example.org:Circle of Life", "SHA-256");
        let ha2 = digest_with("GET:/dir/index.html", "SHA-256");
        assert_eq!(
            digest_response_value(
                &base_ha1,
                false,
                "7ypf/xlj9XXwfDPEoM4URrv/xwf94BcCAzFZH4GiTo0v",
                "00000001",
                "f2/wE4q74E6zIJEtWaHKaf5wv/H5QzzpXusqGemxURZJ",
                Some("auth"),
                &ha2,
                "SHA-256",
            ),
            "753927fa0e85d155564e2e272a28d1802ca10daf4496794697cf8db5856cb6c1",
            "RFC 7616 §3.9.1 published SHA-256 response"
        );
    }

    #[test]
    fn digest_no_qop_form() {
        let www = r#"Digest realm="x", nonce="abc123", algorithm=MD5"#;
        let req = build_request("GET", "http://example.com/", None);
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        // algorithm is a bare token per RFC 7616 §3.4.1.
        assert!(header.contains("algorithm=MD5"));
        assert!(!header.contains("algorithm=\"MD5\""));
        assert!(!header.contains("qop="));
    }

    #[test]
    fn digest_quoted_multi_qop_selects_auth() {
        // A challenge quoting a qop LIST must still select the `auth` form
        // (the old comma-split broke on commas inside the quotes, so a
        // `qop="auth-int, auth"` challenge was mis-parsed as only
        // `auth-int` and fell back to the no-qop response).
        let www = r#"Digest realm="x", qop="auth-int, auth", nonce="abc123""#;
        let req = build_request("GET", "http://example.com/", None);
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        assert!(header.contains("qop=auth"));
        assert!(header.contains("nc=00000001"));
        assert!(header.contains("cnonce=\""));
        // The response for the qop form is 32 hex chars (MD5).
        let response = header
            .split("response=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        assert_eq!(response.len(), 32);
    }

    #[test]
    fn digest_sess_algorithm_folds_nonce_and_cnonce() {
        // RFC 7616 §3.4.4: MD5-sess / SHA-256-sess fold nonce + cnonce into
        // HA1 and require a cnonce even without qop; the hash function stays
        // the base algorithm. The response must be 32 hex chars (MD5 base)
        // and the algorithm echoed unquoted as a bare token.
        let www = r#"Digest realm="x", nonce="abc123", algorithm=MD5-sess"#;
        let req = build_request("GET", "http://example.com/", None);
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        assert!(header.contains("algorithm=MD5-SESS"));
        assert!(header.contains("cnonce=\""));
        let response = header
            .split("response=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        assert_eq!(response.len(), 32);
        assert!(response.chars().all(|c| c.is_ascii_hexdigit()));

        // SHA-256-sess with qop: base hash is SHA-256 → 64 hex chars.
        let www = r#"Digest realm="x", qop="auth", nonce="abc123", algorithm=SHA-256-sess"#;
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        assert!(header.contains("algorithm=SHA-256-SESS"));
        assert!(header.contains("cnonce=\""));
        assert!(header.contains("qop=auth"));
        let response = header
            .split("response=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        assert_eq!(response.len(), 64);
        assert!(response.chars().all(|c| c.is_ascii_hexdigit()));

        // Non-sess SHA-256 stays 64 hex chars and needs no cnonce without qop.
        let www = r#"Digest realm="x", nonce="abc123", algorithm=SHA-256"#;
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        assert!(!header.contains("cnonce="));
        let response = header
            .split("response=\"")
            .nth(1)
            .and_then(|s| s.split('"').next())
            .unwrap();
        assert_eq!(response.len(), 64);
    }

    #[test]
    fn digest_non_digest_challenge_returns_none() {
        let req = build_request("GET", "http://example.com/", None);
        assert!(DigestAuth::new("u", "p")
            .challenge_response("Basic realm=\"x\"", &req)
            .is_none());
    }

    #[test]
    fn build_auth_signer_covers_all_variants() {
        use tropel_core::types::AuthConfig;
        let cases = vec![
            (AuthConfig::Bearer { token: "t".into() }, "bearer"),
            (
                AuthConfig::Basic {
                    username: "u".into(),
                    password: "p".into(),
                },
                "basic",
            ),
            (
                AuthConfig::ApiKey {
                    key: "k".into(),
                    value: "v".into(),
                    location: ApiKeyLocation::Header,
                },
                "apikey",
            ),
            (
                AuthConfig::OAuth2 {
                    access_token: "a".into(),
                    token_type: None,
                },
                "oauth2",
            ),
            (
                AuthConfig::AwsSigV4 {
                    access_key: "a".into(),
                    secret_key: "s".into(),
                    region: None,
                    service: None,
                    session_token: None,
                },
                "aws-sigv4",
            ),
            (
                AuthConfig::OAuth1 {
                    consumer_key: "c".into(),
                    consumer_secret: "s".into(),
                    token: None,
                    token_secret: None,
                },
                "oauth1",
            ),
            (
                AuthConfig::Hawk {
                    auth_id: "i".into(),
                    auth_key: "k".into(),
                    algorithm: None,
                },
                "hawk",
            ),
            (
                AuthConfig::Digest {
                    username: "u".into(),
                    password: "p".into(),
                },
                "digest",
            ),
        ];
        for (cfg, expected) in cases {
            let signer = build_auth_signer(&cfg).expect("signer");
            assert_eq!(signer.name(), expected);
        }
    }

    #[test]
    fn nonce_is_unique_and_hex() {
        let a = generate_nonce();
        let b = generate_nonce();
        assert_ne!(a, b);
        assert_eq!(a.len(), 16);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn crypto_nonce_is_unique_hex_and_full_width() {
        // Digest cnonce comes from the CSPRNG, not the time-seeded counter —
        // it must be unpredictable AND 32 hex chars (16 bytes = 128 bits).
        let a = generate_crypto_nonce();
        let b = generate_crypto_nonce();
        assert_ne!(a, b, "crypto nonces must be unique");
        assert_eq!(a.len(), 32, "crypto nonce must be 32 hex chars");
        assert_eq!(b.len(), 32);
        assert!(a.chars().all(|c| c.is_ascii_hexdigit()));
        assert!(b.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn digest_cnonce_varies_between_requests() {
        // Two challenge responses to the SAME challenge must carry different
        // cnonces (a replayed cnonce + nc would be a replay vector).
        let www = r#"Digest realm="x", qop="auth", nonce="abc123""#;
        let req = build_request("GET", "http://example.com/", None);
        let auth = DigestAuth::new("u", "p");
        let h1 = auth.challenge_response(www, &req).unwrap();
        let h2 = auth.challenge_response(www, &req).unwrap();
        let cnonce = |h: &str| -> String {
            h.split("cnonce=\"")
                .nth(1)
                .and_then(|s| s.split('"').next())
                .unwrap()
                .to_string()
        };
        assert_ne!(
            cnonce(&h1),
            cnonce(&h2),
            "Digest cnonce must vary per request"
        );
    }

    #[test]
    fn parse_challenge_handles_quotes_and_bare_values() {
        let challenges = parse_challenges(r#"Digest realm="r", qop="auth", algorithm=MD5"#);
        assert_eq!(challenges.len(), 1);
        let (scheme, m) = &challenges[0];
        assert_eq!(scheme, "Digest");
        assert_eq!(m.get("realm").map(|s| s.as_str()), Some("r"));
        assert_eq!(m.get("qop").map(|s| s.as_str()), Some("auth"));
        assert_eq!(m.get("algorithm").map(|s| s.as_str()), Some("MD5"));
    }

    #[test]
    fn parse_challenges_finds_digest_after_basic_in_one_header() {
        // Regression (backlog line 176): `WWW-Authenticate: Basic …, Digest …`
        // in ONE header line — the old parser only looked at the first
        // scheme, so Digest after Basic was silently skipped.
        let www =
            r#"Basic realm="basic-realm", Digest realm="digest-realm", qop="auth", nonce="n1""#;
        let m = find_digest_challenge(www).expect("digest challenge must be found");
        assert_eq!(m.get("realm").map(|s| s.as_str()), Some("digest-realm"));
        assert_eq!(m.get("nonce").map(|s| s.as_str()), Some("n1"));
        assert_eq!(m.get("qop").map(|s| s.as_str()), Some("auth"));
        // The Basic challenge's realm must NOT leak into the digest params.
        assert_ne!(m.get("realm").map(|s| s.as_str()), Some("basic-realm"));
    }

    #[test]
    fn parse_challenges_handles_multi_line_joined_header() {
        // A server may send several WWW-Authenticate header lines (one per
        // scheme); the client joins them with ", " before parsing. The joined
        // value must still resolve the Digest challenge correctly.
        let joined = r#"Basic realm="b", Digest realm="d", nonce="n2", algorithm=SHA-256"#;
        let challenges = parse_challenges(joined);
        assert_eq!(
            challenges.len(),
            2,
            "two schemes must be split: {challenges:?}"
        );
        assert_eq!(challenges[0].0, "Basic");
        assert_eq!(challenges[1].0, "Digest");
        assert_eq!(challenges[1].1.get("realm").map(|s| s.as_str()), Some("d"));
    }

    #[test]
    fn digest_challenge_response_works_when_digest_second_scheme() {
        // End-to-end: challenge_response must build a Digest header even when
        // the Digest challenge is the SECOND scheme in the header.
        let www = r#"Basic realm="b", Digest realm="testrealm@host.com", qop="auth", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093""#;
        let req = build_request("GET", "http://www.example.com/dir/index.html", None);
        let header = DigestAuth::new("Mufasa", "Circle Of Life")
            .challenge_response(www, &req)
            .expect("digest challenge after basic must still respond");
        assert!(header.starts_with("Digest "));
        assert!(header.contains("realm=\"testrealm@host.com\""));
        assert!(header.contains("qop=auth"));
    }

    #[test]
    fn digest_nc_increments_and_sign_preattaches() {
        // Regression (backlog line 176): nc was hardcoded 00000001 with no
        // nonce caching, so EVERY request paid a 401 round-trip. Now the
        // session is cached per host: challenge_response uses nc=00000001,
        // and a later sign() on the same host pre-attaches the digest header
        // with nc=00000002 — no 401 needed.
        let www = r#"Digest realm="r", qop="auth", nonce="n1""#;
        let auth = DigestAuth::new("u", "p");
        let req1 = build_request("GET", "http://example.com/", None);
        let h1 = auth
            .challenge_response(www, &req1)
            .expect("first challenge");
        assert!(h1.contains("nc=00000001"), "first use must be nc=1: {h1}");

        // A fresh request to the SAME host: sign() must attach the cached
        // digest with the incremented nc.
        let mut req2 = build_request("GET", "http://example.com/", None);
        auth.sign(&mut req2).expect("sign must succeed");
        let auth_header = req2
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .expect("sign must pre-attach the digest header")
            .to_string();
        assert!(auth_header.starts_with("Digest "));
        assert!(
            auth_header.contains("nc=00000002"),
            "second use must be nc=2: {auth_header}"
        );

        // A DIFFERENT host has no session: sign() must not attach anything.
        let mut req3 = build_request("GET", "http://other.example.com/", None);
        auth.sign(&mut req3).expect("sign must succeed");
        assert!(
            req3.headers().get("authorization").is_none(),
            "no session for other host — no pre-attached header"
        );
    }

    #[test]
    fn digest_nonce_rotation_resets_nc() {
        // When the server rotates the nonce, the per-nonce nc counter must
        // reset to 00000001 (RFC 7616 §3.4.1: nc counts requests for THIS
        // nonce).
        let auth = DigestAuth::new("u", "p");
        let req = build_request("GET", "http://example.com/", None);
        let h1 = auth
            .challenge_response(r#"Digest realm="r", qop="auth", nonce="nonce-a""#, &req)
            .unwrap();
        assert!(h1.contains("nc=00000001"));
        let h2 = auth
            .challenge_response(r#"Digest realm="r", qop="auth", nonce="nonce-a""#, &req)
            .unwrap();
        assert!(
            h2.contains("nc=00000002"),
            "same nonce continues counting: {h2}"
        );
        let h3 = auth
            .challenge_response(r#"Digest realm="r", qop="auth", nonce="nonce-b""#, &req)
            .unwrap();
        assert!(h3.contains("nonce=\"nonce-b\""));
        assert!(h3.contains("nc=00000001"), "rotated nonce resets nc: {h3}");
    }
}
