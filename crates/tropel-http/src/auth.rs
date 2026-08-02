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
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
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
                let key = reqwest::header::HeaderName::from_bytes(self.key.as_bytes()).map_err(
                    |_| TropelError::Http("API key name is not a valid header name".into()),
                )?;
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

        // Canonical URI: use the path exactly as the url crate stores it — it
        // is already RFC 3986 percent-encoded with the same unreserved rules
        // AWS requires. Re-encoding it would double-encode `%` → `%25` and
        // break signatures for any path containing special characters.
        let path = url.path();
        let canonical_uri = if path.is_empty() {
            "/".to_string()
        } else {
            path.to_string()
        };

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
        // on the request (lowercased, deduped). AWS requires all request-
        // affecting headers to be covered by the signature — hardcoding just
        // the x-amz-* set would leave user headers (Content-Type, custom
        // x-*) unsigned, which strict services reject. Matches k6/Postman,
        // which sign every header present.
        let host = canonical_host(url);
        let mut headers: Vec<(String, String)> = vec![
            ("host".to_string(), host),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = &self.session_token {
            headers.push(("x-amz-security-token".to_string(), token.clone()));
        }
        for (name, value) in request.headers() {
            let key = name.as_str().to_ascii_lowercase();
            if headers.iter().any(|(k, _)| k == &key) {
                continue;
            }
            // AWS canonicalization: trim, collapse sequential spaces.
            let v = value
                .to_str()
                .unwrap_or("")
                .split_whitespace()
                .collect::<Vec<_>>()
                .join(" ");
            headers.push((key, v));
        }
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
    let mut mac = <HmacSha256 as KeyInit>::new_from_slice(key)
        .expect("HMAC-SHA256 accepts any key length");
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
            ("oauth_signature_method".to_string(), "HMAC-SHA1".to_string()),
            ("oauth_timestamp".to_string(), timestamp.clone()),
            ("oauth_version".to_string(), "1.0".to_string()),
        ];
        if let Some(token) = &self.token {
            oauth.push(("oauth_token".to_string(), token.clone()));
        }
        params.extend(oauth.clone());

        // Sort, percent-encode, join.
        let mut encoded: Vec<(String, String)> = params
            .into_iter()
            .map(|(k, v)| (enc(&k), enc(&v)))
            .collect();
        encoded.sort();
        let param_string = encoded
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");

        // Base string URI: scheme://host[:port]/path (no query).
        let base_uri = base_url(url);
        let base_string = format!(
            "{}\n{}\n{}",
            method.to_uppercase(),
            enc(&base_uri),
            enc(&param_string)
        );

        // Signing key: encoded consumer secret & encoded token secret.
        let key = format!(
            "{}&{}",
            enc(&self.consumer_secret),
            enc(self.token_secret.as_deref().unwrap_or(""))
        );
        let signature = {
            let mut mac = <HmacSha1 as KeyInit>::new_from_slice(key.as_bytes())
                .map_err(|_| TropelError::Http("OAuth1 key derivation failed".into()))?;
            mac.update(base_string.as_bytes());
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        };

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

fn is_form_urlencoded(request: &reqwest::Request) -> bool {
    request
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().starts_with("application/x-www-form-urlencoded"))
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
    percent_decode_str(s).decode_utf8().ok().map(|c| c.to_string())
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
        let port = url.port().unwrap_or_else(|| {
            if url.scheme() == "https" {
                443
            } else {
                80
            }
        });

        // Normalized string per Hawk spec ("hawk.1.header\n..."); payload
        // hash and ext are empty (not supported by this shim).
        let normalized = format!(
            "hawk.1.header\n{method}\n{resource}\n{host}\n{port}\n{ts}\n{nonce}\n\n\n"
        );

        let mac = if self
            .algorithm
            .as_deref()
            .map(|a| a.eq_ignore_ascii_case("sha1"))
            .unwrap_or(false)
        {
            let mut mac = <HmacSha1 as KeyInit>::new_from_slice(self.auth_key.as_bytes())
                .map_err(|_| TropelError::Http("Hawk key derivation failed".into()))?;
            mac.update(normalized.as_bytes());
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        } else {
            let mut mac = <HmacSha256 as KeyInit>::new_from_slice(self.auth_key.as_bytes())
                .map_err(|_| TropelError::Http("Hawk key derivation failed".into()))?;
            mac.update(normalized.as_bytes());
            base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
        };

        let header = format!(
            "Hawk id=\"{}\", ts=\"{}\", nonce=\"{}\", mac=\"{}\"",
            self.auth_id, ts, nonce, mac
        );
        set_auth_header(request, &header)
    }
}

// ─────────────────────────── Digest (RFC 7616) ───────────────────────────

/// HTTP Digest authentication (RFC 7616) — challenge-response.
///
/// The first request is sent unauthenticated; on a 401 the engine calls
/// [`AuthSigner::challenge_response`] which parses `WWW-Authenticate`,
/// computes the digest response (MD5 or SHA-256, with or without qop) and
/// returns the `Authorization: Digest ...` header value for the retry.
pub struct DigestAuth {
    username: String,
    password: String,
}

impl DigestAuth {
    pub fn new(username: &str, password: &str) -> Self {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }
}

impl AuthSigner for DigestAuth {
    fn name(&self) -> &str {
        "digest"
    }

    fn sign(&self, _request: &mut reqwest::Request) -> Result<()> {
        // No-op: the challenge must come from the server's 401 first.
        Ok(())
    }

    fn challenge_response(
        &self,
        www_authenticate: &str,
        request: &reqwest::Request,
    ) -> Option<String> {
        let challenge = parse_challenge(www_authenticate)?;
        if !challenge
            .get("scheme")
            .map(|s| s.eq_ignore_ascii_case("digest"))
            .unwrap_or(false)
        {
            return None;
        }
        let realm = challenge.get("realm")?;
        let nonce = challenge.get("nonce")?;
        // RFC 7616 defaults to MD5 when the server omits `algorithm`; only
        // emit the field when the challenge actually specified it (some
        // strict servers reject an explicit `algorithm=MD5`).
        let server_algorithm = challenge.get("algorithm").map(|s| s.to_ascii_uppercase());
        let algorithm = server_algorithm.clone().unwrap_or_else(|| "MD5".to_string());
        let qop = challenge.get("qop");

        let method = request.method().as_str();
        // The digest `uri` is the request-target: path + query (RFC 7616).
        let url = request.url();
        let uri = match url.query() {
            Some(q) => format!("{}?{}", url.path(), q),
            None => url.path().to_string(),
        };

        // HA1 = H(username:realm:password)
        let ha1_input = format!("{}:{}:{}", self.username, realm, self.password);
        let ha1 = match algorithm.as_str() {
            "SHA-256" => hex_sha256(ha1_input.as_bytes()),
            _ => hex_md5(ha1_input.as_bytes()),
        };
        // HA2 = H(method:uri)
        let ha2_input = format!("{method}:{uri}");
        let ha2 = match algorithm.as_str() {
            "SHA-256" => hex_sha256(ha2_input.as_bytes()),
            _ => hex_md5(ha2_input.as_bytes()),
        };

        let mut fields: Vec<(String, String)> = vec![
            ("username".into(), self.username.clone()),
            ("realm".into(), realm.clone()),
            ("nonce".into(), nonce.clone()),
            ("uri".into(), uri),
        ];

        let response = if let Some(qop) = qop {
            if qop.split(',').any(|q| q.trim() == "auth") {
                let nc = "00000001".to_string();
                let cnonce = generate_nonce();
                let response_input =
                    format!("{ha1}:{nonce}:{nc}:{cnonce}:auth:{ha2}");
                let response = digest_with(&response_input, &algorithm);
                fields.push(("qop".into(), "auth".into()));
                fields.push(("nc".into(), nc));
                fields.push(("cnonce".into(), cnonce));
                response
            } else {
                // qop present but no 'auth' — fall back to the no-qop form.
                digest_with(&format!("{ha1}:{nonce}:{ha2}"), &algorithm)
            }
        } else {
            digest_with(&format!("{ha1}:{nonce}:{ha2}"), &algorithm)
        };
        fields.push(("response".into(), response));
        if let Some(alg) = server_algorithm {
            fields.push(("algorithm".into(), alg));
        }
        if let Some(opaque) = challenge.get("opaque") {
            fields.push(("opaque".into(), opaque.clone()));
        }

        let header = fields
            .iter()
            .map(|(k, v)| format!("{k}=\"{}\"", v.replace('"', "")))
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!("Digest {header}"))
    }
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

/// Parse a `WWW-Authenticate` header value into `{key: value}` (unquoted).
/// Scheme name (before the first space) is stored under `"scheme"`.
fn parse_challenge(header: &str) -> Option<HashMap<String, String>> {
    let mut out = HashMap::new();
    let trimmed = header.trim();
    let (scheme, rest) = match trimmed.find(char::is_whitespace) {
        Some(idx) => (&trimmed[..idx], &trimmed[idx..]),
        None => (trimmed, ""),
    };
    out.insert("scheme".to_string(), scheme.to_string());
    for part in rest.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some((k, v)) = part.split_once('=') {
            let v = v.trim().trim_matches('"').to_string();
            out.insert(k.trim().to_ascii_lowercase(), v);
        }
    }
    Some(out)
}

// ─────────────────────────── Shared helpers ───────────────────────────

fn set_auth_header(request: &mut reqwest::Request, value: &str) -> Result<()> {
    insert_header(request, "authorization", value)
}

fn insert_header(request: &mut reqwest::Request, name: &str, value: &str) -> Result<()> {
    let header_name = reqwest::header::HeaderName::from_bytes(name.as_bytes()).map_err(|_| {
        TropelError::Http(format!("'{name}' is not a valid HTTP header name"))
    })?;
    let header_value = value.parse().map_err(|_| {
        TropelError::Http(format!("'{name}' value is not a valid HTTP header value"))
    })?;
    request.headers_mut().insert(header_name, header_value);
    Ok(())
}

static NONCE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Deterministic-but-unique per-process nonce: time-derived seed XORed with a
/// monotonic counter, hex-encoded. Suitable for signing nonces (uniqueness is
/// all that is required — not cryptographic secrecy).
fn generate_nonce() -> String {
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let counter = NONCE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:016x}", seed ^ counter.wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

/// Build an auth signer from an `AuthConfig`.
///
/// This is the single signer-builder used by both the executor runner
/// (`runner.rs`) and the HTTP client (`HttpClient::get_signer`) — the two
/// previously duplicated builders were consolidated into this one function.
pub fn build_auth_signer(auth: &AuthConfig) -> Option<Box<dyn AuthSigner>> {
    match auth {
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
        let mut req = build_request("GET", "https://examplebucket.s3.amazonaws.com/test.txt", None);
        AwsSigV4Auth::new("AKID", "SECRET", Some("us-east-1".into()), Some("s3".into()), None)
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
        assert_eq!(req.headers().get("x-amz-content-sha256"), req2.headers().get("x-amz-content-sha256"));
    }

    #[test]
    fn oauth1_produces_authorization_header() {
        let mut req = build_request("GET", "http://example.com/request?b5=%3D%253D&a3=a&c%40=&a2=r%20b", None);
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
        HawkAuth::new("dh37fgj492je", "werxhqb98rpaxn39848xrunpaw3489ruxnpa98w4rxn", None)
            .sign(&mut req)
            .unwrap();
        let h = auth_header(&req);
        assert!(h.starts_with("Hawk id=\"dh37fgj492je\""));
        assert!(h.contains("ts=\""));
        assert!(h.contains("nonce=\""));
        assert!(h.contains("mac=\""));
    }

    #[test]
    fn digest_challenge_response_parses_and_computes() {
        let www = r#"Digest realm="testrealm@host.com", qop="auth, auth-int", nonce="dcd98b7102dd2f0e8b11d0f600bfb0c093", opaque="5ccc069c403ebaf9f0171e9517f40e41""#;
        let req = build_request("GET", "http://www.example.com/dir/index.html", None);
        let auth = DigestAuth::new("Mufasa", "Circle Of Life");
        let header = auth.challenge_response(www, &req).expect("challenge response");
        assert!(header.starts_with("Digest "));
        assert!(header.contains("username=\"Mufasa\""));
        assert!(header.contains("realm=\"testrealm@host.com\""));
        assert!(header.contains("nonce=\"dcd98b7102dd2f0e8b11d0f600bfb0c093\""));
        assert!(header.contains("uri=\"/dir/index.html\""));
        assert!(header.contains("qop=\"auth\""));
        assert!(header.contains("nc=\"00000001\""));
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

    #[test]
    fn digest_no_qop_form() {
        let www = r#"Digest realm="x", nonce="abc123", algorithm=MD5"#;
        let req = build_request("GET", "http://example.com/", None);
        let header = DigestAuth::new("u", "p")
            .challenge_response(www, &req)
            .unwrap();
        assert!(header.contains("algorithm=\"MD5\""));
        assert!(!header.contains("qop="));
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
    fn parse_challenge_handles_quotes_and_bare_values() {
        let m = parse_challenge(r#"Digest realm="r", qop="auth", algorithm=MD5"#).unwrap();
        assert_eq!(m.get("scheme").map(|s| s.as_str()), Some("Digest"));
        assert_eq!(m.get("realm").map(|s| s.as_str()), Some("r"));
        assert_eq!(m.get("qop").map(|s| s.as_str()), Some("auth"));
        assert_eq!(m.get("algorithm").map(|s| s.as_str()), Some("MD5"));
    }
}
