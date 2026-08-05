use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// HTTP method.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum Method {
    GET,
    HEAD,
    POST,
    PUT,
    PATCH,
    DELETE,
    OPTIONS,
    TRACE,
    CONNECT,
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::GET => write!(f, "GET"),
            Method::HEAD => write!(f, "HEAD"),
            Method::POST => write!(f, "POST"),
            Method::PUT => write!(f, "PUT"),
            Method::PATCH => write!(f, "PATCH"),
            Method::DELETE => write!(f, "DELETE"),
            Method::OPTIONS => write!(f, "OPTIONS"),
            Method::TRACE => write!(f, "TRACE"),
            Method::CONNECT => write!(f, "CONNECT"),
        }
    }
}

impl Method {
    /// Parse a method from a string (case-insensitive).
    ///
    /// Named `parse` (not `from_str`) to avoid colliding with the standard
    /// `FromStr::from_str` — this variant returns `Option`, not `Result`,
    /// so it is deliberately NOT the trait impl.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GET" => Some(Method::GET),
            "HEAD" => Some(Method::HEAD),
            "POST" => Some(Method::POST),
            "PUT" => Some(Method::PUT),
            "PATCH" => Some(Method::PATCH),
            "DELETE" => Some(Method::DELETE),
            "OPTIONS" => Some(Method::OPTIONS),
            "TRACE" => Some(Method::TRACE),
            "CONNECT" => Some(Method::CONNECT),
            _ => None,
        }
    }
}

/// How the response body should be handled for a request.
///
/// Mirrors k6's per-request `params.responseType`:
/// - `Text` (default): body kept as raw bytes; decoded to text lazily on
///   access (`body_text()` / `body_json()`).
/// - `Binary`: body kept as raw bytes, surfaced as-is to scripts that want
///   binary payloads (base64 etc.).
/// - `None`: body is discarded entirely — `execute()` drains it off the wire
///   (so the pooled connection stays reusable) but stores no bytes. Pairs
///   with the global `discardResponseBodies`; scripts see an empty body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ResponseType {
    #[default]
    Text,
    Binary,
    None,
}

impl ResponseType {
    /// Parse from a k6-style string (`"text"`, `"binary"`, `"none"`).
    /// Unrecognized/empty values fall back to `Text` (k6's default).
    pub fn from_k6(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "binary" => ResponseType::Binary,
            "none" => ResponseType::None,
            _ => ResponseType::Text,
        }
    }
}

/// A single HTTP request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Request {
    /// URL string (may contain {{variables}}).
    pub url: String,
    pub method: Method,
    /// Request headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Query parameters.
    #[serde(default)]
    pub query_params: HashMap<String, String>,
    /// Request body.
    pub body: Option<Body>,
    /// Auth configuration.
    pub auth: Option<AuthConfig>,
    /// Certificate configuration.
    pub certificate: Option<CertificateConfig>,
    /// Whether to follow redirects.
    #[serde(default = "default_true")]
    pub follow_redirects: bool,
    /// Connection/read timeout.
    pub timeout: Option<Duration>,
    /// How to handle the response body (k6 `params.responseType`).
    #[serde(default)]
    pub response_type: ResponseType,
}

fn default_true() -> bool {
    true
}

impl Default for Request {
    fn default() -> Self {
        Self {
            url: String::new(),
            method: Method::GET,
            headers: HashMap::new(),
            query_params: HashMap::new(),
            body: None,
            auth: None,
            certificate: None,
            follow_redirects: true,
            timeout: None,
            response_type: ResponseType::Text,
        }
    }
}

/// Request body variants.
///
/// Custom serde: `#[serde(untagged)]` made `Json(Value)` match ANY JSON, so
/// `FormData`/`UrlEncoded`/`Binary`/`GraphQL` were unreachable on
/// deserialize — every JSON round-trip (distributed workers, spool, replay)
/// silently converted `UrlEncoded` → `Json`: Content-Type flipped and wire
/// bytes changed from `a=1&b=2` to `{"a":"1"}` (a form endpoint 400s on the
/// worker and passes locally).
///
/// Wire format (backward compatible for the two common cases):
/// - `Raw(s)` → JSON string (unchanged)
/// - `Json(v)` → the raw JSON value (unchanged)
/// - the other four → a tagged object `{"__tropel_body": "<kind>", …}` so
///   they survive a round-trip intact instead of collapsing into `Json`.
///
/// Known limitation: a user `Json` body that legitimately contains a
/// `__tropel_body` key with a recognized tag value (e.g.
/// `{"__tropel_body": "url_encoded", "fields": …}`) is interpreted as that
/// variant. Unknown tag values are treated as plain `Json` (the key is
/// preserved), so only the four exact tag strings are ambiguous.
#[derive(Debug, Clone)]
pub enum Body {
    Raw(String),
    Json(serde_json::Value),
    FormData(HashMap<String, String>),
    UrlEncoded(HashMap<String, String>),
    Binary(Vec<u8>),
    GraphQL {
        query: String,
        variables: Option<HashMap<String, serde_json::Value>>,
    },
}

impl Serialize for Body {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeMap;
        match self {
            Body::Raw(s) => serializer.serialize_str(s),
            Body::Json(v) => v.serialize(serializer),
            Body::FormData(fields) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("__tropel_body", "form_data")?;
                map.serialize_entry("fields", fields)?;
                map.end()
            }
            Body::UrlEncoded(fields) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("__tropel_body", "url_encoded")?;
                map.serialize_entry("fields", fields)?;
                map.end()
            }
            Body::Binary(data) => {
                let mut map = serializer.serialize_map(Some(2))?;
                map.serialize_entry("__tropel_body", "binary")?;
                map.serialize_entry("data", data)?;
                map.end()
            }
            Body::GraphQL {
                query,
                variables,
            } => {
                let mut map = serializer.serialize_map(Some(3))?;
                map.serialize_entry("__tropel_body", "graphql")?;
                map.serialize_entry("query", query)?;
                map.serialize_entry("variables", variables)?;
                map.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for Body {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            // A JSON string is a Raw body (unchanged wire format).
            serde_json::Value::String(s) => Ok(Body::Raw(s)),
            serde_json::Value::Object(mut obj) => {
                match obj
                    .remove("__tropel_body")
                    .and_then(|v| v.as_str().map(str::to_string))
                {
                    Some(tag) => match tag.as_str() {
                        "form_data" => {
                            let fields = obj
                                .remove("fields")
                                .and_then(|f| serde_json::from_value(f).ok())
                                .unwrap_or_default();
                            Ok(Body::FormData(fields))
                        }
                        "url_encoded" => {
                            let fields = obj
                                .remove("fields")
                                .and_then(|f| serde_json::from_value(f).ok())
                                .unwrap_or_default();
                            Ok(Body::UrlEncoded(fields))
                        }
                        "binary" => {
                            let data = obj
                                .remove("data")
                                .and_then(|d| serde_json::from_value(d).ok())
                                .unwrap_or_default();
                            Ok(Body::Binary(data))
                        }
                        "graphql" => {
                            let query = obj
                                .remove("query")
                                .and_then(|q| q.as_str().map(str::to_string))
                                .unwrap_or_default();
                            let variables = obj
                                .remove("variables")
                                .and_then(|v| serde_json::from_value(v).ok());
                            Ok(Body::GraphQL { query, variables })
                        }
                        // Unknown tag → treat the WHOLE object as a Json body
                        // (restoring the removed key). Hard-erroring here
                        // would be a regression: before this fix ANY object
                        // parsed as Json, including a legit user payload that
                        // happens to carry a `__tropel_body` key.
                        other => {
                            obj.insert(
                                "__tropel_body".to_string(),
                                serde_json::Value::String(other.to_string()),
                            );
                            Ok(Body::Json(serde_json::Value::Object(obj)))
                        }
                    },
                    // No discriminator → Json body (backward compatible with
                    // the old untagged wire form).
                    None => Ok(Body::Json(serde_json::Value::Object(obj))),
                }
            }
            other => Ok(Body::Json(other)),
        }
    }
}

impl Body {
    /// Serialize a GraphQL body to its wire JSON.
    ///
    /// Returns `{"query": "..."}` plus a `"variables"` key ONLY when the
    /// variables map is present and non-empty — strict GraphQL servers reject
    /// an empty `"variables": {}` and Postman/k6 omit the key too. This is
    /// the SINGLE source of truth for the GraphQL wire body: the HTTP client's
    /// `body_to_reqwest` and `body_size` both call it, so the bytes sent and
    /// the `data_sent` accounting can never diverge (the old code dropped
    /// `variables` entirely and estimated size with `query.len() + 50`).
    pub fn graphql_json_string(
        query: &str,
        variables: &Option<HashMap<String, serde_json::Value>>,
    ) -> String {
        let mut body = serde_json::Map::new();
        body.insert("query".to_string(), serde_json::Value::String(query.to_string()));
        if let Some(vars) = variables {
            if !vars.is_empty() {
                let mut obj = serde_json::Map::new();
                for (k, v) in vars {
                    obj.insert(k.clone(), v.clone());
                }
                body.insert("variables".to_string(), serde_json::Value::Object(obj));
            }
        }
        serde_json::Value::Object(body).to_string()
    }
}

/// HTTP response with lazy body decoding.
///
/// The body is stored as raw `Vec<u8>`. `body_text()` and `body_json()`
/// parse on first access — avoiding the cost of `String::from_utf8` and
/// `serde_json::from_str` on every response when scripts rarely inspect
/// the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    /// The URL that produced THIS response. For a redirect chain each hop
    /// carries its own URL; the final response carries the final URL (what
    /// scripts see via pm.response / k6 res.url).
    #[serde(default)]
    pub url: String,
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub response_time: Duration,
    pub timings: Option<Timings>,
    pub cookies: Vec<Cookie>,
    pub size: u64,
    /// Intermediate redirect hops, in order, each captured as its own
    /// request (k6 parity: a 302 chain counts as hops + 1 requests, not
    /// just the final). Empty when the request did not redirect. Callers
    /// emit one http_req_* sample set PER hop plus the final response.
    #[serde(default)]
    pub redirects: Vec<Response>,
}

impl Response {
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
    /// Parses directly from the raw `Vec<u8>` body, skipping the intermediate
    /// `String::from_utf8` step that `body_text()` requires. Uses `simd-json`
    /// for ~2-4x faster parsing on typical payloads.
    pub fn body_json(&self) -> Option<serde_json::Value> {
        if self.body.is_empty() {
            return None;
        }
        let mut body_bytes = self.body.clone();
        simd_json::serde::from_slice(&mut body_bytes).ok()
    }
}

/// Request sub-timings, matching k6's http_req_* breakdown.
///
/// All durations are wall-clock microseconds measured from the start of the
/// HTTP request (`execute()`). `blocked`, `dns`, and `connecting` are filled
/// from real connector instrumentation (reqwest's `dns_resolver` and
/// `connector_layer` hooks — see `tropel-http::subtimings`); they are ZERO
/// when a pooled keep-alive connection is reused (no connection work).
///
/// The measurable phases:
/// - **blocked**: request start until connection attempt begins (pool wait)
/// - **dns**: real DNS resolution time
/// - **connecting**: TCP connect (plus TLS for https, folded into the
///   connector call by reqwest)
/// - **waiting** (TTFB): response headers received
/// - **receiving**: response headers received until full body received
/// - **total**: full request lifecycle (start until body fully received)
///
/// Phases not yet measurable from reqwest alone:
/// - **tls_handshaking**: TLS handshake (included in `connecting` for https)
/// - **sending**: time to transmit the request body
///
/// These two are set to `Duration::ZERO`; a hyper-based custom connector
/// would be required to split them out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timings {
    /// Time blocked before connection starts (pool wait / queueing).
    #[serde(default)]
    pub blocked: Duration,
    /// DNS resolution time.
    #[serde(default)]
    pub dns: Duration,
    /// TCP connect time (TLS included for https).
    #[serde(default)]
    pub connecting: Duration,
    /// TLS handshake time. Always ZERO — folded into `connecting` by reqwest.
    #[serde(default)]
    pub tls_handshaking: Duration,
    /// Time to send the request body. Always ZERO — not exposed by reqwest.
    #[serde(default)]
    pub sending: Duration,
    /// Time to first byte (TTFB) — from request start to response head received.
    #[serde(default)]
    pub waiting: Duration,
    /// Time to receive the full response body.
    #[serde(default)]
    pub receiving: Duration,
    /// Total request duration (start to full body received).
    pub total: Duration,
}

impl Timings {
    /// Create a new Timings from measured phases.
    /// blocked/dns/connecting/tls_handshaking/sending default to ZERO.
    pub fn from_measured(waiting: Duration, receiving: Duration, total: Duration) -> Self {
        Self {
            blocked: Duration::ZERO,
            dns: Duration::ZERO,
            connecting: Duration::ZERO,
            tls_handshaking: Duration::ZERO,
            sending: Duration::ZERO,
            waiting,
            receiving,
            total,
        }
    }
}

/// A cookie.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cookie {
    pub name: String,
    pub value: String,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub http_only: Option<bool>,
    pub secure: Option<bool>,
    pub same_site: Option<String>,
    pub expires: Option<String>,
}

/// Auth configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AuthConfig {
    /// Explicitly NO auth — Postman's `{"type":"noauth"}`. Distinct from
    /// `None` (which means "no auth configured → inherit from the parent
    /// scope"): a request marked noauth must NOT inherit collection/folder
    /// auth. The runner treats `Some(NoAuth)` as "no signer, and don't fall
    /// back to scenario auth" — the inverse of Postman semantics that the
    /// old `None` mapping produced.
    #[serde(rename = "noauth")]
    NoAuth,
    #[serde(rename = "bearer")]
    Bearer { token: String },
    #[serde(rename = "basic")]
    Basic { username: String, password: String },
    #[serde(rename = "apikey")]
    ApiKey {
        key: String,
        value: String,
        location: ApiKeyLocation,
    },
    #[serde(rename = "digest")]
    Digest { username: String, password: String },
    #[serde(rename = "oauth1")]
    OAuth1 {
        consumer_key: String,
        consumer_secret: String,
        token: Option<String>,
        token_secret: Option<String>,
    },
    #[serde(rename = "oauth2")]
    OAuth2 {
        access_token: String,
        token_type: Option<String>,
    },
    #[serde(rename = "aws-sigv4")]
    AwsSigV4 {
        access_key: String,
        secret_key: String,
        region: Option<String>,
        service: Option<String>,
        session_token: Option<String>,
    },
    #[serde(rename = "hawk")]
    Hawk {
        auth_id: String,
        auth_key: String,
        algorithm: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ApiKeyLocation {
    #[serde(rename = "header")]
    Header,
    #[serde(rename = "query")]
    Query,
}

/// Certificate configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateConfig {
    pub cert: Option<String>,
    pub key: Option<String>,
    pub passphrase: Option<String>,
}

/// A set of metric tags backed by a fast hash map with `Arc<str>` key/value
/// interning to reduce per-iteration allocation churn.
///
/// Tag keys are almost always string literals ("url", "method", "status_code",
/// etc.) and tag values are often reused across iterations (status codes,
/// group names, etc.). Using `Arc<str>` avoids cloning the underlying string
/// data when the tag map is cloned (e.g., when building both a duration sample
/// and a counter sample from the same tags).
///
/// Internally backed by `FxHashMap` (faster than std's SipHash for small
/// string keys) with `Arc<str>` values.
#[derive(Debug, Clone)]
pub struct TagMap {
    pub(crate) inner: FxHashMap<Arc<str>, Arc<str>>,
}

impl TagMap {
    /// Create an empty tag map.
    pub fn new() -> Self {
        Self {
            inner: FxHashMap::default(),
        }
    }

    /// Create a tag map with the given pre-allocated capacity.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: FxHashMap::with_capacity_and_hasher(cap, Default::default()),
        }
    }

    /// Create a tag map from an iterator of (key, value) pairs.
    /// Both keys and values are interned as `Arc<str>`.
    pub fn from_pairs(
        pairs: impl IntoIterator<Item = (impl Into<Arc<str>>, impl Into<Arc<str>>)>,
    ) -> Self {
        let mut map = FxHashMap::default();
        for (k, v) in pairs {
            map.insert(k.into(), v.into());
        }
        Self { inner: map }
    }

    /// Insert a tag pair. Both key and value are interned as `Arc<str>`.
    pub fn insert(&mut self, key: impl Into<Arc<str>>, value: impl Into<Arc<str>>) {
        self.inner.insert(key.into(), value.into());
    }

    /// Get a tag value by key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.inner.get(key).map(|s| s.as_ref())
    }

    /// Returns true if the map contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Number of tag pairs.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Iterate over (key, value) pairs as &str references.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> + '_ {
        self.inner.iter().map(|(k, v)| (k.as_ref(), v.as_ref()))
    }

    /// Collect into a sorted Vec of (Arc<str>, Arc<str>) for MetricKey construction.
    /// The Arc references are cloned (ref-count bump only, no string copy).
    pub fn to_sorted_arc_vec(&self) -> Vec<(Arc<str>, Arc<str>)> {
        let mut pairs: Vec<(Arc<str>, Arc<str>)> = self
            .inner
            .iter()
            .map(|(k, v)| (Arc::clone(k), Arc::clone(v)))
            .collect();
        pairs.sort();
        pairs
    }
}

impl Default for TagMap {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for TagMap {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(self.inner.len()))?;
        for (k, v) in self.inner.iter() {
            map.serialize_entry(k.as_ref(), v.as_ref())?;
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for TagMap {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw: HashMap<String, String> = HashMap::deserialize(deserializer)?;
        let mut map = FxHashMap::with_capacity_and_hasher(raw.len(), Default::default());
        for (k, v) in raw {
            map.insert(Arc::from(k), Arc::from(v));
        }
        Ok(Self { inner: map })
    }
}

/// A single metric sample emitted during execution.
///
/// `metric` is a `Cow<'static, str>` so the ~12 static names emitted per
/// request ("http_req_duration", "http_reqs", …) are zero-alloc
/// `Cow::Borrowed` — no per-sample `String` churn on the hot path. `tags` is
/// an `Arc<TagMap>` so the repeated `tags.clone()` calls per request (one per
/// sample) become Arc ref-count bumps instead of full FxHashMap copies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Metric name (e.g. "http_req_duration", "checks").
    pub metric: Cow<'static, str>,
    /// Metric value.
    pub value: f64,
    /// Tags (e.g. url, method, status_code, name).
    pub tags: Arc<TagMap>,
    /// Timestamp.
    pub timestamp: SystemTime,
    /// Sample type.
    #[serde(rename = "type")]
    pub sample_type: SampleType,
}

/// Type of metric sample.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[derive(Default)]
pub enum SampleType {
    #[default]
    Point,
    Counter,
    Trend,
    Rate,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn body_roundtrip_preserves_all_variants() {
        // Regression (backlog line 92): #[serde(untagged)] made `Json(Value)`
        // match ANY JSON, so FormData/UrlEncoded/Binary/GraphQL were
        // unreachable on deserialize — every round-trip silently converted
        // them to Json (Content-Type flips, wire bytes change).
        let mut form = HashMap::new();
        form.insert("a".to_string(), "1".to_string());
        form.insert("b".to_string(), "2".to_string());
        let mut url = HashMap::new();
        url.insert("q".to_string(), "hello world".to_string());
        let mut vars = HashMap::new();
        vars.insert("id".to_string(), serde_json::json!(42));

        let cases = vec![
            Body::Raw("raw body".into()),
            Body::Json(serde_json::json!({"a": 1, "b": [true, null]})),
            Body::FormData(form),
            Body::UrlEncoded(url),
            Body::Binary(vec![0u8, 1, 2, 255]),
            Body::GraphQL {
                query: "{ user { id } }".into(),
                variables: Some(vars),
            },
        ];

        for body in cases {
            let json = serde_json::to_string(&body).expect("serialize");
            let back: Body = serde_json::from_str(&json).expect("deserialize");
            match (&body, &back) {
                (Body::Raw(a), Body::Raw(b)) => assert_eq!(a, b),
                (Body::Json(a), Body::Json(b)) => assert_eq!(a, b),
                (Body::FormData(a), Body::FormData(b)) => assert_eq!(a, b),
                (Body::UrlEncoded(a), Body::UrlEncoded(b)) => {
                    assert_eq!(a, b, "UrlEncoded must not collapse into Json")
                }
                (Body::Binary(a), Body::Binary(b)) => assert_eq!(a, b),
                (
                    Body::GraphQL {
                        query: aq,
                        variables: av,
                    },
                    Body::GraphQL {
                        query: bq,
                        variables: bv,
                    },
                ) => {
                    assert_eq!(aq, bq);
                    assert_eq!(av, bv);
                }
                (a, b) => panic!("variant changed on round-trip: {a:?} -> {b:?}"),
            }
        }
    }

    #[test]
    fn body_untagged_wire_forms_still_parse() {
        // Backward compat: the OLD untagged serialization of Raw (a JSON
        // string) and Json (a bare JSON value) must still deserialize.
        let raw: Body = serde_json::from_str("\"hello\"").unwrap();
        assert!(matches!(raw, Body::Raw(_)));
        let json: Body = serde_json::from_str(r#"{"x":1}"#).unwrap();
        assert!(matches!(json, Body::Json(_)));
        let arr: Body = serde_json::from_str("[1,2,3]").unwrap();
        assert!(matches!(arr, Body::Json(_)));
    }
}

