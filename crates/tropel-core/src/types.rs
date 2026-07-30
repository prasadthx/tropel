use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
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
    pub fn from_str(s: &str) -> Option<Self> {
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
        }
    }
}

/// Request body variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Body {
    Raw(String),
    Json(serde_json::Value),
    FormData(HashMap<String, String>),
    UrlEncoded(HashMap<String, String>),
    Binary(Vec<u8>),
    GraphQL { query: String, variables: Option<HashMap<String, serde_json::Value>> },
}

/// HTTP response with lazy body decoding.
///
/// The body is stored as raw `Vec<u8>`. `body_text()` and `body_json()`
/// parse on first access — avoiding the cost of `String::from_utf8` and
/// `serde_json::from_str` on every response when scripts rarely inspect
/// the body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub response_time: Duration,
    pub timings: Option<Timings>,
    pub cookies: Vec<Cookie>,
    pub size: u64,
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

/// Request timings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Timings {
    pub dns: Option<Duration>,
    pub tcp: Option<Duration>,
    pub tls: Option<Duration>,
    pub first_byte: Option<Duration>,
    pub total: Duration,
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
    #[serde(rename = "bearer")]
    Bearer { token: String },
    #[serde(rename = "basic")]
    Basic { username: String, password: String },
    #[serde(rename = "apikey")]
    ApiKey { key: String, value: String, location: ApiKeyLocation },
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
    OAuth2 { access_token: String, token_type: Option<String> },
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
    pub fn from_pairs(pairs: impl IntoIterator<Item = (impl Into<Arc<str>>, impl Into<Arc<str>>)>) -> Self {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Metric name (e.g. "http_req_duration", "checks").
    pub metric: String,
    /// Metric value.
    pub value: f64,
    /// Tags (e.g. url, method, status_code, name).
    pub tags: TagMap,
    /// Timestamp.
    pub timestamp: SystemTime,
    /// Sample type.
    #[serde(rename = "type")]
    pub sample_type: SampleType,
}

/// Type of metric sample.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SampleType {
    Point,
    Counter,
    Trend,
    Rate,
}

impl Default for SampleType {
    fn default() -> Self {
        Self::Point
    }
}
