use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

/// HTTP response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Response {
    pub status_code: u16,
    pub status_text: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    pub body_text: Option<String>,
    pub body_json: Option<serde_json::Value>,
    pub response_time: Duration,
    pub timings: Option<Timings>,
    pub cookies: Vec<Cookie>,
    pub size: u64,
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

/// A single metric sample emitted during execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Sample {
    /// Metric name (e.g. "http_req_duration", "checks").
    pub metric: String,
    /// Metric value.
    pub value: f64,
    /// Tags (e.g. url, method, status_code, name).
    pub tags: HashMap<String, String>,
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
