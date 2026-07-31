use crate::auth::AuthSigner;
use std::collections::HashMap;
use std::time::Duration;
use tropel_core::config::HttpConfig;
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
}

impl HttpClient {
    /// Create a new HTTP client from config.
    pub fn new(config: &HttpConfig) -> Result<Self> {
        let mut builder = reqwest::Client::builder()
            .cookie_store(true)
            .user_agent(&config.user_agent)
            .pool_max_idle_per_host(config.max_idle_connections)
            .timeout(DEFAULT_REQUEST_TIMEOUT);

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

        let inner = builder
            .build()
            .map_err(|e| TropelError::Http(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            inner,
            discard_bodies: config.discard_response_bodies,
        })
    }

    /// Execute an HTTP request with sub-timing instrumentation.
    ///
    /// Measures three phases of the request lifecycle:
    /// - **waiting** (TTFB): from `execute()` start to response headers received.
    ///   Includes DNS, TCP, TLS, sending, and server processing time.
    /// - **receiving**: from response headers to full body bytes received.
    /// - **total**: entire `execute()` duration.
    ///
    /// Blocked, connecting, tls_handshaking, and sending phases are set to
    /// `Duration::ZERO` — they require connector-level instrumentation that
    /// reqwest's stable API does not expose.
    ///
    /// Returns the response along with the number of bytes sent in the request body.
    pub async fn execute(
        &self,
        request: &Request,
        signer: Option<&dyn AuthSigner>,
    ) -> Result<HttpResponse> {
        let total_start = std::time::Instant::now();

        // Calculate request body size for data_sent tracking
        let request_body_size: u64 = request
            .body
            .as_ref()
            .map(|b| body_size(b) as u64)
            .unwrap_or(0);

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

        // Apply auth — sign takes ownership and returns a new builder
        let req_builder = if let Some(signer) = signer {
            match signer.sign(req_builder) {
                Ok(builder) => builder,
                Err(e) => {
                    return Err(TropelError::Http(format!("Auth signing failed: {}", e)));
                }
            }
        } else {
            req_builder
        };

        // ═══════════════════════════════════════════════════════
        // Phase 1: Send request → receive response head (TTFB)
        // ═══════════════════════════════════════════════════════
        // `send().await` returns the Response once the status line and
        // response headers are received. The request body has been sent
        // by this point (or is being streamed).
        //
        // The measured "waiting" time includes everything up to this point:
        // blocked + DNS + TCP connect + TLS handshake + sending + server
        // processing. Without connector-level instrumentation we cannot
        // separate these phases.
        let waiting_start = std::time::Instant::now();
        let response = req_builder
            .send()
            .await
            .map_err(|e| TropelError::Http(format!("Request failed: {}", e)))?;
        let waiting_duration = waiting_start.elapsed();

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
        let receiving_start = std::time::Instant::now();
        let body_vec = if self.discard_bodies {
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

        // Build sub-timings.
        // blocked/dns/connecting/tls_handshaking/sending are ZERO because
        // reqwest's stable API does not expose connection-level phases.
        let timings = Timings::from_measured(waiting_duration, receiving_duration, total_duration);

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
    pub fn get_signer(&self, auth: &AuthConfig) -> Option<Box<dyn AuthSigner>> {
        match auth {
            AuthConfig::Basic { username, password } => {
                Some(Box::new(crate::auth::BasicAuth::new(username, password)))
            }
            AuthConfig::Bearer { token } => Some(Box::new(crate::auth::BearerAuth::new(token))),
            _ => None, // Other auth types not yet fully implemented
        }
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
        Body::GraphQL { query, .. } => query.len() + 50, // approximate JSON wrapper
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
        Body::GraphQL {
            query,
            variables: _,
        } => {
            let body = serde_json::json!({ "query": query });
            serde_json::to_string(&body).unwrap_or_default().into()
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
}

fn parse_duration(s: &str) -> Result<Duration> {
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
                let k = urlencoding(&k);
                let v = urlencoding(&v);
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

