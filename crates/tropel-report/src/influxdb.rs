//! # InfluxDB streaming output
//!
//! Streams samples to an InfluxDB instance as line-protocol
//! (`metric[,tag=val,...] field=value`). Tags are encoded as InfluxDB tags;
//! the numeric sample value is the field `value`.
//!
//! **Transports** (k6 parity):
//!
//! - **HTTP** — the default when `addr` is an `http(s)://` URL. k6 writes
//!   over HTTP, not UDP. URL shapes:
//!   - v2: `http://host:8086?org=<o>&bucket=<b>[&token=<t>]` → POSTs
//!     `/api/v2/write?org=…&bucket=…&precision=ns` with `Authorization:
//!     Token <t>` (token from the URL or the `INFLUXDB_V2_TOKEN` env var).
//!   - v1: `http://host:8086/<db>` (or `?db=<db>`) → POSTs `/write?db=<db>`.
//! - **UDP** — when `addr` is a bare `host:port` (backward compatible;
//!   k6 dropped UDP, so HTTP is preferred). Per InfluxDB's UDP semantics
//!   the line carries no timestamp — the server assigns arrival time.
//!
//! Samples are buffered and sent every `FLUSH_INTERVAL` (or when the
//! buffer exceeds `MAX_BUFFERED_SAMPLES`); failures are logged, never
//! fatal to the run.

use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::broadcast;
use tropel_core::types::Sample;
use tropel_core::{Result, TropelError};

use crate::output::TagPolicy;
use crate::Output;

/// How often buffered samples are sent.
const FLUSH_INTERVAL: Duration = Duration::from_secs(2);
/// Max buffered samples before a forced send.
const MAX_BUFFERED_SAMPLES: usize = 10_000;
/// Max UDP payload per datagram. InfluxDB's UDP transport caps around
/// 64 KB but typical deployments are lower; chunking keeps the forced-
/// flush path from producing an EMSGSIZE failure that drops everything.
const MAX_DATAGRAM_BYTES: usize = 8 * 1024;

/// Where samples are sent — either a UDP socket or an HTTP write endpoint.
#[derive(Debug)]
enum InfluxTarget {
    /// Bare `host:port` → UDP line-protocol datagrams (no timestamp).
    Udp(SocketAddr),
    /// `http(s)://` URL → HTTP write. For v2 the URL carries `org`/`bucket`
    /// (and optionally `token`) query params; for v1 the db comes from the
    /// path or a `db` query param.
    Http {
        /// Base URL without query (e.g. `http://localhost:8086`).
        base: String,
        /// v1 database name (`POST /write?db=<db>`).
        db: Option<String>,
        /// v2 organization (`POST /api/v2/write?org=<org>`).
        org: Option<String>,
        /// v2 bucket.
        bucket: Option<String>,
        /// v2 auth token (`Authorization: Token <token>`).
        token: Option<String>,
        /// v1 basic auth from the URL userinfo (`http://user:pass@host:…`;
        /// k6 sends `Authorization: Basic …` for v1). Sent on BOTH v1 and v2
        /// writes when present.
        user: Option<String>,
        password: Option<String>,
    },
}

/// InfluxDB line-protocol streaming output (HTTP v1/v2 or UDP).
pub struct InfluxdbOutput {
    target: InfluxTarget,
    /// Buffered lines (joined by `\n` on send).
    buffer: Mutex<Vec<String>>,
    total_buffered: AtomicUsize,
    /// Tag forwarding policy (allowlist + cardinality cap).
    tag_policy: TagPolicy,
}

impl InfluxdbOutput {
    /// Create an output sending to `addr` — either an `http(s)://` URL
    /// (HTTP v1/v2, k6-compatible) or a bare `host:port` (UDP).
    pub fn new(addr: impl Into<String>) -> Result<Self> {
        let addr = addr.into();
        let target = if addr.starts_with("http://") || addr.starts_with("https://") {
            parse_http_target(&addr)?
        } else {
            let sock: SocketAddr = addr
                .parse()
                .map_err(|e| TropelError::Config(format!("invalid influxdb address: {e}")))?;
            InfluxTarget::Udp(sock)
        };
        Ok(Self {
            target,
            buffer: Mutex::new(Vec::new()),
            total_buffered: AtomicUsize::new(0),
            tag_policy: TagPolicy::default(),
        })
    }

    /// Set the tag forwarding policy (allowlist + cardinality cap).
    pub fn with_tag_policy(mut self, policy: TagPolicy) -> Self {
        self.tag_policy = policy;
        self
    }

    /// Spawn a consumer task sending samples to InfluxDB.
    pub fn spawn(
        mut rx: broadcast::Receiver<Sample>,
        addr: String,
        tag_policy: TagPolicy,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let output = match InfluxdbOutput::new(addr).map(|o| o.with_tag_policy(tag_policy)) {
                Ok(o) => o,
                Err(e) => {
                    tracing::warn!("influxdb output disabled: {e}");
                    return;
                }
            };
            let mut tick = tokio::time::interval(FLUSH_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    res = rx.recv() => match res {
                        Ok(sample) => {
                            output.buffer(&sample);
                            if output.total_buffered.load(Ordering::Relaxed) >= MAX_BUFFERED_SAMPLES {
                                if let Err(e) = output.flush().await {
                                    tracing::warn!("influxdb send failed: {e}");
                                }
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => break,
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            tracing::trace!("influxdb dropped {n} samples (consumer lag)");
                        }
                    },
                    _ = tick.tick() => {
                        if output.total_buffered.load(Ordering::Relaxed) > 0 {
                            if let Err(e) = output.flush().await {
                                tracing::warn!("influxdb send failed: {e}");
                            }
                        }
                    }
                }
            }

            if let Err(e) = output.flush().await {
                tracing::warn!("influxdb final send failed: {e}");
            }
        })
    }

    /// Escape a line-protocol component per InfluxDB rules.
    fn escape(s: &str, in_quotes: bool) -> String {
        let mut out = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                ' ' | ',' | '=' if !in_quotes => out.push('\\'),
                '"' if in_quotes => out.push('\\'),
                '\\' => out.push('\\'),
                _ => {}
            }
            out.push(c);
        }
        out
    }

    /// Encode a sample as one line-protocol line and buffer it. HTTP
    /// targets carry a nanosecond timestamp (k6 sends `precision=ns`); UDP
    /// lines carry none (the server assigns arrival time).
    fn buffer(&self, sample: &Sample) {
        // measurement[,tag=val,...] field=value — no stray space between
        // the measurement and the tag set (line protocol is strict).
        let mut line = Self::escape(&sample.metric, false);
        let tags = self.tag_policy.apply(&sample.tags);
        if !tags.is_empty() {
            let tag_list: Vec<String> = tags
                .iter()
                .map(|(k, v)| format!("{}={}", Self::escape(k, false), Self::escape(v, false)))
                .collect();
            line.push_str(&format!(",{}", tag_list.join(",")));
        }
        // field set
        let value = if sample.value.fract() == 0.0 && sample.value.abs() < 9_007_199_254_740_992.0 {
            format!("{}i", sample.value as i64)
        } else {
            sample.value.to_string()
        };
        line.push_str(&format!(" value={value}"));
        if matches!(self.target, InfluxTarget::Http { .. }) {
            let ns = sample
                .timestamp
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            line.push_str(&format!(" {ns}"));
        }
        self.buffer.lock().unwrap().push(line);
        self.total_buffered.fetch_add(1, Ordering::Relaxed);
    }

    /// Drain the buffer and send to the configured transport (HTTP POST or
    /// UDP datagrams, chunked to the UDP payload cap).
    async fn flush(&self) -> Result<()> {
        let lines = {
            let mut guard = self.buffer.lock().unwrap();
            let taken = std::mem::take(&mut *guard);
            self.total_buffered.store(0, Ordering::Relaxed);
            taken
        };
        if lines.is_empty() {
            return Ok(());
        }

        match &self.target {
            InfluxTarget::Udp(addr) => {
                let socket = UdpSocket::bind("0.0.0.0:0")
                    .await
                    .map_err(|e| TropelError::Http(format!("influxdb bind failed: {e}")))?;
                // Chunk lines into ≤ MAX_DATAGRAM_BYTES datagrams so a large
                // forced flush never exceeds the UDP payload cap.
                let mut chunk: Vec<&str> = Vec::new();
                let mut chunk_len = 0usize;
                for line in &lines {
                    if !chunk.is_empty() && chunk_len + line.len() + 1 > MAX_DATAGRAM_BYTES {
                        socket
                            .send_to(chunk.join("\n").as_bytes(), *addr)
                            .await
                            .map_err(|e| TropelError::Http(format!("influxdb send failed: {e}")))?;
                        chunk.clear();
                        chunk_len = 0;
                    }
                    chunk_len += line.len() + 1;
                    chunk.push(line);
                }
                if !chunk.is_empty() {
                    socket
                        .send_to(chunk.join("\n").as_bytes(), *addr)
                        .await
                        .map_err(|e| TropelError::Http(format!("influxdb send failed: {e}")))?;
                }
                Ok(())
            }
            InfluxTarget::Http {
                base,
                db,
                org,
                bucket,
                token,
                user,
                password,
            } => {
                let client = reqwest::Client::new();
                let (url, builder) = if let (Some(org), Some(bucket)) = (org, bucket) {
                    // v2 write endpoint.
                    let url = format!("{base}/api/v2/write?org={org}&bucket={bucket}&precision=ns");
                    let builder = client.post(&url);
                    let builder = match token {
                        Some(t) => builder.header("Authorization", format!("Token {t}")),
                        None => builder,
                    };
                    (url, builder)
                } else {
                    // v1 write endpoint (db from URL path or ?db=).
                    let db = db.as_deref().unwrap_or("k6");
                    let url = format!("{base}/write?db={db}");
                    let builder = client.post(&url);
                    (url, builder)
                };
                // Backlog line 154: v1 (and v2) Basic auth from the URL
                // userinfo — `http://user:pass@host:8086/db` sends
                // `Authorization: Basic …` (k6 parity). reqwest encodes the
                // header itself, so no manual base64.
                let builder = match (user, password) {
                    (Some(u), p) => builder.basic_auth(u, p.clone()),
                    (None, _) => builder,
                };

                let body = lines.join("\n");
                let resp = builder.body(body).send().await.map_err(|e| {
                    TropelError::Http(format!("influxdb HTTP write to {url} failed: {e}"))
                })?;
                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(TropelError::Http(format!(
                        "influxdb HTTP write to {url} returned {status}: {}",
                        text.trim()
                    )));
                }
                Ok(())
            }
        }
    }
}

/// Parse an `http(s)://` InfluxDB URL into an HTTP [`InfluxTarget`].
///
/// - v2 when the query carries `org` + `bucket` (token from `token` param
///   or the `INFLUXDB_V2_TOKEN` env var);
/// - v1 otherwise, with the db from the first non-empty path segment or a
///   `db` query param.
fn parse_http_target(url_str: &str) -> Result<InfluxTarget> {
    let url = reqwest::Url::parse(url_str)
        .map_err(|e| TropelError::Config(format!("invalid influxdb URL: {e}")))?;
    let base = format!(
        "{}://{}",
        url.scheme(),
        url.host_str().unwrap_or("localhost")
    ) + &url.port().map(|p| format!(":{p}")).unwrap_or_default();

    let pairs: std::collections::HashMap<String, String> = url.query_pairs().into_owned().collect();
    let org = pairs.get("org").cloned().filter(|s| !s.is_empty());
    let bucket = pairs.get("bucket").cloned().filter(|s| !s.is_empty());
    if let (Some(org), Some(bucket)) = (&org, &bucket) {
        let token = pairs.get("token").cloned().or_else(|| {
            std::env::var("INFLUXDB_V2_TOKEN")
                .ok()
                .filter(|t| !t.is_empty())
        });
        let (user, password) = userinfo_from_url(&url);
        return Ok(InfluxTarget::Http {
            base,
            db: None,
            org: Some(org.clone()),
            bucket: Some(bucket.clone()),
            token,
            user,
            password,
        });
    }

    // v1: db from path (first non-empty segment) or ?db=.
    let db = pairs.get("db").cloned().or_else(|| {
        url.path_segments()
            .and_then(|mut s| s.find(|p| !p.is_empty()))
            .map(str::to_string)
    });
    let (user, password) = userinfo_from_url(&url);
    Ok(InfluxTarget::Http {
        base,
        db,
        org: None,
        bucket: None,
        token: None,
        user,
        password,
    })
}

/// Extract `(username, password)` from a URL's userinfo, if any. The URL
/// crate percent-decodes these, so `user%40domain:secret` becomes the raw
/// pair — exactly what InfluxDB v1 expects for Basic auth.
fn userinfo_from_url(url: &reqwest::Url) -> (Option<String>, Option<String>) {
    let user = url.username();
    if user.is_empty() {
        (None, None)
    } else {
        (Some(user.to_string()), url.password().map(str::to_string))
    }
}

#[async_trait]
impl Output for InfluxdbOutput {
    fn name(&self) -> &str {
        "influxdb"
    }

    async fn sample(&self, samples: &[Sample]) -> Result<()> {
        for sample in samples {
            self.buffer(sample);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<()> {
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tropel_core::types::{Sample, SampleType, TagMap};

    fn sample(metric: &str, value: f64, tags: &[(&str, &str)]) -> Sample {
        let mut map = TagMap::new();
        for (k, v) in tags {
            map.insert((*k).to_string(), (*v).to_string());
        }
        Sample {
            metric: std::borrow::Cow::Owned(metric.to_string()),
            value,
            tags: std::sync::Arc::new(map),
            timestamp: SystemTime::now(),
            sample_type: SampleType::Trend,
        }
    }

    #[test]
    fn encodes_line_protocol() {
        let output = InfluxdbOutput::new("127.0.0.1:8089").unwrap();
        output.buffer(&sample("http_reqs", 1.0, &[("status", "200")]));
        output.buffer(&sample("http_req_duration", 12.5, &[("status", "200")]));
        let lines = output.buffer.lock().unwrap().clone();
        assert_eq!(lines[0], "http_reqs,status=200 value=1i");
        assert_eq!(lines[1], "http_req_duration,status=200 value=12.5");
    }

    #[test]
    fn escapes_special_chars() {
        let output = InfluxdbOutput::new("127.0.0.1:8089").unwrap();
        output.buffer(&sample("http reqs", 1.0, &[("method=GET", "a,b")]));
        let line = output.buffer.lock().unwrap().first().unwrap().clone();
        assert!(line.starts_with("http\\ reqs,"), "metric escaped: {line}");
        assert!(line.contains("method\\=GET=a\\,b"), "tags escaped: {line}");
    }

    #[test]
    fn rejects_bad_address() {
        assert!(InfluxdbOutput::new("not-an-addr").is_err());
    }

    #[test]
    fn parses_http_v2_target() {
        let output =
            InfluxdbOutput::new("http://localhost:8086?org=myorg&bucket=mybucket&token=sekret")
                .unwrap();
        match &output.target {
            InfluxTarget::Http {
                base,
                org,
                bucket,
                token,
                db,
                user,
                password,
            } => {
                assert_eq!(base, "http://localhost:8086");
                assert_eq!(org.as_deref(), Some("myorg"));
                assert_eq!(bucket.as_deref(), Some("mybucket"));
                assert_eq!(token.as_deref(), Some("sekret"));
                assert!(db.is_none());
                assert!(user.is_none() && password.is_none());
            }
            other => panic!("expected HTTP target, got {other:?}"),
        }
    }

    #[test]
    fn parses_v1_basic_auth_from_userinfo() {
        // Backlog line 154: `http://user:pass@host:8086/db` must carry the
        // credentials for the v1 `Authorization: Basic` header.
        let output = InfluxdbOutput::new("http://admin:s3cret@localhost:8086/k6db").unwrap();
        match &output.target {
            InfluxTarget::Http {
                base,
                db,
                user,
                password,
                ..
            } => {
                assert_eq!(base, "http://localhost:8086");
                assert_eq!(db.as_deref(), Some("k6db"));
                assert_eq!(user.as_deref(), Some("admin"));
                assert_eq!(password.as_deref(), Some("s3cret"));
            }
            other => panic!("expected HTTP target, got {other:?}"),
        }
    }

    #[test]
    fn parses_http_v1_target_from_path() {
        let output = InfluxdbOutput::new("http://localhost:8086/k6db").unwrap();
        match &output.target {
            InfluxTarget::Http { base, db, .. } => {
                assert_eq!(base, "http://localhost:8086");
                assert_eq!(db.as_deref(), Some("k6db"));
            }
            other => panic!("expected HTTP target, got {other:?}"),
        }
    }

    #[test]
    fn http_lines_carry_ns_timestamp_udp_lines_do_not() {
        // HTTP lines append a ns timestamp; UDP lines stay bare.
        let http = InfluxdbOutput::new("http://localhost:8086?org=o&bucket=b").unwrap();
        http.buffer(&sample("http_reqs", 1.0, &[]));
        let line = http.buffer.lock().unwrap().first().unwrap().clone();
        // line = "http_reqs value=1i <ns>" — exactly 3 whitespace tokens and
        // the final token is all digits (the ns timestamp).
        let tokens: Vec<&str> = line.split_whitespace().collect();
        assert_eq!(tokens.len(), 3, "metric tags value ns: {line}");
        assert!(
            tokens[2].chars().all(|c| c.is_ascii_digit()),
            "ns timestamp must be numeric: {line}"
        );

        let udp = InfluxdbOutput::new("127.0.0.1:8089").unwrap();
        udp.buffer(&sample("http_reqs", 1.0, &[]));
        let line = udp.buffer.lock().unwrap().first().unwrap().clone();
        assert_eq!(
            line, "http_reqs value=1i",
            "UDP line has no timestamp: {line}"
        );
    }

    /// End-to-end: send to a live UDP socket and verify the datagram.
    #[tokio::test]
    async fn flush_sends_datagram() {
        use tokio::net::UdpSocket;

        let receiver = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let addr = receiver.local_addr().unwrap();

        let output = InfluxdbOutput::new(addr.to_string()).unwrap();
        output
            .sample(&[sample("http_reqs", 1.0, &[("status", "200")])])
            .await
            .unwrap();
        output.stop().await.unwrap();

        let mut buf = [0u8; 1024];
        let (n, _from) = receiver.recv_from(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf[..n]);
        assert_eq!(text, "http_reqs,status=200 value=1i");
    }
}
