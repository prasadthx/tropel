//! # tropel-input-har
//!
//! Input adapter that reads [HTTP Archive (HAR)][har] files and produces a
//! protocol-agnostic `Scenario`. HAR is the standard format for exporting
//! browser network logs and is supported by Chrome DevTools, Firefox,
//! Charles, Fiddler, and most API clients.
//!
//! [har]: https://w3c.github.io/web-performance/specs/HAR/Overview.html
//!
//! ## Mapping
//!
//! Each HAR `entry` (request+response pair) becomes one `ScenarioItem`:
//!
//! | HAR field | Scenario field |
//! |-----------|---------------|
//! | `entry.request.url` | `request.url` (kept verbatim — already contains the query string) |
//! | `entry.request.method` | `request.method` |
//! | `entry.request.headers` | `request.headers` (duplicates combined with `, `) |
//! | `entry.request.postData.text` | `request.body` (preferred over `params`; base64 decoded when `encoding` is set) |
//!
//! ## Resource filtering
//!
//! Browsers record every asset they load, but a load test only wants the
//! API traffic. Entries that replay uselessly or break the runner are
//! dropped: `data:` URIs (reqwest rejects them), and static assets
//! (images, fonts, stylesheets, media, manifests, beacons, scripts) as
//! classified by Chrome's `_resourceType` field or the response
//! `content.mimeType`.

use std::collections::HashMap;
use base64::Engine;
use tropel_sdk::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_sdk::{Body, Method, Request};
use tropel_sdk::{Result, TropelError};
use tropel_sdk::{InputAdapter, InputAdapterRegistration};
use serde::Deserialize;

// ── HAR data model (minimal — only what we need) ────────────────

/// Top-level HAR structure.
#[derive(Debug, Deserialize)]
struct HarRoot {
    log: HarLog,
}

#[derive(Debug, Deserialize)]
struct HarLog {
    #[serde(default)]
    version: Option<String>,
    entries: Vec<HarEntry>,
}

#[derive(Debug, Deserialize)]
struct HarEntry {
    request: HarRequest,
    /// Partial HAR exports may omit the response — tolerate that.
    #[serde(default)]
    response: HarResponse,
    #[serde(default)]
    pageref: Option<String>,
    /// Chrome DevTools extension field classifying the resource
    /// (document, script, stylesheet, image, font, ping, fetch, xhr, ...).
    #[serde(default, rename = "_resourceType")]
    resource_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HarRequest {
    method: String,
    url: String,
    #[serde(default)]
    headers: Vec<HarHeader>,
    #[serde(default, rename = "queryString")]
    query_string: Vec<HarQueryParam>,
    #[serde(default, rename = "postData")]
    post_data: Option<HarPostData>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct HarResponse {
    status: u16,
    #[serde(rename = "statusText")]
    status_text: String,
    content: HarResponseContent,
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
struct HarResponseContent {
    #[serde(rename = "mimeType")]
    mime_type: String,
}

#[derive(Debug, Deserialize)]
struct HarHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct HarQueryParam {
    name: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct HarPostData {
    #[serde(default, rename = "mimeType")]
    mime_type: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    params: Vec<HarPostParam>,
    /// When present, `text` is base64-encoded (e.g. binary uploads).
    #[serde(default)]
    encoding: Option<String>,
}

#[derive(Debug, Deserialize)]
struct HarPostParam {
    name: String,
    #[serde(default)]
    value: String,
}

// ── InputAdapter implementation ─────────────────────────────────

/// Input adapter for HTTP Archive (HAR) files.
pub struct HarInputAdapter;

inventory::submit!(InputAdapterRegistration::new("har", || Box::new(HarInputAdapter)));

impl InputAdapter for HarInputAdapter {
    fn id(&self) -> &str {
        "har"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        // Structural detection: a HAR is a JSON document with a top-level
        // `log` object containing a `version` and an `entries` array.
        // Substring matching is forbidden — embedded content (JS bundles,
        // page text) may contain any word, including "log" or "postman".
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return false;
        };
        let log = match value.get("log") {
            Some(log) if log.is_object() => log,
            _ => return false,
        };
        log.get("version").is_some()
            && log.get("entries").map(|e| e.is_array()).unwrap_or(false)
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let root: HarRoot = serde_json::from_slice(bytes)
            .map_err(|e| TropelError::Parse(format!("Failed to parse HAR file: {}", e)))?;

        if root.log.entries.is_empty() {
            return Err(TropelError::Parse("HAR file contains no entries".into()));
        }

        // Drop static assets / unsupported URIs that would error or pollute
        // the load test (Chrome HARs record every image/CSS/script).
        let entries: Vec<HarEntry> = root
            .log
            .entries
            .into_iter()
            .filter(|e| !is_static_resource(e))
            .collect();

        if entries.is_empty() {
            return Err(TropelError::Parse(
                "HAR file contains only static resources; nothing to load-test".into(),
            ));
        }

        let scenario_name = entries
            .first()
            .and_then(|e| e.pageref.as_deref())
            .unwrap_or("har-export")
            .to_string();

        let items: Vec<ScenarioItem> = entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| har_entry_to_item(entry, i))
            .collect();

        Ok(Scenario {
            info: ScenarioInfo {
                name: scenario_name,
                description: Some("Imported from HTTP Archive (HAR)".into()),
                schema: None,
            },
            items,
            variables: HashMap::new(),
            auth: None,
        })
    }
}

/// Should this HAR entry be dropped as a static asset?
///
/// `data:` URIs are dropped unconditionally — reqwest refuses to send them
/// and they'd surface as per-request errors. Everything else is classified
/// by Chrome's `_resourceType` when present, falling back to the response
/// content-type.
fn is_static_resource(entry: &HarEntry) -> bool {
    let url = entry.request.url.to_lowercase();
    if url.starts_with("data:") {
        return true;
    }
    if let Some(rt) = &entry.resource_type {
        let rt = rt.to_lowercase();
        if matches!(
            rt.as_str(),
            "image" | "font" | "stylesheet" | "media" | "manifest" | "ping" | "script"
        ) {
            return true;
        }
    }
    let mime = entry.response.content.mime_type.to_lowercase();
    mime.starts_with("image/")
        || mime.starts_with("font/")
        || mime.starts_with("video/")
        || mime.starts_with("audio/")
        || mime.starts_with("text/css") // also matches text/css; charset=...
}

/// Convert a HAR entry to a ScenarioItem.
fn har_entry_to_item(entry: HarEntry, index: usize) -> ScenarioItem {
    let method = Method::from_str(&entry.request.method).unwrap_or(Method::GET);
    let url = entry.request.url.clone();

    let item_name = generate_item_name(&url, index);

    let headers = merge_pairs(entry.request.headers.into_iter().map(|h| (h.name, h.value)));

    // The recorded URL already carries the query string. Populating
    // query_params as well would make the HTTP layer re-append it
    // (→ `?x=1&x=1`). Only populate query_params for HARs whose URL lacks a
    // query entirely.
    let query_params = if url.contains('?') {
        HashMap::new()
    } else {
        merge_pairs(entry.request.query_string.into_iter().map(|q| (q.name, q.value)))
    };

    let body = entry.request.post_data.map(build_body);

    ScenarioItem {
        id: format!("har-item-{}", index),
        name: item_name,
        request: Some(Request {
            url,
            method,
            headers,
            query_params,
            body,
            auth: None,
            certificate: None,
            follow_redirects: true,
            timeout: None,
        }),
        prerequest: None,
        test: None,
        assertions: vec![],
        items: vec![],
    }
}

/// Build the request body from HAR `postData`.
///
/// Chrome puts the wire-format body in `postData.text` (and often leaves
/// `params` empty), so `text` is preferred. `params` is the structured
/// fallback for exporters that omit `text`.
fn build_body(pd: HarPostData) -> Body {
    // base64-encoded payload (binary uploads) → decode to bytes.
    if pd.encoding.as_deref() == Some("base64") {
        if let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(pd.text.as_bytes()) {
            return Body::Binary(bytes);
        }
    }

    let mime = pd.mime_type.to_lowercase();
    let has_text = !pd.text.trim().is_empty();

    if mime.contains("json") {
        // Parse JSON text into serde_json::Value for Body::Json
        let json_val = serde_json::from_str(&pd.text)
            .unwrap_or_else(|_| serde_json::Value::String(pd.text));
        Body::Json(json_val)
    } else if mime.contains("x-www-form-urlencoded") {
        if has_text {
            // Faithful replay: the encoded body as recorded (content-type
            // header is preserved from the HAR headers).
            Body::Raw(pd.text)
        } else {
            Body::UrlEncoded(
                pd.params
                    .into_iter()
                    .map(|p| (p.name, p.value))
                    .collect(),
            )
        }
    } else if mime.contains("form-data") || mime.contains("multipart") {
        if has_text {
            // Raw multipart body (with its boundary) — re-encoding would
            // corrupt it. Content-Type header with boundary is preserved.
            Body::Raw(pd.text)
        } else {
            Body::FormData(
                pd.params
                    .into_iter()
                    .map(|p| (p.name, p.value))
                    .collect(),
            )
        }
    } else {
        Body::Raw(pd.text)
    }
}

/// Combine duplicate keys by appending values with `, ` (RFC 9110 allows
/// combining field lines) instead of silently dropping data.
fn merge_pairs<I: Iterator<Item = (String, String)>>(pairs: I) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (k, v) in pairs {
        match map.get_mut(&k) {
            Some(existing) => {
                existing.push_str(", ");
                existing.push_str(&v);
            }
            None => {
                map.insert(k, v);
            }
        }
    }
    map
}

/// Generate a human-readable item name from a URL.
fn generate_item_name(url: &str, index: usize) -> String {
    // Try to extract the last meaningful path segment using basic string ops
    // (avoids pulling in the `url` crate dependency just for naming)
    if let Some(path_start) = url.find("://") {
        let after_scheme = &url[path_start + 3..];
        if let Some(path_pos) = after_scheme.find('/') {
            let path = &after_scheme[path_pos..];
            let path = path.trim_end_matches('/');
            if let Some(last_seg) = path.rsplit('/').filter(|s: &&str| !s.is_empty()).next() {
                return format!("request #{} ({})", index + 1, last_seg);
            }
        }
    }
    format!("request #{}", index + 1)
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn test_detect_har() {
        let adapter = HarInputAdapter;
        let data = br#"{"log":{"version":"1.2","entries":[]}}"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_requires_version() {
        // A JSON object with "log"/"entries" but no log.version is not HAR.
        let adapter = HarInputAdapter;
        let data = br#"{"log":{"entries":[]}}"#;
        assert!(!adapter.detect(data), "HAR detect must require log.version");
    }

    #[test]
    fn test_detect_postman_not_har() {
        let adapter = HarInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(!adapter.detect(data), "Postman JSON should not be detected as HAR");
    }

    #[test]
    fn test_detect_har_with_postman_word_in_content() {
        // Regression: a real-world HAR (e.g. a Google-search capture)
        // embeds JS bundles whose text contains the words "postman" and
        // "collection". Substring-based detect() mis-classified it as a
        // Postman collection; structural detection must not care.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "creator": {"name": "WebInspector", "version": "537.36"},
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://www.google.com/search?q=postman+collection",
                            "headers": [],
                            "queryString": []
                        },
                        "response": {"status": 200, "statusText": "OK"}
                    }
                ]
            }
        }"#;
        assert!(adapter.detect(data), "HAR with 'postman'/'collection' words in content must still be detected as HAR");
    }

    #[test]
    fn test_detect_random_json_not_har() {
        let adapter = HarInputAdapter;
        let data = br#"{"foo":"bar","baz":123}"#;
        assert!(!adapter.detect(data));
    }

    #[test]
    fn test_parse_simple_har() {
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://api.example.com/users/123",
                            "headers": [
                                {"name": "Accept", "value": "application/json"}
                            ],
                            "queryString": []
                        },
                        "response": {
                            "status": 200,
                            "statusText": "OK"
                        }
                    }
                ]
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].request.as_ref().unwrap().url, "https://api.example.com/users/123");
        assert_eq!(scenario.items[0].request.as_ref().unwrap().method, Method::GET);
    }

    #[test]
    fn test_partial_har_without_response_parses() {
        // Some exporters omit `response` — must not hard-fail.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://api.example.com/users/123",
                            "headers": [],
                            "queryString": []
                        }
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 1);
    }

    #[test]
    fn test_parse_with_body() {
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "POST",
                            "url": "https://api.example.com/data",
                            "headers": [],
                            "queryString": [],
                            "postData": {
                                "mimeType": "application/json",
                                "text": "{\"key\":\"value\"}"
                            }
                        },
                        "response": {
                            "status": 201,
                            "statusText": "Created"
                        }
                    }
                ]
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(req.body.is_some());
        match req.body.as_ref().unwrap() {
            Body::Json(_) => {}, // valid JSON was parsed
            other => panic!("Expected Body::Json, got {:?}", other),
        }
    }

    #[test]
    fn test_urlencoded_body_prefers_text() {
        // Chrome exports form submissions with `text` (wire format) and
        // often empty `params` — the body must not be lost.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "POST",
                            "url": "https://api.example.com/login",
                            "headers": [{"name": "Content-Type", "value": "application/x-www-form-urlencoded"}],
                            "queryString": [],
                            "postData": {
                                "mimeType": "application/x-www-form-urlencoded",
                                "text": "user=alice&pass=secret",
                                "params": []
                            }
                        },
                        "response": {"status": 200, "statusText": "OK"}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.body.as_ref().unwrap() {
            Body::Raw(t) => assert_eq!(t, "user=alice&pass=secret"),
            other => panic!("Expected Body::Raw with text, got {:?}", other),
        }
    }

    #[test]
    fn test_base64_postdata_decoded() {
        let adapter = HarInputAdapter;
        let encoded = base64::engine::general_purpose::STANDARD.encode("hello bytes");
        let data = format!(
            r#"{{"log":{{"version":"1.2","entries":[{{"request":{{"method":"POST","url":"https://api.example.com/upload","headers":[],"queryString":[],"postData":{{"mimeType":"application/octet-stream","encoding":"base64","text":"{}"}}}},"response":{{"status":200,"statusText":"OK"}}}}]}}}}"#,
            encoded
        );
        let scenario = adapter.parse(data.as_bytes()).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.body.as_ref().unwrap() {
            Body::Binary(b) => assert_eq!(b, b"hello bytes"),
            other => panic!("Expected Body::Binary, got {:?}", other),
        }
    }

    #[test]
    fn test_query_not_double_sent() {
        // URL already contains the query — query_params must stay empty so
        // the HTTP layer doesn't re-append it (→ `?x=1&x=1`).
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://api.example.com/users?limit=10&page=2",
                            "headers": [],
                            "queryString": [
                                {"name": "limit", "value": "10"},
                                {"name": "page", "value": "2"}
                            ]
                        },
                        "response": {"status": 200, "statusText": "OK"}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(req.query_params.is_empty(), "query_params must be empty when URL already has a query");
        assert_eq!(req.url, "https://api.example.com/users?limit=10&page=2");
    }

    #[test]
    fn test_duplicate_headers_merged() {
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://api.example.com/",
                            "headers": [
                                {"name": "X-Trace", "value": "a"},
                                {"name": "X-Trace", "value": "b"},
                                {"name": "Accept", "value": "*/*"}
                            ],
                            "queryString": []
                        },
                        "response": {"status": 200, "statusText": "OK"}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.headers.get("X-Trace").unwrap(), "a, b");
        assert_eq!(req.headers.get("Accept").unwrap(), "*/*");
    }

    #[test]
    fn test_static_resources_filtered() {
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {"method": "GET", "url": "data:image/png;base64,iVBORw0KGgo=", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK"}
                    },
                    {
                        "_resourceType": "image",
                        "request": {"method": "GET", "url": "https://cdn.example.com/logo.png", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK", "content": {"mimeType": "image/png"}}
                    },
                    {
                        "_resourceType": "xhr",
                        "request": {"method": "GET", "url": "https://api.example.com/users", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK", "content": {"mimeType": "application/json"}}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 1, "only the xhr entry should survive");
        assert_eq!(scenario.items[0].request.as_ref().unwrap().url, "https://api.example.com/users");
    }

    #[test]
    fn test_har_multiple_entries() {
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {"request": {"method": "GET", "url": "https://example.com/a", "headers": [], "queryString": []}, "response": {"status": 200, "statusText": "OK"}},
                    {"request": {"method": "POST", "url": "https://example.com/b", "headers": [], "queryString": []}, "response": {"status": 200, "statusText": "OK"}},
                    {"request": {"method": "DELETE", "url": "https://example.com/c", "headers": [], "queryString": []}, "response": {"status": 204, "statusText": "No Content"}}
                ]
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 3);
        assert_eq!(scenario.items[0].request.as_ref().unwrap().method, Method::GET);
        assert_eq!(scenario.items[1].request.as_ref().unwrap().method, Method::POST);
        assert_eq!(scenario.items[2].request.as_ref().unwrap().method, Method::DELETE);
    }
}
