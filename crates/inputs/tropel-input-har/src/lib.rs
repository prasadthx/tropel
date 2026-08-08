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
//! | `entry.request.headers` | `request.headers` (duplicates combined with `, `; `Cookie` joins with `; ` per RFC 6265) |
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

use base64::Engine;
use serde::Deserialize;
use std::collections::HashMap;
use tropel_sdk::{Body, Method, Request};
use tropel_sdk::{InputAdapter, InputAdapterRegistration};
use tropel_sdk::{Result, TropelError};
use tropel_sdk::{Scenario, ScenarioInfo, ScenarioItem};

// ── HAR data model (minimal — only what we need) ────────────────

/// Top-level HAR structure.
#[derive(Debug, Deserialize)]
struct HarRoot {
    log: HarLog,
}

#[derive(Debug, Deserialize)]
struct HarLog {
    // `version` is parsed for spec fidelity but not consumed downstream
    // (detect() checks it structurally on the raw JSON).
    #[serde(default)]
    #[allow(dead_code)]
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

inventory::submit!(
    InputAdapterRegistration::new("har", || Box::new(HarInputAdapter)).with_priority(30)
);

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
        log.get("version").is_some() && log.get("entries").map(|e| e.is_array()).unwrap_or(false)
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let root: HarRoot = serde_json::from_slice(bytes)
            .map_err(|e| TropelError::Parse(format!("Failed to parse HAR file: {}", e)))?;

        if root.log.entries.is_empty() {
            return Err(TropelError::Parse("HAR file contains no entries".into()));
        }

        // Drop static assets / unsupported URIs that would error or pollute
        // the load test (Chrome HARs record every image/CSS/script).
        //
        // One exception: the first 2xx `text/html` response is treated as the
        // primary document and kept, even in `_resourceType`-less HARs
        // (Firefox/Charles) where it would otherwise be filtered — dropping
        // the document would leave a page-load workload with nothing to
        // replay. Redirects/error pages never claim the slot, and all
        // subsequent HTML (iframes, sub-navigations, error pages) is filtered
        // as static by is_static_resource.
        let mut html_kept = false;
        let entries: Vec<HarEntry> = root
            .log
            .entries
            .into_iter()
            .filter(|e| {
                let mime = e.response.content.mime_type.to_lowercase();
                // Only a 2xx HTML response can claim the document slot — a
                // 3xx redirect or 4xx/5xx error page with an HTML body must
                // not consume it, or the real document that follows would be
                // filtered out. All OTHER text/html (subsequent 2xx documents,
                // redirects, error pages) is filtered as static by
                // is_static_resource below.
                let is_2xx_html =
                    mime.starts_with("text/html") && (200..300).contains(&e.response.status);
                if is_2xx_html && !html_kept {
                    html_kept = true;
                    return true;
                }
                !is_static_resource(e)
            })
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

        // `Result` here is the tropel-sdk alias (1 generic: the error type),
        // so collect with the single-generic turbofish.
        let items: Vec<ScenarioItem> = entries
            .into_iter()
            .enumerate()
            .map(|(i, entry)| har_entry_to_item(entry, i))
            .collect::<Result<Vec<_>>>()?;

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

/// Should this HAR entry be dropped as a static asset or non-HTTP scheme?
///
/// Unsupported schemes are dropped unconditionally — reqwest refuses to send
/// `data:`/`blob:` URIs (they'd surface as per-request errors), and
/// `about:`/`javascript:`/`chrome-extension:`/`file:`/`ws:`/`wss:` aren't
/// plain HTTP requests at all (WebSocket handshakes record as HTTP 101
/// upgrades, which a plain GET would never reproduce).
///
/// Everything else is classified by Chrome's `_resourceType` when present,
/// falling back to the response content-type. The MIME fallback is expanded
/// to catch JavaScript and HTML payloads, so non-Chrome HARs (Firefox /
/// Charles — no `_resourceType`) still drop static assets instead of
/// replaying them as bogus API calls.
fn is_static_resource(entry: &HarEntry) -> bool {
    let url = entry.request.url.to_lowercase();
    const UNSUPPORTED_SCHEMES: [&str; 8] = [
        "data:",
        "blob:",
        "about:",
        "javascript:",
        "chrome-extension:",
        "file:",
        "ws:",
        "wss:",
    ];
    if UNSUPPORTED_SCHEMES.iter().any(|s| url.starts_with(s)) {
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
    // text/html IS filtered here — the primary document is preserved by
    // parse() as a single exception (first 2xx HTML entry); everything else
    // with an HTML body (redirects, error pages, sub-navigations) is static.
    mime.starts_with("text/html") // also matches text/html; charset=...
        || mime.starts_with("image/")
        || mime.starts_with("font/")
        || mime.starts_with("video/")
        || mime.starts_with("audio/")
        || mime.starts_with("text/css") // also matches text/css; charset=...
        || mime.starts_with("text/javascript")
        || mime.starts_with("application/javascript")
        || mime.starts_with("application/x-javascript")
        || mime.starts_with("application/ecmascript")
        || mime.starts_with("application/wasm")
        || mime.starts_with("application/font-woff")
        || mime.starts_with("application/x-font")
        || mime.starts_with("application/manifest+json")
}

/// Convert a HAR entry to a ScenarioItem.
///
/// Returns `Result` so a genuinely invalid method token (empty, whitespace
/// inside, non-token chars) fails the whole parse loudly instead of
/// silently becoming GET and "testing" the read path.
fn har_entry_to_item(entry: HarEntry, index: usize) -> Result<ScenarioItem> {
    let method = Method::parse(&entry.request.method).ok_or_else(|| {
        TropelError::Parse(format!(
            "HAR entry #{} has invalid HTTP method {:?}",
            index + 1,
            entry.request.method
        ))
    })?;
    let url = entry.request.url.clone();

    let item_name = generate_item_name(&url, index);

    let headers = merge_headers(entry.request.headers.into_iter().map(|h| (h.name, h.value)));

    // The recorded URL already carries the query string. Populating
    // query_params as well would make the HTTP layer re-append it
    // (→ `?x=1&x=1`). Only populate query_params for HARs whose URL lacks a
    // query entirely.
    //
    // If the URL has no `?` AND the queryString contains DUPLICATE keys, a
    // HashMap cannot represent them (merge_pairs would collapse `a=1&a=2`
    // into `a=1, 2`) — instead fold the raw query string into the URL,
    // preserving order and duplicates, and keep query_params empty.
    let mut url = entry.request.url.clone();
    let query_params = if url.contains('?') {
        HashMap::new()
    } else {
        let pairs: Vec<(String, String)> = entry
            .request
            .query_string
            .iter()
            .map(|q| (q.name.clone(), q.value.clone()))
            .collect();
        if has_duplicate_keys(&pairs) {
            let qs: Vec<String> = pairs.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
            url.push('?');
            url.push_str(&qs.join("&"));
            HashMap::new()
        } else {
            merge_pairs(pairs.into_iter())
        }
    };

    let body = entry.request.post_data.map(build_body);

    Ok(ScenarioItem {
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
            response_type: tropel_sdk::ResponseType::Text,
        }),
        prerequest: None,
        test: None,
        assertions: vec![],
        items: vec![],
    })
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
        // Parse JSON text into serde_json::Value for Body::Json. If the text
        // is NOT valid JSON (a browser may record a text/plain-ish body under
        // a *json* mime), fall back to Body::Raw so the body is sent verbatim
        // — wrapping it as Value::String would re-quote it on the wire
        // (`hello` → `"hello"`), changing the payload.
        match serde_json::from_str(&pd.text) {
            Ok(v) => Body::Json(v),
            Err(_) => Body::Raw(pd.text),
        }
    } else if mime.contains("x-www-form-urlencoded") {
        if has_text {
            // Faithful replay: the encoded body as recorded (content-type
            // header is preserved from the HAR headers).
            Body::Raw(pd.text)
        } else {
            Body::UrlEncoded(pd.params.into_iter().map(|p| (p.name, p.value)).collect())
        }
    } else if mime.contains("form-data") || mime.contains("multipart") {
        if has_text {
            // Raw multipart body (with its boundary) — re-encoding would
            // corrupt it. Content-Type header with boundary is preserved.
            Body::Raw(pd.text)
        } else {
            Body::FormData(pd.params.into_iter().map(|p| (p.name, p.value)).collect())
        }
    } else {
        Body::Raw(pd.text)
    }
}

/// Combine duplicate keys by appending values with `, ` (RFC 9110 allows
/// combining field lines) instead of silently dropping data.
/// True if any query key appears more than once (order-preserving duplicate
/// detection — used to decide whether to fold the query into the URL).
fn has_duplicate_keys(pairs: &[(String, String)]) -> bool {
    let mut seen = std::collections::HashSet::new();
    pairs.iter().any(|(k, _)| !seen.insert(k.clone()))
}

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

/// Combine duplicate header lines into one value per name.
///
/// Most headers join with `, ` (RFC 9110 field-line combination), but the
/// `Cookie` header MUST join with `; ` per RFC 6265 §5.4 — a `, `-joined
/// value is a single cookie with a comma in it, not two cookies, and servers
/// (and the request's own cookie jar) would mis-read it. `Cookie` matching is
/// case-insensitive per HTTP semantics.
fn merge_headers<I: Iterator<Item = (String, String)>>(pairs: I) -> HashMap<String, String> {
    let mut map: HashMap<String, String> = HashMap::new();
    for (k, v) in pairs {
        let is_cookie = k.eq_ignore_ascii_case("cookie");
        match map.get_mut(&k) {
            Some(existing) => {
                existing.push_str(if is_cookie { "; " } else { ", " });
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
            if let Some(last_seg) = path.rsplit('/').find(|s: &&str| !s.is_empty()) {
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
        assert!(
            !adapter.detect(data),
            "Postman JSON should not be detected as HAR"
        );
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
        assert!(
            adapter.detect(data),
            "HAR with 'postman'/'collection' words in content must still be detected as HAR"
        );
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
        assert_eq!(
            scenario.items[0].request.as_ref().unwrap().url,
            "https://api.example.com/users/123"
        );
        assert_eq!(
            scenario.items[0].request.as_ref().unwrap().method,
            Method::GET
        );
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
            Body::Json(_) => {} // valid JSON was parsed
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
    fn test_non_json_text_under_json_mime_sent_verbatim() {
        // A browser may record a non-JSON body under a *json* mime (e.g. a
        // stale Content-Type header). Wrapping it as Body::Json(Value::String)
        // would re-quote the text on the wire (`hello` → `"hello"`). It must
        // be sent verbatim as Body::Raw.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "POST",
                            "url": "https://api.example.com/echo",
                            "headers": [],
                            "queryString": [],
                            "postData": {
                                "mimeType": "application/json",
                                "text": "this is not json, just text"
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
            Body::Raw(t) => assert_eq!(t, "this is not json, just text"),
            other => panic!("Expected Body::Raw verbatim, got {:?}", other),
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
        assert!(
            req.query_params.is_empty(),
            "query_params must be empty when URL already has a query"
        );
        assert_eq!(req.url, "https://api.example.com/users?limit=10&page=2");
    }

    #[test]
    fn test_dup_query_keys_folded_into_url() {
        // URL has no query AND queryString has duplicate keys — a HashMap
        // can't hold them, so the raw query string is folded into the URL
        // preserving order and duplicates (`?tag=a&tag=b`), not collapsed.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {
                            "method": "GET",
                            "url": "https://api.example.com/search",
                            "headers": [],
                            "queryString": [
                                {"name": "tag", "value": "a"},
                                {"name": "tag", "value": "b"},
                                {"name": "sort", "value": "asc"}
                            ]
                        },
                        "response": {"status": 200, "statusText": "OK"}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(
            req.url, "https://api.example.com/search?tag=a&tag=b&sort=asc",
            "duplicate query keys must be preserved in the URL"
        );
        assert!(
            req.query_params.is_empty(),
            "query_params must stay empty when the query is folded into the URL"
        );
    }

    #[test]
    fn test_duplicate_cookie_headers_joined_with_semicolon() {
        // RFC 6265 §5.4: duplicate Cookie headers must join with `; `, not
        // `, ` — a comma-joined value would read as ONE cookie containing a
        // comma, not two cookies. Matching is case-insensitive.
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
                                {"name": "Cookie", "value": "session=abc"},
                                {"name": "Cookie", "value": "theme=dark"},
                                {"name": "X-Trace", "value": "a"},
                                {"name": "X-Trace", "value": "b"}
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
        assert_eq!(
            req.headers.get("Cookie").unwrap(),
            "session=abc; theme=dark"
        );
        // Non-Cookie duplicates still join with `, `.
        assert_eq!(req.headers.get("X-Trace").unwrap(), "a, b");
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
        assert_eq!(
            scenario.items[0].request.as_ref().unwrap().url,
            "https://api.example.com/users"
        );
    }

    #[test]
    fn test_blob_and_non_chrome_mimes_filtered() {
        // blob: URIs and JS/HTML payloads must be dropped even when the HAR
        // has no `_resourceType` (Firefox/Charles exports) — the MIME fallback
        // catches them, so they aren't replayed as bogus API calls.
        let adapter = HarInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [
                    {
                        "request": {"method": "GET", "url": "blob:https://app.example.com/uuid", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK"}
                    },
                    {
                        "request": {"method": "GET", "url": "wss://app.example.com/socket", "headers": [], "queryString": []},
                        "response": {"status": 101, "statusText": "Switching Protocols"}
                    },
                    {
                        "request": {"method": "GET", "url": "https://app.example.com/bundle.js", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK", "content": {"mimeType": "application/javascript"}}
                    },
                    {
                        "request": {"method": "GET", "url": "https://app.example.com/", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK", "content": {"mimeType": "text/html"}}
                    },
                    {
                        "request": {"method": "GET", "url": "https://api.example.com/orders", "headers": [], "queryString": []},
                        "response": {"status": 200, "statusText": "OK", "content": {"mimeType": "application/json"}}
                    }
                ]
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        // blob / wss / JS are filtered; the first text/html (the document) is
        // preserved, plus the API JSON entry → 2 survivors.
        assert_eq!(
            scenario.items.len(),
            2,
            "document HTML + API JSON should survive, got {:?}",
            scenario
                .items
                .iter()
                .map(|i| i
                    .request
                    .as_ref()
                    .map(|r| r.url.clone())
                    .unwrap_or_default())
                .collect::<Vec<_>>()
        );
        let urls: Vec<&str> = scenario
            .items
            .iter()
            .filter_map(|i| i.request.as_ref().map(|r| r.url.as_str()))
            .collect();
        assert!(urls.contains(&"https://app.example.com/"));
        assert!(urls.contains(&"https://api.example.com/orders"));
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
        assert_eq!(
            scenario.items[0].request.as_ref().unwrap().method,
            Method::GET
        );
        assert_eq!(
            scenario.items[1].request.as_ref().unwrap().method,
            Method::POST
        );
        assert_eq!(
            scenario.items[2].request.as_ref().unwrap().method,
            Method::DELETE
        );
    }
}
