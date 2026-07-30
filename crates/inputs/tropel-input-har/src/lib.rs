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
//! | `entry.request.url` | `request.url` |
//! | `entry.request.method` | `request.method` |
//! | `entry.request.headers` | `request.headers` (flattened to key-value) |
//! | `entry.request.postData` | `request.body` |
//! | `entry.request.queryString` | `request.query_params` |

use std::collections::HashMap;
use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_core::types::{Body, Method, Request};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::{InputAdapter, InputAdapterRegistration};
use serde::Deserialize;

// ── HAR data model (minimal — only what we need) ────────────────

/// Top-level HAR structure.
#[derive(Debug, Deserialize)]
struct HarRoot {
    log: HarLog,
}

#[derive(Debug, Deserialize)]
struct HarLog {
    entries: Vec<HarEntry>,
}

#[derive(Debug, Deserialize)]
struct HarEntry {
    request: HarRequest,
    #[allow(dead_code)]
    response: HarResponse,
    #[serde(default)]
    pageref: Option<String>,
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
}

#[derive(Debug, Deserialize)]
struct HarHeader {
    name: String,
    value: String,
}

#[derive(Debug, Deserialize)]
struct HarQueryParam {
    name: String,
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
        if let Ok(text) = std::str::from_utf8(bytes) {
            let text = text.trim_start();
            if text.starts_with('{') {
                text.contains("\"log\"") && text.contains("\"entries\"")
                    && !text.contains("postman")
            } else {
                false
            }
        } else {
            false
        }
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let root: HarRoot = serde_json::from_slice(bytes)
            .map_err(|e| TropelError::Parse(format!("Failed to parse HAR file: {}", e)))?;

        let entries = root.log.entries;
        if entries.is_empty() {
            return Err(TropelError::Parse("HAR file contains no entries".into()));
        }

        let scenario_name = entries.first()
            .and_then(|e| e.pageref.as_deref())
            .unwrap_or("har-export")
            .to_string();

        let items: Vec<ScenarioItem> = entries.into_iter()
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

/// Convert a HAR entry to a ScenarioItem.
fn har_entry_to_item(entry: HarEntry, index: usize) -> ScenarioItem {
    let method = Method::from_str(&entry.request.method).unwrap_or(Method::GET);
    let url = entry.request.url.clone();

    let item_name = generate_item_name(&url, index);

    let headers: HashMap<String, String> = entry.request.headers.into_iter()
        .map(|h| (h.name, h.value))
        .collect();

    let query_params: HashMap<String, String> = entry.request.query_string.into_iter()
        .map(|q| (q.name, q.value))
        .collect();

    let body = entry.request.post_data.map(|pd| {
        let mime = pd.mime_type.to_lowercase();
        if mime.contains("json") {
            // Parse JSON text into serde_json::Value for Body::Json
            let json_val = serde_json::from_str(&pd.text)
                .unwrap_or(serde_json::Value::String(pd.text));
            Body::Json(json_val)
        } else if mime.contains("x-www-form-urlencoded") {
            Body::UrlEncoded(
                pd.params.into_iter()
                    .map(|p| (p.name, p.value))
                    .collect()
            )
        } else if mime.contains("form-data") || mime.contains("multipart") {
            Body::FormData(
                pd.params.into_iter()
                    .map(|p| (p.name, p.value))
                    .collect()
            )
        } else {
            Body::Raw(pd.text)
        }
    });

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

    #[test]
    fn test_detect_har() {
        let adapter = HarInputAdapter;
        let data = br#"{"log":{"version":"1.2","entries":[]}}"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_postman_not_har() {
        let adapter = HarInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(!adapter.detect(data), "Postman JSON should not be detected as HAR");
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
