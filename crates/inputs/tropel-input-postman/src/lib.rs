//! # tropel-input-postman
//!
//! Input adapter that reads Postman Collection v2.1/v2.0 files and
//! produces a protocol-agnostic `Scenario`.

use tropel_collection::{collection_to_scenario, parse_collection};
use tropel_core::scenario::Scenario;
use tropel_core::Result;
use tropel_core::TropelError;
use tropel_ext::traits::{InputAdapter, InputAdapterRegistration};

/// Input adapter for Postman Collection files.
pub struct PostmanInputAdapter;

// Register PostmanInputAdapter for compile-time discovery by the engine.
// When `tropel-ext` calls `ExtensionRegistry::collect_inventory()`, this
// registration is picked up and the adapter is added to the registry.
// Uses a fn pointer (captureless closure) for const-compatibility with inventory.
inventory::submit!(InputAdapterRegistration::new("postman", || Box::new(PostmanInputAdapter)));

impl InputAdapter for PostmanInputAdapter {
    fn id(&self) -> &str {
        "postman"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        // Structural detection: a Postman Collection is a JSON document
        // whose top-level `info.schema` points at the getpostman.com
        // collection schema. Substring matching is forbidden — a HAR or
        // any document may legitimately contain the words "postman" /
        // "collection" in embedded content (e.g. a Google-search capture
        // of getpostman.com pages) and must NOT be mis-detected.
        let Ok(value) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            return false;
        };
        let schema = value
            .get("info")
            .and_then(|info| info.get("schema"))
            .and_then(|s| s.as_str())
            .unwrap_or("");
        schema.contains("getpostman.com") && schema.contains("collection")
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let collection = parse_collection(bytes)
            .map_err(|e| TropelError::Parse(format!("Failed to parse Postman collection: {}", e)))?;

        Ok(collection_to_scenario(collection, std::collections::HashMap::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_postman() {
        let adapter = PostmanInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_no_postman() {
        let adapter = PostmanInputAdapter;
        let data = br#"{"info":{"name":"Test"}}"#;
        assert!(!adapter.detect(data));
    }

    #[test]
    fn test_detect_har_not_postman() {
        // Regression: a HAR whose embedded JS bundles contain the words
        // "postman" and "collection" must NOT be detected as a Postman
        // collection — substring matching mis-classified it before.
        let adapter = PostmanInputAdapter;
        let data = br#"{
            "log": {
                "version": "1.2",
                "entries": [{
                    "request": {"method": "GET", "url": "https://www.google.com/search?q=postman+collection", "headers": [], "queryString": []},
                    "response": {"status": 200, "statusText": "OK"}
                }]
            }
        }"#;
        assert!(!adapter.detect(data), "HAR content mentioning postman must not be detected as a Postman collection");
    }

    #[test]
    fn test_detect_requires_schema_url() {
        // The schema field must be the actual getpostman.com URL.
        let adapter = PostmanInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://example.com/collection.json"},"item":[]}"#;
        assert!(!adapter.detect(data));
    }

    #[test]
    fn test_parse_simple() {
        let adapter = PostmanInputAdapter;
        let data = br#"{
            "info": {
                "name": "Test Collection",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": [
                {
                    "name": "GET Users",
                    "request": {
                        "method": "GET",
                        "url": {"raw": "https://api.example.com/users"}
                    }
                }
            ]
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.info.name, "Test Collection");
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].name, "GET Users");
    }
}
