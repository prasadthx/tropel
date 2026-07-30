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
        // Check for Postman Collection schema indicator
        if let Ok(text) = std::str::from_utf8(bytes) {
            text.contains("postman") && text.contains("collection")
        } else {
            false
        }
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
