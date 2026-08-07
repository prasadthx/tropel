//! # tropel-input-k6
//!
//! Input adapter + Driver for k6-style JavaScript/TypeScript test scripts.
//!
//! Provides two entry points:
//! - **InputAdapter** (declarative): wraps transpiled JS as a single-item
//!   Scenario, for backward compatibility.
//! - **Driver** (imperative): creates per-VU JS contexts, bootstraps shims,
//!   and runs the user's exported default function per iteration.
//!
//! The Driver is tried first by the engine's input resolution. The InputAdapter
//! serves as a fallback for older execution paths.

pub mod driver;
mod options;

use std::path::Path;
use tropel_sdk::{InputAdapter, InputAdapterRegistration};
use tropel_sdk::{Result, TropelError};
use tropel_sdk::{Scenario, ScenarioInfo, ScenarioItem};

/// Input adapter for k6-style JS/TS test scripts.
pub struct K6ScriptAdapter;

impl InputAdapter for K6ScriptAdapter {
    fn id(&self) -> &str {
        "k6"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        if let Ok(text) = std::str::from_utf8(bytes) {
            // Detect by looking for k6-like patterns:
            // - `export default function` (k6 default export)
            // - `import { ... } from "k6/..."` (k6 module import)
            // - Common test patterns like `http.get`, `check`, `group`
            //
            // We're lenient here — just check for JS/TS source characteristics
            // that wouldn't be a Postman collection or HAR file.
            let looks_like_collection = text.contains("postman") || text.contains("\"item\"");
            if looks_like_collection {
                return false;
            }

            let has_export_default = text.contains("export default");
            let has_k6_import = text.contains("from \"k6/") || text.contains("from 'k6/");
            let has_test_patterns = text.contains("http.get")
                || text.contains("http.post")
                || text.contains("check(")
                || text.contains("group(");

            has_export_default || has_k6_import || has_test_patterns
        } else {
            false
        }
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        // Without a file path, treat as a plain JS file.
        // TypeScript and ESM imports won't be resolved, but plain JS works.
        let _source = std::str::from_utf8(bytes)
            .map_err(|_| TropelError::Parse("k6 script is not valid UTF-8".into()))?;
        build_scenario_from_source(_source, "script")
    }

    fn parse_with_path(&self, bytes: &[u8], source_path: Option<&Path>) -> Result<Scenario> {
        // Validate UTF-8
        std::str::from_utf8(bytes)
            .map_err(|_| TropelError::Parse("k6 script is not valid UTF-8".into()))?;

        let path = source_path.unwrap_or(Path::new("script.js"));
        let name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("script")
            .to_string();

        let js_code = tropel_es::transpile_file(path)
            .map_err(|e| TropelError::Parse(format!("k6 script transpilation failed: {}", e)))?;

        build_scenario_from_source(&js_code, &name)
    }
}

/// Build a Scenario from transpiled JS source code.
fn build_scenario_from_source(js_code: &str, name: &str) -> Result<Scenario> {
    // Wrap the transpiled code in a self-executing function so it runs
    // regardless of request execution status.
    let wrapped_code = format!(
        "(function() {{\n{}    }})();",
        js_code
            .lines()
            .map(|l| format!("    {}", l))
            .collect::<Vec<_>>()
            .join("\n")
    );

    Ok(Scenario {
        info: ScenarioInfo {
            name: name.to_string(),
            description: Some(format!("k6 script: {}", name)),
            schema: None,
        },
        items: vec![ScenarioItem {
            id: format!("k6-script-{}", name),
            name: name.to_string(),
            request: None,
            prerequest: None,
            test: Some(wrapped_code),
            assertions: vec![],
            items: vec![],
        }],
        variables: std::collections::HashMap::new(),
        auth: None,
    })
}

// Register K6ScriptAdapter for compile-time discovery by the engine.
inventory::submit!(
    InputAdapterRegistration::new("k6", || Box::new(K6ScriptAdapter)).with_priority(10)
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_k6_export_default() {
        let adapter = K6ScriptAdapter;
        let data = br#"export default function() { http.get("https://example.com"); }"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_k6_import() {
        let adapter = K6ScriptAdapter;
        let data = br#"import { check } from "k6"; export default function() {}"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_postman_not_k6() {
        let adapter = K6ScriptAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(
            !adapter.detect(data),
            "Postman JSON should not be detected as k6"
        );
    }

    #[test]
    fn test_detect_plain_js_http() {
        let adapter = K6ScriptAdapter;
        let data = br#"http.get("https://example.com");"#;
        assert!(adapter.detect(data));
    }
}
