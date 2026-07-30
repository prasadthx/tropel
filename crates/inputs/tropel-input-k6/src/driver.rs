//! # K6 Driver — imperative execution path for k6 scripts
//!
//! Implements `Driver` + `DriverInstance` traits to run k6-style JS/TS test
//! scripts natively through the engine's imperative input path.
//!
//! ## Flow
//!
//! 1. **Pre-process** the raw source to capture `export default function`
//!    as the global `__tropel_iteration`. Also remove k6 virtual imports
//!    (`import { … } from "k6/…"`) since those are provided by shim libraries.
//!
//! 2. **Transpile** (if `.ts` source): strip TypeScript type annotations via
//!    `tropel_es::typescript_to_javascript`. ES module bundling is NOT used
//!    for k6 scripts — their imports (k6/http, k6/metrics, etc.) are virtual
//!    module names that don't correspond to files on disk.
//!
//! 3. **Bootstrap**: create a `JsContext`, bootstrap shim libraries (pm-api,
//!    chai, lodash, cryptojs, exec, sleep), install native modules.
//!
//! 4. **Eval**: evaluate the pre-processed + transpiled source so
//!    `__tropel_iteration` becomes a callable global function.
//!
//! 5. **Run**: each call to `run_iteration()` invokes `__tropel_iteration()`
//!    and drains metrics/abort state from the `VuContext`.

use async_trait::async_trait;
use std::path::Path;
use std::time::{Duration, Instant};
use regex::Regex;
use tropel_core::{Result, TropelError};
use tropel_core::types::TagMap;
use tropel_ext::traits::{Driver, DriverInstance, DriverRegistration, VuContext};
use tropel_js::JsContext;

// ══════════════════════════════════════════════════════════════════
// K6Driver — the stateless factory
// ══════════════════════════════════════════════════════════════════

pub struct K6Driver;

#[async_trait]
impl Driver for K6Driver {
    fn id(&self) -> &str {
        "k6"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        if let Ok(text) = std::str::from_utf8(bytes) {
            // Reject Postman collections (handled by the Postman adapter)
            let looks_like_collection = text.contains("postman") || text.contains("\"item\"");
            if looks_like_collection {
                return false;
            }
            let has_export_default = text.contains("export default");
            let has_k6_import = text.contains("from \"k6/") || text.contains("from 'k6/");
            let has_test_patterns = text.contains("http.get") || text.contains("http.post")
                || text.contains("check(") || text.contains("group(");
            has_export_default || has_k6_import || has_test_patterns
        } else {
            false
        }
    }

    async fn init(&self, bytes: &[u8], source_path: Option<&Path>) -> Result<Box<dyn DriverInstance>> {
        let original = std::str::from_utf8(bytes)
            .map_err(|e| TropelError::Parse(format!("k6 script is not valid UTF-8: {}", e)))?;

        // Step 1: Pre-process — capture `export default function` as
        // `function __tropel_iteration` and remove k6 virtual imports.
        let preprocessed = preprocess_k6_source(original);

        // Step 2: Transpile TypeScript if needed
        let final_source = if let Some(path) = source_path {
            let ext = path.extension()
                .and_then(|e| e.to_str())
                .unwrap_or("js")
                .to_lowercase();
            if matches!(ext.as_str(), "ts" | "mts" | "tsx") {
                tropel_es::typescript_to_javascript(&preprocessed, &path.to_string_lossy())
                    .map_err(|e| TropelError::Parse(format!("TS transpile error: {}", e)))?
            } else {
                preprocessed
            }
        } else {
            // No path hint — detect TS patterns heuristically
            if preprocessed.contains(": string") || preprocessed.contains(": number")
                || preprocessed.contains(": boolean") || preprocessed.contains("interface ")
            {
                tropel_es::typescript_to_javascript(&preprocessed, "script.js")
                    .map_err(|e| TropelError::Parse(format!("TS transpile error: {}", e)))?
            } else {
                preprocessed
            }
        };

        // Step 3: Create JS context
        let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
            .await
            .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

        // Step 4: Bootstrap shim libraries & native modules
        bootstrap_js_libs(&js_ctx).await?;

        // Step 5: Eval the transpiled source
        js_ctx.eval(&final_source).await
            .map_err(|e| TropelError::Other(format!("Script eval error: {}", e)))?;

        // Verify __tropel_iteration was defined
        let has_iter = js_ctx.get_global("__tropel_iteration").await
            .unwrap_or(None);
        if has_iter.is_none() {
            tracing::warn!("k6 script did not define a default export function — __tropel_iteration is not set");
        }

        Ok(Box::new(K6DriverInstance {
            js_ctx,
            _source_path: source_path.map(|p| p.to_path_buf()),
        }))
    }
}

// Register K6Driver for compile-time discovery.
inventory::submit!(DriverRegistration::new("k6", || Box::new(K6Driver)));

// ══════════════════════════════════════════════════════════════════
// K6DriverInstance — per-iteration execution
// ══════════════════════════════════════════════════════════════════

pub struct K6DriverInstance {
    js_ctx: JsContext,
    _source_path: Option<std::path::PathBuf>,
}

// Safety: each DriverInstance runs on its own VU thread (thread-per-core).
// JsContext already has unsafe impl Send + Sync in tropel_js.
unsafe impl Send for K6DriverInstance {}
unsafe impl Sync for K6DriverInstance {}

#[async_trait]
impl DriverInstance for K6DriverInstance {
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()> {
        // Sync VuContext state into JS globals
        self.sync_globals(ctx).await?;

        // Call __tropel_iteration()
        let iter_start = Instant::now();

        match self.js_ctx.eval("__tropel_iteration()").await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("k6 iteration error: {}", e);
                // Non-fatal — log and continue
            }
        }

        let iter_dur = iter_start.elapsed();

        // Emit iteration_duration sample
        ctx.emit_sample(
            "iteration_duration",
            iter_dur.as_micros() as f64,
            TagMap::new(),
        );

        Ok(())
    }
}

impl K6DriverInstance {
    /// Sync VuContext state into JS globals so the script can read
    /// environment variables, data rows, etc.
    async fn sync_globals(&self, ctx: &VuContext) -> Result<()> {
        let _ = self.js_ctx.set_global_str("__tropel_vu_id", &ctx.vu_id.to_string()).await;
        let _ = self.js_ctx.set_global_str("__tropel_iteration_num", &ctx.iteration.to_string()).await;
        let _ = self.js_ctx.set_global_str("__tropel_scenario", &ctx.scenario_name).await;

        // Set env vars as JS global
        if !ctx.env.is_empty() {
            let _ = self.js_ctx.set_global_json("__tropel_env", &serde_json::to_value(&ctx.env).unwrap_or_default()).await;
        }

        // Set data row
        if let Some(ref row) = ctx.data_row {
            let _ = self.js_ctx.set_global_json("__tropel_data_row", &serde_json::to_value(row).unwrap_or_default()).await;
        }

        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════
// Source pre-processing
// ══════════════════════════════════════════════════════════════════

/// Pre-process a k6 source string before transpilation and evaluation.
///
/// 1. Captures `export default function(name) { … }` / `export default function(…) { … }`
///    as the global `function __tropel_iteration(…) { … }`.
/// 2. Captures `export default () => expr` / `export default () => { … }`
///    as `var __tropel_iteration = () => expr`.
/// 3. Removes k6 virtual import lines (`import … from "k6/…"`).
/// 4. Removes `export const options = …` — keeps the `const options = …` part.
///    (The transpiler's `remove_exports` does this too, but we do it here as well
///     for the path-less case where the transpiler isn't invoked.)
fn preprocess_k6_source(source: &str) -> String {
    let mut result = source.to_string();

    // 1a. `export default function name(...)` → `function __tropel_iteration(...)`
    let re_named = Regex::new(r"\bexport\s+default\s+function\s+\w+\s*\(").unwrap();
    result = re_named.replace_all(&result, "function __tropel_iteration(").to_string();

    // 1b. `export default function(` (anonymous) → `function __tropel_iteration(`
    //     Only if not already replaced above (the named regex won't match anonymous).
    let re_anon = Regex::new(r"\bexport\s+default\s+function\s*\(").unwrap();
    result = re_anon.replace_all(&result, "function __tropel_iteration(").to_string();

    // 1c. `export default () => { … }` — arrow function default export
    let re_arrow = Regex::new(r"\bexport\s+default\s*(\([^)]*\)\s*=>)").unwrap();
    result = re_arrow.replace_all(&result, "var __tropel_iteration = $1").to_string();

    // 1d. `export default expr` (any other, e.g. object literal) — assign to var.
    //     This catches `export default {…}` or `export default someVar`.
    //     The function/class/arrow cases are already handled by 1a–1c above.
    //     Uses `^.*?` to consume any line-beginning whitespace and then the
    //     entire `export default ` prefix, so the replacement starts clean.
    //     We deliberately use a simple `export default ` prefix match without
    //     look-arounds (the `regex` crate doesn't support them).
    let re_other = Regex::new(r"\bexport\s+default\s+").unwrap();
    result = re_other.replace_all(&result, "var __tropel_iteration = ").to_string();

    // 2. Remove k6 virtual import lines entirely:
    //    `import … from "k6";`, `import … from "k6/http";`, etc.
    let re_import = Regex::new(r#"(?m)^\s*import\s+.*?from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import.replace_all(&result, "").to_string();

    // 3. Remove `import "k6/…"` side-effect imports
    let re_import_side = Regex::new(r#"(?m)^\s*import\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import_side.replace_all(&result, "").to_string();

    // 4. Remove re-export lines: `export { … } from "k6/…"`
    let re_reexport = Regex::new(r#"(?m)^\s*export\s+\{[^}]*\}.*from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_reexport.replace_all(&result, "").to_string();

    result
}

/// Check if a file path has a TypeScript extension.
fn is_typescript_ext(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|e| matches!(e.to_lowercase().as_str(), "ts" | "mts" | "tsx"))
        .unwrap_or(false)
}

// ══════════════════════════════════════════════════════════════════
// JS bootstrapping
// ══════════════════════════════════════════════════════════════════

/// Bootstrap vendored JS libraries into a fresh context.
/// Mirrors the engine's `create_vu_js_context()` setup.
async fn bootstrap_js_libs(ctx: &JsContext) -> Result<()> {
    let libraries: [(&str, &str); 6] = [
        ("pm-api", include_str!("../../../../js/pm-api/pm.js")),
        ("chai-shim", include_str!("../../../../js/chai/chai-shim.js")),
        ("lodash-shim", include_str!("../../../../js/lodash/lodash-shim.js")),
        ("cryptojs-shim", include_str!("../../../../js/cryptojs-shim/cryptojs.js")),
        ("exec-shim", include_str!("../../../../js/exec/exec.js")),
        ("sleep-shim", SLEEP_SHIM),
    ];

    for (name, code) in &libraries {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!("Failed to bootstrap JS library '{}': {}", name, e);
        }
    }

    // Install native module functions
    if let Err(e) = tropel_native::install_all(ctx).await {
        tracing::warn!("Failed to install native modules: {}", e);
    }

    // Install __tropel_native_sleep (blocks the OS thread, safe under thread-per-core)
    ctx.with_ctx(|rq_ctx| {
        let globals = rq_ctx.globals();
        let _ = globals.set(
            "__tropel_native_sleep",
            rquickjs::function::Func::from(move |ms: f64| {
                if ms > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(ms / 1000.0));
                }
            }),
        );
    });

    // Eval the sleep(seconds) wrapper
    let _ = ctx.eval(SLEEP_SHIM).await;

    Ok(())
}

const SLEEP_SHIM: &str = r#"
if (typeof sleep === 'undefined') {
  function sleep(seconds) {
    if (typeof __tropel_native_sleep === 'function') {
      __tropel_native_sleep(seconds * 1000);
    }
  }
}
"#;

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_export_default() {
        let driver = K6Driver;
        let data = br#"export default function() { http.get("https://example.com"); }"#;
        assert!(driver.detect(data));
    }

    #[test]
    fn test_detect_k6_import() {
        let driver = K6Driver;
        let data = br#"import { check } from "k6"; export default function() {}"#;
        assert!(driver.detect(data));
    }

    #[test]
    fn test_detect_postman_not_k6() {
        let driver = K6Driver;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(!driver.detect(data), "Postman JSON should not be detected as k6");
    }

    #[test]
    fn test_preprocess_export_default_named() {
        let code = "export default function handler() { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(result.contains("function __tropel_iteration("), "Expected __tropel_iteration, got: {}", result);
        assert!(!result.contains("export default function"), "Still has export default: {}", result);
    }

    #[test]
    fn test_preprocess_export_default_anonymous() {
        let code = "export default function() { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(result.contains("function __tropel_iteration("), "Expected __tropel_iteration, got: {}", result);
        assert!(!result.contains("export default function"), "Still has export default: {}", result);
    }

    #[test]
    fn test_preprocess_export_default_arrow() {
        let code = "export default () => { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(result.contains("__tropel_iteration = ("), "Expected __tropel_iteration assignment, got: {}", result);
    }

    #[test]
    fn test_preprocess_removes_k6_imports() {
        let code = r#"
            import http from "k6/http";
            import { check, sleep } from "k6";
            export default function() { http.get("https://example.com"); }
        "#;
        let result = preprocess_k6_source(code);
        assert!(!result.contains("from \"k6"), "k6 import not removed: {}", result);
        assert!(!result.contains("from 'k6"), "k6 import not removed: {}", result);
        assert!(result.contains("__tropel_iteration"), "__tropel_iteration not found: {}", result);
    }

    #[test]
    fn test_preprocess_preserves_non_k6_imports() {
        let code = r#"
            import { someUtil } from "./local-utils";
            export default function() { someUtil(); }
        "#;
        let result = preprocess_k6_source(code);
        assert!(result.contains("./local-utils"), "Non-k6 import was removed: {}", result);
        assert!(result.contains("__tropel_iteration"), "__tropel_iteration not found: {}", result);
    }

    #[test]
    fn test_preprocess_removes_reexport() {
        let code = r#"
            export { default } from "k6/http";
            export default function() {}
        "#;
        let result = preprocess_k6_source(code);
        assert!(!result.contains("from \"k6/http\""), "k6 re-export not removed: {}", result);
    }

    #[test]
    fn test_driver_id() {
        assert_eq!(K6Driver.id(), "k6");
    }

    #[test]
    fn test_is_typescript_ext() {
        assert!(is_typescript_ext(Path::new("script.ts")));
        assert!(is_typescript_ext(Path::new("script.mts")));
        assert!(is_typescript_ext(Path::new("script.tsx")));
        assert!(!is_typescript_ext(Path::new("script.js")));
        assert!(!is_typescript_ext(Path::new("script.json")));
    }
}
