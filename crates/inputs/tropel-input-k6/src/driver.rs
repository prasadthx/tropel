//! # K6 Driver — imperative execution path for k6 scripts
//!
//! Implements `Driver` + `DriverInstance` traits to run k6-style JS/TS test
//! scripts natively through the engine's imperative input path.
//!
//! ## Flow
//!
//! 1. **Pre-process** the raw source: remove k6 virtual imports
//!    (`import { … } from "k6/…"`), capture `export default function` as the
//!    global `__tropel_iteration`, and strip all other top-level export
//!    modifiers (`export const options`, named exports, re-exports) so the
//!    script can be eval'd in script mode.
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
use futures::future::join_all;
use regex::Regex;
use rquickjs::function::Func;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tropel_js::JsContext;
use tropel_sdk::{Body, Method, Request, TagMap};
use tropel_sdk::{Driver, DriverInstance, DriverRegistration, VuContext};
use tropel_sdk::{Result, TropelError};

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
            let has_test_patterns = text.contains("http.get")
                || text.contains("http.post")
                || text.contains("check(")
                || text.contains("group(");
            has_export_default || has_k6_import || has_test_patterns
        } else {
            false
        }
    }

    async fn init(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
    ) -> Result<Box<dyn DriverInstance>> {
        let original = std::str::from_utf8(bytes)
            .map_err(|e| TropelError::Parse(format!("k6 script is not valid UTF-8: {}", e)))?;

        // Step 1: Pre-process — capture `export default function` as
        // `function __tropel_iteration` and remove k6 virtual imports.
        let preprocessed = preprocess_k6_source(original);

        // Step 2: Transpile TypeScript if needed
        let final_source = if let Some(path) = source_path {
            let ext = path
                .extension()
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
            if preprocessed.contains(": string")
                || preprocessed.contains(": number")
                || preprocessed.contains(": boolean")
                || preprocessed.contains("interface ")
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
        js_ctx
            .eval(&final_source)
            .await
            .map_err(|e| TropelError::Other(format!("Script eval error: {}", e)))?;

        // Verify __tropel_iteration was defined
        let has_iter = js_ctx
            .get_global("__tropel_iteration")
            .await
            .unwrap_or(None);
        if has_iter.is_none() {
            tracing::warn!("k6 script did not define a default export function — __tropel_iteration is not set");
        }

        Ok(Box::new(K6DriverInstance {
            js_ctx,
            _source_path: source_path.map(|p| p.to_path_buf()),
            http_bridge_registered: false,
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
    /// Whether the native HTTP bridge (__tropel_k6_http_request) has been
    /// registered. Registration happens on the first run_iteration() call
    /// because the HttpClient isn't available until init() completes.
    http_bridge_registered: bool,
}

// Safety: each DriverInstance runs on its own VU thread (thread-per-core).
// JsContext already has unsafe impl Send + Sync in tropel_js.
unsafe impl Send for K6DriverInstance {}
unsafe impl Sync for K6DriverInstance {}

#[async_trait]
impl DriverInstance for K6DriverInstance {
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()> {
        // Lazy-init: register the native HTTP bridge on the first iteration.
        // The HttpClient is only available at runtime (from engine), not during
        // init(). The bridge calls the async HTTP client synchronously via the
        // shared `tropel_http::blocking::execute_blocking` helper — safe from
        // inside a current-thread VU runtime (no block_on, which would panic
        // or deadlock the VU's own reactor).
        if !self.http_bridge_registered {
            self.maybe_register_http_bridge(ctx).await;
        }

        // Sync VuContext state into JS globals (__tropel_vu_id, etc.)
        self.sync_globals(ctx).await?;

        // Call __tropel_iteration() — the user's k6 script entry point.
        // Uses the cached `Persistent<Function>` fast path: the invocation
        // expression is compiled once (first iteration) and re-invoked from
        // the script cache on every subsequent iteration — no re-parsing.
        let iter_start = Instant::now();

        match self
            .js_ctx
            .run_script_cached("__tropel_iteration()", Some("k6-iteration.js".to_string()))
            .await
        {
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
    /// Lazily register the `__tropel_k6_http_request` native function.
    /// This function wraps the per-VU HttpClient so the k6 shim can
    /// execute HTTP requests synchronously from JS.
    async fn maybe_register_http_bridge(&mut self, ctx: &VuContext) {
        let http_client = match ctx.http_client.clone() {
            Some(c) => c,
            None => {
                tracing::warn!(
                    "K6Driver: http_client not available on first iteration — k6 http.* will fail"
                );
                self.http_bridge_registered = true; // Don't retry
                return;
            }
        };

        self.js_ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();
            let http_client_request = http_client.clone();
            let _ = globals.set(
                "__tropel_k6_http_request",
                Func::from(
                    move |method: String,
                          url: String,
                          headers_json: String,
                          body: String,
                          _timeout_ms: f64|
                          -> String {
                        let headers: HashMap<String, String> =
                            serde_json::from_str(&headers_json).unwrap_or_default();
                        let req_body = if body.is_empty() {
                            None
                        } else {
                            Some(Body::Raw(body))
                        };
                        let req = Request {
                            url,
                            method: Method::from_str(&method).unwrap_or(Method::GET),
                            headers,
                            query_params: HashMap::new(),
                            body: req_body,
                            auth: None,
                            certificate: None,
                            follow_redirects: true,
                            timeout: None,
                        };
                        // Execute on the dedicated I/O runtime via the shared
                        // blocking helper — safe from inside ctx.with on a
                        // current-thread VU runtime. No block_on here: that
                        // deadlocks the VU's own reactor.
                        let http_for_io = http_client_request.clone();
                        let result = tropel_http::blocking::execute_blocking(async move {
                            http_for_io.execute(&req).await
                        });
                        match result {
                            Ok(resp) => {
                                let body_text = String::from_utf8(resp.body).unwrap_or_default();
                                serde_json::json!({
                                    "code": resp.status_code,
                                    "status": resp.status_code,
                                    "status_text": resp.status_text,
                                    "body": body_text,
                                    "headers": resp.headers,
                                    "response_time": resp.response_time.as_secs_f64() * 1000.0,
                                })
                                .to_string()
                            }
                            Err(e) => serde_json::json!({
                                "code": 0,
                                "status": 0,
                                "status_text": format!("HTTP error: {}", e),
                                "body": "",
                                "headers": {},
                                "response_time": 0,
                            })
                            .to_string(),
                        }
                    },
                ),
            );

            let _ = globals.set(
                "__tropel_k6_http_batch",
                Func::from(move |requests_json: String| -> String {
                    let batch_requests: Vec<serde_json::Value> =
                        serde_json::from_str(&requests_json).unwrap_or_default();

                    let http_for_io = http_client.clone();
                    let futures = batch_requests.into_iter().map(move |entry| {
                        let key = entry.get("key").cloned().unwrap_or_else(|| serde_json::Value::String(String::new()));
                        let method = entry
                            .get("method")
                            .and_then(|v| v.as_str())
                            .unwrap_or("GET")
                            .to_string();
                        let url = entry
                            .get("url")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let headers_json = entry
                            .get("headers_json")
                            .and_then(|v| v.as_str())
                            .unwrap_or("{}");
                        let headers: HashMap<String, String> =
                            serde_json::from_str(headers_json).unwrap_or_default();
                        let body = entry
                            .get("body")
                            .and_then(|v| v.as_str())
                            .unwrap_or("")
                            .to_string();
                        let request_body = if body.is_empty() {
                            None
                        } else {
                            Some(Body::Raw(body))
                        };
                        let timeout_ms = entry
                            .get("timeout_ms")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(30000.0);
                        let timeout = if timeout_ms > 0.0 {
                            Some(Duration::from_millis(timeout_ms as u64))
                        } else {
                            None
                        };
                        let req = Request {
                            url,
                            method: Method::from_str(&method).unwrap_or(Method::GET),
                            headers,
                            query_params: HashMap::new(),
                            body: request_body,
                            auth: None,
                            certificate: None,
                            follow_redirects: true,
                            timeout,
                        };
                        let http_client = http_for_io.clone();
                        async move {
                            let resp = http_client.execute(&req).await;
                            (key, resp)
                        }
                    });

                    let responses = tropel_http::blocking::execute_blocking(async move {
                        let results = join_all(futures).await;
                        Ok(results)
                    });

                    let mut response_map = serde_json::Map::new();
                    if let Ok(results) = responses {
                        for (key, result) in results {
                            let key_str = match key {
                                serde_json::Value::String(s) => s,
                                serde_json::Value::Number(n) => n.to_string(),
                                serde_json::Value::Bool(b) => b.to_string(),
                                other => serde_json::to_string(&other).unwrap_or_default(),
                            };
                            let entry_resp = match result {
                                Ok(resp) => {
                                    let body_text = String::from_utf8(resp.body).unwrap_or_default();
                                    serde_json::json!({
                                        "code": resp.status_code,
                                        "status": resp.status_code,
                                        "status_text": resp.status_text,
                                        "body": body_text,
                                        "headers": resp.headers,
                                        "response_time": resp.response_time.as_secs_f64() * 1000.0,
                                    })
                                }
                                Err(e) => serde_json::json!({
                                    "code": 0,
                                    "status": 0,
                                    "status_text": format!("HTTP error: {}", e),
                                    "body": "",
                                    "headers": {},
                                    "response_time": 0,
                                }),
                            };
                            response_map.insert(key_str, entry_resp);
                        }
                    }

                    serde_json::Value::Object(response_map).to_string()
                }),
            );
        });

        self.http_bridge_registered = true;
        tracing::debug!("K6Driver: registered __tropel_k6_http_request native bridge");
    }
}

impl K6DriverInstance {
    /// Sync VuContext state into JS globals so the script can read
    /// environment variables, data rows, etc.
    async fn sync_globals(&self, ctx: &VuContext) -> Result<()> {
        let _ = self
            .js_ctx
            .set_global_str("__tropel_vu_id", &ctx.vu_id.to_string())
            .await;
        let _ = self
            .js_ctx
            .set_global_str("__tropel_iteration_num", &ctx.iteration.to_string())
            .await;
        let _ = self
            .js_ctx
            .set_global_str("__tropel_scenario", &ctx.scenario_name)
            .await;
        // k6-compatible globals: __VU and __ITER
        let _ = self
            .js_ctx
            .set_global_str("__VU", &ctx.vu_id.to_string())
            .await;
        let _ = self
            .js_ctx
            .set_global_str("__ITER", &ctx.iteration.to_string())
            .await;

        // Set env vars as JS globals. k6 scripts read `__ENV` (and Tropel's
        // own `__tropel_env`); both get the same object. Always set __ENV so
        // `__ENV` is never undefined inside the script.
        let env_value = serde_json::to_value(&ctx.env).unwrap_or_default();
        let _ = self.js_ctx.set_global_json("__ENV", &env_value).await;
        let _ = self
            .js_ctx
            .set_global_json("__tropel_env", &env_value)
            .await;

        // Set data row
        if let Some(ref row) = ctx.data_row {
            let _ = self
                .js_ctx
                .set_global_json(
                    "__tropel_data_row",
                    &serde_json::to_value(row).unwrap_or_default(),
                )
                .await;
        }

        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════
// Source pre-processing
// ══════════════════════════════════════════════════════════════════

/// Pre-process a k6 source string before transpilation and evaluation.
///
/// Order matters:
/// 1. Removes k6 virtual import lines (`import … from "k6/…"`) and k6
///    re-exports — these reference module specifiers that don't exist on disk
///    (the k6 shim provides the APIs as globals instead).
/// 2. Captures `export default function(name) { … }` as the global
///    `function __tropel_iteration(…) { … }` (also arrow and expr forms).
/// 3. Strips ALL remaining top-level export modifiers so the script can be
///    eval'd in script mode (QuickJS rejects `export` outside a module):
///    - `export const options = …` → `const options = …` (k6's `options` object)
///    - `export function setup() …` → `function setup() …` (named exports)
///    - `export class C …` → `class C …`
///    - `export { … }` → commented out (named export blocks)
///    - `export … from "…"` → removed entirely (re-exports, incl. multi-line)
///
/// The transpiler's `remove_exports` covers `.ts` files, but plain-JS k6 scripts
/// skip transpilation — so the stripping must happen here for the path-less case.
fn preprocess_k6_source(source: &str) -> String {
    let mut result = source.to_string();

    // ── 1. Remove k6 virtual import / re-export lines entirely ──
    //    `import … from "k6";`, `import … from "k6/http";`, etc.
    let re_import =
        Regex::new(r#"(?m)^\s*import\s+.*?from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import.replace_all(&result, "").to_string();

    // `import "k6/…"` side-effect imports
    let re_import_side =
        Regex::new(r#"(?m)^\s*import\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import_side.replace_all(&result, "").to_string();

    // `export { … } from "k6/…"` — k6 virtual re-exports (removed BEFORE the
    // generic strips below, which would otherwise comment instead of delete).
    let re_reexport =
        Regex::new(r#"(?m)^\s*export\s+\{[^}]*\}.*from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#)
            .unwrap();
    result = re_reexport.replace_all(&result, "").to_string();

    // ── 2. Capture the default export as the iteration entry ──

    // 2a. `export default function name(...)` → `function __tropel_iteration(...)`
    let re_named = Regex::new(r"\bexport\s+default\s+function\s+\w+\s*\(").unwrap();
    result = re_named
        .replace_all(&result, "function __tropel_iteration(")
        .to_string();

    // 2b. `export default function(` (anonymous) → `function __tropel_iteration(`
    //     Only if not already replaced above (the named regex won't match anonymous).
    let re_anon = Regex::new(r"\bexport\s+default\s+function\s*\(").unwrap();
    result = re_anon
        .replace_all(&result, "function __tropel_iteration(")
        .to_string();

    // 2c. `export default () => { … }` — arrow function default export
    let re_arrow = Regex::new(r"\bexport\s+default\s*(\([^)]*\)\s*=>)").unwrap();
    result = re_arrow
        .replace_all(&result, "var __tropel_iteration = $1")
        .to_string();

    // 2d. `export default expr` (any other, e.g. object literal) — assign to var.
    //     This catches `export default {…}`, `export default someVar`, and
    //     `export default async function …` / `export default class …`.
    //     Uses `^.*?` to consume any line-beginning whitespace and then the
    //     entire `export default ` prefix, so the replacement starts clean.
    //     We deliberately use a simple `export default ` prefix match without
    //     look-arounds (the `regex` crate doesn't support them).
    let re_other = Regex::new(r"\bexport\s+default\s+").unwrap();
    result = re_other
        .replace_all(&result, "var __tropel_iteration = ")
        .to_string();

    // ── 3. Strip remaining top-level export modifiers (plain-JS scripts) ──

    // `export async function F(...)` → `async function F(...)`
    let re_async_fn = Regex::new(r"\bexport\s+async\s+function\b").unwrap();
    result = re_async_fn
        .replace_all(&result, "async function")
        .to_string();

    // `export function F(...)` → `function F(...)`
    let re_fn = Regex::new(r"\bexport\s+function\b").unwrap();
    result = re_fn.replace_all(&result, "function").to_string();

    // `export class C` → `class C`
    let re_class = Regex::new(r"\bexport\s+class\b").unwrap();
    result = re_class.replace_all(&result, "class").to_string();

    // `export const|let|var X = …` → `const|let|var X = …`
    // Handles k6's ubiquitous `export const options = { … }`.
    let re_var = Regex::new(r"\bexport\s+(const|let|var)\b").unwrap();
    result = re_var.replace_all(&result, "$1").to_string();

    // `export { … } from "…"` / `export * from "…"` / `export * as ns from "…"`
    // — generic re-exports → delete entirely (including multi-line
    // `export {\n…\n} from "…"`). Runs BEFORE the standalone-block regex so
    // `export { x } from "…"` is consumed wholesale here instead of mangled there.
    let re_reexport_generic =
        Regex::new(r#"\bexport\s*\{[^}]*\}\s*from\s+['"][^'"]+['"]\s*;?"#).unwrap();
    result = re_reexport_generic.replace_all(&result, "").to_string();
    // `[^'"]*?` (lazy) between `*` and `from` also covers `export * as ns from "…"`
    // without risking over-match across statements in contrived multi-line code.
    let re_reexport_star =
        Regex::new(r#"\bexport\s*\*\s*[^'"]*?from\s+['"][^'"]+['"]\s*;?"#).unwrap();
    result = re_reexport_star.replace_all(&result, "").to_string();

    // `export { … }` — standalone named-export block (single- or multi-line,
    // with or without trailing `;`) → comment it out. Runs AFTER re-exports
    // are deleted, so a `…} from "…"` never reaches this pattern.
    let re_export_block = Regex::new(r"\bexport\s*\{[^}]*\}\s*;?").unwrap();
    result = re_export_block
        .replace_all(&result, "/* named exports stripped */")
        .to_string();

    result
}

/// Check if a file path has a TypeScript extension (used in tests).
#[cfg(test)]
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
    // Phase 1: Base shim libraries (no native dependencies)
    let base_libraries: [(&str, &str); 4] = [
        (
            "chai-shim",
            include_str!("../../../../js/chai/chai-shim.js"),
        ),
        (
            "lodash-shim",
            include_str!("../../../../js/lodash/lodash-shim.js"),
        ),
        (
            "cryptojs-shim",
            include_str!("../../../../js/cryptojs-shim/cryptojs.js"),
        ),
        ("exec-shim", include_str!("../../../../js/exec/exec.js")),
    ];

    for (name, code) in &base_libraries {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!("Failed to bootstrap JS library '{}': {}", name, e);
        }
    }

    // Phase 2: Install native module functions (needed by pm-api and k6-shim)
    if let Err(e) = tropel_native::install_all(ctx).await {
        tracing::warn!("Failed to install native modules: {}", e);
    }

    // Phase 3: Bootstrapping libraries that depend on native functions
    let native_dependent_libraries: [(&str, &str); 3] = [
        ("pm-api", include_str!("../../../../js/pm-api/pm.js")),
        ("sleep-shim", SLEEP_SHIM),
        ("k6-shim", include_str!("../../../../js/k6-shim/k6-shim.js")),
    ];

    for (name, code) in &native_dependent_libraries {
        if let Err(e) = ctx.bootstrap_library(code).await {
            tracing::warn!("Failed to bootstrap JS library '{}': {}", name, e);
        }
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

    // Eval the sleep(seconds) wrapper (sits behind native_sleep)
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
        assert!(
            !driver.detect(data),
            "Postman JSON should not be detected as k6"
        );
    }

    #[test]
    fn test_preprocess_export_default_named() {
        let code = "export default function handler() { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("function __tropel_iteration("),
            "Expected __tropel_iteration, got: {}",
            result
        );
        assert!(
            !result.contains("export default function"),
            "Still has export default: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_export_default_anonymous() {
        let code = "export default function() { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("function __tropel_iteration("),
            "Expected __tropel_iteration, got: {}",
            result
        );
        assert!(
            !result.contains("export default function"),
            "Still has export default: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_export_default_arrow() {
        let code = "export default () => { http.get('https://example.com'); }";
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("__tropel_iteration = ("),
            "Expected __tropel_iteration assignment, got: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_removes_k6_imports() {
        let code = r#"
            import http from "k6/http";
            import { check, sleep } from "k6";
            export default function() { http.get("https://example.com"); }
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("from \"k6"),
            "k6 import not removed: {}",
            result
        );
        assert!(
            !result.contains("from 'k6"),
            "k6 import not removed: {}",
            result
        );
        assert!(
            result.contains("__tropel_iteration"),
            "__tropel_iteration not found: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_preserves_non_k6_imports() {
        let code = r#"
            import { someUtil } from "./local-utils";
            export default function() { someUtil(); }
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("./local-utils"),
            "Non-k6 import was removed: {}",
            result
        );
        assert!(
            result.contains("__tropel_iteration"),
            "__tropel_iteration not found: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_removes_reexport() {
        let code = r#"
            export { default } from "k6/http";
            export default function() {}
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("from \"k6/http\""),
            "k6 re-export not removed: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_strips_export_const_options() {
        // The #2 blocker: plain-JS k6 scripts always have `export const options`,
        // which is a SyntaxError in script-mode eval unless stripped.
        let code = r#"
            export const options = {
                vus: 10,
                duration: '30s',
            };
            export default function() { http.get('https://example.com'); }
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("const options = {"),
            "options const not kept: {}",
            result
        );
        assert!(
            !result.contains("export const options"),
            "export const options not stripped: {}",
            result
        );
        assert!(
            result.contains("__tropel_iteration"),
            "default export not captured: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_strips_export_named_functions() {
        // setup/teardown + arbitrary named exports must not reach script-mode eval.
        let code = r#"
            export function setup() { return {}; }
            export function teardown(data) {}
            export default function() {}
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            result.contains("function setup()"),
            "setup lost: {}",
            result
        );
        assert!(
            result.contains("function teardown(data)"),
            "teardown lost: {}",
            result
        );
        assert!(
            !result.contains("export function"),
            "export function not stripped: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_strips_export_named_blocks() {
        let code = "const x = 1; export { x };\nexport default function() {}";
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("export { x };"),
            "named export block not stripped: {}",
            result
        );
        assert!(result.contains("const x = 1"), "const lost: {}", result);
    }

    #[test]
    fn test_preprocess_strips_export_class_and_var() {
        let code = r#"
            export var COUNT = 3;
            export class Helper {}
            export default function() {}
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("export var"),
            "export var not stripped: {}",
            result
        );
        assert!(
            !result.contains("export class"),
            "export class not stripped: {}",
            result
        );
        assert!(result.contains("var COUNT = 3"), "var lost: {}", result);
        assert!(result.contains("class Helper"), "class lost: {}", result);
    }

    #[test]
    fn test_preprocess_strips_generic_reexports() {
        let code = r#"
            export * from "./helpers";
            export { default } from "./other";
            export default function() {}
        "#;
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("./helpers"),
            "re-export not stripped: {}",
            result
        );
        assert!(
            !result.contains("./other"),
            "re-export not stripped: {}",
            result
        );
        assert!(
            result.contains("__tropel_iteration"),
            "default export lost: {}",
            result
        );
    }

    #[test]
    fn test_preprocess_strips_same_line_reexports() {
        // Regression: the standalone `export { … }` block regex must NOT mangle
        // a same-line re-export into a dangling `from "…"` fragment.
        let code = "const x = 1; export { x } from \"./other\";\nexport default function() {}";
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("./other"),
            "same-line re-export not deleted wholesale: {}",
            result
        );
        assert!(
            !result.contains("from \"./other\""),
            "dangling 'from' fragment left behind: {}",
            result
        );
        assert!(result.contains("const x = 1"), "const lost: {}", result);
    }

    #[test]
    fn test_preprocess_strips_namespace_reexports() {
        // `export * as ns from "…"` — namespace re-export (from after ` as ns `).
        let code = "export * as httpAlias from \"./http\";\nexport default function() {}";
        let result = preprocess_k6_source(code);
        assert!(
            !result.contains("./http"),
            "namespace re-export not stripped: {}",
            result
        );
        assert!(
            result.contains("__tropel_iteration"),
            "default export lost: {}",
            result
        );
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
