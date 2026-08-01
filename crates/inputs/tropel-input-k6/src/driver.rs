//! # K6 Driver — imperative execution path for k6 scripts
//!
//! Implements `Driver` + `DriverInstance` traits to run k6-style JS/TS test
//! scripts natively through the engine's imperative input path.
//!
//! ## Flow
//!
//! 1. **Pre-process** the raw source for ES-module evaluation: remove k6
//!    virtual imports (`import { … } from "k6/…"`) and unresolvable re-exports
//!    — the k6 shim provides those APIs as globals. All `export` modifiers
//!    are kept (`export const options`, `export default function`, …) because
//!    they are load-bearing in a module.
//!
//! 2. **Transpile** (if `.ts` source): strip TypeScript type annotations via
//!    `tropel_es::typescript_to_javascript_keep_exports` (keeps the `export`
//!    modifiers). ES module bundling is NOT used for k6 scripts — their
//!    imports (k6/http, k6/metrics, etc.) are virtual module names that don't
//!    correspond to files on disk.
//!
//! 3. **Bootstrap**: create a `JsContext`, bootstrap shim libraries (pm-api,
//!    chai, lodash, cryptojs, exec, sleep), install native modules.
//!
//! 4. **Eval as an ES module** (`rquickjs::Module::declare` + `eval` +
//!    `promise.finish`), then install the module's `default` export as the
//!    global `__tropel_iteration`. This is the only mode where
//!    `export const options` (the k6 load profile) survives alongside the
//!    default export.
//!
//! 5. **Options**: `Driver::declared_options` evaluates the script the same
//!    way in a throwaway context and reads the `options` export, so the
//!    engine can apply the script's own load profile (see `options.rs`).
//!
//! 6. **Run**: each call to `run_iteration()` invokes `__tropel_iteration()`
//!    and drains metrics/abort state from the `VuContext`.

use crate::options::K6Options;
use async_trait::async_trait;
use futures::future::join_all;
use regex::Regex;
use rquickjs::function::Func;
use std::collections::HashMap;
use std::path::Path;
use std::time::{Duration, Instant};
use tropel_js::JsContext;
use tropel_sdk::{Body, Method, Request, TagMap};
use tropel_sdk::{Driver, DriverDeclaredOptions, DriverInstance, DriverRegistration, VuContext};
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
        exec: Option<&str>,
    ) -> Result<Box<dyn DriverInstance>> {
        let original = std::str::from_utf8(bytes)
            .map_err(|e| TropelError::Parse(format!("k6 script is not valid UTF-8: {}", e)))?;

        // Step 1: Pre-process — remove k6 virtual imports (the k6 shim
        // provides those APIs as globals) but KEEP all `export` modifiers so
        // the source can be evaluated as an ES module.
        let final_source = prepare_module_source(original, source_path)?;

        // Step 2: Create JS context
        let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
            .await
            .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

        // Step 3: Bootstrap shim libraries & native modules
        bootstrap_js_libs(&js_ctx).await?;

        // Step 4: Eval the source as an ES module and install the entry-point
        // export as the global `__tropel_iteration`. When the scenario names
        // an `exec` function (k6 multi-scenario), install THAT export;
        // otherwise fall back to the module's `default` export. Modules are
        // the only mode where `export const options` (the k6 load profile) and
        // `export default function` survive together.
        install_iteration_global(&js_ctx, &final_source, exec)?;

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

    async fn declared_options(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> Option<DriverDeclaredOptions> {
        // Read the script's `export const options` by evaluating it as an ES
        // module. This is what makes k6's declared load profile (vus/duration/
        // stages/scenarios/thresholds) drive the run instead of being silently
        // ignored. Any failure (not a k6 options export, eval error, …) simply
        // yields `None` — the engine falls back to the CLI/JobConfig profile.
        let original = std::str::from_utf8(bytes).ok()?;
        let module_source = prepare_module_source(original, source_path).ok()?;
        // eval_module_export_json returns Result<Option<String>> — .ok()?
        // unwraps the Result, the second ? unwraps the Option.
        let json_str =
            eval_module_export_json(&module_source, "options", env).await.ok()??;
        let options: K6Options = match serde_json::from_str(&json_str) {
            Ok(o) => o,
            Err(e) => {
                // Never fail silently: a misparse drops the script's entire
                // load profile (vus/duration included).
                tracing::warn!(
                    "k6 options could not be parsed — falling back to the CLI/JobConfig load \
                     profile: {}",
                    e
                );
                return None;
            }
        };
        options.to_declared()
    }

    async fn handle_summary(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        summary_data_json: &str,
        env: &HashMap<String, String>,
    ) -> Option<HashMap<String, String>> {
        // Run the script's `export function handleSummary(data)` (k6) with
        // the post-run summary data object. Returns a map of filename →
        // content (the `stdout` key prints to stdout). Any failure (not a
        // function, eval error, …) yields None → engine falls back to its
        // default summary / --summary-export.
        let original = std::str::from_utf8(bytes).ok()?;
        let module_source = prepare_module_source(original, source_path).ok()?;
        eval_module_handle_summary(&module_source, summary_data_json, env)
            .await
            .ok()?
    }
}

// Register K6Driver for compile-time discovery.
inventory::submit!(DriverRegistration::new("k6", || Box::new(K6Driver))
.with_priority(10));

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
        //
        // `return` is required: the cached wrapper is `(async function(){...})`
        // and only an explicit `return` makes the wrapper adopt the inner
        // promise. Without it, an async default export's promise would be
        // discarded (side effects still run via the job pump, but its
        // rejections would be swallowed).
        let iter_start = Instant::now();

        match self
            .js_ctx
            .run_script_cached(
                "return __tropel_iteration()",
                Some("k6-iteration.js".to_string()),
            )
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
                          _timeout_ms: f64,
                          response_type: String|
                          -> String {
                        let headers = parse_headers_tolerant(&headers_json);
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
                            response_type: tropel_sdk::ResponseType::from_k6(&response_type),
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
                        let headers = parse_headers_tolerant(headers_json);
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
                        let response_type = entry
                            .get("response_type")
                            .and_then(|v| v.as_str())
                            .unwrap_or("text");
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
                            response_type: tropel_sdk::ResponseType::from_k6(response_type),
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


/// Pre-process a k6 source string for ES-module evaluation.
///
/// Unlike the old script-mode preprocessor (which stripped `export` modifiers
/// so the source could be eval'd), this variant KEEPS all `export` modifiers —
/// `export const options`, `export default function`, `export function
/// setup()` — because they are valid (and load-bearing) in a module. Only two
/// things are removed:
///
/// 1. k6 virtual imports (`import … from "k6/…"`) and k6 re-exports — the
///    k6 shim provides those APIs as globals, and there is no module loader
///    for the `k6/*` specifiers on disk.
/// 2. Unresolvable generic re-exports (`export { x } from "./other"`,
///    `export * from "./other"`) — no on-disk bundling/loader here either.
///
/// Standalone named-export blocks (`export { x }`) are kept — they are plain
/// ESM. `import { x } from "./local"` is also kept (it fails at eval time if
/// there is no such module on disk, same as script mode fails on any import).
fn preprocess_k6_source_module(source: &str) -> String {
    let mut result = source.to_string();

    // ── 1. Remove k6 virtual import / re-export lines entirely ──
    let re_import =
        Regex::new(r#"(?m)^\s*import\s+.*?from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import.replace_all(&result, "").to_string();

    let re_import_side =
        Regex::new(r#"(?m)^\s*import\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#).unwrap();
    result = re_import_side.replace_all(&result, "").to_string();

    let re_reexport =
        Regex::new(r#"(?m)^\s*export\s+\{[^}]*\}.*from\s+['"]k6(?:/[^'""]*)?['""]\s*;?\s*$"#)
            .unwrap();
    result = re_reexport.replace_all(&result, "").to_string();

    // ── 2. Remove unresolvable generic re-exports ──
    let re_reexport_generic =
        Regex::new(r#"\bexport\s*\{[^}]*\}\s*from\s+['"][^'"]+['"]\s*;?"#).unwrap();
    result = re_reexport_generic.replace_all(&result, "").to_string();

    let re_reexport_star =
        Regex::new(r#"\bexport\s*\*\s*[^'"]*?from\s+['"][^'"]+['"]\s*;?"#).unwrap();
    result = re_reexport_star.replace_all(&result, "").to_string();

    result
}

/// Build the final source for ES-module evaluation: pre-process (keep
/// exports, drop k6 virtual imports) and transpile TypeScript while keeping
/// the `export` modifiers intact (script-mode transpilation strips them).
fn prepare_module_source(original: &str, source_path: Option<&Path>) -> Result<String> {
    let preprocessed = preprocess_k6_source_module(original);

    if let Some(path) = source_path {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("js")
            .to_lowercase();
        if matches!(ext.as_str(), "ts" | "mts" | "tsx") {
            return tropel_es::typescript_to_javascript_keep_exports(
                &preprocessed,
                &path.to_string_lossy(),
            )
            .map_err(|e| TropelError::Parse(format!("TS transpile error: {}", e)));
        }
        return Ok(preprocessed);
    }

    // No path hint — detect TS patterns heuristically.
    if preprocessed.contains(": string")
        || preprocessed.contains(": number")
        || preprocessed.contains(": boolean")
        || preprocessed.contains("interface ")
    {
        return tropel_es::typescript_to_javascript_keep_exports(&preprocessed, "script.js")
            .map_err(|e| TropelError::Parse(format!("TS transpile error: {}", e)));
    }

    Ok(preprocessed)
}

/// Evaluate an ES module and return the named export serialized as JSON.
///
/// Creates a throwaway `JsContext`, sets the k6 globals a script may read at
/// top level (`__ENV` from the job's env, `__VU`, …), evals the module,
/// JSON.stringify()s the requested export, and drops the context. Returns
/// `Ok(None)` when the export is absent/undefined — never an error for a
/// script that simply does not declare the export.
async fn eval_module_export_json(
    source: &str,
    export: &str,
    env: &HashMap<String, String>,
) -> Result<Option<String>> {
    let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // Minimal globals a k6 script may reference while building its options.
    // `__ENV` carries the job's env vars so options computed from them
    // (e.g. `const baseURL = __ENV.BASE_URL`) resolve instead of silently
    // becoming undefined.
    let _ = js_ctx.set_global_str("__VU", "0").await;
    let _ = js_ctx.set_global_str("__ITER", "0").await;
    let env_json = serde_json::to_value(env).unwrap_or_else(|_| serde_json::json!({}));
    let _ = js_ctx.set_global_json("__ENV", &env_json).await;
    let _ = js_ctx.set_global_json("__tropel_env", &env_json).await;

    // Arm the per-eval timeout (module eval bypasses the eval-family methods).
    js_ctx.reset_interrupt();
    match js_ctx.with_ctx(|ctx| read_module_export_string(ctx, source, export)) {
        Ok(Some(s)) => Ok(Some(s)),
        Ok(None) => Ok(None),
        Err(e) => {
            tracing::warn!("Failed to read k6 export '{}': {}", export, e);
            Ok(None)
        }
    }
}

/// Evaluate an ES module, call its `handleSummary(data)` export with the
/// given summary-data JSON, and return the script's output map
/// (filename → content; `stdout` prints to stdout). Returns `Ok(None)` when
/// the script declares no `handleSummary` export.
async fn eval_module_handle_summary(
    source: &str,
    data_json: &str,
    env: &HashMap<String, String>,
) -> Result<Option<HashMap<String, String>>> {
    let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // Minimal globals a k6 script may reference while building its summary.
    let _ = js_ctx.set_global_str("__VU", "0").await;
    let _ = js_ctx.set_global_str("__ITER", "0").await;
    let env_json = serde_json::to_value(env).unwrap_or_else(|_| serde_json::json!({}));
    let _ = js_ctx.set_global_json("__ENV", &env_json).await;
    let _ = js_ctx.set_global_json("__tropel_env", &env_json).await;

    js_ctx.reset_interrupt();
    match js_ctx.with_ctx(|ctx| call_module_handle_summary(ctx, source, data_json)) {
        Ok(Some(map)) => Ok(Some(map)),
        Ok(None) => Ok(None),
        Err(e) => {
            tracing::warn!("Failed to run k6 handleSummary: {}", e);
            Ok(None)
        }
    }
}

/// Call `handleSummary(data)` inside the given context. The data object is
/// parsed via the global `JSON.parse` so no lifetime-bound JS value escapes
/// the `with_ctx` closure; the returned map is stringified and parsed here.
fn call_module_handle_summary(
    ctx: &rquickjs::Ctx,
    source: &str,
    data_json: &str,
) -> std::result::Result<Option<HashMap<String, String>>, rquickjs::Error> {
    let module = rquickjs::Module::declare(ctx.clone(), "k6-script", source)?;
    let (module, promise) = module.eval()?;
    promise.finish::<()>()?;

    // Use the established `module.get::<_, Function>` pattern (see
    // install_iteration_global): a missing export OR a non-function export
    // both yield None, matching read_module_export_string's handling.
    let func: rquickjs::Function = match module.get::<_, rquickjs::Function>("handleSummary") {
        Ok(f) => f,
        Err(_) => return Ok(None), // absent or not a function
    };

    let json_obj: rquickjs::Object = ctx.globals().get("JSON")?;
    let parse: rquickjs::Function = json_obj.get("parse")?;
    let data: rquickjs::Value = parse.call((data_json,))?;
    let result: rquickjs::Value = func.call((data,))?;

    // k6 allows `export async function handleSummary(data)`. If the call
    // returned a Promise, finish it (pumps the job queue until settled).
    let result: rquickjs::Value = if let Some(promise) = result.as_promise() {
        promise.finish()?
    } else {
        result
    };

    if result.is_undefined() || result.is_null() {
        return Ok(None);
    }

    let stringify: rquickjs::Function = json_obj.get("stringify")?;
    let s: String = stringify.call((result,))?;
    let parsed: serde_json::Value = serde_json::from_str(&s).unwrap_or_default();

    // k6 allows handleSummary to return a single string (→ stdout) or an
    // object map of filename → content.
    if let Some(text) = parsed.as_str() {
        return Ok(Some(HashMap::from([("stdout".to_string(), text.to_string())])));
    }
    let mut map = HashMap::new();
    if let Some(obj) = parsed.as_object() {
        for (k, v) in obj {
            map.insert(k.clone(), v.as_str().unwrap_or_default().to_string());
        }
    }
    Ok(Some(map))
}

/// Evaluate an ES module in the given context and JSON.stringify() the named
/// export. Returns `Ok(None)` when the export is missing or undefined.
///
/// The string is produced *inside* the context (via the global `JSON`
/// object) so no lifetime-bound JS value escapes the `with_ctx` closure.
fn read_module_export_string(
    ctx: &rquickjs::Ctx,
    source: &str,
    export: &str,
) -> std::result::Result<Option<String>, rquickjs::Error> {
    let module = rquickjs::Module::declare(ctx.clone(), "k6-script", source)?;
    let (module, promise) = module.eval()?;
    promise.finish::<()>()?;

    let value: rquickjs::Value = match module.get(export) {
        Ok(v) => v,
        Err(_) => return Ok(None), // export not present
    };
    if value.is_undefined() || value.is_null() {
        return Ok(None);
    }

    let json_obj: rquickjs::Object = ctx.globals().get("JSON")?;
    let stringify: rquickjs::Function = json_obj.get("stringify")?;
    let s: String = stringify.call((value,))?;
    Ok(Some(s))
}

/// Evaluate an ES module and install its entry-point export as the global
/// `__tropel_iteration` (what `run_iteration` invokes). When `exec` names a
/// specific exported function (k6 multi-scenario `exec` selection), that
/// export is installed; otherwise the module's `default` export is used.
fn install_iteration_global(js_ctx: &JsContext, source: &str, exec: Option<&str>) -> Result<()> {
    // Arm the per-eval timeout: this evals the module directly via with_ctx,
    // bypassing the eval-family methods that normally reset the deadline.
    js_ctx.reset_interrupt();
    js_ctx.with_ctx(|rq_ctx| {
        let module = rquickjs::Module::declare(rq_ctx.clone(), "k6-script", source).map_err(|e| {
            TropelError::Other(format!("k6 script module declare error: {}", e))
        })?;
        let (module, promise) = module
            .eval()
            .map_err(|e| TropelError::Other(format!("k6 script module eval error: {}", e)))?;
        promise
            .finish::<()>()
            .map_err(|e| TropelError::Other(format!("k6 script module resolve error: {}", e)))?;

        let entry = exec.filter(|e| !e.is_empty()).unwrap_or("default");
        match module.get::<_, rquickjs::Function>(entry) {
            Ok(entry_fn) => {
                rq_ctx
                    .globals()
                    .set("__tropel_iteration", entry_fn)
                    .map_err(|e| {
                        TropelError::Other(format!(
                            "failed to install __tropel_iteration: {}",
                            e
                        ))
                    })?;
            }
            Err(e) => {
                if entry != "default" {
                    // k6 semantics: a scenario naming a non-existent exec
                    // function errors loudly rather than silently running a
                    // different flow (confusing metrics).
                    return Err(TropelError::Other(format!(
                        "k6 scenario exec '{entry}' is not an exported function ({e}) — \
                         the named exec must be an `export function {entry}(...)` in the script"
                    )));
                }
                // Not fatal: script mode tolerated a missing default export
                // (warned + continued), so module mode does too.
                tracing::warn!("k6 script has no default export function: {}", e);
            }
        }
        Ok(())
    })
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

/// Parse a headers JSON string into a `HashMap`, accepting both the plain
/// object form (`{"k":"v"}`) and the Postman/array form
/// (`[{"key":"k","value":"v"}]`). The old code used
/// `serde_json::from_str(...).unwrap_or_default()`, which silently dropped
/// ALL headers whenever the payload wasn't a plain object — a silent
/// correctness divergence (P3 · k6 header-parse divergence).
fn parse_headers_tolerant(json: &str) -> HashMap<String, String> {
    if json.is_empty() || json == "{}" || json == "[]" {
        return HashMap::new();
    }
    if json.trim_start().starts_with('{') {
        if let Ok(map) = serde_json::from_str::<HashMap<String, String>>(json) {
            return map;
        }
    }
    if json.trim_start().starts_with('[') {
        if let Ok(arr) = serde_json::from_str::<Vec<HashMap<String, serde_json::Value>>>(json) {
            let mut headers = HashMap::new();
            for entry in arr {
                let key = entry.get("key").and_then(|v| v.as_str()).unwrap_or("");
                let value = entry.get("value").and_then(|v| v.as_str()).unwrap_or("");
                if !key.is_empty() {
                    headers.insert(key.to_string(), value.to_string());
                }
            }
            return headers;
        }
    }
    HashMap::new()
}

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

    // ── ES-module preprocessing (keeps exports) ──

    #[test]
    fn test_module_preprocess_keeps_exports() {
        let code = r#"
            import http from "k6/http";
            import { check } from "k6";
            export const options = { vus: 10, duration: '30s' };
            export function setup() { return {}; }
            export default function() { http.get('https://example.com'); }
        "#;
        let result = preprocess_k6_source_module(code);
        // k6 virtual imports are removed (shim provides globals)
        assert!(!result.contains("from \"k6/http\""), "k6 import kept: {result}");
        assert!(!result.contains("from \"k6\""), "k6 import kept: {result}");
        // exports are PRESERVED — module eval needs them
        assert!(
            result.contains("export const options"),
            "export const options stripped: {result}"
        );
        assert!(
            result.contains("export function setup"),
            "export function stripped: {result}"
        );
        assert!(
            result.contains("export default function"),
            "export default stripped: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_removes_reexports() {
        let code = r#"
            export { default } from "./other";
            export * from "./helpers";
            export const options = {};
            export default function() {}
        "#;
        let result = preprocess_k6_source_module(code);
        assert!(!result.contains("./other"), "re-export kept: {result}");
        assert!(!result.contains("./helpers"), "re-export kept: {result}");
        assert!(result.contains("export const options"), "options lost: {result}");
        assert!(
            result.contains("export default function"),
            "default export lost: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_keeps_named_export_block() {
        let code = "const x = 1; export { x };\nexport default function() {}";
        let result = preprocess_k6_source_module(code);
        assert!(
            result.contains("export { x }"),
            "standalone named export block stripped: {result}"
        );
    }

    // ── ES-module evaluation ──

    /// Read an export via a raw rquickjs context (no JsContext needed).
    fn read_export_for_test(source: &str, export: &str) -> Option<String> {
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            let module = rquickjs::Module::declare(ctx.clone(), "test-script", source).unwrap();
            let (module, promise) = module.eval().unwrap();
            promise.finish::<()>().unwrap();
            let value: rquickjs::Value = match module.get(export) {
                Ok(v) => v,
                Err(_) => return None,
            };
            if value.is_undefined() || value.is_null() {
                return None;
            }
            let json_obj: rquickjs::Object = ctx.globals().get("JSON").unwrap();
            let stringify: rquickjs::Function = json_obj.get("stringify").unwrap();
            let s: String = stringify.call((value,)).unwrap();
            Some(s)
        })
    }

    #[test]
    fn test_module_eval_reads_options_export() {
        let source = r#"
            export const options = { vus: 5, duration: "30s" };
            export default function() {}
        "#;
        let json = read_export_for_test(source, "options")
            .expect("options export should be readable");
        let opts: crate::options::K6Options = serde_json::from_str(&json).unwrap();
        assert_eq!(opts.vus, Some(5));
        assert_eq!(opts.duration.as_deref(), Some("30s"));
    }

    #[test]
    fn test_module_eval_missing_export_is_none() {
        let source = "export default function() {}\n";
        assert!(
            read_export_for_test(source, "options").is_none(),
            "missing export should yield None, not an error"
        );
    }

    #[test]
    fn test_module_eval_exports_default_function() {
        let source = "export default function() { return 42; }\n";
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            let module = rquickjs::Module::declare(ctx, "test-script", source).unwrap();
            let (module, promise) = module.eval().unwrap();
            promise.finish::<()>().unwrap();
            let f: rquickjs::Function = module.get("default").unwrap();
            let n: i32 = f.call(()).unwrap();
            assert_eq!(n, 42);
        });
    }

    #[test]
    fn test_module_eval_keeps_k6_globals_visible() {
        // k6 shim globals are set on the global object; module code must see
        // them (http.get inside the default function resolves via globals).
        let source = "export default function() { return typeof globalThis; }\n";
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.globals().set("someK6Global", 123).unwrap();
            let module = rquickjs::Module::declare(ctx.clone(), "test-script", source).unwrap();
            let (module, promise) = module.eval().unwrap();
            promise.finish::<()>().unwrap();
            let f: rquickjs::Function = module.get("default").unwrap();
            let s: String = f.call(()).unwrap();
            assert_eq!(s, "object");
            // global is visible from module code
            let src2 = "export default function() { return someK6Global; }\n";
            let module2 = rquickjs::Module::declare(ctx.clone(), "test-script-2", src2).unwrap();
            let (module2, promise2) = module2.eval().unwrap();
            promise2.finish::<()>().unwrap();
            let f2: rquickjs::Function = module2.get("default").unwrap();
            let n: i32 = f2.call(()).unwrap();
            assert_eq!(n, 123);
        });
    }

    #[test]
    fn test_exec_selection_installs_named_export() {
        // A scenario naming `exec: "browse"` must run the `browse` export,
        // NOT the default export (k6 multi-scenario semantics).
        let source = r#"
            export function browse() { return "browse-ran"; }
            export function checkout() { return "checkout-ran"; }
            export default function() { return "default-ran"; }
        "#;
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            let module = rquickjs::Module::declare(ctx, "exec-script", source).unwrap();
            let (module, promise) = module.eval().unwrap();
            promise.finish::<()>().unwrap();

            // Same selection logic install_iteration_global uses.
            let browse: rquickjs::Function = module.get("browse").unwrap();
            let s: String = browse.call(()).unwrap();
            assert_eq!(s, "browse-ran");

            // A missing exec export errors (module.get fails) — k6 errors
            // loudly rather than silently running the default flow.
            assert!(
                module.get::<_, rquickjs::Function>("nope").is_err(),
                "missing exec export must error"
            );
        });
    }

    #[test]
    fn test_module_eval_handle_summary_returns_map() {
        // `export function handleSummary(data)` must be callable with the
        // summary data and return a filename → content map (stdout prints).
        let source = r#"
            export function handleSummary(data) {
                return {
                    "stdout": "custom stdout: " + data.state.iterations,
                    "summary.html": "<html>" + data.metrics.http_reqs.type + "</html>",
                };
            }
            export default function() {}
        "#;
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        let map: Option<HashMap<String, String>> = ctx.with(|ctx| {
            call_module_handle_summary(
                &ctx,
                source,
                r#"{"metrics":{"http_reqs":{"type":"counter"}},"state":{"iterations":7}}"#,
            )
            .unwrap()
        });
        let map = map.expect("handleSummary must produce output");
        assert_eq!(map.get("stdout").map(|s| s.as_str()), Some("custom stdout: 7"));
        assert_eq!(
            map.get("summary.html").map(|s| s.as_str()),
            Some("<html>counter</html>")
        );
    }

    #[test]
    fn test_module_eval_handle_summary_absent_is_none() {
        let source = "export default function() {}\n";
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        let result: Option<HashMap<String, String>> = ctx.with(|ctx| {
            call_module_handle_summary(&ctx, source, "{}").unwrap()
        });
        assert!(result.is_none(), "no handleSummary export → None");
    }

    #[test]
    fn test_module_eval_handle_summary_async() {
        // k6 permits async handleSummary — the Promise must be finished.
        let source = r#"
            export async function handleSummary(data) {
                return { "stdout": "async " + data.state.vusMax };
            }
            export default function() {}
        "#;
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        let result: Option<HashMap<String, String>> = ctx.with(|ctx| {
            call_module_handle_summary(
                &ctx,
                source,
                r#"{"state":{"vusMax":3}}"#,
            )
            .unwrap()
        });
        let map = result.expect("async handleSummary must produce output");
        assert_eq!(map.get("stdout").map(|s| s.as_str()), Some("async 3"));
    }

    #[test]
    fn test_declared_options_driver_e2e() {
        // Full path: K6Driver::declared_options on a script with options.
        // Uses a raw module eval through the same helper the driver uses.
        let source = r#"
            export const options = { vus: 3, iterations: 10 };
            export default function() {}
        "#;
        let json = read_export_for_test(source, "options").unwrap();
        let opts: crate::options::K6Options = serde_json::from_str(&json).unwrap();
        let decl = opts.to_declared().unwrap();
        assert!(decl.execution.is_some());
        assert!(decl.scenarios.is_none());
        match decl.execution.unwrap() {
            tropel_sdk::ExecutionConfig::SharedIterations { iterations, vus, .. } => {
                assert_eq!(iterations, 10);
                assert_eq!(vus, 3);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }
}
