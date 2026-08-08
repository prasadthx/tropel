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
use futures_util::{SinkExt, StreamExt};
use rquickjs::function::Func;
use rquickjs::loader::{ImportAttributes, Loader, Resolver};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tropel_js::JsContext;
use tropel_sdk::{
    AuthConfig, Body, Method, Request, Response, Sample, SampleType, TagMap, Timings,
};
use tropel_sdk::{
    Driver, DriverDeclaredOptions, DriverHttpClient, DriverInstance, DriverRegistration, VuContext,
};
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

    // `#[allow]`: WsSession embeds a std::sync::mpsc::Receiver (not Sync), so
    // Arc<WsSession> is not Send+Sync — but the session registry never
    // crosses threads: each DriverInstance runs on its own VU thread
    // (thread-per-core), the same invariant the unsafe impl Send/Sync for
    // K6DriverInstance below documents.
    #[allow(clippy::arc_with_non_send_sync)]
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
        let mut js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
            .await
            .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

        // Step 3: Bootstrap shim libraries & native modules
        bootstrap_js_libs(&mut js_ctx).await?;

        // Install the k6 file-access bridges (open() + SharedArray cache).
        // Needs the script directory for relative path resolution.
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        register_k6_file_bridges(&mut js_ctx, script_dir.clone());

        // Register the ES-module resolver/loader so local imports
        // (`import { x } from "./helpers.js"`) resolve to files on disk,
        // with on-the-fly TypeScript transpilation for imported `.ts` files.
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: script_dir.clone(),
            },
            K6ModuleLoader,
        );

        // Step 4: Eval the source as an ES module and install the entry-point
        // export as the global `__tropel_iteration`. When the scenario names
        // an `exec` function (k6 multi-scenario), install THAT export;
        // otherwise fall back to the module's `default` export. Modules are
        // the only mode where `export const options` (the k6 load profile) and
        // `export default function` survive together.
        install_iteration_global(&mut js_ctx, &final_source, exec)?;

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
            script_bridges_registered: false,
            sample_sink: Arc::new(Mutex::new(Vec::new())),
            group_stack: Arc::new(Mutex::new(Vec::new())),
            exec_state: Arc::new(Mutex::new(K6ExecState::default())),
            abort_requested: Arc::new(Mutex::new(None)),
            ws_sessions: Arc::new(Mutex::new(HashMap::new())),
            ws_next_id: Arc::new(AtomicU64::new(0)),
            ws_bridges_registered: false,
            globals_seeded: false,
        }))
    }

    async fn declared_options(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> Result<Option<DriverDeclaredOptions>> {
        // Read the script's `export const options` by evaluating it as an ES
        // module. This is what makes k6's declared load profile (vus/duration/
        // stages/scenarios/thresholds) drive the run instead of being silently
        // ignored.
        //
        // Backlog line 153: `None` (nothing declared) and `Err` (declared but
        // MALFORMED) are now distinct. A type mismatch anywhere in `options`
        // — e.g. `stages: [{duration: 60}]` where k6 wants a string — used to
        // fail the whole parse, warn, and silently fall back to the CLI
        // profile: the run succeeded reporting numbers for a profile nobody
        // asked for. k6 hard-errors, so Tropel aborts with a Parse error too.
        // A non-UTF8 script / module-prep failure / eval failure / missing
        // export all mean "nothing declared" → Ok(None) → the engine falls
        // back to the CLI profile (unchanged from before line 153). Only a
        // PRESENT `options` export that fails to deserialize is fatal (Err).
        let original = match std::str::from_utf8(bytes) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let module_source = match prepare_module_source(original, source_path) {
            Ok(s) => s,
            Err(_) => return Ok(None),
        };
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        let json_str =
            match eval_module_export_json(&module_source, "options", env, script_dir).await {
                Ok(Some(s)) => s,
                _ => return Ok(None),
            };
        let options: K6Options = match serde_json::from_str(&json_str) {
            Ok(o) => o,
            Err(e) => {
                return Err(TropelError::Parse(format!(
                    "k6 script declares `options` but they failed to parse: {} \
                     (k6 would abort — fix the type mismatch, e.g. `stages: \
                     [{{duration: 60}}]` needs a string duration like \"60s\")",
                    e
                )));
            }
        };
        Ok(options.to_declared())
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
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        eval_module_handle_summary(&module_source, summary_data_json, env, script_dir)
            .await
            .ok()?
    }

    async fn setup(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        env: &HashMap<String, String>,
        http_client: Arc<dyn DriverHttpClient + Send + Sync>,
        sink: Arc<Mutex<Vec<Sample>>>,
    ) -> Option<String> {
        // Run the script's `export function setup()` (k6) ONCE per scenario,
        // before any VU spawns. The engine threads the serialized return
        // value into every VU (so `export default function (data)` receives
        // it) and passes it to `teardown(data)` after the run. Returns
        // `None` when the script declares no `setup` export (VUs see
        // `undefined`, matching k6). A throwing setup is LOGGED and yields
        // `None` — the engine falls back to no data rather than aborting
        // (never silently: a throwing setup is a broken artifact and must be
        // visible, even though the run proceeds with undefined data).
        //
        // k6 §4 (backlog line 119): setup() may make HTTP calls — the
        // throwaway context registers the HTTP bridges (via
        // eval_module_call_export) against the scenario's shared client, and
        // the samples land in `sink` for the engine to drain into the run's
        // metrics (k6 counts setup http_reqs). The canonical
        // login-in-setup pattern therefore works: `http.post(...)` resolves
        // and its value can be returned as `data`.
        let original = std::str::from_utf8(bytes).ok()?;
        let module_source = prepare_module_source(original, source_path).ok()?;
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        match eval_module_call_export(
            &module_source,
            "setup",
            None,
            env,
            script_dir,
            http_client,
            sink,
        )
        .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("k6 setup() failed: {}", e);
                None
            }
        }
    }

    async fn teardown(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        setup_data_json: Option<&str>,
        env: &HashMap<String, String>,
        http_client: Arc<dyn DriverHttpClient + Send + Sync>,
        sink: Arc<Mutex<Vec<Sample>>>,
    ) {
        // Run the script's `export function teardown(data)` (k6) ONCE after
        // all VUs finish, with the `setup()` return value as `data`. A
        // missing export is a silent no-op (k6); a throwing teardown is
        // logged but never affects the run's exit status (k6 parity).
        // teardown() may also make HTTP calls (k6 §4) — same bridges +
        // sink as setup(), drained by the engine after the call.
        let Some(original) = std::str::from_utf8(bytes).ok() else {
            return;
        };
        let Ok(module_source) = prepare_module_source(original, source_path) else {
            return;
        };
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        if let Err(e) = eval_module_call_export(
            &module_source,
            "teardown",
            setup_data_json,
            env,
            script_dir,
            http_client,
            sink,
        )
        .await
        {
            tracing::warn!("k6 teardown() failed: {}", e);
        }
    }
}

// Register K6Driver for compile-time discovery.
inventory::submit!(DriverRegistration::new("k6", || Box::new(K6Driver)).with_priority(10));

// ──────────────────────────────────────────────────────────────────────
// k6 `open()` + `k6/data` SharedArray native cache
// ──────────────────────────────────────────────────────────────────────
//
// k6 semantics: `new SharedArray(name, factory)` computes the data ONCE per
// process (in the init context) and shares it read-only across all VUs. In
// Tropel each VU owns its own JsContext (thread-per-core), so the "shared"
// payload lives on the native side: the first VU context that constructs a
// given SharedArray runs the factory, and its parsed elements are stored in
// this process-global cache as ONE `Arc<Vec<Value>>`. Every other VU context
// gets only a name + length from the accessor bridges and fetches elements
// through `__tropel_k6_shared_array_get(name, i)` — no per-VU copy of the
// array (the old design re-serialized the whole JSON into every context,
// O(VUs × size)).
//
// Keyed by name only — matches k6 (the name is the identity).
//
// Locking model: an `Arc` SNAPSHOT under an `RwLock`. The map is written
// once per SharedArray name (rare) and read per element (hot). Readers do
// ONE shared read-lock acquisition to clone the `Arc` (a refcount bump, no
// string copy), then drop the guard and read their private snapshot
// LOCK-FREE — so the per-element path never holds a lock during key
// building / element serialization, and concurrent readers never collide
// (the old `Mutex` serialized every VU thread on every element read — the
// one place all VUs collided). Writers clone the small map and swap it in.
/// name -> frozen element rows. `Arc<Vec<...>>` lets readers grab a cheap
/// refcount bump; writers clone the small map and swap it in.
type SharedArrayCache = RwLock<Arc<HashMap<String, Arc<Vec<serde_json::Value>>>>>;

static SHARED_ARRAY_CACHE: OnceLock<SharedArrayCache> = OnceLock::new();

fn shared_array_cache() -> &'static SharedArrayCache {
    SHARED_ARRAY_CACHE.get_or_init(|| RwLock::new(Arc::new(HashMap::new())))
}

/// Register the k6 file-access native bridges on a JS context:
///
/// - `__tropel_k6_open(path, mode)` — reads a file (relative to the script's
///   directory, or absolute) and returns its contents: `"t"` mode returns the
///   UTF-8 text, `"b"` mode returns base64-encoded bytes (the shim decodes
///   into an ArrayBuffer, matching k6's `open(path, 'b')`). A missing/unreadable
///   file throws a JS `Error` (k6 behavior).
/// - `__tropel_k6_shared_array_len(name)` — element count, or `-1` if absent
///   (the shim runs the factory only when absent).
/// - `__tropel_k6_shared_array_get(name, i)` — native JS value of ONE element,
///   or `undefined` when absent/out-of-range (no per-element JSON parse).
/// - `__tropel_k6_shared_array_set(name, json)` — parse the computed array
///   ONCE and share it as a process-global `Arc<Vec<Value>>`.
///
/// The bridges must be installed on EVERY k6 context that may evaluate script
/// code (the per-VU init context AND the throwaway options/handleSummary
/// contexts), because k6 scripts routinely call `open()`/`new SharedArray()`
/// at module top level while building `export const options`.
fn register_k6_file_bridges(ctx: &mut JsContext, script_dir: Option<PathBuf>) {
    ctx.with_ctx(|rq_ctx| {
        let globals = rq_ctx.globals();
        let dir = script_dir.clone();
        // Key the SharedArray cache by script dir + name so two different
        // scripts sharing a process (multi-scenario, repeated in-process runs)
        // never collide on the same name (k6 keys by the init context).
        let cache_prefix = dir
            .as_ref()
            .map(|d| d.to_string_lossy().into_owned())
            .unwrap_or_default();

        let _ = globals.set(
            "__tropel_k6_open",
            Func::from(
                move |ctx: rquickjs::Ctx,
                      path: String,
                      mode: String|
                      -> std::result::Result<String, rquickjs::Error> {
                    let p = Path::new(&path);
                    let full = if p.is_absolute() {
                        p.to_path_buf()
                    } else {
                        match &dir {
                            Some(d) => d.join(p),
                            None => p.to_path_buf(),
                        }
                    };
                    match std::fs::read(&full) {
                        Ok(bytes) => {
                            if mode == "b" {
                                use base64::Engine;
                                Ok(base64::engine::general_purpose::STANDARD.encode(bytes))
                            } else {
                                Ok(String::from_utf8_lossy(&bytes).into_owned())
                            }
                        }
                        Err(e) => {
                            let msg = format!("open('{}'): {}", path, e);
                            let exc = rquickjs::Exception::from_message(ctx.clone(), &msg)
                                .map_err(|_| rquickjs::Error::Exception)?;
                            Err(ctx.throw(exc.into_object().into_value()))
                        }
                    }
                },
            ),
        );

        // The SharedArray bridges live in a generic fn (below) so the element
        // accessor can return a native `Value<'js>` instead of a JSON string.
        register_shared_array_bridges(rq_ctx, cache_prefix);
    });
}

/// Convert a parsed `serde_json::Value` into a native QuickJS value.
///
/// Elements are stored as parsed JSON in the shared cache; this builds the
/// corresponding JS value directly in the QuickJS heap (objects, arrays,
/// primitives, null), so the accessor bridge returns a native value and the
/// shim needs no `JSON.parse` per element.
fn json_to_value<'js>(ctx: &rquickjs::Ctx<'js>, v: &serde_json::Value) -> rquickjs::Value<'js> {
    match v {
        serde_json::Value::Null => rquickjs::Value::new_null(ctx.clone()),
        serde_json::Value::Bool(b) => rquickjs::IntoJs::into_js(*b, ctx)
            .unwrap_or_else(|_| rquickjs::Value::new_undefined(ctx.clone())),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rquickjs::IntoJs::into_js(i, ctx)
                    .unwrap_or_else(|_| rquickjs::Value::new_undefined(ctx.clone()))
            } else if let Some(f) = n.as_f64() {
                rquickjs::IntoJs::into_js(f, ctx)
                    .unwrap_or_else(|_| rquickjs::Value::new_undefined(ctx.clone()))
            } else {
                rquickjs::Value::new_undefined(ctx.clone())
            }
        }
        serde_json::Value::String(s) => rquickjs::IntoJs::into_js(s.as_str(), ctx)
            .unwrap_or_else(|_| rquickjs::Value::new_undefined(ctx.clone())),
        serde_json::Value::Array(items) => match rquickjs::Array::new(ctx.clone()) {
            Ok(arr) => {
                for (i, item) in items.iter().enumerate() {
                    let _ = arr.set(i, json_to_value(ctx, item));
                }
                arr.into_value()
            }
            Err(_) => rquickjs::Value::new_undefined(ctx.clone()),
        },
        serde_json::Value::Object(map) => match rquickjs::Object::new(ctx.clone()) {
            Ok(obj) => {
                for (k, val) in map {
                    let _ = obj.set(k.as_str(), json_to_value(ctx, val));
                }
                obj.into_value()
            }
            Err(_) => rquickjs::Value::new_undefined(ctx.clone()),
        },
    }
}

/// Register the SharedArray native bridges on a JS context. Lives in a
/// generic fn — not an inline closure — because the element-accessor bridge's
/// closure must name the QuickJS lifetime `'js` to return a native
/// `Value<'js>` (eliminating the per-element `to_string` + `JSON.parse` round
/// trip), and closures cannot declare lifetimes.
fn register_shared_array_bridges<'js>(rq_ctx: &rquickjs::Ctx<'js>, cache_prefix: String) {
    let globals = rq_ctx.globals();

    // `len` returns -1 when the name is absent (factory must run) or the
    // element count when cached — the JS shim decides between the two. Uses
    // the same Arc-snapshot pattern as `get`: clone the Arc under a shared
    // read lock, drop the guard, read lock-free.
    let len_prefix = cache_prefix.clone();
    let _ = globals.set(
        "__tropel_k6_shared_array_len",
        Func::from(move |name: String| -> i32 {
            let key = format!("{}|{}", len_prefix, name);
            let snapshot = shared_array_cache()
                .read()
                .map(|c| c.clone())
                .unwrap_or_default();
            snapshot.get(&key).map(|v| v.len() as i32).unwrap_or(-1)
        }),
    );

    // Element accessor — returns a NATIVE JS value for ONE element, or
    // `undefined` when absent/out-of-range. The shim consumes the value
    // directly (no `JSON.parse` per element), so no context ever materializes
    // the whole array or re-parses it.
    let get_prefix = cache_prefix.clone();
    let _ = globals.set(
        "__tropel_k6_shared_array_get",
        Func::from(
            move |ctx: rquickjs::Ctx<'js>, name: String, i: i32| -> rquickjs::Value<'js> {
                let key = format!("{}|{}", get_prefix, name);
                // Snapshot: one shared read lock to clone the Arc (refcount
                // bump), then drop the guard and read lock-free.
                let snapshot = shared_array_cache()
                    .read()
                    .map(|c| c.clone())
                    .unwrap_or_default();
                match snapshot.get(&key) {
                    Some(v) if i >= 0 && (i as usize) < v.len() => {
                        json_to_value(&ctx, &v[i as usize])
                    }
                    _ => rquickjs::Value::new_undefined(ctx.clone()),
                }
            },
        ),
    );

    // Parse the computed array ONCE (first VU) and share it as an Arc.
    let set_prefix = cache_prefix.clone();
    let _ = globals.set(
        "__tropel_k6_shared_array_set",
        Func::from(move |name: String, json: String| {
            let key = format!("{}|{}", set_prefix, name);
            if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(&json) {
                if let Ok(mut c) = shared_array_cache().write() {
                    // Clone the small map and swap it in (the snapshot model);
                    // per-name writes are rare (once per SharedArray).
                    Arc::make_mut(&mut c).insert(key, Arc::new(parsed));
                }
            }
        }),
    );
}

/// The k6 `open()` + `k6/data` SharedArray shim (globals `open` and
/// `SharedArray`), which delegates to the native bridges above.
const OPEN_DATA_SHIM: &str = include_str!("../../../../js/k6-shim/open-data-shim.js");

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
    /// Whether the script-state bridges (__tropel_pm_test,
    /// __tropel_pm_custom_metric_add, __tropel_exec_*, __tropel_test_abort)
    /// have been registered. Same lazy pattern as the HTTP bridge; these
    /// read the per-VU exec_state / abort flag.
    script_bridges_registered: bool,
    /// Shared sink for samples recorded by the native HTTP bridge closures
    /// (__tropel_k6_http_request / __tropel_k6_http_batch). The closures are
    /// 'static and can't reach the VuContext, so they push into this buffer
    /// and run_iteration() drains it into ctx.samples after each iteration.
    sample_sink: Arc<Mutex<Vec<Sample>>>,
    /// Per-VU group() stack (backlog line 154). `group(name)` pushes here,
    /// the matching group_end pops; the http bridges read the top when
    /// stamping http_req_* samples so metrics recorded inside a group carry
    /// `group=::name` (k6 parity) instead of the hardcoded `group=http`.
    group_stack: Arc<Mutex<Vec<String>>>,
    /// Shared exec.* state — the pm.js / k6-shim / exec.js scripts read it
    /// through __tropel_exec_* closures registered lazily; sync_globals()
    /// refreshes it from the VuContext before each iteration.
    exec_state: Arc<Mutex<K6ExecState>>,
    /// test.abort() flag — set by __tropel_test_abort, drained by
    /// run_iteration() into ctx.abort() so the engine stops the run.
    abort_requested: Arc<Mutex<Option<String>>>,
    /// Live WebSocket sessions created by `__tropel_k6_ws_connect`. The
    /// bridge closures are 'static and can't own the VuContext, so the
    /// registry lives here; `__tropel_k6_ws_finish` removes the session and
    /// emits its ws_* samples into the sample_sink.
    ///
    ws_sessions: Arc<Mutex<HashMap<u64, Arc<WsSession>>>>,
    /// Monotonic session-id allocator for ws sessions.
    ws_next_id: Arc<AtomicU64>,
    /// Whether the ws bridges (__tropel_k6_ws_*) have been registered.
    ws_bridges_registered: bool,
    /// Whether the per-VU IMMUTABLE globals (__VU, __tropel_vu_id,
    /// __tropel_scenario, __ENV, __tropel_env) have been seeded. The env is
    /// constant for the whole run, so re-serializing it every iteration was
    /// pure waste; sync_globals() seeds these once on the first iteration and
    /// only refreshes the per-iteration values (__ITER, data row, exec.*)
    /// thereafter.
    globals_seeded: bool,
}

/// Execution-context values exposed to scripts via `exec.*` (k6 API).
/// Populated from the VuContext by sync_globals() before each iteration.
#[derive(Debug, Clone, Default)]
struct K6ExecState {
    scenario_name: String,
    executor_name: String,
    vu_id: u32,
    iteration: u64,
    iterations_completed: u64,
    vus_active: u32,
}

/// A live `ws.connect()` session. The bridge side owns the events channel
/// (JS polls it with `__tropel_k6_ws_step`) and the command channel (JS sends
/// text/ping/close frames via `__tropel_k6_ws_send` / `_ping` / `_close`).
struct WsSession {
    /// Events produced by the background reader task, drained by `step()`.
    events_rx: Receiver<WsEvent>,
    /// Commands (send/ping/close) forwarded to the background writer task.
    cmd_tx: tokio::sync::mpsc::Sender<WsCommand>,
    /// Peer URL (for ws_* metric tags).
    url: String,
    /// Wall-clock when the session started (ws_req_duration).
    start: Instant,
    /// Handshake duration (ws_connecting trend).
    connecting: Duration,
    /// Counters accumulated by the JS-facing bridges (atomics: the session
    /// is shared through an `Arc` across the send/step/finish closures).
    msgs_sent: AtomicU64,
    bytes_sent: AtomicU64,
    msgs_received: AtomicU64,
    bytes_received: AtomicU64,
}

/// A single WebSocket event delivered to JS via `__tropel_k6_ws_step`.
enum WsEvent {
    Open,
    Text(String),
    Binary(usize),
    Ping,
    Pong,
    Close { code: u16, reason: String },
    Error(String),
}

/// A command sent from JS (via the ws bridges) to the background writer task.
enum WsCommand {
    SendText(String),
    Ping,
    Close { code: u16, reason: String },
}

// Safety: each DriverInstance runs on its own VU thread (thread-per-core),
// so it is only ever used from a single thread at a time. JsContext is
// `Send` (see tropel_js) but NOT `Sync` (rquickjs uses a non-atomic Rc), so
// the instance can move to its VU thread but must never be shared across
// threads — which the thread-per-core loop guarantees.
unsafe impl Send for K6DriverInstance {}

/// Lock-free interning for the static http tag keys/values: one `Arc<str>`
/// allocation per key for the whole process instead of one per request.
/// `OnceLock::get_or_init` is a relaxed atomic load after warm-up; the
/// returned clone is a refcount bump, not a string copy.
fn interned(s: &'static str) -> Arc<str> {
    static URL: OnceLock<Arc<str>> = OnceLock::new();
    static METHOD: OnceLock<Arc<str>> = OnceLock::new();
    static STATUS: OnceLock<Arc<str>> = OnceLock::new();
    static NAME: OnceLock<Arc<str>> = OnceLock::new();
    static GROUP: OnceLock<Arc<str>> = OnceLock::new();
    static HTTP: OnceLock<Arc<str>> = OnceLock::new();
    static SCENARIO: OnceLock<Arc<str>> = OnceLock::new();
    match s {
        "url" => URL.get_or_init(|| Arc::from("url")).clone(),
        "method" => METHOD.get_or_init(|| Arc::from("method")).clone(),
        "status" => STATUS.get_or_init(|| Arc::from("status")).clone(),
        "name" => NAME.get_or_init(|| Arc::from("name")).clone(),
        "group" => GROUP.get_or_init(|| Arc::from("group")).clone(),
        "http" => HTTP.get_or_init(|| Arc::from("http")).clone(),
        "scenario" => SCENARIO.get_or_init(|| Arc::from("scenario")).clone(),
        other => Arc::from(other),
    }
}

/// Build the standard http_req_* tag set (url/method/status/name/group).
/// The url value is allocated ONCE and shared by both `url` and `name`
/// (refcount bump, not a second string copy); static keys come from
/// [`interned`]. The scenario tag is stamped here when present, so the
/// drain's scenario pass is a no-op for http samples — it never
/// `Arc::make_mut`-clones the map shared by the 5 per-request samples.
fn http_tags(
    req: &Request,
    status: &str,
    scenario: &Arc<str>,
    extra: Option<&HashMap<String, String>>,
    group: Option<&str>,
) -> TagMap {
    http_tags_for(
        &req.url,
        req.method.as_str(),
        status,
        scenario,
        extra,
        group,
    )
}

/// [`http_tags`] with explicit URL/method (redirect hops reuse it).
///
/// `extra` carries k6 `params.tags` for the request — merged over the
/// defaults so a user `name`/`url`/… tag wins (k6 semantics: params.tags
/// add to AND override the auto tags; the `name` tag is the common case,
/// grouping `http_req_duration` for a specific request).
///
/// `group` is the active group() path (backlog line 154: k6 stamps metrics
/// recorded inside `group("x")` with `group=::x`, not the hardcoded
/// `group=http`). None → default `group=http` (back-compat).
fn http_tags_for(
    url: &str,
    method: &str,
    status: &str,
    scenario: &Arc<str>,
    extra: Option<&HashMap<String, String>>,
    group: Option<&str>,
) -> TagMap {
    let mut tags = TagMap::with_capacity(6);
    let url_arc: Arc<str> = Arc::from(url);
    tags.insert(interned("url"), url_arc.clone());
    tags.insert(interned("method"), Arc::from(method));
    tags.insert(interned("status"), Arc::from(status));
    tags.insert(interned("name"), url_arc);
    tags.insert(
        interned("group"),
        group.map(Arc::from).unwrap_or_else(|| interned("http")),
    );
    if !scenario.is_empty() {
        // Refcount bump — the Arc was created once at bridge registration.
        tags.insert(interned("scenario"), scenario.clone());
    }
    if let Some(extra) = extra {
        for (k, v) in extra {
            tags.insert(k.clone(), v.clone());
        }
    }
    tags
}

/// Record the standard `http_req_*` samples for a completed request.
///
/// Mirrors the declarative runner's tag set and k6's default success
/// semantics: a request is failed unless its status is in 2xx–3xx. `sent`
/// is the request-body byte count (for `data_sent`).
#[allow(clippy::too_many_arguments)] // 10 fields mirror the k6 http_req_* tag/attr set
fn push_http_samples(
    sink: &Mutex<Vec<Sample>>,
    req: &Request,
    status_code: u16,
    duration: Duration,
    size: u64,
    sent: usize,
    timings: Option<&Timings>,
    scenario: &Arc<str>,
    extra_tags: Option<&HashMap<String, String>>,
    group: Option<&str>,
) {
    push_http_samples_for(
        sink,
        &req.url,
        req.method.as_str(),
        status_code,
        duration,
        size,
        sent,
        timings,
        scenario,
        extra_tags,
        group,
    );
}

/// Record http_req_* samples for EVERY redirect hop of a response (k6
/// parity: each hop is its own request — the test.k6.io 302 chain counted
/// 136 http_reqs for 68 iterations while Tropel recorded only the final
/// 64). Called BEFORE the final response's samples so hop order matches k6.
fn push_redirect_hops(
    sink: &Mutex<Vec<Sample>>,
    resp: &Response,
    method: &str,
    scenario: &Arc<str>,
    extra_tags: Option<&HashMap<String, String>>,
    group: Option<&str>,
) {
    for hop in &resp.redirects {
        push_http_samples_for(
            sink,
            &hop.url,
            method,
            hop.status_code,
            hop.response_time,
            hop.size,
            0, // redirect hops carry no request body
            hop.timings.as_ref(),
            scenario,
            extra_tags,
            group,
        );
    }
}

/// Implementation of [`push_http_samples`] with an explicit URL/method so
/// redirect hops (different URL, same method) reuse the same emitter.
#[allow(clippy::too_many_arguments)] // redirect-hop variant of push_http_samples (same field set + scenario)
fn push_http_samples_for(
    sink: &Mutex<Vec<Sample>>,
    url: &str,
    method: &str,
    status_code: u16,
    duration: Duration,
    size: u64,
    sent: usize,
    timings: Option<&Timings>,
    scenario: &Arc<str>,
    extra_tags: Option<&HashMap<String, String>>,
    group: Option<&str>,
) {
    let now = tropel_core::clock::monotonic_wall_now();
    let tags = Arc::new(http_tags_for(
        url,
        method,
        &status_code.to_string(),
        scenario,
        extra_tags,
        group,
    ));

    let is_failed = !(200..400).contains(&status_code);
    let mut v = sink.lock().unwrap();
    v.push(Sample {
        metric: "http_req_duration".into(),
        value: duration.as_secs_f64() * 1000.0,
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Trend,
    });
    // Backlog line 150: the k6 path now emits the same connection-phase
    // sub-timing samples as the declarative runner — real blocked/dns/
    // connecting/waiting/receiving (from reqwest's resolver + connector
    // hooks); tls_handshaking/sending stay 0 (folded into connecting /
    // waiting by reqwest, same as the declarative path). Emitted with the
    // same tags so thresholds like http_req_waiting:p(95) resolve.
    if let Some(t) = timings {
        let sub = [
            ("http_req_blocked", t.blocked),
            ("http_req_dns", t.dns),
            ("http_req_connecting", t.connecting),
            ("http_req_tls_handshaking", t.tls_handshaking),
            ("http_req_sending", t.sending),
            ("http_req_waiting", t.waiting),
            ("http_req_receiving", t.receiving),
        ];
        for (name, dur) in &sub {
            v.push(Sample {
                metric: (*name).into(),
                value: dur.as_secs_f64() * 1000.0,
                tags: tags.clone(),
                timestamp: now,
                sample_type: SampleType::Trend,
            });
        }
    }
    v.push(Sample {
        metric: "http_reqs".into(),
        value: 1.0,
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Counter,
    });
    v.push(Sample {
        metric: "http_req_failed".into(),
        value: if is_failed { 1.0 } else { 0.0 },
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Rate,
    });
    v.push(Sample {
        metric: "data_received".into(),
        value: size as f64,
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Counter,
    });
    v.push(Sample {
        metric: "data_sent".into(),
        value: sent as f64,
        tags,
        timestamp: now,
        sample_type: SampleType::Counter,
    });
}

/// k6 `params.compression`: gzip/deflate the request body (flate2). Returns
/// the compressed bytes, or `None` when the requested algorithm is unsupported
/// (k6 accepts "gzip", "deflate", or both comma-separated; anything else is
/// silently ignored — matching k6, which warns but proceeds uncompressed).
/// Only called when a body exists (the shim always sends one for POST etc.).
fn compress_k6_body(kind: &str, data: &[u8]) -> Option<Vec<u8>> {
    use flate2::write::{DeflateEncoder, GzEncoder};
    use flate2::Compression;
    use std::io::Write;
    if data.is_empty() {
        return None;
    }
    if kind.contains("gzip") {
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(data).ok()?;
        enc.finish().ok()
    } else if kind.contains("deflate") {
        let mut enc = DeflateEncoder::new(Vec::new(), Compression::fast());
        enc.write_all(data).ok()?;
        enc.finish().ok()
    } else {
        None
    }
}

/// k6-style error codes for `res.error_code`. k6 defines a 1xxx series:
/// 1000 generic, 1010 non-TCP network error, 1020 malformed HTTP, 1050
/// timeout, 1100 DNS, 1200 TCP connect, 1300 TLS, 1600 HTTP/2, 1990
/// unknown. reqwest surfaces most failures as strings, so we map by
/// substring (best-effort, k6 parity for the common `if (res.error)` idiom
/// plus `res.error_code` for programmatic branching).
fn k6_error_code(msg: &str) -> i32 {
    let m = msg.to_ascii_lowercase();
    if m.contains("dns") || m.contains("nodename") || m.contains("resolve") {
        1100
    } else if m.contains("timed out") || m.contains("timeout") || m.contains("deadline") {
        1050
    } else if m.contains("tls") || m.contains("certificate") || m.contains("handshake") {
        1300
    } else if m.contains("http2") || m.contains("h2") {
        1600
    } else if m.contains("connect") || m.contains("refused") || m.contains("reset") {
        1200
    } else if m.contains("url") || m.contains("uri") {
        1010
    } else {
        1000
    }
}

/// Send a ws command to the session's writer task without silently dropping
/// frames. `try_send` + a short bounded retry: the writer task lives on the
/// separate I/O runtime (not the VU's reactor), so parking this VU thread for
/// a few ms never deadlocks it — while `blocking_send` would panic inside the
/// VU runtime (it calls `block_on`). Returns false if the session is gone.
fn try_send_cmd(tx: &tokio::sync::mpsc::Sender<WsCommand>, mut cmd: WsCommand) -> bool {
    for _ in 0..50 {
        match tx.try_send(cmd) {
            Ok(()) => return true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(c)) => {
                // Channel full — writer draining; park briefly and retry.
                cmd = c;
                std::thread::sleep(Duration::from_millis(2));
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => return false,
        }
    }
    false
}

/// Record samples for a request that failed at the transport level (timeout,
/// connection refused, …). Failed requests must still appear in the summary:
/// `http_reqs` increments and `http_req_failed` (Rate) becomes 1.0 — matching
/// the declarative runner's error branch and k6 semantics.
fn push_http_failure(
    sink: &Mutex<Vec<Sample>>,
    req: &Request,
    scenario: &Arc<str>,
    extra_tags: Option<&HashMap<String, String>>,
    group: Option<&str>,
) {
    let now = tropel_core::clock::monotonic_wall_now();
    let tags = Arc::new(http_tags(req, "0", scenario, extra_tags, group));

    let mut v = sink.lock().unwrap();
    v.push(Sample {
        metric: "http_reqs".into(),
        value: 1.0,
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Counter,
    });
    v.push(Sample {
        metric: "http_req_failed".into(),
        value: 1.0,
        tags,
        timestamp: now,
        sample_type: SampleType::Rate,
    });
}

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
        if !self.script_bridges_registered {
            self.register_script_bridges();
        }
        if !self.ws_bridges_registered {
            self.register_ws_bridges();
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
        let iter_result = self
            .js_ctx
            .run_script_cached(
                "return __tropel_iteration(__tropel_setup)",
                Some("k6-iteration.js".to_string()),
            )
            .await;

        // Drain samples recorded by the native bridge closures during this
        // iteration (http_req_*, checks, custom metrics) into the VuContext
        // for the engine's metrics pipeline. k6 tags EVERY sample with the
        // active scenario name (this is what makes scenario-scoped thresholds
        // like `http_req_duration{scenario:api_load}` resolve) — stamp it on
        // the drained samples so they match k6 semantics.
        let mut bridge_samples = std::mem::take(&mut *self.sample_sink.lock().unwrap());
        if !ctx.scenario_name.is_empty() {
            let scenario = ctx.scenario_name.clone();
            for s in &mut bridge_samples {
                // Check BEFORE make_mut: the http_req_* samples already carry
                // the scenario tag (stamped at tag-creation time in
                // http_tags_for), so make_mut never fires for them — no 4x
                // deep-clone of the map shared by the 5 per-request samples.
                // Only samples from other bridges (checks, custom metrics —
                // each an independent refcount-1 Arc) reach make_mut here.
                if s.tags.get("scenario").is_none() {
                    let tags = std::sync::Arc::make_mut(&mut s.tags);
                    tags.insert("scenario", scenario.clone());
                }
            }
        }
        ctx.samples.extend(bridge_samples);

        // Surface test.abort() to the engine so the run stops cleanly.
        if let Some(msg) = std::mem::take(&mut *self.abort_requested.lock().unwrap()) {
            ctx.abort(Some(msg));
        }

        // A rejected/thrown default export must fail the iteration (the
        // engine logs it and bumps the error path), not be swallowed.
        if let Err(e) = iter_result {
            tracing::warn!("k6 iteration error: {}", e);
            return Err(tropel_sdk::TropelError::Other(format!(
                "k6 iteration failed: {}",
                e
            )));
        }

        // NOTE: `iteration_duration` is NOT emitted here — the shared VU loop
        // (vu_loop.rs) already emits it as a Trend for every iteration. A
        // duplicate emit here was typed Point, so MetricSet took its type from
        // the first sample (Gauge-like) and the stock k6 threshold
        // `iteration_duration: ['p(95)<2000']` compared against 0 → always
        // PASS. The shared Trend emit is the single source of truth.
        Ok(())
    }
}

/// Build a native JS response object for the k6 shim, avoiding the old
/// Rust → escaped-JSON-string → JS `JSON.parse` round trip (3-4 full body
/// copies: `from_utf8`, `json!`, `.to_string()`, `JSON.parse`, then another
/// `res.json()` parse). `Object::new` + `set` materialize the envelope
/// directly in the JS heap; the body becomes a single JS string. The shim
/// treats a native object and the legacy JSON string identically.
///
/// Backlog line 150: the object now carries REAL `timings` (blocked/dns/
/// connecting/tls_handshaking/sending/waiting/receiving/duration in ms, from
/// the reqwest resolver+connector hooks), k6's `error` ("" on success) /
/// `error_code` (0 on success, 1xxx series on transport failure), and for
/// `responseType: "binary"` the body is a native JS ArrayBuffer instead of a
/// UTF-8 string (binary payloads are no longer silently destroyed).
#[allow(clippy::too_many_arguments)] // response object fields mirror k6's Response shape
fn build_k6_response_object<'js>(
    ctx: &rquickjs::Ctx<'js>,
    code: u16,
    status_text: String,
    body: Vec<u8>,
    headers: &HashMap<String, String>,
    response_time_ms: f64,
    timings: Option<&Timings>,
    error: &str,
    error_code: i32,
    response_type: &str,
) -> rquickjs::Object<'js> {
    let obj = rquickjs::Object::new(ctx.clone()).expect("Object::new for k6 response");
    let _ = obj.set("code", code as i32);
    let _ = obj.set("status", code as i32);
    let _ = obj.set("status_text", status_text);
    let _ = obj.set("error", error);
    let _ = obj.set("error_code", error_code);
    let headers_obj = rquickjs::Object::new(ctx.clone()).expect("Object::new for k6 headers");
    for (k, v) in headers {
        let _ = headers_obj.set(k.as_str(), v.as_str());
    }
    let _ = obj.set("headers", headers_obj);
    let _ = obj.set("response_time", response_time_ms);
    // Real connection-phase timings (k6 keys + Tropel's extra `dns`).
    if let Some(t) = timings {
        let timings_obj = rquickjs::Object::new(ctx.clone()).expect("Object::new for k6 timings");
        let _ = timings_obj.set("blocked", t.blocked.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("dns", t.dns.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("connecting", t.connecting.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("tls_handshaking", t.tls_handshaking.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("sending", t.sending.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("waiting", t.waiting.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("receiving", t.receiving.as_secs_f64() * 1000.0);
        let _ = timings_obj.set("duration", response_time_ms);
        let _ = obj.set("timings", timings_obj);
    }
    // `responseType: "binary"` → native ArrayBuffer body (raw bytes survive);
    // otherwise UTF-8 text. `none` (k6) sends an empty body.
    if response_type.eq_ignore_ascii_case("binary") {
        let ab =
            rquickjs::ArrayBuffer::new(ctx.clone(), body).expect("ArrayBuffer for k6 binary body");
        let _ = obj.set("body", ab);
    } else {
        let _ = obj.set("body", String::from_utf8_lossy(&body).into_owned());
    }
    obj
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

        let sink = self.sample_sink.clone();
        // The bridge closures must name the QuickJS lifetime `'js` in their
        // signatures (to return a native `Object<'js>`), which a closure
        // cannot declare — the registration lives in the generic free fn
        // below. Behavior is identical; only the marshalling changes.
        let (deadline, max_exec) = self.js_ctx.interrupt_deadline_handle();
        self.js_ctx.with_ctx(|rq_ctx| {
            register_http_bridges(
                rq_ctx,
                http_client.clone(),
                sink.clone(),
                &ctx.scenario_name,
                self.group_stack.clone(),
                deadline,
                max_exec,
            );
        });

        self.http_bridge_registered = true;
        tracing::debug!("K6Driver: registered __tropel_k6_http_request native bridge");
    }
}

/// Register the native HTTP bridges (`__tropel_k6_http_request` /
/// `__tropel_k6_http_batch`) on a per-VU QuickJS context. Lives in a generic
/// fn — not an inline closure — because the request bridge's closure must
/// name the QuickJS lifetime `'js` in its signature to return a native
/// `Object<'js>` (eliminating the escaped-JSON-string round trip), and
/// closures cannot declare lifetimes.
fn register_http_bridges<'js>(
    rq_ctx: &rquickjs::Ctx<'js>,
    http_client: Arc<dyn DriverHttpClient + Send + Sync>,
    sink: Arc<Mutex<Vec<Sample>>>,
    scenario: &str,
    group_stack: Arc<Mutex<Vec<String>>>,
    deadline: Arc<AtomicU64>,
    max_exec: Duration,
) {
    let globals = rq_ctx.globals();
    let http_client_request = http_client.clone();
    let sink_req = sink.clone();
    // The scenario name is constant per VU — capture it once here (as an
    // Arc<str> so http tag builders stamp it at CREATION time via a refcount
    // bump, zero per-request alloc) so the drain then skips these samples
    // entirely: no Arc::make_mut, no 4x deep-clone of the map shared by the
    // 5 per-request samples.
    let scenario_req = Arc::from(scenario);
    // Backlog line 154: current group() path (::a::b) read from the per-VU
    // group stack at REQUEST time, so http_req_* samples recorded inside a
    // group carry `group=::path` instead of the hardcoded `group=http`.
    let group_stack_req = group_stack.clone();
    // The blocking HTTP call burns wall time; re-arm the per-eval JS
    // deadline after it (backlog line 104).
    let deadline_req = deadline.clone();
    let max_exec_req = max_exec;
    let _ = globals.set(
        "__tropel_k6_http_request",
        Func::from(
            move |ctx: rquickjs::Ctx<'js>,
                  method: String,
                  url: String,
                  headers_json: String,
                  body: String,
                  response_type: String,
                  extras_json: String|
                  -> rquickjs::Object<'js> {
                // Backlog line 140: the shim packs per-request
                // params.timeout/tags/auth/redirects/compression into
                // ONE JSON string (the bridge closure is arity-capped
                // at ctx + 6 script args). Unpack them here and apply
                // each: timeout bounds the request, tags merge into
                // the http_req_* sample tags, auth becomes
                // Request.auth (the client impl builds the signer),
                // redirects controls follow_redirects, and compression
                // gzip/deflates the request body.
                let extras: serde_json::Value =
                    serde_json::from_str(&extras_json).unwrap_or(serde_json::Value::Null);
                let timeout_ms: f64 = extras
                    .get("timeoutMs")
                    .and_then(|t| t.as_f64())
                    .unwrap_or(0.0);
                let extra_tags: HashMap<String, String> = extras
                    .get("tags")
                    .and_then(|t| t.as_object())
                    .map(|o| {
                        o.iter()
                            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                            .collect()
                    })
                    .unwrap_or_default();
                let auth: Option<AuthConfig> = extras
                    .get("auth")
                    .filter(|a| !a.is_null())
                    .and_then(|a| serde_json::from_value(a.clone()).ok());
                // k6 params.redirects: 0 = don't follow, -1 = follow
                // all, N = follow up to N hops. Our Request only has a
                // bool; N>0 and -1 both mean follow.
                let redirects: i64 = extras
                    .get("redirects")
                    .and_then(|r| r.as_i64())
                    .unwrap_or(-1);
                let compression: String = extras
                    .get("compression")
                    .and_then(|c| c.as_str())
                    .unwrap_or("")
                    .to_string();
                // Backlog line 150: the shim base64-encodes binary
                // request bodies (ArrayBuffer / Uint8Array / http.file
                // data) and flags bodyB64 — the bridge is arity-capped
                // at String, so raw bytes ride as base64 and are
                // decoded here to Body::Binary.
                let body_b64: bool = extras
                    .get("bodyB64")
                    .and_then(|b| b.as_bool())
                    .unwrap_or(false);

                let mut headers = parse_headers_tolerant(&headers_json);
                // k6 params.compression: "gzip" / "deflate" — compress
                // the request body and stamp Content-Encoding. Only
                // when a body exists (k6 compresses the body, and an
                // empty-body GET has nothing to compress).
                let mut req_body: Option<Body> = if body.is_empty() {
                    None
                } else if body_b64 {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD
                        .decode(&body)
                        .ok()
                        .map(Body::Binary)
                } else {
                    Some(Body::Raw(body))
                };
                if !compression.is_empty() {
                    if let Some(Body::Raw(text)) = &req_body {
                        if let Some(compressed) = compress_k6_body(&compression, text.as_bytes()) {
                            req_body = Some(Body::Binary(compressed));
                            headers.insert(
                                "Content-Encoding".to_string(),
                                if compression.contains("gzip") {
                                    "gzip".to_string()
                                } else {
                                    "deflate".to_string()
                                },
                            );
                        }
                    }
                }
                // A genuinely invalid method token must not silently
                // become GET (a write-path "PURGE" must not degrade
                // into a read-path GET that reports green). Surfaced
                // as a status-0 error response the shim returns to
                // the script (checks fail, http_req_failed counts).
                // Valid-but-uncommon tokens (PURGE/LINK/…)
                // parse fine via Method::Custom.
                // NOTE: inside the let-else else-block, `method` still
                // refers to the original String (the parsed binding is
                // only visible after the statement).
                let Some(parsed_method) = Method::parse(&method) else {
                    return build_k6_response_object(
                        &ctx,
                        0,
                        format!("invalid HTTP method {}", method),
                        Vec::new(),
                        &HashMap::new(),
                        0.0,
                        None,
                        &format!("invalid HTTP method {}", method),
                        1000,
                        &response_type,
                    );
                };
                let req = Request {
                    url,
                    method: parsed_method,
                    headers,
                    query_params: HashMap::new(),
                    body: req_body,
                    auth,
                    certificate: None,
                    follow_redirects: redirects != 0,
                    // Backlog line 140: timeout was parsed by the shim
                    // then DISCARDED here (`_timeout_ms`) — wire it
                    // through so a per-request timeout actually bounds
                    // the request.
                    timeout: if timeout_ms > 0.0 {
                        Some(Duration::from_millis(timeout_ms as u64))
                    } else {
                        None
                    },
                    response_type: tropel_sdk::ResponseType::from_k6(&response_type),
                };
                // Execute on the dedicated I/O runtime via the shared
                // blocking helper — safe from inside ctx.with on a
                // current-thread VU runtime. No block_on here: that
                // deadlocks the VU's own reactor.
                // Clone the request into the 'static I/O future; the
                // original stays alive for sample-tag construction.
                let req_for_io = req.clone();
                let http_for_io = http_client_request.clone();
                let result = tropel_http::blocking::execute_blocking(async move {
                    http_for_io.execute(&req_for_io).await
                });
                // The HTTP call burned wall time; re-arm the per-eval JS
                // deadline so slow requests don't trip the interrupt on
                // resume (backlog line 104).
                tropel_js::rearm_deadline(&deadline_req, max_exec_req);
                match result {
                    Ok(resp) => {
                        // Record the standard http_req_* samples so
                        // the summary/thresholds see this request
                        // (mirrors the declarative runner + WASM
                        // driver). The req body is counted for
                        // data_sent; k6 semantics: 2xx-3xx success.
                        // Exact wire size via the SINGLE serializer
                        // (percent-encoded urlencoded, multipart
                        // framing) — the deleted Body::encoded_len
                        // measured raw k=v&k=v with no encoding.
                        let sent = req.body.as_ref().map(tropel_http::body_size).unwrap_or(0);
                        // k6 parity: every redirect hop counts as its
                        // own request (test.k6.io 302 chain = 2 reqs
                        // per iteration, not 1).
                        let group = group_stack_req.lock().unwrap().last().cloned();
                        push_redirect_hops(
                            &sink_req,
                            &resp,
                            req.method.as_str(),
                            &scenario_req,
                            Some(&extra_tags),
                            group.as_deref(),
                        );
                        push_http_samples(
                            &sink_req,
                            &req,
                            resp.status_code,
                            resp.response_time,
                            resp.size,
                            sent,
                            resp.timings.as_ref(),
                            &scenario_req,
                            Some(&extra_tags),
                            group.as_deref(),
                        );
                        build_k6_response_object(
                            &ctx,
                            resp.status_code,
                            resp.status_text,
                            resp.body,
                            &resp.headers,
                            resp.response_time.as_secs_f64() * 1000.0,
                            resp.timings.as_ref(),
                            "",
                            0,
                            &response_type,
                        )
                    }
                    Err(e) => {
                        tracing::debug!("k6 http request failed: {}", e);
                        let group = group_stack_req.lock().unwrap().last().cloned();
                        push_http_failure(
                            &sink_req,
                            &req,
                            &scenario_req,
                            Some(&extra_tags),
                            group.as_deref(),
                        );
                        let err = e.to_string();
                        build_k6_response_object(
                            &ctx,
                            0,
                            format!("HTTP error: {}", err),
                            Vec::new(),
                            &HashMap::new(),
                            0.0,
                            None,
                            &err,
                            k6_error_code(&err),
                            &response_type,
                        )
                    }
                }
            },
        ),
    );

    let batch_sink = sink.clone();
    let scenario_batch = Arc::from(scenario);
    let group_stack_batch = group_stack.clone();
    let deadline_batch = deadline.clone();
    let max_exec_batch = max_exec;
    let _ = globals.set(
        "__tropel_k6_http_batch",
        Func::from(move |requests_json: String| -> String {
            let batch_requests: Vec<serde_json::Value> =
                serde_json::from_str(&requests_json).unwrap_or_default();

            let http_for_io = http_client.clone();
            let futures = batch_requests.into_iter().map(move |entry| {
                let key = entry
                    .get("key")
                    .cloned()
                    .unwrap_or_else(|| serde_json::Value::String(String::new()));
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
                let mut headers = parse_headers_tolerant(headers_json);
                let body = entry
                    .get("body")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                // Backlog line 140: batch entries carry the same
                // per-request params.tags/auth/redirects/compression
                // as the single-request bridge (the shim packs them
                // into the entry object).
                let extra_tags: HashMap<String, String> = entry
                    .get("tags_json")
                    .and_then(|v| v.as_str())
                    .and_then(|s| serde_json::from_str(s).ok())
                    .unwrap_or_default();
                // auth_json is DOUBLE-ENCODED: the shim sends
                // JSON.stringify(auth) — a JSON string whose CONTENT is
                // the tagged auth object. from_value(Value::String)
                // would try to deserialize the internally-tagged
                // AuthConfig enum from a string and fail silently
                // (auth → None, the exact silent-drop this backlog
                // line kills) — read the inner string instead.
                let auth: Option<AuthConfig> = entry
                    .get("auth_json")
                    .and_then(|v| v.as_str())
                    .filter(|s| *s != "null")
                    .and_then(|s| serde_json::from_str(s).ok());
                let redirects: i64 = entry
                    .get("redirects")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(-1);
                let compression: String = entry
                    .get("compression")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                // Backlog line 150: binary batch bodies arrive base64
                // (the shim flags body_b64), decoded here to raw bytes.
                let body_b64: bool = entry
                    .get("body_b64")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false);
                let mut request_body: Option<Body> = if body.is_empty() {
                    None
                } else if body_b64 {
                    use base64::Engine as _;
                    base64::engine::general_purpose::STANDARD
                        .decode(&body)
                        .ok()
                        .map(Body::Binary)
                } else {
                    Some(Body::Raw(body))
                };
                if !compression.is_empty() {
                    if let Some(Body::Raw(text)) = &request_body {
                        if let Some(compressed) = compress_k6_body(&compression, text.as_bytes()) {
                            request_body = Some(Body::Binary(compressed));
                            headers.insert(
                                "Content-Encoding".to_string(),
                                if compression.contains("gzip") {
                                    "gzip".to_string()
                                } else {
                                    "deflate".to_string()
                                },
                            );
                        }
                    }
                }
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
                // Same loud-failure guard as the first bridge: an
                // invalid method token must not silently become GET.
                // This closure must return a UNIFORM future type, so
                // the invalid-method case is carried as an immediate
                // Err result inside the async block (never executed)
                // instead of a String — the caller maps it to a
                // status-0 response + failure samples.
                let parsed_method = Method::parse(&method);
                let req = Request {
                    url,
                    method: parsed_method
                        .clone()
                        .unwrap_or_else(|| Method::Custom(method.clone())),
                    headers,
                    query_params: HashMap::new(),
                    body: request_body,
                    auth,
                    certificate: None,
                    follow_redirects: redirects != 0,
                    timeout,
                    response_type: tropel_sdk::ResponseType::from_k6(response_type),
                };
                let http_client = http_for_io.clone();
                async move {
                    let resp = match parsed_method {
                        Some(_) => http_client.execute(&req).await,
                        None => Err(TropelError::Other(format!(
                            "invalid HTTP method {}",
                            method
                        ))),
                    };
                    (key, req, resp, extra_tags)
                }
            });

            let responses = tropel_http::blocking::execute_blocking(async move {
                let results = join_all(futures).await;
                Ok(results)
            });
            // The batch burned wall time; re-arm the per-eval JS deadline
            // (backlog line 104).
            tropel_js::rearm_deadline(&deadline_batch, max_exec_batch);

            let mut response_map = serde_json::Map::new();
            if let Ok(results) = responses {
                for (key, req, result, extra_tags) in results {
                    let key_str = match key {
                        serde_json::Value::String(s) => s,
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        other => serde_json::to_string(&other).unwrap_or_default(),
                    };
                    let entry_resp = match result {
                        Ok(resp) => {
                            // Record the standard http_req_* samples
                            // for each batch request (mirrors the
                            // single-request bridge).
                            // Exact wire size via the SINGLE
                            // serializer (see single-request path).
                            let sent = req.body.as_ref().map(tropel_http::body_size).unwrap_or(0);
                            // k6 parity: every redirect hop counts as
                            // its own request, same as the single-
                            // request path.
                            let group = group_stack_batch.lock().unwrap().last().cloned();
                            push_redirect_hops(
                                &batch_sink,
                                &resp,
                                req.method.as_str(),
                                &scenario_batch,
                                Some(&extra_tags),
                                group.as_deref(),
                            );
                            push_http_samples(
                                &batch_sink,
                                &req,
                                resp.status_code,
                                resp.response_time,
                                resp.size,
                                sent,
                                resp.timings.as_ref(),
                                &scenario_batch,
                                Some(&extra_tags),
                                group.as_deref(),
                            );
                            // Backlog line 150: batch responses now
                            // carry real timings + error/error_code
                            // (k6 parity), and binary bodies arrive
                            // base64 (body_b64) since JSON can't hold
                            // raw bytes — the shim decodes to an
                            // ArrayBuffer. Timings serialize to ms.
                            let mut resp_json = serde_json::json!({
                                "code": resp.status_code,
                                "status": resp.status_code,
                                "status_text": resp.status_text,
                                "headers": resp.headers,
                                "response_time": resp.response_time.as_secs_f64() * 1000.0,
                                "error": "",
                                "error_code": 0,
                            });
                            if let Some(t) = &resp.timings {
                                resp_json["timings"] = serde_json::json!({
                                    "blocked": t.blocked.as_secs_f64() * 1000.0,
                                    "dns": t.dns.as_secs_f64() * 1000.0,
                                    "connecting": t.connecting.as_secs_f64() * 1000.0,
                                    "tls_handshaking": t.tls_handshaking.as_secs_f64() * 1000.0,
                                    "sending": t.sending.as_secs_f64() * 1000.0,
                                    "waiting": t.waiting.as_secs_f64() * 1000.0,
                                    "receiving": t.receiving.as_secs_f64() * 1000.0,
                                    "duration": resp.response_time.as_secs_f64() * 1000.0,
                                });
                            }
                            // `response_type` (a &str borrowed from
                            // `entry`) is not capturable into this
                            // async block; the Request already carries
                            // the parsed ResponseType — use it.
                            if req.response_type == tropel_sdk::ResponseType::Binary {
                                use base64::Engine as _;
                                resp_json["body"] = serde_json::Value::String(
                                    base64::engine::general_purpose::STANDARD.encode(&resp.body),
                                );
                                resp_json["body_b64"] = serde_json::Value::Bool(true);
                            } else {
                                resp_json["body"] = serde_json::Value::String(
                                    String::from_utf8_lossy(&resp.body).into_owned(),
                                );
                            }
                            resp_json
                        }
                        Err(e) => {
                            tracing::debug!("k6 batch request failed: {}", e);
                            let group = group_stack_batch.lock().unwrap().last().cloned();
                            push_http_failure(
                                &batch_sink,
                                &req,
                                &scenario_batch,
                                Some(&extra_tags),
                                group.as_deref(),
                            );
                            let err = e.to_string();
                            serde_json::json!({
                                "code": 0,
                                "status": 0,
                                "status_text": format!("HTTP error: {}", err),
                                "body": "",
                                "headers": {},
                                "response_time": 0,
                                "error": err,
                                "error_code": k6_error_code(&err),
                            })
                        }
                    };
                    response_map.insert(key_str, entry_resp);
                }
            }

            serde_json::Value::Object(response_map).to_string()
        }),
    );
}

impl K6DriverInstance {
    /// Lazily register the script-state bridges (`__tropel_pm_test`,
    /// `__tropel_pm_custom_metric_add`, `__tropel_exec_*`, `__tropel_test_abort`).
    /// The k6 driver doesn't depend on tropel-pm (which installs these for the
    /// declarative path), so it installs its own equivalents backed by the
    /// per-VU sample_sink / exec_state / abort flag.
    fn register_script_bridges(&mut self) {
        let sink = self.sample_sink.clone();
        let exec_state = self.exec_state.clone();
        let abort = self.abort_requested.clone();

        self.js_ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // check() / pm.test() → checks Rate sample
            let sink_test = sink.clone();
            let _ = globals.set(
                "__tropel_pm_test",
                // 3rd arg: optional k6 check() tags JSON (backlog line 149).
                Func::from(
                    move |name: String, passed: bool, tags_json: Option<String>| {
                        let mut v = sink_test.lock().unwrap();
                        let now = tropel_core::clock::monotonic_wall_now();
                        let mut tags = TagMap::with_capacity(2);
                        tags.insert("check", name);
                        if let Some(j) = tags_json {
                            if let Ok(extra) = serde_json::from_str::<HashMap<String, String>>(&j) {
                                for (k, val) in extra {
                                    tags.insert(k, val);
                                }
                            }
                        }
                        v.push(Sample {
                            metric: "checks".into(),
                            value: if passed { 1.0 } else { 0.0 },
                            tags: Arc::new(tags),
                            timestamp: now,
                            sample_type: SampleType::Rate,
                        });
                    },
                ),
            );

            // Custom metric .add() → typed sample (Counter/Gauge/Rate/Trend).
            // Backlog line 154: the 5th arg carries k6's `isTime` flag — when
            // true the metric name is registered in the tropel-metrics time
            // registry so json-stream stamps `contains: "time"` and stdout
            // renders it in ms (k6's `new Trend('x', true)` behavior).
            let sink_metric = sink.clone();
            let _ = globals.set(
                "__tropel_pm_custom_metric_add",
                Func::from(
                    move |name: String,
                          value: f64,
                          tags_json: String,
                          metric_type_str: String,
                          is_time: bool| {
                        if is_time {
                            tropel_metrics::time_metrics::register(&name);
                        }
                        let tags = if tags_json.is_empty() || tags_json == "{}" {
                            TagMap::new()
                        } else {
                            let parsed: HashMap<String, String> =
                                serde_json::from_str(&tags_json).unwrap_or_default();
                            TagMap::from_pairs(parsed)
                        };
                        let sample_type = match metric_type_str.as_str() {
                            "counter" => SampleType::Counter,
                            "gauge" => SampleType::Point,
                            "rate" => SampleType::Rate,
                            _ => SampleType::Trend,
                        };
                        let mut v = sink_metric.lock().unwrap();
                        v.push(Sample {
                            metric: name.into(),
                            value,
                            tags: Arc::new(tags),
                            timestamp: tropel_core::clock::monotonic_wall_now(),
                            sample_type,
                        });
                    },
                ),
            );

            // exec.scenario.name / executor (value properties — line 141)
            let es_name = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_scenario_name",
                Func::from(move || es_name.lock().unwrap().scenario_name.clone()),
            );
            let es_executor = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_scenario_executor",
                Func::from(move || es_executor.lock().unwrap().executor_name.clone()),
            );

            // exec.vu.idInTest() / iterationInScenario()
            let es_vu = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_vu_id",
                Func::from(move || es_vu.lock().unwrap().vu_id + 1),
            );
            let es_iter = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_iteration",
                Func::from(move || es_iter.lock().unwrap().iteration),
            );

            // exec.instance.iterationsCompleted() / vusActive()
            let es_completed = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_iterations_completed",
                Func::from(move || es_completed.lock().unwrap().iterations_completed),
            );
            let es_vus = exec_state.clone();
            let _ = globals.set(
                "__tropel_exec_vus_active",
                Func::from(move || es_vus.lock().unwrap().vus_active),
            );

            // group() → group_duration Trend sample (duration in ms). The
            // shim's group() wraps fn() between __tropel_pm_group_start/end.
            // Backlog line 154: the START bridge pushes the name onto the
            // per-VU group stack (http bridges read the top when stamping
            // http_req_* tags), and END pops it — the two must balance or
            // the stack leaks across iterations. The group_duration sample
            // keeps its k6-style `group=<name>` tag.
            let sink_group = sink.clone();
            let group_start = self.group_stack.clone();
            let _ = globals.set(
                "__tropel_pm_group_start",
                Func::from(move |name: String| {
                    group_start.lock().unwrap().push(name);
                }),
            );
            let group_end = self.group_stack.clone();
            let _ = globals.set(
                "__tropel_pm_group_end",
                Func::from(move |name: String, duration_ms: f64| {
                    if let Some(top) = group_end.lock().unwrap().pop() {
                        // Defensive: the JS shim always calls end with the
                        // matching name; if a script mismatches, don't lose
                        // the whole stack — just drop the stale top.
                        if top != name {
                            tracing::debug!("k6 group_end mismatch: top={} got={}", top, name);
                        }
                    }
                    let mut v = sink_group.lock().unwrap();
                    let mut tags = TagMap::with_capacity(1);
                    tags.insert("group", name);
                    v.push(Sample {
                        metric: "group_duration".into(),
                        value: duration_ms, // ms — the public unit (k6 semantics)
                        tags: Arc::new(tags),
                        timestamp: tropel_core::clock::monotonic_wall_now(),
                        sample_type: SampleType::Trend,
                    });
                }),
            );

            // test.abort(msg)
            let abort_flag = abort.clone();
            let _ = globals.set(
                "__tropel_test_abort",
                Func::from(move |msg: String| {
                    *abort_flag.lock().unwrap() = Some(msg);
                }),
            );
        });

        self.script_bridges_registered = true;
        tracing::debug!("K6Driver: registered script-state bridges");
    }

    /// Lazily register the WebSocket bridges backing the k6-shim's `ws.*`
    /// event-driven API (`ws.connect` + `socket.on('open'|'message'|'close')`).
    ///
    /// Bridge contract (see `js/k6-shim/k6-shim.js`):
    /// - `__tropel_k6_ws_connect(url, headers_json) -> {id, error}` opens the
    ///   connection (blocking, on the dedicated I/O runtime) and returns a
    ///   session id.
    /// - `__tropel_k6_ws_step(id, timeout_ms) -> {type, data?, code?, reason?}`
    ///   blocks up to timeout_ms for the next event (open/message/close/error/
    ///   ping/pong). Each call also resets the per-eval interrupt deadline so a
    ///   long-lived session isn't killed by the eval timeout.
    /// - `__tropel_k6_ws_send(id, data)` / `_ping(id)` / `_close(id, code,
    ///   reason)` forward frames to the background writer task.
    /// - `__tropel_k6_ws_finish(id)` tears the session down and emits its
    ///   `ws_*` samples into the sample_sink (same metric names as the
    ///   declarative WebSocket protocol extension).
    // `#[allow]`: WsSession (with its std::sync::mpsc::Receiver) is !Sync,
    // so Arc::new(WsSession { .. }) is not Send+Sync — but the session
    // registry is confined to this VU's own thread (thread-per-core; see
    // the unsafe impl Send/Sync for K6DriverInstance below).
    #[allow(clippy::arc_with_non_send_sync)]
    fn register_ws_bridges(&mut self) {
        let sessions = self.ws_sessions.clone();
        let next_id = self.ws_next_id.clone();
        let sink = self.sample_sink.clone();
        let (deadline, max_exec) = self.js_ctx.interrupt_deadline_handle();

        self.js_ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // ── ws.connect(url, headers_json) -> {id, error} ──
            let sessions_conn = sessions.clone();
            let next_id_conn = next_id.clone();
            let deadline_conn = deadline.clone();
            let max_exec_conn = max_exec;
            let _ = globals.set(
                "__tropel_k6_ws_connect",
                Func::from(
                    move |url: String, headers_json: String| -> String {
                        let headers = parse_headers_tolerant(&headers_json);
                        // Build the handshake request with the given headers.
                        let mut handshake = match url.clone().into_client_request() {
                            Ok(r) => r,
                            Err(e) => {
                                return serde_json::json!({
                                    "id": 0,
                                    "error": format!("invalid ws url '{url}': {e}"),
                                })
                                .to_string();
                            }
                        };
                        for (k, v) in &headers {
                            if let (Ok(hname), Ok(hv)) = (
                                http::HeaderName::from_bytes(k.as_bytes()),
                                http::HeaderValue::from_str(v),
                            ) {
                                handshake.headers_mut().insert(hname, hv);
                            }
                        }

                        let id = next_id_conn.fetch_add(1, Ordering::Relaxed) + 1;
                        let (events_tx, events_rx) = std::sync::mpsc::channel::<WsEvent>();
                        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<WsCommand>(64);
                        let connect_start = Instant::now();

                        // Connect on the dedicated I/O runtime (safe from inside
                        // ctx.with on a current-thread VU runtime), then spawn a
                        // background reader/writer task that owns the socket and
                        // streams events into the channel. The task lives on the
                        // I/O runtime, so blocking this VU thread on recv_timeout
                        // never deadlocks a VU reactor.
                        // `events_tx` is cloned for the reader task: the outer
                        // future keeps the original to deliver the Open event
                        // before returning, so the two don't fight over ownership.
                        let events_tx_reader = events_tx.clone();
                        let url_err = url.clone();
                        let connect_result = tropel_http::blocking::execute_blocking(
                            async move {
                                let (ws, _resp) =
                                    tokio_tungstenite::connect_async(handshake)
                                        .await
                                        .map_err(|e| {
                                            TropelError::Extension(format!(
                                                "WebSocket connect to '{}': {}",
                                                url_err, e
                                            ))
                                        })?;
                                let connecting = connect_start.elapsed();
                                let (sink, stream) = ws.split();

                                tokio::spawn(async move {
                                    let mut sink = sink;
                                    let mut stream = stream;
                                    let mut cmd_rx = cmd_rx;
                                    loop {
                                        tokio::select! {
                                            cmd = cmd_rx.recv() => match cmd {
                                                Some(WsCommand::SendText(t)) => {
                                                    if sink.send(Message::Text(t.into())).await.is_err() {
                                                        break;
                                                    }
                                                }
                                                Some(WsCommand::Ping) => {
                                                    if sink.send(Message::Ping(Vec::new().into())).await.is_err() {
                                                        break;
                                                    }
                                                }
                                                Some(WsCommand::Close { code, reason }) => {
                                                    // SplitSink has no inherent close();
                                                    // send a Close frame instead.
                                                    let _ = sink
                                                        .send(Message::Close(Some(CloseFrame {
                                                            code: CloseCode::from(code),
                                                            reason: reason.into(),
                                                        })))
                                                        .await;
                                                    break;
                                                }
                                                None => break,
                                            },
                                            msg = stream.next() => match msg {
                                                Some(Ok(Message::Text(t))) => {
                                                    if events_tx_reader
                                                        .send(WsEvent::Text(t.to_string()))
                                                        .is_err()
                                                    {
                                                        break;
                                                    }
                                                }
                                                Some(Ok(Message::Binary(b))) => {
                                                    if events_tx_reader
                                                        .send(WsEvent::Binary(b.len()))
                                                        .is_err()
                                                    {
                                                        break;
                                                    }
                                                }
                                                Some(Ok(Message::Ping(_))) => {
                                                    if events_tx_reader.send(WsEvent::Ping).is_err() {
                                                        break;
                                                    }
                                                }
                                                Some(Ok(Message::Pong(_))) => {
                                                    if events_tx_reader.send(WsEvent::Pong).is_err() {
                                                        break;
                                                    }
                                                }
                                                Some(Ok(Message::Frame(_))) => {} // raw frame passthrough

                                                Some(Ok(Message::Close(f))) => {
                                                    let (code, reason) = f
                                                        .map(|f| {
                                                            (u16::from(f.code), f.reason.to_string())
                                                        })
                                                        .unwrap_or((1000, String::new()));
                                                    let _ = events_tx_reader.send(WsEvent::Close {
                                                        code,
                                                        reason,
                                                    });
                                                    break;
                                                }
                                                Some(Err(e)) => {
                                                    let _ = events_tx_reader.send(
                                                        WsEvent::Error(e.to_string()),
                                                    );
                                                    break;
                                                }
                                                None => {
                                                    let _ = events_tx_reader.send(WsEvent::Close {
                                                        code: 1006,
                                                        reason: "connection closed".into(),
                                                    });
                                                    break;
                                                }
                                            },
                                        }
                                    }
                                });

                                // Open is delivered as the first event once the
                                // handshake completes (step() returns it).
                                let _ = events_tx.send(WsEvent::Open);
                                Ok::<Duration, TropelError>(connecting)
                            },
                        );

                        // The connect burned wall time; re-arm the per-eval JS
                        // deadline so a slow handshake doesn't trip the
                        // interrupt on resume (backlog line 104).
                        tropel_js::rearm_deadline(&deadline_conn, max_exec_conn);
                        match connect_result {
                            Ok(connecting) => {
                                sessions_conn.lock().unwrap().insert(
                                    id,
                                    Arc::new(WsSession {
                                        events_rx,
                                        cmd_tx,
                                        url: url.clone(),
                                        start: Instant::now(),
                                        connecting,
                                        msgs_sent: AtomicU64::new(0),
                                        bytes_sent: AtomicU64::new(0),
                                        msgs_received: AtomicU64::new(0),
                                        bytes_received: AtomicU64::new(0),
                                    }),
                                );
                                serde_json::json!({ "id": id, "error": null }).to_string()
                            }
                            Err(e) => serde_json::json!({
                                "id": 0,
                                "error": e.to_string(),
                            })
                            .to_string(),
                        }
                    },
                ),
            );

            // ── ws step(id, timeout_ms) -> event JSON ──
            let sessions_step = sessions.clone();
            let deadline_step = deadline.clone();
            let _ = globals.set(
                "__tropel_k6_ws_step",
                Func::from(
                    move |id: u64, timeout_ms: f64| -> String {
                        // Reset the per-eval interrupt deadline so a long ws
                        // session isn't killed by the eval timeout mid-pump.
                        // Same MONOTONIC base as JsContext's interrupt handler
                        // (backlog P3: an NTP step must not kill a script).
                        // Same arithmetic as the sleep/HTTP/WS-connect bridges
                        // (shared monotonic-clock helper, backlog line 104).
                        tropel_js::rearm_deadline(&deadline_step, max_exec);

                        let timeout = Duration::from_millis(timeout_ms.max(1.0) as u64);
                        let guard = sessions_step.lock().unwrap();
                        let Some(session) = guard.get(&id).cloned() else {
                            return serde_json::json!({
                                "type": "close",
                                "code": 1006,
                                "reason": "session not found",
                            })
                            .to_string();
                        };
                        drop(guard);
                        match session.events_rx.recv_timeout(timeout) {
                            Ok(WsEvent::Open) => {
                                serde_json::json!({"type": "open"}).to_string()
                            }
                            Ok(WsEvent::Text(t)) => {
                                session.msgs_received.fetch_add(1, Ordering::Relaxed);
                                session.bytes_received.fetch_add(t.len() as u64, Ordering::Relaxed);
                                serde_json::json!({"type": "message", "data": t}).to_string()
                            }
                            Ok(WsEvent::Binary(n)) => {
                                session.msgs_received.fetch_add(1, Ordering::Relaxed);
                                session.bytes_received.fetch_add(n as u64, Ordering::Relaxed);
                                serde_json::json!({
                                    "type": "message",
                                    "data": format!("<binary {n} bytes>"),
                                })
                                .to_string()
                            }
                            Ok(WsEvent::Ping) => {
                                serde_json::json!({"type": "ping"}).to_string()
                            }
                            Ok(WsEvent::Pong) => {
                                serde_json::json!({"type": "pong"}).to_string()
                            }
                            Ok(WsEvent::Close { code, reason }) => serde_json::json!({
                                "type": "close",
                                "code": code,
                                "reason": reason,
                            })
                            .to_string(),
                            Ok(WsEvent::Error(m)) => serde_json::json!({
                                "type": "error",
                                "message": m,
                            })
                            .to_string(),
                            Err(RecvTimeoutError::Timeout) => {
                                serde_json::json!({"type": "none"}).to_string()
                            }
                            Err(RecvTimeoutError::Disconnected) => serde_json::json!({
                                "type": "close",
                                "code": 1006,
                                "reason": "connection closed",
                            })
                            .to_string(),
                        }
                    },
                ),
            );

            // ── ws send / ping / close ──
            // `try_send` + a bounded retry: the writer task lives on the
            // separate I/O runtime (NOT this VU's reactor), so parking this
            // VU thread briefly never deadlocks it — and no frame is silently
            // dropped under a send burst. `blocking_send` is NOT used: it
            // block_on's and would panic inside the VU runtime.
            let sessions_send = sessions.clone();
            let _ = globals.set(
                "__tropel_k6_ws_send",
                Func::from(
                    move |id: u64, data: String| -> String {
                        let guard = sessions_send.lock().unwrap();
                        let Some(session) = guard.get(&id).cloned() else {
                            return serde_json::json!({"ok": false}).to_string();
                        };
                        drop(guard);
                        session.msgs_sent.fetch_add(1, Ordering::Relaxed);
                        session.bytes_sent.fetch_add(data.len() as u64, Ordering::Relaxed);
                        let ok = try_send_cmd(&session.cmd_tx, WsCommand::SendText(data));
                        serde_json::json!({"ok": ok}).to_string()
                    },
                ),
            );
            let sessions_ping = sessions.clone();
            let _ = globals.set(
                "__tropel_k6_ws_ping",
                Func::from(
                    move |id: u64| -> String {
                        let guard = sessions_ping.lock().unwrap();
                        let Some(session) = guard.get(&id).cloned() else {
                            return serde_json::json!({"ok": false}).to_string();
                        };
                        drop(guard);
                        let ok = try_send_cmd(&session.cmd_tx, WsCommand::Ping);
                        serde_json::json!({"ok": ok}).to_string()
                    },
                ),
            );
            let sessions_close = sessions.clone();
            let _ = globals.set(
                "__tropel_k6_ws_close",
                Func::from(
                    move |id: u64, code: f64, reason: String| -> String {
                        let guard = sessions_close.lock().unwrap();
                        let Some(session) = guard.get(&id).cloned() else {
                            return serde_json::json!({"ok": false}).to_string();
                        };
                        drop(guard);
                        let ok = try_send_cmd(
                            &session.cmd_tx,
                            WsCommand::Close {
                                code: code as u16,
                                reason,
                            },
                        );
                        serde_json::json!({"ok": ok}).to_string()
                    },
                ),
            );

            // ── ws finish(id) -> teardown + ws_* metrics ──
            let sessions_finish = sessions.clone();
            let sink_finish = sink.clone();
            let _ = globals.set(
                "__tropel_k6_ws_finish",
                Func::from(
                    move |id: u64| -> String {
                        let session = sessions_finish.lock().unwrap().remove(&id);
                        let Some(session) = session else {
                            return serde_json::json!({"ok": false}).to_string();
                        };
                        let duration = session.start.elapsed();
                        let now = tropel_core::clock::monotonic_wall_now();
                        let mut tags = TagMap::with_capacity(5);
                        tags.insert("url", session.url.clone());
                        tags.insert("method", String::from("GET"));
                        tags.insert("status", String::from("101"));
                        tags.insert("name", session.url.clone());
                        tags.insert("group", String::from("ws"));
                        let tags = Arc::new(tags);

                        let msgs_sent = session.msgs_sent.load(Ordering::Relaxed);
                        let bytes_sent = session.bytes_sent.load(Ordering::Relaxed);
                        let msgs_received = session.msgs_received.load(Ordering::Relaxed);
                        let bytes_received = session.bytes_received.load(Ordering::Relaxed);

                        let mut v = sink_finish.lock().unwrap();
                        v.push(Sample {
                            metric: "ws_connecting".into(),
                            value: session.connecting.as_secs_f64() * 1000.0,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Trend,
                        });
                        v.push(Sample {
                            metric: "ws_msgs_sent".into(),
                            value: msgs_sent as f64,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "ws_msgs_received".into(),
                            value: msgs_received as f64,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "ws_bytes_sent".into(),
                            value: bytes_sent as f64,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "ws_bytes_received".into(),
                            value: bytes_received as f64,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "ws_sessions".into(),
                            value: 1.0,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "ws_req_duration".into(),
                            value: duration.as_secs_f64() * 1000.0,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Trend,
                        });
                        v.push(Sample {
                            metric: "ws_req_failed".into(),
                            value: 0.0,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Rate,
                        });
                        v.push(Sample {
                            metric: "data_sent".into(),
                            value: bytes_sent as f64,
                            tags: tags.clone(),
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        v.push(Sample {
                            metric: "data_received".into(),
                            value: bytes_received as f64,
                            tags,
                            timestamp: now,
                            sample_type: SampleType::Counter,
                        });
                        serde_json::json!({"ok": true}).to_string()
                    },
                ),
            );
        });

        self.ws_bridges_registered = true;
        tracing::debug!("K6Driver: registered ws bridges");
    }

    /// Sync VuContext state into JS globals so the script can read
    /// environment variables, data rows, etc.
    ///
    /// Split into a one-time seed of the per-VU IMMUTABLE globals (__VU,
    /// __tropel_vu_id, __tropel_scenario, __ENV, __tropel_env — the env is
    /// constant for the whole run) and a per-iteration refresh of the mutable
    /// ones (__ITER, __tropel_iteration_num, __tropel_data_row, exec.*). This
    /// removes the previous per-iteration cost of serializing the env twice
    /// and compile+eval'ing a JSON.parse on every iteration.
    async fn sync_globals(&mut self, ctx: &VuContext) -> Result<()> {
        if !self.globals_seeded {
            self.globals_seeded = true;

            // Numeric, matching __VU and the __tropel_exec_vu_id bridge (a
            // string here would hit the same arithmetic bug as the old __VU:
            // `__tropel_vu_id % len` → NaN).
            let _ = self
                .js_ctx
                .set_global_json("__tropel_vu_id", &serde_json::json!(ctx.vu_id))
                .await;
            let _ = self
                .js_ctx
                .set_global_str("__tropel_scenario", &ctx.scenario_name)
                .await;
            // k6-compatible: __VU is 1-based (like k6); __ITER is 0-based and
            // refreshed below each iteration. Backlog line 142: these must be
            // NUMBERS, not strings — `__ITER === 0` (the once-per-VU setup
            // guard) never fired on "0" and `__VU + 1` produced "11" on
            // string concatenation. set_global_json parses the JSON number
            // natively (no eval, no string round trip).
            let _ = self
                .js_ctx
                .set_global_json("__VU", &serde_json::json!(ctx.vu_id + 1))
                .await;

            // Set env vars as JS globals. k6 scripts read `__ENV` (and
            // Tropel's own `__tropel_env`); both get the same object. Always
            // set __ENV so `__ENV` is never undefined inside the script.
            let env_value = serde_json::to_value(&ctx.env).unwrap_or_default();
            let _ = self.js_ctx.set_global_json("__ENV", &env_value).await;
            let _ = self
                .js_ctx
                .set_global_json("__tropel_env", &env_value)
                .await;

            // Seed the per-VU IMMUTABLE exec.* fields once too (scenario name,
            // executor name, vu id never change for a VU) — the per-iteration
            // refresh below only touches the mutable counters, saving two
            // string clones per iteration.
            let mut es = self.exec_state.lock().unwrap();
            es.scenario_name = ctx.scenario_name.clone();
            es.executor_name = ctx.executor_name.clone();
            es.vu_id = ctx.vu_id;

            // k6 lifecycle: `export default function (data)` receives the
            // script's `setup()` return value. The engine ran setup() once
            // per scenario and threaded the serialized result into
            // ctx.setup_data — parse it into a native JS value ONCE here
            // (it is immutable for the whole run) so every iteration call
            // below passes it without re-serializing. When the script
            // declares no setup, `__tropel_setup` stays undefined, matching
            // k6 (data === undefined).
            let setup_value = ctx
                .setup_data
                .as_deref()
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok());
            self.js_ctx.with_ctx(|rq_ctx| {
                let val = match &setup_value {
                    Some(v) => json_to_value(rq_ctx, v),
                    None => rquickjs::Value::new_undefined(rq_ctx.clone()),
                };
                let _ = rq_ctx.globals().set("__tropel_setup", val);
            });
        }

        // Per-iteration mutable globals. Backlog line 142: numbers, not
        // strings (see the __VU comment above).
        let _ = self
            .js_ctx
            .set_global_json("__tropel_iteration_num", &serde_json::json!(ctx.iteration))
            .await;
        let _ = self
            .js_ctx
            .set_global_json("__ITER", &serde_json::json!(ctx.iteration))
            .await;

        // Refresh the per-iteration exec.* counters read by the __tropel_exec_*
        // bridges (iteration / iterations_completed / vus_active change every
        // iteration; scenario_name / executor_name / vu_id were seeded once).
        {
            let mut es = self.exec_state.lock().unwrap();
            es.iteration = ctx.iteration;
            es.iterations_completed = ctx.iterations_completed;
            es.vus_active = ctx.vus_active;
        }

        // Set data row (changes per iteration)
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
// ES-module local-import support (module resolver + loader)
// ══════════════════════════════════════════════════════════════════

/// Resolves relative ES-module specifiers to files on disk.
///
/// k6 scripts import local helpers with relative specifiers
/// (`import { x } from "./helpers.js"`). rquickjs consults this resolver
/// whenever a declared module contains an `import`/`export … from`
/// statement. Bare specifiers (`k6`, `k6/http`, npm packages) are not
/// resolvable on disk — k6 virtual modules are stripped by
/// `preprocess_k6_source_module` and provided by the shim, so a bare
/// specifier reaching the resolver is an error.
#[derive(Clone)]
struct K6ModuleResolver {
    script_dir: Option<PathBuf>,
}

impl Resolver for K6ModuleResolver {
    fn resolve<'js>(
        &mut self,
        _ctx: &rquickjs::Ctx<'js>,
        base: &str,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<String> {
        // Only relative/absolute specifiers can point at files. Bare
        // specifiers (k6 virtual modules, npm packages) error loudly.
        if !(name.starts_with("./") || name.starts_with("../") || Path::new(name).is_absolute()) {
            return Err(rquickjs::Error::new_loading_message(
                name,
                "bare module specifiers are not supported (k6 virtual modules are provided by the shim)",
            ));
        }

        // Base directory: the importing module's directory. For the entry
        // module (named "k6-script") or non-path bases, fall back to the
        // script directory.
        let base_dir = if base == "k6-script" || base.is_empty() {
            self.script_dir.clone().unwrap_or_default()
        } else {
            Path::new(base)
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_default()
        };

        let candidate = if Path::new(name).is_absolute() {
            PathBuf::from(name)
        } else {
            base_dir.join(name)
        };

        // Extension probing: try as-is, then with common JS/TS extensions,
        // then index files. `with_extension` returns a fresh PathBuf (the
        // original candidate is never mutated), so each attempt is distinct.
        let mut attempts: Vec<PathBuf> = Vec::new();
        attempts.push(candidate.clone());
        if candidate.extension().is_none() {
            for ext in ["js", "mjs", "cjs", "ts", "mts", "tsx"] {
                attempts.push(candidate.with_extension(ext));
            }
            attempts.push(candidate.join("index.js"));
            attempts.push(candidate.join("index.ts"));
            attempts.push(candidate.join("index.mjs"));
        }
        for a in &attempts {
            if a.is_file() {
                return Ok(a.to_string_lossy().into_owned());
            }
        }
        Err(rquickjs::Error::new_loading_message(
            name,
            format!("cannot resolve module '{}' from '{}'", name, base),
        ))
    }
}

/// Loads a resolved module file into the runtime, transpiling TypeScript
/// on the fly when the file is `.ts`/`.mts`/`.tsx`.
struct K6ModuleLoader;

impl Loader for K6ModuleLoader {
    fn load<'js>(
        &mut self,
        ctx: &rquickjs::Ctx<'js>,
        name: &str,
        _attributes: Option<ImportAttributes<'js>>,
    ) -> rquickjs::Result<rquickjs::Module<'js>> {
        let raw = std::fs::read_to_string(name).map_err(|e| {
            rquickjs::Error::new_loading_message(name, format!("read error: {}", e))
        })?;
        // Strip k6-virtual imports from the loaded module too — helper files
        // commonly `import { check } from "k6"` / `import http from "k6/http"`,
        // and those specifiers have no on-disk module (the shim provides the
        // globals). Mirroring the entry module's preprocess keeps imported
        // files consistent; local imports inside them still resolve via the
        // resolver.
        let preprocessed = preprocess_k6_source_module(&raw);
        let source = if tropel_es::is_typescript_file(name) {
            tropel_es::typescript_to_javascript_keep_exports(&preprocessed, name).map_err(|e| {
                rquickjs::Error::new_loading_message(name, format!("TS transpile error: {}", e))
            })?
        } else {
            preprocessed
        };
        rquickjs::Module::declare(ctx.clone(), name, source)
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
/// setup()` — because they are valid (and load-bearing) in a module.
///
/// k6 virtual imports and re-exports are removed on the oxc AST (see
/// [`tropel_es::strip_k6_virtual_imports`]): `import … from "k6/…"`,
/// `import "k6/…"`, `export { x } from "k6/…"`, `export * from "k6/…"`, and
/// remote `https://…` (jslib) specifiers — the k6 shim provides those APIs as
/// globals and there is no `k6/*` module or fetched jslib file on disk. The
/// AST-based splice (vs. the old line-anchored regex) also strips multi-line
/// imports (`import {\n check\n} from 'k6';`) and imports with trailing
/// comments, which used to survive, reach the module resolver, hard-error,
/// and kill `init` before iteration 1 → zero-metric, exit-0 runs.
///
/// Local imports (`import { x } from "./helpers.js"`) and local re-exports
/// (`export { x } from "./helpers"`, `export * from "./helpers"`) are KEPT:
/// the ES-module loader registered on the context (`K6ModuleResolver` +
/// `K6ModuleLoader`) resolves them to files on disk, transpiling TypeScript
/// on the fly.
fn preprocess_k6_source_module(source: &str) -> String {
    tropel_es::strip_k6_virtual_imports(source)
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
    script_dir: Option<PathBuf>,
) -> Result<Option<String>> {
    let mut js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // k6 scripts often read data files at init time (`JSON.parse(open(...))`
    // or `new SharedArray(...)`) while building `export const options` —
    // install the file bridges AND the open/SharedArray shim so the throwaway
    // context can resolve them (the shim defines the JS globals on top of the
    // native bridges). Also register the module loader so `options` blocks
    // that import local helpers (`import { x } from "./helpers.js"`) resolve.
    register_k6_file_bridges(&mut js_ctx, script_dir.clone());
    js_ctx.set_module_loader(
        K6ModuleResolver {
            script_dir: script_dir.clone(),
        },
        K6ModuleLoader,
    );
    js_ctx
        .bootstrap_library(OPEN_DATA_SHIM)
        .await
        .map_err(|e| {
            TropelError::Other(format!("k6 open/SharedArray shim bootstrap failed: {}", e))
        })?;
    // The k6 shim libs (Rate/check/http/…) must be present: options blocks
    // commonly run k6 API at module top level (e.g. `new Rate('errors')`),
    // which threw QuickJS exceptions when the shim was missing.
    bootstrap_js_libs(&mut js_ctx).await.map_err(|e| {
        TropelError::Other(format!("k6 shim bootstrap failed for options eval: {}", e))
    })?;

    // Minimal globals a k6 script may reference while building its options.
    // `__ENV` carries the job's env vars so options computed from them
    // (e.g. `const baseURL = __ENV.BASE_URL`) resolve instead of silently
    // becoming undefined. Backlog line 142: __VU/__ITER are NUMBERS in k6
    // (script code doing `__ITER === 0` or `__VU + 1` must not see strings).
    let _ = js_ctx.set_global_json("__VU", &serde_json::json!(0)).await;
    let _ = js_ctx
        .set_global_json("__ITER", &serde_json::json!(0))
        .await;
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
    script_dir: Option<PathBuf>,
) -> Result<Option<HashMap<String, String>>> {
    let mut js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // `handleSummary` may reference `open()`/SharedArray captured at init, so
    // install the file bridges + shim on the throwaway context too. Also
    // register the module loader so a `handleSummary` module that imports
    // local helpers (`import { x } from "./helpers.js"`) resolves them.
    register_k6_file_bridges(&mut js_ctx, script_dir.clone());
    js_ctx.set_module_loader(
        K6ModuleResolver {
            script_dir: script_dir.clone(),
        },
        K6ModuleLoader,
    );
    js_ctx
        .bootstrap_library(OPEN_DATA_SHIM)
        .await
        .map_err(|e| {
            TropelError::Other(format!("k6 open/SharedArray shim bootstrap failed: {}", e))
        })?;
    // Same k6-shim requirement as the options eval: a script that touches
    // k6 API at module top level must not throw while handleSummary is read.
    bootstrap_js_libs(&mut js_ctx).await.map_err(|e| {
        TropelError::Other(format!(
            "k6 shim bootstrap failed for handleSummary eval: {}",
            e
        ))
    })?;

    // Minimal globals a k6 script may reference while building its summary.
    // Backlog line 142: numbers, not strings (see options eval above).
    let _ = js_ctx.set_global_json("__VU", &serde_json::json!(0)).await;
    let _ = js_ctx
        .set_global_json("__ITER", &serde_json::json!(0))
        .await;
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

/// Evaluate an ES module and CALL a named exported function, serializing its
/// return value as JSON.
///
/// Backs the k6 lifecycle functions `setup()` (no argument) and
/// `teardown(data)` (the setup return value as argument). Mirrors
/// [`eval_module_handle_summary`]'s throwaway-context setup: file bridges +
/// module loader + shims installed so a script that touches `open()`,
/// SharedArray, or k6 API at module top level doesn't throw while the
/// export is read. Returns `Ok(None)` when the export is absent/not a
/// function (k6: no setup/teardown declared); `Err` when the call itself
/// throws (setup/teardown body error) so the caller decides how to surface
/// it (setup → None + warn, teardown → warn only, k6 parity).
async fn eval_module_call_export(
    source: &str,
    export: &str,
    arg_json: Option<&str>,
    env: &HashMap<String, String>,
    script_dir: Option<PathBuf>,
    http_client: Arc<dyn DriverHttpClient + Send + Sync>,
    sink: Arc<Mutex<Vec<Sample>>>,
) -> Result<Option<String>> {
    let mut js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    register_k6_file_bridges(&mut js_ctx, script_dir.clone());
    js_ctx.set_module_loader(
        K6ModuleResolver {
            script_dir: script_dir.clone(),
        },
        K6ModuleLoader,
    );
    js_ctx
        .bootstrap_library(OPEN_DATA_SHIM)
        .await
        .map_err(|e| {
            TropelError::Other(format!("k6 open/SharedArray shim bootstrap failed: {}", e))
        })?;
    bootstrap_js_libs(&mut js_ctx).await.map_err(|e| {
        TropelError::Other(format!("k6 shim bootstrap failed for {export} eval: {}", e))
    })?;

    // k6 §4 (backlog line 119): setup()/teardown() may make HTTP calls —
    // register the native HTTP bridges against the scenario's shared client
    // so `http.*` resolves in the throwaway context (previously it threw and
    // the login-in-setup pattern produced data === undefined). Samples land
    // in `sink`, which the engine drains into the run's metrics. The
    // blocking HTTP calls re-arm the per-eval deadline (backlog line 104).
    let (deadline, max_exec) = js_ctx.interrupt_deadline_handle();
    let group_stack: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    js_ctx.with_ctx(|rq_ctx| {
        register_http_bridges(
            rq_ctx,
            http_client.clone(),
            sink.clone(),
            "", // no scenario tag in the lifecycle context
            group_stack.clone(),
            deadline,
            max_exec,
        );
    });

    // Minimal globals a k6 script may reference at module top level while
    // defining setup()/teardown() (same set as the options/handleSummary
    // evals). Backlog line 142: numbers, not strings.
    let _ = js_ctx.set_global_json("__VU", &serde_json::json!(0)).await;
    let _ = js_ctx
        .set_global_json("__ITER", &serde_json::json!(0))
        .await;
    let env_json = serde_json::to_value(env).unwrap_or_else(|_| serde_json::json!({}));
    let _ = js_ctx.set_global_json("__ENV", &env_json).await;
    let _ = js_ctx.set_global_json("__tropel_env", &env_json).await;

    js_ctx.reset_interrupt();
    match js_ctx.with_ctx(|ctx| call_module_export(ctx, source, export, arg_json)) {
        Ok(Some(s)) => Ok(Some(s)),
        Ok(None) => Ok(None),
        // Do NOT warn here — the callers own the error path: setup() logs a
        // warn and returns None (data stays undefined), teardown() logs a
        // warn and continues (k6 parity: a throwing teardown never affects
        // the run's exit status). A second warn here would double-report.
        Err(e) => Err(TropelError::Other(format!("k6 {export}() failed: {}", e))),
    }
}

/// Call `setup()`/`teardown(data)` inside the given context. `arg_json` is
/// parsed via the global `JSON.parse` (k6 data must be JSON-serializable);
/// the return value is stringified and returned. Absent/non-function export
/// → `Ok(None)`; a throwing call → `Err`.
fn call_module_export(
    ctx: &rquickjs::Ctx,
    source: &str,
    export: &str,
    arg_json: Option<&str>,
) -> std::result::Result<Option<String>, rquickjs::Error> {
    let module = rquickjs::Module::declare(ctx.clone(), "k6-script", source)?;
    let (module, promise) = module.eval()?;
    promise.finish::<()>()?;

    let func: rquickjs::Function = match module.get::<_, rquickjs::Function>(export) {
        Ok(f) => f,
        Err(_) => return Ok(None), // absent or not a function
    };

    let json_obj: rquickjs::Object = ctx.globals().get("JSON")?;
    let result: rquickjs::Value = match arg_json {
        Some(json) => {
            let parse: rquickjs::Function = json_obj.get("parse")?;
            let data: rquickjs::Value = parse.call((json,))?;
            func.call((data,))?
        }
        None => func.call(())?,
    };

    // k6 allows `export async function setup()/teardown(data)`. If the call
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
    Ok(Some(s))
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
        return Ok(Some(HashMap::from([(
            "stdout".to_string(),
            text.to_string(),
        )])));
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
fn install_iteration_global(
    js_ctx: &mut JsContext,
    source: &str,
    exec: Option<&str>,
) -> Result<()> {
    // Arm the per-eval timeout: this evals the module directly via with_ctx,
    // bypassing the eval-family methods that normally reset the deadline.
    js_ctx.reset_interrupt();
    js_ctx.with_ctx(|rq_ctx| {
        let module = rquickjs::Module::declare(rq_ctx.clone(), "k6-script", source)
            .map_err(|e| TropelError::Other(format!("k6 script module declare error: {}", e)))?;
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
                        TropelError::Other(format!("failed to install __tropel_iteration: {}", e))
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
/// Base shim libraries (no native dependencies) concatenated at COMPILE TIME
/// (concat!) into one bundle. Per VU the bundle is loaded from the process-
/// wide bytecode cache (compiled ONCE, then `JS_ReadObject`+run) instead of
/// being re-parsed + re-compiled — at 1000 VUs that saves ~130 MB of QuickJS
/// parsing. Each separate eval also resets the JS interrupt timer and pumps
/// the promise queue, so one eval per phase cuts the per-VU bootstrap
/// overhead ~4× beyond the bytecode win.
const K6_BASE_SHIM_BUNDLE: &str = concat!(
    "// ==== shim: chai-shim ====\n",
    include_str!("../../../../js/chai/chai-shim.js"),
    "\n",
    "// ==== shim: lodash-shim ====\n",
    include_str!("../../../../js/lodash/lodash-shim.js"),
    "\n",
    "// ==== shim: cryptojs-shim ====\n",
    include_str!("../../../../js/cryptojs-shim/cryptojs.js"),
    "\n",
    "// ==== shim: exec-shim ====\n",
    include_str!("../../../../js/exec/exec.js"),
);

/// Native-dependent shim libraries (pm-api, sleep, k6-shim, open/SharedArray)
/// concatenated at COMPILE TIME into one bundle (see K6_BASE_SHIM_BUNDLE).
const K6_NATIVE_SHIM_BUNDLE: &str = concat!(
    "// ==== shim: pm-api ====\n",
    include_str!("../../../../js/pm-api/pm.js"),
    "\n",
    "// ==== shim: sleep-shim ====\n",
    include_str!("../../../../js/k6-shim/sleep-shim.js"),
    "\n",
    "// ==== shim: k6-shim ====\n",
    include_str!("../../../../js/k6-shim/k6-shim.js"),
    "\n",
    "// ==== shim: open-data-shim ====\n",
    include_str!("../../../../js/k6-shim/open-data-shim.js"),
);

/// Process-wide cache of a compiled k6 shim bundle's QuickJS bytecode.
///
/// Compiled ONCE by the first context (qjsc-style `JS_Eval` COMPILE_ONLY +
/// `JS_WriteObject`), then every subsequent context loads the blob and runs
/// it instead of re-parsing + re-compiling the ~130 KB of shim source per VU.
/// QuickJS bytecode is tied to the build (version + feature flags), not to a
/// particular context, so one compilation is valid for every VU context in
/// this process.
///
/// One cache exists per bundle (base + native-dependent): the native bundle
/// can only run AFTER `tropel_native::install_all`, so they are separate
/// blobs and must never be cross-served.
struct ShimBytecodeCache {
    bytecode: OnceLock<Option<Vec<u8>>>,
    /// True once compilation failed — every context then falls back to the
    /// per-VU source eval path instead of retrying the compile each time.
    compile_failed: AtomicBool,
    /// True once the cached bytecode failed to RUN in a context. A run
    /// failure is deterministic (same bytecode + same bundle), so after the
    /// first failure all subsequent contexts short-circuit straight to the
    /// source eval fallback.
    run_failed: AtomicBool,
}

impl ShimBytecodeCache {
    const fn new() -> Self {
        Self {
            bytecode: OnceLock::new(),
            compile_failed: AtomicBool::new(false),
            run_failed: AtomicBool::new(false),
        }
    }

    /// Bootstrap `bundle` into `ctx`: run the cached bytecode when available
    /// and known-good, otherwise fall back to a source eval (and remember the
    /// failure so the doomed path is not retried per VU).
    async fn bootstrap(&self, ctx: &mut JsContext, bundle: &'static str) {
        let bytecode = self.bytecode.get_or_init(|| {
            if self.compile_failed.load(Ordering::Relaxed) {
                return None;
            }
            match ctx.compile_global_bytecode(bundle) {
                Ok(bc) => {
                    tracing::info!(
                        "Compiled k6 shim bundle to bytecode once ({} bytes) — reusing across VUs",
                        bc.len()
                    );
                    Some(bc)
                }
                Err(e) => {
                    self.compile_failed.store(true, Ordering::Relaxed);
                    tracing::warn!(
                        "k6 shim bytecode compilation failed ({}); falling back to per-VU source eval",
                        e
                    );
                    None
                }
            }
        });

        if let (Some(bc), false) = (bytecode, self.run_failed.load(Ordering::Relaxed)) {
            if let Err(e) = ctx.run_global_bytecode(bc).await {
                self.run_failed.store(true, Ordering::Relaxed);
                tracing::warn!(
                    "Failed to run k6 shim bytecode: {} (disabling bytecode path; falling back to source eval)",
                    e
                );
                if let Err(e2) = ctx.bootstrap_library(bundle).await {
                    tracing::warn!("Failed to bootstrap k6 shim bundle: {}", e2);
                }
            }
        } else if let Err(e) = ctx.bootstrap_library(bundle).await {
            tracing::warn!("Failed to bootstrap k6 shim bundle: {}", e);
        }
    }
}

/// Bytecode caches for the two k6 shim bundles (see [`ShimBytecodeCache`]).
static K6_BASE_BYTECODE: ShimBytecodeCache = ShimBytecodeCache::new();
static K6_NATIVE_BYTECODE: ShimBytecodeCache = ShimBytecodeCache::new();

async fn bootstrap_js_libs(ctx: &mut JsContext) -> Result<()> {
    // Phase 1: Base shim libraries (no native dependencies) — compiled to
    // bytecode ONCE per process, run in every context thereafter.
    K6_BASE_BYTECODE.bootstrap(ctx, K6_BASE_SHIM_BUNDLE).await;

    // Phase 2: Install native module functions (needed by pm-api and k6-shim)
    if let Err(e) = tropel_native::install_all(ctx).await {
        tracing::warn!("Failed to install native modules: {}", e);
    }

    // Phase 3: Bootstrapping libraries that depend on native functions — a
    // SEPARATE bytecode cache (it must run after install_all above).
    K6_NATIVE_BYTECODE
        .bootstrap(ctx, K6_NATIVE_SHIM_BUNDLE)
        .await;

    // Install __tropel_native_sleep (blocks the OS thread, safe under thread-per-core).
    // The sleep burns WALL time; the per-eval JS interrupt deadline must not
    // count it against the JS execution budget, or a stock k6 pacing idiom
    // like `sleep(Math.random()*10)` is interrupted on resume (backlog line
    // 104). Re-arm the deadline after the blocking sleep, like the WS loop.
    let (deadline, max_exec) = ctx.interrupt_deadline_handle();
    ctx.with_ctx(|rq_ctx| {
        let globals = rq_ctx.globals();
        let deadline_sleep = deadline.clone();
        let _ = globals.set(
            "__tropel_native_sleep",
            rquickjs::function::Func::from(move |ms: f64| {
                if ms > 0.0 {
                    std::thread::sleep(Duration::from_secs_f64(ms / 1000.0));
                }
                tropel_js::rearm_deadline(&deadline_sleep, max_exec);
            }),
        );
    });

    // The sleep(seconds) wrapper is included in K6_NATIVE_SHIM_BUNDLE above
    // (js/k6-shim/sleep-shim.js), which is evaluated BEFORE __tropel_native_sleep
    // is installed in the with_ctx block above this comment. That ordering is
    // safe because the shim only dereferences `typeof __tropel_native_sleep` at
    // call time (inside sleep()), never at eval time.
    Ok(())
}

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
    // Object form must tolerate non-string values (e.g. {"Content-Length":
    // 123}) — the old `HashMap<String, String>` parse fell through to the
    // array form and returned an EMPTY map whenever any value was
    // non-string, silently dropping every header (backlog P3). Scalars are
    // stringified; null/complex values are skipped.
    if json.trim_start().starts_with('{') {
        if let Ok(map) = serde_json::from_str::<HashMap<String, serde_json::Value>>(json) {
            let mut headers = HashMap::new();
            for (k, v) in map {
                match v {
                    serde_json::Value::String(s) => {
                        headers.insert(k, s);
                    }
                    serde_json::Value::Number(n) => {
                        headers.insert(k, n.to_string());
                    }
                    serde_json::Value::Bool(b) => {
                        headers.insert(k, b.to_string());
                    }
                    _ => {}
                }
            }
            return headers;
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
        assert!(
            !result.contains("from \"k6/http\""),
            "k6 import kept: {result}"
        );
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
    fn test_module_preprocess_strips_only_k6_reexports() {
        // Local re-exports are KEPT — the ES-module loader resolves them to
        // files on disk. Only k6-virtual re-exports (no such module on disk,
        // shim provides globals) are stripped.
        let code = r#"
            export { default } from "./other";
            export * from "./helpers";
            export { check } from "k6";
            export * from "k6/http";
            export const options = {};
            export default function() {}
        "#;
        let result = preprocess_k6_source_module(code);
        assert!(
            result.contains("./other"),
            "local re-export stripped: {result}"
        );
        assert!(
            result.contains("./helpers"),
            "local re-export stripped: {result}"
        );
        assert!(
            !result.contains("from \"k6\""),
            "k6 re-export kept: {result}"
        );
        assert!(
            !result.contains("from \"k6/http\""),
            "k6 re-export kept: {result}"
        );
        assert!(
            result.contains("export const options"),
            "options lost: {result}"
        );
        assert!(
            result.contains("export default function"),
            "default export lost: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_strips_multiline_import() {
        // The §0-7 regression: the old line-anchored regex left multi-line
        // imports (`import {\n check\n} from 'k6';`) in place → they reached
        // the module resolver, hard-errored, and killed init before iteration
        // 1 → zero metrics, exit 0. The AST splice must remove them.
        let code = r#"
            import {
                check,
                group,
            } from "k6";
            import http from "k6/http";
            export const options = { vus: 2 };
            export default function() {}
        "#;
        let result = preprocess_k6_source_module(code);
        assert!(
            !result.contains("from \"k6\""),
            "multiline k6 import kept: {result}"
        );
        assert!(
            !result.contains("check,"),
            "multiline k6 import body kept: {result}"
        );
        assert!(
            !result.contains("from \"k6/http\""),
            "k6/http import kept: {result}"
        );
        assert!(
            result.contains("export const options"),
            "options lost: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_strips_import_with_trailing_comment() {
        // `import http from 'k6/http'; // c` — the old line-anchored regex
        // required the line to END after the specifier, so the trailing
        // comment made it survive. The AST splice strips the statement
        // regardless of trailing comment.
        let code =
            "import http from 'k6/http'; // shim provides this\nexport default function() {}\n";
        let result = preprocess_k6_source_module(code);
        assert!(
            !result.contains("from 'k6/http'"),
            "k6 import with trailing comment kept: {result}"
        );
        assert!(
            result.contains("export default function"),
            "default export lost: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_strips_jslib_url_import() {
        // `https://jslib.k6.io/...` imports can't be fetched by the local
        // module resolver — strip them so init doesn't hard-fail.
        let code =
            "import { randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';\nexport default function() {}\n";
        let result = preprocess_k6_source_module(code);
        assert!(
            !result.contains("jslib.k6.io"),
            "jslib URL import kept: {result}"
        );
        assert!(
            result.contains("export default function"),
            "default export lost: {result}"
        );
    }

    #[test]
    fn test_module_preprocess_keeps_local_import() {
        // `import { x } from "./helpers.js"` must survive preprocessing —
        // the registered module resolver resolves it at eval time.
        let code = "import { triple } from './helpers.js';\nexport default function() {}\n";
        let result = preprocess_k6_source_module(code);
        assert!(
            result.contains("from './helpers.js'"),
            "local import stripped: {result}"
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
        let json =
            read_export_for_test(source, "options").expect("options export should be readable");
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
    fn test_http_params_headers_not_mutated_and_multipart_boundary() {
        // Backlog line 138: serializeK6Body wrote Content-Type onto the
        // CALLER's headers object (every real k6 script hoists params to
        // module scope) — iteration 2 posted a string body still labelled
        // application/json. And the `!headers['Content-Type']` multipart
        // guard was false exactly when the user declared multipart/form-data,
        // so the generated boundary never reached the header → every
        // multipart request unparseable.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            // Stub the native HTTP bridge so no real network is needed;
            // capture exactly what the shim would hand to the bridge.
            ctx.eval::<(), _>(
                r#"
                globalThis.__captured = [];
                globalThis.__tropel_k6_http_request = function (method, url, headersJson, body) {
                    globalThis.__captured.push({ headers: JSON.parse(headersJson), body: body });
                    return { code: 200, status: 200, body: '{}', headers: {}, responseTime: 5 };
                };
            "#,
            )
            .expect("stub should eval");
            ctx.eval::<(), _>(
                r#"
                var params = { headers: {} };
                http.post('https://example.com/a', { a: 1 }, params);   // iter 1: object body
                http.post('https://example.com/b', 'plain text', params); // iter 2: string body
                var mp = { headers: { 'Content-Type': 'multipart/form-data' } };
                http.post('https://example.com/c', { field: 'value' }, mp);
                var mpLower = { headers: { 'content-type': 'multipart/form-data' } };
                http.post('https://example.com/d', { f2: 'v2' }, mpLower);
            "#,
            )
            .expect("script should eval");

            // 1. Caller's module-scope params.headers must NOT be mutated.
            let params_after: String = ctx
                .eval("JSON.stringify(params.headers)")
                .expect("read params.headers");
            assert_eq!(
                params_after, "{}",
                "params.headers was mutated in place: {params_after}"
            );

            // 2. Iteration 2 (string body) must not inherit Content-Type from
            //    iteration 1's object-body mutation.
            let second_headers: String = ctx
                .eval("JSON.stringify(__captured[1].headers)")
                .expect("read captured[1] headers");
            assert!(
                !second_headers.to_lowercase().contains("content-type"),
                "string-body request inherited stale Content-Type: {second_headers}"
            );
            let second_body: String = ctx
                .eval("__captured[1].body")
                .expect("read captured[1] body");
            assert_eq!(second_body, "plain text");

            // 3. Iteration 1 (object body) still gets application/json ON THE
            //    COPY (not on the caller's object).
            let first_headers: String = ctx
                .eval("JSON.stringify(__captured[0].headers)")
                .expect("read captured[0] headers");
            assert!(
                first_headers.to_lowercase().contains("application/json"),
                "object body not labelled json: {first_headers}"
            );

            // 4. Multipart: the generated boundary MUST reach the header.
            let mp_headers: String = ctx
                .eval("JSON.stringify(__captured[2].headers)")
                .expect("read captured[2] headers");
            assert!(
                mp_headers.contains("boundary="),
                "multipart boundary missing from header: {mp_headers}"
            );
            let mp_body: String = ctx
                .eval("__captured[2].body")
                .expect("read captured[2] body");
            assert!(
                mp_body.contains("----TropelFormBoundary"),
                "multipart body missing framing: {mp_body}"
            );

            // 5. A lowercase `content-type` declaration must be replaced (not
            //    duplicated) — leaving both would send TWO Content-Type
            //    headers, one boundary-less.
            let mp_lower: String = ctx
                .eval("JSON.stringify(__captured[3].headers)")
                .expect("read captured[3] headers");
            assert!(
                mp_lower.contains("boundary="),
                "lowercase multipart boundary missing: {mp_lower}"
            );
            assert!(
                !mp_lower
                    .to_lowercase()
                    .contains("\"content-type\":\"multipart/form-data\""),
                "boundary-less content-type variant leaked into header: {mp_lower}"
            );
        });
    }

    #[test]
    fn test_response_headers_keep_canonical_case() {
        // Backlog line 139: the shim force-lowercased response header keys
        // (hk.toLowerCase()), so `res.headers['Content-Type']` — every k6
        // doc example — returned undefined. The native bridge now delivers
        // Go MIME canonical keys (Content-Type, X-Request-Id); the shim must
        // keep them EXACTLY as-is.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__tropel_k6_http_request = function (method, url, headersJson, body) {
                    // Canonical MIME form, exactly what client.rs now emits.
                    return { code: 200, status: 200, body: '{}',
                             headers: { 'Content-Type': 'application/json',
                                        'X-Request-Id': 'abc-123',
                                        'Location': 'https://example.com/2' },
                             responseTime: 5 };
                };
                globalThis.__res = http.get('https://example.com/1', {});
                globalThis.__ct = __res.headers['Content-Type'];
                globalThis.__xri = __res.headers['X-Request-Id'];
                globalThis.__loc = __res.headers['Location'];
            "#,
            )
            .expect("script should eval");
            let ct: String = ctx.eval("__ct").expect("read Content-Type");
            assert_eq!(ct, "application/json");
            let xri: String = ctx.eval("__xri").expect("read X-Request-Id");
            assert_eq!(xri, "abc-123");
            let loc: String = ctx.eval("__loc").expect("read Location");
            assert_eq!(loc, "https://example.com/2");
            // The keys must be stored verbatim (canonical), not lowercased.
            let keys: String = ctx
                .eval("JSON.stringify(Object.keys(__res.headers))")
                .expect("read header keys");
            assert!(
                keys.contains("Content-Type"),
                "canonical key Content-Type missing: {keys}"
            );
            assert!(
                keys.contains("X-Request-Id") && keys.contains("Location"),
                "canonical keys missing: {keys}"
            );
            let has_lowercase: bool = ctx
                .eval("Object.keys(__res.headers).indexOf('content-type') !== -1")
                .expect("check lowercase key absent");
            assert!(
                !has_lowercase,
                "lowercase key 'content-type' present instead of canonical: {keys}"
            );
        });
    }

    #[test]
    fn test_http_params_extras_reach_the_bridge() {
        // Backlog line 140: params.tags/auth/redirects/cookies/compression
        // were silently dropped and timeout was parsed then discarded. The
        // shim must pack ALL of them into the 6th bridge arg (the extras JSON
        // string — the closure is arity-capped), with auth translated into
        // the tagged AuthConfig form the Rust side deserializes.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__captured = [];
                globalThis.__tropel_k6_http_request = function (method, url, headersJson, body, responseType, extrasJson) {
                    globalThis.__captured.push({ extras: JSON.parse(extrasJson) });
                    return { code: 200, status: 200, body: '{}', headers: {}, responseTime: 5 };
                };
                http.get('https://example.com/x', {
                    tags: { name: 'my-request', foo: 'bar' },
                    auth: { token: 's3cret' },
                    redirects: 0,
                    compression: 'gzip',
                    timeout: '2s',
                });
            "#,
            )
            .expect("script should eval");

            let extras: String = ctx
                .eval("JSON.stringify(__captured[0].extras)")
                .expect("read captured extras");
            // timeout: '2s' must be parsed to ms and actually shipped.
            assert!(
                extras.contains("\"timeoutMs\":2000"),
                "timeout parsed then dropped: {extras}"
            );
            assert!(extras.contains("\"name\":\"my-request\""), "tags dropped: {extras}");
            assert!(extras.contains("\"foo\":\"bar\""), "tags dropped: {extras}");
            assert!(extras.contains("\"redirects\":0"), "redirects dropped: {extras}");
            assert!(extras.contains("\"compression\":\"gzip\""), "compression dropped: {extras}");
            // auth must be the tagged AuthConfig form ({type:'bearer',…}), not
            // k6's bare {token} — that's what the Rust AuthConfig enum parses.
            assert!(
                extras.contains("\"type\":\"bearer\"") && extras.contains("\"token\":\"s3cret\""),
                "auth not translated to tagged AuthConfig: {extras}"
            );
            // No auth param → null (the bridge must not invent one).
            ctx.eval::<(), _>(
                r#"
                http.get('https://example.com/y', {});
            "#,
            )
            .expect("script should eval");
            let second: String = ctx
                .eval("JSON.stringify(__captured[1].extras)")
                .expect("read second captured extras");
            assert!(second.contains("\"auth\":null"), "auth not null when absent: {second}");
        });
    }

    #[test]
    fn test_http_batch_entries_carry_per_request_params() {
        // Backlog line 140 (batch half): batch entries must carry the same
        // per-request tags/auth/redirects/compression/timeout as the single-
        // request path — and auth_json is DOUBLE-ENCODED by the shim
        // (JSON.stringify inside the entry), so the Rust bridge must parse
        // the inner string (from_value(Value::String) on an internally-tagged
        // enum fails silently → auth dropped).
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__captured = [];
                globalThis.__tropel_k6_http_batch = function (requestsJson) {
                    globalThis.__captured.push(JSON.parse(requestsJson));
                    return '{}';
                };
                http.batch([
                    ['GET', 'https://example.com/1', null, {
                        tags: { name: 'b1' },
                        auth: { username: 'u', password: 'p' },
                        redirects: 0,
                        timeout: '1s',
                    }],
                    { 'https://example.com/2': { method: 'POST', body: 'x' } },
                ]);
            "#,
            )
            .expect("script should eval");

            let entries: String = ctx
                .eval("JSON.stringify(__captured[0])")
                .expect("read batch entries");
            // Entry 1: every per-request param present.
            assert!(
                entries.contains("\"tags_json\":\"{\\\"name\\\":\\\"b1\\\"}\""),
                "batch tags missing: {entries}"
            );
            assert!(
                entries.contains("\"redirects\":0"),
                "batch redirects missing: {entries}"
            );
            assert!(
                entries.contains("\"timeout_ms\":1000"),
                "batch timeout missing: {entries}"
            );
            // auth_json is a STRING containing the tagged auth JSON — the
            // Rust side must parse the inner string.
            assert!(
                entries.contains("\\\"type\\\":\\\"basic\\\""),
                "batch auth not translated to tagged AuthConfig: {entries}"
            );
            // Entry 2 (no params): no auth, no tags, default redirects.
            assert!(
                entries.contains("\"auth_json\":\"null\""),
                "batch auth not null when absent: {entries}"
            );
        });
    }

    #[test]
    fn test_compress_k6_body_gzip_and_deflate() {
        // Backlog line 140: compression param was dropped; the helper must
        // produce valid gzip/deflate and pass through the uncompressed bytes
        // (no-op) for an unsupported algorithm.
        let data = b"{\"hello\":\"world\"}".repeat(3);
        let gz = compress_k6_body("gzip", &data).expect("gzip should compress");
        assert_ne!(gz, data, "gzip must change the bytes");
        let mut dec = flate2::read::GzDecoder::new(gz.as_slice());
        use std::io::Read;
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        assert_eq!(out, data, "gzip round-trip must restore the body");

        let df = compress_k6_body("deflate", &data).expect("deflate should compress");
        let mut dec2 = flate2::read::DeflateDecoder::new(df.as_slice());
        let mut out2 = Vec::new();
        dec2.read_to_end(&mut out2).unwrap();
        assert_eq!(out2, data, "deflate round-trip must restore the body");

        // Unsupported algorithm → None (k6 proceeds uncompressed).
        assert!(
            compress_k6_body("br", &data).is_none(),
            "unsupported must be None"
        );
        assert!(
            compress_k6_body("gzip", b"").is_none(),
            "empty body must be None"
        );
    }

    #[test]
    fn test_pm_send_request_headers_not_mutated_and_multipart_boundary() {
        // Backlog line 138 (pm.js half): pm.sendRequest aliased
        // `headers = options.headers`, so the formdata branch stamped the
        // generated Content-Type onto the CALLER's object, and the
        // `!headers['Content-Type']` guard dropped the boundary whenever the
        // caller declared multipart/form-data.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__pm_captured = [];
                globalThis.__tropel_pm_send_request = function (method, url, headersJson, body) {
                    globalThis.__pm_captured.push({ headers: JSON.parse(headersJson), body: body });
                    return JSON.stringify({ code: 200, body: '{}', headers: {}, responseTime: 5 });
                };
                var opts = { url: 'https://example.com/u', method: 'POST',
                             headers: { 'Content-Type': 'multipart/form-data' },
                             body: { mode: 'formdata', formdata: [{ key: 'field', value: 'value' }] } };
                pm.sendRequest(opts);
                var opts2 = { url: 'https://example.com/v', method: 'POST',
                              headers: { 'Content-Type': 'multipart/form-data' },
                              body: { mode: 'formdata', formdata: [{ key: 'x', value: 'y' }] } };
                pm.sendRequest(opts2);
            "#,
            )
            .expect("script should eval");
            // Caller's options.headers must not be mutated.
            let caller_headers: String = ctx
                .eval("JSON.stringify(opts.headers)")
                .expect("read opts.headers");
            assert_eq!(
                caller_headers, "{\"Content-Type\":\"multipart/form-data\"}",
                "caller's options.headers was mutated: {caller_headers}"
            );
            // Generated boundary must reach the request header.
            let first: String = ctx
                .eval("JSON.stringify(__pm_captured[0].headers)")
                .expect("read captured headers");
            assert!(
                first.contains("boundary="),
                "pm multipart boundary missing from header: {first}"
            );
            // A fresh boundary per call (not reused/stale across iterations).
            let second: String = ctx
                .eval("JSON.stringify(__pm_captured[1].headers)")
                .expect("read captured headers");
            assert!(
                second.contains("boundary=") && second != first,
                "pm boundary reused across calls: {second}"
            );
        });
    }

    #[test]
    fn test_pm_send_request_transport_failure_fires_error_callback() {
        // Backlog line 147: pm.sendRequest reported transport failures
        // (DNS/conn refused/timeout) as SUCCESS — callback(null, {code: 0}) —
        // so the universal `if (err)` guard in user scripts never fired and
        // auth-token-fetch retry logic was dead. The bridge now stamps an
        // `error` field; the shim must surface it as the first (err) arg.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                // Transport failure — what the real bridge returns on a
                // connection error (code 0 + error field).
                globalThis.__tropel_pm_send_request = function () {
                    return JSON.stringify({
                        error: 'Request failed: error sending request for url (http://down:9/)',
                        code: 0, statusText: '', body: '', headers: {}, responseTime: 0
                    });
                };
                globalThis.__cb_args = null;
                pm.sendRequest('http://down:9/', function (err, resp) {
                    globalThis.__cb_args = { err: err ? err.message : null, resp: resp };
                });

                // A healthy response must still arrive via (null, resp).
                globalThis.__tropel_pm_send_request = function () {
                    return JSON.stringify({ code: 200, statusText: 'OK', body: '{}', headers: {},
                                            responseTime: 5 });
                };
                globalThis.__ok_args = null;
                pm.sendRequest('http://ok/', function (err, resp) {
                    globalThis.__ok_args = { err: err ? err.message : null, resp: resp };
                });
            "#,
            )
            .expect("script should eval");

            let err_msg: String = ctx
                .eval("globalThis.__cb_args.err")
                .expect("read err message");
            assert!(
                err_msg.contains("Request failed"),
                "transport failure must fire callback(err, null): {err_msg}"
            );
            let resp_null: bool = ctx
                .eval("globalThis.__cb_args.resp === null")
                .expect("read resp nullity");
            assert!(resp_null, "err path must pass null response");

            let ok_err: bool = ctx
                .eval("globalThis.__ok_args.err === null")
                .expect("read ok err");
            assert!(ok_err, "healthy response must fire callback(null, resp)");
            let ok_code: i64 = ctx
                .eval("globalThis.__ok_args.resp.code")
                .expect("read ok code");
            assert_eq!(ok_code, 200, "success path code must round-trip");
        });
    }

    #[test]
    fn test_pm_response_members_are_value_properties() {
        // Backlog line 143: pm.response.code/status/responseTime/headers/
        // cookies are VALUE properties in Postman, not functions. The old
        // function-object form made `pm.expect(pm.response.code).to.eql(200)`
        // compare a Function to 200 (never eql) and `pm.response.headers.get('X')`
        // throw a TypeError (headers was a function). Only text()/json() are
        // methods in Postman.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            // Stub the response bridges with known values.
            ctx.eval::<(), _>(
                r#"
                globalThis.__tropel_pm_response_code = function () { return 200; };
                globalThis.__tropel_pm_response_status = function () { return 'OK'; };
                globalThis.__tropel_pm_response_time = function () { return 42.5; };
                globalThis.__tropel_pm_response_headers = function () {
                    return { 'Content-Type': 'application/json' };
                };
                globalThis.__tropel_pm_response_header = function (key) {
                    if (String(key).toLowerCase() === 'content-type') return 'application/json';
                    return null;
                };
                globalThis.__tropel_pm_response_cookies = function () {
                    return { session: 'abc123' };
                };
                globalThis.__type_code = typeof pm.response.code;
                globalThis.__type_headers = typeof pm.response.headers;
                globalThis.__type_cookies = typeof pm.response.cookies;
                globalThis.__code = pm.response.code;
                globalThis.__status = pm.response.status;
                globalThis.__rtime = pm.response.responseTime;
                globalThis.__hdr = pm.response.headers.get('content-type');
                globalThis.__hdr_all = JSON.stringify(pm.response.headers.toObject());
                globalThis.__ck = pm.response.cookies[0].value;
                globalThis.__ck_get = pm.response.cookies.get('session').value;
                // The two canonical idioms that the function-form broke.
                // Coerce to String so the result is always eval-able as Rust
                // String (a raw boolean/string mix fails String::from_js).
                globalThis.__eql_ok = String((function () {
                    try { pm.expect(pm.response.code).to.eql(200); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
                globalThis.__status_ok = String((function () {
                    try { pm.expect(pm.response).to.have.status(200); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
            "#,
            )
            .expect("script should eval");

            assert_eq!(
                ctx.eval::<String, _>("__type_code").unwrap(),
                "number",
                "pm.response.code must be a number value, not a function"
            );
            assert_eq!(
                ctx.eval::<String, _>("__type_headers").unwrap(),
                "object",
                "pm.response.headers must be a Headers object"
            );
            assert_eq!(
                ctx.eval::<String, _>("__type_cookies").unwrap(),
                "object",
                "pm.response.cookies must be a list"
            );
            assert_eq!(ctx.eval::<i64, _>("__code").unwrap(), 200);
            assert_eq!(ctx.eval::<String, _>("__status").unwrap(), "OK");
            assert_eq!(ctx.eval::<f64, _>("__rtime").unwrap(), 42.5);
            assert_eq!(
                ctx.eval::<String, _>("__hdr").unwrap(),
                "application/json",
                "pm.response.headers.get() must work (was a TypeError)"
            );
            assert!(
                ctx.eval::<String, _>("__hdr_all")
                    .unwrap()
                    .contains("Content-Type"),
                "headers.toObject() must expose the map"
            );
            assert_eq!(
                ctx.eval::<String, _>("__ck").unwrap(),
                "abc123",
                "cookies[0].value must read as a value property"
            );
            assert_eq!(
                ctx.eval::<String, _>("__ck_get").unwrap(),
                "abc123",
                "cookies.get('session') must find by name"
            );
            assert_eq!(
                ctx.eval::<String, _>("__eql_ok").unwrap(),
                "true",
                "pm.expect(pm.response.code).to.eql(200) must pass (compared a Function before)"
            );
            assert_eq!(
                ctx.eval::<String, _>("__status_ok").unwrap(),
                "true",
                "pm.expect(pm.response).to.have.status(200) must pass"
            );
        });
    }

    #[test]
    fn test_pm_expect_eql_is_deep_equal() {
        // Backlog line 144: pm.expect(...).to.eql() was strict === while
        // .equal() delegated to eql — inverted vs chai. Deep-equal means a
        // freshly-parsed response body can eql a literal; strict equal must
        // NOT deep-compare (=== semantics).
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__deep_ok = String((function () {
                    // Key order differs — === would fail, deep eql must pass.
                    try { pm.expect({ b: 2, a: { x: [1, 2] } }).to.eql({ a: { x: [1, 2] }, b: 2 }); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
                globalThis.__deep_neg = String((function () {
                    // .not.to.eql with differing values must pass.
                    try { pm.expect({ a: 1 }).not.to.eql({ a: 2 }); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
                globalThis.__strict_ok = String((function () {
                    // .equal stays strict: same-primitive passes.
                    try { pm.expect(200).to.equal(200); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
                globalThis.__strict_distinct = String((function () {
                    // .equal must NOT deep-compare objects (chai semantics).
                    try { pm.expect({ a: 1 }).to.equal({ a: 1 }); return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__eql_json_body = String((function () {
                    // The canonical Postman idiom: parsed body vs literal.
                    var body = JSON.parse('{"userId":1,"name":"ada"}');
                    try { pm.expect(body).to.eql({ name: 'ada', userId: 1 }); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                })());
                globalThis.__json_body_ok = String((function () {
                    // to.have.jsonBody must deep-compare too (was key-order
                    // sensitive JSON.stringify comparison).
                    var saved = globalThis.__tropel_pm_response_json;
                    globalThis.__tropel_pm_response_json = function () {
                        return '{"name":"ada","userId":1}';
                    };
                    try { pm.expect({}).to.have.jsonBody({ userId: 1, name: 'ada' }); return true; }
                    catch (e) { return 'threw: ' + e.message; }
                    finally { globalThis.__tropel_pm_response_json = saved; }
                })());
            "#,
            )
            .expect("script should eval");

            assert_eq!(ctx.eval::<String, _>("__deep_ok").unwrap(), "true", "eql must deep-compare (key order irrelevant)");
            assert_eq!(ctx.eval::<String, _>("__deep_neg").unwrap(), "true", "not.to.eql must deep-compare");
            assert_eq!(ctx.eval::<String, _>("__strict_ok").unwrap(), "true", "equal must still accept identical primitives");
            assert_eq!(ctx.eval::<String, _>("__strict_distinct").unwrap(), "threw", "equal must stay strict (no deep-compare)");
            assert_eq!(ctx.eval::<String, _>("__eql_json_body").unwrap(), "true", "parsed JSON body must eql a literal object");
            assert_eq!(ctx.eval::<String, _>("__json_body_ok").unwrap(), "true", "to.have.jsonBody must deep-compare (key order insensitive)");
        });
    }

    #[test]
    fn test_unimplemented_assertion_properties_fail_closed() {
        // Backlog §1 P0: unimplemented assertion PROPERTIES (pm.expect(false)
        // .to.be.true, .to.be.null/.undefined/.ok/.empty, pm.expect(null)
        // .to.exist, and chai .empty/.exist/.NaN/.finite) used to read as
        // `undefined` and pm.test recorded GREEN — a silent pass. The Proxy
        // guard now THROWS on unknown assertion names, and the common
        // property getters are implemented for real.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/chai/chai-shim.js"))
                .expect("chai shim should eval");
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                // Implemented getters must WORK (pass silently is fine — no throw).
                globalThis.__pm_true_ok = String((function () {
                    try { pm.expect(true).to.be.true; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_exist_ok = String((function () {
                    try { pm.expect(1).to.exist; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_empty_ok = String((function () {
                    try { pm.expect([]).to.be.empty; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_ok_fail = String((function () {
                    // pm.expect(false).to.be.true must THROW (was silent pass).
                    try { pm.expect(false).to.be.true; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_null_exist_fail = String((function () {
                    // pm.expect(null).to.exist must THROW.
                    try { pm.expect(null).to.exist; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_unknown_prop = String((function () {
                    // A typo'd/unimplemented name must THROW, not pass silently.
                    try { pm.expect(1).to.be.bogusAssertion; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_not_true_fail = String((function () {
                    // Negated: true IS true, so .not.to.be.true must THROW.
                    try { pm.expect(true).not.to.be.true; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_not_true_ok = String((function () {
                    // Negated: false is NOT true, so .not.to.be.true passes.
                    try { pm.expect(false).not.to.be.true; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__pm_not_exist_fail = String((function () {
                    // Negated exist: 1 EXISTS, so .not.to.exist must THROW.
                    // (chai: expect(null).not.to.exist PASSES — null doesn't
                    // exist — so use a value that exists for the throw case.)
                    try { pm.expect(1).not.to.exist; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                // chai side: implemented getters work, unknown throws.
                globalThis.__chai_empty_ok = String((function () {
                    try { chai.expect([]).to.be.empty; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_nan_ok = String((function () {
                    try { chai.expect(NaN).to.be.NaN; return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_exist_fail = String((function () {
                    try { chai.expect(null).to.exist; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_finite_fail = String((function () {
                    try { chai.expect(Infinity).to.be.finite; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_unknown_prop = String((function () {
                    try { chai.expect(1).to.be.bogusAssertion; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_inspect_ok = String((function () {
                    // Inspection/promise-interop names must resolve normally
                    // (the guard's allowlist) — JSON.stringify must not throw.
                    try { JSON.stringify(chai.expect(1)); return 'ok'; }
                    catch (e) { return 'threw'; }
                })());
                globalThis.__chai_true_fail = String((function () {
                    try { chai.expect(false).to.be.true; return 'passed'; }
                    catch (e) { return 'threw'; }
                })());
            "#,
            )
            .expect("script should eval");

            assert_eq!(
                ctx.eval::<String, _>("__pm_true_ok").unwrap(),
                "ok",
                "pm true passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_exist_ok").unwrap(),
                "ok",
                "pm exist passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_empty_ok").unwrap(),
                "ok",
                "pm empty passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_ok_fail").unwrap(),
                "threw",
                "pm false.to.be.true must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_null_exist_fail").unwrap(),
                "threw",
                "pm null.to.exist must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_unknown_prop").unwrap(),
                "threw",
                "pm unknown assertion prop must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_not_true_fail").unwrap(),
                "threw",
                "pm true.not.to.be.true must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_not_true_ok").unwrap(),
                "ok",
                "pm false.not.to.be.true passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__pm_not_exist_fail").unwrap(),
                "threw",
                "pm 1.not.to.exist must throw (1 exists)"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_empty_ok").unwrap(),
                "ok",
                "chai empty passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_nan_ok").unwrap(),
                "ok",
                "chai NaN passes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_exist_fail").unwrap(),
                "threw",
                "chai null.to.exist must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_finite_fail").unwrap(),
                "threw",
                "chai Infinity.to.be.finite must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_unknown_prop").unwrap(),
                "threw",
                "chai unknown assertion prop must throw"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_inspect_ok").unwrap(),
                "ok",
                "chai JSON.stringify must not throw (allowlist)"
            );
            assert_eq!(
                ctx.eval::<String, _>("__chai_true_fail").unwrap(),
                "threw",
                "chai false.to.be.true must throw"
            );
        });
    }

    #[test]
    fn test_pm_collection_vars_globals_request_cookies() {
        // Backlog line 145: pm.collectionVariables / pm.globals / pm.request /
        // pm.cookies / pm.expect.fail / pm.test.skip / postman.setNextRequest
        // were all missing — collections using them threw TypeError and failed
        // the run. pm.request mutations must feed back through the bridges.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                // Variable-store stubs.
                globalThis.__tropel_pm_collection_vars_get = function (k) {
                    if (k === 'base') return '"https://api.example.com"';
                    return null;
                };
                globalThis.__tropel_pm_collection_vars_set = function (k, v) { globalThis.__cv_set = k + '=' + v; };
                globalThis.__tropel_pm_collection_vars_unset = function (k) { globalThis.__cv_unset = k; };
                globalThis.__tropel_pm_collection_vars_has = function (k) { return k === 'base'; };
                globalThis.__tropel_pm_collection_vars_to_object = function () { return { base: '"x"', n: '3' }; };
                globalThis.__tropel_pm_globals_get = function (k) { return k === 'g' ? '"global"' : null; };
                globalThis.__tropel_pm_globals_set = function (k, v) { globalThis.__g_set = k + '=' + v; };
                globalThis.__tropel_pm_globals_unset = function (k) { globalThis.__g_unset = k; };
                globalThis.__tropel_pm_globals_has = function (k) { return k === 'g'; };
                globalThis.__tropel_pm_globals_to_object = function () { return { g: '"global"' }; };
                globalThis.__tropel_pm_environment_has = function (k) { return k === 'env'; };
                globalThis.__tropel_pm_environment_to_object = function () { return { env: 'e' }; };

                // pm.request stubs — capture what the shim sends back.
                globalThis.__tropel_pm_request_url = function () { return 'http://x/old'; };
                globalThis.__tropel_pm_request_url_set = function (u) { globalThis.__r_url = u; };
                globalThis.__tropel_pm_request_method = function () { return 'GET'; };
                globalThis.__tropel_pm_request_method_set = function (m) { globalThis.__r_method = m; };
                globalThis.__tropel_pm_request_headers = function () { return { Authorization: 'Bearer old' }; };
                globalThis.__tropel_pm_request_header_get = function (k) { return k.toLowerCase() === 'authorization' ? 'Bearer old' : null; };
                globalThis.__tropel_pm_request_header_set = function (k, v) { globalThis.__r_hdr = k + '=' + v; };
                globalThis.__tropel_pm_request_header_unset = function (k) { globalThis.__r_hdr_unset = k; };
                globalThis.__tropel_pm_request_body = function () { return 'old-body'; };
                globalThis.__tropel_pm_request_body_set = function (b) { globalThis.__r_body = b; };
                globalThis.__tropel_pm_request_auth_set = function (a) { globalThis.__r_auth = a; };

                // Response cookies + test-skip + setNextRequest stubs.
                globalThis.__tropel_pm_response_cookies = function () { return { sid: 'abc' }; };
                globalThis.__tropel_pm_test_skip = function (n) { globalThis.__skipped = n; };
                globalThis.__tropel_pm_set_next_request = function (n) { globalThis.__next_req = n; };

                // collectionVariables / globals surface.
                globalThis.__cv_get = pm.collectionVariables.get('base');
                globalThis.__cv_has = pm.collectionVariables.has('base');
                globalThis.__cv_obj = JSON.stringify(pm.collectionVariables.toObject());
                pm.collectionVariables.set('k', 'v');
                pm.collectionVariables.unset('k');
                globalThis.__g_get = pm.globals.get('g');
                globalThis.__g_has = pm.globals.has('g');
                globalThis.__env_has = pm.environment.has('env');
                globalThis.__env_obj = JSON.stringify(pm.environment.toObject());

                // pm.request mutation idioms. Capture AFTER each write so
                // later calls (upsert/remove) can't clobber the captured value.
                pm.request.url = 'http://x/new';
                globalThis.__r_url = globalThis.__r_url;
                pm.request.method = 'POST';
                globalThis.__r_method = globalThis.__r_method;
                pm.request.headers.add({ key: 'Authorization', value: 'Bearer new' });
                globalThis.__r_hdr_add = globalThis.__r_hdr;
                pm.request.headers.upsert({ key: 'X-Extra', value: '1' });
                globalThis.__r_hdr_upsert = globalThis.__r_hdr;
                pm.request.headers.remove('X-Extra');
                globalThis.__r_hdr_unset = globalThis.__r_hdr_unset;
                pm.request.body = 'new-body';
                pm.request.body.raw = 'raw-body';
                globalThis.__r_body = globalThis.__r_body;
                pm.request.auth = { type: 'bearer', token: 't' };
                globalThis.__r_auth = globalThis.__r_auth;
                globalThis.__r_get_hdr = pm.request.headers.get('authorization');
                // Canonical Postman body idiom: pm.request.body.raw = ...
                globalThis.__r_body_raw = pm.request.body.raw;

                // pm.cookies surface.
                globalThis.__ck_get = pm.cookies.get('sid');
                globalThis.__ck_has = pm.cookies.has('sid');
                globalThis.__ck_obj = JSON.stringify(pm.cookies.toObject());

                // pm.expect.fail always throws.
                globalThis.__expect_fail_threw = String((function () {
                    try { pm.expect.fail('boom'); return 'no'; } catch (e) { return 'threw'; }
                })());

                // pm.test.skip records without running.
                pm.test.skip('slow-test');

                // postman.setNextRequest legacy global delegates.
                postman.setNextRequest('login');
            "#,
            )
            .expect("script should eval");

            assert_eq!(ctx.eval::<String, _>("__cv_get").unwrap(), "https://api.example.com", "pm.collectionVariables.get");
            assert!(ctx.eval::<bool, _>("__cv_has").unwrap(), "pm.collectionVariables.has");
            assert_eq!(ctx.eval::<String, _>("__cv_obj").unwrap(), r#"{"base":"x","n":3}"#, "toObject must JSON-decode values");
            assert_eq!(ctx.eval::<String, _>("__cv_set").unwrap(), "k=v", "collectionVariables.set must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__cv_unset").unwrap(), "k", "collectionVariables.unset must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__g_get").unwrap(), "global", "pm.globals.get");
            assert!(ctx.eval::<bool, _>("__g_has").unwrap(), "pm.globals.has");
            assert!(ctx.eval::<bool, _>("__env_has").unwrap(), "pm.environment.has");
            assert_eq!(ctx.eval::<String, _>("__env_obj").unwrap(), r#"{"env":"e"}"#, "environment.toObject");

            assert_eq!(ctx.eval::<String, _>("__r_url").unwrap(), "http://x/new", "pm.request.url setter must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_method").unwrap(), "POST", "pm.request.method setter must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_hdr_add").unwrap(), "Authorization=Bearer new", "pm.request.headers.add must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_hdr_upsert").unwrap(), "X-Extra=1", "pm.request.headers.upsert must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_hdr_unset").unwrap(), "X-Extra", "pm.request.headers.remove must reach the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_body").unwrap(), "raw-body", "pm.request.body setter must reach the bridge (last write: body.raw)");
            assert_eq!(ctx.eval::<String, _>("__r_body_raw").unwrap(), "old-body", "pm.request.body.raw getter must read through the bridge");
            assert_eq!(ctx.eval::<String, _>("__r_auth").unwrap(), r#"{"type":"bearer","token":"t"}"#, "pm.request.auth must JSON-encode the config");
            assert_eq!(ctx.eval::<String, _>("__r_get_hdr").unwrap(), "Bearer old", "pm.request.headers.get must be case-insensitive");

            assert_eq!(ctx.eval::<String, _>("__ck_get").unwrap(), "abc", "pm.cookies.get");
            assert!(ctx.eval::<bool, _>("__ck_has").unwrap(), "pm.cookies.has");
            assert_eq!(ctx.eval::<String, _>("__ck_obj").unwrap(), r#"{"sid":"abc"}"#, "pm.cookies.toObject");
            assert_eq!(ctx.eval::<String, _>("__expect_fail_threw").unwrap(), "threw", "pm.expect.fail must always throw");
            assert_eq!(ctx.eval::<String, _>("__skipped").unwrap(), "slow-test", "pm.test.skip must record via the bridge");
            assert_eq!(ctx.eval::<String, _>("__next_req").unwrap(), "login", "postman.setNextRequest must delegate");
        });
    }

    #[test]
    fn test_pm_variables_set_coerces_and_skip_request() {
        // Backlog line 146: pm.variables.set('id', 42) threw TypeError (the
        // shim passed the RAW value into a strict String bridge param) while
        // environment.set/collectionVariables.set/globals.set all coerced;
        // pm.execution.skipRequest() threw (routed null into a String) AND
        // semantically stopped the whole run instead of skipping the current
        // item; and variable values were silently retyped by JSON.parse
        // ("1.10" → 1.1). All three are fixed here.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                // Bridge stubs — variables_get returns JSON-encoded strings
                // (the real bridge JSON-encodes so the shim can restore the
                // type), set/skipRequest capture what the shim sends.
                globalThis.__tropel_pm_variables_get = function (k) {
                    if (k === 'id') return '"42"';
                    if (k === 'one10') return '"1.10"';
                    return null;
                };
                globalThis.__tropel_pm_variables_set = function (k, v) { globalThis.__v_set = k + '=' + v; };
                globalThis.__tropel_pm_variables_unset = function (k) { globalThis.__v_unset = k; };
                globalThis.__tropel_pm_skip_request = function () { globalThis.__skipped = true; };
                globalThis.__tropel_pm_set_next_request = function (n) { globalThis.__next_req = n; };

                // 1) Number value must not throw, must be coerced like the
                // other stores, and must reach the bridge as a string.
                pm.variables.set('id', 42);
                globalThis.__v_set_num = globalThis.__v_set;

                // 2) get must restore types WITHOUT retyping string values:
                // "42" stays the string "42" (not number 42) and "1.10"
                // stays the string "1.10" (not number 1.1).
                globalThis.__v_get_num = pm.variables.get('id');
                globalThis.__v_get_num_type = typeof globalThis.__v_get_num;
                globalThis.__v_get_one10 = pm.variables.get('one10');
                globalThis.__v_get_one10_type = typeof globalThis.__v_get_one10;

                // 3) skipRequest must call the dedicated bridge (no throw,
                // no setNextRequest(null) routing).
                pm.execution.skipRequest();
                globalThis.__skipped_after = globalThis.__skipped;
                globalThis.__next_req_after_skip = globalThis.__next_req === undefined ? 'unset' : String(globalThis.__next_req);

                // 4) setNextRequest(null) must not throw (null → Option<String>).
                pm.execution.setNextRequest(null);
                globalThis.__next_req_null = globalThis.__next_req === undefined || globalThis.__next_req === null ? 'null-or-unset' : String(globalThis.__next_req);
            "#,
            )
            .expect("script should eval");

            assert_eq!(
                ctx.eval::<String, _>("__v_set_num").unwrap(),
                "id=42",
                "pm.variables.set must coerce a number to a string like environment.set"
            );
            assert_eq!(
                ctx.eval::<String, _>("__v_get_num").unwrap(),
                "42",
                "pm.variables.get must return the string \"42\", never the number 42"
            );
            assert_eq!(
                ctx.eval::<String, _>("__v_get_num_type").unwrap(),
                "string",
                "numeric-looking string must not be retyped to a number"
            );
            assert_eq!(
                ctx.eval::<String, _>("__v_get_one10").unwrap(),
                "1.10",
                "\"1.10\" must round-trip as the string \"1.10\", not the number 1.1"
            );
            assert_eq!(
                ctx.eval::<String, _>("__v_get_one10_type").unwrap(),
                "string",
                "trailing-zero string must not be silently retyped by JSON.parse"
            );
            assert!(
                ctx.eval::<bool, _>("__skipped_after").unwrap(),
                "pm.execution.skipRequest must reach the dedicated __tropel_pm_skip_request bridge"
            );
            assert_eq!(
                ctx.eval::<String, _>("__next_req_after_skip").unwrap(),
                "unset",
                "skipRequest must NOT route through setNextRequest"
            );
            assert_eq!(
                ctx.eval::<String, _>("__next_req_null").unwrap(),
                "null-or-unset",
                "setNextRequest(null) must not throw (Option<String> bridge param)"
            );
        });
    }

    #[test]
    fn test_ws_local_close_dispatches_close_handler() {
        // Backlog line 148: the event-driven k6/ws API (socket.on/send/ping/
        // close/setTimeout + native __tropel_k6_ws_* bridges) is implemented,
        // but a LOCAL socket.close() only set _closed — the synchronous pump
        // then exited at the next iteration and `socket.on('close', ...)`
        // never fired. The k6 idiom of closing inside on('open')/'message'
        // leaked the final cleanup callback.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            // Stub the native ws bridges — no real socket needed.
            ctx.eval::<(), _>(
                r#"
                globalThis.__ws_step_calls = 0;
                globalThis.__tropel_k6_ws_connect = function (url, headersJson) {
                    return JSON.stringify({ id: 42, error: null });
                };
                globalThis.__tropel_k6_ws_step = function (id, timeoutMs) {
                    globalThis.__ws_step_calls++;
                    // First step delivers 'open'; a local close() ends the
                    // pump right after, so later steps are never reached.
                    if (globalThis.__ws_step_calls === 1) {
                        return JSON.stringify({ type: 'open' });
                    }
                    return JSON.stringify({ type: 'none' });
                };
                globalThis.__tropel_k6_ws_send = function (id, data) { globalThis.__ws_sent = data; return '{"ok":true}'; };
                globalThis.__tropel_k6_ws_ping = function (id) { globalThis.__ws_pinged = true; return '{"ok":true}'; };
                globalThis.__tropel_k6_ws_close = function (id, code, reason) {
                    globalThis.__ws_close_bridge = code + ':' + reason;
                    return '{"ok":true}';
                };
                globalThis.__tropel_k6_ws_finish = function (id) { globalThis.__ws_finished = id; return '{"ok":true}'; };

                globalThis.__ws_close_events = [];
                var ret = ws.connect('ws://localhost:1/', {}, function (socket) {
                    socket.on('open', function () {
                        socket.send('hi');
                        socket.ping();
                        socket.close(1000, 'bye');
                    });
                    socket.on('close', function (code, reason) {
                        globalThis.__ws_close_events.push(code + ':' + reason);
                    });
                });
                // ws.connect returns the socket (k6 semantics).
                globalThis.__ret_is_socket = ret && typeof ret.on === 'function';
                // Capture BEFORE the second scenario runs (its defensive
                // socket.close() would otherwise clobber __ws_close_bridge
                // via the unconditional native bridge call).
                globalThis.__ws_close_bridge_a = globalThis.__ws_close_bridge;

                // Second scenario: SERVER closes first, and the close handler
                // defensively calls socket.close() again. The pump marks the
                // socket closed BEFORE dispatching, so 'close' must fire once.
                globalThis.__ws_step_calls = 0;
                globalThis.__tropel_k6_ws_step = function (id, timeoutMs) {
                    globalThis.__ws_step_calls++;
                    if (globalThis.__ws_step_calls === 1) {
                        return JSON.stringify({ type: 'close', code: 1006, reason: 'server gone' });
                    }
                    return JSON.stringify({ type: 'none' });
                };
                globalThis.__ws_close2 = [];
                ws.connect('ws://localhost:2/', {}, function (socket) {
                    socket.on('close', function (code, reason) {
                        globalThis.__ws_close2.push(code + ':' + reason);
                        socket.close(1000, 'defensive'); // must not double-dispatch
                    });
                });
            "#,
            )
            .expect("script should eval");

            assert_eq!(
                ctx.eval::<String, _>("__ws_close_bridge_a").unwrap(),
                "1000:bye",
                "native close bridge must receive code+reason"
            );
            let close_events: String = ctx
                .eval("JSON.stringify(__ws_close_events)")
                .expect("read close events");
            assert_eq!(
                close_events, "[\"1000:bye\"]",
                "local close() must dispatch the close handler exactly once: {close_events}"
            );
            assert_eq!(
                ctx.eval::<String, _>("__ws_sent").unwrap(),
                "hi",
                "socket.send must reach the native bridge"
            );
            assert!(
                ctx.eval::<bool, _>("__ws_pinged").unwrap(),
                "socket.ping must reach the native bridge"
            );
            assert_eq!(
                ctx.eval::<i64, _>("__ws_finished").unwrap(),
                42,
                "ws.connect must tear the session down in its finally block"
            );
            assert!(
                ctx.eval::<bool, _>("__ret_is_socket").unwrap(),
                "ws.connect must return the K6Socket"
            );
            assert!(
                ctx.eval::<i64, _>("__ws_step_calls").unwrap() >= 1,
                "event pump must have run"
            );
            let close2_events: String = ctx
                .eval("JSON.stringify(__ws_close2)")
                .expect("read server-close events");
            assert_eq!(
                close2_events, "[\"1006:server gone\"]",
                "server close must dispatch once even with a defensive user close(): {close2_events}"
            );
        });
    }

    #[test]
    fn test_check_throws_on_nonsense_and_forwards_tags() {
        // Backlog line 149: check() accepted nonsense as success —
        // check(1, null) and check(1, 'x') returned true (k6 throws); it
        // dropped k6's 3rd tags arg; prefixed names with "check "; and
        // swallowed a throwing predicate (k6 records a failed check then
        // propagates the error to fail the iteration).
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__recorded = [];
                globalThis.__tropel_pm_test = function (name, passed, tagsJson) {
                    globalThis.__recorded.push({
                        name: name,
                        passed: passed,
                        tags: tagsJson ? JSON.parse(tagsJson) : null
                    });
                };

                // 1) Nonsense conds must THROW, not return true.
                globalThis.__null_threw = false;
                try { check(1, null); } catch (e) { globalThis.__null_threw = e instanceof TypeError; }
                globalThis.__str_threw = false;
                try { check(1, 'x'); } catch (e) { globalThis.__str_threw = e instanceof TypeError; }

                // 2) Raw names (no "check " prefix) + 3rd tags arg forwarded.
                check(1, { 'status is 200': function (v) { return v === 1; } }, { tag1: 'a', tag2: 'b' });
                // 3) Non-function conditions are boolean constants (k6 parity).
                check(1, { constTrue: true, constFalse: false });

                // 4) Throwing predicate: records a failed check, then propagates.
                globalThis.__threw_after = null;
                try {
                    check(1, { boom: function () { throw new Error('boom-check'); } });
                } catch (e) {
                    globalThis.__threw_after = e.message;
                }
            "#,
            )
            .expect("script should eval");

            assert!(
                ctx.eval::<bool, _>("__null_threw").unwrap(),
                "check(1, null) must throw a TypeError (k6 parity), not return true"
            );
            assert!(
                ctx.eval::<bool, _>("__str_threw").unwrap(),
                "check(1, 'x') must throw a TypeError (k6 parity), not return true"
            );
            assert_eq!(
                ctx.eval::<String, _>("__recorded[0].name").unwrap(),
                "status is 200",
                "check name must NOT be prefixed with 'check '"
            );
            let tags: String = ctx
                .eval("JSON.stringify(__recorded[0].tags)")
                .expect("read recorded tags");
            assert_eq!(
                tags, "{\"tag1\":\"a\",\"tag2\":\"b\"}",
                "k6 3rd tags arg must reach the bridge: {tags}"
            );
            assert!(
                ctx.eval::<bool, _>("__recorded[1].passed").unwrap(),
                "non-function truthy condition must pass (ToBoolean k6 parity)"
            );
            assert!(
                !ctx.eval::<bool, _>("__recorded[2].passed").unwrap(),
                "non-function falsy condition must fail (ToBoolean k6 parity)"
            );
            // Throwing predicate: failed check recorded, then error propagates.
            assert!(
                !ctx.eval::<bool, _>("__recorded[3].passed").unwrap(),
                "throwing predicate must record a failed check"
            );
            assert_eq!(
                ctx.eval::<String, _>("__threw_after").unwrap(),
                "boom-check",
                "throwing predicate must propagate (k6 fails the iteration)"
            );
        });
    }

    #[test]
    fn test_timings_error_binary_and_http_file() {
        // Backlog line 150: the k6 path emitted no sub-timing samples, res.
        // timings.* were 0 except waiting/duration, res.error/error_code did
        // not exist, binary bodies were destroyed (ArrayBuffer → ''), and
        // http.file() was missing. This exercises the shim + bridge contract:
        // real timings flow to res.timings, errors set res.error/error_code,
        // binary responses become ArrayBuffer, and http.file() uploads base64
        // bytes flagged bodyB64.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/k6-shim/k6-shim.js"))
                .expect("k6 shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__captured = null;
                globalThis.__tropel_k6_http_request = function (method, url, headersJson, body, responseType, extrasJson) {
                    globalThis.__captured = {
                        method: method,
                        url: url,
                        body: body,
                        responseType: responseType,
                        extras: JSON.parse(extrasJson)
                    };
                    // Success path with real timings.
                    return {
                        code: 200,
                        status: 200,
                        status_text: 'OK',
                        headers: { 'Content-Type': 'text/plain' },
                        response_time: 150.0,
                        error: '',
                        error_code: 0,
                        timings: {
                            blocked: 1.0, dns: 2.0, connecting: 3.0,
                            tls_handshaking: 0, sending: 0,
                            waiting: 100.0, receiving: 50.0, duration: 150.0
                        },
                        body: 'hello'
                    };
                };

                // 1) Real timings + error/error_code on success.
                var res1 = http.get('http://example.com/');
                globalThis.__t_waiting = res1.timings.waiting;
                globalThis.__t_blocked = res1.timings.blocked;
                globalThis.__t_receiving = res1.timings.receiving;
                globalThis.__err = res1.error;
                globalThis.__ecode = res1.error_code;

                // 2) Binary response body via body_b64 → ArrayBuffer.
                var b64bytes = '';
                // base64("\x00\x01\xFF") = "AAH/"
                globalThis.__tropel_k6_http_request = function (m, u, h, b, rt, ex) {
                    return { code: 200, status_text: 'OK', headers: {}, response_time: 5,
                             body: 'AAH/', body_b64: true, error: '', error_code: 0 };
                };
                var res2 = http.get('http://example.com/bin', { responseType: 'binary' });
                globalThis.__bin = res2.body instanceof ArrayBuffer;
                globalThis.__bin_len = res2.body instanceof ArrayBuffer ? res2.body.byteLength : -1;
                var bytes = new Uint8Array(res2.body);
                globalThis.__bin_0 = bytes[0];
                globalThis.__bin_1 = bytes[1];
                globalThis.__bin_2 = bytes[2];

                // 3) http.file() with binary data → base64 + bodyB64 flag.
                var file = http.file(new Uint8Array([104, 105, 33]), 'hello.bin', 'application/octet-stream');
                globalThis.__file_data_len = file.data.byteLength;
                globalThis.__file_name = file.filename;
                globalThis.__file_ct = file.content_type;
                // Re-arm the CAPTURING stub before the upload: the binary
                // stub above does not capture, so reading __captured here
                // without re-arming would see the stale GET from step 1.
                globalThis.__tropel_k6_http_request = function (method, url, headersJson, body, responseType, extrasJson) {
                    globalThis.__captured = {
                        method: method,
                        url: url,
                        body: body,
                        responseType: responseType,
                        extras: JSON.parse(extrasJson)
                    };
                    return { code: 200, status_text: 'OK', headers: {}, response_time: 5,
                             error: '', error_code: 0, body: 'ok' };
                };
                http.post('http://example.com/upload', file);
                globalThis.__up_body = globalThis.__captured.body;
                globalThis.__up_b64 = globalThis.__captured.extras.bodyB64;

                // 4) Transport failure → res.error / res.error_code set.
                globalThis.__tropel_k6_http_request = function (m, u, h, b, rt, ex) {
                    return { code: 0, status: 0, status_text: 'HTTP error: dns lookup failed',
                             headers: {}, response_time: 0, error: 'dns lookup failed',
                             error_code: 1100, body: '' };
                };
                var res3 = http.get('http://nope.invalid/');
                globalThis.__err3 = res3.error;
                globalThis.__ecode3 = res3.error_code;
            "#,
            )
            .expect("script should eval");

            assert_eq!(
                ctx.eval::<f64, _>("__t_waiting").unwrap(),
                100.0,
                "res.timings.waiting must carry the REAL TTFB (was duration)"
            );
            assert_eq!(
                ctx.eval::<f64, _>("__t_blocked").unwrap(),
                1.0,
                "res.timings.blocked must carry the real pool-wait value"
            );
            assert_eq!(
                ctx.eval::<f64, _>("__t_receiving").unwrap(),
                50.0,
                "res.timings.receiving must carry the real body-receive value"
            );
            assert_eq!(
                ctx.eval::<String, _>("__err").unwrap(),
                "",
                "res.error must be '' on success (k6 parity)"
            );
            assert_eq!(
                ctx.eval::<i64, _>("__ecode").unwrap(),
                0,
                "res.error_code must be 0 on success (k6 parity)"
            );
            assert!(
                ctx.eval::<bool, _>("__bin").unwrap(),
                "binary response body must surface as an ArrayBuffer"
            );
            assert_eq!(
                ctx.eval::<i64, _>("__bin_len").unwrap(),
                3,
                "binary response body must keep its byte length"
            );
            assert_eq!(ctx.eval::<i64, _>("__bin_0").unwrap(), 0, "byte 0");
            assert_eq!(ctx.eval::<i64, _>("__bin_1").unwrap(), 1, "byte 1");
            assert_eq!(ctx.eval::<i64, _>("__bin_2").unwrap(), 255, "byte 2");
            assert_eq!(
                ctx.eval::<i64, _>("__file_data_len").unwrap(),
                3,
                "http.file().data must keep the raw bytes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__file_name").unwrap(),
                "hello.bin",
                "http.file() must preserve the filename"
            );
            assert_eq!(
                ctx.eval::<String, _>("__file_ct").unwrap(),
                "application/octet-stream",
                "http.file() must preserve the content type"
            );
            assert_eq!(
                ctx.eval::<String, _>("__up_body").unwrap(),
                "aGkh",
                "http.file() binary upload must base64-encode the bytes for the bridge"
            );
            assert!(
                ctx.eval::<bool, _>("__up_b64").unwrap(),
                "binary upload must flag bodyB64 so Rust decodes to raw bytes"
            );
            assert_eq!(
                ctx.eval::<String, _>("__err3").unwrap(),
                "dns lookup failed",
                "transport failure must set res.error (k6 `if (res.error)` idiom)"
            );
            assert_eq!(
                ctx.eval::<i64, _>("__ecode3").unwrap(),
                1100,
                "transport failure must set a k6-style error_code (DNS=1100)"
            );
        });
    }

    #[test]
    fn test_push_http_samples_emits_sub_timing_metrics() {
        // Backlog line 150: the k6 path emitted ONLY http_req_duration /
        // http_reqs / http_req_failed / data_* — no waiting/blocked/connecting
        // samples, so k6 thresholds like http_req_waiting:p(95) could never
        // resolve. push_http_samples_for now emits all seven connection-phase
        // Trend samples from the real Timings.
        let sink = std::sync::Mutex::new(Vec::<Sample>::new());
        let scenario: Arc<str> = Arc::from("s");
        let timings = Timings {
            blocked: Duration::from_micros(100),
            dns: Duration::from_micros(200),
            connecting: Duration::from_micros(300),
            tls_handshaking: Duration::ZERO,
            sending: Duration::ZERO,
            waiting: Duration::from_micros(4000),
            receiving: Duration::from_micros(500),
            total: Duration::from_micros(5100),
        };
        push_http_samples_for(
            &sink,
            "http://example.com/",
            "GET",
            200,
            Duration::from_micros(5100),
            42,
            7,
            Some(&timings),
            &scenario,
            None,
            None,
        );
        let samples = sink.lock().unwrap();
        let names: Vec<&str> = samples.iter().map(|s| s.metric.as_ref()).collect();
        for m in [
            "http_req_duration",
            "http_req_blocked",
            "http_req_dns",
            "http_req_connecting",
            "http_req_tls_handshaking",
            "http_req_sending",
            "http_req_waiting",
            "http_req_receiving",
        ] {
            assert!(names.contains(&m), "k6 path must emit {m}, got {names:?}");
        }
        let waiting = samples
            .iter()
            .find(|s| s.metric == "http_req_waiting")
            .unwrap();
        // Values are milliseconds end-to-end (backlog §0): 4000 µs → 4.0 ms,
        // 100 µs → 0.1 ms.
        assert_eq!(waiting.value, 4.0, "waiting must carry the TTFB in ms");
        let blocked = samples
            .iter()
            .find(|s| s.metric == "http_req_blocked")
            .unwrap();
        assert_eq!(blocked.value, 0.1, "blocked must carry the pool-wait in ms");
        assert_eq!(samples.len(), 12, "5 base + 7 sub-timing samples");
    }

    #[test]
    fn test_pm_response_to_be_status_classes() {
        // Backlog line 145: pm.response.to.be.* — the chai-postman status-class
        // getters and to.have.header/body/jsonBody. Getters THROW on failure
        // (so pm.test() records the single failed check).
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/pm-api/pm.js"))
                .expect("pm shim should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__tropel_pm_response_code = function () { return 404; };
                globalThis.__tropel_pm_response_header = function (k) {
                    if (String(k).toLowerCase() === 'content-type') return 'application/json';
                    return null;
                };
                globalThis.__tropel_pm_response_headers = function () {
                    return { 'Content-Type': 'application/json' };
                };
                globalThis.__tropel_pm_response_json = function () { return '{"a":1}'; };
                globalThis.__tropel_pm_response_body = function () { return 'not found'; };

                globalThis.__be_success = String((function () {
                    try { pm.response.to.be.success; return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__be_client_error = String((function () {
                    try { pm.response.to.be.clientError; return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__be_server_error = String((function () {
                    try { pm.response.to.be.serverError; return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__be_error = String((function () {
                    try { pm.response.to.be.error; return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__be_json = String((function () {
                    try { pm.response.to.be.json(); return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__have_hdr = String((function () {
                    try { pm.response.to.have.header('Content-Type', 'application/json'); return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__have_body = String((function () {
                    try { pm.response.to.have.body('not'); return 'passed'; } catch (e) { return 'threw'; }
                })());
                globalThis.__have_json_body = String((function () {
                    try { pm.response.to.have.jsonBody({ a: 1 }); return 'passed'; } catch (e) { return 'threw'; }
                })());
            "#,
            )
            .expect("script should eval");

            assert_eq!(ctx.eval::<String, _>("__be_success").unwrap(), "threw", "404 must NOT be success");
            assert_eq!(ctx.eval::<String, _>("__be_client_error").unwrap(), "passed", "404 must be clientError");
            assert_eq!(ctx.eval::<String, _>("__be_server_error").unwrap(), "threw", "404 must NOT be serverError");
            assert_eq!(ctx.eval::<String, _>("__be_error").unwrap(), "passed", "404 must be error (>=400)");
            assert_eq!(ctx.eval::<String, _>("__be_json").unwrap(), "passed", "content-type json + valid body must pass to.be.json()");
            assert_eq!(ctx.eval::<String, _>("__have_hdr").unwrap(), "passed", "to.have.header must pass when header matches");
            assert_eq!(ctx.eval::<String, _>("__have_body").unwrap(), "passed", "to.have.body must pass when substring present");
            assert_eq!(ctx.eval::<String, _>("__have_json_body").unwrap(), "passed", "to.have.jsonBody must deep-compare");
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
    fn test_exec_members_are_value_properties() {
        // Backlog line 141: exec.vu.idInTest etc. must be VALUE properties
        // (k6 semantics), not functions. The old function-object form broke
        // `if (exec.vu.iterationInScenario === 0)` (always truthy) and
        // `data[exec.vu.idInTest % len]` (NaN → undefined). exec.test must
        // exist so exec.test.abort() doesn't TypeError.
        let rt = rquickjs::Runtime::new().unwrap();
        let ctx = rquickjs::Context::full(&rt).unwrap();
        ctx.with(|ctx| {
            ctx.eval::<(), _>(include_str!("../../../../js/exec/exec.js"))
                .expect("exec shim should eval");
            // Stub the native bridges with known values.
            ctx.eval::<(), _>(
                r#"
                globalThis.__tropel_exec_scenario_name = function () { return 'api_load'; };
                globalThis.__tropel_exec_scenario_executor = function () { return 'shared-iterations'; };
                globalThis.__tropel_exec_vu_id = function () { return 3; };
                globalThis.__tropel_exec_iteration = function () { return 0; };
                globalThis.__tropel_exec_iterations_completed = function () { return 42; };
                globalThis.__tropel_exec_vus_active = function () { return 2; };
                globalThis.__tropel_test_abort = function (msg) { globalThis.__aborted = msg; };
                globalThis.__aborted = null;
            "#,
            )
            .expect("stub should eval");
            ctx.eval::<(), _>(
                r#"
                globalThis.__type_id = typeof exec.vu.idInTest;
                globalThis.__type_iter = typeof exec.vu.iterationInScenario;
                // The two idioms the function-form broke:
                globalThis.__is_first_iter = (exec.vu.iterationInScenario === 0);
                var data = ['a', 'b', 'c', 'd'];
                globalThis.__indexed = data[exec.vu.idInTest % data.length];
                globalThis.__id = exec.vu.idInTest;
                globalThis.__completed = exec.instance.iterationsCompleted;
                globalThis.__vus = exec.instance.vusActive;
                globalThis.__scen = exec.scenario.name;
                globalThis.__exec = exec.scenario.executor;
                // exec.test.abort must exist and reach the bridge.
                globalThis.__test_type = typeof exec.test.abort;
                exec.test.abort('stop now');
            "#,
            )
            .expect("script should eval");

            assert_eq!(ctx.eval::<String, _>("__type_id").unwrap(), "number", "exec.vu.idInTest must be a number value, not a function");
            assert_eq!(ctx.eval::<String, _>("__type_iter").unwrap(), "number", "exec.vu.iterationInScenario must be a number value");
            assert!(ctx.eval::<bool, _>("__is_first_iter").unwrap(), "iterationInScenario === 0 must fire (was always truthy as a function)");
            assert_eq!(ctx.eval::<String, _>("__indexed").unwrap(), "d", "data[idInTest % len] must index (was NaN → undefined)");
            assert_eq!(ctx.eval::<i64, _>("__id").unwrap(), 3);
            assert_eq!(ctx.eval::<i64, _>("__completed").unwrap(), 42);
            assert_eq!(ctx.eval::<i64, _>("__vus").unwrap(), 2);
            assert_eq!(ctx.eval::<String, _>("__scen").unwrap(), "api_load");
            assert_eq!(ctx.eval::<String, _>("__exec").unwrap(), "shared-iterations");
            assert_eq!(ctx.eval::<String, _>("__test_type").unwrap(), "function", "exec.test.abort must be a function");
            assert_eq!(ctx.eval::<String, _>("__aborted").unwrap(), "stop now", "exec.test.abort must reach the native bridge");
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
        assert_eq!(
            map.get("stdout").map(|s| s.as_str()),
            Some("custom stdout: 7")
        );
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
        let result: Option<HashMap<String, String>> =
            ctx.with(|ctx| call_module_handle_summary(&ctx, source, "{}").unwrap());
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
            call_module_handle_summary(&ctx, source, r#"{"state":{"vusMax":3}}"#).unwrap()
        });
        let map = result.expect("async handleSummary must produce output");
        assert_eq!(map.get("stdout").map(|s| s.as_str()), Some("async 3"));
    }

    // ── k6 `open()` + `k6/data` SharedArray ──

    /// Create a JsContext with the k6 file bridges + shim installed.
    async fn ctx_with_file_bridges(script_dir: Option<PathBuf>) -> JsContext {
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        register_k6_file_bridges(&mut js_ctx, script_dir);
        js_ctx
            .bootstrap_library(OPEN_DATA_SHIM)
            .await
            .expect("open-data shim should bootstrap");
        js_ctx
    }

    // ── lodash / CryptoJS shim fidelity (backlog line 155) ──

    /// JsContext with native modules + the full base shim bundle (lodash,
    /// CryptoJS, chai, exec) installed — the same bootstrap production VUs
    /// get, so shim behavior is verified under real native crypto.
    async fn ctx_with_base_shims() -> JsContext {
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(10)))
            .await
            .expect("context creation should succeed");
        bootstrap_js_libs(&mut js_ctx)
            .await
            .expect("shim bootstrap should succeed");
        js_ctx
    }

    /// Regression (backlog line 104): the per-iteration JS interrupt
    /// deadline counted WALL time spent in blocking host calls, so a stock
    /// k6 pacing idiom `http.get(u); sleep(Math.random()*10);` was
    /// interrupted on resume and the run exited non-zero. The sleep bridge
    /// re-arms the deadline, so a sleep LONGER than the eval budget must
    /// still let the following JS run.
    #[tokio::test]
    async fn test_sleep_rearms_interrupt_deadline() {
        // 1 s per-eval deadline; the sleep below is 2 s — longer than the
        // whole budget. With the re-arm the eval must complete.
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(1)))
            .await
            .expect("context creation should succeed");
        bootstrap_js_libs(&mut js_ctx)
            .await
            .expect("shim bootstrap should succeed");
        let out = js_ctx
            .eval("__tropel_native_sleep(2000); 1 + 1")
            .await
            .expect("sleep must not count against the JS execution deadline");
        assert_eq!(out.trim(), "2");
    }

    #[tokio::test]
    async fn test_lodash_get_bracket_and_primitive_paths() {
        // Backlog line 155: `_.get(o,'a[0].b')` returned undefined and
        // `_.get({name:'bob'},'name.length')` THREW ('length' in 'bob').
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    _.get({ a: [{ b: 42 }] }, 'a[0].b'),
                    _.get({ name: 'bob' }, 'name.length'),
                    _.get({ a: { b: null } }, 'a.b.c', 'fallback'),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[42,3,\"fallback\"]",
            "bracket paths + primitive .length + default"
        );
    }

    #[tokio::test]
    async fn test_lodash_every_object_matcher() {
        // Backlog line 155: `_.every([{active:false}],{active:true})`
        // returned TRUE (truthiness branch ignored the object predicate).
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    _.every([{active:false}], {active:true}),
                    _.every([{active:true},{active:true}], {active:true}),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(out, "[false,true]", "object matcher must be honored");
    }

    #[tokio::test]
    async fn test_lodash_set_blocks_proto_pollution() {
        // Backlog line 155: `_.set({}, '__proto__.polluted', 1)` polluted
        // the per-VU context for the whole run.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                _.set({}, '__proto__.polluted', 1);
                ({}).polluted === undefined && Object.prototype.polluted === undefined
                "#,
            )
            .await
            .unwrap();
        assert_eq!(out, "true", "prototype pollution must be blocked");
    }

    #[tokio::test]
    async fn test_lodash_clone_deep_preserves_dates_and_cycles() {
        // Backlog line 155: cloneDeep was JSON round-trip — Dates became
        // strings and cycles threw.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var d = new Date(12345);
                var cyc = { a: 1 }; cyc.self = cyc;
                var c = _.cloneDeep({ d: d, u: undefined });
                var cc = _.cloneDeep(cyc);
                JSON.stringify([
                    c.d instanceof Date,
                    c.d.getTime(),
                    'u' in c,
                    cc.self === cc,
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[true,12345,true,true]",
            "Dates preserved + cycles handled"
        );
    }

    #[tokio::test]
    async fn test_lodash_debounce_coalesces_and_throttle_default_fires() {
        // Backlog line 155: debounce fired once PER CALL in a timer-less
        // runtime (3 calls -> 3 invocations); throttle with no wait NEVER
        // fired (`now - lastCall >= undefined` was always false).
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var n = 0;
                var d = _.debounce(function(){ n++; }, 100);
                d(); d(); d();
                var debounced = n;
                n = 0;
                var t = _.throttle(function(){ n++; });
                t(); t(); t();
                JSON.stringify([debounced, n])
                "#,
            )
            .await
            .unwrap();
        // The eval pumps the microtask queue, so the coalesced debounce fires
        // exactly once by the time we read `debounced`... actually the read
        // happens synchronously in the same eval BEFORE the microtask. Assert
        // debounced is 0 here and verify the coalesced single-fire below via
        // an awaited tick.
        assert_eq!(
            out, "[0,3]",
            "debounce must not fire per call; throttle(no wait) fires"
        );
    }

    #[tokio::test]
    async fn test_lodash_debounce_single_trailing_invocation() {
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval_async(
                r#"
                var n = 0;
                var d = _.debounce(function(){ n++; }, 100);
                d(); d(); d();
                // eval_async uses plain ctx.eval (no top-level await). Return
                // a promise instead: finish_promise drives the job queue in
                // FIFO order, so the debounce's coalesced flush (queued by
                // the 3 d() calls, BEFORE this .then) runs first, then this
                // callback reads n.
                Promise.resolve().then(function(){ return n; })
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "1",
            "3 sync calls must coalesce to ONE trailing invocation"
        );
    }

    #[tokio::test]
    async fn test_lodash_object_collections_and_take_drop_chunk() {
        // Backlog line 155: map/filter/find returned EMPTY for object
        // collections; n===0 off-by-one in take/drop/chunk.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    _.map({a:1,b:2}, function(v){ return v * 2; }),
                    _.filter({a:1,b:0,c:3}, function(v){ return v > 0; }),
                    _.find({a:1,b:2}, function(v){ return v === 2; }),
                    _.take([1,2,3], 0),
                    _.drop([1,2,3], 0),
                    _.chunk([1,2,3], 0),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[[2,4],[1,3],2,[],[1,2,3],[]]",
            "object collections + n===0 semantics"
        );
    }

    #[tokio::test]
    async fn test_lodash_merge_and_reduce_exist() {
        // Backlog line 155: `_.merge`/`_.reduce` did not exist.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    _.merge({a:{b:1}}, {a:{c:2}}),
                    _.reduce([1,2,3,4], function(a,b){ return a + b; }, 0),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[{\"a\":{\"b\":1,\"c\":2}},10]",
            "merge deep-merges; reduce sums"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_hashes_are_real() {
        // Backlog line 155: the fallback FABRICATED a plausible digest
        // (SHA1 and SHA512 of 'hello' both returned 05e918d2...).
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    CryptoJS.SHA1('hello').toString(),
                    CryptoJS.SHA256('hello').toString(),
                    CryptoJS.SHA256('').toString(),
                    CryptoJS.SHA512('hello').toString(),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out,
            "[\"aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d\",\"2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824\",\"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\",\"9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043\"]",
            "real digests must match known vectors"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_sha3_is_keccak_512() {
        // Backlog line 155: SHA3 was SHA3-256; CryptoJS.SHA3 defaults to
        // KECCAK-512. SHA3('hello') must match the Keccak-512 vector.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    CryptoJS.SHA3('hello').toString(),
                    CryptoJS.SHA3('hello', 256).toString(),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out,
            "[\"52fa80662e64c128f8389c9ea6c73d4c02368004bf4463491900d11aaadca39d47de1b01361f207c512cfa79f0f92c3395c67ff7928e3f5ce3e3c852b392f976\",\"1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8\"]",
            "SHA3 default must be Keccak-512 (hello = 52fa…6f976, 256-bit = 1c8a…eac8)"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_base64_parse_padding_and_wordarray_create() {
        // Backlog line 155: Base64.parse corrupted EVERY padded input ('='
        // was in the alphabet at index 64); WordArray.create ignored args.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                JSON.stringify([
                    CryptoJS.enc.Utf8.stringify(CryptoJS.enc.Base64.parse('aGVsbG8=')),
                    CryptoJS.enc.Utf8.stringify(CryptoJS.enc.Base64.parse('aGk=')),
                    CryptoJS.lib.WordArray.create([0x12345678], 4).toString(),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[\"hello\",\"hi\",\"12345678\"]",
            "padded base64 round-trips; WordArray.create honors args"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_utf8_emoji_is_4byte() {
        // Backlog line 155: Utf8.parse emitted CESU-8 (two 3-byte sequences)
        // for emoji instead of one 4-byte UTF-8 code point.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                CryptoJS.enc.Utf8.parse('\u{1F600}').toString(CryptoJS.enc.Hex)
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "f09f9880",
            "emoji must encode as a single 4-byte UTF-8 sequence"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_aes_default_cbc_and_mode_pad_namespaces() {
        // Backlog line 155: AES defaulted to GCM (CryptoJS defaults to
        // CBC/PKCS7), and CryptoJS.mode/CryptoJS.pad did not exist so the
        // universal incantation TypeErrored.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var key = CryptoJS.lib.WordArray.random(32);
                var iv = CryptoJS.lib.WordArray.random(16);
                var ct = CryptoJS.AES.encrypt('hello world', key, {
                    mode: CryptoJS.mode.CBC,
                    padding: CryptoJS.pad.Pkcs7,
                    iv: iv
                });
                var pt = CryptoJS.enc.Utf8.stringify(
                    CryptoJS.AES.decrypt(ct, key, {
                        mode: CryptoJS.mode.CBC,
                        padding: CryptoJS.pad.Pkcs7
                    })
                );
                // Also verify the default (no options) is a working CBC round-trip.
                var ct2 = CryptoJS.AES.encrypt('abc', key);
                var pt2 = CryptoJS.enc.Utf8.stringify(CryptoJS.AES.decrypt(ct2, key));
                JSON.stringify([pt, pt2])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[\"hello world\",\"abc\"]",
            "CBC/PKCS7 with mode+pad namespaces must round-trip"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_aes_128_192_keys_accepted() {
        // Backlog line 155: AES-128/192 were rejected (hardcoded 32-byte).
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var k16 = CryptoJS.enc.Hex.parse('000102030405060708090a0b0c0d0e0f');
                var k24 = CryptoJS.enc.Hex.parse('000102030405060708090a0b0c0d0e0f1011121314151617');
                var c16 = CryptoJS.AES.encrypt('sixteen', k16);
                var c24 = CryptoJS.AES.encrypt('twentyfour', k24);
                JSON.stringify([
                    CryptoJS.enc.Utf8.stringify(CryptoJS.AES.decrypt(c16, k16)),
                    CryptoJS.enc.Utf8.stringify(CryptoJS.AES.decrypt(c24, k24)),
                ])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[\"sixteen\",\"twentyfour\"]",
            "AES-128/192 must round-trip"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_aes_decrypt_uses_passed_key() {
        // Backlog line 155: `ciphertext.key || key` IGNORED the passed key,
        // so a wrong-password decrypt "succeeded" against the embedded key.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var key = CryptoJS.enc.Hex.parse('000102030405060708090a0b0c0d0e0f');
                var wrong = CryptoJS.enc.Hex.parse('000102030405060708090a0b0c0d0e10');
                var ct = CryptoJS.AES.encrypt('secret', key);
                var threw = false;
                try {
                    var out = CryptoJS.enc.Utf8.stringify(CryptoJS.AES.decrypt(ct, wrong));
                    if (out !== 'secret') threw = true;
                } catch (e) { threw = true; }
                threw
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "true",
            "wrong key must fail (not silently use embedded key)"
        );
    }

    #[tokio::test]
    async fn test_cryptojs_aes_passphrase_roundtrip() {
        // Passphrase path: EVP_BytesToKey + Salted__ header round-trip.
        let mut ctx = ctx_with_base_shims().await;
        let out = ctx
            .eval(
                r#"
                var ct = CryptoJS.AES.encrypt('phrase msg', 'correct horse');
                var s = ct.toString();
                var back = CryptoJS.enc.Utf8.stringify(CryptoJS.AES.decrypt(s, 'correct horse'));
                JSON.stringify([s.slice(0, 8) === 'U2FsdGVk', back])
                "#,
            )
            .await
            .unwrap();
        assert_eq!(
            out, "[true,\"phrase msg\"]",
            "passphrase encrypt/decrypt round-trips"
        );
    }

    fn temp_script_dir(tag: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("tropel-k6-open-{}-{}", tag, std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    // ── ES-module local imports (module resolver + loader) ──

    #[tokio::test]
    async fn test_module_local_import_resolves_to_disk() {
        // k6 script importing a local helper: `import { x } from "./helpers.js"`
        // must resolve via the registered module resolver/loader, not fail at
        // eval time (the pre-existing behavior before the loader landed).
        let dir = temp_script_dir("localimport");
        std::fs::write(
            dir.join("helpers.js"),
            "export function triple(x) { return x * 3; }\n",
        )
        .unwrap();
        let source = r#"
            import { triple } from "./helpers.js";
            export default function() { globalThis.__tropel_import_result = triple(14); }
        "#;
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        install_iteration_global(&mut js_ctx, source, None)
            .expect("module with local import should install");
        js_ctx
            .run_script_cached(
                "return __tropel_iteration()",
                Some("k6-iteration.js".to_string()),
            )
            .await
            .expect("iteration should run");
        let result = js_ctx.get_global("__tropel_import_result").await.unwrap();
        assert_eq!(result.as_deref(), Some("42"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_module_local_import_typescript_transpiles() {
        // Imported `.ts` helpers must be transpiled on the fly by the loader.
        let dir = temp_script_dir("localimportts");
        std::fs::write(
            dir.join("calc.ts"),
            "export function add(a: number, b: number): number { return a + b; }\n",
        )
        .unwrap();
        let source = r#"
            import { add } from "./calc.ts";
            export default function() { globalThis.__tropel_ts_result = add(20, 22); }
        "#;
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        install_iteration_global(&mut js_ctx, source, None)
            .expect("module with TS local import should install");
        js_ctx
            .run_script_cached(
                "return __tropel_iteration()",
                Some("k6-iteration.js".to_string()),
            )
            .await
            .expect("iteration should run");
        let result = js_ctx.get_global("__tropel_ts_result").await.unwrap();
        assert_eq!(result.as_deref(), Some("42"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_module_local_import_missing_file_errors() {
        // A local import that doesn't exist on disk must fail loudly at
        // module-install time (matches k6's behavior for unresolvable
        // imports), not silently no-op.
        let dir = temp_script_dir("localimportmissing");
        let source = r#"
            import { nope } from "./does-not-exist.js";
            export default function() {}
        "#;
        let mut js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        let err = install_iteration_global(&mut js_ctx, source, None).err();
        assert!(
            err.is_some(),
            "unresolvable local import must fail module install"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_open_reads_text_relative_to_script_dir() {
        let dir = temp_script_dir("text");
        std::fs::write(dir.join("data.txt"), "hello from open").unwrap();
        let mut js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
        let out = js_ctx
            .eval("open('data.txt')")
            .await
            .expect("open should succeed");
        assert_eq!(out, "hello from open");
        // Absolute path also works.
        let abs = dir.join("data.txt").to_string_lossy().to_string();
        let out = js_ctx
            .eval(&format!("open('{}')", abs.replace('\\', "\\\\")))
            .await
            .expect("absolute open should succeed");
        assert_eq!(out, "hello from open");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_open_binary_returns_array_buffer() {
        let dir = temp_script_dir("bin");
        std::fs::write(dir.join("blob.bin"), [0u8, 1, 2, 255, 128]).unwrap();
        let mut js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
        let out = js_ctx
            .eval(
                "var b = open('blob.bin', 'b');\
                 (b instanceof ArrayBuffer) ? 'AB:' + b.byteLength : 'not-ab';",
            )
            .await
            .expect("binary open should succeed");
        assert_eq!(out, "AB:5");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_open_missing_file_throws_js_error() {
        let dir = temp_script_dir("missing");
        let mut js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
        let out = js_ctx
            .eval(
                "try { open('nope.txt'); 'no-throw'; }\
                 catch (e) { 'threw:' + (e && e.message ? e.message : String(e)); }",
            )
            .await
            .expect("eval should succeed");
        assert!(
            out.starts_with("threw:"),
            "missing file must throw a JS error, got: {out}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_shared_array_computes_once_across_contexts() {
        // First context constructs the SharedArray and populates the native
        // cache; a second context (another VU) must see the same data WITHOUT
        // re-running the factory (k6 semantics: computed once, shared).
        let dir = temp_script_dir("shared");
        let name = "tropel-shared-test-1";
        let mut js_ctx1 = ctx_with_file_bridges(Some(dir.clone())).await;
        let script = format!(
            "var calls = 0;\
             var sa = new SharedArray('{name}', function () {{ calls++; return [10, 20, 30]; }});\
             JSON.stringify({{ len: sa.length, first: sa[0], at: sa.at(1), calls: calls }});"
        );
        let out1 = js_ctx1
            .eval(&script)
            .await
            .expect("first SharedArray construction should succeed");
        assert!(
            out1.contains("\"len\":3")
                && out1.contains("\"first\":10")
                && out1.contains("\"at\":20")
                && out1.contains("\"calls\":1"),
            "first context must run the factory once, got: {out1}"
        );

        // Second context: same name -> cached, factory NOT re-run.
        let mut js_ctx2 = ctx_with_file_bridges(Some(dir.clone())).await;
        let script2 = format!(
            "var calls = 0;\
             var sa = new SharedArray('{name}', function () {{ calls++; return [99]; }});\
             JSON.stringify({{ len: sa.length, first: sa[0], calls: calls }});"
        );
        let out2 = js_ctx2
            .eval(&script2)
            .await
            .expect("cached SharedArray construction should succeed");
        assert!(
            out2.contains("\"len\":3")
                && out2.contains("\"first\":10")
                && out2.contains("\"calls\":0"),
            "second context must reuse cached data without re-running the factory, got: {out2}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_shared_array_is_read_only() {
        let dir = temp_script_dir("ro");
        let mut js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
        let out = js_ctx
            .eval(
                "var sa = new SharedArray('tropel-shared-test-ro', function () { return [1, 2, 3]; });\
                 try { sa[0] = 999; 'no-throw'; }\
                 catch (e) { 'threw:' + (e && e.message ? e.message : String(e)); }",
            )
            .await
            .expect("read-only assignment should be catchable");
        assert!(
            out.starts_with("threw:"),
            "SharedArray writes must throw, got: {out}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn test_declared_options_driver_e2e() {
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
            tropel_sdk::ExecutionConfig::SharedIterations {
                iterations, vus, ..
            } => {
                assert_eq!(iterations, 10);
                assert_eq!(vus, 3);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }

    // ── options type-mismatch hard error (backlog line 153) ──

    #[tokio::test]
    async fn test_declared_options_malformed_returns_err() {
        // Backlog line 153: a script that DECLARES `options` but with a type
        // mismatch must hard-error (k6 aborts) — NOT silently fall back to
        // the CLI profile. `stages[].duration` must be a string; a number is
        // the canonical k6 mistake.
        let driver = K6Driver;
        let script = br#"
            export const options = {
                stages: [ { duration: 60, target: 10 } ]
            };
            export default function() {}
        "#;
        let res = driver.declared_options(script, None, &HashMap::new()).await;
        assert!(
            res.is_err(),
            "malformed options must return Err, got {res:?}"
        );
        let msg = format!("{}", res.unwrap_err());
        assert!(
            msg.contains("k6 script declares `options` but they failed to parse"),
            "error should explain the parse failure, got: {msg}"
        );

        // And a WELL-FORMED script still returns Ok(Some(_)) — the hard-error
        // path must not leak into the happy path.
        let good = br#"
            export const options = { vus: 2, duration: "10s" };
            export default function() {}
        "#;
        let res = driver.declared_options(good, None, &HashMap::new()).await;
        assert!(res.is_ok(), "well-formed options must be Ok, got {res:?}");
        assert!(res.unwrap().is_some(), "declared options must be Some");
    }

    // ── k6 lifecycle: setup() / teardown() (backlog line 127) ──

    // k6 §4 (backlog line 119): setup()/teardown() may make HTTP calls.
    // Stub client returning a canned 200 so the tests exercise the bridge
    // wiring (registration + sink), not real I/O.
    struct StubClient;

    #[async_trait]
    impl DriverHttpClient for StubClient {
        async fn execute(&self, req: &Request) -> Result<Response> {
            Ok(Response {
                url: req.url.clone(),
                status_code: 200,
                status_text: "OK".into(),
                headers: HashMap::new(),
                body: b"ok".to_vec(),
                text_cache: std::cell::OnceCell::new(),
                json_cache: std::cell::OnceCell::new(),
                response_time: std::time::Duration::from_millis(2),
                timings: None,
                cookies: vec![],
                size: 2,
                redirects: vec![],
            })
        }
    }

    async fn test_ctx() -> (
        Arc<dyn DriverHttpClient + Send + Sync>,
        Arc<Mutex<Vec<Sample>>>,
    ) {
        let client: Arc<dyn DriverHttpClient + Send + Sync> = Arc::new(StubClient);
        let sink: Arc<Mutex<Vec<Sample>>> = Arc::new(Mutex::new(Vec::new()));
        (client, sink)
    }

    #[tokio::test]
    async fn test_setup_runs_and_serializes_return_value() {
        // K6Driver::setup must run the script's `export function setup()`
        // ONCE and return its return value serialized as JSON (the engine
        // threads it into every VU as the default function's `data`).
        let driver = K6Driver;
        let script = br#"
            export function setup() { return { token: "abc", n: 42 }; }
            export default function() {}
        "#;
        let (client, sink) = test_ctx().await;
        let data = driver
            .setup(script, None, &HashMap::new(), client, sink)
            .await;
        let json = data.expect("setup() must return serialized data");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["token"], "abc");
        assert_eq!(v["n"], 42);
    }

    #[tokio::test]
    async fn test_setup_absent_returns_none() {
        // A script without `export function setup()` yields None — VUs then
        // see `undefined` data (k6 parity), never a hard error.
        let driver = K6Driver;
        let script = br#"export default function() {}"#;
        let (client, sink) = test_ctx().await;
        assert!(
            driver
                .setup(script, None, &HashMap::new(), client, sink)
                .await
                .is_none(),
            "no setup export must yield None"
        );
    }

    #[tokio::test]
    async fn test_teardown_receives_setup_data() {
        // teardown(data) must receive the setup() return value: with the
        // correct data it completes silently; with WRONG data it throws —
        // which is warn-only and must never surface as a panic/error (k6
        // parity: a throwing teardown never affects the run's exit status).
        let driver = K6Driver;
        let script = br#"
            export function setup() { return { token: "abc" }; }
            export function teardown(data) {
                if (!data || data.token !== "abc") throw new Error("teardown got wrong data");
            }
            export default function() {}
        "#;
        let (client, sink) = test_ctx().await;
        let data = driver
            .setup(script, None, &HashMap::new(), client.clone(), sink.clone())
            .await;
        // Happy path: correct data — no panic, no error.
        driver
            .teardown(
                script,
                None,
                data.as_deref(),
                &HashMap::new(),
                client.clone(),
                sink.clone(),
            )
            .await;
        // Throwing path: wrong data — teardown throws internally, but the
        // driver only logs (warn) and returns; no panic, no error.
        driver
            .teardown(
                script,
                None,
                Some("{\"token\":\"WRONG\"}"),
                &HashMap::new(),
                client.clone(),
                sink.clone(),
            )
            .await;
        driver
            .teardown(
                script,
                None,
                Some("not-json"),
                &HashMap::new(),
                client,
                sink,
            )
            .await;
    }

    #[tokio::test]
    async fn test_setup_can_make_http_calls() {
        // k6 §4 (backlog line 119) regression: the throwaway setup() context
        // previously registered ONLY the file/SharedArray bridges, so
        // `http.get()` threw, the call was logged, and the canonical
        // login-in-setup pattern gave every VU `data === undefined`. The
        // HTTP bridges must now be registered: the call succeeds, its
        // samples land in the sink (for the engine to drain into the run
        // totals — k6 counts setup http_reqs), and its return value is
        // serialized as data.
        let driver = K6Driver;
        let script = br#"
            import http from 'k6/http';
            export function setup() {
                const res = http.get('http://example.com/');
                return { status: res.status };
            }
            export default function() {}
        "#;
        let (client, sink) = test_ctx().await;
        let data = driver
            .setup(script, None, &HashMap::new(), client, sink.clone())
            .await;
        let json = data.expect("setup() with http.get must return data, not None");
        let v: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(
            v["status"], 200,
            "setup http response status must flow through"
        );

        // The http.get call must have recorded samples into the sink.
        let samples = sink.lock().unwrap();
        let names: Vec<&str> = samples.iter().map(|s| s.metric.as_ref()).collect();
        assert!(
            names.contains(&"http_req_duration"),
            "setup http.get must record http_req_duration, got: {:?}",
            names
        );
        assert!(
            names.contains(&"http_reqs"),
            "setup http.get must record http_reqs, got: {:?}",
            names
        );
    }

    #[tokio::test]
    async fn test_setup_data_reaches_default_function() {
        // Full path: the engine sets VuContext.setup_data, the driver seeds
        // __tropel_setup from it once, and the cached iteration call passes
        // it to `export default function (data)`. If data were undefined,
        // `data.token` would throw and run_iteration would error — so a
        // successful iteration proves the data flowed through.
        let driver = K6Driver;
        let script = br#"
            export default function (data) {
                if (!data || data.token !== "abc") throw new Error("setup data not passed");
            }
        "#;
        let mut inst = driver.init(script, None, None).await.unwrap();
        let mut ctx = VuContext::new(0, 0, "default".into());
        ctx.setup_data = Some("{\"token\":\"abc\"}".to_string());
        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration must succeed with setup data");
    }

    #[tokio::test]
    async fn test_no_setup_data_is_undefined() {
        // Without setup data, `data` must be undefined (k6 parity) — a
        // script asserting `data === undefined` succeeds.
        let driver = K6Driver;
        let script = br#"
            export default function (data) {
                if (data !== undefined) throw new Error("expected undefined data");
            }
        "#;
        let mut inst = driver.init(script, None, None).await.unwrap();
        let mut ctx = VuContext::new(0, 0, "default".into());
        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration must succeed with undefined data");
    }

    #[tokio::test]
    async fn test_vu_and_iter_globals_are_numbers() {
        // Backlog line 142: __VU/__ITER must be NUMBERS (k6), not strings.
        // The old set_global_str made `__ITER === 0` never true (the
        // once-per-VU guard never fired), `__VU + 1` produce "11" (string
        // concat), and typeof was "string". __VU is also 1-based (k6).
        let driver = K6Driver;
        let script = br#"
            export default function () {
                if (typeof __VU !== 'number') throw new Error('__VU not a number: ' + typeof __VU);
                if (typeof __ITER !== 'number') throw new Error('__ITER not a number: ' + typeof __ITER);
                if (__VU !== 1) throw new Error('__VU must be 1-based, got ' + __VU);
                if (__VU + 1 !== 2) throw new Error('__VU arithmetic broken: ' + (__VU + 1));
                if (__ITER !== 0) throw new Error('__ITER must start at 0, got ' + __ITER);
                // The idiom the string form broke: `=== 0` must fire.
                globalThis.__guard = (globalThis.__guard || 0) + (__ITER === 0 ? 1 : 0);
                if (globalThis.__guard !== 1) throw new Error('__ITER === 0 guard never fired');
            }
        "#;
        let mut inst = driver.init(script, None, None).await.unwrap();
        let mut ctx = VuContext::new(0, 0, "default".into());
        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration must succeed with numeric __VU/__ITER");
    }
}
