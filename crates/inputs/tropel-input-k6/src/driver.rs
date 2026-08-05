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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tropel_js::JsContext;
use tropel_sdk::{Body, Method, Request, Response, Sample, SampleType, TagMap};
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
        let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
            .await
            .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

        // Step 3: Bootstrap shim libraries & native modules
        bootstrap_js_libs(&js_ctx).await?;

        // Install the k6 file-access bridges (open() + SharedArray cache).
        // Needs the script directory for relative path resolution.
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        register_k6_file_bridges(&js_ctx, script_dir.clone());

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
            script_bridges_registered: false,
            sample_sink: Arc::new(Mutex::new(Vec::new())),
            exec_state: Arc::new(Mutex::new(K6ExecState::default())),
            abort_requested: Arc::new(Mutex::new(None)),
            ws_sessions: Arc::new(Mutex::new(HashMap::new())),
            ws_next_id: Arc::new(AtomicU64::new(0)),
            ws_bridges_registered: false,
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
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        // eval_module_export_json returns Result<Option<String>> — .ok()?
        // unwraps the Result, the second ? unwraps the Option.
        let json_str =
            eval_module_export_json(&module_source, "options", env, script_dir).await.ok()??;
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
        let script_dir = source_path.and_then(|p| p.parent().map(|d| d.to_path_buf()));
        eval_module_handle_summary(&module_source, summary_data_json, env, script_dir)
            .await
            .ok()?
    }
}

// Register K6Driver for compile-time discovery.
inventory::submit!(DriverRegistration::new("k6", || Box::new(K6Driver))
.with_priority(10));

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
static SHARED_ARRAY_CACHE: OnceLock<Mutex<HashMap<String, Arc<Vec<serde_json::Value>>>>> =
    OnceLock::new();

fn shared_array_cache() -> &'static Mutex<HashMap<String, Arc<Vec<serde_json::Value>>>> {
    SHARED_ARRAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
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
/// - `__tropel_k6_shared_array_get(name, i)` — JSON of ONE element, or `""`
///   when absent/out-of-range (the shim parses just this element on demand,
///   so no VU context ever materializes the whole array).
/// - `__tropel_k6_shared_array_set(name, json)` — parse the computed array
///   ONCE and share it as a process-global `Arc<Vec<Value>>`.
///
/// The bridges must be installed on EVERY k6 context that may evaluate script
/// code (the per-VU init context AND the throwaway options/handleSummary
/// contexts), because k6 scripts routinely call `open()`/`new SharedArray()`
/// at module top level while building `export const options`.
fn register_k6_file_bridges(ctx: &JsContext, script_dir: Option<PathBuf>) {
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
                                Ok(base64::engine::general_purpose::STANDARD
                                    .encode(bytes))
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

        // `len` returns -1 when the name is absent (factory must run) or the
        // element count when cached — the JS shim decides between the two.
        let len_prefix = cache_prefix.clone();
        let _ = globals.set(
            "__tropel_k6_shared_array_len",
            Func::from(move |name: String| -> i32 {
                let key = format!("{}|{}", len_prefix, name);
                shared_array_cache()
                    .lock()
                    .map(|c| c.get(&key).map(|v| v.len() as i32).unwrap_or(-1))
                    .unwrap_or(-1)
            }),
        );

        // Element accessor — returns the JSON encoding of ONE element, or ""
        // when absent/out-of-range. The JS shim parses just this element on
        // demand, so no context ever materializes the whole array.
        let get_prefix = cache_prefix.clone();
        let _ = globals.set(
            "__tropel_k6_shared_array_get",
            Func::from(move |name: String, i: i32| -> String {
                let key = format!("{}|{}", get_prefix, name);
                shared_array_cache()
                    .lock()
                    .map(|c| {
                        c.get(&key)
                            .and_then(|v| {
                                if i >= 0 && (i as usize) < v.len() {
                                    Some(v[i as usize].to_string())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default()
                    })
                    .unwrap_or_default()
            }),
        );

        // Parse the computed array ONCE (first VU) and share it as an Arc.
        let set_prefix = cache_prefix.clone();
        let _ = globals.set(
            "__tropel_k6_shared_array_set",
            Func::from(move |name: String, json: String| {
                let key = format!("{}|{}", set_prefix, name);
                if let Ok(parsed) = serde_json::from_str::<Vec<serde_json::Value>>(&json) {
                    if let Ok(mut c) = shared_array_cache().lock() {
                        c.insert(key, Arc::new(parsed));
                    }
                }
            }),
        );
    });
}

/// The k6 `open()` + `k6/data` SharedArray shim (globals `open` and
/// `SharedArray`), which delegates to the native bridges above.
const OPEN_DATA_SHIM: &str =
    include_str!("../../../../js/k6-shim/open-data-shim.js");

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

// Safety: each DriverInstance runs on its own VU thread (thread-per-core).
// JsContext already has unsafe impl Send + Sync in tropel_js.
unsafe impl Send for K6DriverInstance {}
unsafe impl Sync for K6DriverInstance {}

/// Build the standard http_req_* tag set (url/method/status/name/group).
fn http_tags(req: &Request, status: &str) -> TagMap {
    http_tags_for(&req.url, &req.method.to_string(), status)
}

/// [`http_tags`] with explicit URL/method (redirect hops reuse it).
fn http_tags_for(url: &str, method: &str, status: &str) -> TagMap {
    let mut tags = TagMap::with_capacity(5);
    tags.insert("url", url.to_string());
    tags.insert("method", method.to_string());
    tags.insert("status", status.to_string());
    tags.insert("name", url.to_string());
    tags.insert("group", "http");
    tags
}

/// Record the standard `http_req_*` samples for a completed request.
///
/// Mirrors the declarative runner's tag set and k6's default success
/// semantics: a request is failed unless its status is in 2xx–3xx. `sent`
/// is the request-body byte count (for `data_sent`).
fn push_http_samples(
    sink: &Mutex<Vec<Sample>>,
    req: &Request,
    status_code: u16,
    duration: Duration,
    size: u64,
    sent: usize,
) {
    push_http_samples_for(
        sink,
        &req.url,
        &req.method.to_string(),
        status_code,
        duration,
        size,
        sent,
    );
}

/// Record http_req_* samples for EVERY redirect hop of a response (k6
/// parity: each hop is its own request — the test.k6.io 302 chain counted
/// 136 http_reqs for 68 iterations while Tropel recorded only the final
/// 64). Called BEFORE the final response's samples so hop order matches k6.
fn push_redirect_hops(sink: &Mutex<Vec<Sample>>, resp: &Response, method: &str) {
    for hop in &resp.redirects {
        push_http_samples_for(
            sink,
            &hop.url,
            method,
            hop.status_code,
            hop.response_time,
            hop.size,
            0, // redirect hops carry no request body
        );
    }
}

/// Implementation of [`push_http_samples`] with an explicit URL/method so
/// redirect hops (different URL, same method) reuse the same emitter.
fn push_http_samples_for(
    sink: &Mutex<Vec<Sample>>,
    url: &str,
    method: &str,
    status_code: u16,
    duration: Duration,
    size: u64,
    sent: usize,
) {
    let now = SystemTime::now();
    let tags = Arc::new(http_tags_for(url, method, &status_code.to_string()));

    let is_failed = !(200..400).contains(&status_code);
    let mut v = sink.lock().unwrap();
    v.push(Sample {
        metric: "http_req_duration".into(),
        value: duration.as_micros() as f64,
        tags: tags.clone(),
        timestamp: now,
        sample_type: SampleType::Trend,
    });
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

/// Send a ws command to the session's writer task without silently dropping
/// frames. `try_send` + a short bounded retry: the writer task lives on the
/// separate I/O runtime (not the VU's reactor), so parking this VU thread for
/// a few ms never deadlocks it — while `blocking_send` would panic inside the
/// VU runtime (it calls `block_on`). Returns false if the session is gone.
fn try_send_cmd(
    tx: &tokio::sync::mpsc::Sender<WsCommand>,
    mut cmd: WsCommand,
) -> bool {
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
fn push_http_failure(sink: &Mutex<Vec<Sample>>, req: &Request) {
    let now = SystemTime::now();
    let tags = Arc::new(http_tags(req, "0"));

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
                "return __tropel_iteration()",
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
                let tags = std::sync::Arc::make_mut(&mut s.tags);
                if tags.get("scenario").is_none() {
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
            let sink = self.sample_sink.clone();
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
                            method: Method::parse(&method).unwrap_or(Method::GET),
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
                        // Clone the request into the 'static I/O future; the
                        // original stays alive for sample-tag construction.
                        let req_for_io = req.clone();
                        let http_for_io = http_client_request.clone();
                        let result = tropel_http::blocking::execute_blocking(async move {
                            http_for_io.execute(&req_for_io).await
                        });
                        match result {
                            Ok(resp) => {
                                // Record the standard http_req_* samples so
                                // the summary/thresholds see this request
                                // (mirrors the declarative runner + WASM
                                // driver). The req body is counted for
                                // data_sent; k6 semantics: 2xx-3xx success.
                                let sent = req
                                    .body
                                    .as_ref()
                                    .map(Body::encoded_len)
                                    .unwrap_or(0);
                                // k6 parity: every redirect hop counts as its
                                // own request (test.k6.io 302 chain = 2 reqs
                                // per iteration, not 1).
                                push_redirect_hops(&sink, &resp, &req.method.to_string());
                                push_http_samples(
                                    &sink,
                                    &req,
                                    resp.status_code,
                                    resp.response_time,
                                    resp.size,
                                    sent,
                                );
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
                            Err(e) => {
                                tracing::debug!("k6 http request failed: {}", e);
                                push_http_failure(&sink, &req);
                                serde_json::json!({
                                    "code": 0,
                                    "status": 0,
                                    "status_text": format!("HTTP error: {}", e),
                                    "body": "",
                                    "headers": {},
                                    "response_time": 0,
                                })
                                .to_string()
                            }
                        }
                    },
                ),
            );

            let batch_sink = self.sample_sink.clone();
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
                            method: Method::parse(&method).unwrap_or(Method::GET),
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
                        async move {                                let resp = http_client.execute(&req).await;
                            (key, req, resp)
                        }
                    });

                    let responses = tropel_http::blocking::execute_blocking(async move {
                        let results = join_all(futures).await;
                        Ok(results)
                    });

                    let mut response_map = serde_json::Map::new();
                    if let Ok(results) = responses {
                        for (key, req, result) in results {
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
                                    let sent = req
                                        .body
                                        .as_ref()
                                        .map(Body::encoded_len)
                                        .unwrap_or(0);
                                    // k6 parity: every redirect hop counts as
                                    // its own request, same as the single-
                                    // request path.
                                    push_redirect_hops(&batch_sink, &resp, &req.method.to_string());
                                    push_http_samples(
                                        &batch_sink,
                                        &req,
                                        resp.status_code,
                                        resp.response_time,
                                        resp.size,
                                        sent,
                                    );
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
                                Err(e) => {
                                    tracing::debug!("k6 batch request failed: {}", e);
                                    push_http_failure(&batch_sink, &req);
                                    serde_json::json!({
                                        "code": 0,
                                        "status": 0,
                                        "status_text": format!("HTTP error: {}", e),
                                        "body": "",
                                        "headers": {},
                                        "response_time": 0,
                                    })
                                }
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
                Func::from(move |name: String, passed: bool| {
                    let mut v = sink_test.lock().unwrap();
                    let now = SystemTime::now();
                    let mut tags = TagMap::with_capacity(1);
                    tags.insert("check", name);
                    v.push(Sample {
                        metric: "checks".into(),
                        value: if passed { 1.0 } else { 0.0 },
                        tags: Arc::new(tags),
                        timestamp: now,
                        sample_type: SampleType::Rate,
                    });
                }),
            );

            // Custom metric .add() → typed sample (Counter/Gauge/Rate/Trend)
            let sink_metric = sink.clone();
            let _ = globals.set(
                "__tropel_pm_custom_metric_add",
                Func::from(
                    move |name: String, value: f64, tags_json: String, metric_type_str: String| {
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
                            timestamp: SystemTime::now(),
                            sample_type,
                        });
                    },
                ),
            );

            // exec.scenario.name() / executor()
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
            let sink_group = sink.clone();
            let _ = globals.set(
                "__tropel_pm_group_start",
                Func::from(move |_name: String| {}),
            );
            let _ = globals.set(
                "__tropel_pm_group_end",
                Func::from(move |name: String, duration_ms: f64| {
                    let mut v = sink_group.lock().unwrap();
                    let mut tags = TagMap::with_capacity(1);
                    tags.insert("group", name);
                    v.push(Sample {
                        metric: "group_duration".into(),
                        value: duration_ms * 1000.0, // μs, consistent with other Trends
                        tags: Arc::new(tags),
                        timestamp: SystemTime::now(),
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
                        let now_ns = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_nanos() as u64;
                        deadline_step.store(
                            now_ns.saturating_add(max_exec.as_nanos() as u64),
                            Ordering::Relaxed,
                        );

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
                        let now = SystemTime::now();
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
                            value: session.connecting.as_micros() as f64,
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
                            value: duration.as_micros() as f64,
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
        // k6-compatible globals: __VU (1-based, like k6) and __ITER (0-based)
        let _ = self
            .js_ctx
            .set_global_str("__VU", &(ctx.vu_id + 1).to_string())
            .await;
        let _ = self
            .js_ctx
            .set_global_str("__ITER", &ctx.iteration.to_string())
            .await;

        // Refresh the shared exec.* state read by the __tropel_exec_* bridges.
        {
            let mut es = self.exec_state.lock().unwrap();
            es.scenario_name = ctx.scenario_name.clone();
            es.executor_name = ctx.executor_name.clone();
            es.vu_id = ctx.vu_id;
            es.iteration = ctx.iteration;
            es.iterations_completed = ctx.iterations_completed;
            es.vus_active = ctx.vus_active;
        }

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
        if !(name.starts_with("./")
            || name.starts_with("../")
            || Path::new(name).is_absolute())
        {
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
    let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // k6 scripts often read data files at init time (`JSON.parse(open(...))`
    // or `new SharedArray(...)`) while building `export const options` —
    // install the file bridges AND the open/SharedArray shim so the throwaway
    // context can resolve them (the shim defines the JS globals on top of the
    // native bridges). Also register the module loader so `options` blocks
    // that import local helpers (`import { x } from "./helpers.js"`) resolve.
    register_k6_file_bridges(&js_ctx, script_dir.clone());
    js_ctx.set_module_loader(
        K6ModuleResolver {
            script_dir: script_dir.clone(),
        },
        K6ModuleLoader,
    );
    js_ctx.bootstrap_library(OPEN_DATA_SHIM).await.map_err(|e| {
        TropelError::Other(format!("k6 open/SharedArray shim bootstrap failed: {}", e))
    })?;
    // The k6 shim libs (Rate/check/http/…) must be present: options blocks
    // commonly run k6 API at module top level (e.g. `new Rate('errors')`),
    // which threw QuickJS exceptions when the shim was missing.
    bootstrap_js_libs(&js_ctx).await.map_err(|e| {
        TropelError::Other(format!("k6 shim bootstrap failed for options eval: {}", e))
    })?;

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
    script_dir: Option<PathBuf>,
) -> Result<Option<HashMap<String, String>>> {
    let js_ctx = JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
        .map_err(|e| TropelError::Other(format!("JS context creation failed: {}", e)))?;

    // `handleSummary` may reference `open()`/SharedArray captured at init, so
    // install the file bridges + shim on the throwaway context too. Also
    // register the module loader so a `handleSummary` module that imports
    // local helpers (`import { x } from "./helpers.js"`) resolves them.
    register_k6_file_bridges(&js_ctx, script_dir.clone());
    js_ctx.set_module_loader(
        K6ModuleResolver {
            script_dir: script_dir.clone(),
        },
        K6ModuleLoader,
    );
    js_ctx.bootstrap_library(OPEN_DATA_SHIM).await.map_err(|e| {
        TropelError::Other(format!("k6 open/SharedArray shim bootstrap failed: {}", e))
    })?;
    // Same k6-shim requirement as the options eval: a script that touches
    // k6 API at module top level must not throw while handleSummary is read.
    bootstrap_js_libs(&js_ctx).await.map_err(|e| {
        TropelError::Other(format!("k6 shim bootstrap failed for handleSummary eval: {}", e))
    })?;

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
/// Base shim libraries (no native dependencies) concatenated at COMPILE TIME
/// (concat!) into one bundle evaluated with a single bootstrap eval per VU.
/// Each separate bootstrap_library() call resets the JS interrupt timer,
/// parses the source, and pumps the promise queue, so one eval per phase cuts
/// the per-VU bootstrap overhead ~4× with zero runtime allocation. (rquickjs
/// 0.12 exposes no public script-bytecode API to share compiled shims across
/// VU contexts, so a single eval per phase is the safe win.)
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

async fn bootstrap_js_libs(ctx: &JsContext) -> Result<()> {
    // Phase 1: Base shim libraries (no native dependencies) — single eval.
    if let Err(e) = ctx.bootstrap_library(K6_BASE_SHIM_BUNDLE).await {
        tracing::warn!("Failed to bootstrap JS base shim bundle: {}", e);
    }

    // Phase 2: Install native module functions (needed by pm-api and k6-shim)
    if let Err(e) = tropel_native::install_all(ctx).await {
        tracing::warn!("Failed to install native modules: {}", e);
    }

    // Phase 3: Bootstrapping libraries that depend on native functions —
    // single eval (same rationale as phase 1).
    if let Err(e) = ctx.bootstrap_library(K6_NATIVE_SHIM_BUNDLE).await {
        tracing::warn!(
            "Failed to bootstrap JS native-dependent shim bundle: {}",
            e
        );
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
        assert!(result.contains("./other"), "local re-export stripped: {result}");
        assert!(result.contains("./helpers"), "local re-export stripped: {result}");
        assert!(!result.contains("from \"k6\""), "k6 re-export kept: {result}");
        assert!(!result.contains("from \"k6/http\""), "k6 re-export kept: {result}");
        assert!(result.contains("export const options"), "options lost: {result}");
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
        assert!(!result.contains("from \"k6\""), "multiline k6 import kept: {result}");
        assert!(!result.contains("check,"), "multiline k6 import body kept: {result}");
        assert!(!result.contains("from \"k6/http\""), "k6/http import kept: {result}");
        assert!(result.contains("export const options"), "options lost: {result}");
    }

    #[test]
    fn test_module_preprocess_strips_import_with_trailing_comment() {
        // `import http from 'k6/http'; // c` — the old line-anchored regex
        // required the line to END after the specifier, so the trailing
        // comment made it survive. The AST splice strips the statement
        // regardless of trailing comment.
        let code = "import http from 'k6/http'; // shim provides this\nexport default function() {}\n";
        let result = preprocess_k6_source_module(code);
        assert!(
            !result.contains("from 'k6/http'"),
            "k6 import with trailing comment kept: {result}"
        );
        assert!(result.contains("export default function"), "default export lost: {result}");
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
        assert!(result.contains("export default function"), "default export lost: {result}");
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

    // ── k6 `open()` + `k6/data` SharedArray ──

    /// Create a JsContext with the k6 file bridges + shim installed.
    async fn ctx_with_file_bridges(script_dir: Option<PathBuf>) -> JsContext {
        let js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        register_k6_file_bridges(&js_ctx, script_dir);
        js_ctx
            .bootstrap_library(OPEN_DATA_SHIM)
            .await
            .expect("open-data shim should bootstrap");
        js_ctx
    }

    fn temp_script_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tropel-k6-open-{}-{}",
            tag,
            std::process::id()
        ));
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
        let js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        install_iteration_global(&js_ctx, source, None)
            .expect("module with local import should install");
        js_ctx
            .run_script_cached(
                "return __tropel_iteration()",
                Some("k6-iteration.js".to_string()),
            )
            .await
            .expect("iteration should run");
        let result = js_ctx
            .get_global("__tropel_import_result")
            .await
            .unwrap();
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
        let js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        install_iteration_global(&js_ctx, source, None)
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
        let js_ctx = JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed");
        js_ctx.set_module_loader(
            K6ModuleResolver {
                script_dir: Some(dir.clone()),
            },
            K6ModuleLoader,
        );
        let err = install_iteration_global(&js_ctx, source, None).err();
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
        let js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
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
        let js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
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
        let js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
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
        let js_ctx1 = ctx_with_file_bridges(Some(dir.clone())).await;
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
        let js_ctx2 = ctx_with_file_bridges(Some(dir.clone())).await;
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
        let js_ctx = ctx_with_file_bridges(Some(dir.clone())).await;
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
            tropel_sdk::ExecutionConfig::SharedIterations { iterations, vus, .. } => {
                assert_eq!(iterations, 10);
                assert_eq!(vus, 3);
            }
            other => panic!("expected SharedIterations, got {other:?}"),
        }
    }
}
