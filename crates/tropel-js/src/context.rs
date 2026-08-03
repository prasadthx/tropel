use crate::error::*;
use rquickjs::function::Func;
use rquickjs::{Context, Coerced, FromJs, Function, Persistent, Promise, Runtime};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

static NEXT_CTX_ID: AtomicU64 = AtomicU64::new(1);

/// A compiled script function persisted across `ctx.with()` calls.
///
/// Wraps `rquickjs::Persistent<Function>` which roots the compiled JS
/// function in the Runtime so it survives across `Context::with()` calls
/// without polluting the global namespace.
///
/// Also stores the original source and wrapper offset so that runtime
/// errors from the cached function can report adjusted line numbers
/// pointing back to the user's original source, not the wrapped source.
///
/// # Safety
/// Each `JsContext` owns its own `Runtime`, and a `CachedScript` is only
/// ever created and restored within that Runtime. Sending the cache as
/// part of its owning `JsContext` is safe.
#[derive(Clone)]
pub struct CachedScript {
    func: Persistent<Function<'static>>,
    /// Original (unwrapped) source text, kept for error message context.
    source: Arc<str>,
    /// Optional identifier used as `//# sourceURL` in stack traces.
    source_url: Option<String>,
    /// Number of wrapper lines prepended to user source (e.g. 2 for the
    /// `function __tropel_script(){` + `//# sourceURL=` lines).
    wrapper_offset: u32,
}

impl CachedScript {
    /// Compile a JS function and persist it with source metadata.
    pub fn compile<'js>(
        ctx: &rquickjs::Ctx<'js>,
        func: Function<'js>,
        source: &str,
        source_url: Option<String>,
        wrapper_offset: u32,
    ) -> Self {
        Self {
            func: Persistent::save(ctx, func),
            source: Arc::from(source),
            source_url,
            wrapper_offset,
        }
    }

    /// Restore the function and invoke it with no arguments.
    /// Returns the raw return value (so async scripts can be awaited by
    /// the caller). On error, adjusts line numbers by subtracting the
    /// wrapper offset and includes the original source in the diagnostic.
    pub fn invoke<'js>(&self, ctx: &rquickjs::Ctx<'js>) -> Result<rquickjs::Value<'js>> {
        let func = self
            .func
            .clone()
            .restore(ctx)
            .map_err(|e| JsError::Eval(format!("Script restore error: {}", e)))?;
        func.call::<_, rquickjs::Value>(()).map_err(|e| {
            let err_msg = format!("{}", e);
            let adjusted =
                adjust_error_lines(&err_msg, self.wrapper_offset, self.source_url.as_deref());
            // Show adjusted error + source excerpt
            let label = self.source_url.as_deref().unwrap_or("<script>");
            // Show first N lines of source for context
            let max_preview_lines = 20usize;
            let source_lines: Vec<&str> = self.source.lines().collect();
            let source_preview = if source_lines.len() > max_preview_lines {
                format!(
                    "{}... ({} lines total)",
                    source_lines[..max_preview_lines].join("\n"),
                    source_lines.len()
                )
            } else {
                self.source.to_string()
            };
            JsError::Eval(format!(
                "Script error ({}): {}\n--- source ---\n{}\n--------------",
                label, adjusted, source_preview
            ))
        })
    }
}

/// Adjust line numbers in a QuickJS error message by subtracting the
/// wrapper offset. QuickJS reports line numbers relative to the eval'd
/// source (which includes wrapper lines), but we want to report them
/// relative to the user's original source.
///
/// Handles three patterns:
/// 1. `<eval>:LINE:COL` — runtime errors in eval'd code
/// 2. `sourceURL:LINE:COL` — when `//# sourceURL` is used
/// 3. SyntaxError format: `"(line N, column M)"` — compile-time errors
fn adjust_error_lines(msg: &str, offset: u32, source_url: Option<&str>) -> String {
    if offset == 0 {
        return msg.to_string();
    }

    // Build all known prefixes that introduce a line number.
    let mut prefixes: Vec<&str> = vec!["<eval>:"];
    if let Some(url) = source_url {
        prefixes.push(url); // e.g. "item_name.js" — followed by ":LINE:COL"
    }

    let mut out = String::with_capacity(msg.len());
    let bytes = msg.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        let mut handled = false;

        // Pattern 3: SyntaxError format "(line N, column M)"
        // Only match when preceded by '(' to avoid false positives.
        if bytes[i] == b'(' && i + 7 <= bytes.len() && &bytes[i + 1..i + 6] == b"line " {
            out.push('(');
            out.push_str("line ");
            i += 6;

            let line_start = i;
            while i < bytes.len() && bytes[i].is_ascii_digit() {
                i += 1;
            }
            if i > line_start {
                let line_str = std::str::from_utf8(&bytes[line_start..i]).unwrap_or("0");
                if let Ok(line) = line_str.parse::<u32>() {
                    let adjusted = if line > offset { line - offset } else { 1 };
                    out.push_str(&adjusted.to_string());
                } else {
                    out.push_str(line_str);
                }
            }
            handled = true;
        }

        // Patterns 1 & 2: `<eval>:LINE:COL` and `sourceURL:LINE:COL`
        if !handled {
            for prefix in &prefixes {
                let pb = prefix.as_bytes();
                if i + pb.len() <= bytes.len() && &bytes[i..i + pb.len()] == pb {
                    // Found a known prefix — copy it
                    out.push_str(prefix);
                    i += pb.len();

                    // Skip optional " (" after sourceURL (stack frame format:
                    // "item.js (eval at ..., item.js:LINE:COL)")
                    if i < bytes.len() && bytes[i] == b'(' {
                        out.push('(');
                        i += 1;
                    }

                    // At this point we expect ":LINE" or ":" already consumed
                    // For "<eval>:" we're at ":" and need to advance past it
                    // For sourceURL we may be at ":"
                    if i < bytes.len() && bytes[i] == b':' {
                        out.push(':');
                        i += 1;
                    }

                    // Read line number digits
                    let line_start = i;
                    while i < bytes.len() && bytes[i].is_ascii_digit() {
                        i += 1;
                    }

                    if i > line_start {
                        let line_str = std::str::from_utf8(&bytes[line_start..i]).unwrap_or("0");
                        if let Ok(line) = line_str.parse::<u32>() {
                            let adjusted = if line > offset { line - offset } else { 1 };
                            out.push_str(&adjusted.to_string());
                        } else {
                            out.push_str(line_str);
                        }
                    }

                    handled = true;
                    break;
                }
            }
        }

        if !handled {
            out.push(bytes[i] as char);
            i += 1;
        }
    }

    out
}

/// A per-VU JavaScript execution context backed by rquickjs.
///
/// # Drop-order safety
/// Field order is deliberate: `script_cache` (Persistent<Function>) is declared
/// before `ctx` (Context/Runtime) so Rust drops the cache (Persistents) first,
/// then the Runtime. If ctx were dropped first, live Persistents would reference
/// freed heap memory and rquickjs would abort the process.
pub struct JsContext {
    /// Compiled script cache: source-hash → persistent function.
    /// Declared FIRST so it is dropped BEFORE ctx.
    /// Avoids re-parsing scripts on every iteration.
    script_cache: Mutex<HashMap<u64, CachedScript>>,
    rt: Runtime,
    ctx: Context,
    context_id: u64,
    /// Shared deadline (epoch nanos) for the interrupt handler.
    /// Reset before each eval/eval_async to allow per-script timeouts.
    interrupt_deadline: Arc<AtomicU64>,
    /// Maximum execution time per script eval.
    max_execution_time: Duration,
}

// Safety: each JsContext owns its own rquickjs Runtime, and thread-per-core
// architecture ensures it is only ever used from a single thread at a time.
// Sync is required because `&self` async methods (eval, run_script_cached,
// etc.) need `&JsContext: Send`, which requires `JsContext: Sync`.
unsafe impl Send for JsContext {}
unsafe impl Sync for JsContext {}

/// Get the current time as nanoseconds since UNIX epoch.
fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

impl JsContext {
    /// Create a new JS context with memory cap and interrupt handler.
    pub async fn new(
        memory_limit: Option<usize>,
        max_execution_time: Option<Duration>,
    ) -> Result<Self> {
        let rt = Runtime::new()
            .map_err(|e| JsError::ContextCreation(format!("Runtime creation failed: {}", e)))?;

        // Set memory limit (in bytes)
        if let Some(limit) = memory_limit {
            rt.set_memory_limit(limit);
        }

        let max_execution_time = max_execution_time.unwrap_or(Duration::from_secs(10));
        let initial_deadline = now_nanos() + max_execution_time.as_nanos() as u64;
        let interrupt_deadline = Arc::new(AtomicU64::new(initial_deadline));

        // Set interrupt handler using atomic deadline (reset per-eval)
        let deadline = interrupt_deadline.clone();
        rt.set_interrupt_handler(Some(Box::new(move || {
            now_nanos() > deadline.load(Ordering::Relaxed)
        })));

        // Create a full-featured context
        let ctx = Context::full(&rt)
            .map_err(|e| JsError::ContextCreation(format!("Context creation failed: {}", e)))?;

        // Set up the global `console` object
        ctx.with(|ctx| {
            let global = ctx.globals();
            let console = rquickjs::Object::new(ctx).ok();
            if let Some(console) = console {
                let _ = console.set(
                    "log",
                    Func::from(|msg: String| {
                        tracing::trace!("[JS console.log] {}", msg);
                    }),
                );
                let _ = console.set(
                    "warn",
                    Func::from(|msg: String| {
                        tracing::warn!("[JS console.warn] {}", msg);
                    }),
                );
                let _ = console.set(
                    "error",
                    Func::from(|msg: String| {
                        tracing::error!("[JS console.error] {}", msg);
                    }),
                );
                let _ = global.set("console", console);
            }
        });

        let context_id = NEXT_CTX_ID.fetch_add(1, Ordering::SeqCst);

        Ok(Self {
            ctx,
            rt,
            context_id,
            interrupt_deadline,
            max_execution_time,
            script_cache: Mutex::new(HashMap::new()),
        })
    }

    /// Reset the interrupt deadline to now + max_execution_time.
    /// Called before each eval/eval_async to ensure per-script timeouts.
    ///
    /// Public so callers that evaluate code outside the `eval`-family methods
    /// (e.g. raw ES-module evaluation via `with_ctx`) can also arm the
    /// per-eval timeout instead of inheriting a stale deadline.
    pub fn reset_interrupt(&self) {
        let deadline = now_nanos() + self.max_execution_time.as_nanos() as u64;
        self.interrupt_deadline.store(deadline, Ordering::Relaxed);
    }

    /// Return the interrupt-deadline handle and the max execution time so a
    /// caller that drives a LONG native session from inside a single eval
    /// (e.g. a WebSocket event loop in a k6 `ws.connect`) can re-arm the
    /// per-eval deadline as the session progresses. Without this, a ws
    /// session longer than the script timeout would have its JS handler
    /// invocations interrupted mid-session.
    pub fn interrupt_deadline_handle(&self) -> (Arc<AtomicU64>, Duration) {
        (self.interrupt_deadline.clone(), self.max_execution_time)
    }

    /// Pump the QuickJS job queue to resolve pending promises.
    ///
    /// After evaluating code that creates Promises (via `async` functions or
    /// `new Promise(...)`), the Promise callbacks are queued as pending jobs
    /// in the JS runtime. This method drives those jobs to completion.
    ///
    /// Returns the number of times we pumped (0 means nothing was pending).
    fn pump_promise_queue(&self) -> Result<u32> {
        let mut pump_count = 0u32;
        let max_iterations = 1000; // safety limit
        for _ in 0..max_iterations {
            match self.rt.execute_pending_job() {
                Ok(true) => {
                    pump_count += 1;
                    // More pending — keep pumping
                }
                Ok(false) => {
                    // No more pending jobs
                    break;
                }
                Err(e) => {
                    return Err(JsError::Eval(format!("Promise job error: {}", e)));
                }
            }
        }
        if pump_count >= max_iterations {
            tracing::warn!(
                "Promise queue reached max pump iterations ({}), possible infinite loop",
                max_iterations
            );
        }
        Ok(pump_count)
    }

    /// Drive a JS Promise to completion, returning its resolved value.
    ///
    /// Uses `rquickjs::Promise::finish` which loops over the QuickJS job
    /// queue until the promise is resolved or rejected:
    /// - **resolved** → returns the resolved value
    /// - **rejected** → the rejection reason is converted into a `JsError`
    ///   (no more swallowed rejections)
    /// - **WouldBlock** → the job queue drained without the promise settling
    ///   (e.g. it is pending on an operation the synchronous runtime cannot
    ///   drive, like a real timer) — reported as a clear error instead of
    ///   hanging or silently dropping the promise
    ///
    /// Must be called inside `ctx.with()`. `Ctx::execute_pending_job` used
    /// internally is lock-free, so this does not deadlock against the
    /// runtime lock held by `with`.
    fn finish_promise<'js>(
        ctx: &rquickjs::Ctx<'js>,
        promise: &rquickjs::Promise<'js>,
    ) -> Result<rquickjs::Value<'js>> {
        promise.finish::<rquickjs::Value>().map_err(|e| match e {
            rquickjs::Error::Exception => {
                // The promise rejected — retrieve the thrown value.
                // Because the cached wrapper is an async function, runtime
                // errors in user source arrive here as rejections, so this is
                // the primary error surface. Prefer the stack trace (it
                // carries QuickJS line info) over the bare message, falling
                // back to JS `String(err)` coercion.
                let caught = ctx.catch();
                let reason = Self::rejection_to_string(ctx, &caught)
                    .unwrap_or_else(|| "<non-string rejection reason>".to_string());
                JsError::Eval(format!("Async script rejected: {}", reason))
            }
            rquickjs::Error::WouldBlock => {
                // An infinite microtask loop trips the per-eval interrupt
                // (deadline), which `Ctx::execute_pending_job`'s `res != 0`
                // collapses into WouldBlock. If an exception is pending,
                // report it as the interrupt rather than a misleading
                // "blocked" message.
                if ctx.has_exception() {
                    let caught = ctx.catch();
                    let reason = Self::rejection_to_string(ctx, &caught)
                        .unwrap_or_else(|| "<non-string rejection reason>".to_string());
                    JsError::Eval(format!("Async script interrupted: {}", reason))
                } else {
                    JsError::Eval(
                        "Async script: promise never resolved (blocked on an operation the runtime cannot drive, e.g. a real timer)"
                            .into(),
                    )
                }
            }
            other => JsError::Eval(format!("Async script error: {}", other)),
        })
    }

    /// Convert a promise rejection reason to a readable string.
    ///
    /// Tries, in order:
    /// 1. A JS helper that prefers `e.stack` (line info) over `e`.
    /// 2. `Coerced<String>` (JS `String(value)` coercion — "Error: msg").
    /// 3. `value_to_string` fallback.
    fn rejection_to_string<'js>(
        ctx: &rquickjs::Ctx<'js>,
        caught: &rquickjs::Value<'js>,
    ) -> Option<String> {
        // Stack-first coercion via a tiny JS helper. QuickJS's `e.stack`
        // omits the "Error: <message>" header, so we prepend `e.message`
        // when it isn't already part of the stack.
        if let Ok(stack_fn) = ctx.eval::<rquickjs::Function, _>(
            "(function(e){ var s = e && e.stack ? String(e.stack) : String(e); \
             var m = e && e.message && typeof e.message === 'string' ? e.message : ''; \
             return (m && s.indexOf(m) === -1) ? (m + '\\n' + s) : s; })",
        ) {
            if let Ok(s) = stack_fn.call::<_, std::string::String>((caught.clone(),)) {
                if !s.is_empty() && s != "undefined" && s != "null" {
                    return Some(s);
                }
            }
        }
        // Fall back to JS String() coercion.
        if let Ok(s) = Coerced::<std::string::String>::from_js(ctx, caught.clone()) {
            let s = s.to_string();
            if !s.is_empty() && s != "undefined" && s != "null" {
                return Some(s);
            }
        }
        // Last resort: our own stringifier.
        value_to_string(caught, ctx).ok().filter(|s| !s.is_empty())
    }

    /// Convert a resolved JS value to a useful string: JSON for
    /// objects/arrays, plain string for scalars.
    fn resolved_value_to_string<'js>(
        value: &rquickjs::Value<'js>,
        ctx: &rquickjs::Ctx<'js>,
    ) -> Result<String> {
        if value.is_object() || value.is_array() {
            let globals = ctx.globals();
            let json_fn: rquickjs::Function = globals
                .get("JSON")
                .and_then(|json: rquickjs::Object| json.get("stringify"))
                .map_err(|e| JsError::Conversion(format!("JSON.stringify lookup failed: {}", e)))?;
            json_fn
                .call::<_, String>((value.clone(),))
                .map_err(|e| JsError::Conversion(format!("JSON.stringify failed: {}", e)))
        } else {
            value_to_string(value, ctx)
        }
    }

    /// Evaluate JavaScript code and return the result as a string.
    /// After evaluation, pumps the promise job queue to resolve any
    /// pending microtasks (Promise callbacks, async/await continuations).
    pub async fn eval(&self, code: &str) -> Result<String> {
        self.reset_interrupt();
        let code = code.to_string();
        let result = self.ctx.with(move |ctx| {
            let value: rquickjs::Value = ctx
                .eval(code)
                .map_err(|e| JsError::Eval(format!("JS eval error: {}", e)))?;

            value_to_string(&value, &ctx)
        })?;

        // Pump the promise queue to resolve microtasks
        self.pump_promise_queue()?;

        Ok(result)
    }

    /// Evaluate an async script and resolve its Promise.
    ///
    /// The script should return a Promise (e.g., an async function invocation).
    /// This method:
    /// 1. Evaluates the code
    /// 2. Drives the returned Promise to completion (resolved *value* is
    ///    returned, not a type-name placeholder)
    /// 3. Surfaces rejections as errors instead of swallowing them
    ///
    /// If the script does NOT return a Promise, it behaves like `eval()`.
    pub async fn eval_async(&self, code: &str) -> Result<String> {
        self.reset_interrupt();
        let code = code.to_string();

        // Evaluate the code and resolve any returned promise inside the
        // context lock (Promise::finish drives the job queue itself).
        let result = self.ctx.with(move |ctx| {
            let value: rquickjs::Value = ctx
                .eval(code)
                .map_err(|e| JsError::Eval(format!("JS eval_async error: {}", e)))?;

            if let Some(promise) = value.as_promise() {
                let resolved = Self::finish_promise(&ctx, promise)?;
                Self::resolved_value_to_string(&resolved, &ctx)
            } else {
                value_to_string(&value, &ctx)
            }
        })?;

        // Pump the job queue to resolve any remaining pending microtasks
        // (promises created as side effects, not returned).
        let pump_count = self.pump_promise_queue()?;
        if pump_count > 0 {
            tracing::trace!("Resolved async script (pumped {} times)", pump_count);
        }

        Ok(result)
    }

    /// Set a global variable from a string value.
    pub async fn set_global_str(&self, name: &str, value: &str) -> Result<()> {
        let name = name.to_string();
        let value = value.to_string();
        self.ctx.with(move |ctx| {
            let globals = ctx.globals();
            globals
                .set(name, value)
                .map_err(|e| JsError::Conversion(format!("set_global_str error: {}", e)))
        })
    }

    /// Set a global variable from a JSON value.
    pub async fn set_global_json(&self, name: &str, json_value: &serde_json::Value) -> Result<()> {
        let s = serde_json::to_string(json_value)
            .map_err(|e| JsError::Conversion(format!("JSON serialization error: {}", e)))?;
        let name = name.to_string();

        self.ctx.with(move |ctx| {
            // Parse JSON as a JS value
            let val: rquickjs::Value = ctx
                .eval(format!(
                    "JSON.parse({})",
                    serde_json::to_string(&s).unwrap_or_default()
                ))
                .map_err(|e| {
                    JsError::Conversion(format!("JSON parse in JS context error: {}", e))
                })?;

            let globals = ctx.globals();
            globals
                .set(name, val)
                .map_err(|e| JsError::Conversion(format!("set_global_json error: {}", e)))
        })
    }

    /// Get a global variable as a string.
    pub async fn get_global(&self, name: &str) -> Result<Option<String>> {
        let name = name.to_string();
        self.ctx.with(move |ctx| {
            let globals = ctx.globals();
            let val: rquickjs::Value = globals
                .get(&name)
                .map_err(|e| JsError::Conversion(format!("get_global error: {}", e)))?;

            if val.is_undefined() || val.is_null() {
                return Ok(None);
            }

            value_to_string(&val, &ctx).map(Some)
        })
    }

    /// Execute a JS script and return whether it completed successfully.
    pub async fn run_script(&self, code: &str) -> Result<bool> {
        self.eval(code).await?;
        Ok(true)
    }

    /// Execute a JS script that may contain `await` expressions using a cached
    /// async function.
    ///
    /// Wraps the source in `(async function(){...})()` so `await` is valid,
    /// evaluates it (getting a Promise), drives it to completion via
    /// `Promise::finish` — surfacing rejections as errors — then pumps any
    /// remaining microtasks.
    ///
    /// Note: kept as public API; in-tree callers (runner.rs) now use
    /// `run_script_cached` exclusively (its wrapper is async too), so this
    /// method has no internal callers but remains available for embedders.
    pub async fn run_script_async(&self, source: &str) -> Result<bool> {
        self.reset_interrupt();
        let source = source.to_string();

        // Wrap in an async IIFE so `await` is valid syntax
        let wrapped = format!("(async function __tropel_script(){{{source}}})()");

        self.ctx.with(move |ctx| {
            let promise: Promise = ctx
                .eval(wrapped)
                .map_err(|e| JsError::Eval(format!("Async script compile error: {}", e)))?;
            Self::finish_promise(&ctx, &promise)?;
            Ok::<_, JsError>(())
        })?;

        // Pump the promise queue to resolve any remaining microtasks
        self.pump_promise_queue()?;

        Ok(true)
    }

    /// Execute a JS script using a cached compiled function.
    ///
    /// On first call, the source is wrapped in:
    /// ```text
    /// (async function __tropel_script(){
    /// //# sourceURL=<source_url>
    /// <source>
    /// })
    /// ```
    /// compiled via `ctx.eval()`, and persisted via `Persistent<Function>`.
    /// Subsequent calls restore the persisted function from the cache and
    /// invoke it directly — avoiding re-parsing the source on every iteration.
    ///
    /// The wrapper is an **async** function so top-level `await` / `Promise`
    /// in user scripts is always valid — no fragile substring sniffing to
    /// pick a sync/async path. The returned Promise is driven to completion
    /// via [`JsContext::finish_promise`], so rejections surface as errors and
    /// `await`-dependent code runs to completion.
    ///
    /// The wrapped source puts user code on its own lines so QuickJS error
    /// line numbers are shifted by a known offset (2 lines). When reporting
    /// errors, the offset is subtracted to show the correct location in the
    /// user's original source. The `//# sourceURL` directive gives stack
    /// traces a meaningful identifier instead of bare `<eval>`.
    ///
    /// Uses `rquickjs::Persistent<Function>` which roots the compiled
    /// function in the Runtime (not the global object), so it survives
    /// across `ctx.with()` calls without namespace pollution.
    ///
    /// `source_url` is an optional identifier shown in stack traces (e.g.
    /// `"prerequest.js"` or `"test.js"`). When set, it's injected as
    /// `//# sourceURL=<source_url>` in the wrapper and used in error messages.
    pub async fn run_script_cached(
        &self,
        source: &str,
        source_url: Option<String>,
    ) -> Result<bool> {
        self.reset_interrupt();
        let source = source.to_string();

        let hash = {
            let mut hasher = DefaultHasher::new();
            source.hash(&mut hasher);
            hasher.finish()
        };

        // Wrapper format — 2 lines before user source:
        //   Line 1: (async function __tropel_script(){
        //   Line 2: //# sourceURL=...
        //   Line 3+: user source...
        //   Last:   })
        const WRAPPER_OFFSET: u32 = 2;
        let source_url_str = source_url.as_deref().unwrap_or("script.js");

        // Check cache (lock dropped before ctx.with)
        let cached = {
            let cache = self.script_cache.lock().unwrap();
            cache.get(&hash).cloned()
        };

        // Behavior note: a script whose *returned* promise never settles
        // (e.g. `return new Promise(() => {})`) now errors with
        // "promise never resolved" instead of silently pumping the job queue
        // and moving on — a deliberate improvement (clear error > silent
        // hang), but a semantic change from the old sync wrapper.
        if let Some(script) = cached {
            // Fast path: restore and invoke the persisted function, then
            // drive any returned promise to completion.
            let result = self.ctx.with(|ctx| {
                let value = script.invoke(&ctx)?;
                if let Some(promise) = value.as_promise() {
                    Self::finish_promise(&ctx, promise)?;
                }
                Ok::<_, JsError>(true)
            });
            // Pump promise queue after cached script execution
            self.pump_promise_queue()?;
            return result;
        }

        // Slow path: compile, persist, cache, invoke
        let script = self.ctx.with(move |ctx| {
            let wrapped = format!(
                "(async function __tropel_script(){{\n//# sourceURL={}\n{source}\n}})",
                source_url_str
            );
            let func: Function = ctx.eval(wrapped.as_str()).map_err(|e| {
                let err_msg = format!("{}", e);
                let adjusted = adjust_error_lines(&err_msg, WRAPPER_OFFSET, Some(source_url_str));
                JsError::Eval(format!(
                    "Script compile error ({}): {}",
                    source_url_str, adjusted
                ))
            })?;

            let script = CachedScript::compile(
                &ctx,
                func,
                &source,
                Some(source_url_str.to_string()),
                WRAPPER_OFFSET,
            );

            // Execute now before caching; drive any returned promise.
            let value = script.invoke(&ctx)?;
            if let Some(promise) = value.as_promise() {
                Self::finish_promise(&ctx, promise)?;
            }

            Ok::<_, JsError>(script)
        })?;

        // Pump promise queue after script compilation and execution
        self.pump_promise_queue()?;

        // Store in cache for future calls
        {
            let mut cache = self.script_cache.lock().unwrap();
            cache.entry(hash).or_insert_with(|| script.clone());
        }

        Ok(true)
    }

    /// Run embedded JS library code (bootstrap).
    pub async fn bootstrap_library(&self, code: &str) -> Result<()> {
        tracing::debug!("Bootstrapping JS library ({} chars)", code.len());
        self.eval(code).await?;
        Ok(())
    }

    /// Get the context ID.
    pub fn id(&self) -> u64 {
        self.context_id
    }

    /// Register an ES-module resolver/loader for `import` / `export … from`
    /// specifiers that point at files on disk.
    ///
    /// rquickjs consults the runtime's module loader whenever a declared
    /// module contains an `import` or `export … from` statement, so registering
    /// a resolver + loader lets embedded scripts import local modules (e.g. a
    /// k6 script doing `import { x } from "./helpers.js"`).
    ///
    /// Must be called before the importing module is evaluated. The loader is
    /// installed on the underlying `JSRuntime` (`JS_SetModuleLoaderFunc2`), so
    /// it applies to all contexts of this runtime — no ordering constraint
    /// relative to `Context::full`.
    pub fn set_module_loader<R, L>(&self, resolver: R, loader: L)
    where
        R: rquickjs::loader::Resolver + 'static,
        L: rquickjs::loader::Loader + 'static,
    {
        self.rt.set_loader(resolver, loader);
    }

    /// Execute a closure with access to the underlying rquickjs Ctx.
    /// This is used by bridge modules to register native functions as JS globals.
    /// The closure runs synchronously within the JS context lock.
    pub fn with_ctx<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&rquickjs::Ctx) -> R,
    {
        self.ctx.with(|ctx| f(&ctx))
    }
}

/// Convert a rquickjs Value to a String representation.
fn value_to_string(value: &rquickjs::Value, _ctx: &rquickjs::Ctx) -> Result<String> {
    if value.is_string() {
        value
            .as_string()
            .and_then(|s| s.to_string().ok())
            .ok_or_else(|| JsError::Conversion("Failed to convert JS string".into()))
    } else if value.is_number() {
        let n = value.as_number().unwrap_or(0.0);
        Ok(n.to_string())
    } else if value.is_bool() {
        let b = value.as_bool().unwrap_or(false);
        Ok(b.to_string())
    } else if value.is_object() || value.is_array() {
        // Return the JS type name for complex values.
        // Callers who need serialized JSON should use JSON.stringify()
        // in their JS code so eval returns a string.
        Ok(format!("{:?}", value.type_of()))
    } else {
        Ok(String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn new_ctx() -> JsContext {
        JsContext::new(None, Some(Duration::from_secs(5)))
            .await
            .expect("context creation should succeed")
    }

    #[tokio::test]
    async fn eval_async_returns_resolved_value() {
        let ctx = new_ctx().await;
        // A script that returns a Promise must return the *resolved value*,
        // not a type-name placeholder.
        let r = ctx.eval_async("Promise.resolve(42)").await.unwrap();
        assert_eq!(r, "42");

        let r = ctx.eval_async("Promise.resolve('hello')").await.unwrap();
        assert_eq!(r, "hello");
    }

    #[tokio::test]
    async fn eval_async_returns_json_for_objects() {
        let ctx = new_ctx().await;
        let r = ctx
            .eval_async("Promise.resolve({a: 1, b: [2, 3]})")
            .await
            .unwrap();
        assert!(r.contains("\"a\":1") || r.contains("\"a\": 1"), "got: {}", r);
        assert!(r.contains("\"b\""));
    }

    #[tokio::test]
    async fn eval_async_surfaces_rejections() {
        let ctx = new_ctx().await;
        let err = ctx.eval_async("Promise.reject(new Error('boom'))").await;
        let msg = format!("{:?}", err.err());
        assert!(msg.contains("rejected"), "got: {}", msg);
        assert!(msg.contains("boom"), "got: {}", msg);
    }

    #[tokio::test]
    async fn eval_async_awaits_internal_awaits() {
        let ctx = new_ctx().await;
        // The awaited value must be computed after internal awaits, not the
        // pre-resolution placeholder.
        let r = ctx
            .eval_async("(async () => { await Promise.resolve(1); return 99; })()")
            .await
            .unwrap();
        assert_eq!(r, "99");
    }

    #[tokio::test]
    async fn run_script_cached_handles_top_level_await() {
        let ctx = new_ctx().await;
        // Top-level `await` must be valid inside the cached wrapper.
        let ok = ctx
            .run_script_cached(
                "globalThis.__tropel_flag = 0; await Promise.resolve(); globalThis.__tropel_flag = 1;",
                Some("async-test.js".to_string()),
            )
            .await
            .unwrap();
        assert!(ok);
        let flag = ctx.get_global("__tropel_flag").await.unwrap();
        assert_eq!(flag.as_deref(), Some("1"), "post-await code must run");
    }

    #[tokio::test]
    async fn run_script_cached_surfaces_rejected_promise() {
        let ctx = new_ctx().await;
        // A cached script whose returned promise rejects must surface the
        // error instead of silently swallowing it.
        let err = ctx
            .run_script_cached("return Promise.reject(new Error('kaboom'))", Some("reject.js".to_string()))
            .await
            .err();
        let msg = format!("{:?}", err);
        assert!(msg.contains("rejected"), "got: {}", msg);
        assert!(msg.contains("kaboom"), "got: {}", msg);
    }

    #[tokio::test]
    async fn run_script_cached_sync_script_still_works() {
        let ctx = new_ctx().await;
        let ok = ctx
            .run_script_cached("globalThis.__tropel_x = 7;", Some("sync.js".to_string()))
            .await
            .unwrap();
        assert!(ok);
        let x = ctx.get_global("__tropel_x").await.unwrap();
        assert_eq!(x.as_deref(), Some("7"));
    }
}
