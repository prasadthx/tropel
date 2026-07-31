use crate::error::*;
use rquickjs::function::Func;
use rquickjs::{Context, Function, Persistent, Promise, Runtime};
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
    /// On error, adjusts line numbers by subtracting the wrapper offset
    /// and includes the original source in the diagnostic.
    pub fn invoke<'js>(&self, ctx: &rquickjs::Ctx<'js>) -> Result<()> {
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
        })?;
        Ok(())
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
    /// 2. Pumps the job queue to resolve any pending microtasks
    /// 3. Returns the result as a string
    ///
    /// If the script does NOT return a Promise, it behaves like `eval()`.
    pub async fn eval_async(&self, code: &str) -> Result<String> {
        self.reset_interrupt();
        let code = code.to_string();

        // Evaluate the code
        let result = self.ctx.with(move |ctx| {
            let value: rquickjs::Value = ctx
                .eval(code)
                .map_err(|e| JsError::Eval(format!("JS eval_async error: {}", e)))?;
            value_to_string(&value, &ctx)
        })?;

        // Pump the job queue to resolve any pending promises
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
    /// evaluates it (getting a Promise), then pumps the job queue to resolve
    /// the Promise to completion.
    pub async fn run_script_async(&self, source: &str) -> Result<bool> {
        self.reset_interrupt();
        let source = source.to_string();

        // Wrap in an async IIFE so `await` is valid syntax
        let wrapped = format!("(async function __tropel_script(){{{source}}})()");

        self.ctx.with(move |ctx| {
            let _promise: Promise = ctx
                .eval(wrapped)
                .map_err(|e| JsError::Eval(format!("Async script compile error: {}", e)))?;
            Ok::<_, JsError>(())
        })?;

        // Pump the promise queue to resolve the async function
        self.pump_promise_queue()?;

        Ok(true)
    }

    /// Execute a JS script using a cached compiled function.
    ///
    /// On first call, the source is wrapped in:
    /// ```text
    /// (function __tropel_script(){
    /// //# sourceURL=<source_url>
    /// <source>
    /// })
    /// ```
    /// compiled via `ctx.eval()`, and persisted via `Persistent<Function>`.
    /// Subsequent calls restore the persisted function from the cache and
    /// invoke it directly — avoiding re-parsing the source on every iteration.
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
        //   Line 1: (function __tropel_script(){
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

        if let Some(script) = cached {
            // Fast path: restore and invoke the persisted function
            let result = self.ctx.with(|ctx| {
                script.invoke(&ctx)?;
                Ok::<_, JsError>(true)
            });
            // Pump promise queue after cached script execution
            self.pump_promise_queue()?;
            return result;
        }

        // Slow path: compile, persist, cache, invoke
        let script = self.ctx.with(move |ctx| {
            let wrapped = format!(
                "(function __tropel_script(){{\n//# sourceURL={}\n{source}\n}})",
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

            // Execute now before caching
            script.invoke(&ctx)?;

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
