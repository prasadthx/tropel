use crate::error::*;
use rquickjs::function::Func;
use rquickjs::{Context, Runtime};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_CTX_ID: AtomicU64 = AtomicU64::new(1);

/// A per-VU JavaScript execution context backed by rquickjs.
pub struct JsContext {
    ctx: Context,
    context_id: u64,
}

impl JsContext {
    /// Create a new JS context with memory cap and interrupt handler.
    pub async fn new(memory_limit: Option<usize>, max_execution_time: Option<Duration>) -> Result<Self> {
        let runtime = Runtime::new()
            .map_err(|e| JsError::ContextCreation(format!("Runtime creation failed: {}", e)))?;

        // Set memory limit (in bytes)
        if let Some(limit) = memory_limit {
            runtime.set_memory_limit(limit);
        }

        // Set interrupt handler for execution time limit
        let start = Instant::now();
        let duration = max_execution_time.unwrap_or(Duration::from_secs(10));

        runtime.set_interrupt_handler(Some(Box::new(move || {
            start.elapsed() > duration
        })));

        // Create a full-featured context
        let ctx = Context::full(&runtime)
            .map_err(|e| JsError::ContextCreation(format!("Context creation failed: {}", e)))?;

        // Set up the global `console` object
        ctx.with(|ctx| {
            let global = ctx.globals();
            let console = rquickjs::Object::new(ctx).ok();
            if let Some(console) = console {
                let _ = console.set("log", Func::from(|msg: String| {
                    tracing::trace!("[JS console.log] {}", msg);
                }));
                let _ = console.set("warn", Func::from(|msg: String| {
                    tracing::warn!("[JS console.warn] {}", msg);
                }));
                let _ = console.set("error", Func::from(|msg: String| {
                    tracing::error!("[JS console.error] {}", msg);
                }));
                let _ = global.set("console", console);
            }
        });

        let context_id = NEXT_CTX_ID.fetch_add(1, Ordering::SeqCst);

        Ok(Self { ctx, context_id })
    }

    /// Evaluate JavaScript code and return the result as a string.
    pub async fn eval(&self, code: &str) -> Result<String> {
        let code = code.to_string();
        self.ctx
            .with(move |ctx| {
                let value: rquickjs::Value = ctx
                    .eval(code)
                    .map_err(|e| JsError::Eval(format!("JS eval error: {}", e)))?;

                value_to_string(&value, &ctx)
            })
    }

    /// Evaluate a script that returns a Promise and resolve it.
    pub async fn eval_async(&self, code: &str) -> Result<String> {
        let code = code.to_string();
        self.ctx
            .with(move |ctx| {
                let value: rquickjs::Value = ctx
                    .eval(code)
                    .map_err(|e| JsError::Eval(format!("JS eval_async error: {}", e)))?;

                value_to_string(&value, &ctx)
            })
    }

    /// Set a global variable from a string value.
    pub async fn set_global_str(&self, name: &str, value: &str) -> Result<()> {
        let name = name.to_string();
        let value = value.to_string();
        self.ctx
            .with(move |ctx| {
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

        self.ctx
            .with(move |ctx| {
                // Parse JSON as a JS value
                let val: rquickjs::Value = ctx
                    .eval(format!(
                        "JSON.parse({})",
                        serde_json::to_string(&s).unwrap_or_default()
                    ))
                    .map_err(|e| JsError::Conversion(format!("JSON parse in JS context error: {}", e)))?;

                let globals = ctx.globals();
                globals
                    .set(name, val)
                    .map_err(|e| JsError::Conversion(format!("set_global_json error: {}", e)))
            })
    }

    /// Get a global variable as a string.
    pub async fn get_global(&self, name: &str) -> Result<Option<String>> {
        let name = name.to_string();
        self.ctx
            .with(move |ctx| {
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
