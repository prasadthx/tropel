//! Per-VU QuickJS context bootstrap.
//!
//! Moved out of the former `engine.rs` god-file.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tropel_http::client::HttpClient;
use tropel_pm::bridge::SharedPmState;

/// All shim libraries concatenated at COMPILE TIME (concat!) into a single
/// `&'static str`, byte-identical for every VU and every scenario.
const JS_SHIM_BUNDLE: &str = concat!(
    "// ==== shim: pm-api ====\n",
    include_str!("../../../js/pm-api/pm.js"),
    "\n",
    "// ==== shim: chai-shim ====\n",
    include_str!("../../../js/chai/chai-shim.js"),
    "\n",
    "// ==== shim: lodash-shim ====\n",
    include_str!("../../../js/lodash/lodash-shim.js"),
    "\n",
    "// ==== shim: cryptojs-shim ====\n",
    include_str!("../../../js/cryptojs-shim/cryptojs.js"),
    "\n",
    "// ==== shim: exec-shim ====\n",
    include_str!("../../../js/exec/exec.js"),
);

/// Process-wide cache of the compiled shim bundle bytecode.
///
/// Compiled ONCE by the first VU (qjsc-style: `JS_Eval` COMPILE_ONLY +
/// `JS_WriteObject`), then every subsequent VU loads the byte blob and runs
/// it instead of re-parsing + re-compiling the shim source. QuickJS bytecode
/// is tied to the build (version + feature flags), not to a particular
/// context, so one compilation is valid for all VU contexts in this process.
///
/// NOTE: this cache is keyed to the single [`JS_SHIM_BUNDLE`] constant. If a
/// second (different) shim bundle is ever added, this needs a per-bundle key
/// (e.g. `OnceLock<HashMap<&'static str, Option<Vec<u8>>>>`) — reusing this
/// static for a different bundle would silently serve the wrong bytecode.
static SHIM_BYTECODE: OnceLock<Option<Vec<u8>>> = OnceLock::new();

/// True once bytecode compilation failed — every VU then falls back to the
/// per-VU source eval path instead of retrying the compile each time.
static SHIM_BYTECODE_FAILED: AtomicBool = AtomicBool::new(false);

/// True once the cached bytecode failed to RUN in a VU. A run failure is
/// deterministic (same bytecode + same bundle for every VU), so after the
/// first failure all subsequent VUs short-circuit straight to the source eval
/// fallback instead of re-attempting the failing bytecode per VU.
static SHIM_BYTECODE_RUN_FAILED: AtomicBool = AtomicBool::new(false);

/// Create a JS context for one VU, bootstrap the bundled shim libraries
/// (pm-api, chai, lodash, crypto, exec), install the native modules and PM
/// bridge functions, and wire a blocking `sleep(seconds)` helper.
///
/// Returns `None` (with a logged warning) if context creation fails — the
/// VU still runs, just without scripts.
pub(crate) async fn create_vu_js_context(
    vu_id: u32,
    pm_state: &SharedPmState,
    http_client: &Arc<HttpClient>,
) -> Option<tropel_js::JsContext> {
    let mut ctx = match tropel_js::JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
        .await
    {
        Ok(ctx) => ctx,
        Err(e) => {
            tracing::warn!(
                "VU {}: Failed to create JS context: {} (scripts will be skipped)",
                vu_id,
                e
            );
            return None;
        }
    };

    bootstrap_shims(&mut ctx, vu_id).await;

    if let Err(e) = tropel_native::install_all(&mut ctx).await {
        tracing::warn!("VU {}: Failed to install native modules: {}", vu_id, e);
    }

    let bridge = tropel_pm::bridge_fns::PmBridge::new(pm_state.clone(), http_client.clone());
    if let Err(e) = bridge.install(&mut ctx) {
        tracing::warn!("VU {}: Failed to install PM bridge functions: {}", vu_id, e);
    }

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

    let sleep_code = [
        "if (typeof sleep === 'undefined') {",
        "  function sleep(seconds) {",
        "    if (typeof __tropel_native_sleep === 'function') {",
        "      __tropel_native_sleep(seconds * 1000);",
        "    }",
        "  }",
        "}",
    ]
    .join("\n");
    let _ = ctx.eval(&sleep_code).await;

    Some(ctx)
}

/// Bootstrap the shared shim libraries into `ctx`.
///
/// Preferred path: load the process-wide compiled [`JS_SHIM_BUNDLE`]
/// bytecode (compiled once by the first VU) and run it in this context —
/// no per-VU parse/compile. Fallback: if bytecode compilation failed or the
/// load/run errored, evaluate the source directly (the pre-bytecode path).
async fn bootstrap_shims(ctx: &mut tropel_js::JsContext, vu_id: u32) {
    let bytecode = SHIM_BYTECODE.get_or_init(|| {
        if SHIM_BYTECODE_FAILED.load(Ordering::Relaxed) {
            return None;
        }
        match ctx.compile_global_bytecode(JS_SHIM_BUNDLE) {
            Ok(bc) => {
                tracing::info!(
                    "Compiled JS shim bundle to bytecode once ({} bytes) — reusing across VUs",
                    bc.len()
                );
                Some(bc)
            }
            Err(e) => {
                SHIM_BYTECODE_FAILED.store(true, Ordering::Relaxed);
                tracing::warn!(
                    "Shim bytecode compilation failed ({}); falling back to per-VU source eval",
                    e
                );
                None
            }
        }
    });

    if let (Some(bc), false) = (bytecode, SHIM_BYTECODE_RUN_FAILED.load(Ordering::Relaxed)) {
        if let Err(e) = ctx.run_global_bytecode(bc).await {
            SHIM_BYTECODE_RUN_FAILED.store(true, Ordering::Relaxed);
            tracing::warn!(
                "VU {}: Failed to run JS shim bytecode: {} (disabling bytecode path; falling back to source eval)",
                vu_id,
                e
            );
            if let Err(e2) = ctx.bootstrap_library(JS_SHIM_BUNDLE).await {
                tracing::warn!("VU {}: Failed to bootstrap JS shim bundle: {}", vu_id, e2);
            }
        }
    } else if let Err(e) = ctx.bootstrap_library(JS_SHIM_BUNDLE).await {
        tracing::warn!("VU {}: Failed to bootstrap JS shim bundle: {}", vu_id, e);
    }
}
