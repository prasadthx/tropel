//! Per-VU QuickJS context bootstrap.
//!
//! Moved out of the former `engine.rs` god-file.

use std::sync::Arc;
use std::time::Duration;
use tropel_http::client::HttpClient;
use tropel_pm::bridge::SharedPmState;

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
    let ctx = match tropel_js::JsContext::new(Some(10 * 1024 * 1024), Some(Duration::from_secs(10)))
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

    // All shim libraries are concatenated at COMPILE TIME (concat!) into a
    // single &'static str and evaluated with ONE bootstrap eval per VU. Each
    // separate bootstrap_library() call resets the JS interrupt timer, parses
    // the source, and pumps the promise queue, so N calls cost N × that
    // overhead. The shim sources are static include_str! strings byte-identical
    // for every VU, so one combined eval is semantically equivalent while
    // cutting the per-VU bootstrap cost ~5× and allocating nothing at runtime.
    // (rquickjs 0.12 exposes no public script-bytecode API to share compiled
    // shims across VU contexts — only Module bytecode, which doesn't apply to
    // plain-script shims — so a single compile-time-bundled eval is the safe,
    // verifiable win.)
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
    if let Err(e) = ctx.bootstrap_library(JS_SHIM_BUNDLE).await {
        tracing::warn!(
            "VU {}: Failed to bootstrap JS shim bundle: {}",
            vu_id,
            e
        );
    }

    if let Err(e) = tropel_native::install_all(&ctx).await {
        tracing::warn!("VU {}: Failed to install native modules: {}", vu_id, e);
    }

    let bridge = tropel_pm::bridge_fns::PmBridge::new(pm_state.clone(), http_client.clone());
    if let Err(e) = bridge.install(&ctx) {
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
