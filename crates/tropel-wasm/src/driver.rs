//! # Imperative WASM driver — run a WASM module as a per-iteration driver
//!
//! This is the imperative counterpart to the declarative [`WasmInputAdapter`]
//! (super::WasmInputAdapter). Where the declarative path maps a WASM module to
//! a static [`Scenario`] via `adapter_parse`, the imperative path runs the
//! module once per VU *iteration* through the [`Driver`]/[`DriverInstance`]
//! contract — the same entry point k6 scripts use.
//!
//! ## Module ABI
//!
//! A driver module must export:
//!
//! ```wasm
//! ;; Run ONE iteration. `ptr`/`len` point at a JSON document describing the
//! ;; iteration (see "Iteration input" below). Returns 0 on success, non-zero
//! ;; on error (the engine logs the iteration as failed and continues).
//! (func $adapter_run_iteration (export "adapter_run_iteration")
//!   (param $ptr i32) (param $len i32) (result i32))
//! ```
//!
//! and a linear `memory` export (standard `wasm32` cdylib pattern — the host
//! functions read/write request/response buffers through it; modules that
//! *import* a memory are not supported on the driver path). Exporting
//! `malloc`/`free` is recommended: the host then allocates the iteration-input
//! buffer through the module's own allocator (no region collision with the
//! module's persistent state).
//!
//! The module may import host functions:
//!
//! ```wasm
//! ;; Synchronous HTTP request (executed on the engine's I/O runtime; the
//! ;; calling VU thread is parked, so this is safe inside a current-thread VU
//! ;; runtime). `req` is a JSON request document, `resp` is the JSON response
//! ;; document written by the host. Returns bytes written to `resp` (>= 0) or
//! ;; a negative error code. Records http_req_duration / http_reqs /
//! ;; http_req_failed / data_received / data_sent samples for the iteration.
//! (import "env" "http_request" (func $http_request
//!   (param $req_ptr i32) (param $req_len i32)
//!   (param $resp_ptr i32) (param $resp_cap i32) (result i32)))
//!
//! ;; Blocking sleep in milliseconds (blocks the VU thread, matching k6).
//! (import "env" "sleep" (func $sleep (param $ms f64)))
//!
//! ;; Emit a typed sample into the current iteration's metrics.
//! ;; `tags` is a JSON object string; `type_code` is 0=Point, 1=Counter,
//! ;; 2=Trend, 3=Rate (typed samples let thresholds evaluate them).
//! (import "env" "metric_add" (func $metric_add
//!   (param $name_ptr i32) (param $name_len i32) (param $value f64)
//!   (param $tags_ptr i32) (param $tags_len i32) (param $type_code i32)))
//! ```
//!
//! ## Iteration input
//!
//! The `adapter_run_iteration` pointer points at a JSON document:
//!
//! ```json
//! {"vu_id":1, "iteration":0, "scenario_name":"default",
//!  "env":{"KEY":"value"}, "data_row":{"col":"value"} | null}
//! ```
//!
//! ## Request / response JSON
//!
//! Request (module → host):
//! `{"url":"…","method":"GET","headers":{…},"body":"…"|null,"timeout_ms":5000|null,"follow_redirects":true}`
//!
//! Response (host → module): `{"code":200,"status":200,"status_text":"OK",
//! "headers":{…},"body":"…","response_time":12.3,"size":123}`

use crate::{load_module_aot, wasm_engine, DEFAULT_CALL_FUEL, FALLBACK_BASE};
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tropel_core::types::{Body, Method, Request, Sample, SampleType, TagMap};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::{Driver, DriverHttpClient, DriverInstance, DriverRegistration, VuContext};
use wasmtime::{Caller, Extern, Linker, Memory, Module, Store, TypedFunc};

// ══════════════════════════════════════════════════════════════════
// Host-call DoS bounds
// ══════════════════════════════════════════════════════════════════

/// Per-call ceiling for guest-controlled `sleep` / `timeout_ms` values (ms).
/// A hostile module can pass `1e300`; the float→int conversion saturates to
/// `u64::MAX` ms (≈ 584 M years). Fuel does NOT tick during host calls, so
/// this clamp is the DoS guard for the sleep path.
const MAX_HOST_CALL_MS: f64 = 60_000.0;

/// Per-iteration total sleep budget (ms). Host `env.sleep` draws from it;
/// once exhausted, further sleeps are refused (no blocking) and the
/// iteration is failed when the module returns. This bounds a module that
/// would otherwise sleep in repeated 60 s chunks forever (each
/// `adapter_run_iteration` gets a fresh budget).
const ITERATION_SLEEP_BUDGET_MS: f64 = 60_000.0;

/// Per-iteration cap on samples a WASM driver may emit (via `env.metric_add`
/// and the auto-recorded `http_req_*` set). Fuel buys a hostile module
/// millions of `metric_add` calls in one iteration; without a cap its
/// `samples` Vec grows toward multi-GB and downstream tag maps grow
/// unbounded. Exceeding the cap fails the iteration (like the sleep budget).
/// Matches the collector's `MAX_PENDING_SAMPLES` (100k).
const MAX_ITERATION_SAMPLES: usize = 100_000;

/// Cap on a single metric name read from guest memory (bytes). Longer names
/// are refused — otherwise a hostile module could drive unbounded
/// cardinality / per-call allocations with a single 16 MiB name.
const MAX_METRIC_NAME_LEN: usize = 256;

/// Cap on the tag count per `env.metric_add` call, and on the raw JSON tags
/// buffer size (bytes). Refusing oversized tag sets bounds the downstream
/// tag maps.
const MAX_METRIC_TAGS: usize = 32;
const MAX_METRIC_TAGS_BYTES: usize = 64 * 1024;

/// Caps on a single tag KEY / VALUE length (bytes). The per-call buffer cap
/// bounds the JSON parse but not the stored map — without these, 100k capped
/// samples × 32 unbounded tags could still grow resident memory toward
/// multi-GB per VU. Oversized keys/values refuse the whole sample.
const MAX_METRIC_TAG_KEY_LEN: usize = 256;
const MAX_METRIC_TAG_VALUE_LEN: usize = 4 * 1024;

/// Cumulative per-iteration budget on stored tag bytes across ALL samples
/// (both `env.metric_add` and the auto-recorded `http_req_*` set, whose `url`
/// tag is also guest-controlled). The sample-count cap alone does not bound
/// memory when each sample carries up to [`MAX_METRIC_TAGS_BYTES`] of tags;
/// this budget does. Exceeding it fails the iteration like the sample cap.
const MAX_ITERATION_TAG_BYTES: usize = 8 * 1024 * 1024;

// ══════════════════════════════════════════════════════════════════
// WasmDriver — the stateless Driver factory
// ══════════════════════════════════════════════════════════════════

/// The imperative WASM driver. Stateless: `init()` loads the module from the
/// input bytes (or AOT-cached `.cwasm` when a `.wasm` source path is given)
/// and returns a fresh per-VU [`WasmDriverInstance`].
pub struct WasmDriver;

#[async_trait]
impl Driver for WasmDriver {
    fn id(&self) -> &str {
        "wasm"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        // WASM binary magic (\0asm) or WAT text.
        bytes.starts_with(b"\0asm") || bytes.starts_with(b"(module")
    }

    async fn init(
        &self,
        bytes: &[u8],
        source_path: Option<&Path>,
        _exec: Option<&str>,
    ) -> Result<Box<dyn DriverInstance>> {
        // Prefer the AOT cache when a real .wasm file is available; fall back
        // to compiling the raw bytes (e.g. a plugin fed from stdin / memory).
        let module = if let Some(path) = source_path {
            match load_module_aot(path) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        "WasmDriver: AOT load of '{}' failed ({}); compiling from bytes",
                        path.display(),
                        e
                    );
                    Module::new(wasm_engine(), bytes).map_err(|e| {
                        TropelError::Other(format!("WASM driver module is invalid: {}", e))
                    })?
                }
            }
        } else {
            Module::new(wasm_engine(), bytes)
                .map_err(|e| TropelError::Other(format!("WASM driver module is invalid: {}", e)))?
        };

        // Must be an imperative driver module.
        if !module
            .exports()
            .any(|e| e.name() == "adapter_run_iteration")
        {
            return Err(TropelError::Other(
                "WASM module does not export 'adapter_run_iteration' — not an imperative \
                 driver module (declarative adapters use adapter_parse)"
                    .into(),
            ));
        }

        let mut store = Store::new(wasm_engine(), WasmDriverState::default());
        store
            .set_fuel(DEFAULT_CALL_FUEL)
            .map_err(|e| TropelError::Other(format!("WASM fuel setup failed: {}", e)))?;

        let mut linker = Linker::new(wasm_engine());
        linker
            .func_wrap("env", "http_request", http_request_host)
            .map_err(wasm_err)?;
        linker
            .func_wrap(
                "env",
                "sleep",
                |mut caller: Caller<'_, WasmDriverState>, ms: f64| {
                    // NaN / negative / absurd values are coerced first:
                    // NaN.max(0.0) → 0.0, so a hostile `sleep(1e300)` is handled
                    // below instead of saturating to u64::MAX ms (≈ 584 M years).
                    let ms = ms.max(0.0);
                    // Check the RAW value against the remaining budget BEFORE any
                    // clamping: `sleep(60001)` must trip the budget even though
                    // the per-call clamp would reduce it to exactly 60 000 ms.
                    // Over budget → do NOT block (the P0 hang vector). Record the
                    // violation; run_iteration fails the iteration after the call.
                    let sleep_for = ms.min(MAX_HOST_CALL_MS);
                    {
                        let state = caller.data_mut();
                        if ms > state.sleep_budget_ms {
                            state.sleep_over_budget = true;
                            return;
                        }
                        state.sleep_budget_ms -= sleep_for;
                    }
                    if sleep_for > 0.0 {
                        std::thread::sleep(Duration::from_millis(sleep_for as u64));
                    }
                },
            )
            .map_err(wasm_err)?;
        linker
            .func_wrap("env", "metric_add", metric_add_host)
            .map_err(wasm_err)?;
        // Any other imports (WASI etc.) become traps — WASI-less capabilities.
        linker
            .define_unknown_imports_as_traps(&module)
            .map_err(wasm_err)?;

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| TropelError::Other(format!("WASM driver instantiation failed: {}", e)))?;

        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            TropelError::Other(
                "WASM driver module must export a linear 'memory' (cdylib pattern)".into(),
            )
        })?;

        let run_iteration = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "adapter_run_iteration")
            .map_err(wasm_err)?;

        let malloc_fn = instance
            .get_typed_func::<i32, i32>(&mut store, "malloc")
            .ok();
        // C's free is (i32) -> () — looking it up as (i32) -> i32 returns
        // None for every real cdylib, silently disabling the free path and
        // leaking the guest heap every iteration (malloc failure ~1/3 into a
        // long run, surfacing as a generic "WASM memory write failed").
        let free_fn = instance.get_typed_func::<i32, ()>(&mut store, "free").ok();

        Ok(Box::new(WasmDriverInstance {
            store,
            run_iteration,
            memory,
            call_fuel: DEFAULT_CALL_FUEL,
            malloc_fn,
            free_fn,
            module,
            linker,
        }))
    }
}

fn wasm_err(e: impl std::fmt::Display) -> TropelError {
    TropelError::Other(format!("WASM driver error: {}", e))
}

// ══════════════════════════════════════════════════════════════════
// WasmDriverState — the per-store data host functions reach via Caller
// ══════════════════════════════════════════════════════════════════

#[derive(Default)]
pub struct WasmDriverState {
    pub http_client: Option<Arc<dyn DriverHttpClient + Send + Sync>>,
    pub samples: Vec<Sample>,
    /// Remaining per-iteration sleep budget in ms. Host `env.sleep` draws
    /// from it; once exhausted, further sleeps are refused (no blocking) and
    /// [`Self::sleep_over_budget`] is set so the iteration fails. (Fuel does
    /// not tick during host calls, so without this a hostile module could
    /// sleep in repeated 60 s chunks indefinitely.) Reset every iteration.
    pub sleep_budget_ms: f64,
    /// Set when a host `env.sleep` call exceeded the remaining per-iteration
    /// budget. `run_iteration` fails the iteration after the module returns
    /// (the host function cannot trap — it just refuses to block).
    pub sleep_over_budget: bool,
    /// Set when the per-iteration sample cap ([`MAX_ITERATION_SAMPLES`]) or
    /// the cumulative tag-bytes budget ([`MAX_ITERATION_TAG_BYTES`]) was
    /// hit. `run_iteration` fails the iteration after the module returns
    /// (further samples are refused, so the buffer stays bounded).
    pub metric_spam_exceeded: bool,
    /// Stored tag bytes across all samples in this iteration. [`MAX_ITERATION_TAG_BYTES`]
    /// caps the aggregate so 100k capped samples cannot carry multi-GB of tags.
    pub iteration_tag_bytes: usize,
}

// ══════════════════════════════════════════════════════════════════
// WasmDriverInstance — per-VU, holds a persistent Store across iterations
// ══════════════════════════════════════════════════════════════════

pub struct WasmDriverInstance {
    store: Store<WasmDriverState>,
    run_iteration: TypedFunc<(i32, i32), i32>,
    memory: Memory,
    call_fuel: u64,
    malloc_fn: Option<TypedFunc<i32, i32>>,
    free_fn: Option<TypedFunc<i32, ()>>,
    /// The compiled module + linker are retained so a guest trap can be
    /// recovered by re-instantiating into a fresh store (see [`Self::reset`]).
    module: Module,
    linker: Linker<WasmDriverState>,
}

impl WasmDriverInstance {
    /// Discard the current store + instance and re-instantiate the module
    /// into a pristine store. Called after a guest trap: fuel exhaustion is
    /// the EXPECTED trap for a slow iteration, so this is the common path —
    /// the linear memory is left half-mutated (allocator free-list, RefCell
    /// flags, in-progress guards) and must NOT be reused by the next
    /// iteration. Re-instantiating gives a fresh linear memory, fresh
    /// globals, and a default [`WasmDriverState`] (cleared samples/budgets).
    fn reset(&mut self) -> Result<()> {
        // Build the replacement store + instance fully before touching self:
        // if re-instantiation fails, the (trapped) old store is left in place
        // so `self` stays internally consistent.
        let mut store = Store::new(wasm_engine(), WasmDriverState::default());
        store
            .set_fuel(self.call_fuel)
            .map_err(|e| TropelError::Other(format!("WASM fuel setup failed: {}", e)))?;
        let instance = self
            .linker
            .instantiate(&mut store, &self.module)
            .map_err(|e| {
                TropelError::Other(format!("WASM driver re-instantiation failed: {}", e))
            })?;

        // Fetch every handle against the LOCAL store, then commit — reset()
        // is all-or-nothing: on any failure `self` keeps its old store.
        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            TropelError::Other(
                "WASM driver module must export a linear 'memory' (cdylib pattern)".into(),
            )
        })?;
        let run_iteration = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "adapter_run_iteration")
            .map_err(wasm_err)?;
        let malloc_fn = instance
            .get_typed_func::<i32, i32>(&mut store, "malloc")
            .ok();
        // C's free is (i32) -> () — looking it up as (i32) -> i32 returns
        // None for every real cdylib (same reasoning as in init()).
        let free_fn = instance.get_typed_func::<i32, ()>(&mut store, "free").ok();

        self.store = store;
        self.memory = memory;
        self.run_iteration = run_iteration;
        self.malloc_fn = malloc_fn;
        self.free_fn = free_fn;
        Ok(())
    }
}

#[async_trait]
impl DriverInstance for WasmDriverInstance {
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()> {
        // Per-iteration state: fresh client handle + fresh sample buffer +
        // fresh sleep budget (each iteration may legitimately sleep up to
        // ITERATION_SLEEP_BUDGET_MS; a hostile module is bounded per iteration).
        {
            let state = self.store.data_mut();
            state.http_client = ctx.http_client.clone();
            state.samples.clear();
            state.sleep_budget_ms = ITERATION_SLEEP_BUDGET_MS;
            state.sleep_over_budget = false;
            state.metric_spam_exceeded = false;
            state.iteration_tag_bytes = 0;
        }

        // Reset the per-call instruction budget (fuel is consumed per call;
        // set_fuel replaces, so each iteration gets a fresh DoS budget).
        self.store.set_fuel(self.call_fuel).map_err(wasm_err)?;

        let input = serde_json::json!({
            "vu_id": ctx.vu_id,
            "iteration": ctx.iteration,
            "scenario_name": ctx.scenario_name,
            "env": ctx.env,
            "data_row": ctx.data_row,
        });
        let input_bytes = serde_json::to_vec(&input)?;

        // Write the input buffer via the module's malloc when available (no
        // collision with the module's own persistent allocations); otherwise
        // bump from the fallback region (transient per-iteration buffer).
        let (ptr, used_malloc) = if let Some(malloc) = &self.malloc_fn {
            let p = malloc
                .call(&mut self.store, input_bytes.len() as i32)
                .map_err(wasm_err)? as usize;
            self.memory
                .write(&mut self.store, p, &input_bytes)
                .map_err(|e| TropelError::Other(format!("WASM memory write failed: {}", e)))?;
            (p, true)
        } else {
            let end = FALLBACK_BASE + input_bytes.len();
            let needed_pages = end.div_ceil(65536);
            let current = self.memory.size(&self.store) as usize;
            if needed_pages > current {
                self.memory
                    .grow(&mut self.store, (needed_pages - current) as u64)
                    .map_err(|e| TropelError::Other(format!("WASM memory grow failed: {}", e)))?;
            }
            self.memory
                .write(&mut self.store, FALLBACK_BASE, &input_bytes)
                .map_err(|e| TropelError::Other(format!("WASM memory write failed: {}", e)))?;
            (FALLBACK_BASE, false)
        };

        // Capture the call result WITHOUT early-returning on error: samples
        // recorded by host functions must be drained even when the iteration
        // fails (mirrors the declarative runner, which records samples on
        // request failures too). The input buffer is freed ONLY when the
        // guest returned cleanly — never hand a buffer back to the heap of a
        // trapped module (that heap is discarded wholesale on reset below).
        let call_result = self
            .run_iteration
            .call(&mut self.store, (ptr as i32, input_bytes.len() as i32));

        let ret = match call_result {
            Ok(r) => r,
            Err(e) => {
                // Guest trap — fuel exhaustion is the EXPECTED trap for a
                // slow iteration, so this is the common path. The linear
                // memory is left half-mutated (allocator free-list, RefCell
                // flags, in-progress guards) and must NOT be reused: calling
                // free on that heap would corrupt the allocator further, and
                // the next iteration would run on poisoned memory. Drain the
                // samples the host recorded (the reset drops the old store),
                // then re-instantiate into a pristine store.
                {
                    let state = self.store.data_mut();
                    ctx.samples.append(&mut state.samples);
                }
                let err = wasm_err(e);
                if let Err(reset_err) = self.reset() {
                    tracing::warn!(
                        "WASM driver: iteration trapped and re-instantiation failed: {}",
                        reset_err
                    );
                }
                return Err(err);
            }
        };

        // Only reachable when the guest returned cleanly: its heap is in a
        // consistent state, so freeing the malloc'd input buffer is safe.
        if used_malloc {
            if let Some(free) = &self.free_fn {
                let _ = free.call(&mut self.store, ptr as i32);
            }
        }

        // Drain samples collected by host functions — including any recorded
        // by `free` above — unconditionally.
        {
            let state = self.store.data_mut();
            let over_budget = state.sleep_over_budget;
            let spam = state.metric_spam_exceeded;
            ctx.samples.append(&mut state.samples);

            // A host sleep that refused to block (budget exhausted) fails the
            // iteration, mirroring a trap: the module's declared pacing
            // cannot hang the run. (The guest returned normally here — the
            // heap is consistent — so no reset is needed.)
            if over_budget {
                return Err(TropelError::Other(
                    "WASM driver iteration exceeded its per-iteration sleep budget".into(),
                ));
            }
            // Sample spam is bounded the same way: past the cap, further
            // samples are refused and the iteration fails, so a hostile
            // module cannot flood the metrics pipeline with multi-GB of
            // samples.
            if spam {
                return Err(TropelError::Other(
                    "WASM driver iteration exceeded its per-iteration sample cap".into(),
                ));
            }
        }

        if ret != 0 {
            return Err(TropelError::Other(format!(
                "WASM driver iteration returned error code {}",
                ret
            )));
        }
        Ok(())
    }
}

// ══════════════════════════════════════════════════════════════════
// Host functions
// ══════════════════════════════════════════════════════════════════

#[derive(serde::Deserialize)]
struct WasmHttpRequest {
    url: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    timeout_ms: Option<f64>,
    #[serde(default = "default_true")]
    follow_redirects: bool,
}

fn default_true() -> bool {
    true
}

impl WasmHttpRequest {
    /// Convert into a `Request`, failing loudly on a genuinely invalid method
    /// token (empty, whitespace inside, non-tchar chars). A write-path method
    /// must not silently degrade into GET — the host call returns -8 so the
    /// iteration visibly fails. Valid-but-uncommon tokens (PURGE/LINK/…) parse
    /// fine via `Method::Custom`.
    //
    // Note: uses `std::result::Result` (not the tropel_core `Result` alias,
    // which takes one generic arg and fixes the error type to TropelError) —
    // the error here is a plain message string.
    fn into_request(self) -> std::result::Result<Request, String> {
        let req_body = self.body.filter(|b| !b.is_empty()).map(Body::Raw);
        let method = Method::parse(&self.method)
            .ok_or_else(|| format!("invalid HTTP method {:?}", self.method))?;
        Ok(Request {
            url: self.url,
            method,
            headers: self.headers,
            query_params: HashMap::new(),
            body: req_body,
            auth: None,
            certificate: None,
            follow_redirects: self.follow_redirects,
            // Bound the guest-supplied timeout. `timeout_ms: 1e300` would
            // saturate to u64::MAX ms and silently replace the client's
            // default request timeout, parking the caller on rx.recv() with
            // no bound. A non-positive value falls back to the client's
            // DEFAULT_REQUEST_TIMEOUT (10 s) — never an unbounded wait — and
            // an over-ceiling value is clamped with a warning.
            timeout: self.timeout_ms.and_then(|ms| {
                let ms = ms.max(0.0);
                if ms <= 0.0 {
                    // Client default applies (bounded); reqwest treats a
                    // zero timeout as an instant failure, so None is the
                    // correct "no per-request override" spelling.
                    None
                } else if ms > MAX_HOST_CALL_MS {
                    tracing::warn!(
                        "WASM driver timeout_ms({ms}ms) clamped to {}s (DoS guard)",
                        MAX_HOST_CALL_MS as u64 / 1000
                    );
                    Some(Duration::from_millis(MAX_HOST_CALL_MS as u64))
                } else {
                    Some(Duration::from_millis(ms as u64))
                }
            }),
            response_type: tropel_core::types::ResponseType::Text,
        })
    }
}

/// `env.http_request(req_ptr, req_len, resp_ptr, resp_cap) -> i32`
///
/// Reads a JSON request document from WASM memory, executes it synchronously
/// through the per-VU HTTP client (via the shared thread-park helper — safe
/// inside a current-thread VU runtime), records the standard http_req_*
/// samples for the iteration, and writes a JSON response document back to the
/// module's buffer. Returns bytes written (>= 0) or a negative error code.
fn http_request_host(
    mut caller: Caller<'_, WasmDriverState>,
    req_ptr: i32,
    req_len: i32,
    resp_ptr: i32,
    resp_cap: i32,
) -> i32 {
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(m)) => m,
        _ => return -1,
    };
    // Bounds-check BEFORE allocating (P1): a hostile `req_len = i32::MAX`
    // would zero-allocate ~2 GiB and abort() the host (non-unwinding,
    // killing every VU). Clamp to the module's actual memory size — any
    // claim beyond it is a lie, and memory is engine-capped at 16 MiB — so
    // the read below either succeeds or fails with -2, never OOM-aborts.
    let req_len = (req_len.max(0) as usize).min(memory.data_size(&caller));
    let mut req_buf = vec![0u8; req_len];
    if memory
        .read(&caller, req_ptr.max(0) as usize, &mut req_buf)
        .is_err()
    {
        return -2;
    }
    let request: WasmHttpRequest = match serde_json::from_slice(&req_buf) {
        Ok(r) => r,
        Err(_) => return -3,
    };
    let http_client = match caller.data().http_client.clone() {
        Some(c) => c,
        None => return -4,
    };

    // A genuinely invalid method must fail loudly, not silently become GET
    // (backlog line 95).
    let req = match request.into_request() {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("WASM driver http_request rejected: {}", e);
            return -8;
        }
    };
    // Clone the request into the 'static future for the I/O runtime; the
    // original stays alive for sample-tag construction below.
    let req_for_io = req.clone();
    let client_for_io = http_client.clone();
    let result =
        tropel_http::blocking::execute_blocking(
            async move { client_for_io.execute(&req_for_io).await },
        );
    let resp = match result {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("WASM driver http_request failed: {}", e);
            return -5;
        }
    };

    // Record standard samples (mirrors the declarative runner's tags) for
    // EVERY redirect hop plus the final response — k6 parity: a 302 chain
    // counts as hops + 1 requests, not just the final. The final response's
    // URL/status is what the script sees; each hop gets its own sample set.
    {
        // Exact wire size via the SINGLE serializer (percent-encoded
        // urlencoded, multipart framing) — the deleted Body::encoded_len
        // measured raw k=v&k=v with no encoding.
        let data_sent = req.body.as_ref().map(tropel_http::body_size).unwrap_or(0) as f64;
        let chain = resp.redirects.iter().chain(std::iter::once(&resp));
        for hop in chain {
            let now = SystemTime::now();
            let mut tags = TagMap::with_capacity(5);
            tags.insert("url", hop.url.clone());
            tags.insert("method", req.method.to_string());
            tags.insert("status", hop.status_code.to_string());
            tags.insert("name", hop.url.clone());
            tags.insert("group", "http");
            let tags = Arc::new(tags);

            push_iteration_sample(
                caller.data_mut(),
                Sample {
                    metric: "http_req_duration".into(),
                    value: hop.response_time.as_secs_f64() * 1000.0,
                    tags: tags.clone(),
                    timestamp: now,
                    sample_type: SampleType::Trend,
                },
            );
            push_iteration_sample(
                caller.data_mut(),
                Sample {
                    metric: "http_reqs".into(),
                    value: 1.0,
                    tags: tags.clone(),
                    timestamp: now,
                    sample_type: SampleType::Counter,
                },
            );
            // http_req_failed: k6's default semantics (2xx-3xx = success).
            // The declarative runner instead consults the configurable
            // expectedStatuses; the WASM driver has no config channel, so it
            // deliberately matches the k6 default.
            let is_failed = !(200..400).contains(&hop.status_code);
            push_iteration_sample(
                caller.data_mut(),
                Sample {
                    metric: "http_req_failed".into(),
                    value: if is_failed { 1.0 } else { 0.0 },
                    tags: tags.clone(),
                    timestamp: now,
                    sample_type: SampleType::Rate,
                },
            );
            push_iteration_sample(
                caller.data_mut(),
                Sample {
                    metric: "data_received".into(),
                    value: hop.size as f64,
                    tags: tags.clone(),
                    timestamp: now,
                    sample_type: SampleType::Counter,
                },
            );
            // data_sent only on the FINAL response (the one carrying the
            // redirects chain) — redirect hops carry no request body.
            push_iteration_sample(
                caller.data_mut(),
                Sample {
                    metric: "data_sent".into(),
                    value: if std::ptr::eq(hop, &resp) {
                        data_sent
                    } else {
                        0.0
                    },
                    tags,
                    timestamp: now,
                    sample_type: SampleType::Counter,
                },
            );
        }
    }

    let resp_json = serde_json::json!({
        "code": resp.status_code,
        "status": resp.status_code,
        "status_text": resp.status_text,
        "headers": resp.headers,
        "body": String::from_utf8_lossy(&resp.body),
        "response_time": resp.response_time.as_secs_f64() * 1000.0,
        "size": resp.size,
    });
    let bytes = resp_json.to_string().into_bytes();
    if bytes.len() > resp_cap.max(0) as usize {
        return -6; // response buffer too small
    }
    if memory
        .write(&mut caller, resp_ptr.max(0) as usize, &bytes)
        .is_err()
    {
        return -7;
    }
    bytes.len() as i32
}

/// `env.metric_add(name_ptr, name_len, value, tags_ptr, tags_len, type_code)`
///
/// Emits a typed sample for the current iteration. Tags is a JSON object.
/// `type_code` selects the [`SampleType`]: 0=Point, 1=Counter, 2=Trend, 3=Rate
/// — so a WASM module can drive typed custom metrics that thresholds
/// (e.g. `my_trend p95 < 500`) can actually evaluate.
fn metric_add_host(
    mut caller: Caller<'_, WasmDriverState>,
    name_ptr: i32,
    name_len: i32,
    value: f64,
    tags_ptr: i32,
    tags_len: i32,
    type_code: i32,
) {
    // Fast-fail once the iteration is already over the sample/tag caps: skip
    // the reads and parse entirely. A hostile module could otherwise keep
    // paying the full per-call path (name read + tags parse) for every one
    // of the ~50 M fuel-bought calls after the cap trips.
    if caller.data().metric_spam_exceeded {
        return;
    }
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(m)) => m,
        _ => return,
    };
    // Refuse oversized names (cardinality / allocation bound): read at most
    // MAX_METRIC_NAME_LEN + 1 bytes so a hostile module cannot force a 16 MiB
    // allocation per call, and refuse when the name is longer than the cap
    // (strict refusal, not silent truncation — a truncated 300-byte name
    // could collide with a legitimate 256-byte one).
    let name = read_mem_string(
        &memory,
        &caller,
        name_ptr,
        name_len.min(MAX_METRIC_NAME_LEN as i32 + 1),
    );
    if name.is_empty() || name.len() > MAX_METRIC_NAME_LEN {
        return;
    }
    // Bounds-check BEFORE allocating (P1): a hostile tags_len could abort
    // the host with a multi-GB zeroed allocation. Clamp to memory size AND
    // to a sane per-call cap.
    let tags_len = (tags_len.max(0) as usize)
        .min(memory.data_size(&caller))
        .min(MAX_METRIC_TAGS_BYTES);
    let mut tags_buf = vec![0u8; tags_len];
    let mut tags = TagMap::new();
    if memory
        .read(&caller, tags_ptr.max(0) as usize, &mut tags_buf)
        .is_ok()
    {
        if let Ok(map) = serde_json::from_slice::<HashMap<String, String>>(&tags_buf) {
            if map.len() > MAX_METRIC_TAGS {
                return; // refuse oversized tag sets (cardinality bound)
            }
            for (k, v) in map {
                // Per-key/value caps: without these, 100k capped samples × 32
                // unbounded tags could still grow resident memory toward
                // multi-GB (the buffer cap bounds the parse, not the map).
                if k.len() > MAX_METRIC_TAG_KEY_LEN || v.len() > MAX_METRIC_TAG_VALUE_LEN {
                    return;
                }
                tags.insert(k, v);
            }
        }
    }
    // Refuse non-finite values: a guest NaN/Inf sample would poison Counter/
    // Trend aggregates and thresholds downstream (avg/p95 become NaN).
    if !value.is_finite() {
        return;
    }
    let sample_type = match type_code {
        1 => SampleType::Counter,
        2 => SampleType::Trend,
        3 => SampleType::Rate,
        _ => SampleType::Point,
    };
    push_iteration_sample(
        caller.data_mut(),
        Sample {
            metric: name.into(),
            value,
            tags: Arc::new(tags),
            timestamp: SystemTime::now(),
            sample_type,
        },
    );
}

/// Push a sample into the current iteration's buffer, enforcing
/// [`MAX_ITERATION_SAMPLES`] AND the cumulative tag-bytes budget
/// ([`MAX_ITERATION_TAG_BYTES`], computed from the sample's own tags — so it
/// also covers the auto-recorded `http_req_*` set whose `url` tag is
/// guest-controlled). Once a cap is reached, further samples are dropped and
/// [`WasmDriverState::metric_spam_exceeded`] is set so the iteration fails
/// (mirroring the sleep-budget pattern). This bounds the per-iteration
/// `samples` Vec, its resident tag memory, AND the downstream metrics
/// pipeline — fuel buys a hostile module ~50 M `metric_add` calls, which
/// would otherwise grow the Vec toward multi-GB.
fn push_iteration_sample(state: &mut WasmDriverState, sample: Sample) {
    if state.samples.len() >= MAX_ITERATION_SAMPLES {
        state.metric_spam_exceeded = true;
        return;
    }
    let tag_bytes: usize = sample.tags.iter().map(|(k, v)| k.len() + v.len()).sum();
    if state.iteration_tag_bytes.saturating_add(tag_bytes) > MAX_ITERATION_TAG_BYTES {
        state.metric_spam_exceeded = true;
        return;
    }
    state.iteration_tag_bytes += tag_bytes;
    state.samples.push(sample);
}

/// Read a UTF-8 string from WASM memory, stopping at the first NUL.
fn read_mem_string(
    memory: &Memory,
    store: &impl wasmtime::AsContext,
    ptr: i32,
    len: i32,
) -> String {
    if ptr < 0 || len <= 0 {
        return String::new();
    }
    // Bounds-check BEFORE allocating (P1): a hostile length could abort the
    // host with a huge zeroed allocation. Clamp to the module's memory size.
    let len = (len as usize).min(memory.data_size(store));
    let mut buf = vec![0u8; len];
    if memory.read(store, ptr as usize, &mut buf).is_err() {
        return String::new();
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    String::from_utf8_lossy(&buf[..end]).to_string()
}

// ══════════════════════════════════════════════════════════════════
// Registration — compile-time discovery via inventory
// ══════════════════════════════════════════════════════════════════

inventory::submit!(DriverRegistration::new("wasm", || Box::new(WasmDriver)));

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tropel_core::types::Response;

    const DRIVER_WAT: &str = r#"
(module
  (import "env" "http_request" (func $http_request (param i32 i32 i32 i32) (result i32)))
  (import "env" "sleep" (func $sleep (param f64)))
  (import "env" "metric_add" (func $metric_add (param i32 i32 f64 i32 i32 i32)))
  (memory (export "memory") 64 256)
  (global $heap (mut i32) (i32.const 1024))
  (data (i32.const 4096) "{\"url\":\"http://example.com/\",\"method\":\"GET\"}")
  (data (i32.const 8192) "driver_ok\00")
  (data (i32.const 8300) "{}\00")
  (func (export "malloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $size)))
    (local.get $ptr))
  (func (export "free") (param $ptr i32))
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (local $r i32)
    ;; http_request(req at 4096, 44 bytes, resp at 12288, cap 1024)
    (local.set $r (call $http_request (i32.const 4096) (i32.const 44) (i32.const 12288) (i32.const 1024)))
    (if (i32.lt_s (local.get $r) (i32.const 0)) (then (return (i32.const 1))))
    ;; metric_add("driver_ok", 1.0, "{}", type=1 Counter)
    (call $metric_add (i32.const 8192) (i32.const 9) (f64.const 1.0) (i32.const 8300) (i32.const 2) (i32.const 1))
    (i32.const 0))
)
"#;

    const LOOP_DRIVER_WAT: &str = r#"
(module
  (memory (export "memory") 64 256)
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (block $exit
      (loop $spin
        (br $spin)))
    (i32.const 0))
)
"#;

    const SLEEP_DRIVER_WAT: &str = r#"
(module
  (import "env" "sleep" (func $sleep (param f64)))
  (memory (export "memory") 64 256)
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    ;; sleep(ITERATION_SLEEP_BUDGET_MS + 1) — must trap via the budget, not
    ;; actually block the thread.
    (call $sleep (f64.const 60001.0))
    (i32.const 0))
)
"#;

    const SLEEP_OK_DRIVER_WAT: &str = r#"
(module
  (import "env" "sleep" (func $sleep (param f64)))
  (memory (export "memory") 64 256)
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    ;; sleep(1ms) — well within budget, must succeed.
    (call $sleep (f64.const 1.0))
    (i32.const 0))
)
"#;

    const HOSTILE_LEN_DRIVER_WAT: &str = r#"
(module
  (import "env" "http_request" (func $http_request (param i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 64 256)
  (data (i32.const 4096) "{\"url\":\"http://example.com/\"}")
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (local $r i32)
    ;; req_len = i32::MAX — the host must clamp to memory size and fail the
    ;; read (negative return), NOT abort() the process with a ~2 GiB alloc.
    (local.set $r (call $http_request (i32.const 4096) (i32.const 2147483647) (i32.const 12288) (i32.const 1024)))
    ;; Negative result = host rejected safely → iteration succeeds.
    (if (i32.lt_s (local.get $r) (i32.const 0)) (then (return (i32.const 0))))
    (i32.const 1))
)
"#;

    const SPAM_DRIVER_WAT: &str = r#"
(module
  (import "env" "metric_add" (func $metric_add (param i32 i32 f64 i32 i32 i32)))
  (memory (export "memory") 64 256)
  (data (i32.const 4096) "spam\00")
  (data (i32.const 8192) "{}\00")
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (local $i i32)
    (block $done
      (loop $loop
        ;; metric_add("spam", 1.0, "{}", type=1 Counter) — MAX_ITERATION_SAMPLES+2 times
        (call $metric_add (i32.const 4096) (i32.const 4) (f64.const 1.0) (i32.const 8192) (i32.const 2) (i32.const 1))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br_if $done (i32.gt_u (local.get $i) (i32.const 100001)))
        (br $loop)))
    (i32.const 0))
)
"#;

    // A 4 KiB tag VALUE per sample (just under MAX_METRIC_TAG_VALUE_LEN) —
    // repeated calls must trip the cumulative per-iteration tag-bytes budget
    // (MAX_ITERATION_TAG_BYTES = 8 MiB -> ~2047 samples) BEFORE the 100k
    // sample-count cap, and the iteration must fail. The ~4 KiB of tag data
    // is embedded in the WAT data segment, generated with format!.
    fn tag_spam_driver_wat() -> String {
        let big_value = "a".repeat(4096);
        let tags_json = format!(r#"{{"k":"{}"}}"#, big_value);
        let tags_len = tags_json.len();
        // WAT data strings need " and \ escaped.
        let escaped = tags_json.replace('\\', "\\\\").replace('"', "\\\"");
        format!(
            r#"(module
  (import "env" "metric_add" (func $metric_add (param i32 i32 f64 i32 i32 i32)))
  (memory (export "memory") 64 256)
  (data (i32.const 4096) "tagspam\00")
  (data (i32.const 8192) "{escaped}")
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (local $i i32)
    (block $done
      (loop $loop
        ;; metric_add("tagspam", 1.0, <4 KiB tags at 8192>, type=1 Counter)
        (call $metric_add (i32.const 4096) (i32.const 7) (f64.const 1.0) (i32.const 8192) (i32.const {tags_len}) (i32.const 1))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br_if $done (i32.gt_u (local.get $i) (i32.const 5000)))
        (br $loop)))
    (i32.const 0))
)"#,
            escaped = escaped,
            tags_len = tags_len,
        )
    }

    struct StubClient;

    #[async_trait]
    impl DriverHttpClient for StubClient {
        async fn execute(&self, req: &Request) -> Result<Response> {
            Ok(Response {
                url: req.url.clone(),
                status_code: 200,
                status_text: "OK".into(),
                headers: HashMap::new(),
                body: b"hello".to_vec(),
                text_cache: std::cell::OnceCell::new(),
                json_cache: std::cell::OnceCell::new(),
                response_time: Duration::from_millis(5),
                timings: None,
                cookies: vec![],
                size: 5,
                redirects: vec![],
            })
        }
    }

    #[tokio::test]
    async fn test_detect() {
        let driver = WasmDriver;
        assert!(driver.detect(b"\0asm\x01\x00\x00\x00"));
        assert!(driver.detect(b"(module"));
        assert!(!driver.detect(b"export default function() {}"));
    }

    // free is C's `void free(void*)` = (i32)->(). This module's free emits a
    // marker metric through the registered `env.metric_add` host function so
    // the test can assert the host actually invoked free (a lookup with the
    // wrong signature silently yields None and leaks the guest heap).
    const FREE_MARKER_WAT: &str = r#"
(module
  (import "env" "metric_add" (func $metric_add (param i32 i32 f64 i32 i32 i32)))
  (memory (export "memory") 64 256)
  (global $heap (mut i32) (i32.const 1024))
  (data (i32.const 8192) "free_called\00")
  (data (i32.const 8300) "{}\00")
  (func (export "malloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $size)))
    (local.get $ptr))
  (func (export "free") (param $ptr i32)
    ;; (i32)->() — matches C's free. Emits a marker so the host call is observable.
    (call $metric_add (i32.const 8192) (i32.const 11) (f64.const 1.0) (i32.const 8300) (i32.const 2) (i32.const 1)))
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (i32.const 0))
)
"#;

    const DECLARATIVE_ONLY_WAT: &str = r#"
(module
  (memory (export "memory") 64 256)
  (data (i32.const 0) "declarative-only\00")
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $p i32) (param $n i32) (result i32)
    (i32.const 0))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    (i32.const 0))
)
"#;

    #[tokio::test]
    async fn test_init_requires_run_iteration_export() {
        // A declarative-only module (adapter_parse, no adapter_run_iteration)
        // must be rejected as a driver with a clear error. The module is VALID
        // wasm — only the export is missing — so the rejection genuinely
        // exercises the export check, not a parse failure.
        let result = WasmDriver
            .init(DECLARATIVE_ONLY_WAT.as_bytes(), None, None)
            .await;
        let msg = match result {
            Ok(_) => panic!("declarative-only module must be rejected as a driver"),
            Err(e) => format!("{}", e),
        };
        assert!(
            msg.contains("adapter_run_iteration"),
            "error should mention the missing export, got: {}",
            msg
        );
    }

    #[tokio::test]
    async fn test_many_concurrent_driver_instances_start() {
        // P0 regression: the pooling allocator capped concurrent WASM memories
        // engine-wide at `total_memories(16)`. On the DRIVER path every VU
        // holds a live Store/Instance for the whole test, so `--vus 500`
        // silently ran only 16 VUs — VU #17 failed to instantiate and
        // `vu_loop.rs` swallowed the error (the summary reported the requested
        // count). The pool now holds 4096 instances; 32 concurrent driver
        // instances must all start.
        let driver = WasmDriver;
        let mut instances: Vec<Box<dyn DriverInstance>> = Vec::new();
        for _ in 0..32 {
            let inst = driver
                .init(DRIVER_WAT.as_bytes(), None, None)
                .await
                .expect("every concurrent driver instance must start");
            instances.push(inst);
        }
        assert_eq!(instances.len(), 32);
    }

    #[tokio::test]
    async fn test_free_invoked_after_iteration() {
        // P1 regression: free was looked up as TypedFunc<i32, i32> but C's
        // free is (i32)->() — get_typed_func returned None for every real
        // module, so the host's free path was dead code and the guest heap
        // leaked one malloc per iteration (malloc failures ~1/3 into a long
        // run, surfacing as a generic "WASM memory write failed"). The module
        // here emits a marker metric from its free; the host must invoke free
        // after run_iteration so the marker lands in ctx.samples.
        let driver = WasmDriver;
        let mut inst = driver
            .init(FREE_MARKER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        ctx.http_client = Some(Arc::new(StubClient));

        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration must succeed");

        let names: Vec<&str> = ctx.samples.iter().map(|s| s.metric.as_ref()).collect();
        assert!(
            names.contains(&"free_called"),
            "free must be invoked after run_iteration (marker 'free_called' missing): {:?}",
            names
        );
    }

    // Module that reads the `iteration` field from the host-provided input
    // JSON (by scanning for the byte sequence "iteration"): iteration 0 is a
    // "slow" iteration — it grows the linear memory, scribbles the heap via
    // malloc, then spins until fuel exhaustion (the EXPECTED trap) — while
    // iteration 1+ is healthy and reports memory.size so the test can tell
    // whether the instance was reset (fresh store ⇒ 64 pages) or reused
    // (grown store ⇒ 65 pages). free() emits a marker so the test can also
    // assert the host never frees into a trapped heap.
    const TRAP_RESET_WAT: &str = r#"
(module
  (import "env" "metric_add" (func $metric_add (param i32 i32 f64 i32 i32 i32)))
  (memory (export "memory") 64 256)
  (global $heap (mut i32) (i32.const 1024))
  (data (i32.const 8192) "mem_pages\00")
  (data (i32.const 8210) "pages_after_grow\00")
  (data (i32.const 8300) "{}\00")
  (data (i32.const 8500) "free_called\00")
  (data (i32.const 8600) "digit\00")
  (func $malloc (export "malloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $size)))
    (local.get $ptr))
  (func (export "free") (param $ptr i32)
    ;; free is C's (i32)->() signature; emits a marker so the host call is
    ;; observable (must NEVER fire for a trapped iteration).
    (call $metric_add (i32.const 8500) (i32.const 11) (f64.const 1.0) (i32.const 8300) (i32.const 2) (i32.const 1)))
  ;; Read the iteration digit from the input JSON at a FIXED offset. The host
  ;; serializes {"data_row":null,"env":{},"iteration":N,"scenario_name":...,"vu_id":1}
  ;; (serde_json Map keys sort), so with a null data_row / empty env /
  ;; single-digit N the digit is always at p+38 — the test's own byte probes
  ;; pin that layout, so a host format change fails loudly here, not silently.
  ;; NOTE: deliberately unrolled with NO loop / br_if / br — this engine
  ;; miscompiles br_if-conditional loops at runtime (a counting loop returns
  ;; after ONE iteration; probes proved if-with-return is fine), which is what
  ;; made the scanning version of this function fail.
  (func $iter_digit (param $p i32) (result i32)
    (if (i32.and (i32.and (i32.and (i32.and
          (i32.eq (i32.load (i32.add (local.get $p) (i32.const 27))) (i32.const 0x72657469))
          (i32.eq (i32.load (i32.add (local.get $p) (i32.const 31))) (i32.const 0x6F697461)))
          (i32.eq (i32.load8_u (i32.add (local.get $p) (i32.const 35))) (i32.const 110)))
          (i32.eq (i32.load8_u (i32.add (local.get $p) (i32.const 36))) (i32.const 34)))
          (i32.eq (i32.load8_u (i32.add (local.get $p) (i32.const 37))) (i32.const 58)))
      (then (return (i32.load8_u (i32.add (local.get $p) (i32.const 38)))))
      (else (return (i32.const 0))))
    ;; Trailing value required: this engine fails to compile a function whose
    ;; body is a statement-if with no value after it (function[3] error).
    (i32.const 0))
  (func (export "adapter_run_iteration") (param $in i32) (param $in_len i32) (result i32)
    (if (i32.eq (call $iter_digit (local.get $in)) (i32.const 48))
      (then
        ;; Iteration 0 = slow iteration: grow memory and scribble the heap,
        ;; then trap hard via `unreachable` — a GUARANTEED runtime trap (the
        ;; spin-loop fuel variant depends on `br`, also unreliable in this
        ;; engine). The linear memory is left half-mutated: a store that is
        ;; NOT reset would report 65 pages on the next iteration.
        (drop (memory.grow (i32.const 1)))
        (drop (call $malloc (i32.const 70000)))
        (call $metric_add (i32.const 8210) (i32.const 16)
          (f64.convert_i32_u (memory.size))
          (i32.const 8300) (i32.const 2) (i32.const 1))
        (unreachable))
      (else
        ;; Healthy iteration: on a FRESH store (post-reset) memory.size is
        ;; back to its initial 64 pages; on a reused store it stayed at 65.
        (call $metric_add (i32.const 8600) (i32.const 5)
          (f64.convert_i32_u (call $iter_digit (local.get $in)))
          (i32.const 8300) (i32.const 2) (i32.const 1))
        (call $metric_add
          (i32.const 8192) (i32.const 9)
          (f64.convert_i32_u (memory.size))
          (i32.const 8300) (i32.const 2) (i32.const 1))
        (return (i32.const 0))))
    (i32.const 0))
)
"#;

    #[tokio::test]
    async fn test_trap_resets_store_for_next_iteration() {
        // P1 regression: a guest trap (the test module deliberately traps
        // via `unreachable` after mutating its heap) left the Store in place
        // and the next
        // iteration reused the half-mutated linear memory, with free() called
        // on that poisoned heap. The instance must now re-instantiate into a
        // fresh store: the trapped iteration surfaces the trap and emits NO
        // free marker, and the next iteration runs on pristine memory.
        let driver = WasmDriver;
        let mut inst = driver
            .init(TRAP_RESET_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        ctx.http_client = Some(Arc::new(StubClient));

        // Iteration 0: the module grows memory, scribbles the heap, emits a
        // pages_after_grow=65 sample, then traps via `unreachable`.
        let err = match inst.run_iteration(&mut ctx).await {
            Err(e) => e,
            Ok(()) => panic!(
                "iteration 0 must trap (input layout at fixed offsets p+27..p+38 may have drifted)"
            ),
        };
        assert!(
            format!("{}", err).contains("WASM driver error"),
            "trap must surface through wasm_err, got: {}",
            err
        );

        // The grow must have really happened (the trap path drains samples
        // before resetting) — otherwise the 64-pages assertion on iteration 1
        // would pass vacuously even on a reused store.
        let grown = ctx
            .samples
            .iter()
            .find(|s| s.metric == "pages_after_grow")
            .expect("pages_after_grow sample from the trapped iteration");
        assert_eq!(grown.value, 65.0, "iteration 0 must have grown the memory");

        // The host must NOT free the input buffer into a trapped heap — the
        // marker emitted from `free` must be absent.
        let names: Vec<&str> = ctx.samples.iter().map(|s| s.metric.as_ref()).collect();
        assert!(
            !names.contains(&"free_called"),
            "free must never run on a trapped heap: {:?}",
            names
        );
        ctx.samples.clear();

        // Iteration 1 (healthy): the instance must have been reset — the
        // store is fresh, so the linear memory is back at its initial 64
        // pages, the module reads iteration digit '1' (49), and reports
        // mem_pages = 64 (a reused store would report 65).
        ctx.iteration = 1;
        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration 1 must succeed on the reset store");
        let digit = ctx
            .samples
            .iter()
            .find(|s| s.metric == "digit")
            .expect("digit sample");
        assert_eq!(digit.value, 49.0, "iteration 1 digit must be '1'");
        let mem = ctx
            .samples
            .iter()
            .find(|s| s.metric == "mem_pages")
            .expect("mem_pages sample");
        assert_eq!(
            mem.value, 64.0,
            "store must be reset after a trap (memory back to initial size)"
        );
    }

    #[tokio::test]
    async fn test_run_iteration_http_and_metric() {
        let driver = WasmDriver;
        let mut inst = driver
            .init(DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        ctx.http_client = Some(Arc::new(StubClient));

        inst.run_iteration(&mut ctx)
            .await
            .expect("iteration must succeed");

        let names: Vec<&str> = ctx.samples.iter().map(|s| s.metric.as_ref()).collect();
        assert!(
            names.contains(&"http_req_duration"),
            "http_req_duration missing: {:?}",
            names
        );
        assert!(
            names.contains(&"driver_ok"),
            "driver_ok missing: {:?}",
            names
        );
        let driver_ok = ctx
            .samples
            .iter()
            .find(|s| s.metric == "driver_ok")
            .expect("driver_ok sample");
        assert_eq!(driver_ok.value, 1.0);

        // The standard http samples carry status/url/method tags.
        let dur = ctx
            .samples
            .iter()
            .find(|s| s.metric == "http_req_duration")
            .unwrap();
        assert_eq!(dur.tags.get("status"), Some("200"));
        assert_eq!(dur.tags.get("url"), Some("http://example.com/"));

        // The custom metric respects its type code (1 = Counter).
        let driver_ok = ctx
            .samples
            .iter()
            .find(|s| s.metric == "driver_ok")
            .unwrap();
        assert_eq!(
            driver_ok.sample_type,
            tropel_core::types::SampleType::Counter
        );
    }

    #[tokio::test]
    async fn test_infinite_loop_traps_via_fuel() {
        // An infinite adapter_run_iteration must be interrupted by the fuel
        // budget rather than hang the VU.
        let driver = WasmDriver;
        let mut inst = driver
            .init(LOOP_DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        let start = std::time::Instant::now();
        let result = inst.run_iteration(&mut ctx).await;
        assert!(result.is_err(), "infinite loop must trap, got {:?}", result);
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "infinite loop must trap quickly"
        );
    }

    #[tokio::test]
    async fn test_sleep_over_budget_traps_quickly() {
        // P0 regression: `sleep(1e300)` saturated to u64::MAX ms (584 M
        // years) and hung the run — fuel doesn't tick during host calls, so
        // the instruction budget never applied. A sleep that would exceed the
        // per-iteration budget must trap IMMEDIATELY (no actual sleeping).
        let driver = WasmDriver;
        let mut inst = driver
            .init(SLEEP_DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        let start = std::time::Instant::now();
        let result = inst.run_iteration(&mut ctx).await;
        assert!(
            result.is_err(),
            "over-budget sleep must trap, got {:?}",
            result
        );
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "over-budget sleep must trap without blocking (took {:?})",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn test_sleep_within_budget_succeeds() {
        // A legitimate small sleep must still work.
        let driver = WasmDriver;
        let mut inst = driver
            .init(SLEEP_OK_DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        let start = std::time::Instant::now();
        inst.run_iteration(&mut ctx)
            .await
            .expect("1ms sleep within budget must succeed");
        assert!(
            start.elapsed() >= Duration::from_millis(1),
            "sleep should have actually blocked ~1ms"
        );
    }

    #[tokio::test]
    async fn test_metric_spam_capped() {
        // P1 regression: a hostile module could call metric_add ~50 M times
        // per iteration (fuel-bounded), growing state.samples toward
        // multi-GB and flooding the metrics pipeline. The per-iteration
        // sample cap must bound the buffer AND fail the iteration.
        let driver = WasmDriver;
        let mut inst = driver
            .init(SPAM_DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        let start = std::time::Instant::now();
        let result = inst.run_iteration(&mut ctx).await;
        assert!(
            result.is_err(),
            "sample spam must fail the iteration, got {:?}",
            result
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "spam cap must trip quickly"
        );
        // The buffer is bounded: at most MAX_ITERATION_SAMPLES samples were
        // drained into ctx.samples (the 100002nd call sets the flag).
        assert!(
            ctx.samples.len() <= MAX_ITERATION_SAMPLES,
            "samples must be capped, got {}",
            ctx.samples.len()
        );
    }

    #[tokio::test]
    async fn test_tag_bytes_budget_capped() {
        // P1 regression: the sample-count cap alone does not bound memory
        // when each sample carries up to MAX_METRIC_TAGS_BYTES of tags —
        // 100k capped samples x 64 KiB would be ~6.4 GB per VU. The
        // cumulative per-iteration tag-bytes budget must trip (~2048 samples
        // of 4 KiB) and fail the iteration.
        let driver = WasmDriver;
        let mut inst = driver
            .init(tag_spam_driver_wat().as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        let start = std::time::Instant::now();
        let result = inst.run_iteration(&mut ctx).await;
        assert!(
            result.is_err(),
            "tag-bytes spam must fail the iteration, got {:?}",
            result
        );
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "tag-bytes cap must trip quickly"
        );
        // ~2047 samples of ~4 KiB tags fit in the 8 MiB budget; anything more
        // is refused. The count never reaches the 100k sample cap.
        assert!(
            ctx.samples.len() < MAX_ITERATION_SAMPLES,
            "tag budget should trip before the sample cap, got {}",
            ctx.samples.len()
        );
    }

    #[tokio::test]
    async fn test_hostile_guest_length_does_not_abort() {
        // P1 regression: `req_len = i32::MAX` used to zero-allocate ~2 GiB
        // (vec![0u8; guest_len]) BEFORE any bounds check, hitting
        // handle_alloc_error → abort() (non-unwinding — kills every VU and
        // discards metrics). The host must clamp to the module's memory size
        // (engine-capped at 16 MiB), fail the read, and return a negative
        // error code instead.
        let driver = WasmDriver;
        let mut inst = driver
            .init(HOSTILE_LEN_DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        ctx.http_client = Some(Arc::new(StubClient));
        let start = std::time::Instant::now();
        inst.run_iteration(&mut ctx)
            .await
            .expect("hostile length must be rejected without aborting");
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "clamped read must complete quickly"
        );
    }

    #[test]
    fn test_timeout_ms_clamped() {
        // P0 regression: `timeout_ms: 1e300` saturated to u64::MAX and
        // replaced the client's default request timeout, parking the caller
        // on rx.recv() with no bound. The clamp caps it at MAX_HOST_CALL_MS.
        let req = WasmHttpRequest {
            url: "http://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            timeout_ms: Some(1e300),
            follow_redirects: true,
        }
        .into_request()
        .unwrap();
        assert_eq!(
            req.timeout,
            Some(Duration::from_millis(MAX_HOST_CALL_MS as u64))
        );

        // Sub-ceiling values pass through unchanged.
        let req = WasmHttpRequest {
            url: "http://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            timeout_ms: Some(2500.0),
            follow_redirects: true,
        }
        .into_request()
        .unwrap();
        assert_eq!(req.timeout, Some(Duration::from_millis(2500)));

        // Non-positive → falls back to the client's default request timeout
        // (bounded) instead of an instant-fail zero or an unbounded wait.
        let req = WasmHttpRequest {
            url: "http://example.com".into(),
            method: "GET".into(),
            headers: HashMap::new(),
            body: None,
            timeout_ms: Some(-5.0),
            follow_redirects: true,
        }
        .into_request()
        .unwrap();
        assert_eq!(req.timeout, None);
    }

    #[test]
    fn test_into_request_rejects_invalid_method() {
        // Backlog line 95: a genuinely invalid method token must fail loudly,
        // not silently become GET. Empty, whitespace-inside and non-tchar
        // tokens are rejected; valid-but-uncommon tokens (PURGE/LINK/…) parse
        // as Method::Custom.
        for bad in ["", " ", "GE T", "GE\nT", "POTS,", "{GET}"] {
            let req = WasmHttpRequest {
                url: "http://example.com".into(),
                method: bad.into(),
                headers: HashMap::new(),
                body: None,
                timeout_ms: None,
                follow_redirects: true,
            };
            assert!(
                req.into_request().is_err(),
                "method {:?} must be rejected, not silently become GET",
                bad
            );
        }

        // Valid-but-uncommon tokens survive as Custom (write path preserved).
        let req = WasmHttpRequest {
            url: "http://example.com".into(),
            method: "PURGE".into(),
            headers: HashMap::new(),
            body: None,
            timeout_ms: None,
            follow_redirects: true,
        }
        .into_request()
        .unwrap();
        assert_eq!(req.method, Method::Custom("PURGE".into()));
    }
}
