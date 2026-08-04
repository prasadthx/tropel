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

use crate::{wasm_engine, DEFAULT_CALL_FUEL, FALLBACK_BASE, load_module_aot};
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
            Module::new(wasm_engine(), bytes).map_err(|e| {
                TropelError::Other(format!("WASM driver module is invalid: {}", e))
            })?
        };

        // Must be an imperative driver module.
        if !module.exports().any(|e| e.name() == "adapter_run_iteration") {
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
            .func_wrap("env", "sleep", |_: Caller<'_, WasmDriverState>, ms: f64| {
                std::thread::sleep(Duration::from_millis(ms.max(0.0) as u64));
            })
            .map_err(wasm_err)?;
        linker
            .func_wrap("env", "metric_add", metric_add_host)
            .map_err(wasm_err)?;
        // Any other imports (WASI etc.) become traps — WASI-less capabilities.
        linker.define_unknown_imports_as_traps(&module).map_err(wasm_err)?;

        let instance = linker
            .instantiate(&mut store, &module)
            .map_err(|e| TropelError::Other(format!("WASM driver instantiation failed: {}", e)))?;

        let memory = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| {
                TropelError::Other(
                    "WASM driver module must export a linear 'memory' (cdylib pattern)".into(),
                )
            })?;

        let run_iteration = instance
            .get_typed_func::<(i32, i32), i32>(&mut store, "adapter_run_iteration")
            .map_err(wasm_err)?;

        let malloc_fn = match instance.get_typed_func::<i32, i32>(&mut store, "malloc") {
            Ok(f) => Some(f),
            Err(_) => None,
        };
        let free_fn = match instance.get_typed_func::<i32, i32>(&mut store, "free") {
            Ok(f) => Some(f),
            Err(_) => None,
        };

        Ok(Box::new(WasmDriverInstance {
            store,
            run_iteration,
            memory,
            call_fuel: DEFAULT_CALL_FUEL,
            malloc_fn,
            free_fn,
        }))
    }
}

fn wasm_err(e: impl std::fmt::Display) -> TropelError {
    TropelError::Other(format!("WASM driver error: {}", e))
}

// ══════════════════════════════════════════════════════════════════
// WasmDriverState — the per-store data host functions reach via Caller
// ══════════════════════════════════════════════════════════════════

pub struct WasmDriverState {
    pub http_client: Option<Arc<dyn DriverHttpClient + Send + Sync>>,
    pub samples: Vec<Sample>,
}

impl Default for WasmDriverState {
    fn default() -> Self {
        Self {
            http_client: None,
            samples: Vec::new(),
        }
    }
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
    free_fn: Option<TypedFunc<i32, i32>>,
}

#[async_trait]
impl DriverInstance for WasmDriverInstance {
    async fn run_iteration(&mut self, ctx: &mut VuContext) -> Result<()> {
        // Per-iteration state: fresh client handle + fresh sample buffer.
        {
            let state = self.store.data_mut();
            state.http_client = ctx.http_client.clone();
            state.samples.clear();
        }

        // Reset the per-call instruction budget (fuel is consumed per call;
        // set_fuel replaces, so each iteration gets a fresh DoS budget).
        self.store
            .set_fuel(self.call_fuel)
            .map_err(wasm_err)?;

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
        // request failures too). Free the input buffer only when it was
        // allocated through the module's own malloc — never hand FALLBACK_BASE
        // (a host-owned bump region) to the module's free.
        let call_result = self
            .run_iteration
            .call(&mut self.store, (ptr as i32, input_bytes.len() as i32));

        if used_malloc {
            if let Some(free) = &self.free_fn {
                let _ = free.call(&mut self.store, ptr as i32);
            }
        }

        // Drain samples collected by host functions, unconditionally.
        let state = self.store.data_mut();
        ctx.samples.append(&mut state.samples);

        let ret = call_result.map_err(wasm_err)?;
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
    fn into_request(self) -> Request {
        let req_body = self.body.filter(|b| !b.is_empty()).map(Body::Raw);
        Request {
            url: self.url,
            method: Method::parse(&self.method).unwrap_or(Method::GET),
            headers: self.headers,
            query_params: HashMap::new(),
            body: req_body,
            auth: None,
            certificate: None,
            follow_redirects: self.follow_redirects,
            timeout: self.timeout_ms.map(|ms| Duration::from_millis(ms as u64)),
            response_type: tropel_core::types::ResponseType::Text,
        }
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
    let mut req_buf = vec![0u8; req_len.max(0) as usize];
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

    let req = request.into_request();
    // Clone the request into the 'static future for the I/O runtime; the
    // original stays alive for sample-tag construction below.
    let req_for_io = req.clone();
    let client_for_io = http_client.clone();
    let result = tropel_http::blocking::execute_blocking(async move {
        client_for_io.execute(&req_for_io).await
    });
    let resp = match result {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("WASM driver http_request failed: {}", e);
            return -5;
        }
    };

    // Record standard samples (mirrors the declarative runner's tags).
    {
        let now = SystemTime::now();
        let mut tags = TagMap::with_capacity(5);
        tags.insert("url", req.url.clone());
        tags.insert("method", req.method.to_string());
        tags.insert("status", resp.status_code.to_string());
        tags.insert("name", req.url.clone());
        tags.insert("group", "http");
        let tags = Arc::new(tags);

        let data_sent = req
            .body
            .as_ref()
            .map(Body::encoded_len)
            .unwrap_or(0) as f64;

        let samples = &mut caller.data_mut().samples;
        samples.push(Sample {
            metric: "http_req_duration".into(),
            value: resp.response_time.as_micros() as f64,
            tags: tags.clone(),
            timestamp: now,
            sample_type: SampleType::Trend,
        });
        samples.push(Sample {
            metric: "http_reqs".into(),
            value: 1.0,
            tags: tags.clone(),
            timestamp: now,
            sample_type: SampleType::Counter,
        });
        // http_req_failed: k6's default semantics (2xx-3xx = success). The
        // declarative runner instead consults the configurable
        // expectedStatuses; the WASM driver has no config channel, so it
        // deliberately matches the k6 default.
        let is_failed = !(200..400).contains(&resp.status_code);
        samples.push(Sample {
            metric: "http_req_failed".into(),
            value: if is_failed { 1.0 } else { 0.0 },
            tags: tags.clone(),
            timestamp: now,
            sample_type: SampleType::Rate,
        });
        samples.push(Sample {
            metric: "data_received".into(),
            value: resp.size as f64,
            tags: tags.clone(),
            timestamp: now,
            sample_type: SampleType::Counter,
        });
        samples.push(Sample {
            metric: "data_sent".into(),
            value: data_sent,
            tags,
            timestamp: now,
            sample_type: SampleType::Counter,
        });
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
    if memory.write(&mut caller, resp_ptr.max(0) as usize, &bytes).is_err() {
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
    let memory = match caller.get_export("memory") {
        Some(Extern::Memory(m)) => m,
        _ => return,
    };
    let name = read_mem_string(&memory, &caller, name_ptr, name_len);
    if name.is_empty() {
        return;
    }
    let mut tags_buf = vec![0u8; tags_len.max(0) as usize];
    let mut tags = TagMap::new();
    if memory
        .read(&caller, tags_ptr.max(0) as usize, &mut tags_buf)
        .is_ok()
    {
        if let Ok(map) = serde_json::from_slice::<HashMap<String, String>>(&tags_buf) {
            for (k, v) in map {
                tags.insert(k, v);
            }
        }
    }
    let sample_type = match type_code {
        1 => SampleType::Counter,
        2 => SampleType::Trend,
        3 => SampleType::Rate,
        _ => SampleType::Point,
    };
    caller.data_mut().samples.push(Sample {
        metric: name.into(),
        value,
        tags: Arc::new(tags),
        timestamp: SystemTime::now(),
        sample_type,
    });
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
    let mut buf = vec![0u8; len as usize];
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

    struct StubClient;

    #[async_trait]
    impl DriverHttpClient for StubClient {
        async fn execute(&self, _req: &Request) -> Result<Response> {
            Ok(Response {
                status_code: 200,
                status_text: "OK".into(),
                headers: HashMap::new(),
                body: b"hello".to_vec(),
                response_time: Duration::from_millis(5),
                timings: None,
                cookies: vec![],
                size: 5,
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
    async fn test_run_iteration_http_and_metric() {
        let driver = WasmDriver;
        let mut inst = driver
            .init(DRIVER_WAT.as_bytes(), None, None)
            .await
            .expect("driver init must succeed");

        let mut ctx = VuContext::new(1, 0, "default".into());
        ctx.http_client = Some(Arc::new(StubClient));

        inst.run_iteration(&mut ctx).await.expect("iteration must succeed");

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
        assert_eq!(driver_ok.sample_type, tropel_core::types::SampleType::Counter);
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
}
