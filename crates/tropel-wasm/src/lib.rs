//! # tropel-wasm — WASM plugin runtime for Tropel
//!
//! Tier 2 WASM plugin mechanism: sandboxed, portable input adapters
//! compiled to WebAssembly. Uses wasmtime with a simple C ABI interface
//! (no Component Model WIT complexity).
//!
//! ## WASM plugin ABI
//!
//! A WASM plugin module must export the following functions:
//!
//! ```wasm
//! ;; Return the adapter identifier string (written to a buffer).
//! ;; Allocates memory inside the WASM module's linear memory.
//! (func $adapter_id (export "adapter_id") (result i32))
//!   ;; Returns: pointer to a null-terminated UTF-8 string in WASM memory
//!
//! ;; Detect whether this adapter can handle the given bytes.
//! (func $adapter_detect (export "adapter_detect")
//!   (param $ptr i32) (param $len i32) (result i32))
//!   ;; ptr: pointer to bytes in WASM memory
//!   ;; len: number of bytes
//!   ;; Returns: 1 if the adapter claims the format, 0 otherwise
//!
//! ;; Parse the given bytes into a JSON Scenario.
//! (func $adapter_parse (export "adapter_parse")
//!   (param $in_ptr i32) (param $in_len i32)
//!   (param $out_ptr i32) (param $out_len i32) (result i32))
//!   ;; in_ptr: pointer to input bytes in WASM memory
//!   ;; in_len: number of input bytes
//!   ;; out_ptr: pointer to output buffer in WASM memory
//!   ;; out_len: maximum output buffer size
//!   ;; Returns: actual output length on success, 0 on failure
//!   ;; On success, writes JSON-encoded Scenario to out_ptr
//! ```

use std::collections::HashMap;
use std::path::Path;
use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_core::types::{AuthConfig, Body, Method, Request};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::InputAdapter;
use wasmtime::{Config, Engine, Memory, MemoryType, Module, Store, TypedFunc};

// ══════════════════════════════════════════════════════════════════
// WASM Engine — shared across all plugins
// ══════════════════════════════════════════════════════════════════

pub fn create_wasm_engine() -> std::result::Result<Engine, anyhow::Error> {
    let mut config = Config::new();
    config.max_wasm_stack(512 * 1024); // 512 KB stack per plugin
    Engine::new(&config)
}

static ENGINE: std::sync::OnceLock<Engine> = std::sync::OnceLock::new();

fn global_engine() -> &'static Engine {
    ENGINE.get_or_init(|| create_wasm_engine().expect("Failed to create wasmtime engine"))
}

// ══════════════════════════════════════════════════════════════════
// WasmPlugin — manages a single WASM module
// ══════════════════════════════════════════════════════════════════

pub struct WasmPlugin {
    plugin_id: String,
    module: Module,
}

impl WasmPlugin {
    /// Load a WASM module from raw bytes.
    pub fn load(wasm_bytes: &[u8]) -> std::result::Result<Self, anyhow::Error> {
        let engine = global_engine();
        let module = Module::new(engine, wasm_bytes)?;

        // Instantiate once to get the plugin ID
        let plugin_id = Self::with_instance(&module, |store, instance, _memory, _has_malloc| {
            let id_ptr: i32 = instance
                .get_typed_func::<(), i32>(&mut *store, "adapter_id")?
                .call(&mut *store, ())?;
            let id = read_wasm_string(&mut *store, instance, id_ptr);
            anyhow::Ok((id, ()))
        })?;

        Ok(Self { plugin_id, module })
    }

    /// Create a store + instance, run a closure, return the result.
    /// The WASM module must export `memory`. If it also exports `malloc`,
    /// allocation uses the module's own allocator (preferred). Otherwise,
    /// the host allocates from a fixed region starting at page 2.
    fn with_instance<T>(
        module: &Module,
        f: impl FnOnce(&mut Store<()>, &wasmtime::Instance, Memory, bool) -> anyhow::Result<(T, ())>,
    ) -> std::result::Result<T, anyhow::Error> {
        let engine = global_engine();
        let mut store = Store::new(engine, ());

        // Create linear memory: 64 pages (4 MB) initial, 256 pages (16 MB) max
        let memory_ty = MemoryType::new(64, Some(256));
        let memory = Memory::new(&mut store, memory_ty)?;

        let instance = wasmtime::Instance::new(&mut store, module, &[memory.into()])?;

        let mem = instance
            .get_memory(&mut store, "memory")
            .ok_or_else(|| anyhow::anyhow!("WASM module must export 'memory'"))?;

        // Check if the module exports a malloc function
        let has_malloc = instance.get_func(&mut store, "malloc").is_some();

        let (result, _) = f(&mut store, &instance, mem, has_malloc)?;
        Ok(result)
    }

    /// Allocate a buffer of the given size in WASM linear memory.
    /// Returns the pointer (i32) and a cleanup closure that calls free.
    /// Uses the module's `malloc`/`free` if available, otherwise falls
    /// back to a fixed offset (simplified, for tiny allocations only).
    fn alloc_wasm_memory(
        store: &mut Store<()>,
        instance: &wasmtime::Instance,
        memory: &Memory,
        size: usize,
        use_malloc: bool,
    ) -> anyhow::Result<(i32, Option<Box<dyn FnOnce(&mut Store<()>) -> anyhow::Result<()>>>)>
    {
        if use_malloc {
            let malloc_fn = instance.get_typed_func::<i32, i32>(&mut *store, "malloc")?;
            let ptr = malloc_fn.call(&mut *store, size as i32)?;

            // Build a cleanup closure that calls free
            let free_fn: TypedFunc<i32, ()> = instance.get_typed_func(&mut *store, "free")?;
            let cleanup: Box<dyn FnOnce(&mut Store<()>) -> anyhow::Result<()>> =
                Box::new(move |s| {
                    free_fn.call(s, ptr)?;
                    Ok(())
                });
            Ok((ptr, Some(cleanup)))
        } else {
            // Fallback: use fixed offset at page 2 (131072).
            // Documented limitation: the WASM module must not use pages 2-3
            // (offsets 131072–262143) for its own data.
            let ptr = 131072i32;
            let needed_end = (ptr as usize) + size;
            let current_bytes = memory.size(&*store) as usize * 65536;
            if needed_end > current_bytes {
                let pages_needed = (needed_end + 65535) / 65536;
                let grow = pages_needed - memory.size(&*store) as usize;
                memory.grow(&mut *store, grow as u64)?;
            }
            Ok((ptr, None))
        }
    }

    /// Get the plugin's identifier string.
    pub fn id(&self) -> &str {
        &self.plugin_id
    }

    /// Write bytes into WASM linear memory at an allocated region.
    /// If `cleanup` is provided, it will free the allocation when called.
    fn write_bytes(
        store: &mut Store<()>,
        memory: &Memory,
        instance: &wasmtime::Instance,
        bytes: &[u8],
        use_malloc: bool,
    ) -> anyhow::Result<(i32, Option<Box<dyn FnOnce(&mut Store<()>) -> anyhow::Result<()>>>)>
    {
        let (ptr, cleanup) =
            Self::alloc_wasm_memory(store, instance, memory, bytes.len(), use_malloc)?;
        memory.write(store, ptr as usize, bytes)?;
        Ok((ptr, cleanup))
    }

    /// Detect whether this plugin can handle the given bytes.
    pub fn detect(&self, bytes: &[u8]) -> bool {
        let module = &self.module;
        let result = Self::with_instance(module, |store, instance, memory, has_malloc| {
            let (ptr, _cleanup) =
                Self::write_bytes(&mut *store, &memory, &instance, bytes, has_malloc)?;

            let detect_fn =
                instance.get_typed_func::<(i32, i32), i32>(&mut *store, "adapter_detect")?;

            let result = detect_fn.call(&mut *store, (ptr, bytes.len() as i32))?;
            anyhow::Ok((result != 0, ()))
        });
        result.unwrap_or(false)
    }

    /// Parse the given bytes into a Scenario.
    pub fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let module = &self.module;
        let result = Self::with_instance(module, |store, instance, memory, has_malloc| {
            let (input_ptr, _in_cleanup) =
                Self::write_bytes(&mut *store, &memory, &instance, bytes, has_malloc)?;

            let parse_fn =
                instance
                    .get_typed_func::<(i32, i32, i32, i32), i32>(&mut *store, "adapter_parse")?;

            // Allocate output buffer in WASM memory (64 KB max output)
            let output_size = 65536i32;
            let (output_ptr, _out_cleanup) = Self::alloc_wasm_memory(
                &mut *store,
                &instance,
                &memory,
                output_size as usize,
                has_malloc,
            )?;

            let written =
                parse_fn.call(&mut *store, (input_ptr, bytes.len() as i32, output_ptr, output_size))?;

            if written <= 0 {
                anyhow::bail!("WASM adapter returned parse error (code: {})", written);
            }

            let json_str = read_wasm_buffer(&*store, &memory, output_ptr, written as u32);
            anyhow::Ok((json_str, ()))
        });

        match result {
            Ok(json_str) => {
                let wit_scenario: WasmScenario = serde_json::from_str(&json_str)
                    .map_err(|e| TropelError::Parse(format!("WASM adapter returned invalid JSON: {}", e)))?;
                convert_scenario(wit_scenario)
            }
            Err(e) => Err(TropelError::Parse(format!("WASM adapter error: {}", e))),
        }
    }
} // ← closes impl WasmPlugin

/// Read a null-terminated string from WASM memory at the given pointer.
fn read_wasm_string(store: &mut Store<()>, instance: &wasmtime::Instance, ptr: i32) -> String {
    let mut buf = vec![0u8; 1024];
    if let Some(memory) = instance.get_memory(&mut *store, "memory") {
        if memory.read(&*store, ptr as usize, &mut buf).is_ok() {
            let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
            String::from_utf8_lossy(&buf[..end]).to_string()
        } else {
            String::new()
        }
    } else {
        String::new()
    }
}

/// Read a buffer from WASM memory.
fn read_wasm_buffer(store: &Store<()>, memory: &Memory, ptr: i32, len: u32) -> String {
    if len == 0 {
        return String::new();
    }
    let mut buf = vec![0u8; len as usize];
    if memory.read(store, ptr as usize, &mut buf).is_ok() {
        String::from_utf8_lossy(&buf).to_string()
    } else {
        String::new()
    }
}

// ══════════════════════════════════════════════════════════════════
// WASM JSON Scenario (the on-the-wire format)
// ══════════════════════════════════════════════════════════════════

/// JSON-serializable Scenario that the WASM adapter produces.
/// Mirrors the Scenario structure but is WASM-friendly (no recursion).
#[derive(serde::Deserialize)]
struct WasmScenario {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    items: Vec<WasmItem>,
    #[serde(default)]
    variables: HashMap<String, String>,
    #[serde(default)]
    auth: Option<WasmAuth>,
}

#[derive(serde::Deserialize)]
struct WasmItem {
    id: String,
    name: String,
    #[serde(default)]
    request: Option<WasmRequest>,
    #[serde(default)]
    prerequest: Option<String>,
    #[serde(default)]
    test: Option<String>,
    #[serde(default)]
    assertions: Vec<String>,
    #[serde(default)]
    parent_index: i32,
    #[serde(default)]
    items: Vec<WasmItem>,
}

#[derive(serde::Deserialize)]
struct WasmRequest {
    url: String,
    #[serde(default)]
    method: String,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    query_params: HashMap<String, String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    body_type: String,
    #[serde(default)]
    auth: Option<WasmAuth>,
    #[serde(default = "return_true")]
    follow_redirects: bool,
    #[serde(default)]
    timeout_ms: Option<u64>,
}

#[derive(serde::Deserialize)]
struct WasmAuth {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    credentials: String,
}

fn return_true() -> bool {
    true
}

// ══════════════════════════════════════════════════════════════════
// JSON-to-Rust conversion
// ══════════════════════════════════════════════════════════════════

fn convert_scenario(ws: WasmScenario) -> Result<Scenario> {
    let items = build_item_tree(&ws.items);
    let variables: HashMap<String, serde_json::Value> = ws
        .variables
        .into_iter()
        .map(|(k, v)| (k, serde_json::Value::String(v)))
        .collect();

    Ok(Scenario {
        info: ScenarioInfo {
            name: ws.name,
            description: ws.description,
            schema: ws.schema,
        },
        items,
        variables,
        auth: ws.auth.as_ref().and_then(|a| convert_auth(a)),
    })
}

fn build_item_tree(flat: &[WasmItem]) -> Vec<ScenarioItem> {
    // Collect flat items first
    let items: Vec<ScenarioItem> = flat
        .iter()
        .map(|wi| ScenarioItem {
            id: wi.id.clone(),
            name: wi.name.clone(),
            request: wi.request.as_ref().map(convert_request),
            prerequest: wi.prerequest.clone(),
            test: wi.test.clone(),
            assertions: wi.assertions.clone(),
            items: Vec::new(),
        })
        .collect();

    // Build tree from parent_index references
    // For recursive items (self-contained), use the `items` field directly
    if flat.is_empty() {
        return Vec::new();
    }

    // If any item has child items directly (recursive format), use those
    let has_recursive = flat.iter().any(|wi| !wi.items.is_empty());
    if has_recursive {
        return flat
            .iter()
            .map(|wi| {
                let children = build_item_tree(&wi.items);
                ScenarioItem {
                    id: wi.id.clone(),
                    name: wi.name.clone(),
                    request: wi.request.as_ref().map(convert_request),
                    prerequest: wi.prerequest.clone(),
                    test: wi.test.clone(),
                    assertions: wi.assertions.clone(),
                    items: children,
                }
            })
            .collect();
    }

    // Otherwise, use parent-index flat format
    let mut result: Vec<ScenarioItem> = Vec::new();
    for (i, item) in items.into_iter().enumerate() {
        let pidx = flat[i].parent_index;
        if pidx < 0 {
            result.push(item);
        } else if let Some(parent) = result.get_mut(pidx as usize) {
            parent.items.push(item);
        } else {
            result.push(item);
        }
    }
    result
}

fn convert_request(wr: &WasmRequest) -> Request {
    let method = match wr.method.to_uppercase().as_str() {
        "GET" => Method::GET,
        "HEAD" => Method::HEAD,
        "POST" => Method::POST,
        "PUT" => Method::PUT,
        "DELETE" => Method::DELETE,
        "CONNECT" => Method::CONNECT,
        "OPTIONS" => Method::OPTIONS,
        "TRACE" => Method::TRACE,
        "PATCH" => Method::PATCH,
        _ => Method::GET,
    };

    let body = wr.body.as_ref().map(|b| match wr.body_type.as_str() {
        "json" => serde_json::from_str(b)
            .map(Body::Json)
            .unwrap_or_else(|_| Body::Raw(b.clone())),
        "form" => {
            let mut map = HashMap::new();
            for param in b.split('&') {
                if let Some(eq) = param.find('=') {
                    let k = &param[..eq];
                    let v = &param[eq + 1..];
                    map.insert(k.to_string(), v.to_string());
                }
            }
            Body::UrlEncoded(map)
        }
        _ => Body::Raw(b.clone()),
    });

    Request {
        url: wr.url.clone(),
        method,
        headers: wr.headers.clone(),
        query_params: wr.query_params.clone(),
        body,
        auth: wr.auth.as_ref().and_then(|a| convert_auth(a)),
        certificate: None,
        follow_redirects: wr.follow_redirects,
        timeout: wr.timeout_ms.map(std::time::Duration::from_millis),
    }
}

fn convert_auth(wa: &WasmAuth) -> Option<AuthConfig> {
    match wa.kind.as_str() {
        "bearer" => Some(AuthConfig::Bearer {
            token: wa.credentials.clone(),
        }),
        "basic" => {
            let parts: Vec<&str> = wa.credentials.splitn(2, ':').collect();
            Some(AuthConfig::Basic {
                username: parts.first().unwrap_or(&"").to_string(),
                password: parts.get(1).unwrap_or(&"").to_string(),
            })
        }
        "api-key" => {
            let parts: Vec<&str> = wa.credentials.splitn(2, ':').collect();
            Some(AuthConfig::ApiKey {
                key: parts.first().unwrap_or(&"").to_string(),
                value: parts.get(1).unwrap_or(&"").to_string(),
                location: tropel_core::types::ApiKeyLocation::Header,
            })
        }
        _ => None,
    }
}

// ══════════════════════════════════════════════════════════════════
// WasmInputAdapter — wraps a WasmPlugin as an InputAdapter
// ══════════════════════════════════════════════════════════════════

pub struct WasmInputAdapter {
    plugin: WasmPlugin,
}

impl WasmInputAdapter {
    pub fn new(wasm_bytes: &[u8]) -> Result<Self> {
        let plugin =
            WasmPlugin::load(wasm_bytes).map_err(|e| {
                TropelError::Other(format!("Failed to load WASM plugin: {}", e))
            })?;
        Ok(Self { plugin })
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let wasm_bytes = std::fs::read(path).map_err(TropelError::Io)?;
        Self::new(&wasm_bytes)
    }

    pub fn plugin_id(&self) -> &str {
        self.plugin.id()
    }
}

impl InputAdapter for WasmInputAdapter {
    fn id(&self) -> &str {
        self.plugin.id()
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        self.plugin.detect(bytes)
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        self.plugin.parse(bytes)
    }

    fn parse_with_path(&self, bytes: &[u8], _source_path: Option<&Path>) -> Result<Scenario> {
        self.plugin.parse(bytes)
    }
}

// ══════════════════════════════════════════════════════════════════
// Plugin discovery
// ══════════════════════════════════════════════════════════════════

pub fn discover_plugins(plugins_dir: &Path) -> Vec<WasmInputAdapter> {
    let mut adapters = Vec::new();
    let dir = match std::fs::read_dir(plugins_dir) {
        Ok(d) => d,
        Err(_) => return adapters,
    };

    for entry in dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        let wasm_bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(_) => continue,
        };
        match WasmInputAdapter::new(&wasm_bytes) {
            Ok(adapter) => {
                tracing::info!("Loaded WASM plugin '{}'", path.display());
                adapters.push(adapter);
            }
            Err(e) => {
                tracing::warn!("Failed to load WASM plugin '{}': {}", path.display(), e);
            }
        }
    }
    adapters
}

// ══════════════════════════════════════════════════════════════════
// Tests
// ══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_creation() {
        let engine = create_wasm_engine();
        assert!(engine.is_ok(), "wasmtime engine should create successfully");
    }

    #[test]
    fn test_discover_empty_dir() {
        let temp = tempfile::tempdir().unwrap();
        let adapters = discover_plugins(temp.path());
        assert!(
            adapters.is_empty(),
            "no WASM files should produce no adapters"
        );
    }

    #[test]
    fn test_json_deser() {
        let json = r#"{
            "name": "test",
            "items": [{
                "id": "r1",
                "name": "GET /",
                "request": {
                    "url": "https://example.com",
                    "method": "GET"
                }
            }]
        }"#;
        let ws: WasmScenario = serde_json::from_str(json).unwrap();
        assert_eq!(ws.name, "test");
        assert_eq!(ws.items.len(), 1);
        assert_eq!(
            ws.items[0].request.as_ref().unwrap().url,
            "https://example.com"
        );
    }

    #[test]
    fn test_convert_scenario() {
        let json = r#"{
            "name": "test-api",
            "items": [
                {"id": "r1", "name": "GET /", "request": {"url": "https://example.com", "method": "GET"}}
            ]
        }"#;
        let ws: WasmScenario = serde_json::from_str(json).unwrap();
        let scenario = convert_scenario(ws).unwrap();
        assert_eq!(scenario.info.name, "test-api");
        assert_eq!(scenario.items.len(), 1);
    }
}
