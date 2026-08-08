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
//!
//! ## Engine hardening (per C3 / TROPEL_ARCH_REVIEW)
//!
//! - **AOT**: modules are precompiled to `.cwasm` (`Engine::precompile_module`)
//!   and cached next to the `.wasm`; `Module::deserialize` skips JIT on load.
//! - **Pooling allocator**: `PoolingAllocationConfig` reuses memory/tables/
//!   stacks across instances — cheap per-call `Store`/`Instance` creation.
//! - **Fuel interruption**: `Config::consume_fuel` + per-call `Store::set_fuel`
//!   gives every call a bounded instruction budget, so an infinite WASM loop
//!   traps with `Trap::OutOfFuel` instead of hanging the host (DoS guard).
//!   Fuel is used rather than epoch interruption because epoch traps crash
//!   with a non-unwinding panic on Windows (wasmtime SEH fragility). (The
//!   trap-unwinding mechanism was rewritten in wasmtime 47 — a dedicated
//!   unwinder crate replaces the old longjmp path — and the Windows abort is
//!   gone; the infinite-loop test traps cleanly with `Trap::OutOfFuel`.)
//! - **`InstancePre`**: import-free modules are pre-linked once and
//!   instantiated cheaply per call.
//! - **Load paths**: modules that *import* a memory get a host-supplied one;
//!   modules that *export* a memory (typical `wasm32` cdylib) use it. Any other
//!   imports become traps (WASI-less capabilities).
//! - **Distinct I/O regions**: input and output buffers never alias (regression:
//!   both used to land at the same fixed offset).

//! ## Imperative driver
//!
//! Modules that export `adapter_run_iteration` (plus a `memory` export) can
//! also be run as an imperative **driver** — the engine calls the export once
//! per VU iteration with a JSON iteration context, and the module drives HTTP
//! / sleep / custom metrics through host imports (`env.http_request`,
//! `env.sleep`, `env.metric_add`). See [`driver`] for the ABI.

pub mod driver;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_core::types::{AuthConfig, Body, Method, Request};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::InputAdapter;
use wasmtime::{
    Config, Engine, ExternType, Instance, InstanceAllocationStrategy, InstancePre, Linker, Memory,
    MemoryType, Module, PoolingAllocationConfig, Store,
};

// ══════════════════════════════════════════════════════════════════
// Engine — shared across all plugins
// ══════════════════════════════════════════════════════════════════

/// Default per-call WASM instruction budget (fuel units, 1 unit ≈ 1
/// instruction). Generous enough for any real parse/iteration; an infinite
/// loop burns through it in well under a second.
pub(crate) const DEFAULT_CALL_FUEL: u64 = 500_000_000;
/// Maximum output buffer we hand to a plugin's `adapter_parse` (4 MiB).
pub(crate) const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
/// Engine-wide maximum linear-memory size (256 pages = 16 MiB), matching the
/// imported-memory clamp in [`clamp_memory_type`]. Enforced via
/// `PoolingAllocationConfig::max_memory_size`, this applies to **exported**
/// memories too: a module whose declared minimum exceeds it fails to
/// instantiate, and any `memory.grow` beyond it fails at runtime — closing
/// the gap where a cdylib-exported memory could previously grow toward 4 GiB.
pub(crate) const MAX_MEMORY_BYTES: usize = 256 * 65536;
/// Fallback allocation region base (page 2). Only used when the module does
/// not export `malloc`/`free`. Input and output are bump-allocated *after*
/// this base so they never alias.
pub(crate) const FALLBACK_BASE: usize = 131072;

pub fn create_wasm_engine() -> std::result::Result<Engine, anyhow::Error> {
    let mut config = Config::new();
    config.max_wasm_stack(512 * 1024); // 512 KB stack per plugin

    // DoS guard: fuel metering gives every call a bounded instruction budget.
    // An infinite WASM loop traps with Trap::OutOfFuel instead of hanging the
    // host. (Epoch interruption was considered but its trap handler aborts the
    // process with a non-unwinding panic on Windows.)
    config.consume_fuel(true);

    // Pooling allocator (per C3): reuse memory/table/stack slots across
    // instances. Cheap Store/Instance creation per call.
    // (total_stacks is async-gated in wasmtime, so it stays at its default.)
    //
    // Pool sizing matters for the DRIVER path: every WASM driver VU holds a
    // live Store/Instance for the WHOLE test, so a small pool silently caps
    // the VU count. The old config kept wasmtime's default 4 GiB
    // `memory_reservation` per slot, so 16 slots already reserved ~64 GiB of
    // *virtual* address space — and `--vus 500` silently ran 16 VUs (VU #17
    // failed to instantiate; the engine swallowed the error and the summary
    // reported the requested count).
    //
    // Fix: shrink the per-slot reservation to the 16 MiB memory cap (and the
    // guard to 64 KiB) so the pool holds 4096 concurrent instances at the
    // SAME ~64 GiB of virtual address space. Runs that exhaust the pool now
    // fail LOUDLY in the engine instead of silently truncating the VU count.
    config.memory_reservation(MAX_MEMORY_BYTES as u64);
    config.memory_guard_size(64 * 1024);

    let mut pooling = PoolingAllocationConfig::default();
    pooling.total_memories(4096).total_tables(4096);
    // Cap linear memory to 16 MiB for ALL instances — imported AND exported
    // memories alike (memory_pages was removed in wasmtime 47; max_memory_size
    // is the modern engine-level ceiling and it covers exported memories). A
    // module declaring a min above the cap fails to instantiate; memory.grow
    // beyond the cap fails at runtime. This closes the exported-memory DoS
    // gap that the 256-page clamp on imports alone could not.
    pooling.max_memory_size(MAX_MEMORY_BYTES);
    config.allocation_strategy(InstanceAllocationStrategy::Pooling(pooling));

    // `wasmtime::Result` is not `anyhow::Result`; convert explicitly so the
    // caller gets a uniform `anyhow::Error`.
    Ok(Engine::new(&config)?)
}

static ENGINE: std::sync::OnceLock<Engine> = std::sync::OnceLock::new();

fn global_engine() -> &'static Engine {
    ENGINE.get_or_init(|| create_wasm_engine().expect("Failed to create wasmtime engine"))
}

/// Shared wasmtime engine accessor for sibling modules (driver.rs).
pub(crate) fn wasm_engine() -> &'static Engine {
    global_engine()
}

// ══════════════════════════════════════════════════════════════════
// Link strategy — how a module gets its memory
// ══════════════════════════════════════════════════════════════════

#[derive(Clone)]
enum LinkStrategy {
    /// Module exports its own memory and has no imports (typical cdylib).
    /// Pre-linked once; instantiated per call.
    PreLinked(InstancePre<()>),
    /// Module imports a memory — the host must supply one per call.
    MemoryImport {
        module: String,
        name: String,
        mem_type: MemoryType,
    },
}

/// Build the link strategy for a module.
///
/// Load-path fix (review bug a): a normal `wasm32` cdylib *exports* its memory
/// and declares zero imports; the old code unconditionally passed a host memory
/// as the sole import *and* required an exported `memory`, so such modules
/// failed to instantiate. Now we only supply a memory when the module actually
/// imports one; otherwise we rely on its exported memory.
fn build_link_strategy(engine: &Engine, module: &Module) -> anyhow::Result<LinkStrategy> {
    for import in module.imports() {
        if let ExternType::Memory(mem_ty) = import.ty() {
            let mt = clamp_memory_type(mem_ty);
            return Ok(LinkStrategy::MemoryImport {
                module: import.module().to_string(),
                name: import.name().to_string(),
                mem_type: mt,
            });
        }
    }

    // No memory import: the module must export its own memory. Pre-link once;
    // any other imports (e.g. WASI) become traps — WASI-less capabilities.
    let mut linker = Linker::new(engine);
    linker.define_unknown_imports_as_traps(module)?;
    let pre = linker.instantiate_pre(module)?;
    Ok(LinkStrategy::PreLinked(pre))
}

/// Clamp an *imported* memory type to a sane 256-page (16 MiB) ceiling so
/// `Memory::new` succeeds regardless of the module's declared maximum, and
/// so a plugin cannot grow a host-supplied memory unboundedly. Both the
/// minimum and maximum are clamped: a module importing `(memory 300 300)`
/// must not produce an invalid `min > max` MemoryType.
///
/// Note: modules that *export* their own memory (the typical `wasm32`
/// cdylib) are not clamped *here* — but they are still bounded at runtime by
/// the engine-level `MAX_MEMORY_BYTES` ceiling (see [`create_wasm_engine`]):
/// a declared minimum above the cap fails to instantiate and any
/// `memory.grow` past it fails. So exported memories are capped engine-wide;
/// this clamp only normalizes the host-supplied memory type for imports.
pub(crate) fn clamp_memory_type(mem_ty: MemoryType) -> MemoryType {
    let max = mem_ty.maximum().map(|m| m.min(256) as u32).unwrap_or(256);
    let min = (mem_ty.minimum() as u32).min(max);
    MemoryType::new(min, Some(max))
}

// ══════════════════════════════════════════════════════════════════
// WasmPlugin — manages a single WASM module
// ══════════════════════════════════════════════════════════════════

#[derive(Clone)]
pub struct WasmPlugin {
    plugin_id: String,
    module: Module,
    link_strategy: LinkStrategy,
    call_fuel: u64,
}

impl WasmPlugin {
    /// Load a WASM module from raw bytes (binary or WAT text).
    pub fn load(wasm_bytes: &[u8]) -> std::result::Result<Self, anyhow::Error> {
        let engine = global_engine();
        let module = Module::new(engine, wasm_bytes)?;
        Self::from_module(module)
    }

    /// Load from a compiled module with a custom per-call fuel budget.
    fn from_module(module: Module) -> std::result::Result<Self, anyhow::Error> {
        let engine = global_engine();
        let link_strategy = build_link_strategy(engine, &module)?;
        let mut plugin = Self {
            plugin_id: String::new(),
            module,
            link_strategy,
            call_fuel: DEFAULT_CALL_FUEL,
        };
        plugin.plugin_id = plugin.read_adapter_id()?;
        Ok(plugin)
    }

    /// Set the per-call WASM instruction budget (fuel units). A plugin that
    /// exceeds it traps with `Trap::OutOfFuel` instead of hanging the host.
    pub fn with_call_fuel(mut self, fuel: u64) -> Self {
        self.call_fuel = fuel;
        self
    }

    /// Load from a `.wasm` file, AOT-compiling to a `.cwasm` cache next to it.
    /// The cache is reused on subsequent loads (no JIT) and is invalidated
    /// when the source `.wasm` is newer than the cache (mtime check).
    pub fn from_file(path: &Path) -> std::result::Result<Self, anyhow::Error> {
        let module = load_module_aot(path)?;
        Self::from_module(module)
    }

    /// Create a store + instance, run a closure, return the result.
    fn with_instance<T>(
        &self,
        f: impl FnOnce(&mut Store<()>, &Instance, Memory, bool) -> anyhow::Result<T>,
    ) -> std::result::Result<T, anyhow::Error> {
        let engine = global_engine();
        let mut store = Store::new(engine, ());

        // Per-call instruction budget: an infinite loop traps with
        // Trap::OutOfFuel once `call_fuel` is consumed.
        store.set_fuel(self.call_fuel)?;

        let (instance, memory) = match &self.link_strategy {
            LinkStrategy::PreLinked(pre) => {
                let instance = pre.instantiate(&mut store)?;
                let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
                    anyhow::anyhow!("WASM module must export a 'memory' (or import one)")
                })?;
                (instance, memory)
            }
            LinkStrategy::MemoryImport {
                module,
                name,
                mem_type,
            } => {
                let memory = Memory::new(&mut store, mem_type.clone())?;
                let mut linker = Linker::new(engine);
                linker.define(&store, module, name, memory)?;
                linker.define_unknown_imports_as_traps(&self.module)?;
                let instance = linker.instantiate(&mut store, &self.module)?;
                (instance, memory)
            }
        };

        let has_malloc = instance.get_func(&mut store, "malloc").is_some();
        let result = f(&mut store, &instance, memory, has_malloc)?;
        Ok(result)
    }

    /// Get the plugin's identifier string.
    pub fn id(&self) -> &str {
        &self.plugin_id
    }

    /// Bump-allocate `size` bytes in WASM memory starting at `FALLBACK_BASE`.
    /// Grows the memory as needed. Only used when the module has no `malloc`.
    fn fallback_alloc(
        memory: &Memory,
        store: &mut Store<()>,
        arena_next: &mut usize,
        size: usize,
    ) -> anyhow::Result<usize> {
        let ptr = *arena_next;
        let end = ptr + size;
        let current_pages = memory.size(&*store) as usize;
        let needed_pages = end.div_ceil(65536);
        if needed_pages > current_pages {
            memory.grow(&mut *store, (needed_pages - current_pages) as u64)?;
        }
        *arena_next = end;
        Ok(ptr)
    }

    /// Allocate a buffer of `size` bytes in WASM memory and copy `bytes` into
    /// it. Uses the module's `malloc` if exported; otherwise bump-allocates in
    /// the fallback region (distinct from any other allocation).
    fn write_bytes(
        store: &mut Store<()>,
        instance: &Instance,
        memory: &Memory,
        bytes: &[u8],
        has_malloc: bool,
        arena_next: &mut usize,
    ) -> anyhow::Result<usize> {
        let ptr = if has_malloc {
            let malloc_fn: wasmtime::TypedFunc<i32, i32> =
                instance.get_typed_func(&mut *store, "malloc")?;
            malloc_fn.call(&mut *store, bytes.len() as i32)? as usize
        } else {
            Self::fallback_alloc(memory, store, arena_next, bytes.len())?
        };
        memory.write(&mut *store, ptr, bytes)?;
        Ok(ptr)
    }

    /// Detect whether this plugin can handle the given bytes.
    pub fn detect(&self, bytes: &[u8]) -> bool {
        let result = self.with_instance(|store, instance, memory, has_malloc| {
            let mut arena_next = FALLBACK_BASE;
            let ptr =
                Self::write_bytes(store, instance, &memory, bytes, has_malloc, &mut arena_next)?;

            let detect_fn =
                instance.get_typed_func::<(i32, i32), i32>(&mut *store, "adapter_detect")?;
            let result = detect_fn.call(&mut *store, (ptr as i32, bytes.len() as i32))?;
            Ok(result != 0)
        });
        result.unwrap_or(false)
    }

    /// Parse the given bytes into a Scenario.
    pub fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let result = self.with_instance(|store, instance, memory, has_malloc| {
            let mut arena_next = FALLBACK_BASE;
            let input_ptr =
                Self::write_bytes(store, instance, &memory, bytes, has_malloc, &mut arena_next)?;

            let parse_fn = instance
                .get_typed_func::<(i32, i32, i32, i32), i32>(&mut *store, "adapter_parse")?;

            // Allocate the output buffer AFTER input in the fallback arena
            // (or via the module's malloc) so the two can never alias
            // (regression fix for the old fixed-offset collision).
            let output_ptr = if has_malloc {
                let malloc_fn: wasmtime::TypedFunc<i32, i32> =
                    instance.get_typed_func(&mut *store, "malloc")?;
                malloc_fn.call(&mut *store, MAX_OUTPUT_BYTES as i32)? as usize
            } else {
                Self::fallback_alloc(&memory, store, &mut arena_next, MAX_OUTPUT_BYTES)?
            };

            let written = parse_fn.call(
                &mut *store,
                (
                    input_ptr as i32,
                    bytes.len() as i32,
                    output_ptr as i32,
                    MAX_OUTPUT_BYTES as i32,
                ),
            )?;

            if written <= 0 {
                anyhow::bail!("WASM adapter returned parse error (code: {})", written);
            }

            // DoS guard: clamp the plugin's claimed written length. We handed
            // the adapter a MAX_OUTPUT_BYTES buffer, so anything larger is a
            // lie; trusting it would allocate vec![0u8; written] and abort the
            // host for ~2 GB claims (defeating the fuel guard's purpose).
            let written = written.min(MAX_OUTPUT_BYTES as i32) as u32;
            let json_str = read_wasm_buffer(&*store, &memory, output_ptr, written);
            Ok(json_str)
        });

        match result {
            Ok(json_str) => {
                let wit_scenario: WasmScenario = serde_json::from_str(&json_str).map_err(|e| {
                    TropelError::Parse(format!("WASM adapter returned invalid JSON: {}", e))
                })?;
                convert_scenario(wit_scenario)
            }
            Err(e) => Err(TropelError::Parse(format!("WASM adapter error: {}", e))),
        }
    }

    /// Read the plugin id by invoking `adapter_id` on a fresh instance.
    fn read_adapter_id(&self) -> anyhow::Result<String> {
        self.with_instance(|store, instance, memory, _has_malloc| {
            let id_ptr: i32 = instance
                .get_typed_func::<(), i32>(&mut *store, "adapter_id")?
                .call(&mut *store, ())?;
            Ok(read_wasm_string(&*store, &memory, id_ptr))
        })
    }
}

/// Load a `.wasm` module with the AOT `.cwasm` cache.
///
/// Shared by the declarative adapters ([`WasmPlugin::from_file`]) and the
/// imperative driver (`driver.rs`), so both get JIT-free reloads.
///
/// # Cache security model (P0 fixes)
///
/// The old cache sat **next to the plugin file** and its sidecar hashed the
/// *source* `.wasm`; a malicious tarball pairing a benign `evil.wasm` with an
/// attacker-authored `evil.cwasm` + `evil.cwasm.sha256 = sha256(evil.wasm)`
/// passed the check and `unsafe Module::deserialize` ran the attacker's
/// native code — bypassing fuel, the 16 MiB cap, and
/// `define_unknown_imports_as_traps` (the `.wasm` never compiled).
///
/// Now:
/// - The cache lives in a **host-private `0700` dir** (`wasm_cache_dir()`),
///   keyed by `sha256(source)` — never beside third-party plugin files, so a
///   plugin tarball cannot plant a cache entry.
/// - The sidecar authenticates the **artifact we deserialize**: it stores
///   `sha256(.cwasm bytes)`, and the cache is only trusted when the file on
///   disk re-hashes to it. A tampered/torn/foreign artifact fails the check
///   and is recompiled from the trusted source bytes.
/// - Writes are **atomic** (`<tmp>.<pid>` + rename): concurrent `init()`s on
///   a cold cache can never hand `deserialize` torn bytes.
/// - A process-global in-memory cache means each unique source is compiled
///   **once per process**, not once per VU.
pub(crate) fn load_module_aot(path: &Path) -> std::result::Result<Module, anyhow::Error> {
    let wasm_bytes = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("failed to read '{}': {}", path.display(), e))?;
    let key = sha256_hex(&wasm_bytes);
    let engine = global_engine();

    // In-memory module cache: compile once per unique source, share the
    // compiled artifact across all VUs (thread-per-core calls init() from
    // many VU threads concurrently).
    if let Some(m) = module_cache()
        .lock()
        .ok()
        .and_then(|c| c.get(&key).cloned())
    {
        return Ok(m);
    }

    let cache_dir = wasm_cache_dir();
    let cache_path = cache_dir.join(format!("{key}.cwasm"));
    let hash_path = cache_dir.join(format!("{key}.sha256"));

    // Cache is trusted ONLY if the artifact on disk re-hashes to the sidecar
    // we wrote for it. A missing/mismatched cache falls through to compile.
    let cache_artifact = std::fs::read(&cache_path).ok();
    let cache_trusted = cache_artifact
        .as_ref()
        .zip(std::fs::read_to_string(&hash_path).ok())
        .map(|(bytes, sidecar)| sidecar.trim() == sha256_hex(bytes))
        .unwrap_or(false);

    let module = if cache_trusted {
        let cached = cache_artifact.expect("cache_trusted implies artifact exists");
        // SAFETY: `cached` re-hashes to the sidecar we wrote for this exact
        // source, in a host-private dir, produced by this engine version.
        // A cache from an incompatible engine fails deserialize and we
        // recompile below.
        match unsafe { Module::deserialize(engine, &cached) } {
            Ok(m) => m,
            Err(_) => aot_compile(engine, &wasm_bytes, &cache_path, &hash_path)?,
        }
    } else {
        aot_compile(engine, &wasm_bytes, &cache_path, &hash_path)?
    };

    if let Ok(mut c) = module_cache().lock() {
        c.insert(key, module.clone());
    }
    Ok(module)
}

/// Host-private AOT cache directory: `~/.cache/tropel/wasm-cache` (unix) /
/// `%LOCALAPPDATA%\tropel\wasm-cache` (Windows), created with `0700`
/// permissions. Never the plugin's own directory — a third-party plugin must
/// not be able to plant a `.cwasm` next to its `.wasm`.
fn wasm_cache_dir() -> PathBuf {
    let base = if cfg!(windows) {
        std::env::var("LOCALAPPDATA")
            .map(PathBuf::from)
            .unwrap_or_else(|_| std::env::temp_dir())
    } else {
        std::env::var("XDG_CACHE_HOME")
            .map(PathBuf::from)
            .ok()
            .filter(|p| !p.as_os_str().is_empty())
            .or_else(|| {
                std::env::var("HOME")
                    .ok()
                    .map(|h| PathBuf::from(h).join(".cache"))
            })
            .unwrap_or_else(std::env::temp_dir)
    };
    let dir = base.join("tropel").join("wasm-cache");
    let _ = std::fs::create_dir_all(&dir);
    // 0700 is load-bearing even on the temp_dir fallback (unix /tmp is
    // world-writable): another local user must not be able to pre-plant a
    // `.cwasm` + sidecar pair that matches a source hash we later load.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    dir
}

/// Process-global compiled-module cache, keyed by `sha256(source)`. Lets all
/// VUs share ONE compiled artifact per unique plugin (no per-VU JIT/AOT).
fn module_cache() -> &'static Mutex<HashMap<String, Module>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Module>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Unique suffix for `atomic_write` temp files. The pid alone is not unique:
/// concurrent VU threads in one process share the pid, so two writers could
/// collide on the same tmp name (truncate+write interleave → torn file, and a
/// failed rename on Windows where rename-over-existing is an error). The
/// per-call counter makes every writer's tmp file distinct.
static TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Atomic write: write to a unique temp name, then rename over the target.
/// Concurrent writers can never produce a torn file (rename is atomic on the
/// same filesystem), and readers never observe partial bytes. A failed rename
/// (e.g. Windows refusing to replace an existing file) degrades to a logged
/// cache miss — never corruption, since the in-memory `module_cache` covers
/// intra-process reuse anyway.
fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let unique = format!(
        "tmp.{}.{}",
        std::process::id(),
        TMP_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let tmp = path.with_extension(unique);
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Precompile wasm bytes, persist the `.cwasm` cache + its artifact-hash
/// sidecar atomically, and load it (shared by adapters and the driver).
pub(crate) fn aot_compile(
    engine: &Engine,
    wasm_bytes: &[u8],
    cache_path: &Path,
    hash_path: &Path,
) -> std::result::Result<Module, anyhow::Error> {
    let compiled = engine.precompile_module(wasm_bytes)?;
    if let Err(e) = atomic_write(cache_path, &compiled) {
        tracing::warn!(
            "Failed to write WASM AOT cache '{}': {}",
            cache_path.display(),
            e
        );
    } else if let Err(e) = atomic_write(hash_path, sha256_hex(&compiled).as_bytes()) {
        tracing::warn!(
            "Failed to write WASM cache hash '{}': {}",
            hash_path.display(),
            e
        );
    }
    // SAFETY: `compiled` was just produced by `Engine::precompile_module`
    // on this same engine.
    Ok(unsafe { Module::deserialize(engine, &compiled) }?)
}

/// Hex-encode the SHA-256 digest of `bytes` (cache sidecar format).
pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(digest.len() * 2);
    for b in digest {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

/// Read a null-terminated string from WASM memory at the given pointer.
///
/// Scans the live memory region directly (no fixed-size buffer), so a long
/// plugin id is never silently truncated.
pub(crate) fn read_wasm_string(store: &Store<()>, memory: &Memory, ptr: i32) -> String {
    if ptr < 0 {
        return String::new();
    }
    let data = memory.data(store);
    let start = ptr as usize;
    let rest = data.get(start..).unwrap_or(&[]);
    let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
    String::from_utf8_lossy(&rest[..end]).to_string()
}

/// Read a buffer from WASM memory.
pub(crate) fn read_wasm_buffer(store: &Store<()>, memory: &Memory, ptr: usize, len: u32) -> String {
    if len == 0 {
        return String::new();
    }
    let mut buf = vec![0u8; len as usize];
    if memory.read(store, ptr, &mut buf).is_ok() {
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
    let items = build_item_tree(&ws.items)?;
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
        auth: ws.auth.as_ref().and_then(convert_auth),
    })
}

/// Threads `Result` because a request's method token may be invalid — the
/// whole scenario conversion fails loudly instead of silently becoming GET.
fn build_item_tree(flat: &[WasmItem]) -> Result<Vec<ScenarioItem>> {
    // Collect flat items first
    let items: Vec<ScenarioItem> = flat
        .iter()
        .map(|wi| -> Result<ScenarioItem> {
            Ok(ScenarioItem {
                name: wi.name.clone(),
                request: wi.request.as_ref().map(convert_request).transpose()?,
                prerequest: wi.prerequest.clone(),
                test: wi.test.clone(),
                assertions: wi.assertions.clone(),
                items: Vec::new(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    // Build tree from parent_index references
    // For recursive items (self-contained), use the `items` field directly
    if flat.is_empty() {
        return Ok(Vec::new());
    }

    // If any item has child items directly (recursive format), use those
    let has_recursive = flat.iter().any(|wi| !wi.items.is_empty());
    if has_recursive {
        return flat
            .iter()
            .map(|wi| -> Result<ScenarioItem> {
                let children = build_item_tree(&wi.items)?;
                Ok(ScenarioItem {
                    name: wi.name.clone(),
                    request: wi.request.as_ref().map(convert_request).transpose()?,
                    prerequest: wi.prerequest.clone(),
                    test: wi.test.clone(),
                    assertions: wi.assertions.clone(),
                    items: children,
                })
            })
            .collect::<Result<Vec<_>>>();
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
    Ok(result)
}

fn convert_request(wr: &WasmRequest) -> Result<Request> {
    // A genuinely invalid method token must fail loudly, not silently become
    // GET (backlog line 95). Valid-but-uncommon tokens (PURGE/LINK/…) parse
    // fine via Method::Custom.
    let method = Method::parse(&wr.method).ok_or_else(|| {
        TropelError::Parse(format!(
            "WASM plugin request has invalid HTTP method {:?}",
            wr.method
        ))
    })?;

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

    Ok(Request {
        url: wr.url.clone(),
        method,
        headers: wr.headers.clone(),
        query_params: wr.query_params.clone(),
        body,
        auth: wr.auth.as_ref().and_then(convert_auth),
        certificate: None,
        follow_redirects: wr.follow_redirects,
        timeout: wr.timeout_ms.map(std::time::Duration::from_millis),
        response_type: tropel_core::types::ResponseType::Text,
    })
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

#[derive(Clone)]
pub struct WasmInputAdapter {
    plugin: WasmPlugin,
}

impl WasmInputAdapter {
    pub fn new(wasm_bytes: &[u8]) -> Result<Self> {
        let plugin = WasmPlugin::load(wasm_bytes)
            .map_err(|e| TropelError::Other(format!("Failed to load WASM plugin: {}", e)))?;
        Ok(Self { plugin })
    }

    pub fn from_file(path: &Path) -> Result<Self> {
        let plugin = WasmPlugin::from_file(path)
            .map_err(|e| TropelError::Other(format!("Failed to load WASM plugin: {}", e)))?;
        Ok(Self { plugin })
    }

    pub fn plugin_id(&self) -> &str {
        self.plugin.id()
    }

    /// Set the per-call WASM instruction budget (fuel units).
    pub fn with_call_fuel(mut self, fuel: u64) -> Self {
        self.plugin = self.plugin.with_call_fuel(fuel);
        self
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

/// Discover `.wasm` plugins in a directory and load each (with AOT `.cwasm`
/// caching). Malformed modules are skipped with a warning.
pub fn discover_plugins(plugins_dir: &Path) -> Vec<WasmInputAdapter> {
    let mut adapters = Vec::new();
    let dir = match std::fs::read_dir(plugins_dir) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(
                "Cannot read plugins directory '{}': {}",
                plugins_dir.display(),
                e
            );
            return adapters;
        }
    };

    for entry in dir.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("wasm") {
            continue;
        }
        match WasmInputAdapter::from_file(&path) {
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

    const ECHO_WAT: &str = r#"
(module
  (memory (export "memory") 64 256)
  (global $heap (mut i32) (i32.const 1024))
  (data (i32.const 0) "roundtrip-plugin\00")
  (data (i32.const 32) "{\"name\":\"wasm\",\"items\":[]}\00")
  (func (export "malloc") (param $size i32) (result i32)
    (local $ptr i32)
    (local.set $ptr (global.get $heap))
    (global.set $heap (i32.add (global.get $heap) (local.get $size)))
    (local.get $ptr))
  (func (export "free") (param $ptr i32))
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $ptr i32) (param $len i32) (result i32)
    (if (i32.eqz (local.get $len)) (then (return (i32.const 0))))
    (i32.eq (i32.load8_u (local.get $ptr)) (i32.const 0x7f)))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    ;; strlen of the fixed JSON at offset 32
    (local $len i32)
    (local $i i32)
    (block $strlen
      (loop $loop
        (br_if $strlen (i32.eqz (i32.load8_u (i32.add (i32.const 32) (local.get $len)))))
        (local.set $len (i32.add (local.get $len) (i32.const 1)))
        (br $loop)))
    ;; fail if output buffer too small
    (if (i32.lt_u (local.get $out_len) (local.get $len)) (then (return (i32.const 0))))
    ;; copy JSON -> out
    (block $copy
      (loop $cloop
        (br_if $copy (i32.ge_u (local.get $i) (local.get $len)))
        (i32.store8 (i32.add (local.get $out) (local.get $i))
                    (i32.load8_u (i32.add (i32.const 32) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cloop)))
    ;; Regression: input must still be intact after writing output.
    ;; If output aliased input (old fixed-offset bug), this fails.
    (if (i32.ne (i32.load8_u (local.get $in)) (i32.const 0x7f)) (then (return (i32.const 0))))
    (local.get $len))
)
"#;

    const LOOP_WAT: &str = r#"
(module
  (memory (export "memory") 64 256)
  (data (i32.const 0) "loop-plugin\00")
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $p i32) (param $n i32) (result i32)
    ;; Valid infinite loop: the loop never falls through, so the function
    ;; result is only statically reachable via the trailing const.
    (block $exit
      (loop $spin
        (br $spin)))
    (i32.const 0))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    (i32.const 0))
)
"#;

    const MEMORY_IMPORT_WAT: &str = r#"
(module
  (import "env" "memory" (memory 64 256))
  (data (i32.const 0) "import-plugin\00")
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $ptr i32) (param $len i32) (result i32)
    (if (i32.eqz (local.get $len)) (then (return (i32.const 0))))
    (i32.eq (i32.load8_u (local.get $ptr)) (i32.const 0x7f)))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    ;; echo input -> output
    (local $i i32)
    (if (i32.lt_u (local.get $out_len) (local.get $in_len)) (then (return (i32.const 0))))
    (block $copy
      (loop $cloop
        (br_if $copy (i32.ge_u (local.get $i) (local.get $in_len)))
        (i32.store8 (i32.add (local.get $out) (local.get $i))
                    (i32.load8_u (i32.add (local.get $in) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cloop)))
    (local.get $in_len))
)
"#;

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

    #[test]
    fn test_real_module_roundtrip() {
        let plugin = WasmPlugin::load(ECHO_WAT.as_bytes()).expect("module must load");
        assert_eq!(plugin.id(), "roundtrip-plugin");

        // detect: first byte == 0x7f
        assert!(plugin.detect(&[0x7f, 1, 2, 3]));
        assert!(!plugin.detect(&[0x00, 1, 2, 3]));
        assert!(!plugin.detect(&[]));

        // parse returns the fixed JSON scenario
        let scenario = plugin.parse(&[0x7f, 1, 2, 3]).expect("parse must succeed");
        assert_eq!(scenario.info.name, "wasm");
        assert!(scenario.items.is_empty());
    }

    #[test]
    fn test_infinite_loop_traps() {
        // A plugin whose detect() spins forever must be interrupted by fuel
        // metering rather than hang the host.
        let plugin = WasmPlugin::load(LOOP_WAT.as_bytes())
            .expect("module must load")
            .with_call_fuel(1_000_000);
        let start = std::time::Instant::now();
        // detect traps → with_instance returns Err → detect() == false
        assert!(!plugin.detect(&[0x7f]));
        let elapsed = start.elapsed();
        assert!(
            elapsed < std::time::Duration::from_secs(5),
            "infinite loop must trap quickly, took {:?}",
            elapsed
        );
    }

    #[test]
    fn test_memory_import_module() {
        // Load-path fix (a): a module that *imports* memory must get a
        // host-supplied memory (no exported 'memory' required).
        let plugin =
            WasmPlugin::load(MEMORY_IMPORT_WAT.as_bytes()).expect("memory-import module must load");
        assert_eq!(plugin.id(), "import-plugin");
        assert!(plugin.detect(&[0x7f, 9, 9]));

        // echo parse: feed a minimal scenario JSON, get it back. The echo
        // module copies input→output verbatim, so the input must already be
        // valid JSON (no 0x7f detect prefix here — detect and parse are
        // independent calls).
        let json = r#"{"name":"echo","items":[]}"#;
        let scenario = plugin.parse(json.as_bytes()).expect("parse must succeed");
        assert_eq!(scenario.info.name, "echo");
    }

    #[test]
    fn test_aot_cache_roundtrip() {
        let temp = tempfile::tempdir().unwrap();
        let wasm_path = temp.path().join("plugin.wasm");
        // Write WAT text (wasmtime accepts text or binary on Module::new /
        // precompile_module alike).
        std::fs::write(&wasm_path, ECHO_WAT.as_bytes()).unwrap();

        let plugin1 = WasmInputAdapter::from_file(&wasm_path).expect("first load must succeed");
        assert_eq!(plugin1.plugin_id(), "roundtrip-plugin");

        // The cache lives in the host-private dir keyed by sha256(source),
        // never beside the plugin file (P0: an attacker-authored .cwasm next
        // to a benign .wasm must not be deserialized).
        let key = sha256_hex(ECHO_WAT.as_bytes());
        let cache_path = wasm_cache_dir().join(format!("{key}.cwasm"));
        assert!(cache_path.exists(), "AOT cache must be written");
        assert!(
            !temp.path().join("plugin.cwasm").exists(),
            "cache must NOT sit beside the plugin file"
        );
        // Sidecar authenticates the ARTIFACT: it must equal the hash of the
        // .cwasm bytes (not the source) — tampering with the artifact breaks
        // the match and forces a recompile.
        let sidecar =
            std::fs::read_to_string(wasm_cache_dir().join(format!("{key}.sha256"))).unwrap();
        let artifact = std::fs::read(&cache_path).unwrap();
        assert_eq!(sidecar.trim(), sha256_hex(&artifact));

        // Second load reuses the .cwasm cache.
        let plugin2 = WasmInputAdapter::from_file(&wasm_path).expect("cached load must succeed");
        assert_eq!(plugin2.plugin_id(), "roundtrip-plugin");
        assert!(plugin2.detect(&[0x7f]));
    }

    const OVER_MIN_MEMORY_WAT: &str = r#"
(module
  (memory (export "memory") 300 512)
  (data (i32.const 0) "over-memory-plugin\00")
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $p i32) (param $n i32) (result i32)
    (i32.const 0))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    (i32.const 0))
)
"#;

    const HOSTILE_LENGTH_WAT: &str = r#"
(module
  (memory (export "memory") 64 256)
  (data (i32.const 0) "hostile-length-plugin\00")
  (func (export "adapter_id") (result i32) (i32.const 0))
  (func (export "adapter_detect") (param $p i32) (param $n i32) (result i32)
    (i32.const 1))
  (func (export "adapter_parse") (param $in i32) (param $in_len i32) (param $out i32) (param $out_len i32) (result i32)
    ;; Claim a ~2 GB written length without writing anything. The host must
    ;; clamp to MAX_OUTPUT_BYTES instead of allocating 2 GB (DoS guard).
    (i32.const 2147483647))
)
"#;

    #[test]
    fn test_exported_memory_capped() {
        // A module that *exports* its own memory with a declared minimum above
        // the engine-level MAX_MEMORY_BYTES cap (300 pages = ~19 MiB > 16 MiB)
        // must fail to load — wasmtime's pooling max_memory_size applies to
        // exported memories too, closing the cdylib memory-DoS gap.
        let result = WasmPlugin::load(OVER_MIN_MEMORY_WAT.as_bytes());
        assert!(
            result.is_err(),
            "module with exported memory above the cap must fail to load, got {:?}",
            result.map(|_| ())
        );
    }

    #[test]
    fn test_hostile_written_length_clamped() {
        // A plugin claiming an absurd written length must not OOM/abort the
        // host. parse() clamps to MAX_OUTPUT_BYTES, reads that many bytes
        // (mostly zeroed memory -> invalid JSON) and returns a Parse error.
        let plugin = WasmPlugin::load(HOSTILE_LENGTH_WAT.as_bytes()).expect("module must load");
        assert_eq!(plugin.id(), "hostile-length-plugin");

        let start = std::time::Instant::now();
        let result = plugin.parse(&[0x7f, 1, 2, 3]);
        assert!(
            result.is_err(),
            "hostile written length must produce a parse error, got {:?}",
            result
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "clamped read must complete quickly"
        );
    }

    #[test]
    fn test_discover_plugins_finds_real_module() {
        let temp = tempfile::tempdir().unwrap();
        let wasm_path = temp.path().join("plugin.wasm");
        std::fs::write(&wasm_path, ECHO_WAT.as_bytes()).unwrap();

        let adapters = discover_plugins(temp.path());
        assert_eq!(adapters.len(), 1);
        assert_eq!(adapters[0].plugin_id(), "roundtrip-plugin");
    }
}
