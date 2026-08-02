//! Tropel criterion benchmark suite.
//!
//! Covers the PERF/P3 matrix:
//! 1. **context_bootstrap** — cost of creating a fresh per-VU `JsContext`
//!    (QuickJS Runtime + Context + console bootstrap), with/without a memory cap.
//! 2. **script_iteration** — per-iteration overhead: cold `eval` (re-parse every
//!    iteration) vs `run_script_cached` (Persistent<Function> compiled once).
//! 3. **native_vs_js** — the same logical operation (hex-encode ×1000) executed
//!    via the native bridge (`__tropel_native_hex_encode`) vs a pure-JS
//!    implementation — the headline native-vs-JS speedup.
//! 4. **vus_per_sec** — `VUWorkerPool` task dispatch throughput (thread-per-core
//!    sharding): spawn + await N trivial tasks across the worker pool.
//! 5. **memory_per_vu** — process RSS growth per live `JsContext` (real
//!    memory-per-VU, the thing `vus_max` should track).
//!
//! Run: `cargo bench -p tropel-bench`.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;
use tropel_js::JsContext;

/// A small current-thread tokio runtime to drive the async JS bridge from
/// criterion's synchronous harness.
fn tokio_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bench tokio runtime")
}

/// Process resident set size in bytes (best-effort; None where unsupported).
fn process_rss_bytes() -> Option<u64> {
    #[cfg(target_os = "windows")]
    {
        use windows_sys::Win32::System::ProcessStatus::{
            GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
        };
        use windows_sys::Win32::System::Threading::GetCurrentProcess;
        let mut pmc: PROCESS_MEMORY_COUNTERS = unsafe { std::mem::zeroed() };
        let proc_handle = unsafe { GetCurrentProcess() };
        let ok = unsafe {
            GetProcessMemoryInfo(
                proc_handle,
                &mut pmc,
                std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            )
        };
        if ok != 0 {
            Some(pmc.WorkingSetSize as u64)
        } else {
            None
        }
    }
    #[cfg(target_os = "linux")]
    {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
        let kb: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
        Some(kb * 1024)
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    {
        None
    }
}

/// 1. Context bootstrap — the fixed per-VU startup cost.
fn context_bootstrap(c: &mut Criterion) {
    let rt = tokio_rt();
    let mut group = c.benchmark_group("context_bootstrap");
    group.sample_size(30);

    group.bench_function("new_default", |b| {
        b.iter(|| rt.block_on(JsContext::new(None, None)).unwrap());
    });

    // 16 MiB memory cap + 10s interrupt — the settings the engine actually uses.
    group.bench_function("new_capped_16mb", |b| {
        b.iter(|| {
            rt.block_on(JsContext::new(
                Some(16 * 1024 * 1024),
                Some(Duration::from_secs(10)),
            ))
            .unwrap()
        });
    });

    group.finish();
}

/// 2. Per-iteration overhead — compile-once vs re-parse every iteration.
fn script_iteration(c: &mut Criterion) {
    let rt = tokio_rt();
    let src = "globalThis.__x = (globalThis.__x || 0) + 1;";
    let mut group = c.benchmark_group("script_iteration");
    group.sample_size(50);

    // Cold path: parse + compile + execute the source every iteration.
    group.bench_function("eval_cold", |b| {
        let ctx = rt.block_on(JsContext::new(None, None)).unwrap();
        b.iter(|| rt.block_on(ctx.eval(src)).unwrap());
    });

    // Warm path: Persistent<Function> compiled once, invoked per iteration.
    group.bench_function("run_script_cached_warm", |b| {
        let ctx = rt.block_on(JsContext::new(None, None)).unwrap();
        // Compile once (fills the cache), then measure repeated invocations.
        rt.block_on(ctx.run_script_cached(src, None)).unwrap();
        b.iter(|| rt.block_on(ctx.run_script_cached(src, None)).unwrap());
    });

    group.finish();
}

/// 3. Native-vs-JS — same operation, native bridge vs pure JS.
fn native_vs_js(c: &mut Criterion) {
    let rt = tokio_rt();
    let ctx = rt.block_on(JsContext::new(None, None)).unwrap();
    rt.block_on(tropel_native::install_all(&ctx)).unwrap();

    let mut group = c.benchmark_group("native_vs_js");
    group.sample_size(30);

    // Native: hex-encode via the Rust bridge, 1000 calls per script invocation.
    // The bridge takes a byte array (rquickjs Vec<u8> <-> JS Array), so the
    // payload string is converted to char codes first — exactly what the JS
    // shim layer does before calling the native function.
    let native_src = r#"
        const bytes = Array.from('benchmark payload 0123456789', (c) => c.charCodeAt(0));
        let s = '';
        for (let i = 0; i < 1000; i++) {
            s = __tropel_native_hex_encode(bytes);
        }
    "#;

    // JS: equivalent loop against a pure-JS hex encoder.
    let js_src = r#"
        function jsHexEncode(str) {
            let out = '';
            for (let i = 0; i < str.length; i++) {
                let c = str.charCodeAt(i).toString(16);
                out += c.length === 1 ? '0' + c : c;
            }
            return out;
        }
        let s = '';
        for (let i = 0; i < 1000; i++) {
            s = jsHexEncode('benchmark payload 0123456789');
        }
    "#;

    group.bench_function("native_hex_encode_x1000", |b| {
        // Compile once, then measure repeated invocations of the loop.
        rt.block_on(ctx.run_script_cached(native_src, None)).unwrap();
        b.iter(|| rt.block_on(ctx.run_script_cached(native_src, None)).unwrap());
    });

    group.bench_function("js_hex_encode_x1000", |b| {
        rt.block_on(ctx.run_script_cached(js_src, None)).unwrap();
        b.iter(|| rt.block_on(ctx.run_script_cached(js_src, None)).unwrap());
    });

    group.finish();
}

/// 4. VUs/sec — thread-per-core pool dispatch throughput.
fn vus_per_sec(c: &mut Criterion) {
    let pool = tropel_engine::worker::VUWorkerPool::new(4);
    let rt = tokio_rt();
    const N: usize = 10_000;
    let mut group = c.benchmark_group("vus_per_sec");
    group.throughput(Throughput::Elements(N as u64));
    group.sample_size(10);

    group.bench_function("spawn_await_trivial_10k", |b| {
        b.iter(|| {
            let mut handles = Vec::with_capacity(N);
            for _ in 0..N {
                handles.push(pool.spawn(async {}).1);
            }
            rt.block_on(async {
                for h in handles {
                    h.await.unwrap();
                }
            });
        });
    });

    group.finish();
}

/// 5. Memory-per-VU — RSS growth across live contexts.
fn memory_per_vu(c: &mut Criterion) {
    let rt = tokio_rt();
    let before = process_rss_bytes().unwrap_or(0);
    const N: usize = 25;
    let mut contexts = Vec::with_capacity(N);
    for _ in 0..N {
        contexts.push(rt.block_on(JsContext::new(None, None)).unwrap());
    }
    let after = process_rss_bytes().unwrap_or(0);
    let per_vu = if after > before {
        (after - before) / N as u64
    } else {
        0
    };
    eprintln!(
        "[memory_per_vu] {N} contexts: RSS {before}B -> {after}B, per-context ~= {per_vu}B"
    );

    let mut group = c.benchmark_group("memory_per_vu");
    group.sample_size(10);
    group.bench_function("rss_bytes_per_context", |b| {
        b.iter(|| std::hint::black_box(per_vu));
    });
    group.finish();
}

criterion_group!(
    perf,
    context_bootstrap,
    script_iteration,
    native_vs_js,
    vus_per_sec,
    memory_per_vu
);
criterion_main!(perf);
