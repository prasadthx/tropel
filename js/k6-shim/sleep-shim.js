// ══════════════════════════════════════════════════════════════════
// k6 sleep() shim — delegates to the native __tropel_native_sleep
// bridge (installed per-VU by the K6DriverInstance / engine). The
// bridge blocks the OS thread, which is safe under thread-per-core
// (1 VU per dedicated worker thread).
// ══════════════════════════════════════════════════════════════════
if (typeof sleep === 'undefined') {
  function sleep(seconds) {
    if (typeof __tropel_native_sleep === 'function') {
      __tropel_native_sleep(seconds * 1000);
    }
  }
}
