//! Extension-registry construction shared by the `run`, `inspect` and
//! `extensions` commands. Split out of the former `cli.rs` god-file.

use std::path::Path;
use std::sync::Arc;
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;

/// Build the extension registry exactly like `run_command` does: built-in
/// adapters/drivers from `inventory` plus any `--subprocess-adapter` and
/// `--plugins-dir` extras. Shared by `run`, `inspect` and `list` so resolution
/// is always identical.
pub(crate) fn build_registry(
    subprocess_adapter: &[String],
    plugins_dir: Option<&Path>,
) -> Result<ExtensionRegistry> {
    let mut registry = ExtensionRegistry::new();

    // Register any subprocess adapters specified via --subprocess-adapter
    for cmd in subprocess_adapter {
        // `SubprocessAdapter::new` rejects empty commands with a TropelError
        // (the old `parts[1..]` panicked) — surface that here, at CLI parse
        // time, instead of letting the factory panic later.
        if cmd.trim().is_empty() {
            return Err(TropelError::Other(format!(
                "--subprocess-adapter requires a non-empty command (got {cmd:?})"
            )));
        }
        let id = format!("subprocess:{}", cmd);
        tracing::info!("Registering subprocess adapter (command: {})", cmd);
        let cmd_clone = cmd.clone();
        registry.register_adapter_factory(
            &id,
            Arc::new(move || {
                Box::new(
                    tropel_input_subprocess::SubprocessAdapter::new(&cmd_clone)
                        .expect("command validated non-empty above"),
                )
            }),
        );
    }

    // Register WASM plugins from --plugins-dir (Tier 2 no-recompile adapters).
    if let Some(dir) = plugins_dir {
        let adapters = tropel_wasm::discover_plugins(dir);
        tracing::info!(
            "Loaded {} WASM plugin(s) from {}",
            adapters.len(),
            dir.display()
        );
        for adapter in adapters {
            let id = format!("wasm:{}", adapter.plugin_id());
            let adapter = adapter.clone();
            registry.register_adapter_factory(&id, Arc::new(move || Box::new(adapter.clone())));
        }
    }

    Ok(registry)
}
