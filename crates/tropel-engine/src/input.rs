//! Input resolution — Driver or Scenario dispatch.
//!
//! Moved out of the former `engine.rs` god-file.

use std::collections::HashMap;
use std::sync::Arc;
use tropel_core::scenario::Scenario;
use tropel_core::{Result, TropelError};
use tropel_ext::registry::ExtensionRegistry;
use tropel_ext::traits::{Driver, InputAdapter};

pub(crate) enum ResolvedInput {
    Scenario(Arc<Scenario>),
    Driver(Box<dyn Driver>),
}

pub(crate) fn resolve_input_or_driver(
    input_path: &str,
    format_hint: Option<&str>,
    registry: &ExtensionRegistry,
    base_env: &HashMap<String, String>,
) -> Result<ResolvedInput> {
    let input_p = std::path::Path::new(input_path);
    let bytes = std::fs::read(input_path)
        .map_err(|e| TropelError::Parse(format!("Failed to read '{}': {}", input_path, e)))?;

    // 1. Try drivers first
    let driver: Option<Box<dyn Driver>> = if let Some(fmt) = format_hint {
        registry.resolve_driver_by_id(fmt)
    } else {
        registry.resolve_driver(&bytes)
    };

    if let Some(driver) = driver {
        tracing::info!(
            "Input '{}' resolved by driver '{}'",
            input_path,
            driver.id()
        );
        return Ok(ResolvedInput::Driver(driver));
    }

    // 2. Fall back to input adapters
    let adapter: Box<dyn InputAdapter> = if let Some(fmt) = format_hint {
        registry.resolve_input_by_id(fmt).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Config(format!(
                "Unknown input format '{}'. Available formats: {}",
                fmt,
                available.join(", ")
            ))
        })?
    } else {
        registry.resolve_input(&bytes).ok_or_else(|| {
            let available = registry.list_inputs();
            TropelError::Parse(format!(
                "No input adapter recognized '{}'. Available adapters: {}",
                input_path,
                if available.is_empty() {
                    "(none registered — check build configuration)".to_string()
                } else {
                    available.join(", ")
                }
            ))
        })?
    };

    tracing::info!(
        "Input '{}' resolved by adapter '{}'",
        input_path,
        adapter.id()
    );

    let mut scenario = adapter.parse_with_path(&bytes, Some(input_p))?;
    for (key, val) in base_env {
        scenario
            .variables
            .entry(key.clone())
            .or_insert_with(|| serde_json::Value::String(val.clone()));
    }

    Ok(ResolvedInput::Scenario(Arc::new(scenario)))
}
