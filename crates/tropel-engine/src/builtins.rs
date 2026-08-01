//! # Built-in extension wiring
//!
//! The built-in input adapters (postman, har, openapi, k6) register
//! themselves via `inventory::submit!`. However, Rust's linker only pulls
//! object code from a dependency crate that is actually *referenced* from
//! the final binary. Since nothing else in `tropel-engine` mentions these
//! adapter types, their registration statics get dead-stripped and the
//! binary reports "no input adapter recognized" / "none registered".
//!
//! The functions below construct each built-in adapter/driver type, forcing
//! the linker to include the crate's object code — and therefore its
//! `inventory::submit!` registration. This mirrors what `tropel build` does
//! when it emits `extern crate {name};` lines into the generated `main.rs`.
//!
//! `register_builtins()` is invoked from the CLI at startup, before the
//! `ExtensionRegistry` performs its `collect_inventory()` pass.

use tropel_ext::traits::{Driver, InputAdapter, Protocol};

/// Force-link every built-in input adapter and driver by constructing it.
/// Returns the total number of built-ins so the call is observable.
pub fn link_builtins() -> usize {
    let adapters: Vec<Box<dyn InputAdapter>> = vec![
        Box::new(tropel_input_postman::PostmanInputAdapter),
        Box::new(tropel_input_har::HarInputAdapter),
        Box::new(tropel_input_openapi::OpenApiInputAdapter),
        Box::new(tropel_input_k6::K6ScriptAdapter),
    ];
    let drivers: Vec<Box<dyn Driver>> = vec![Box::new(tropel_input_k6::driver::K6Driver)];
    // Force-link the gRPC protocol so its `inventory::submit!` registration
    // survives dead-stripping — this is what makes `grpc://` / `grpcs://`
    // URLs reachable through the VU runner's scheme dispatch.
    let protocols: Vec<Box<dyn Protocol>> = vec![Box::new(tropel_x_grpc::GrpcProtocol)];
    adapters.len() + drivers.len() + protocols.len()
}

/// Call from the CLI before registry collection so the linker keeps the
/// built-in `inventory::submit!` registrations alive.
pub fn register_builtins() {
    let count = link_builtins();
    tracing::debug!("Force-linked {count} built-in adapter/driver type(s)");
}
