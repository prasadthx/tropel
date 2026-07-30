//! # tropel-build
//!
//! xk6-style custom-binary builder tool.
//! Takes a list of extension crates (git/crates.io/path) and generates
//! a thin binary crate that depends on `tropel-engine` + those extensions.
//! Then runs `cargo build --release` to produce a custom `tropel` binary.
//!
//! ## Usage
//!
//! ```ignore
//! tropel build --with tropel-x-grpc --with tropel-x-websocket
//! ```
//!
//! This generates a temporary crate, adds the extensions as dependencies,
//! and builds a custom binary with those extensions linked in.
//!
//! ## How it works
//!
//! 1. Create a temporary directory.
//! 2. Generate `Cargo.toml` with `tropel-engine` + all extensions as dependencies.
//! 3. Generate `src/main.rs` that imports every extension (so their
//!    `inventory::submit!` calls are linked into the binary) and then
//!    delegates to `tropel_engine::cli::run_cli()`.
//! 4. Run `cargo build` in the temp directory.
//! 5. Copy the resulting binary to the output path.
//! 6. Clean up the temp directory on success (or leave it on failure for debugging).

use std::path::PathBuf;
use std::process::Command;
use tropel_core::{Result, TropelError};

/// Configuration for building a custom Tropel binary.
pub struct BuildConfig {
    /// Extension dependencies (e.g. "tropel-x-grpc", "./my-local-ext").
    pub extensions: Vec<ExtensionDep>,
    /// Output binary path (directory or full path).
    pub output: PathBuf,
    /// Whether to build in release mode.
    pub release: bool,
}

/// An extension dependency specification.
pub enum ExtensionDep {
    /// A crate from crates.io with optional version.
    /// e.g. `tropel-x-grpc = "0.1"`
    Registry { name: String, version: String },
    /// A path dependency.
    /// e.g. `tropel-x-grpc = { path = "../tropel-x-grpc" }`
    Path { name: String, path: String },
    /// A git dependency.
    /// e.g. `tropel-x-grpc = { git = "https://...", branch = "main" }`
    Git { name: String, url: String, reference: Option<String> },
}

impl std::fmt::Debug for ExtensionDep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtensionDep::Registry { name, version } => write!(f, "{} = \"{}\"", name, version),
            ExtensionDep::Path { name, path } => write!(f, "{} = {{ path = \"{}\" }}", name, path),
            ExtensionDep::Git { name, url, reference } => {
                if let Some(ref r) = reference {
                    let key = if r.contains('/') { "rev" } else if r.chars().all(|c| c.is_ascii_digit() || c == '.') { "tag" } else { "branch" };
                    write!(f, "{} = {{ git = \"{}\", {} = \"{}\" }}", name, url, key, r)
                } else {
                    write!(f, "{} = {{ git = \"{}\" }}", name, url)
                }
            }
        }
    }
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            extensions: vec![],
            output: PathBuf::from("./tropel"),
            release: true,
        }
    }
}

/// Build a custom Tropel binary with the given extensions.
///
/// Generates a temporary crate, runs `cargo build`, and copies the
/// resulting binary to the configured output path. The temporary
/// directory is cleaned up on success.
pub fn build(config: &BuildConfig) -> Result<()> {
    if config.extensions.is_empty() {
        tracing::warn!("No extensions specified — building standard tropel binary");
    } else {
        tracing::info!("Building custom Tropel binary with {} extension(s)", config.extensions.len());
    }

    let workspace_root = resolve_workspace_root()?;
    println!("Workspace root: {}", workspace_root.display());

    // Create a temporary directory
    let temp_dir = tempfile::tempdir()
        .map_err(|e| TropelError::Other(format!("Failed to create temp dir: {}", e)))?;
    let temp_path = temp_dir.path().to_path_buf();

    // Generate the crate structure
    let src_dir = temp_path.join("src");
    std::fs::create_dir_all(&src_dir)
        .map_err(|e| TropelError::Other(format!("Failed to create src dir: {}", e)))?;

    // Generate Cargo.toml
    let cargo_toml = generate_cargo_toml(config, &workspace_root);
    std::fs::write(temp_path.join("Cargo.toml"), cargo_toml)
        .map_err(|e| TropelError::Other(format!("Failed to write Cargo.toml: {}", e)))?;

    // Generate src/main.rs
    let main_rs = generate_main_rs(config);
    std::fs::write(src_dir.join("main.rs"), main_rs)
        .map_err(|e| TropelError::Other(format!("Failed to write src/main.rs: {}", e)))?;

    println!("Generated temporary crate at: {}", temp_path.display());

    // Run cargo build
    let build_profile = if config.release { "--release" } else { "" };
    println!("Running: cargo build {} ...", build_profile);

    let mut cmd = Command::new("cargo");
    cmd.current_dir(&temp_path).arg("build");
    if config.release {
        cmd.arg("--release");
    }

    let output = cmd.output()
        .map_err(|e| TropelError::Other(format!("Failed to run cargo build: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        eprintln!("{}", stderr);
        if !stdout.is_empty() {
            println!("{}", stdout);
        }
        let temp_path = temp_dir.path().to_path_buf();
        std::mem::forget(temp_dir); // prevent cleanup — preserve artifacts for debugging
        return Err(TropelError::Other(format!(
            "Cargo build failed. Build artifacts left at: {}",
            temp_path.display()
        )));
    }

    // Find the built binary
    let build_target_dir = temp_path.join("target").join(if config.release { "release" } else { "debug" });
    let binary_name = if cfg!(windows) { "tropel.exe" } else { "tropel" };
    let built_binary = build_target_dir.join(binary_name);

    if !built_binary.exists() {
        let temp_path = temp_dir.path().to_path_buf();
        std::mem::forget(temp_dir); // prevent cleanup — preserve artifacts for debugging
        return Err(TropelError::Other(format!(
            "Built binary not found at '{}'. Artifacts left at: {}",
            built_binary.display(), temp_path.display()
        )));
    }

    // Copy the binary to the output path
    let output_path = if config.output.is_dir() {
        config.output.join(binary_name)
    } else {
        config.output.clone()
    };

    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| TropelError::Other(format!("Failed to create output dir: {}", e)))?;
    }

    std::fs::copy(&built_binary, &output_path)
        .map_err(|e| TropelError::Other(format!("Failed to copy binary to '{}': {}", output_path.display(), e)))?;

    // Make the binary executable (no-op on Windows)
    #[cfg(not(windows))]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(&output_path) {
            let mut perms = meta.permissions();
            perms.set_mode(0o755);
            let _ = std::fs::set_permissions(&output_path, perms);
        }
    }

    println!();
    println!("✓ Custom Tropel binary built: {}", output_path.display());
    println!("  Extensions ({}):", config.extensions.len());
    for ext in &config.extensions {
        println!("    - {:?}", ext);
    }

    // Temp dir is automatically cleaned up when `temp_dir` is dropped
    Ok(())
}

/// Resolve the workspace root by walking up from the current directory
/// looking for a Cargo.toml with [workspace].
fn resolve_workspace_root() -> Result<PathBuf> {
    let mut current = std::env::current_dir()
        .map_err(|e| TropelError::Other(format!("Failed to get current dir: {}", e)))?;

    loop {
        let cargo_toml = current.join("Cargo.toml");
        if cargo_toml.exists() {
            if let Ok(content) = std::fs::read_to_string(&cargo_toml) {
                if content.contains("[workspace]") {
                    return Ok(current);
                }
            }
        }
        if !current.pop() {
            return Err(TropelError::Other(
                "Could not find workspace root (no Cargo.toml with [workspace] found)".into()
            ));
        }
    }
}

/// Generate the Cargo.toml content for the temporary build crate.
fn generate_cargo_toml(config: &BuildConfig, workspace_root: &PathBuf) -> String {
    let mut deps_lines = String::new();
    let root = workspace_root.to_string_lossy().replace('\\', "/");

    // Always depend on tropel-engine (re-exports everything needed)
    deps_lines.push_str(&format!(
        "tropel-engine = {{ path = \"{}/crates/tropel-engine\" }}\n", root
    ));

    // Add each extension as a dependency
    for ext in &config.extensions {
        match ext {
            ExtensionDep::Registry { name, version } => {
                deps_lines.push_str(&format!("{} = \"{}\"\n", name, version));
            }
            ExtensionDep::Path { name, path } => {
                let resolved = if path.starts_with(".") || path.starts_with("..") {
                    workspace_root.join(path).to_string_lossy().replace('\\', "/")
                } else {
                    path.replace('\\', "/")
                };
                deps_lines.push_str(&format!("{} = {{ path = \"{}\" }}\n", name, resolved));
            }
            ExtensionDep::Git { name, url, reference } => {
                if let Some(ref r) = reference {
                    let key = if r.contains('/') { "rev" } else if r.chars().all(|c| c.is_ascii_digit() || c == '.') { "tag" } else { "branch" };
                    deps_lines.push_str(&format!("{} = {{ git = \"{}\", {} = \"{}\" }}\n", name, url, key, r));
                } else {
                    deps_lines.push_str(&format!("{} = {{ git = \"{}\" }}\n", name, url));
                }
            }
        }
    }

    format!(
        r#"[package]
name = "tropel-custom"
version = "0.1.0"
edition = "2021"

[dependencies]
{}

# Use mimalloc by default (matching standard tropel)
mimalloc = "0.1"
"#,
        deps_lines.trim_end()
    )
}

/// Generate the src/main.rs content for the temporary build crate.
///
/// The generated main.rs imports each extension crate so that their
/// `inventory::submit!` calls are compiled and linked into the binary.
/// It then delegates to `tropel_engine::cli::run_cli()` which provides
/// the full CLI (tropel run, tropel extensions, etc.).
fn generate_main_rs(config: &BuildConfig) -> String {
    let mut imports = String::new();

    // Import each user-specified extension so its inventory::submit!
    // registrations are linked into the binary. Extensions are direct
    // dependencies of the generated crate (added in Cargo.toml), so
    // `extern crate` is valid.
    //
    // Built-in adapters (postman, k6, har) are transitive deps via
    // tropel-engine — they don't need `extern crate` because the engine
    // already links them. inventory::submit! uses `#[used]` +
    // `#[link_section]` which prevents linker dead-stripping.
    for ext in &config.extensions {
        let name = match ext {
            ExtensionDep::Registry { name, .. } => name,
            ExtensionDep::Path { name, .. } => name,
            ExtensionDep::Git { name, .. } => name,
        };
        let import_name = name.replace('-', "_");
        imports.push_str(&format!("extern crate {};\n", import_name));
    }

    format!(
        r#"//! Custom Tropel binary — built with `tropel build`
//! This file is auto-generated. Do not edit manually.

{imports}

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
    // Delegate to the shared CLI entry point.
    tropel_engine::cli::run_cli().await?;
    Ok(())
}}
"#,
        imports = imports.trim_end(),
    )
}
