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

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;
use regex::Regex;
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
                    write!(f, "{} = {{ git = \"{}\", {} = \"{}\" }}", name, url, git_ref_key(r), r)
                } else {
                    write!(f, "{} = {{ git = \"{}\" }}", name, url)
                }
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════
// Validation — every user-supplied value is injected into the generated
// Cargo.toml / main.rs, so it MUST be validated first (build-time code
// injection otherwise).
// ══════════════════════════════════════════════════════════════════

/// Crate names are injected into `extern crate {name};` and used as the
/// dependency key, so they must be plain identifiers: `^[a-zA-Z0-9_-]+$`.
/// Beyond the charset we also reject names that would generate an
/// uncompilable `extern crate` line: leading digits (`9lives` is a syntax
/// error as a bare identifier) and the reserved words that cannot name a
/// crate (`crate`, `self`, `super`, `Self`, `_`).
fn valid_crate_name(name: &str) -> bool {
    if name.is_empty() || name.starts_with(|c: char| c.is_ascii_digit()) {
        return false;
    }
    match name {
        "crate" | "self" | "super" | "Self" | "_" => return false,
        _ => {}
    }
    name_re().is_match(name)
}

fn name_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"^[a-zA-Z0-9_-]+$").expect("valid crate-name regex"))
}

/// Version requirements are injected into `name = "{version}"`, so they
/// must be a plausible Cargo version requirement with no TOML-breaking
/// characters. Accepts exact versions, `*`, `^`/`~` prefaces, comparison
/// ranges, prerelease/build metadata, and wildcards — but never `"`, `\`,
/// braces, brackets, or control characters.
fn valid_version(version: &str) -> bool {
    !version.is_empty()
        && !version.chars().any(|c| c.is_control())
        && (version == "*"
            || (version_re().is_match(version)
                && version.chars().any(|c| c.is_ascii_digit())))
}

fn version_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"^[0-9A-Za-z.*^~<>=,\s+-]+$").expect("valid version-requirement regex")
    })
}

/// A value that lands inside a TOML basic string must not contain `"`, `\`,
/// or any control character (newline breaks the string, `\` escapes out).
fn valid_toml_string(s: &str) -> bool {
    !s.is_empty()
        && !s.chars().any(|c| c == '"' || c == '\\' || c.is_control())
}

/// Classify a git reference for the generated `Cargo.toml` key:
/// - hex SHA (>=7 chars, e.g. `deadbeef`) → `rev`
/// - semver-ish tag (`v1.2.3`, `1.2.3`, `1.2.3-rc.1`) → `tag`
/// - anything else (incl. branch names with `/`) → `branch`
fn git_ref_key(reference: &str) -> &'static str {
    if reference.len() >= 7 && reference.chars().all(|c| c.is_ascii_hexdigit()) {
        "rev"
    } else if reference
        .strip_prefix('v')
        .unwrap_or(reference)
        .split(['-', '+', '.'])
        .next()
        .map(|head| !head.is_empty() && head.chars().all(|c| c.is_ascii_digit()))
        .unwrap_or(false)
    {
        "tag"
    } else {
        "branch"
    }
}

/// Validate a single extension dependency spec. Returns an error naming the
/// offending field instead of silently emitting an injectable `Cargo.toml`.
pub fn validate_extension(ext: &ExtensionDep) -> Result<()> {
    match ext {
        ExtensionDep::Registry { name, version } => {
            if !valid_crate_name(name) {
                return Err(TropelError::Other(format!(
                    "Invalid registry crate name '{name}': must be a valid Rust crate identifier (^[a-zA-Z][a-zA-Z0-9_-]*$, no leading digit, not a reserved word like crate/self/super)"
                )));
            }
            if !valid_version(version) {
                return Err(TropelError::Other(format!(
                    "Invalid version requirement '{version}' for '{name}': expected a Cargo version (e.g. \"1.2.3\", \"^1.2\", \"*\")"
                )));
            }
        }
        ExtensionDep::Path { name, path } => {
            if !valid_crate_name(name) {
                return Err(TropelError::Other(format!(
                    "Invalid path crate name '{name}': must be a valid Rust crate identifier (^[a-zA-Z][a-zA-Z0-9_-]*$, no leading digit, not a reserved word like crate/self/super)"
                )));
            }
            if !valid_toml_string(path) {
                return Err(TropelError::Other(format!(
                    "Invalid path '{path}': must not contain quotes, backslashes, or control characters"
                )));
            }
        }
        ExtensionDep::Git { name, url, reference } => {
            if !valid_crate_name(name) {
                return Err(TropelError::Other(format!(
                    "Invalid git crate name '{name}': must be a valid Rust crate identifier (^[a-zA-Z][a-zA-Z0-9_-]*$, no leading digit, not a reserved word like crate/self/super)"
                )));
            }
            if !valid_toml_string(url) {
                return Err(TropelError::Other(format!(
                    "Invalid git URL '{url}': must not contain quotes, backslashes, or control characters"
                )));
            }
            if let Some(r) = reference {
                if !valid_toml_string(r) {
                    return Err(TropelError::Other(format!(
                        "Invalid git reference '{r}': must not contain quotes, backslashes, or control characters"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Validate every extension before generating any file.
pub fn validate_extensions(extensions: &[ExtensionDep]) -> Result<()> {
    for ext in extensions {
        validate_extension(ext)?;
    }
    Ok(())
}

// ══════════════════════════════════════════════════════════════════
// `--with` spec parsing — lets the CLI express versions, paths, and git
// refs (branch / tag / rev) instead of hardcoding "0.1" / default branch.
// ══════════════════════════════════════════════════════════════════

/// Parse a `--with <spec>` argument into an [`ExtensionDep`].
///
/// Accepted forms:
/// - `name` or `name@1.2.3` — crates.io (version defaults to `*` = latest)
/// - `./rel` / `/abs` / `~` / `C:\...` — path dependency
/// - `https://host/user/repo` / `git@host:user/repo.git` — git (default branch)
/// - `git-url@main` / `git-url@v1.2.3` / `git-url@<sha>` — git with a branch,
///   tag, or rev (classified automatically)
/// - `git-url#feature/foo` — git with an explicit ref via cargo's fragment
///   syntax; the ONLY form that supports refs containing `/`
pub fn parse_dep_spec(spec: &str) -> Result<ExtensionDep> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err(TropelError::Other("Empty --with spec".into()));
    }

    let dep = if spec.starts_with("http://")
        || spec.starts_with("https://")
        || spec.starts_with("git://")
        || spec.starts_with("ssh://")
        || spec.starts_with("git@")
    {
        split_git_spec(spec)
    } else if spec.starts_with('.') || spec.starts_with('/') || spec.starts_with('~') || is_drive_path(spec) {
        // Normalize Windows backslashes to forward slashes BEFORE validation:
        // the generated Cargo.toml does the same normalization, so rejecting
        // the raw `\` would needlessly break `--with .\my-ext` on Windows.
        // The validated, stored path is always forward-slashed.
        let path = spec.replace('\\', "/");
        let name = Path::new(&path)
            .file_stem()
            .and_then(|n| n.to_str())
            .unwrap_or("ext")
            .to_string();
        ExtensionDep::Path { name, path }
    } else {
        // crates.io: name or name@version
        let (name, version) = match spec.rsplit_once('@') {
            Some((n, v)) if !n.is_empty() && !v.is_empty() => (n, v),
            _ => (spec, "*"),
        };
        ExtensionDep::Registry {
            name: name.to_string(),
            version: version.to_string(),
        }
    };

    validate_extension(&dep)?;
    Ok(dep)
}

/// True if `spec` is a Windows drive-absolute path (`C:\...`, `C:/...`).
/// Drive paths are otherwise mistaken for registry names (they don't start
/// with `.`/`/`/`~`), and the trailing backslash fails `valid_crate_name`
/// with a misleading "invalid crate name" error.
fn is_drive_path(spec: &str) -> bool {
    let b = spec.as_bytes();
    b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':' && (b.get(2) == Some(&b'/') || b.get(2) == Some(&b'\\'))
}

/// Split a git URL spec into (url, optional reference).
///
/// Two unambiguous forms are supported, in priority order:
///
/// 1. **`url#ref`** (cargo's `git+https://…#ref` fragment syntax) — the last
///    `#` splits URL from reference. This is the ONLY form that can express
///    refs containing `/` (e.g. `feature/foo`), which the `@` form can't
///    disambiguate from a URL path segment.
/// 2. **`url@ref`** — the last `@` that sits in the URL's *path* portion
///    (after the last `/`). This ignores userinfo `@`s like
///    `https://TOKEN@github.com/u/r` and the SSH `git@host` transport prefix,
///    while still parsing `https://…/r@v1.2.3`, `ssh://git@…/r@main`, and
///    `git@host:u/r@main` correctly.
fn split_git_spec(spec: &str) -> ExtensionDep {
    // Form 1: url#ref — split on the last '#'. Unambiguous for any ref.
    if let Some(hash) = spec.rfind('#') {
        let (url, r) = spec.split_at(hash);
        if !url.is_empty() && !r.is_empty() {
            return git_dep(url, Some(r.trim_start_matches('#').to_string()));
        }
    }

    // Form 2: url@ref — strip the SSH `git@` transport prefix so its '@' is
    // not mistaken for a ref separator; re-add it when reconstructing the URL.
    let body = spec.strip_prefix("git@").unwrap_or(spec);
    let (url, reference) = match body.rfind('@') {
        Some(idx) => {
            let path_start = body.rfind('/').map_or(0, |i| i + 1);
            if idx >= path_start {
                let (u, r) = body.split_at(idx);
                let url = if spec.starts_with("git@") {
                    format!("git@{}", u)
                } else {
                    u.to_string()
                };
                (url, Some(r.trim_start_matches('@').to_string()))
            } else {
                (spec.to_string(), None)
            }
        }
        None => (spec.to_string(), None),
    };

    git_dep(&url, reference)
}

/// Build a [`ExtensionDep::Git`] from a URL + optional ref, deriving the
/// crate name from the URL's last path segment (stripping `.git`).
fn git_dep(url: &str, reference: Option<String>) -> ExtensionDep {
    let name = url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("ext")
        .trim_end_matches(".git")
        .to_string();
    ExtensionDep::Git {
        name,
        url: url.to_string(),
        reference,
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
    // Validate every extension BEFORE generating any file: names/versions/
    // URLs are injected into Cargo.toml and main.rs, so an unvalidated
    // value is build-time code injection.
    validate_extensions(&config.extensions)?;

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
                if let Some(r) = reference {
                    deps_lines.push_str(&format!(
                        "{} = {{ git = \"{}\", {} = \"{}\" }}\n",
                        name, url, git_ref_key(r), r
                    ));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_crate_names_accepted() {
        for name in ["tropel-x-grpc", "tropel_x_websocket", "Ext123", "a", "A_1-B"] {
            assert!(valid_crate_name(name), "should accept '{name}'");
        }
    }

    #[test]
    fn invalid_crate_names_rejected() {
        for name in [
            "",
            "a b",
            "a\"b",
            "a\nb",
            "{evil}",
            "a=b",
            "..",
            ".hidden",
            "x.y",
            "foo/bar",
            "foo:bar",
            "a=b}; path=\"/tmp/x\"",
            // Would generate uncompilable `extern crate {name};` lines:
            "9lives",
            "crate",
            "self",
            "super",
            "Self",
            "_",
            "0x-bad",
        ] {
            assert!(!valid_crate_name(name), "should reject '{name}'");
        }
    }

    #[test]
    fn parse_windows_drive_path_specs() {
        // Absolute Windows drive paths must route to the Path branch (they
        // start with neither `.`/`/`/`~` nor a git scheme) and get the
        // backslash → forward-slash normalization.
        for (spec, expected_path, expected_name) in [
            (r"C:\my-ext", "C:/my-ext", "my-ext"),
            (r"D:\extensions\grpc", "D:/extensions/grpc", "grpc"),
            ("C:/my-ext", "C:/my-ext", "my-ext"),
        ] {
            let dep = parse_dep_spec(spec).unwrap();
            match dep {
                ExtensionDep::Path { name, path } => {
                    assert_eq!(name, expected_name, "crate name of '{spec}'");
                    assert_eq!(path, expected_path);
                }
                _ => panic!("expected Path for '{spec}'"),
            }
        }
    }

    #[test]
    fn parse_git_hash_ref_supports_slash_branches() {
        // cargo's `url#ref` fragment syntax is the only form that can
        // express refs containing '/', which `@`-heuristics mis-parse.
        let dep = parse_dep_spec("https://github.com/user/repo#feature/foo").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "https://github.com/user/repo");
                assert_eq!(reference.as_deref(), Some("feature/foo"));
                assert_eq!(git_ref_key(reference.as_deref().unwrap()), "branch");
            }
            _ => panic!("expected Git"),
        }

        // Also works with tag / scp-style ssh URLs
        let dep = parse_dep_spec("git@github.com:user/repo.git#v2.0.0").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "git@github.com:user/repo.git");
                assert_eq!(reference.as_deref(), Some("v2.0.0"));
                assert_eq!(git_ref_key(reference.as_deref().unwrap()), "tag");
            }
            _ => panic!("expected Git"),
        }
    }

    #[test]
    fn digit_and_keyword_crate_names_rejected_by_parse() {
        // `crate`/`self`/`super`/`Self` and digit-leading names pass the
        // charset regex but would emit `extern crate {name};` that fails to
        // compile in the generated main.rs — must be rejected at --with time.
        for spec in ["9lives", "crate", "self", "super", "Self"] {
            assert!(
                parse_dep_spec(spec).is_err(),
                "should reject '{spec}'"
            );
        }
        // ...but a legit registry crate with digits mid-name is still fine.
        assert!(parse_dep_spec("tropel-x-grpc2").is_ok());
    }

    #[test]
    fn versions_accepted() {
        for v in [
            "0.1", "1.2.3", "^1.2", "~0.1.0", "*", ">=1.0, <2.0", "1.2.3-alpha.1",
            "1.2.3+build.5", "0.1.0-rc1", "2", "1.*", "^1.2.3-alpha", "latest2",
        ] {
            assert!(valid_version(v), "should accept '{v}'");
        }
    }

    #[test]
    fn versions_rejected() {
        for v in ["", "abc", "1.2\"3", "1.2\n3", "\"; malicious = true", "{1}", "a\tb"] {
            assert!(!valid_version(v), "should reject '{v}'");
        }
    }

    #[test]
    fn git_ref_key_classification() {
        assert_eq!(git_ref_key("deadbeef"), "rev");
        assert_eq!(git_ref_key("0123456789abcdef0123456789abcdef01234567"), "rev");
        assert_eq!(git_ref_key("v1.2.3"), "tag");
        assert_eq!(git_ref_key("1.2.3"), "tag");
        assert_eq!(git_ref_key("1.2.3-rc.1"), "tag");
        assert_eq!(git_ref_key("main"), "branch");
        assert_eq!(git_ref_key("feature/x"), "branch");
        // A branch that happens to be short hex still works as branch
        assert_eq!(git_ref_key("abc"), "branch");
    }

    #[test]
    fn validate_rejects_injection_payloads() {
        let evil_registry = ExtensionDep::Registry {
            name: "tropel-x\"}; malicious = true".into(),
            version: "0.1".into(),
        };
        assert!(validate_extension(&evil_registry).is_err());

        let evil_path = ExtensionDep::Path {
            name: "ext".into(),
            path: "../evil\"; ransomware = true".into(),
        };
        assert!(validate_extension(&evil_path).is_err());

        let evil_git = ExtensionDep::Git {
            name: "ext".into(),
            url: "https://evil.com/x\"; x = { y = \"z\" }".into(),
            reference: None,
        };
        assert!(validate_extension(&evil_git).is_err());

        let evil_ref = ExtensionDep::Git {
            name: "ext".into(),
            url: "https://evil.com/x".into(),
            reference: Some("main\"}; pwned = true".into()),
        };
        assert!(validate_extension(&evil_ref).is_err());
    }

    #[test]
    fn parse_registry_specs() {
        let plain = parse_dep_spec("tropel-x-grpc").unwrap();
        match plain {
            ExtensionDep::Registry { name, version } => {
                assert_eq!(name, "tropel-x-grpc");
                assert_eq!(version, "*"); // no longer hardcoded "0.1"
            }
            _ => panic!("expected Registry"),
        }

        let pinned = parse_dep_spec("tropel-x-grpc@0.2.0").unwrap();
        match pinned {
            ExtensionDep::Registry { name, version } => {
                assert_eq!(name, "tropel-x-grpc");
                assert_eq!(version, "0.2.0");
            }
            _ => panic!("expected Registry"),
        }
    }

    #[test]
    fn parse_path_specs() {
        for (spec, expected_name) in [
            ("./my-ext", "my-ext"),
            ("../ext", "ext"),
            ("/abs/path/ext", "ext"),
            ("~/ext", "ext"),
        ] {
            let dep = parse_dep_spec(spec).unwrap();
            match dep {
                ExtensionDep::Path { name, path } => {
                    assert_eq!(name, expected_name, "file_stem of '{spec}'");
                    assert_eq!(path, spec);
                }
                _ => panic!("expected Path for '{spec}'"),
            }
        }
    }

    #[test]
    fn parse_windows_backslash_path_normalized() {
        // A Windows-style path must not be rejected (the `\` is normalized
        // to `/` before validation) and must be stored forward-slashed.
        let dep = parse_dep_spec(r".\my-ext").unwrap();
        match dep {
            ExtensionDep::Path { name, path } => {
                assert_eq!(name, "my-ext");
                assert_eq!(path, "./my-ext");
            }
            _ => panic!("expected Path"),
        }
    }

    #[test]
    fn parse_git_url_with_userinfo_keeps_full_url() {
        // `https://TOKEN@github.com/u/r` — the '@' is userinfo, not a ref.
        let dep = parse_dep_spec("https://TOKEN@github.com/user/repo").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "https://TOKEN@github.com/user/repo");
                assert!(reference.is_none());
            }
            _ => panic!("expected Git"),
        }
    }

    #[test]
    fn parse_ssh_url_with_userinfo_and_ref() {
        // ssh:// transport with a userinfo '@' AND a trailing ref
        let dep = parse_dep_spec("ssh://git@github.com/user/repo@main").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "ssh://git@github.com/user/repo");
                assert_eq!(reference.as_deref(), Some("main"));
            }
            _ => panic!("expected Git"),
        }
    }

    #[test]
    fn parse_git_specs_with_refs() {
        // default branch
        let dep = parse_dep_spec("https://github.com/user/repo").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "https://github.com/user/repo");
                assert!(reference.is_none());
            }
            _ => panic!("expected Git"),
        }

        // explicit tag
        let dep = parse_dep_spec("https://github.com/user/repo@v1.2.3").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "https://github.com/user/repo");
                assert_eq!(reference.as_deref(), Some("v1.2.3"));
                assert_eq!(git_ref_key(reference.as_deref().unwrap()), "tag");
            }
            _ => panic!("expected Git"),
        }

        // SSH url + branch ref
        let dep = parse_dep_spec("git@github.com:user/repo.git@main").unwrap();
        match &dep {
            ExtensionDep::Git { name, url, reference } => {
                assert_eq!(name, "repo");
                assert_eq!(url, "git@github.com:user/repo.git");
                assert_eq!(reference.as_deref(), Some("main"));
                assert_eq!(git_ref_key(reference.as_deref().unwrap()), "branch");
            }
            _ => panic!("expected Git"),
        }

        // SHA rev
        let dep = parse_dep_spec("https://github.com/user/repo@0123456789abcdef0123456789abcdef01234567").unwrap();
        match &dep {
            ExtensionDep::Git { reference, .. } => {
                assert_eq!(git_ref_key(reference.as_deref().unwrap()), "rev");
            }
            _ => panic!("expected Git"),
        }
    }

    #[test]
    fn parse_rejects_injection_specs() {
        for spec in [
            "tropel-x\"}; malicious = true",
            "tropel-x@1.0\"; x = y",
            "https://evil.com/x\"}; pwned = 1",
            "a=b; drop table",
        ] {
            assert!(parse_dep_spec(spec).is_err(), "should reject '{spec}'");
        }
    }

    #[test]
    fn empty_spec_rejected() {
        assert!(parse_dep_spec("").is_err());
        assert!(parse_dep_spec("   ").is_err());
    }

    #[test]
    fn debug_fmt_generates_toml_fragment() {
        let dep = ExtensionDep::Git {
            name: "repo".into(),
            url: "https://github.com/user/repo".into(),
            reference: Some("v1.2.3".into()),
        };
        assert_eq!(
            format!("{:?}", dep),
            "repo = { git = \"https://github.com/user/repo\", tag = \"v1.2.3\" }"
        );
    }
}
