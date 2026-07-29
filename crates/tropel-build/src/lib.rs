//! # tropel-build
//!
//! xk6-style custom-binary builder tool.
//! Takes a list of extension crates (git/crates.io/path) and generates
//! a thin binary crate that depends on `tropel-engine` + those extensions.
//! Then runs `cargo build --release` to produce a custom `tropel` binary.

use tropel_core::Result;

/// Configuration for building a custom Tropel binary.
pub struct BuildConfig {
    /// Extension dependencies (e.g. "tropel-x-grpc", "./my-local-ext").
    pub extensions: Vec<String>,
    /// Output binary path.
    pub output: String,
    /// Whether to build in release mode.
    pub release: bool,
}

impl Default for BuildConfig {
    fn default() -> Self {
        Self {
            extensions: vec![],
            output: "./tropel".to_string(),
            release: true,
        }
    }
}

/// Build a custom Tropel binary with the given extensions.
pub fn build(config: &BuildConfig) -> Result<()> {
    tracing::info!("Building custom Tropel binary with extensions: {:?}", config.extensions);

    // TODO: Generate a thin binary crate that imports the selected extensions
    // and then run cargo build.

    // For now, just print what would be done
    println!("Tropel build v{}", env!("CARGO_PKG_VERSION"));
    println!("Extensions: {:?}", config.extensions);
    println!("Output: {}", config.output);
    println!("Release: {}", config.release);
    println!();
    println!("The custom binary builder will be fully implemented in a future milestone.");
    println!("For now, use `cargo build --release` to build the standard binary.");

    Ok(())
}
