//! # tropel-es
//!
//! TypeScript transpilation and ES module bundling for Tropel load-test scripts.
//!
//! Uses the **oxc** toolchain (real Rust-native parser/transformer/codegen,
//! no Node.js dependency) to:
//! - Strip TypeScript type annotations from `.ts` files → plain JS
//! - Bundle ES module `import`/`export` statements into a single script
//!
//! # Architecture
//!
//! At load time, before a script reaches the QuickJS runtime:
//!
//! 1. **File detection** — `.ts`/`.mts`/`.tsx` triggers TypeScript stripping.
//! 2. **Type stripping** — oxc parses, removes type annotations, codegens JS.
//! 3. **Module bundling** — `import`/`export` statements are resolved relative to
//!    the script file, each dependency is transpiled (if needed), and all are
//!    concatenated into a single JS bundle with local-scope module wrappers.
//!
//! The resulting JS text is passed to `tropel-js::JsContext` for evaluation.

pub mod bundler;
pub mod transpiler;

pub use bundler::*;
pub use transpiler::*;

use std::path::Path;
use tropel_core::Result;

/// Transpile a script file at the given path into plain JavaScript.
///
/// - `.ts` / `.mts` files are transpiled via SWC (strips types).
/// - `.js` / `.mjs` files are passed through as-is.
/// - ES import/export statements are bundled into a single script
///   (all dependencies resolved relative to the file).
pub fn transpile_file(path: &Path) -> Result<String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("js")
        .to_lowercase();

    // Read the source
    let source = std::fs::read_to_string(path).map_err(tropel_core::TropelError::Io)?;

    let is_typescript = matches!(ext.as_str(), "ts" | "mts" | "tsx");

    // First pass: strip TypeScript types if needed
    let js_source = if is_typescript {
        transpiler::typescript_to_javascript(&source, &path.to_string_lossy())
            .map_err(|e| tropel_core::TropelError::Parse(format!("TS transpile error: {}", e)))?
    } else {
        source
    };

    // Second pass: bundle ES module imports into a single script
    if has_import_or_export(&js_source) {
        bundler::bundle_module(&js_source, path)
    } else {
        Ok(js_source)
    }
}

/// Check if a source string contains import or export statements.
fn has_import_or_export(source: &str) -> bool {
    source.contains("import ") || source.contains("export ")
}
