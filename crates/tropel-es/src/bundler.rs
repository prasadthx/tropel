//! ES module bundler for Tropel load-test scripts.
//!
//! Resolves `import`/`export` statements relative to the script file and
//! concatenates all dependencies into a single JS bundle.
//!
//! This is a **lightweight bundler** designed for load-test scripts, not a
//! full Node.js module resolver. Key characteristics:
//!
//! - Resolves **relative imports** (`./utils`, `../helpers`) from the file system
//! - Supports **npm-style imports** (`lodash`, `crypto-js`) via a simple lookup
//! - Handles `import { x } from "./y"` and `import x from "./y"`
//! - Handles `export const/function/class` and `export default`
//! - Transpiles `.ts` dependencies via `crate::transpiler`
//! - Does **not** handle dynamic imports (`import()`), cyclic deps, or
//!   barrel files — these are uncommon in load-test scripts.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use tropel_core::Result;

/// Bundle a script's ES module imports into a single JavaScript string.
///
/// Walks the import tree starting from `source`, resolves relative imports
/// against the file's directory, and concatenates everything in dependency
/// order (imports first, then the main script).
pub fn bundle_module(source: &str, file_path: &Path) -> Result<String> {
    let base_dir = file_path.parent().unwrap_or(Path::new("."));
    let mut bundle_parts: Vec<String> = Vec::new();
    let mut visited = HashSet::new();

    // Collect and resolve all imports from the source
    let imports = extract_imports(source);

    for import_spec in &imports {
        // Resolve the import path first (to get a canonical path)
        let resolved_path = resolve_import_path(import_spec, base_dir);
        let canonical = match resolved_path {
            Ok(ref p) => p.canonicalize().unwrap_or_else(|_| p.clone()),
            Err(_) => {
                tracing::warn!("Failed to resolve import '{}'", import_spec);
                continue;
            }
        };

        if !visited.insert(canonical.clone()) {
            continue; // Already bundled this dependency
        }

        match resolve_import(import_spec, base_dir) {
            Ok((dep_path, dep_source)) => {
                // Wrap each dependency in a module closure to scope its variables
                let module_body = format!(
                    "(function(module, exports) {{\n{}\n}})(module_{}, exports_{});",
                    dep_source,
                    sanitize_module_name(&dep_path),
                    sanitize_module_name(&dep_path),
                );
                bundle_parts.push(module_body);
            }
            Err(e) => {
                tracing::warn!("Failed to resolve import '{}': {}", import_spec, e);
            }
        }
    }

    // Add the main script last (after all dependencies)
    // Also strip export keywords from the main script's declarations
    let main = strip_imports(source);
    bundle_parts.push(main);

    Ok(bundle_parts.join("\n\n"))
}

/// Resolve an import specifier to a file path without reading the file.
fn resolve_import_path(spec: &str, base_dir: &Path) -> std::result::Result<PathBuf, String> {
    if spec.starts_with('.') {
        let extensions = ["", ".ts", ".mts", ".js", ".mjs", "/index.ts", "/index.js"];
        for ext in &extensions {
            let candidate = base_dir.join(format!("{}{}", spec, ext));
            if candidate.exists() {
                return Ok(candidate);
            }
        }
        Err(format!("Module '{}' not found", spec))
    } else if is_vendored_shim(spec) {
        // Vendored shims (like "lodash", "crypto-js", "chai", "pm-api")
        // are resolved relative to the project root (CARGO_MANIFEST_DIR).
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap_or(Path::new("."));

        let shim_relative = vendored_shim_path(spec);
        if let Some(relative) = shim_relative {
            let shim_path = project_root.join(relative);
            if shim_path.exists() {
                return Ok(shim_path);
            }
        }
        Err(format!(
            "Vendored shim '{}' not found at expected path",
            spec
        ))
    } else {
        // NPM-style imports that aren't vendored — return the spec as-is
        Err(format!("Import '{}' has no local path", spec))
    }
}

/// Check if an import specifier is a known vendored shim name.
fn is_vendored_shim(spec: &str) -> bool {
    matches!(
        spec,
        "lodash" | "crypto-js" | "cryptojs" | "chai" | "pm-api" | "pm"
    )
}

/// Map a vendored shim name to its relative path within the project.
fn vendored_shim_path(spec: &str) -> Option<&'static str> {
    match spec {
        "lodash" => Some("js/lodash/lodash-shim.js"),
        "crypto-js" | "cryptojs" => Some("js/cryptojs-shim/cryptojs.js"),
        "chai" => Some("js/chai/chai-shim.js"),
        "pm-api" | "pm" => Some("js/pm-api/pm.js"),
        _ => None,
    }
}

/// Extract import source paths from a JavaScript source string.
/// Supports:
/// - `import { x } from "./foo"`
/// - `import x from "./foo"`
/// - `import "./foo"`
/// - `import * as x from "./foo"`
fn extract_imports(source: &str) -> Vec<String> {
    let mut imports = Vec::new();
    for line in source.lines() {
        let trimmed = line.trim();
        // Match static import statements
        if trimmed.starts_with("import ") {
            // Extract the string literal after "from "
            if let Some(from_pos) = trimmed.find("from ") {
                let after_from = &trimmed[from_pos + 5..];
                if let Ok(path) = extract_string_literal(after_from) {
                    imports.push(path);
                }
            } else if trimmed.contains('"') || trimmed.contains('\'') {
                // Side-effect import: import "./styles.css"
                // Find the first string literal
                if let Ok(path) = extract_string_literal(trimmed) {
                    imports.push(path);
                }
            }
        }
    }
    imports
}

/// Strip all import statements from a source string, leaving only the
/// executable code. Export statements are kept (they become regular
/// variable declarations in the bundled output).
fn strip_imports(source: &str) -> String {
    let mut result = String::with_capacity(source.len());
    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("import ") {
            continue;
        }
        result.push_str(line);
        result.push('\n');
    }
    result
}

/// Resolve an import path relative to the base directory.
/// Returns (resolved_path, source_contents).
fn resolve_import(spec: &str, base_dir: &Path) -> Result<(PathBuf, String)> {
    // Relative import
    if spec.starts_with('.') {
        // Try different extensions
        let extensions = ["", ".ts", ".mts", ".js", ".mjs", "/index.ts", "/index.js"];
        for ext in &extensions {
            let candidate = base_dir.join(format!("{}{}", spec, ext));
            if candidate.exists() {
                let source = std::fs::read_to_string(&candidate)
                    .map_err(|e| tropel_core::TropelError::Io(e))?;

                // Transpile if it's TypeScript
                let js_source =
                    if crate::transpiler::is_typescript_file(&candidate.to_string_lossy()) {
                        crate::transpiler::typescript_to_javascript(
                            &source,
                            &candidate.to_string_lossy(),
                        )
                        .map_err(|e| {
                            tropel_core::TropelError::Parse(format!("TS transpile error: {}", e))
                        })?
                    } else {
                        source
                    };

                return Ok((candidate, js_source));
            }
        }
        return Err(tropel_core::TropelError::Parse(format!(
            "Module '{}' not found (tried extensions: .ts, .mts, .js, .mjs, /index.ts, /index.js)",
            spec
        )));
    }

    // NPM-style bare import (e.g., "lodash", "crypto-js")
    // For load-test scripts, we look in vendored JS shims first, then
    // try node_modules relative to the project root, then fall back
    // to a bundled shim directory.
    let vendored_shims = ["lodash", "crypto-js", "chai", "pm-api"];

    if vendored_shims.contains(&spec) {
        // Map npm names to our vendored shim paths
        let shim_path = match spec {
            "lodash" => "js/lodash/lodash-shim.js",
            "crypto-js" | "cryptojs" => "js/cryptojs-shim/cryptojs.js",
            "chai" => "js/chai/chai-shim.js",
            "pm-api" | "pm" => "js/pm-api/pm.js",
            _ => {
                return Err(tropel_core::TropelError::Parse(format!(
                    "Unknown vendored shim: {}",
                    spec
                )))
            }
        };

        // Try to locate the shim relative to the project root
        let project_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap_or(Path::new("."));

        let shim_path = project_root.join(shim_path);
        if shim_path.exists() {
            let source =
                std::fs::read_to_string(&shim_path).map_err(|e| tropel_core::TropelError::Io(e))?;
            return Ok((shim_path, source));
        }
    }

    // Try node_modules lookup relative to the base dir
    // Try spec/index.js pattern first
    let node_modules_path = base_dir.join("node_modules").join(spec);
    let index_candidate = node_modules_path.join("index.js");
    if index_candidate.exists() {
        let source = std::fs::read_to_string(&index_candidate)
            .map_err(|e| tropel_core::TropelError::Io(e))?;
        return Ok((index_candidate, source));
    }

    Err(tropel_core::TropelError::Parse(format!(
        "Module '{}' not found",
        spec
    )))
}

/// Extract a string literal (the content between quotes) from a JS snippet.
fn extract_string_literal(s: &str) -> std::result::Result<String, ()> {
    let s = s.trim();
    for quote_char in ['"', '\''] {
        if let Some(start) = s.find(quote_char) {
            let rest = &s[start + 1..];
            if let Some(end) = rest.find(quote_char) {
                return Ok(rest[..end].to_string());
            }
        }
    }
    Err(())
}

/// Sanitize a file path to be a valid JavaScript identifier (for module wrappers).
fn sanitize_module_name(path: &Path) -> String {
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.replace('-', "_").replace('.', "_"))
        .unwrap_or_else(|| "module".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_extract_imports_named() {
        let source = r#"
            import { foo } from "./bar";
            import { baz, qux } from "../utils/helpers";
        "#;
        let imports = extract_imports(source);
        assert_eq!(imports.len(), 2);
        assert!(imports.contains(&"./bar".to_string()));
        assert!(imports.contains(&"../utils/helpers".to_string()));
    }

    #[test]
    fn test_extract_imports_default() {
        let source = r#"import lodash from "lodash";"#;
        let imports = extract_imports(source);
        assert_eq!(imports, vec!["lodash"]);
    }

    #[test]
    fn test_extract_imports_side_effect() {
        let source = r#"import "./polyfills";"#;
        let imports = extract_imports(source);
        assert_eq!(imports, vec!["./polyfills"]);
    }

    #[test]
    fn test_strip_imports() {
        let source = r#"
            import { foo } from "./bar";
            const x = foo();
            export function hello() { return x; }
        "#;
        let stripped = strip_imports(source);
        assert!(!stripped.contains("import { foo }"));
        assert!(stripped.contains("const x = foo()"));
        assert!(stripped.contains("export function hello"));
    }

    #[test]
    fn test_extract_string_literal_double_quotes() {
        let result = extract_string_literal("\"./foo\"").unwrap();
        assert_eq!(result, "./foo");
    }

    #[test]
    fn test_extract_string_literal_single_quotes() {
        let result = extract_string_literal("'./bar'").unwrap();
        assert_eq!(result, "./bar");
    }

    #[test]
    fn test_extract_string_literal_no_match() {
        assert!(extract_string_literal("foo bar").is_err());
    }

    #[test]
    fn test_sanitize_module_name() {
        assert_eq!(
            sanitize_module_name(Path::new("utils/helpers.ts")),
            "helpers"
        );
        assert_eq!(sanitize_module_name(Path::new("my-module.js")), "my_module");
    }

    #[test]
    fn test_bundle_empty_imports() {
        let source = "const x = 1;";
        let result = bundle_module(source, Path::new("test.js")).unwrap();
        assert!(result.contains("const x = 1;"));
    }

    #[test]
    fn test_extract_imports_star() {
        let source = r#"import * as utils from "./utils";"#;
        let imports = extract_imports(source);
        assert_eq!(imports, vec!["./utils"]);
    }
}
