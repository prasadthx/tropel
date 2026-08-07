//! TypeScript → JavaScript transpilation for load-test scripts.
//!
//! Uses the **oxc** toolchain (real parser + transformer + codegen, all pure
//! Rust, no Node.js dependency) to strip TypeScript type annotations. This
//! replaces the earlier regex-based approach, which broke on valid TS/ESM:
//! nested braces in types, comma-separated generics, bare `as` assertions,
//! arrow-function generics, and strings containing type-looking text.
//!
//! The pipeline:
//!
//! 1. **Parse** with `oxc_parser` (TypeScript + module mode so `import`/
//!    `export` are legal).
//! 2. **Transform** with `oxc_transformer`'s TypeScript pass — removes
//!    interfaces, type aliases, param/return/variable annotations, generics,
//!    `as` casts, `import type`, and lowers `enum` to runtime JS. Legacy
//!    (`experimentalDecorators`) decorators are ALSO lowered — see
//!    [`decorator_options`]. Exports are preserved.
//! 3. **Codegen** with `oxc_codegen` to plain JavaScript.
//! 4. If the transform emitted `babelHelpers.*` calls (decorator lowering),
//!    prepend a minimal [`BABEL_HELPERS_SHIM`] so the output runs standalone
//!    in QuickJS.
//! 5. Optionally strip `export` keywords (script-mode eval).
//!
//! Diagnostics are classified by severity: **recoverable** ones (oxc's
//! parser recovers and still produces a valid AST) are logged as warnings
//! and the pipeline continues; only **Error**-severity diagnostics or a
//! parser panic abort. This keeps the gate honest — a stray TS warning no
//! longer kills a script that oxc handles fine.
//!
//! Two public entry points mirror the old API:
//! - [`typescript_to_javascript`] — exports stripped (script-mode eval).
//! - [`typescript_to_javascript_keep_exports`] — exports preserved
//!   (module-mode eval, e.g. reading a k6 script's `export const options`).

use oxc_allocator::Allocator;
use oxc_ast::ast::Statement;
use oxc_codegen::Codegen;
use oxc_diagnostics::Severity;
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::{GetSpan, SourceType};
use oxc_transformer::{
    DecoratorOptions, HelperLoaderMode, HelperLoaderOptions, TransformOptions, Transformer,
};
use regex::Regex;

/// Transpile TypeScript source code to plain JavaScript.
/// Strips types via oxc, then removes `export` keywords (script-mode eval).
pub fn typescript_to_javascript(source: &str, filename: &str) -> anyhow::Result<String> {
    let js = transpile_typescript(source, filename)?;
    Ok(remove_exports(&js))
}

/// Transpile TypeScript source code to plain JavaScript, **keeping** the
/// `export` modifiers intact.
///
/// The regular `typescript_to_javascript` strips `export` keywords so the
/// output can be eval'd in script mode (QuickJS rejects `export` outside a
/// module). Callers that want to evaluate the transpiled source as an ES
/// module — e.g. to read k6's `export const options` — must keep the exports,
/// so this variant skips the `remove_exports` pass.
pub fn typescript_to_javascript_keep_exports(
    source: &str,
    filename: &str,
) -> anyhow::Result<String> {
    transpile_typescript(source, filename)
}

/// The shared oxc pipeline: parse → transform (strip TS + lower decorators)
/// → codegen → prepend the decorator helper shim when needed.
fn transpile_typescript(source: &str, filename: &str) -> anyhow::Result<String> {
    let allocator = Allocator::default();

    // SourceType: honor a real .ts/.mts/.tsx path, otherwise force TypeScript
    // + module mode (covers the k6 heuristic path which passes a fake
    // "script.js" filename for content-detected TS).
    let source_type = match SourceType::from_path(filename) {
        Ok(st) if st.is_typescript() => st.with_module(true),
        _ => SourceType::default()
            .with_typescript(true)
            .with_module(true),
    };

    let parser_return = Parser::new(&allocator, source, source_type).parse();
    if parser_return.panicked {
        return Err(anyhow::anyhow!(
            "TypeScript parse failed: {}",
            format_diagnostics(&parser_return.errors)
        ));
    }
    // Recoverable diagnostics: oxc recovers and still yields a valid AST.
    // The old gate aborted on ANY diagnostic (even a warning), which killed
    // scripts oxc handles fine — warn and continue instead, aborting only on
    // genuine Error-severity diagnostics.
    if let Some(err) = parser_return
        .errors
        .iter()
        .find(|d| d.severity == Severity::Error)
    {
        return Err(anyhow::anyhow!(
            "TypeScript parse error: {}",
            format_diagnostics(&[err.clone()])
        ));
    }
    for d in &parser_return.errors {
        tracing::warn!("TypeScript parse diagnostic (recoverable): {d}");
    }

    let mut program = parser_return.program;

    // Build semantic scoping from the parsed program — the transformer's
    // traversal requires a populated `Scoping` (an empty default panics
    // inside oxc's walker).
    let semantic = SemanticBuilder::new().build(&program).semantic;
    let scoping = semantic.into_scoping();

    let options = decorator_options();
    let transformer = Transformer::new(&allocator, std::path::Path::new(filename), &options);
    let transform_return = transformer.build_with_scoping(scoping, &mut program);
    if let Some(err) = transform_return
        .errors
        .iter()
        .find(|d| d.severity == Severity::Error)
    {
        return Err(anyhow::anyhow!(
            "TypeScript transform error: {}",
            format_diagnostics(&[err.clone()])
        ));
    }
    for d in &transform_return.errors {
        tracing::warn!("TypeScript transform diagnostic (recoverable): {d}");
    }

    let codegen_return = Codegen::new().build(&program);
    let code = codegen_return.code;

    // Decorator lowering emits `babelHelpers.decorate(...)` /
    // `babelHelpers.decorateParam(...)` (External helper mode). QuickJS has no
    // such global, so prepend the minimal shim whenever the output references
    // it. Non-decorated output is untouched.
    let code = if code.contains("babelHelpers.") {
        format!("{BABEL_HELPERS_SHIM}\n{code}")
    } else {
        code
    };

    Ok(code)
}

/// Transform options: strip TypeScript AND lower legacy (`experimentalDecorators`)
/// decorators so QuickJS can eval the output. External helper mode makes oxc
/// emit `babelHelpers.decorate(...)` calls (no `@oxc-project/runtime` import,
/// which QuickJS can't resolve); the shim provides those helpers.
fn decorator_options() -> TransformOptions {
    TransformOptions {
        decorator: DecoratorOptions {
            legacy: true,
            emit_decorator_metadata: false,
        },
        helper_loader: HelperLoaderOptions {
            mode: HelperLoaderMode::External,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// Minimal `babelHelpers` shim for oxc's legacy-decorator lowering.
///
/// oxc emits the canonical TypeScript `__decorate`/`__param` pattern under
/// the `babelHelpers` namespace: class decorators call
/// `babelHelpers.decorate([...], Ctor)`, method/param decorators call
/// `babelHelpers.decorate([...], proto, "name", null)` with
/// `babelHelpers.decorateParam(i, fn)` embedded in the array. The
/// implementations below are the standard Babel/TS legacy helpers (behavior
/// verified against oxc 0.128 output).
const BABEL_HELPERS_SHIM: &str = r#"var babelHelpers = babelHelpers || {};
babelHelpers.decorate = function (decorators, target, key, desc) {
  var c = arguments.length, r = c < 3 ? target : desc === null ? desc = Object.getOwnPropertyDescriptor(target, key) : desc, d;
  if (typeof Reflect === "object" && typeof Reflect.decorate === "function") r = Reflect.decorate(decorators, target, key, desc);
  else for (var i = decorators.length - 1; i >= 0; i--) if (d = decorators[i]) r = (c < 3 ? d(r) : c > 3 ? d(target, key, r) : d(target, key)) || r;
  return c > 3 && r && Object.defineProperty(target, key, r), r;
};
babelHelpers.decorateParam = function (paramIndex, decorator) {
  return function (target, key) { decorator(target, key, paramIndex); };
};
"#;

/// Render oxc diagnostics to a single-line message (no ANSI).
fn format_diagnostics(diagnostics: &[oxc_diagnostics::OxcDiagnostic]) -> String {
    diagnostics
        .iter()
        .map(|d| format!("{}", d))
        .collect::<Vec<_>>()
        .join("; ")
}

// ---------------------------------------------------------------------------
// Export stripping (script mode)
//
// oxc strips the types first, so these regexes only ever run against clean
// JavaScript — the fragile TS constructs that poisoned the old regexes are
// already gone. The remaining job is purely removing real `export` keywords.
// ---------------------------------------------------------------------------

/// Remove `export` keywords from transpiled JS (script-mode eval).
fn remove_exports(s: &str) -> String {
    let mut result = s.to_string();

    // `export default function Name(...` → `function Name(...`
    // Requires a name after `function|class` — an *anonymous* default
    // (`export default class {`) must NOT match here: emitting `class {` as a
    // statement is a SyntaxError in script mode, so it falls through to re3's
    // `/* export default */` comment form instead. The name's first char is
    // captured and re-emitted (the regex crate has no lookahead, so matching
    // it naively would swallow the `F` of `Foo` → `class oo`).
    let re1 = Regex::new(r"\bexport\s+default\s+(function|class)\s+([A-Za-z_$])").unwrap();
    result = re1.replace_all(&result, "$1 $2").to_string();

    // `export default function(...` → `function(...` (anonymous default)
    let re2 = Regex::new(r"\bexport\s+default\s+(function|class)\s*\(").unwrap();
    result = re2.replace_all(&result, "$1(").to_string();

    // `export default X` → `/* export default */ X` (any other default)
    let re3 = Regex::new(r"\bexport\s+default\s+").unwrap();
    result = re3
        .replace_all(&result, "/* export default */ ")
        .to_string();

    // `export function Name(...` → `function Name(...`
    let re4 = Regex::new(r"\bexport\s+(async\s+)?function\b").unwrap();
    result = re4
        .replace_all(&result, |caps: &regex::Captures| {
            let async_prefix = caps.get(1).map(|m| m.as_str()).unwrap_or("");
            format!("{}function", async_prefix)
        })
        .to_string();

    // `export class Name` → `class Name`
    let re5 = Regex::new(r"\bexport\s+class\b").unwrap();
    result = re5.replace_all(&result, "class").to_string();

    // `export const/let/var` → `const/let/var`
    let re6 = Regex::new(r"\bexport\s+(const|let|var)\b").unwrap();
    result = re6.replace_all(&result, "$1").to_string();

    // `export { ... }` — named export block, comment it out
    let re7 = Regex::new(r"\bexport\s*\{[^}]*\}\s*;").unwrap();
    result = re7
        .replace_all(&result, "/* named exports stripped */")
        .to_string();

    // `export * from '...'` / `export { x } from '...'` — re-exports
    result = result
        .lines()
        .map(|line| {
            let trimmed = line.trim();
            if trimmed.starts_with("export ")
                && trimmed.contains(" from ")
                && (trimmed.contains('"') || trimmed.contains('\''))
            {
                if trimmed.starts_with("//") || trimmed.starts_with("/*") {
                    line.to_string()
                } else {
                    format!("// re-export stripped: {}", trimmed)
                }
            } else {
                line.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");

    result
}

/// Strip k6 virtual-module imports / re-exports from a module source using the
/// oxc AST (NOT regex).
///
/// k6 scripts import from virtual modules (`k6`, `k6/http`, `k6/metrics`, …)
/// that have no backing file on disk — the k6 shim provides those APIs as
/// globals. The old line-anchored regexes missed multi-line imports
/// (`import {\n check\n} from 'k6';`), trailing comments, and jslib URLs, so
/// those survived preprocessing, reached the module resolver, hard-errored,
/// and killed `init` before iteration 1 → zero metrics, exit 0.
///
/// This parses the source and splices out any top-level statement whose module
/// specifier is a k6 virtual module (`k6`, `k6/*`) or a remote URL
/// (`https://…`, e.g. `https://jslib.k6.io/…`), by its exact AST span — any
/// syntactic form is handled. Local imports (`./helpers.js`) and local
/// re-exports (`export { x } from "./helpers"`) are PRESERVED: the module
/// loader resolves those to files on disk.
///
/// On a parse failure the source is returned unchanged (never fail hard here —
/// the caller surfaces the real parse error from the eval path).
pub fn strip_k6_virtual_imports(source: &str) -> String {
    let allocator = Allocator::default();
    // Parse in module + TypeScript mode so imports/exports AND `.ts` sources
    // (the preprocessor runs before TS transpilation) both parse.
    let source_type = SourceType::default()
        .with_typescript(true)
        .with_module(true);
    let parser_return = Parser::new(&allocator, source, source_type).parse();
    if parser_return.panicked {
        return source.to_string();
    }
    let program = parser_return.program;

    // Collect byte spans of k6-virtual import / re-export statements, in
    // source order (AST body order == source order).
    let mut spans: Vec<(u32, u32)> = Vec::new();
    for stmt in &program.body {
        let module_specifier: Option<&str> = match stmt {
            Statement::ImportDeclaration(decl) => Some(decl.source.value.as_str()),
            Statement::ExportAllDeclaration(decl) => Some(decl.source.value.as_str()),
            Statement::ExportNamedDeclaration(decl) => {
                decl.source.as_ref().map(|s| s.value.as_str())
            }
            _ => None,
        };
        if let Some(spec) = module_specifier {
            if is_k6_virtual_specifier(spec) {
                let span = stmt.span();
                spans.push((span.start, span.end));
            }
        }
    }

    if spans.is_empty() {
        return source.to_string();
    }

    // Splice out the removed statements, preserving everything else
    // byte-for-byte (comments, spacing, all other statements).
    let mut result = String::with_capacity(source.len());
    let mut cursor = 0usize;
    for (start, end) in spans {
        result.push_str(&source[cursor..start as usize]);
        cursor = end as usize;
    }
    result.push_str(&source[cursor..]);
    result
}

/// Is this module specifier a k6 virtual module or a remote URL that cannot
/// resolve on disk? `k6` and `k6/<sub>` are shim-provided; `http(s)://…`
/// (jslib etc.) can't be fetched by the local module resolver.
fn is_k6_virtual_specifier(spec: &str) -> bool {
    spec == "k6"
        || spec.starts_with("k6/")
        || spec.starts_with("https://")
        || spec.starts_with("http://")
}

/// Check if a file path has a TypeScript extension.
pub fn is_typescript_file(path: &str) -> bool {
    let lower = path.to_lowercase();
    lower.ends_with(".ts") || lower.ends_with(".mts") || lower.ends_with(".tsx")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_function_param_types() {
        let ts = r#"
            function greet(name: string): string {
                return "Hello, " + name;
            }
        "#;
        let js = strip_types(ts);
        assert!(js.contains("function greet(name)"));
        assert!(js.contains("\"Hello, \" + name"));
        assert!(!js.contains(": string"));
    }

    #[test]
    fn test_strip_variable_type_annotations() {
        let ts = r#"
            const user: User = { id: 1, name: "Alice" };
            let count: number = 42;
        "#;
        let js = strip_types(ts);
        // oxc codegen may expand the object literal across lines
        assert!(
            js.contains("const user = {") || js.contains("const user = { id: 1"),
            "got: {js}"
        );
        assert!(js.contains("id: 1"), "got: {js}");
        assert!(js.contains("name: \"Alice\""), "got: {js}");
        assert!(js.contains("let count = 42"), "got: {js}");
        assert!(!js.contains(": User"));
        assert!(!js.contains(": number"));
    }

    #[test]
    fn test_strip_generics() {
        let ts = r#"
            function identity<T>(arg: T): T {
                return arg;
            }
            const result = identity<number>(42);
        "#;
        let js = strip_types(ts);
        assert!(js.contains("function identity(arg)"), "got: {js}");
        assert!(js.contains("return arg"));
        assert!(js.contains("const result = identity(42)"), "got: {js}");
    }

    #[test]
    fn test_strip_interfaces() {
        let ts = r#"
            interface User {
                id: number;
                name: string;
            }
            const user = { id: 1 };
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("interface User"), "got: {js}");
        assert!(js.contains("const user = { id: 1 }"));
    }

    #[test]
    fn test_strip_type_aliases() {
        let ts = r#"
            type MyString = string;
            const x: MyString = "hello";
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("type MyString"), "got: {js}");
        assert!(js.contains("const x = \"hello\""));
    }

    #[test]
    fn test_strip_import_type() {
        let ts = r#"import type { SomeType } from "./types";"#;
        let js = strip_types(ts);
        assert!(!js.contains("import type"), "got: {js}");
    }

    #[test]
    fn test_strip_as_casts() {
        let ts = r#"const x = (getValue() as SomeType);"#;
        let js = strip_types(ts);
        assert!(!js.contains("as SomeType"), "got: {js}");
    }

    #[test]
    fn test_pure_js_passthrough() {
        let js_input = r#"
            function greet(name) {
                return "Hello, " + name;
            }
        "#;
        let js = strip_types(js_input);
        assert!(js.contains("function greet(name)"));
        assert!(js.contains("\"Hello, \""));
    }

    #[test]
    fn test_strip_return_type() {
        let ts = r#"
            function add(a: number, b: number): number {
                return a + b;
            }
        "#;
        let js = strip_types(ts);
        assert!(
            js.contains("function add(a, b)"),
            "Expected 'function add(a, b)' in output, got: {}",
            js
        );
        assert!(js.contains("return a + b"));
    }

    #[test]
    fn test_strip_enum() {
        let ts = r#"
            enum Color {
                Red,
                Green,
                Blue,
            }
            const c = Color.Red;
        "#;
        let js = strip_types(ts);
        // oxc lowers enums to runtime JS (reverse mappings included)
        assert!(js.contains("Color"), "got: {js}");
        assert!(
            js.contains("c = Color.Red") || js.contains("Color.Red"),
            "got: {js}"
        );
        // `enum` keyword must be gone
        assert!(!js.contains("enum Color"), "got: {js}");
    }

    #[test]
    fn test_strip_exports() {
        let ts = r#"
            export default function() {
                return 42;
            }
            export function helper(x: string) {
                return x;
            }
            export const VERSION = 1;
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("export default function"), "got: {js}");
        assert!(!js.contains("export function helper"), "got: {js}");
        assert!(!js.contains("export const VERSION"), "got: {js}");
        assert!(js.contains("function() {"));
        assert!(js.contains("function helper(x)"));
        assert!(js.contains("const VERSION = 1"));
    }

    #[test]
    fn test_as_cast_safe() {
        // Verify `as` in English prose is preserved
        let js_like = r#"
            // This text should act as a fallback
            const x = getValue() as SomeType;
        "#;
        let js = strip_types(js_like);
        // The English "as" in the comment should be preserved
        assert!(js.contains("as a fallback"), "got: {js}");
        // The TS `as` cast in code should be removed
        assert!(!js.contains("as SomeType"), "got: {js}");
    }

    #[test]
    fn test_export_default_function() {
        let ts = r#"export default function() { return 42; }"#;
        let js = strip_types(ts);
        assert!(!js.contains("export default"), "got: {js}");
        // oxc puts the function body on its own line
        assert!(
            js.contains("function() {") || js.contains("function () {"),
            "got: {js}"
        );
        assert!(js.contains("return 42;"), "got: {js}");
    }

    #[test]
    fn test_export_named_function() {
        let ts = r#"export function foo() { return 1; }"#;
        let js = strip_types(ts);
        assert!(!js.contains("export"), "got: {js}");
        assert!(
            js.contains("function foo() {") || js.contains("function foo () {"),
            "got: {js}"
        );
        assert!(js.contains("return 1;"), "got: {js}");
    }

    #[test]
    fn test_export_named_block() {
        let ts = r#"const x = 1; export { x };"#;
        let js = strip_types(ts);
        assert!(!js.contains("export { x };"), "got: {js}");
        assert!(js.contains("const x = 1"));
    }

    #[test]
    fn test_enum_with_initializer() {
        let ts = r#"
            enum HttpStatus {
                OK = 200,
                NotFound = 404,
                ServerError = 500,
            }
        "#;
        let js = strip_types(ts);
        // oxc preserves explicit initializers in the runtime enum
        assert!(js.contains("OK"), "got: {js}");
        assert!(js.contains("200"), "got: {js}");
        assert!(js.contains("404"), "got: {js}");
        assert!(!js.contains("enum HttpStatus"), "got: {js}");
    }

    #[test]
    fn test_enum_mixed_initializers() {
        let ts = r#"
            enum Mixed {
                A,
                B = 10,
                C,
            }
        "#;
        let js = strip_types(ts);
        assert!(js.contains("A"), "got: {js}");
        assert!(js.contains("10"), "got: {js}");
        assert!(!js.contains("enum Mixed"), "got: {js}");
    }

    #[test]
    fn test_k6_export_default() {
        // k6 scripts use `export default function() { ... }` as the entry point
        let ts = r#"
            import http from 'k6/http';
            export const options = { vus: 10 };
            export default function() {
                http.get('https://test.k6.io');
            }
        "#;
        let js = strip_types(ts);
        // export keywords should be stripped
        assert!(!js.contains("export const options"), "got: {js}");
        assert!(!js.contains("export default function"), "got: {js}");
        assert!(js.contains("const options = { vus: 10 }"), "got: {js}");
        assert!(js.contains("function() {"), "got: {js}");
    }

    // --- Cases that broke the old regex transpiler ---

    #[test]
    fn test_nested_braces_in_type() {
        // `type` with nested braces — old regex `[^;]+;` consumed too much
        let ts = r#"
            type Nested = { a: { b: { c: string } } };
            const x = 1;
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("type Nested"), "got: {js}");
        assert!(js.contains("const x = 1"), "got: {js}");
    }

    #[test]
    fn test_comma_generics() {
        // comma-separated generics — old regex treated `<T, U>` as comparison
        let ts = r#"
            function pair<T, U>(a: T, b: U): [T, U] { return [a, b]; }
            const p = pair<number, string>(1, "x");
        "#;
        let js = strip_types(ts);
        assert!(js.contains("function pair(a, b)"), "got: {js}");
        assert!(js.contains("const p = pair(1, \"x\")"), "got: {js}");
        assert!(!js.contains("<number, string>"), "got: {js}");
    }

    #[test]
    fn test_arrow_generics() {
        // arrow function with generics — old regex failed on `<T>(x: T) =>`
        let ts = r#"
            const id = <T>(x: T): T => x;
            const y = id<string>("hello");
        "#;
        let js = strip_types(ts);
        assert!(js.contains("const id = (x) => x"), "got: {js}");
        assert!(js.contains("id(\"hello\")"), "got: {js}");
        assert!(!js.contains("<T>"), "got: {js}");
    }

    #[test]
    fn test_export_default_class() {
        // anonymous default class must not emit a bare `class {` statement
        // (script-mode SyntaxError) — re3's `/* export default */` comment
        // form catches it (the comment intentionally keeps the text).
        // Note: `/* export default */ class { ... }` is still not *evaluable*
        // script-mode JS (anonymous class declarations are illegal; only class
        // expressions may be anonymous) — but that is a pre-existing P3 edge
        // case nobody hits in load-test scripts; the guard here only prevents
        // the named-class regression (see test_export_default_named_class).
        let ts = r#"export default class { method() { return 1; } }"#;
        let js = strip_types(ts);
        assert!(
            !js.contains("export default class {") && !js.contains("export default class{"),
            "got: {js}"
        );
        assert!(
            js.contains("class {") || js.contains("class {\n"),
            "got: {js}"
        );
        assert!(js.contains("method()"), "got: {js}");
    }

    #[test]
    fn test_export_default_named_class() {
        let ts = r#"export default class Foo { method() { return 1; } }"#;
        let js = strip_types(ts);
        assert!(!js.contains("export default"), "got: {js}");
        assert!(js.contains("class Foo"), "got: {js}");
    }

    #[test]
    fn test_bare_as_in_conditionals() {
        // bare `as` inside a conditional expression must be preserved
        let ts = r#"
            const flag = cond as boolean;
            const label = isReady ? "yes" : "no";
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("as boolean"), "got: {js}");
        assert!(js.contains("? \"yes\" : \"no\""), "got: {js}");
    }

    #[test]
    fn test_string_poisoning() {
        // strings containing TS-looking text must be left untouched
        let ts = r#"
            const msg = "type User = { id: number }; const x: number = 1;";
            const url = "https://example.com/api/v1/items?id=1:number";
        "#;
        let js = strip_types(ts);
        assert!(
            js.contains("\"type User = { id: number }; const x: number = 1;\""),
            "got: {js}"
        );
        assert!(js.contains("https://example.com"), "got: {js}");
    }

    #[test]
    fn test_keep_exports_preserves_module() {
        let ts = r#"
            export const options = { vus: 5, duration: "10s" };
            export default function() { return 1; }
        "#;
        let js = typescript_to_javascript_keep_exports(ts, "script.ts").unwrap();
        assert!(js.contains("export const options"), "got: {js}");
        assert!(js.contains("export default function"), "got: {js}");
    }

    // --- Decorator lowering (the point this file previously missed) ---

    #[test]
    fn test_legacy_class_decorator_lowered() {
        // Legacy decorators used to pass through `@sealed` verbatim, which
        // QuickJS can't eval. Now they lower to babelHelpers.decorate and the
        // shim is prepended.
        let ts = r#"
            function sealed(constructor: Function) { Object.freeze(constructor); }
            @sealed
            class Greeter {
                greeting: string;
                constructor(message: string) { this.greeting = message; }
                greet() { return "Hello, " + this.greeting; }
            }
            export default function() { return new Greeter("world").greet(); }
        "#;
        let js = strip_types(ts);
        // No raw decorator syntax left — QuickJS would choke on it.
        assert!(!js.contains("@sealed"), "raw decorator survived: {js}");
        // Lowered to the helper call with the shim present.
        assert!(
            js.contains("babelHelpers.decorate([sealed], Greeter)"),
            "decorator not lowered: {js}"
        );
        assert!(js.contains("var babelHelpers"), "shim not prepended: {js}");
        // The shim must come BEFORE the use.
        assert!(
            js.find("var babelHelpers").unwrap() < js.find("babelHelpers.decorate").unwrap(),
            "shim must precede use"
        );
    }

    #[test]
    fn test_legacy_method_and_param_decorators_lowered() {
        let ts = r#"
            function logMethod(target: any, key: string, desc: PropertyDescriptor) { return desc; }
            function logParam(target: any, key: string, index: number) {}
            class Greeter {
                greeting: string;
                constructor(message: string) { this.greeting = message; }
                @logMethod
                greet(@logParam name: string) { return "Hello, " + name; }
            }
            export default function() { return new Greeter("world").greet(); }
        "#;
        let js = strip_types(ts);
        assert!(
            !js.contains("@logMethod") && !js.contains("@logParam"),
            "raw decorators: {js}"
        );
        assert!(
            js.contains("babelHelpers.decorateParam"),
            "param decorator not lowered: {js}"
        );
        assert!(js.contains("var babelHelpers"), "shim not prepended: {js}");
    }

    #[test]
    fn test_no_decorators_means_no_shim() {
        // A plain script must not grow the babelHelpers shim.
        let ts = r#"
            export default function() { return 42; }
        "#;
        let js = strip_types(ts);
        assert!(
            !js.contains("babelHelpers"),
            "unexpected shim in plain output: {js}"
        );
    }

    /// Test helper: strip types + exports (script mode).
    fn strip_types(source: &str) -> String {
        typescript_to_javascript(source, "test.ts").unwrap()
    }
}
