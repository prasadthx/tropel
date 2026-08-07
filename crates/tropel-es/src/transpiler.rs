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
//! 5. Optionally remove the `export` nodes from the AST (script-mode eval)
//!    — see [`strip_exports_ast`].
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

use std::cell::Cell;

use oxc_allocator::{Allocator, Box};
use oxc_ast::ast::{
    ClassType, Declaration, ExportDefaultDeclaration, ExportDefaultDeclarationKind,
    ExportNamedDeclaration, Expression, ExpressionStatement, FunctionType, ParenthesizedExpression,
    Program, Statement,
};
use oxc_codegen::Codegen;
use oxc_diagnostics::Severity;
use oxc_parser::Parser;
use oxc_semantic::SemanticBuilder;
use oxc_span::{GetSpan, SourceType, Span};
use oxc_syntax::node::NodeId;
use oxc_transformer::{
    DecoratorOptions, HelperLoaderMode, HelperLoaderOptions, TransformOptions, Transformer,
};

/// Transpile TypeScript source code to plain JavaScript.
/// Strips types via oxc, then removes `export` keywords (script-mode eval).
pub fn typescript_to_javascript(source: &str, filename: &str) -> anyhow::Result<String> {
    transpile_typescript(source, filename, true)
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
    transpile_typescript(source, filename, false)
}

/// The shared oxc pipeline: parse → transform (strip TS + lower decorators)
/// → codegen → prepend the decorator helper shim when needed.
fn transpile_typescript(
    source: &str,
    filename: &str,
    strip_exports: bool,
) -> anyhow::Result<String> {
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

    // Script-mode eval cannot contain `export` statements — remove the export
    // nodes on the AST (the old regex pass rewrote string/template-literal
    // contents and emitted invalid JS; see `strip_exports_ast`).
    if strip_exports {
        strip_exports_ast(&mut program, &allocator);
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
// Script-mode eval rejects `export` statements, so they are removed on the
// oxc AST — the SAME AST the transformer produced — before codegen. This
// replaces the old regex pass, which ran on raw output text with no lexical
// awareness: it rewrote string/template-literal contents
// (`` const p = `export default function () { return 1; }` `` got mangled, so
// the load test POSTed a different body than the script said) and emitted
// invalid JS (`export default function(` → `function(` is a SyntaxError —
// anonymous function declarations are legal only as an `export default`
// operand). Every test re-parses its output; see `assert_reparses`.
// ---------------------------------------------------------------------------

/// Remove `export` nodes from the AST (script-mode eval).
///
/// Runs on the AST oxc already built — never on raw output text — so string
/// and template-literal contents are untouched and every replacement is valid
/// script-mode JavaScript:
///
/// - `export const/let/var/function/class X` → the bare declaration.
/// - `export default function f(){}` / `export default class C {}` → the bare
///   declaration (named declarations are legal statements).
/// - `export default function(){}` / `export default class {}` (anonymous)
///   → `(function(){});` / `(class{});` — a parenthesized EXPRESSION, the
///   only legal anonymous form at statement position.
/// - `export default <expr>` → `(<expr>);` (parens keep object literals from
///   parsing as blocks and function/class expressions legal).
/// - `export { a, b }`, `export { a } from './m'`, `export * from './m'`
///   → dropped (script mode has no module bindings to re-export).
fn strip_exports_ast<'a>(program: &mut Program<'a>, allocator: &'a Allocator) {
    let old_body = std::mem::replace(&mut program.body, oxc_allocator::Vec::new_in(allocator));
    let stripped = old_body
        .into_iter()
        .filter_map(|stmt| strip_export(stmt, allocator));
    program.body = oxc_allocator::Vec::from_iter_in(stripped, allocator);
}

/// Rewrite one top-level statement: strip `export` wrappers, drop module
/// re-export statements. Returns `None` for statements to remove entirely.
fn strip_export<'a>(stmt: Statement<'a>, allocator: &'a Allocator) -> Option<Statement<'a>> {
    match stmt {
        Statement::ExportNamedDeclaration(b) => {
            // Unbox so the declaration field can be moved out and spliced
            // into a plain statement.
            let ExportNamedDeclaration { declaration, .. } = b.unbox();
            match declaration {
                // `export const x = 1` / `export function f(){}` / `export class C {}`
                Some(Declaration::VariableDeclaration(v)) => {
                    Some(Statement::VariableDeclaration(v))
                }
                Some(Declaration::FunctionDeclaration(f)) => {
                    Some(Statement::FunctionDeclaration(f))
                }
                Some(Declaration::ClassDeclaration(c)) => Some(Statement::ClassDeclaration(c)),
                // TS-only declarations (interface/type) are erased by the
                // transform; re-exports (`export { x } from './m'`, `export
                // { y };`) have `declaration: None` — drop them.
                _ => None,
            }
        }
        Statement::ExportDefaultDeclaration(b) => {
            let ExportDefaultDeclaration {
                node_id,
                span,
                declaration,
            } = b.unbox();
            match declaration {
                ExportDefaultDeclarationKind::FunctionDeclaration(mut f) => {
                    if f.id.is_some() {
                        Some(Statement::FunctionDeclaration(f))
                    } else {
                        // Codegen auto-parens function EXPRESSIONS at statement
                        // start (is_expression()); a declaration-typed node
                        // would be emitted bare as `function(){};` — a
                        // SyntaxError. Flip the kind so the wrap triggers.
                        f.r#type = FunctionType::FunctionExpression;
                        Some(paren_expr_stmt(
                            allocator,
                            node_id,
                            span,
                            Expression::FunctionExpression(f),
                        ))
                    }
                }
                ExportDefaultDeclarationKind::ClassDeclaration(mut c) => {
                    if c.id.is_some() {
                        Some(Statement::ClassDeclaration(c))
                    } else {
                        c.r#type = ClassType::ClassExpression;
                        Some(paren_expr_stmt(
                            allocator,
                            node_id,
                            span,
                            Expression::ClassExpression(c),
                        ))
                    }
                }
                // `export default interface` — TS-only, drop defensively.
                ExportDefaultDeclarationKind::TSInterfaceDeclaration(_) => None,
                // `export default <expr>` — any inherited Expression variant.
                expr => Some(paren_expr_stmt(
                    allocator,
                    node_id,
                    span,
                    expr.into_expression(),
                )),
            }
        }
        // `export * from './m'` — no local bindings to keep.
        Statement::ExportAllDeclaration(_) => None,
        other => Some(other),
    }
}

/// Build `(<expr>);` — a parenthesized expression statement. Parens are
/// REQUIRED: a bare `function(){}` / `class{}` / `{}` at statement position
/// is a SyntaxError or a block, not an expression. Callers must have already
/// flipped the node's `r#type` to the *Expression* variant — codegen's
/// `is_expression()` wrap (which emits the parens) only fires for expression-
/// typed nodes.
fn paren_expr_stmt<'a>(
    allocator: &'a Allocator,
    node_id: Cell<NodeId>,
    span: Span,
    expression: Expression<'a>,
) -> Statement<'a> {
    let paren = Expression::ParenthesizedExpression(Box::new_in(
        ParenthesizedExpression {
            node_id: node_id.clone(),
            span,
            expression,
        },
        allocator,
    ));
    Statement::ExpressionStatement(Box::new_in(
        ExpressionStatement {
            node_id,
            span,
            expression: paren,
        },
        allocator,
    ))
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
        // anonymous default class must become a parenthesized CLASS
        // EXPRESSION `(class { ... });` — a bare `class {` statement is a
        // script-mode SyntaxError. This is the AST stripper's job; the old
        // regex pass could only emit the comment form, which was still not
        // evaluable.
        let ts = r#"export default class { method() { return 1; } }"#;
        let js = strip_types(ts);
        assert!(
            !js.contains("export default class {") && !js.contains("export default class{"),
            "got: {js}"
        );
        assert!(
            js.contains("(class") || js.contains("class {"),
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
        // The keep-exports output must still re-parse as a module.
        let allocator = Allocator::default();
        let ret =
            Parser::new(&allocator, &js, SourceType::default().with_module(true)).parse();
        assert!(
            ret.errors
                .iter()
                .find(|d| d.severity == Severity::Error)
                .is_none()
                && !ret.panicked,
            "keep-exports output does not re-parse as a module:\n{js}\n{}",
            format_diagnostics(&ret.errors)
        );
    }

    // --- Regression: the old regex export-stripper (backlog line 224) ---
    // It rewrote string/template-literal contents and emitted invalid JS.
    // These pin the AST-based strip: contents preserved, output re-parses.

    #[test]
    fn test_export_default_text_inside_string_preserved() {
        // `export default` inside a string literal must survive verbatim.
        let ts = r#"
            const msg = "export default function() { return 1; }";
            export default function() { return msg; }
        "#;
        let js = strip_types(ts);
        assert!(
            js.contains("\"export default function() { return 1; }\""),
            "got: {js}"
        );
    }

    #[test]
    fn test_export_text_inside_template_literal_preserved() {
        // `export const` text inside a template literal must survive verbatim.
        let ts = r#"
            const tmpl = `export const options = { vus: 10 };`;
            export default function() { return tmpl; }
        "#;
        let js = strip_types(ts);
        assert!(
            js.contains("export const options = { vus: 10 }"),
            "got: {js}"
        );
    }

    #[test]
    fn test_anonymous_default_function_is_parenthesized() {
        // `export default function() {}` must become `(function() {});` — a
        // bare `function(){}` at statement position is a SyntaxError.
        let ts = r#"export default function() { return 42; }"#;
        let js = strip_types(ts);
        assert!(js.contains("(function"), "got: {js}");
        assert!(!js.contains("export default"), "got: {js}");
    }

    #[test]
    fn test_object_literal_default_is_parenthesized() {
        // `export default { a: 1 };` must become `({ a: 1 });` — a bare `{}`
        // at statement position is a block, not an object literal.
        let ts = r#"export default { a: 1, b: "x" };"#;
        let js = strip_types(ts);
        // Codegen may expand the object across lines, so pin the parens (a
        // bare `{}` statement would be a block) and the member presence.
        assert!(js.contains("({"), "got: {js}");
        assert!(js.contains("a: 1"), "got: {js}");
    }

    #[test]
    fn test_reexport_statements_dropped() {
        // `export { x } from './m'` and `export * from './n'` have no local
        // binding — drop the whole statement. Local `export { y };` is also
        // a no-op after stripping; only the binding must survive.
        let ts = r#"
            export { x } from "./m";
            export * from "./n";
            const y = 1;
            export { y };
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("export"), "got: {js}");
        assert!(js.contains("const y = 1"), "got: {js}");
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

    /// Re-parse `js` as plain script-mode JavaScript and panic with the
    /// source on any Error-severity diagnostic — every test pins this, since
    /// the old regex export-stripper emitted invalid JS.
    fn assert_reparses(js: &str) {
        let allocator = Allocator::default();
        let ret = Parser::new(&allocator, js, SourceType::default()).parse();
        let err = ret.errors.iter().find(|d| d.severity == Severity::Error);
        assert!(
            err.is_none() && !ret.panicked,
            "output does not re-parse as script JS:\n{js}\n{}",
            format_diagnostics(&ret.errors)
        );
    }

    /// Test helper: strip types + exports (script mode), then re-parse the
    /// output to guarantee it is valid script JS.
    fn strip_types(source: &str) -> String {
        let js = typescript_to_javascript(source, "test.ts").unwrap();
        assert_reparses(&js);
        js
    }
}
