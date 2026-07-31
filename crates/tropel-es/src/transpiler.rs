//! TypeScript → JavaScript transpilation for load-test scripts.
//!
//! Uses a lightweight regex-based approach to strip TypeScript type annotations.
//! This is NOT a full TypeScript compiler — it handles the subset of TS that
//! appears in typical load-test and Postman/k6 scripts:
//!
//! - Function parameter type annotations: `(name: string, age: number)`
//! - Variable type annotations: `const x: SomeType = ...`
//! - Return type annotations: `function foo(): T { ... }`
//! - Generic type parameters: `function foo<T>(arg: T)`, `array as Foo[]`
//! - Interface/type declarations: `interface Foo { ... }`, `type Foo = ...`
//! - Enum declarations (converts to JS objects)
//! - Type-only imports: `import type { X } from "./y"`
//! - `as` type assertions: `value as SomeType`
//!
//! This approach avoids the heavy SWC dependency and its serde compatibility
//! issues, while being sufficient for the load-testing use case.

use regex::Regex;

/// Transpile TypeScript source code to plain JavaScript.
/// Uses regex-based stripping of type annotations.
pub fn typescript_to_javascript(source: &str, _filename: &str) -> anyhow::Result<String> {
    let js = strip_types(source);
    Ok(js)
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
    _filename: &str,
) -> anyhow::Result<String> {
    let js = strip_types_inner(source, false);
    Ok(js)
}

/// Strip TypeScript type annotations from source code (exports removed).
/// This is a multi-pass process that removes each TS construct.
fn strip_types(source: &str) -> String {
    strip_types_inner(source, true)
}

/// Internal implementation. When `strip_exports` is true the `export` keyword
/// is stripped from declarations (script-mode eval); when false the exports
/// are preserved (module-mode eval).
fn strip_types_inner(source: &str, strip_exports: bool) -> String {
    let mut result = source.to_string();

    // Order matters: remove larger constructs first

    // 1. Remove multi-line comments at the top level (preserve JSDoc style?)
    //    We keep comments as they may contain useful documentation
    //    and don't affect execution.

    // 2. Remove interface declarations (multi-line)
    result = remove_interfaces(&result);

    // 3. Remove type declarations: `type Foo = ...;`
    result = remove_type_aliases(&result);

    // 4. Remove type-only import statements: `import type { X } from "./y"`
    result = remove_import_type(&result);

    // 5. Convert enums to plain JS objects
    //    `enum Foo { A, B }` → `const Foo = { A: 0, B: 1 };`
    result = convert_enums(&result);

    // 6. Remove generic type parameters from function declarations:
    //    `function foo<T>(arg: T)` → `function foo(arg)`
    result = remove_generics_from_functions(&result);

    // 7. Remove generic type parameters from function calls:
    //    `identity<number>(42)` → `identity(42)`
    result = remove_generics_from_calls(&result);

    // 8. Remove return type annotations:
    //    `function foo(): string {` → `function foo() {`
    //    `(): string =>` → `() =>`
    result = remove_return_types(&result);

    // 9. Remove parameter type annotations:
    //    `function foo(x: string, y: number)` → `function foo(x, y)`
    result = remove_param_types(&result);

    // 10. Remove variable/const type annotations:
    //     `const x: SomeType = value` → `const x = value`
    //     `let x: SomeType = value` → `let x = value`
    //     BUT careful: `obj[key]: value` in object literals
    result = remove_variable_types(&result);

    // 11. Remove `export` keyword from declarations (script-mode only):
    //     `export function foo()` → `function foo()`
    //     `export default function()` → `function()`
    //     `export const x = 1` → `const x = 1`
    //     `export { x, y }` → `/* export { x, y } */`
    if strip_exports {
        result = remove_exports(&result);
    }

    // 12. Remove `as Type` assertions:
    //     `value as SomeType` → `value`
    //     Constrained to avoid matching English word `as` in prose:
    //     only replaced near expressions (assignments, returns, params, calls).
    result = remove_as_casts(&result);

    // 13. Clean up empty lines left by removed declarations
    result = remove_empty_lines(&result);

    result
}

fn remove_interfaces(s: &str) -> String {
    // Remove `interface Name { ... }` blocks
    // Matches multi-line interface declarations
    lazy_regex_replace_all(s, r"(?s)\binterface\s+\w+\s*\{[^}]*\}\s*", "")
}

fn remove_type_aliases(s: &str) -> String {
    // Remove `type Name = ...;` declarations (single-line)
    lazy_regex_replace_all(s, r"\btype\s+\w+\s*=\s*[^;]+;", "")
}

fn remove_import_type(s: &str) -> String {
    // Remove `import type { X } from "..."` entirely
    // Use a non-regex approach to avoid escaping hell
    s.lines()
        .filter(|line| !line.trim().starts_with("import type"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn convert_enums(s: &str) -> String {
    // Convert `enum Name { A, B, C = 1, D, ... }` to `const Name = { A: 0, B: 1, C: 1, D: 2 }`
    // "Hand-written" numeric values in the source (e.g. `C = 1`) are kept.
    // Auto-increment is applied for members without initializers.
    //
    // This is a pragmatic approximation — it handles the common case of simple
    // numeric enums. String enums, computed members, and const enums are not handled.
    // For load-test scripts, this is sufficient (enums are rare).
    let mut result = s.to_string();

    // Find enum blocks: `enum Name { ... }`
    let enum_re = Regex::new(r"\benum\s+(\w+)\s*\{([^}]*)\}").unwrap();
    result = enum_re
        .replace_all(&result, |caps: &regex::Captures| {
            let name = &caps[1];
            let body = &caps[2];

            // Parse members with proper numeric assignments
            let mut members = Vec::new();
            let mut next_val: i64 = 0;

            for member in body.split(',') {
                let member = member.trim();
                if member.is_empty() {
                    continue;
                }
                // Check for explicit initializer: `Name = value`
                if let Some(eq_pos) = member.find('=') {
                    let mem_name = member[..eq_pos].trim();
                    let val_str = member[eq_pos + 1..].trim();
                    if let Ok(val) = val_str.parse::<i64>() {
                        members.push(format!("{}: {}", mem_name, val));
                        next_val = val + 1;
                    } else {
                        // String or computed value — just reference the original
                        members.push(format!("{}: {}", mem_name, val_str));
                    }
                } else {
                    // No initializer — use auto-increment
                    members.push(format!("{}: {}", member, next_val));
                    next_val += 1;
                }
            }

            format!("const {} = {{ {} }}", name, members.join(", "))
        })
        .to_string();

    result
}

fn remove_generics_from_functions(s: &str) -> String {
    // Remove `<T, U, ...>` from function declarations
    // `function foo<T>(arg: T)` → `function foo(arg`
    lazy_regex_replace_all(s, r"(function\s+\w+)\s*<[^>]+>\s*\(", "$1(")
}

fn remove_generics_from_calls(s: &str) -> String {
    // Remove `<Type>` from function/method calls
    // `foo<Type>(arg)` or `obj.foo<Type>(arg)`
    // Be careful to only match actual type args, not comparison operators
    let mut result = s.to_string();
    // Match: identifier or dotted path followed by <type, ...>( or <type>( etc.
    let re = Regex::new(r"([\w.]+)\s*<([^<>]+)>\s*\(").unwrap();
    result = re
        .replace_all(&result, |caps: &regex::Captures| {
            let name = &caps[1];
            let inside = &caps[2];
            // Heuristic: if the inside contains only identifiers, dots, and commas,
            // it's likely a generic type argument, not a comparison
            if inside
                .chars()
                .all(|c| c.is_alphanumeric() || c == '.' || c == ',' || c == ' ' || c == '_')
            {
                format!("{}(", name)
            } else {
                caps[0].to_string() // Keep as-is — probably a comparison
            }
        })
        .to_string();
    result
}

fn remove_return_types(s: &str) -> String {
    // Remove return type annotation after function params
    // `function foo(): string {` → `function foo() {`
    // `(): string =>` → `() =>`
    let mut result = s.to_string();

    // Match `): type {` — return annotation before opening brace
    // Match any text between `):` and `{` that looks like a type annotation
    let re1 = Regex::new(r"\):\s*[A-Za-z_][A-Za-z_0-9<>|&, ]*\s*\{").unwrap();
    result = re1.replace_all(&result, ") {").to_string();

    // Match `): type =>` — return annotation before arrow
    let re2 = Regex::new(r"\):\s*[A-Za-z_][A-Za-z_0-9<>|&, ]*\s*=>").unwrap();
    result = re2.replace_all(&result, ") =>").to_string();

    result
}

fn remove_param_types(s: &str) -> String {
    let mut result = s.to_string();

    // Remove `: Type` from function parameters.
    // Match `(name: string` → `(name` and `, name: Type` → `, name`
    // Only matches identifier-based type annotations (starting with letter or _),
    // NOT string/number/object literal values (which start with quotes, digits, or braces).
    // This avoids stripping property values in object literals like `{ id: 1, name: "Alice" }`.
    // IMPORTANT: do NOT include `,` or space in type char class — that would greedily
    // consume multiple params (e.g., `(a: number, b: number` → `(a` losing `b`).
    let re = Regex::new(r"([,(]\s*\w+)\s*:\s*([A-Za-z_][\w<>|&]*(\[\])?)").unwrap();
    result = re.replace_all(&result, "$1").to_string();

    result
}

fn remove_variable_types(s: &str) -> String {
    // Remove type annotations from `const/let/var` declarations
    // `const x: Type = val` → `const x = val`
    // `let x: Type = val` → `let x = val`
    // `var x: Type = val` → `var x = val`
    // BUT NOT `const x = { y: Type }` (object literal)
    // Match `const/let/var x: Type =` — replace with `const x =`
    let re = Regex::new(r"\b(const|let|var)\s+(\w+)\s*:([^=]+)=").unwrap();
    let mut result = s.to_string();
    for _ in 0..3 {
        let before = result.clone();
        result = re
            .replace_all(&result, |caps: &regex::Captures| {
                format!("{} {} =", &caps[1], &caps[2])
            })
            .to_string();
        if result == before {
            break;
        }
    }
    result
}

fn remove_exports(s: &str) -> String {
    let mut result = s.to_string();

    // `export default function Name(...` → `function Name(...`
    let re1 = Regex::new(r"\bexport\s+default\s+(function|class)\s+").unwrap();
    result = re1.replace_all(&result, "$1 ").to_string();

    // `export default function(...` → `function(...` (anonymous default)
    let re2 = Regex::new(r"\bexport\s+default\s+(function|class)\s*\(").unwrap();
    result = re2.replace_all(&result, "$1(").to_string();

    // `export default X` → `/* export default X */` (any other default)
    let re3 = Regex::new(r"\bexport\s+default\s+").unwrap();
    result = re3
        .replace_all(&result, "/* export default */ ")
        .to_string();

    // `export function Name(...` → `function Name(...`
    // Use a closure to avoid any $N expansion ambiguity in the replacement string.
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

fn remove_as_casts(s: &str) -> String {
    let mut result = s.to_string();

    // Remove ` as Type` patterns, but only near expressions to avoid
    // matching the English word "as" in general prose/comments.
    // Type pattern: identifier followed by optional generics `<...>` and optional `[]`.
    // In raw strings, use `\[\]` for literal `[]` (no character class nesting).
    let type_pattern = r"[A-Za-z_][\w<>]*(\[\])?";

    // `return expr as Type` → `return expr`
    let p1 = format!(r"(return|throw|yield)\s+([A-Za-z_][\w.]*)\s+as\s+{type_pattern}");
    let re1 = Regex::new(&p1).unwrap();
    result = re1.replace_all(&result, "$1 $2").to_string();

    // `(expr as Type)` → `(expr)`
    let p2 = format!(r"\(([^()]+)\s+as\s+{type_pattern}\)");
    let re2 = Regex::new(&p2).unwrap();
    result = re2.replace_all(&result, "($1)").to_string();

    // `= expr as Type` → `= expr` (assignment rhs, expr is a function call)
    let p3 = format!(r"=\s*([A-Za-z_][\w.]*\([^)]*\))\s+as\s+{type_pattern}");
    let re3 = Regex::new(&p3).unwrap();
    result = re3.replace_all(&result, "= $1").to_string();

    // `) as Type` — suffix of a cast expression (handles nested parens like `(getValue() as Type)`)
    // The `)` must NOT be followed by `,` `;` or end-of-string-as-part-of-type — it must be
    // the closing paren of the cast, followed by ` as Type`.
    let p4 = format!(r"\)\s+as\s+{type_pattern}");
    if let Ok(re4) = Regex::new(&p4) {
        result = re4.replace_all(&result, ")").to_string();
    }

    result
}

fn remove_empty_lines(s: &str) -> String {
    // Remove lines that are empty or only contain whitespace
    let re = Regex::new(r"^\s*$\n?").unwrap();
    re.replace_all(s, "").to_string()
}

/// Helper: apply a regex replacement that uses lazy_static or just returns a string.
fn lazy_regex_replace_all(s: &str, pattern: &str, replacement: &str) -> String {
    let re = Regex::new(pattern).unwrap();
    re.replace_all(s, replacement).to_string()
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
        assert!(js.contains("const user = { id: 1, name: \"Alice\" }"));
        assert!(js.contains("let count = 42"));
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
        assert!(js.contains("function identity(arg)"));
        assert!(js.contains("return arg"));
        assert!(js.contains("const result = identity(42)"));
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
        assert!(!js.contains("interface User"));
        assert!(js.contains("const user = { id: 1 }"));
    }

    #[test]
    fn test_strip_type_aliases() {
        let ts = r#"
            type MyString = string;
            const x: MyString = "hello";
        "#;
        let js = strip_types(ts);
        assert!(!js.contains("type MyString"));
        assert!(js.contains("const x = \"hello\""));
    }

    #[test]
    fn test_strip_import_type() {
        let ts = r#"import type { SomeType } from "./types";"#;
        let js = strip_types(ts);
        assert!(!js.contains("import type"));
    }

    #[test]
    fn test_strip_as_casts() {
        let ts = r#"const x = (getValue() as SomeType);"#;
        let js = strip_types(ts);
        assert!(!js.contains("as SomeType"));
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
            js.contains("function add(a, b) {"),
            "Expected 'function add(a, b) {{' in output, got: {}",
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
        // Enum keyword should be converted to const with proper numeric values
        assert!(js.contains("const Color = { Red: 0, Green: 1, Blue: 2 }"));
        assert!(js.contains("c = Color.Red"));
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
        assert!(!js.contains("export default function"));
        assert!(!js.contains("export function helper"));
        assert!(!js.contains("export const VERSION"));
        assert!(js.contains("function() {\n                return 42"));
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
        assert!(js.contains("as a fallback"));
        // The TS `as` cast in code should be removed
        assert!(!js.contains("as SomeType"));
    }

    #[test]
    fn test_export_default_function() {
        let ts = r#"export default function() { return 42; }"#;
        let js = strip_types(ts);
        assert!(!js.contains("export default"));
        assert!(js.contains("function() { return 42; }"));
    }

    #[test]
    fn test_export_named_function() {
        // Step-by-step debugging
        let ts = r#"export function foo() { return 1; }"#;
        let js = strip_types(ts);
        assert!(!js.contains("export"));
        assert!(js.trim().contains("function foo() { return 1; }"));
    }

    #[test]
    fn test_export_named_block() {
        let ts = r#"const x = 1; export { x };"#;
        let js = strip_types(ts);
        assert!(!js.contains("export { x };"));
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
        assert!(js.contains("HttpStatus = { OK: 200, NotFound: 404, ServerError: 500 }"));
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
        assert!(js.contains("A: 0"));
        assert!(js.contains("B: 10"));
        assert!(js.contains("C: 11"));
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
        assert!(!js.contains("export const options"));
        assert!(!js.contains("export default function"));
        assert!(js.contains("const options = { vus: 10 }"));
        assert!(js.contains("function() {"));
    }
}
