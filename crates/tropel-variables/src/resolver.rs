use crate::catalog::DynamicCatalog;
use regex::Regex;
use std::collections::HashMap;

/// Variable scope.
#[derive(Debug, Clone, Default)]
pub struct VariableScope {
    /// Iteration data (highest priority).
    pub data: HashMap<String, serde_json::Value>,
    /// Environment variables.
    pub env: HashMap<String, String>,
    /// Collection variables.
    pub collection: HashMap<String, serde_json::Value>,
    /// Global variables.
    pub globals: HashMap<String, serde_json::Value>,
}

/// The `{{var}}` placeholder regex, compiled ONCE per process. `VariableResolver`
/// is constructed per iteration / per VU on the hot path (see the runner), so a
/// `Regex::new` on every construction was pure waste — compiled once, a `Regex`
/// is `Sync` and reused by all threads.
static VAR_RE: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();

fn var_re() -> &'static Regex {
    VAR_RE.get_or_init(|| Regex::new(r"\{\{([^}]+)\}\}").expect("valid variable regex"))
}

/// Resolves {{variable}} references with scope precedence.
pub struct VariableResolver {
    dynamic_catalog: DynamicCatalog,
}

impl VariableResolver {
    pub fn new() -> Self {
        Self {
            dynamic_catalog: DynamicCatalog::new(),
        }
    }

    /// Resolve all variable references in the input string.
    pub fn resolve(&self, input: &str, scope: &VariableScope) -> String {
        self.resolve_with(input, scope, EscapeMode::None)
    }

    /// Resolve variable references, escaping each substituted value for
    /// embedding inside a JSON string literal (`"` `\` control chars are
    /// escaped). This is what makes `{"s":"{{name}}"}` with
    /// `name = he said "hi"` produce VALID JSON instead of a broken document
    /// (backlog line 96: substituted values weren't escaped for their
    /// context — a CSV column containing a quote/backslash/newline made the
    /// error rate a function of the data file).
    pub fn resolve_json(&self, input: &str, scope: &VariableScope) -> String {
        self.resolve_with(input, scope, EscapeMode::Json)
    }

    /// Resolve variable references, percent-encoding each substituted value
    /// so `&` `=` `#` etc. inside a data value cannot split a query string
    /// into extra parameters (backlog line 96: a value with `&` or `=`
    /// silently became extra params).
    pub fn resolve_url(&self, input: &str, scope: &VariableScope) -> String {
        self.resolve_with(input, scope, EscapeMode::Url)
    }

    /// Shared resolution core. `mode` decides how each substituted VALUE is
    /// escaped for its destination context (raw / JSON string / URL query).
    /// The placeholder itself is never touched, and an unresolved variable
    /// stays literal `{{name}}` in every mode.
    fn resolve_with(&self, input: &str, scope: &VariableScope, mode: EscapeMode) -> String {
        if !input.contains("{{") {
            return input.to_string();
        }

        // First resolve dynamic variables ({{$xxx}})
        let after_dynamic = self.dynamic_catalog.resolve(input);

        // Then resolve scoped variables ({{var_name}})
        let result = var_re().replace_all(&after_dynamic, |caps: &regex::Captures| {
            let var_name = caps.get(1).unwrap().as_str().trim();

            // Skip dynamic vars (already handled)
            if var_name.starts_with('$') {
                return caps.get(0).unwrap().as_str().to_string();
            }

            let value = self.resolve_variable(var_name, scope);
            if value.starts_with("{{") && value.ends_with("}}") {
                // Unresolved — keep the literal placeholder.
                value
            } else {
                match mode {
                    EscapeMode::None => value,
                    EscapeMode::Json => json_escape(&value),
                    EscapeMode::Url => url_escape(&value),
                }
            }
        });

        result.to_string()
    }

    /// Resolve a single variable name against the scope.
    pub fn resolve_variable(&self, var_name: &str, scope: &VariableScope) -> String {
        // Priority: data > env > collection > globals

        // Check iteration data
        if let Some(val) = scope.data.get(var_name) {
            return value_to_string(val);
        }

        // Check environment
        if let Some(val) = scope.env.get(var_name) {
            return val.clone();
        }

        // Check collection variables
        if let Some(val) = scope.collection.get(var_name) {
            return value_to_string(val);
        }

        // Check globals
        if let Some(val) = scope.globals.get(var_name) {
            return value_to_string(val);
        }

        // Not found — return the original placeholder
        format!("{{{{{}}}}}", var_name)
    }

    /// Resolve an entire string — including nested variable references.
    /// Multiple passes to handle {{var1_{{var2}}}} style nesting.
    pub fn resolve_deep(&self, input: &str, scope: &VariableScope, max_passes: usize) -> String {
        self.resolve_deep_with(input, scope, max_passes, EscapeMode::None)
    }

    /// [`resolve_deep`] with JSON-string escaping of substituted values.
    pub fn resolve_json_deep(
        &self,
        input: &str,
        scope: &VariableScope,
        max_passes: usize,
    ) -> String {
        self.resolve_deep_with(input, scope, max_passes, EscapeMode::Json)
    }

    /// [`resolve_deep`] with URL percent-encoding of substituted values.
    pub fn resolve_url_deep(
        &self,
        input: &str,
        scope: &VariableScope,
        max_passes: usize,
    ) -> String {
        self.resolve_deep_with(input, scope, max_passes, EscapeMode::Url)
    }

    /// Shared deep-resolution core that threads the escape mode through
    /// every pass.
    fn resolve_deep_with(
        &self,
        input: &str,
        scope: &VariableScope,
        max_passes: usize,
        mode: EscapeMode,
    ) -> String {
        let mut result = input.to_string();
        for _ in 0..max_passes {
            if !result.contains("{{") {
                break;
            }
            let resolved = self.resolve_with(&result, scope, mode);
            if resolved == result {
                break;
            }
            result = resolved;
        }
        result
    }
}

/// How a substituted variable VALUE is escaped for its destination context.
/// The placeholder text itself is never modified.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EscapeMode {
    /// Raw insertion (headers, plain-text bodies) — no escaping.
    None,
    /// Escape for embedding inside a JSON string literal.
    Json,
    /// Percent-encode for a URL / query string.
    Url,
}

/// Escape a value for safe embedding inside a JSON string literal: `"` and
/// `\` get backslash-escaped, control chars become \n \r \t or \uXXXX.
/// JSON bodies built from `{{var}}` templates stay parseable even when the
/// data (e.g. a CSV column) contains quotes or newlines.
fn json_escape(value: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(value.len() + 8);
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

/// Percent-encode a value for safe insertion into a URL / query string: the
/// reserved and unsafe characters (`&` `=` `?` `#` `%` `+` space, non-ASCII)
/// are encoded so a data value cannot split a query into extra params or
/// inject fragments. Bytes in the unreserved set (A-Z a-z 0-9 - _ . ~) and
/// the URL-structural `/` `:` `@` are left as-is so paths and hosts stay
/// readable.
///
/// Values are assumed RAW, not pre-encoded: an already-percent-encoded value
/// (e.g. `caf%C3%A9`) double-encodes. That is the safe default — encoding a
/// `%` that turns out to be literal `%` is harmless, while leaving one
/// unencoded could let a crafted value smuggle reserved characters through.
fn url_escape(value: &str) -> String {
    const UNRESERVED: &str =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~/:@";
    let mut out = String::with_capacity(value.len() + 8);
    for b in value.bytes() {
        if UNRESERVED.contains(b as char) {
            out.push(b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{:02X}", b));
        }
    }
    out
}

impl Default for VariableResolver {
    fn default() -> Self {
        Self::new()
    }
}

fn value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_variable() {
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([("base_url".into(), "https://api.example.com".into())]),
            ..Default::default()
        };

        let result = resolver.resolve("{{base_url}}/users", &scope);
        assert_eq!(result, "https://api.example.com/users");
    }

    #[test]
    fn test_multiple_variables() {
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([
                ("host".into(), "api.example.com".into()),
                ("port".into(), "443".into()),
            ]),
            ..Default::default()
        };

        let result = resolver.resolve("https://{{host}}:{{port}}/v1", &scope);
        assert_eq!(result, "https://api.example.com:443/v1");
    }

    #[test]
    fn test_scope_priority() {
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            data: HashMap::from([("key".into(), serde_json::Value::String("data-value".into()))]),
            env: HashMap::from([("key".into(), "env-value".into())]),
            collection: HashMap::from([(
                "key".into(),
                serde_json::Value::String("col-value".into()),
            )]),
            globals: HashMap::from([(
                "key".into(),
                serde_json::Value::String("global-value".into()),
            )]),
        };

        // Data takes priority
        let result = resolver.resolve("{{key}}", &scope);
        assert_eq!(result, "data-value");
    }

    #[test]
    fn test_missing_variable() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        let result = resolver.resolve("{{missing}}", &scope);
        assert_eq!(result, "{{missing}}");
    }

    #[test]
    fn test_dynamic_variable() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        let result = resolver.resolve("id={{$guid}}", &scope);
        assert!(result.starts_with("id="));
        assert_eq!(result.len(), 39); // "id=" + 36-char UUID
    }

    #[test]
    fn test_no_variables() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        let result = resolver.resolve("plain text", &scope);
        assert_eq!(result, "plain text");
    }

    #[test]
    fn test_deep_resolve() {
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([
                ("host".into(), "{{base_host}}".into()),
                ("base_host".into(), "api.example.com".into()),
            ]),
            ..Default::default()
        };

        let result = resolver.resolve_deep("https://{{host}}/v1", &scope, 5);
        assert_eq!(result, "https://api.example.com/v1");
    }

    #[test]
    fn test_collection_then_globals_priority() {
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            collection: HashMap::from([(
                "key".into(),
                serde_json::Value::String("col-value".into()),
            )]),
            globals: HashMap::from([(
                "key".into(),
                serde_json::Value::String("global-value".into()),
            )]),
            ..Default::default()
        };

        // env > data > collection > globals; with env absent, collection wins.
        assert_eq!(resolver.resolve("{{key}}", &scope), "col-value");

        // Collection value type is preserved through the value form.
        let scope_num = VariableScope {
            collection: HashMap::from([("n".into(), serde_json::json!(42))]),
            ..Default::default()
        };
        assert_eq!(resolver.resolve("n={{n}}", &scope_num), "n=42");
    }

    #[test]
    fn test_dynamic_guid_fresh_per_occurrence() {
        // Regression: {{$guid}}-{{$guid}} once produced the SAME value both
        // times (str::replace of a single resolved string). Each occurrence
        // must be independently fresh.
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        // Delimiter is a comma — NOT a hyphen, because UUIDs themselves are
        // hyphenated, so splitting on '-' would split inside the first UUID.
        let result = resolver.resolve("{{$guid}},{{$guid}}", &scope);

        let (first, second) = result.split_once(',').expect("comma separator present");
        assert_eq!(first.len(), 36, "first is a UUID: {first}");
        assert_eq!(second.len(), 36, "second is a UUID: {second}");
        assert_ne!(first, second, "each occurrence is fresh");
    }

    #[test]
    fn test_dynamic_timestamp_and_random_int() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();

        // {{$timestamp}} — 10-digit Unix seconds.
        let ts = resolver.resolve("ts={{$timestamp}}", &scope);
        let ts_val = ts.strip_prefix("ts=").unwrap();
        assert_eq!(ts_val.len(), 10, "timestamp is 10 digits: {ts}");
        let secs: u64 = ts_val.parse().unwrap();
        assert!(secs > 1_700_000_000, "timestamp is recent: {ts}");

        // {{$randomInt}} — fresh integer in [0, 1000) per occurrence.
        for _ in 0..20 {
            let ri = resolver.resolve("{{$randomInt}}", &scope);
            let n: i64 = ri
                .parse()
                .unwrap_or_else(|_| panic!("randomInt is numeric: {ri}"));
            assert!((0..1000).contains(&n), "randomInt in range: {ri}");
        }
    }

    #[test]
    fn test_unresolved_variable_left_literal_in_deep() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        let result = resolver.resolve_deep("/api/{{missing}}/v1", &scope, 5);
        assert_eq!(result, "/api/{{missing}}/v1");
    }

    #[test]
    fn test_resolve_json_escapes_quotes() {
        // Backlog line 96: `{"s":"{{name}}"}` with `name = he said "hi"`
        // produced INVALID JSON (the quote terminated the string). The value
        // must be JSON-escaped so the document stays parseable.
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([("name".into(), "he said \"hi\"".into())]),
            ..Default::default()
        };

        let result = resolver.resolve_json(r#"{"s":"{{name}}"}"#, &scope);
        assert_eq!(result, r#"{"s":"he said \"hi\""}"#);
        // The result must round-trip as valid JSON with the value intact.
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        assert_eq!(parsed["s"], "he said \"hi\"");
    }

    #[test]
    fn test_resolve_json_escapes_backslash_and_newline() {
        // CSV data with a backslash or embedded newline must not corrupt a
        // JSON body (the error rate was a function of the data file).
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([
                ("path".into(), "C:\\tmp\\f".into()),
                ("note".into(), "line1\nline2".into()),
            ]),
            ..Default::default()
        };

        let result = resolver.resolve_json(r#"{"p":"{{path}}","n":"{{note}}"}"#, &scope);
        let parsed: serde_json::Value = serde_json::from_str(&result).expect("valid JSON");
        assert_eq!(parsed["p"], "C:\\tmp\\f");
        assert_eq!(parsed["n"], "line1\nline2");
    }

    #[test]
    fn test_resolve_json_unresolved_stays_literal() {
        let resolver = VariableResolver::new();
        let scope = VariableScope::default();
        // An unresolved variable inside a JSON body must stay `{{name}}`
        // (literal placeholder), not be escaped into garbage.
        let result = resolver.resolve_json(r#"{"s":"{{missing}}"}"#, &scope);
        assert_eq!(result, r#"{"s":"{{missing}}"}"#);
    }

    #[test]
    fn test_resolve_url_encodes_query_splitters() {
        // Backlog line 96: a data value containing `&` or `=` silently split
        // the query into extra params. The value must be percent-encoded.
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([("q".into(), "a&b=c".into())]),
            ..Default::default()
        };

        let result = resolver.resolve_url("/search?q={{q}}", &scope);
        assert_eq!(result, "/search?q=a%26b%3Dc");
    }

    #[test]
    fn test_resolve_url_keeps_path_and_safe_chars() {
        // URL-structural chars and the unreserved set stay readable; only
        // reserved/unsafe chars are encoded.
        let resolver = VariableResolver::new();
        let scope = VariableScope {
            env: HashMap::from([
                ("id".into(), "u/42".into()),
                ("token".into(), "tok+1 #2".into()),
            ]),
            ..Default::default()
        };

        // Slash is left as-is (paths stay readable), `+` and space encode.
        let result = resolver.resolve_url("/users/{{id}}", &scope);
        assert_eq!(result, "/users/u/42");
        let result = resolver.resolve_url("?t={{token}}", &scope);
        assert_eq!(result, "?t=tok%2B1%20%232");
    }
}
