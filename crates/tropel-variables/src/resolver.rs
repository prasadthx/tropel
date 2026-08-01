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

/// Resolves {{variable}} references with scope precedence.
pub struct VariableResolver {
    var_re: Regex,
    dynamic_catalog: DynamicCatalog,
}

impl VariableResolver {
    pub fn new() -> Self {
        Self {
            var_re: Regex::new(r"\{\{([^}]+)\}\}").unwrap(),
            dynamic_catalog: DynamicCatalog::new(),
        }
    }

    /// Resolve all variable references in the input string.
    pub fn resolve(&self, input: &str, scope: &VariableScope) -> String {
        if !input.contains("{{") {
            return input.to_string();
        }

        // First resolve dynamic variables ({{$xxx}})
        let after_dynamic = self.dynamic_catalog.resolve(input);

        // Then resolve scoped variables ({{var_name}})
        let result = self
            .var_re
            .replace_all(&after_dynamic, |caps: &regex::Captures| {
                let var_name = caps.get(1).unwrap().as_str().trim();

                // Skip dynamic vars (already handled)
                if var_name.starts_with('$') {
                    return caps.get(0).unwrap().as_str().to_string();
                }

                self.resolve_variable(var_name, scope)
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

    /// Resolve all {{variable}} references in request headers, URL, body, etc.
    pub fn resolve_request(&self, input: &str, scope: &VariableScope) -> String {
        self.resolve(input, scope)
    }

    /// Resolve an entire string — including nested variable references.
    /// Multiple passes to handle {{var1_{{var2}}}} style nesting.
    pub fn resolve_deep(&self, input: &str, scope: &VariableScope, max_passes: usize) -> String {
        let mut result = input.to_string();
        for _ in 0..max_passes {
            if !result.contains("{{") {
                break;
            }
            let resolved = self.resolve(&result, scope);
            if resolved == result {
                break;
            }
            result = resolved;
        }
        result
    }
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
            let n: i64 = ri.parse().unwrap_or_else(|_| panic!("randomInt is numeric: {ri}"));
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
}
