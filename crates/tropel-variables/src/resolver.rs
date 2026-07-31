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
}
