use crate::NativeModule;
use serde_json::Value;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct AssertModule;

impl NativeModule for AssertModule {
    fn name(&self) -> &str {
        "__tropel_native_assert"
    }

    fn install(&self, _ctx: &JsContext) -> Result<()> {
        tracing::debug!("Installed assert native module");
        Ok(())
    }
}

/// Deep equality check between two JSON values.
pub fn deep_equal(a: &Value, b: &Value) -> bool {
    a == b
}

/// Type check: is the value a string?
pub fn is_string(val: &Value) -> bool {
    val.is_string()
}

/// Type check: is the value a number?
pub fn is_number(val: &Value) -> bool {
    val.is_number()
}

/// Type check: is the value an array?
pub fn is_array(val: &Value) -> bool {
    val.is_array()
}

/// Type check: is the value an object?
pub fn is_object(val: &Value) -> bool {
    val.is_object()
}

/// Type check: is the value null?
pub fn is_null(val: &Value) -> bool {
    val.is_null()
}

/// Type check: is the value boolean?
pub fn is_boolean(val: &Value) -> bool {
    val.is_boolean()
}

/// Get the length of a string or array.
pub fn length(val: &Value) -> Option<usize> {
    match val {
        Value::String(s) => Some(s.len()),
        Value::Array(arr) => Some(arr.len()),
        _ => None,
    }
}

/// Check that a string matches a regex pattern.
pub fn matches(value: &str, pattern: &str) -> Result<bool> {
    let re = regex::Regex::new(pattern)
        .map_err(|e| tropel_core::TropelError::Parse(format!("Invalid regex '{}': {}", pattern, e)))?;
    Ok(re.is_match(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deep_equal() {
        assert!(deep_equal(&json!({"a": 1}), &json!({"a": 1})));
        assert!(!deep_equal(&json!({"a": 1}), &json!({"a": 2})));
    }

    #[test]
    fn test_type_checks() {
        assert!(is_string(&json!("hello")));
        assert!(is_number(&json!(42)));
        assert!(is_array(&json!([1, 2, 3])));
        assert!(is_object(&json!({"a": 1})));
        assert!(is_null(&json!(null)));
        assert!(is_boolean(&json!(true)));
    }

    #[test]
    fn test_length() {
        assert_eq!(length(&json!("hello")), Some(5));
        assert_eq!(length(&json!([1, 2, 3])), Some(3));
        assert_eq!(length(&json!(42)), None);
    }

    #[test]
    fn test_matches() {
        assert!(matches("hello123", r"^[a-z]+\d+$").unwrap());
        assert!(!matches("123abc", r"^[a-z]+\d+$").unwrap());
    }
}
