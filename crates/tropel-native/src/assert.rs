use crate::NativeModule;
use rquickjs::function::Func;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct AssertModule;

impl NativeModule for AssertModule {
    fn name(&self) -> &str {
        "__tropel_native_assert"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // deep_equal requires serde_json::Value which isn't directly
            // supported by rquickjs Func::from — the JS chai-shim falls
            // back to JSON.stringify comparison, which is sufficient for now.
            let _ = globals.set("__tropel_native_assert_ready", Func::from(|| -> bool { true }));
        });

        tracing::debug!("Installed assert native module");
        Ok(())
    }
}

/// Deep equality check between two JSON values.
pub fn deep_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    a == b
}

pub fn is_string(val: &serde_json::Value) -> bool { val.is_string() }
pub fn is_number(val: &serde_json::Value) -> bool { val.is_number() }
pub fn is_array(val: &serde_json::Value) -> bool { val.is_array() }
pub fn is_object(val: &serde_json::Value) -> bool { val.is_object() }
pub fn is_null(val: &serde_json::Value) -> bool { val.is_null() }
pub fn is_boolean(val: &serde_json::Value) -> bool { val.is_boolean() }

pub fn length(val: &serde_json::Value) -> Option<usize> {
    match val {
        serde_json::Value::String(s) => Some(s.len()),
        serde_json::Value::Array(arr) => Some(arr.len()),
        _ => None,
    }
}

pub fn matches(value: &str, pattern: &str) -> Result<bool> {
    let re = regex::Regex::new(pattern)
        .map_err(|e| tropel_core::TropelError::Parse(format!("Invalid regex '{}': {}", pattern, e)))?;
    Ok(re.is_match(value))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deep_equal() {
        let a = serde_json::json!({"a": 1});
        let b = serde_json::json!({"a": 1});
        let c = serde_json::json!({"a": 2});
        assert!(deep_equal(&a, &b));
        assert!(!deep_equal(&a, &c));
    }

    #[test]
    fn test_type_checks() {
        assert!(is_string(&serde_json::json!("hello")));
        assert!(is_number(&serde_json::json!(42)));
        assert!(is_array(&serde_json::json!([1, 2, 3])));
        assert!(is_object(&serde_json::json!({"a": 1})));
        assert!(is_null(&serde_json::json!(null)));
        assert!(is_boolean(&serde_json::json!(true)));
    }

    #[test]
    fn test_length() {
        assert_eq!(length(&serde_json::json!("hello")), Some(5));
        assert_eq!(length(&serde_json::json!([1, 2, 3])), Some(3));
        assert_eq!(length(&serde_json::json!(42)), None);
    }

    #[test]
    fn test_matches() {
        assert!(matches("hello123", "^[a-z]+\\d+$").unwrap());
        assert!(!matches("123abc", "^[a-z]+\\d+$").unwrap());
    }
}
