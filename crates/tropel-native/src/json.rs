use crate::NativeModule;
use rquickjs::function::Func;
use serde_json::Value;
use tropel_core::Result;
use tropel_js::JsContext;

pub struct JsonModule;

impl NativeModule for JsonModule {
    fn name(&self) -> &str {
        "__tropel_native_json"
    }

    fn install(&self, ctx: &JsContext) -> Result<()> {
        ctx.with_ctx(|rq_ctx| {
            let globals = rq_ctx.globals();

            // uuid generation — simple string return
            let _ = globals.set("__tropel_native_uuid", Func::from(|| -> String {
                uuid::Uuid::new_v4().to_string()
            }));
        });

        tracing::debug!("Installed JSON native module");
        Ok(())
    }
}

/// Fast JSON parse.
pub fn json_parse(s: &str) -> Result<Value> {
    serde_json::from_str(s)
        .map_err(|e| tropel_core::TropelError::Parse(format!("JSON parse error: {}", e)))
}

/// Fast JSON stringify.
pub fn json_stringify(value: &Value) -> Result<String> {
    serde_json::to_string(value)
        .map_err(|e| tropel_core::TropelError::Parse(format!("JSON stringify error: {}", e)))
}

/// Pretty-print JSON.
pub fn json_stringify_pretty(value: &Value) -> Result<String> {
    serde_json::to_string_pretty(value)
        .map_err(|e| tropel_core::TropelError::Parse(format!("JSON stringify error: {}", e)))
}

/// Extract a value from a JSON document using a dot-path.
pub fn json_get<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = value;
    for part in parts {
        match current {
            Value::Object(map) => {
                current = map.get(part)?;
            }
            Value::Array(arr) => {
                let index: usize = part.parse().ok()?;
                current = arr.get(index)?;
            }
            _ => return None,
        }
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {
        let original = serde_json::json!([1, 2, 3]);
        let json_str = json_stringify(&original).unwrap();
        let parsed = json_parse(&json_str).unwrap();
        assert_eq!(original, parsed);
    }

    #[test]
    fn test_json_get() {
        let value = serde_json::json!({
            "user": {
                "name": "Alice",
                "address": {
                    "city": "Wonderland"
                }
            }
        });
        assert_eq!(json_get(&value, "user.name"), Some(&serde_json::json!("Alice")));
        assert_eq!(json_get(&value, "user.address.city"), Some(&serde_json::json!("Wonderland")));
        assert_eq!(json_get(&value, "nonexistent"), None);
    }
}
