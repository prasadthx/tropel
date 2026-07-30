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

            // Fast JSON parse — validates JSON from a string, returns the
            // canonical JSON string for JS-side JSON.parse().
            // Uses simd-json internally for ~2-4x faster parsing.
            // Returns Option<String> (None on parse error) because rquickjs
            // Func::from doesn't support tropel_core::Result error types.
            let _ = globals.set(
                "__tropel_native_json_parse",
                Func::from(|s: String| -> Option<String> {
                    let value = json_parse(&s).ok()?;
                    serde_json::to_string(&value).ok()
                }),
            );

            // Fast JSON stringify — converts a JSON string to canonical form.
            let _ = globals.set(
                "__tropel_native_json_stringify",
                Func::from(|s: String| -> Option<String> {
                    let value = json_parse(&s).ok()?;
                    json_stringify(&value).ok()
                }),
            );

            // JSON get — extract a value from JSON using a dot-path.
            // Returns the extracted value as a JSON string, or null/empty if not found.
            let _ = globals.set(
                "__tropel_native_json_get",
                Func::from(|json_str: String, path: String| -> Option<String> {
                    let value = json_parse(&json_str).ok()?;
                    let extracted = json_get(&value, &path)?;
                    serde_json::to_string(extracted).ok()
                }),
            );
        });

        tracing::debug!("Installed JSON native module with simd-json backend");
        Ok(())
    }
}

/// Fast JSON parse using simd-json.
///
/// Parses a JSON string into a `serde_json::Value` using the simd-json
/// backend, which is ~2-4x faster than `serde_json::from_str` for typical
/// payloads. The string is converted to bytes for simd-json's mutable
/// parsing interface.
pub fn json_parse(s: &str) -> Result<Value> {
    let mut bytes = s.as_bytes().to_vec();
    simd_json::serde::from_slice(&mut bytes)
        .map_err(|e| tropel_core::TropelError::Parse(format!("JSON parse error: {}", e)))
}

/// Fast JSON stringify using serde_json (stringify is already efficient).
/// simd-json provides a to_string but it works on BorrowedValue; serde_json's
/// to_string on Value is already optimal.
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
    fn test_large_json() {
        // Build a realistically-sized JSON payload
        let data: Vec<serde_json::Value> = (0..1000)
            .map(|i| serde_json::json!({
                "id": i,
                "name": format!("item-{}", i),
                "active": i % 2 == 0,
                "tags": ["a", "b", "c"]
            }))
            .collect();
        let json_str = serde_json::to_string(&data).unwrap();

        let start = std::time::Instant::now();
        let parsed = json_parse(&json_str).unwrap();
        let duration = start.elapsed();

        assert!(parsed.is_array());
        assert_eq!(parsed.as_array().unwrap().len(), 1000);
        // Just verify it completed without timing out
        assert!(duration < std::time::Duration::from_secs(5));
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

    #[test]
    fn test_invalid_json() {
        assert!(json_parse("not valid json").is_err());
        assert!(json_parse("").is_err());
    }
}
