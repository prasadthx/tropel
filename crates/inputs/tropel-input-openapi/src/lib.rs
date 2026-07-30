//! # tropel-input-openapi
//!
//! Input adapter that reads [OpenAPI 3.x][openapi] specifications and produces
//
// Allow dead code on deserialization-only structs.
// OauthFlow, OasSecurityScheme, and related types are only used
// for JSON deserialization and never read directly after that.
#![allow(dead_code)]
//
//! a protocol-agnostic `Scenario`. Each operation (path + method combination)
//! becomes one `ScenarioItem`.
//!
//! [openapi]: https://spec.openapis.org/oas/v3.0.3
//!
//! ## Mapping
//!
//! | OpenAPI field | Scenario field |
//! |---------------|---------------|
//! | `info.title` | `ScenarioInfo.name` |
//! | `paths.{path}.{method}` | `ScenarioItem` (one per operation) |
//! | `operationId` or `summary` | `ScenarioItem.name` |
//! | Path parameters (e.g. `{id}`) | Replaced with example values or `__example__` |
//! | `parameters` (query, header) | `request.headers` / `request.query_params` |
//! | `requestBody` | `request.body` |
//! | `security` | `request.auth` |
//! | `servers[0].url` | Base URL prepended to `request.url` |

use std::collections::HashMap;
use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_core::types::{ApiKeyLocation, AuthConfig, Body, Method, Request};
use tropel_core::{Result, TropelError};
use tropel_ext::traits::{InputAdapter, InputAdapterRegistration};
use serde::Deserialize;

// ── OpenAPI 3.x data model (minimal — only what we need) ────────

/// OpenAPI 3.x root document.
#[derive(Debug, Deserialize)]
struct OasDoc {
    openapi: String,
    info: OasInfo,
    #[serde(default)]
    servers: Vec<OasServer>,
    paths: HashMap<String, OasPathItem>,
    #[serde(default)]
    components: Option<OasComponents>,
    #[serde(default)]
    security: Vec<HashMap<String, Vec<String>>>,
}

#[derive(Debug, Deserialize)]
struct OasInfo {
    title: String,
    #[serde(default)]
    version: String,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OasServer {
    url: String,
    #[serde(default)]
    description: Option<String>,
}

/// A path item — can have one or more operations.
#[derive(Debug, Deserialize)]
struct OasPathItem {
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    get: Option<OasOperation>,
    #[serde(default)]
    put: Option<OasOperation>,
    #[serde(default)]
    post: Option<OasOperation>,
    #[serde(default)]
    delete: Option<OasOperation>,
    #[serde(default)]
    options: Option<OasOperation>,
    #[serde(default)]
    head: Option<OasOperation>,
    #[serde(default)]
    patch: Option<OasOperation>,
    #[serde(default)]
    trace: Option<OasOperation>,
    #[serde(default)]
    parameters: Vec<OasParameter>,
}

impl OasPathItem {
    fn operations(&self) -> Vec<(&str, &OasOperation)> {
        let mut ops = Vec::new();
        if let Some(ref op) = self.get { ops.push(("get", op)); }
        if let Some(ref op) = self.put { ops.push(("put", op)); }
        if let Some(ref op) = self.post { ops.push(("post", op)); }
        if let Some(ref op) = self.delete { ops.push(("delete", op)); }
        if let Some(ref op) = self.options { ops.push(("options", op)); }
        if let Some(ref op) = self.head { ops.push(("head", op)); }
        if let Some(ref op) = self.patch { ops.push(("patch", op)); }
        if let Some(ref op) = self.trace { ops.push(("trace", op)); }
        ops
    }
}

#[derive(Debug, Deserialize)]
struct OasOperation {
    #[serde(default, rename = "operationId")]
    operation_id: Option<String>,
    #[serde(default)]
    summary: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    parameters: Vec<OasParameter>,
    #[serde(default, rename = "requestBody")]
    request_body: Option<OasRequestBody>,
    #[serde(default)]
    responses: HashMap<String, OasResponse>,
    #[serde(default)]
    security: Option<Vec<HashMap<String, Vec<String>>>>,
    #[serde(default)]
    deprecated: bool,
}

#[derive(Debug, Deserialize)]
struct OasParameter {
    name: String,
    #[serde(default)]
    r#in: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    required: bool,
    #[serde(default)]
    schema: Option<OasSchema>,
    #[serde(default)]
    example: Option<serde_json::Value>,
    #[serde(default)]
    examples: Option<HashMap<String, OasExample>>,
}

#[derive(Debug, Deserialize)]
struct OasExample {
    #[serde(default)]
    value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct OasSchema {
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    format: Option<String>,
    #[serde(default)]
    example: Option<serde_json::Value>,
    #[serde(default)]
    default: Option<serde_json::Value>,
    #[serde(default)]
    properties: Option<HashMap<String, Box<OasSchema>>>,
    #[serde(default)]
    items: Option<Box<OasSchema>>,
    #[serde(default)]
    required: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct OasRequestBody {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    content: HashMap<String, OasMediaType>,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Deserialize)]
struct OasMediaType {
    #[serde(default)]
    schema: Option<OasSchema>,
    #[serde(default)]
    example: Option<serde_json::Value>,
    #[serde(default)]
    examples: Option<HashMap<String, OasExample>>,
}

#[derive(Debug, Deserialize)]
struct OasResponse {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    content: Option<HashMap<String, OasMediaType>>,
}

#[derive(Debug, Deserialize)]
struct OasComponents {
    #[serde(default)]
    schemas: Option<HashMap<String, OasSchema>>,
    #[serde(default, rename = "securitySchemes")]
    security_schemes: Option<HashMap<String, OasSecurityScheme>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OasSecurityScheme {
    // More specific variants first. Http requires `scheme`, OAuth2
    // requires `flows`, OpenIdConnect requires `openIdConnectUrl`.
    // ApiKey has all-optional fields besides `type`, so it greedily
    // matches any JSON with `type` — keep it last.
    Http {
        r#type: String,
        scheme: String,
        #[serde(default, rename = "bearerFormat")]
        bearer_format: Option<String>,
    },
    OAuth2 {
        r#type: String,
        flows: OauthFlows,
    },
    OpenIdConnect {
        r#type: String,
        #[serde(alias = "openIdConnectUrl")]
        open_id_connect_url: String,
    },
    ApiKey {
        r#type: String,
        #[serde(alias = "name")]
        name: Option<String>,
        #[serde(alias = "in")]
        location: Option<String>,
    },
}

#[derive(Debug, Deserialize)]
struct OauthFlows {
    #[serde(default, rename = "authorizationCode")]
    authorization_code: Option<OauthFlow>,
    #[serde(default)]
    implicit: Option<OauthFlow>,
    #[serde(default)]
    password: Option<OauthFlow>,
    #[serde(default, rename = "clientCredentials")]
    client_credentials: Option<OauthFlow>,
}

#[derive(Debug, Deserialize)]
struct OauthFlow {
    #[serde(default, rename = "authorizationUrl")]
    authorization_url: Option<String>,
    #[serde(default, rename = "tokenUrl")]
    token_url: Option<String>,
    #[serde(default)]
    scopes: HashMap<String, String>,
}

// ── InputAdapter implementation ─────────────────────────────────

/// Input adapter for OpenAPI 3.x specification files.
pub struct OpenApiInputAdapter;

inventory::submit!(InputAdapterRegistration::new("openapi", || Box::new(OpenApiInputAdapter)));

impl InputAdapter for OpenApiInputAdapter {
    fn id(&self) -> &str {
        "openapi"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        if let Ok(text) = std::str::from_utf8(bytes) {
            let text = text.trim_start();
            if text.starts_with('{') {
                // Detect by looking for OpenAPI required fields
                text.contains("\"openapi\"") && text.contains("\"paths\"")
                    && text.contains("\"info\"")
                    && !text.contains("postman")  // exclude Postman
                    && !text.contains("\"log\"")    // exclude HAR
            } else {
                false
            }
        } else {
            false
        }
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        let doc: OasDoc = serde_json::from_slice(bytes)
            .map_err(|e| TropelError::Parse(format!("Failed to parse OpenAPI spec: {}", e)))?;

        if doc.paths.is_empty() {
            return Err(TropelError::Parse(
                "OpenAPI spec contains no paths".into(),
            ));
        }

        // Build base URL from servers
        let base_url = doc.servers.first()
            .map(|s| s.url.trim_end_matches('/').to_string())
            .unwrap_or_default();

        // Flatten global security requirements
        let global_security = doc.security.clone();

        let mut items: Vec<ScenarioItem> = Vec::new();
        let mut index = 0usize;

        // Sort paths for deterministic output
        let mut path_keys: Vec<&String> = doc.paths.keys().collect();
        path_keys.sort();

        for path_str in path_keys {
            let path_item = &doc.paths[path_str];
            let _path_summary = path_item.summary.as_deref();

            for (method, operation) in path_item.operations() {
                if operation.deprecated {
                    continue;
                }

                let item_name = operation.operation_id.clone()
                    .or_else(|| operation.summary.clone())
                    .unwrap_or_else(|| format!("{} {}", method.to_uppercase(), path_str));

                let url = format!("{}{}", base_url, path_str);

                // Collect parameters — path-item-level + operation-level
                let params: Vec<&OasParameter> = path_item.parameters.iter()
                    .chain(operation.parameters.iter())
                    .collect();

                let mut headers: HashMap<String, String> = HashMap::new();
                let mut query_params: HashMap<String, String> = HashMap::new();
                let mut path_params: HashMap<String, String> = HashMap::new();

                for param in &params {
                    let val = extract_param_value(param);
                    match param.r#in.as_str() {
                        "header" => { headers.insert(param.name.clone(), val); }
                        "query" => { query_params.insert(param.name.clone(), val); }
                        "path" => { path_params.insert(param.name.clone(), val); }
                        _ => {}
                    }
                }

                // Resolve path template parameters (e.g. /users/{userId})
                let resolved_url = if !path_params.is_empty() {
                    let mut resolved = url.clone();
                    for (key, val) in &path_params {
                        resolved = resolved.replace(&format!("{{{}}}", key), val);
                        resolved = resolved.replace(&format!("{{{{{}}}}}", key), val); // double-brace
                    }
                    resolved
                } else {
                    url.clone()
                };

                // Build request body
                let body = operation.request_body.as_ref()
                    .and_then(|rb| build_request_body(rb));

                // Resolve auth
                let auth = resolve_auth(&operation, &global_security, &doc.components);

                items.push(ScenarioItem {
                    id: format!("openapi-item-{}", index),
                    name: item_name,
                    request: Some(Request {
                        url: resolved_url,
                        method: Method::from_str(method).unwrap_or(Method::GET),
                        headers,
                        query_params,
                        body,
                        auth,
                        certificate: None,
                        follow_redirects: true,
                        timeout: None,
                    }),
                    prerequest: None,
                    test: None,
                    assertions: vec![],
                    items: vec![],
                });

                index += 1;
            }
        }

        if items.is_empty() {
            return Err(TropelError::Parse(
                "OpenAPI spec has paths but no non-deprecated operations".into(),
            ));
        }

        Ok(Scenario {
            info: ScenarioInfo {
                name: doc.info.title,
                description: doc.info.description
                    .or_else(|| Some(format!("OpenAPI {} — {}", doc.openapi, doc.info.version))),
                schema: None,
            },
            items,
            variables: HashMap::new(),
            auth: None,
        })
    }
}

/// Extract a parameter value from an OpenAPI parameter definition.
fn extract_param_value(param: &OasParameter) -> String {
    // Try example, then examples, then schema example/default, then type-based default
    if let Some(ref example) = param.example {
        return value_to_string(example);
    }
    if let Some(ref examples) = param.examples {
        if let Some((_, ex)) = examples.iter().next() {
            if let Some(ref val) = ex.value {
                return value_to_string(val);
            }
        }
    }
    if let Some(ref schema) = param.schema {
        if let Some(ref example) = schema.example {
            return value_to_string(example);
        }
        if let Some(ref default) = schema.default {
            return value_to_string(default);
        }
        // Generate type-based defaults
        match schema.r#type.as_deref() {
            Some("integer") | Some("number") => return "1".to_string(),
            Some("boolean") => return "true".to_string(),
            Some("string") if schema.format.as_deref() == Some("uuid") => {
                return "550e8400-e29b-41d4-a716-446655440000".to_string()
            }
            Some("string") if schema.format.as_deref() == Some("email") => {
                return "user@example.com".to_string()
            }
            Some("string") if schema.format.as_deref() == Some("date") => {
                return "2024-01-01".to_string()
            }
            Some("string") if schema.format.as_deref() == Some("date-time") => {
                return "2024-01-01T00:00:00Z".to_string()
            }
            Some("string") => return "example".to_string(),
            _ => {}
        }
    }
    "__example__".to_string()
}

/// Convert a serde_json::Value to a string (without quotes for strings).
fn value_to_string(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Build a Request body from an OpenAPI Request Body object.
fn build_request_body(rb: &OasRequestBody) -> Option<Body> {
    // Prefer application/json
    if let Some(mt) = rb.content.get("application/json") {
        let json_val = mt.example.clone()
            .or_else(|| generate_schema_example(mt.schema.as_ref()));
        return Some(json_val.map_or(Body::Json(serde_json::Value::Null), Body::Json));
    }

    // Then application/x-www-form-urlencoded
    if let Some(mt) = rb.content.get("application/x-www-form-urlencoded") {
        if let Some(ref schema) = mt.schema {
            let mut map = HashMap::new();
            if let Some(ref props) = schema.properties {
                for (name, prop_schema) in props {
                    let val = prop_schema.example.clone()
                        .or_else(|| prop_schema.default.clone())
                        .map(|v| value_to_string(&v))
                        .unwrap_or_else(|| "example".to_string());
                    map.insert(name.clone(), val);
                }
            }
            return Some(Body::UrlEncoded(map));
        }
        return None;
    }

    // Then multipart/form-data
    if let Some(mt) = rb.content.get("multipart/form-data") {
        if let Some(ref schema) = mt.schema {
            let mut map = HashMap::new();
            if let Some(ref props) = schema.properties {
                for (name, prop_schema) in props {
                    let val = prop_schema.example.clone()
                        .or_else(|| prop_schema.default.clone())
                        .map(|v| value_to_string(&v))
                        .unwrap_or_else(|| "example".to_string());
                    map.insert(name.clone(), val);
                }
            }
            return Some(Body::FormData(map));
        }
        return None;
    }

    // Fallback: take any content type
    if let Some((_, mt)) = rb.content.iter().next() {
        let json_val = mt.example.clone()
            .or_else(|| generate_schema_example(mt.schema.as_ref()));
        return Some(json_val.map_or(Body::Raw(String::new()), |v| Body::Json(v)));
    }

    None
}

/// Generate an example value from an OpenAPI schema.
fn generate_schema_example(schema: Option<&OasSchema>) -> Option<serde_json::Value> {
    let schema = schema?;

    if let Some(ref example) = schema.example {
        return Some(example.clone());
    }
    if let Some(ref default) = schema.default {
        return Some(default.clone());
    }

    match schema.r#type.as_deref() {
        Some("object") => {
            let mut obj = serde_json::Map::new();
            if let Some(ref props) = schema.properties {
                for (name, prop_schema) in props {
                    let val = generate_schema_example(Some(prop_schema))
                        .unwrap_or(serde_json::Value::Null);
                    obj.insert(name.clone(), val);
                }
            }
            Some(serde_json::Value::Object(obj))
        }
        Some("array") => {
            if let Some(ref items) = schema.items {
                let val = generate_schema_example(Some(items))
                    .unwrap_or(serde_json::Value::Null);
                Some(serde_json::Value::Array(vec![val]))
            } else {
                Some(serde_json::Value::Array(vec![]))
            }
        }
        Some("string") if schema.format.as_deref() == Some("uuid") => {
            Some(serde_json::Value::String("550e8400-e29b-41d4-a716-446655440000".into()))
        }
        Some("string") if schema.format.as_deref() == Some("email") => {
            Some(serde_json::Value::String("user@example.com".into()))
        }
        Some("string") if schema.format.as_deref() == Some("date") => {
            Some(serde_json::Value::String("2024-01-01".into()))
        }
        Some("string") if schema.format.as_deref() == Some("date-time") => {
            Some(serde_json::Value::String("2024-01-01T00:00:00Z".into()))
        }
        Some("string") => Some(serde_json::Value::String("string".into())),
        Some("integer") | Some("number") => {
            Some(serde_json::Value::Number(serde_json::Number::from(1)))
        }
        Some("boolean") => Some(serde_json::Value::Bool(true)),
        _ => None,
    }
}

/// Resolve auth for an operation, considering operation-level,
/// global security, and component security schemes.
fn resolve_auth(
    operation: &OasOperation,
    global_security: &[HashMap<String, Vec<String>>],
    components: &Option<OasComponents>,
) -> Option<AuthConfig> {
    // Use as_deref() to get Option<&[Vec<...>]>, then unwrap_or to the global
    // The Option<&Vec<...>> from operation.security.as_ref() gives us &Vec<...>
    // which derefs to &[...], matching global_security's type.
    let sec_requirements: &[HashMap<String, Vec<String>>] = match operation.security.as_ref() {
        Some(v) => v.as_slice(),
        None => global_security,
    };

    if sec_requirements.is_empty() {
        return None;
    }

    // Take the first security requirement
    let first_sec = &sec_requirements[0];
    let (scheme_name, _scopes) = first_sec.iter().next()?;

    // Look up the scheme in components
    let schemes = components.as_ref()
        .and_then(|c| c.security_schemes.as_ref())?;
    let scheme = schemes.get(scheme_name)?;

    match scheme {
        OasSecurityScheme::Http { scheme: http_scheme, .. } => {
            match http_scheme.to_lowercase().as_str() {
                "bearer" => Some(AuthConfig::Bearer {
                    token: "__token__".to_string(),
                }),
                "basic" => Some(AuthConfig::Basic {
                    username: "__username__".to_string(),
                    password: "__password__".to_string(),
                }),
                _ => None,
            }
        }
        OasSecurityScheme::ApiKey { name, location, .. } => {
            let key_name = name.clone().unwrap_or_else(|| "api_key".to_string());
            let loc = location.as_deref().unwrap_or("header");
            let api_location = if loc == "query" {
                ApiKeyLocation::Query
            } else {
                ApiKeyLocation::Header
            };
            Some(AuthConfig::ApiKey {
                key: key_name,
                value: "__api_key__".to_string(),
                location: api_location,
            })
        }
        _ => None,
    }
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_openapi() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.3",
            "info": {"title": "Test API", "version": "1.0.0"},
            "paths": {}
        }"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_postman_not_openapi() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(!adapter.detect(data), "Postman JSON should not be detected as OpenAPI");
    }

    #[test]
    fn test_detect_har_not_openapi() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{"log":{"version":"1.2","entries":[]}}"#;
        assert!(!adapter.detect(data), "HAR should not be detected as OpenAPI");
    }

    #[test]
    fn test_parse_simple_get() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.3",
            "info": {"title": "Pet Store", "version": "1.0.0"},
            "servers": [{"url": "https://api.petstore.com/v1"}],
            "paths": {
                "/pets": {
                    "get": {
                        "operationId": "listPets",
                        "summary": "List all pets",
                        "parameters": [
                            {"name": "limit", "in": "query", "schema": {"type": "integer"}}
                        ],
                        "responses": {
                            "200": {"description": "A list of pets"}
                        }
                    }
                }
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.info.name, "Pet Store");
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].name, "listPets");
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.petstore.com/v1/pets");
        assert_eq!(req.method, Method::GET);
        assert_eq!(req.query_params.get("limit").unwrap(), "1");
    }

    #[test]
    fn test_parse_multiple_paths() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "Multi", "version": "1.0"},
            "paths": {
                "/users": {
                    "get": {"operationId": "listUsers", "responses": {"200": {"description": "Users"}}}
                },
                "/users/{id}": {
                    "get": {"operationId": "getUser", "responses": {"200": {"description": "User"}}},
                    "delete": {"operationId": "deleteUser", "responses": {"204": {"description": "Deleted"}}}
                }
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 3);
        assert_eq!(scenario.items[0].name, "listUsers");
        assert_eq!(scenario.items[1].name, "getUser");
        assert_eq!(scenario.items[2].name, "deleteUser");
    }

    #[test]
    fn test_path_parameter_resolution() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {
                "/users/{userId}/orders/{orderId}": {
                    "get": {
                        "operationId": "getUserOrder",
                        "parameters": [
                            {"name": "userId", "in": "path", "required": true, "schema": {"type": "integer"}},
                            {"name": "orderId", "in": "path", "required": true, "schema": {"type": "string"}}
                        ],
                        "responses": {"200": {"description": "Order"}}
                    }
                }
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        // Path params should be resolved
        assert!(!req.url.contains('{'), "Path params should be resolved: {}", req.url);
        assert_eq!(req.url, "/users/1/orders/example");
    }

    #[test]
    fn test_deprecated_skipped() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "Test", "version": "1.0"},
            "paths": {
                "/active": {
                    "get": {"operationId": "activeOp", "responses": {"200": {"description": "OK"}}}
                },
                "/deprecated": {
                    "get": {"operationId": "deprecatedOp", "deprecated": true, "responses": {"200": {"description": "OK"}}}
                }
            }
        }"#;

        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].name, "activeOp");
    }

    #[test]
    fn test_empty_paths_errors() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "Empty", "version": "1.0"},
            "paths": {}
        }"#;
        let result = adapter.parse(data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no paths"));
    }
}
