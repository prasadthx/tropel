//! # tropel-input-openapi
//!
//! Input adapter that reads [OpenAPI 3.x][openapi] and [Swagger 2.0][swagger]
//! specifications and produces a protocol-agnostic `Scenario`. Each
//! operation (path + method combination) becomes one `ScenarioItem`.
//!
//! [openapi]: https://spec.openapis.org/oas/v3.0.3
//! [swagger]: https://swagger.io/specification/v2/
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
//! | `servers[0].url` | Base URL prepended to `request.url` (server variables substituted) |
//!
//! ## Robustness
//!
//! - Intra-document `$ref` pointers (`#/components/...`) are resolved before
//!   parsing, so parameter/body/security refs in real specs (Stripe, GitHub)
//!   work instead of hard-failing serde.
//! - Swagger 2.0 documents are normalized to an OpenAPI 3.x shape.
//! - Server variables (`https://{env}.example.com`) use their default value.

use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use tropel_sdk::{ApiKeyLocation, AuthConfig, Body, Method, Request};
use tropel_sdk::{InputAdapter, InputAdapterRegistration};
use tropel_sdk::{Result, TropelError};
use tropel_sdk::{Scenario, ScenarioInfo, ScenarioItem};

/// Parse an OpenAPI document as JSON, falling back to YAML (OpenAPI specs
/// are commonly authored in YAML). Returns the document as a `Value` tree
/// or `None` when it is neither valid JSON nor valid YAML.
fn parse_doc(bytes: &[u8]) -> Option<Value> {
    if let Ok(v) = serde_json::from_slice(bytes) {
        return Some(v);
    }
    let text = std::str::from_utf8(bytes).ok()?;
    yaml_serde::from_str::<Value>(text).ok()
}

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
// `description` is parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
struct OasServer {
    url: String,
    #[serde(default)]
    description: Option<String>,
    /// Server variables (e.g. `https://{env}.example.com`).
    #[serde(default)]
    variables: HashMap<String, OasServerVariable>,
}

#[derive(Debug, Deserialize)]
struct OasServerVariable {
    #[serde(default)]
    default: Option<String>,
    #[serde(default)]
    r#enum: Option<Vec<String>>,
}

/// A path item — can have one or more operations.
// `summary`/`description` are parsed for spec fidelity but not consumed.
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
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
        if let Some(ref op) = self.get {
            ops.push(("get", op));
        }
        if let Some(ref op) = self.put {
            ops.push(("put", op));
        }
        if let Some(ref op) = self.post {
            ops.push(("post", op));
        }
        if let Some(ref op) = self.delete {
            ops.push(("delete", op));
        }
        if let Some(ref op) = self.options {
            ops.push(("options", op));
        }
        if let Some(ref op) = self.head {
            ops.push(("head", op));
        }
        if let Some(ref op) = self.patch {
            ops.push(("patch", op));
        }
        if let Some(ref op) = self.trace {
            ops.push(("trace", op));
        }
        ops
    }
}

#[derive(Debug, Deserialize)]
// `description`/`responses` are parsed for spec fidelity but not consumed.
#[allow(dead_code)]
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
// `description`/`required` are parsed for spec fidelity but not consumed.
#[allow(dead_code)]
struct OasParameter {
    #[serde(default)]
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
// `required` is parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
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
// `description`/`required` are parsed for spec fidelity but not consumed.
#[allow(dead_code)]
struct OasRequestBody {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    content: HashMap<String, OasMediaType>,
    #[serde(default)]
    required: bool,
}

#[derive(Debug, Deserialize)]
// `examples` is parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
struct OasMediaType {
    #[serde(default)]
    schema: Option<OasSchema>,
    #[serde(default)]
    example: Option<serde_json::Value>,
    #[serde(default)]
    examples: Option<HashMap<String, OasExample>>,
}

#[derive(Debug, Deserialize)]
// `description`/`content` are parsed for spec fidelity but not consumed.
#[allow(dead_code)]
struct OasResponse {
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    content: Option<HashMap<String, OasMediaType>>,
}

#[derive(Debug, Deserialize)]
// `schemas` is parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
struct OasComponents {
    #[serde(default)]
    schemas: Option<HashMap<String, OasSchema>>,
    #[serde(default, rename = "securitySchemes")]
    security_schemes: Option<HashMap<String, OasSecurityScheme>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
// `type`/`flows`/`bearerFormat`/`openIdConnectUrl` are parsed for spec
// fidelity but not consumed downstream; `OAuth2` carries the largest payload
// so the size-difference lint is suppressed.
#[allow(dead_code, clippy::large_enum_variant)]
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
// All four flows are parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
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
// URL/scopes fields are parsed for spec fidelity but not consumed downstream.
#[allow(dead_code)]
struct OauthFlow {
    #[serde(default, rename = "authorizationUrl")]
    authorization_url: Option<String>,
    #[serde(default, rename = "tokenUrl")]
    token_url: Option<String>,
    #[serde(default)]
    scopes: HashMap<String, String>,
}

// ── InputAdapter implementation ─────────────────────────────────

/// Input adapter for OpenAPI 3.x / Swagger 2.0 specification files.
pub struct OpenApiInputAdapter;

inventory::submit!(
    InputAdapterRegistration::new("openapi", || Box::new(OpenApiInputAdapter)).with_priority(20)
);

impl InputAdapter for OpenApiInputAdapter {
    fn id(&self) -> &str {
        "openapi"
    }

    fn detect(&self, bytes: &[u8]) -> bool {
        // Structural detection: a spec (JSON or YAML) with a top-level
        // `openapi` (3.x) or `swagger` (2.0) version string plus `info` and
        // `paths`. No substring matching — a HAR capture or Postman export
        // may mention these words in content and must not be detected.
        let Some(value) = parse_doc(bytes) else {
            return false;
        };
        if !value.is_object() {
            return false;
        }
        let version = value
            .get("openapi")
            .or_else(|| value.get("swagger"))
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let has_version = version.starts_with("3.") || version.starts_with("2.");
        has_version
            && value.get("info").map(|v| v.is_object()).unwrap_or(false)
            && value.get("paths").map(|v| v.is_object()).unwrap_or(false)
    }

    fn parse(&self, bytes: &[u8]) -> Result<Scenario> {
        // 1. Parse into a Value tree (JSON or YAML) so we can normalize +
        //    resolve refs before the typed (serde) model, which hard-fails
        //    on $ref.
        let mut doc: Value = parse_doc(bytes).ok_or_else(|| {
            TropelError::Parse("Failed to parse OpenAPI spec: not valid JSON or YAML".into())
        })?;

        // 2. Swagger 2.0 → normalize to an OpenAPI 3.x-shaped document.
        if is_swagger2(&doc) {
            doc = normalize_swagger2(doc).map_err(|e| {
                TropelError::Parse(format!("Failed to normalize Swagger 2.0 spec: {}", e))
            })?;
        }

        // 3. Resolve intra-document $refs (#/components/...) so parameter /
        //    body / security references work instead of failing serde.
        doc = resolve_refs(&doc);

        // 4. Typed parse.
        let parsed: OasDoc = serde_json::from_value(doc)
            .map_err(|e| TropelError::Parse(format!("Failed to parse OpenAPI spec: {}", e)))?;

        parse_typed(parsed)
    }
}

/// Parse a typed OAS 3.x document into a Scenario.
fn parse_typed(doc: OasDoc) -> Result<Scenario> {
    if doc.paths.is_empty() {
        return Err(TropelError::Parse("OpenAPI spec contains no paths".into()));
    }

    // Build base URL from servers (substituting server variables).
    let base_url = doc
        .servers
        .first()
        .map(resolve_server_url)
        .unwrap_or_default();

    // Flatten global security requirements
    let global_security = doc.security.clone();

    let mut items: Vec<ScenarioItem> = Vec::new();

    // Sort paths for deterministic output
    let mut path_keys: Vec<&String> = doc.paths.keys().collect();
    path_keys.sort();

    for path_str in path_keys {
        let path_item = &doc.paths[path_str];

        for (method, operation) in path_item.operations() {
            if operation.deprecated {
                continue;
            }

            let item_name = operation
                .operation_id
                .clone()
                .or_else(|| operation.summary.clone())
                .unwrap_or_else(|| format!("{} {}", method.to_uppercase(), path_str));

            let url = format!("{}{}", base_url, path_str);

            // Collect parameters — path-item-level + operation-level
            let params: Vec<&OasParameter> = path_item
                .parameters
                .iter()
                .chain(operation.parameters.iter())
                .collect();

            let mut headers: HashMap<String, String> = HashMap::new();
            let mut query_params: HashMap<String, String> = HashMap::new();
            let mut path_params: HashMap<String, String> = HashMap::new();

            for param in &params {
                let val = extract_param_value(param);
                match param.r#in.as_str() {
                    "header" => {
                        headers.insert(param.name.clone(), val);
                    }
                    "query" => {
                        query_params.insert(param.name.clone(), val);
                    }
                    "path" => {
                        path_params.insert(param.name.clone(), val);
                    }
                    _ => {}
                }
            }

            // Resolve path template parameters (e.g. /users/{userId})
            let resolved_url = if !path_params.is_empty() {
                let mut resolved = url.clone();
                for (key, val) in &path_params {
                    resolved = resolved.replace(&format!("{{{}}}", key), val);
                    resolved = resolved.replace(&format!("{{{{{}}}}}", key), val);
                    // double-brace
                }
                resolved
            } else {
                url.clone()
            };

            // Build request body (plus its media type — the http client only
            // auto-sets Content-Type for multipart, so a raw XML/text body
            // would otherwise go out headerless and get 415'd).
            let (body, body_content_type) =
                match operation.request_body.as_ref().and_then(build_request_body) {
                    Some((b, ct)) => (Some(b), Some(ct)),
                    None => (None, None),
                };
            if let Some(ct) = body_content_type {
                let has_ct = headers
                    .iter()
                    .any(|(k, _)| k.eq_ignore_ascii_case("content-type"));
                if !has_ct {
                    headers.insert("Content-Type".to_string(), ct);
                }
            }

            // Resolve auth
            let auth = resolve_auth(operation, &global_security, &doc.components);

            // A genuinely invalid method token (empty, whitespace inside,
            // non-tchar chars) must fail the parse loudly — silently
            // becoming GET would "test" the read path while the spec
            // declared a write path.
            let method = Method::parse(method).ok_or_else(|| {
                TropelError::Parse(format!(
                    "OpenAPI operation '{}' has invalid HTTP method '{}'",
                    item_name, method
                ))
            })?;

            items.push(ScenarioItem {
                name: item_name,
                request: Some(Request {
                    url: resolved_url,
                    method,
                    headers,
                    query_params,
                    body,
                    auth,
                    certificate: None,
                    follow_redirects: true,
                    timeout: None,
                    response_type: tropel_sdk::ResponseType::Text,
                }),
                prerequest: None,
                test: None,
                assertions: vec![],
                items: vec![],
            });
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
            description: doc
                .info
                .description
                .or_else(|| Some(format!("OpenAPI {} — {}", doc.openapi, doc.info.version))),
            schema: None,
        },
        items,
        variables: HashMap::new(),
        auth: None,
    })
}

/// True if the document declares Swagger 2.0.
fn is_swagger2(doc: &Value) -> bool {
    doc.get("swagger")
        .and_then(|v| v.as_str())
        .is_some_and(|s| s.starts_with("2."))
}

/// Resolve a server URL, substituting `{variable}` with its default value
/// (or first enum value, or empty string).
fn resolve_server_url(server: &OasServer) -> String {
    let mut url = server.url.trim_end_matches('/').to_string();
    for (name, var) in &server.variables {
        let value = var
            .default
            .clone()
            .or_else(|| var.r#enum.as_ref().and_then(|e| e.first().cloned()))
            .unwrap_or_default();
        url = url.replace(&format!("{{{}}}", name), &value);
    }
    url
}

// ── Swagger 2.0 → OpenAPI 3.x normalization ─────────────────────

/// Normalize a Swagger 2.0 document into an OpenAPI 3.x-shaped Value so the
/// single typed model handles both.
///
/// Transformations:
/// - `swagger` → `openapi`
/// - `host` + `basePath` + `schemes` → `servers[0]`
/// - `definitions` → `components.schemas`
/// - `securityDefinitions` → `components.securitySchemes` (basic stays basic)
/// - global `parameters` / `responses` → `components.parameters` / `responses`
/// - operation `parameters`: `in: body` → `requestBody`; inline `type` (no
///   `schema`) is wrapped into a `schema` object (Swagger 2.0 puts the type
///   directly on the parameter, OAS 3 requires it under `schema`).
fn normalize_swagger2(doc: Value) -> Result<Value> {
    let mut root = doc;
    let obj = root
        .as_object_mut()
        .ok_or_else(|| TropelError::Parse("Swagger 2.0 root must be an object".into()))?;

    // openapi marker
    obj.remove("swagger");
    obj.insert("openapi".into(), serde_json::json!("3.0.0"));

    // servers from host/basePath/schemes
    let host = obj
        .get("host")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let base_path = obj
        .get("basePath")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let schemes = obj
        .get("schemes")
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str())
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_else(|| vec!["https".to_string()]);

    if !host.is_empty() {
        let scheme = schemes
            .first()
            .cloned()
            .unwrap_or_else(|| "https".to_string());
        let url = format!("{}://{}{}", scheme, host, base_path);
        obj.insert("servers".into(), serde_json::json!([{ "url": url }]));
    } else if !base_path.is_empty() {
        obj.insert("servers".into(), serde_json::json!([{ "url": base_path }]));
    }
    obj.remove("host");
    obj.remove("basePath");
    obj.remove("schemes");

    // definitions → components.schemas
    if let Some(defs) = obj.remove("definitions") {
        let components = obj
            .entry("components")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(c) = components.as_object_mut() {
            c.insert("schemas".into(), defs);
        }
    }

    // securityDefinitions → components.securitySchemes
    // Swagger 2.0's `{type: basic}` must become OAS 3's `{type: http,
    // scheme: basic}` — the greedy ApiKey serde variant would otherwise
    // swallow `basic` and produce ApiKey auth instead of Basic.
    if let Some(sec_defs) = obj.remove("securityDefinitions") {
        let components = obj
            .entry("components")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(c) = components.as_object_mut() {
            c.insert(
                "securitySchemes".into(),
                normalize_security_definitions(sec_defs),
            );
        }
    }

    // global parameters / responses → components
    if let Some(params) = obj.remove("parameters") {
        let components = obj
            .entry("components")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(c) = components.as_object_mut() {
            c.insert("parameters".into(), params);
        }
    }
    if let Some(responses) = obj.remove("responses") {
        let components = obj
            .entry("components")
            .or_insert_with(|| serde_json::json!({}));
        if let Some(c) = components.as_object_mut() {
            c.insert("responses".into(), responses);
        }
    }

    // Per-operation: wrap inline `type` into `schema`, body params → requestBody
    if let Some(paths) = obj.get_mut("paths").and_then(|p| p.as_object_mut()) {
        for path_item in paths.values_mut() {
            if let Some(pi) = path_item.as_object_mut() {
                // path-item-level parameters
                if let Some(params) = pi.get_mut("parameters") {
                    *params = wrap_inline_param_types(params.clone());
                }
                for op_key in [
                    "get", "put", "post", "delete", "options", "head", "patch", "trace",
                ] {
                    if let Some(op) = pi.get_mut(op_key) {
                        normalize_swagger2_operation(op);
                    }
                }
            }
        }
    }

    // Rewrite Swagger 2.0 ref pointers (`#/definitions/...`) to their new
    // OAS 3 locations (`#/components/schemas/...`) so the later $ref
    // resolution step still finds them after the reorg above.
    rewrite_ref_prefixes(&mut root);

    Ok(root)
}

/// Normalize Swagger 2.0 `securityDefinitions` entries into OAS 3 shapes:
/// - `{type: basic}` → `{type: http, scheme: basic}` (the greedy ApiKey
///   serde variant would otherwise swallow `basic`)
/// - flat OAuth2 (`{type: oauth2, flow, authorizationUrl, tokenUrl,
///   scopes}`) → `{type: oauth2, flows: {implicit|password|
///   clientCredentials|authorizationCode: {...}}}` (OAS 3 nests the flow
///   data under `flows`; without this the OAuth2 variant never matches and
///   the scheme falls into the greedy ApiKey arm → wrong auth type)
fn normalize_security_definitions(defs: Value) -> Value {
    let map = match defs {
        Value::Object(m) => m,
        other => return other,
    };
    let mut out = serde_json::Map::with_capacity(map.len());
    for (name, mut scheme) in map {
        if let Some(so) = scheme.as_object_mut() {
            match so.get("type").and_then(|t| t.as_str()) {
                Some("basic") => {
                    scheme = serde_json::json!({"type": "http", "scheme": "basic"});
                }
                Some("oauth2") => {
                    let mut flows = serde_json::Map::new();
                    let mut flow = serde_json::Map::new();
                    // Flatten the single `flow` into the right OAS 3 key.
                    let flow_key = match so.get("flow").and_then(|f| f.as_str()) {
                        Some("implicit") => "implicit",
                        Some("password") => "password",
                        Some("application") => "clientCredentials",
                        Some("accessCode") => "authorizationCode",
                        _ => "clientCredentials",
                    };
                    for key in ["authorizationUrl", "tokenUrl", "scopes"] {
                        if let Some(v) = so.get(key) {
                            flow.insert(key.to_string(), v.clone());
                        }
                    }
                    flows.insert(flow_key.to_string(), Value::Object(flow));
                    scheme = serde_json::json!({"type": "oauth2", "flows": Value::Object(flows)});
                }
                _ => {}
            }
        }
        out.insert(name, scheme);
    }
    Value::Object(out)
}

/// Rewrite Swagger 2.0 ref pointers to their OAS 3 locations after the
/// definitions/parameters/responses reorg:
/// `#/definitions/` → `#/components/schemas/`,
/// `#/parameters/` → `#/components/parameters/`,
/// `#/responses/` → `#/components/responses/`.
fn rewrite_ref_prefixes(value: &mut Value) {
    const REWRITES: [(&str, &str); 3] = [
        ("#/definitions/", "#/components/schemas/"),
        ("#/parameters/", "#/components/parameters/"),
        ("#/responses/", "#/components/responses/"),
    ];
    match value {
        Value::Object(map) => {
            if let Some(Value::String(ref_str)) = map.get_mut("$ref") {
                for (old, new) in &REWRITES {
                    if let Some(rest) = ref_str.strip_prefix(old) {
                        *ref_str = format!("{}{}", new, rest);
                        break;
                    }
                }
            }
            for v in map.values_mut() {
                rewrite_ref_prefixes(v);
            }
        }
        Value::Array(arr) => {
            for v in arr.iter_mut() {
                rewrite_ref_prefixes(v);
            }
        }
        _ => {}
    }
}

/// Wrap Swagger 2.0 inline parameter types (`{"name","in","type":...}`)
/// into OAS 3's `schema` object.
fn wrap_inline_param_types(params: Value) -> Value {
    let arr = match params {
        Value::Array(a) => a,
        other => return other,
    };
    let out: Vec<Value> = arr
        .into_iter()
        .map(|mut p| {
            if let Some(po) = p.as_object_mut() {
                // Only wrap when there's no existing `schema` (body params
                // already carry one in Swagger 2.0).
                if !po.contains_key("schema") && !po.contains_key("$ref") {
                    let mut schema = serde_json::Map::new();
                    for key in [
                        "type", "format", "items", "default", "enum", "minimum", "maximum",
                    ] {
                        if let Some(v) = po.remove(key) {
                            schema.insert(key.to_string(), v);
                        }
                    }
                    if !schema.is_empty() {
                        po.insert("schema".into(), Value::Object(schema));
                    }
                }
            }
            p
        })
        .collect();
    Value::Array(out)
}

/// Normalize one Swagger 2.0 operation: move `in: body` parameters into
/// `requestBody`, and wrap inline types for the rest.
fn normalize_swagger2_operation(op: &mut Value) {
    let Some(op_obj) = op.as_object_mut() else {
        return;
    };

    let mut params = match op_obj.remove("parameters") {
        Some(Value::Array(a)) => a,
        _ => return,
    };

    // Separate body/formData params from the rest.
    let mut body_schema: Option<Value> = None;
    let mut form_props: serde_json::Map<String, Value> = serde_json::Map::new();
    let mut form_required: Vec<String> = Vec::new();
    let mut rest: Vec<Value> = Vec::new();

    for mut p in params.drain(..) {
        let in_loc = p
            .get("in")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let name = p
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        match in_loc.as_str() {
            "body" => {
                body_schema = p.get("schema").cloned();
                // required flag moves to requestBody
                if let Some(Value::Bool(b)) = p.get("required") {
                    op_obj.insert("requestBodyRequired".into(), Value::Bool(*b));
                }
            }
            "formData" => {
                // Swagger 2.0 formData params → object schema under requestBody
                let mut schema = serde_json::Map::new();
                for key in [
                    "type", "format", "items", "default", "enum", "minimum", "maximum",
                ] {
                    if let Some(v) = p.as_object_mut().and_then(|po| po.remove(key)) {
                        schema.insert(key.to_string(), v);
                    }
                }
                if let Some(Value::Bool(true)) = p.get("required") {
                    form_required.push(name.clone());
                }
                form_props.insert(name, Value::Object(schema));
            }
            _ => {
                rest.push(p);
            }
        }
    }

    // Build requestBody if body/formData params exist
    if body_schema.is_some() || !form_props.is_empty() {
        let mut content = serde_json::Map::new();
        let mut required = false;
        if let Some(Value::Bool(b)) = op_obj.remove("requestBodyRequired") {
            required = b;
        }
        if let Some(schema) = body_schema {
            content.insert(
                "application/json".into(),
                serde_json::json!({ "schema": schema }),
            );
        } else if !form_props.is_empty() {
            let mut schema = serde_json::Map::new();
            schema.insert("type".into(), Value::String("object".into()));
            schema.insert("properties".into(), Value::Object(form_props));
            if !form_required.is_empty() {
                schema.insert("required".into(), serde_json::json!(form_required));
            }
            content.insert(
                "application/x-www-form-urlencoded".into(),
                serde_json::json!({ "schema": Value::Object(schema) }),
            );
        }
        op_obj.insert(
            "requestBody".into(),
            serde_json::json!({
                "content": Value::Object(content),
                "required": required,
            }),
        );
    }

    // Wrap inline types on the remaining params
    op_obj.insert(
        "parameters".into(),
        wrap_inline_param_types(Value::Array(rest)),
    );
}

// ── Intra-document $ref resolution ──────────────────────────────

/// Resolve intra-document JSON References (`$ref: "#/components/..."`) by
/// replacing the reference object with a deep copy of the target. External
/// refs (`./other.yaml#/...`) are left untouched (they need file access).
/// Cycle-safe: refs are resolved lazily and never inlined recursively.
fn resolve_refs(doc: &Value) -> Value {
    // Clone the root once; each $ref lookup reads from it.
    let root = doc.clone();

    fn resolve(value: &Value, root: &Value, depth: usize) -> Value {
        // Depth guard: never inline a ref chain deeper than this.
        if depth > 16 {
            return value.clone();
        }
        match value {
            Value::Object(map) => {
                // A pure `{"$ref": "..."}` object → replace with target.
                if map.len() == 1 {
                    if let Some(Value::String(ref_str)) = map.get("$ref") {
                        if let Some(target) = resolve_pointer(root, ref_str) {
                            return resolve(&target, root, depth + 1);
                        }
                    }
                }
                // Otherwise recurse into members.
                let mut out = serde_json::Map::with_capacity(map.len());
                for (k, v) in map {
                    out.insert(k.clone(), resolve(v, root, depth));
                }
                Value::Object(out)
            }
            Value::Array(arr) => {
                Value::Array(arr.iter().map(|v| resolve(v, root, depth)).collect())
            }
            other => other.clone(),
        }
    }

    resolve(doc, &root, 0)
}

/// Resolve a JSON Reference pointer (`#/components/schemas/Foo`) against a
/// document Value. Returns `None` for external refs or missing targets.
fn resolve_pointer(root: &Value, pointer: &str) -> Option<Value> {
    let pointer = pointer.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(root.clone());
    }
    let mut cur = root;
    for part in pointer.trim_start_matches('/').split('/') {
        // Decode JSON-pointer escapes: ~1 → '/', ~0 → '~'.
        let decoded = part.replace("~1", "/").replace("~0", "~");
        cur = match cur {
            Value::Object(map) => map.get(&decoded)?,
            Value::Array(arr) => arr.get(decoded.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

// ── Extraction helpers ──────────────────────────────────────────

/// Extract a parameter value from an OpenAPI parameter definition.
fn extract_param_value(param: &OasParameter) -> String {
    // Try example, then examples, then schema example/default, then type-based default
    if let Some(ref example) = param.example {
        return value_to_string(example);
    }
    if let Some(ref examples) = param.examples {
        // Deterministic pick: HashMap iteration order is unstable, so sort the
        // example names and take the first.
        let mut example_keys: Vec<&String> = examples.keys().collect();
        example_keys.sort();
        if let Some(ex) = example_keys.first().and_then(|k| examples.get(*k)) {
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
///
/// Returns `(body, media_type)` — the media type is threaded back to the
/// caller so it can set a `Content-Type` header (the http client only
/// auto-sets one for multipart, so raw XML/text bodies would otherwise go
/// out headerless).
fn build_request_body(rb: &OasRequestBody) -> Option<(Body, String)> {
    // Prefer application/json
    if let Some(mt) = rb.content.get("application/json") {
        let json_val = mt
            .example
            .clone()
            .or_else(|| generate_schema_example(mt.schema.as_ref()));
        return Some((
            json_val.map_or(Body::Json(serde_json::Value::Null), Body::Json),
            "application/json".to_string(),
        ));
    }

    // Then application/x-www-form-urlencoded
    if let Some(mt) = rb.content.get("application/x-www-form-urlencoded") {
        if let Some(ref schema) = mt.schema {
            let mut map = HashMap::new();
            if let Some(ref props) = schema.properties {
                // Sort property keys — HashMap iteration order is unstable,
                // so form bodies must be identical across runs.
                let mut keys: Vec<&String> = props.keys().collect();
                keys.sort();
                for name in keys {
                    let prop_schema = &props[name];
                    let val = prop_schema
                        .example
                        .clone()
                        .or_else(|| prop_schema.default.clone())
                        .map(|v| value_to_string(&v))
                        .unwrap_or_else(|| "example".to_string());
                    map.insert(name.clone(), val);
                }
            }
            return Some((
                Body::UrlEncoded(map),
                "application/x-www-form-urlencoded".to_string(),
            ));
        }
        return None;
    }

    // Then multipart/form-data
    if let Some(mt) = rb.content.get("multipart/form-data") {
        if let Some(ref schema) = mt.schema {
            let mut map = HashMap::new();
            if let Some(ref props) = schema.properties {
                // Sort property keys for deterministic form bodies.
                let mut keys: Vec<&String> = props.keys().collect();
                keys.sort();
                for name in keys {
                    let prop_schema = &props[name];
                    let val = prop_schema
                        .example
                        .clone()
                        .or_else(|| prop_schema.default.clone())
                        .map(|v| value_to_string(&v))
                        .unwrap_or_else(|| "example".to_string());
                    map.insert(name.clone(), val);
                }
            }
            return Some((Body::FormData(map), "multipart/form-data".to_string()));
        }
        return None;
    }

    // Fallback: take any content type, deterministically. HashMap iteration
    // order is unstable across runs — a spec offering several media types
    // must produce the same request body every parse. Keys are sorted and the
    // first is chosen.
    let mut content_keys: Vec<&String> = rb.content.keys().collect();
    content_keys.sort();
    if let Some(content_type) = content_keys.first() {
        let mt = &rb.content[*content_type];
        let example = mt
            .example
            .clone()
            .or_else(|| generate_schema_example(mt.schema.as_ref()));
        // Type the body by the ACTUAL media type. Only *json* content becomes
        // Body::Json — wrapping an XML/text/octet-stream example as Json would
        // serialize it as a JSON value (an XML string becomes a quoted JSON
        // string; an XML object becomes a JSON object), corrupting the
        // payload. Everything else goes out as Body::Raw (verbatim string
        // example, else the generated value stringified).
        if content_type.to_lowercase().contains("json") {
            return Some((
                example.map_or(Body::Json(serde_json::Value::Null), Body::Json),
                content_type.to_string(),
            ));
        }
        let raw = match example {
            Some(serde_json::Value::String(s)) => s,
            Some(v) => v.to_string(),
            None => String::new(),
        };
        return Some((Body::Raw(raw), content_type.to_string()));
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
                // Sort property keys — HashMap iteration order is unstable, so
                // the generated JSON object must have the same key order on
                // every parse (byte-identical request bodies across runs).
                let mut keys: Vec<&String> = props.keys().collect();
                keys.sort();
                for name in keys {
                    let val = generate_schema_example(Some(&props[name]))
                        .unwrap_or(serde_json::Value::Null);
                    obj.insert(name.clone(), val);
                }
            }
            Some(serde_json::Value::Object(obj))
        }
        Some("array") => {
            if let Some(ref items) = schema.items {
                let val = generate_schema_example(Some(items)).unwrap_or(serde_json::Value::Null);
                Some(serde_json::Value::Array(vec![val]))
            } else {
                Some(serde_json::Value::Array(vec![]))
            }
        }
        Some("string") if schema.format.as_deref() == Some("uuid") => Some(
            serde_json::Value::String("550e8400-e29b-41d4-a716-446655440000".into()),
        ),
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
    let sec_requirements: &[HashMap<String, Vec<String>>] = match operation.security.as_ref() {
        Some(v) => v.as_slice(),
        None => global_security,
    };

    if sec_requirements.is_empty() {
        return None;
    }

    // Take the first security requirement; pick its scheme deterministically
    // (HashMap iteration order is unstable — sorting keeps the generated auth
    // identical across runs).
    let first_sec = &sec_requirements[0];
    let mut scheme_names: Vec<&String> = first_sec.keys().collect();
    scheme_names.sort();
    let scheme_name = *scheme_names.first()?;

    // Look up the scheme in components
    let schemes = components
        .as_ref()
        .and_then(|c| c.security_schemes.as_ref())?;
    let scheme = schemes.get(scheme_name)?;

    match scheme {
        OasSecurityScheme::Http {
            scheme: http_scheme,
            ..
        } => match http_scheme.to_lowercase().as_str() {
            "bearer" => Some(AuthConfig::Bearer {
                token: "__token__".to_string(),
            }),
            "basic" => Some(AuthConfig::Basic {
                username: "__username__".to_string(),
                password: "__password__".to_string(),
            }),
            _ => None,
        },
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
        // OAuth2 flows → bearer token placeholder. The token itself can't be
        // generated from a static spec — the placeholder is substituted by
        // the environment/variables at run time.
        OasSecurityScheme::OAuth2 { .. } => Some(AuthConfig::Bearer {
            token: "__access_token__".to_string(),
        }),
        OasSecurityScheme::OpenIdConnect { .. } => Some(AuthConfig::Bearer {
            token: "__id_token__".to_string(),
        }),
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
    fn test_yaml_spec_parse_and_detect() {
        // OpenAPI specs are commonly authored as YAML — detect + parse must
        // accept them, not just JSON.
        let adapter = OpenApiInputAdapter;
        let yaml = br#"
openapi: 3.0.3
info:
  title: YAML API
  version: 1.0.0
servers:
  - url: https://api.example.com
paths:
  /pets:
    get:
      operationId: listPets
      parameters:
        - name: limit
          in: query
          schema:
            type: integer
          example: 5
      responses:
        '200':
          description: OK
"#;
        assert!(adapter.detect(yaml), "YAML spec must be detected");
        let scenario = adapter.parse(yaml).unwrap();
        assert_eq!(scenario.info.name, "YAML API");
        assert_eq!(scenario.items.len(), 1);
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/pets");
        assert_eq!(req.query_params.get("limit").unwrap(), "5");
    }

    #[test]
    fn test_yaml_spec_with_components_refs() {
        // $ref + quoted keys ('200') + components must survive the YAML →
        // Value → typed-model path.
        let adapter = OpenApiInputAdapter;
        let yaml = br#"
openapi: 3.0.3
info:
  title: RefYaml
  version: 1.0.0
paths:
  /posts:
    get:
      operationId: listPosts
      responses:
        '200':
          description: OK
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Post'
components:
  schemas:
    Post:
      type: object
      properties:
        id:
          type: integer
"#;
        let scenario = adapter.parse(yaml).unwrap();
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].name, "listPosts");
    }

    #[test]
    fn test_yaml_not_json_not_detected() {
        let adapter = OpenApiInputAdapter;
        // A YAML file that is not an OpenAPI spec must not be detected.
        let yaml = b"foo: bar\nbaz: [1, 2]\n";
        assert!(!adapter.detect(yaml));
    }

    #[test]
    fn test_detect_openapi_spec_mentioning_log() {
        // A legit spec whose description/path mentions "log" must STILL be
        // detected — no substring exclusions.
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.3",
            "info": {"title": "Log Service", "version": "1.0.0", "description": "query the log stream"},
            "paths": {"/log": {"get": {"responses": {}}}}
        }"#;
        assert!(
            adapter.detect(data),
            "spec containing 'log' must be detected"
        );
    }

    #[test]
    fn test_detect_swagger2() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "swagger": "2.0",
            "info": {"title": "Legacy API", "version": "1.0.0"},
            "paths": {}
        }"#;
        assert!(adapter.detect(data));
    }

    #[test]
    fn test_detect_postman_not_openapi() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{"info":{"name":"Test","schema":"https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},"item":[]}"#;
        assert!(
            !adapter.detect(data),
            "Postman JSON should not be detected as OpenAPI"
        );
    }

    #[test]
    fn test_detect_har_not_openapi() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{"log":{"version":"1.2","entries":[]}}"#;
        assert!(
            !adapter.detect(data),
            "HAR should not be detected as OpenAPI"
        );
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
    fn test_server_variables_resolved() {
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "Env API", "version": "1.0"},
            "servers": [{
                "url": "https://{env}.example.com/v1",
                "variables": {
                    "env": {"default": "api", "enum": ["api", "staging"]}
                }
            }],
            "paths": {
                "/users": {"get": {"operationId": "listUsers", "responses": {"200": {"description": "OK"}}}}
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/v1/users");
    }

    #[test]
    fn test_ref_parameter_resolved() {
        // A $ref'd parameter used to hard-fail serde. Now it must resolve.
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "Refs", "version": "1.0"},
            "paths": {
                "/pets": {
                    "get": {
                        "operationId": "listPets",
                        "parameters": [
                            {"$ref": "#/components/parameters/Limit"}
                        ],
                        "responses": {"200": {"description": "OK"}}
                    }
                }
            },
            "components": {
                "parameters": {
                    "Limit": {
                        "name": "limit",
                        "in": "query",
                        "schema": {"type": "integer"},
                        "example": 25
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 1);
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.query_params.get("limit").unwrap(), "25");
    }

    #[test]
    fn test_ref_request_body_resolved() {
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "RefBody", "version": "1.0"},
            "paths": {
                "/pets": {
                    "post": {
                        "operationId": "createPet",
                        "requestBody": {"$ref": "#/components/requestBodies/PetBody"},
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            },
            "components": {
                "requestBodies": {
                    "PetBody": {
                        "required": true,
                        "content": {
                            "application/json": {
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}}
                                },
                                "example": {"name": "Rex"}
                            }
                        }
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(req.body.is_some());
        match req.body.as_ref().unwrap() {
            Body::Json(v) => assert_eq!(v, &serde_json::json!({"name": "Rex"})),
            other => panic!("Expected Body::Json, got {:?}", other),
        }
    }

    #[test]
    fn test_swagger2_parse() {
        // host/basePath/schemes → servers; inline types wrapped; body params
        // moved to requestBody.
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "swagger": "2.0",
            "info": {"title": "Legacy", "version": "1.0.0"},
            "host": "api.example.com",
            "basePath": "/v1",
            "schemes": ["https"],
            "paths": {
                "/users": {
                    "get": {
                        "operationId": "listUsers",
                        "parameters": [
                            {"name": "limit", "in": "query", "type": "integer", "required": false}
                        ],
                        "responses": {"200": {"description": "OK"}}
                    }
                },
                "/pets": {
                    "post": {
                        "operationId": "createUser",
                        "parameters": [
                            {
                                "name": "body",
                                "in": "body",
                                "schema": {
                                    "type": "object",
                                    "properties": {"name": {"type": "string"}}
                                }
                            }
                        ],
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        assert_eq!(scenario.items.len(), 2);
        // Paths are sorted alphabetically: /pets (POST) before /users (GET).
        let post = &scenario.items[0];
        let post_req = post.request.as_ref().unwrap();
        assert_eq!(post_req.url, "https://api.example.com/v1/pets");
        assert!(
            post_req.body.is_some(),
            "in:body param must become a requestBody"
        );

        let get = &scenario.items[1];
        let req = get.request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/v1/users");
        assert_eq!(req.query_params.get("limit").unwrap(), "1");
        assert!(req.body.is_none());
    }

    #[test]
    fn test_multiple_paths() {
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
        assert!(
            !req.url.contains('{'),
            "Path params should be resolved: {}",
            req.url
        );
        assert_eq!(req.url, "/users/1/orders/example");
    }

    #[test]
    fn test_oauth2_security_mapped() {
        // OAuth2 security scheme used to silently map to None — now it maps
        // to a bearer-token placeholder auth.
        let adapter = OpenApiInputAdapter;
        let data = br#"{
            "openapi": "3.0.0",
            "info": {"title": "OAuth API", "version": "1.0"},
            "security": [{"oauth": []}],
            "components": {
                "securitySchemes": {
                    "oauth": {
                        "type": "oauth2",
                        "flows": {
                            "clientCredentials": {
                                "tokenUrl": "https://auth.example.com/token",
                                "scopes": {"read": "read access"}
                            }
                        }
                    }
                }
            },
            "paths": {
                "/data": {"get": {"operationId": "getData", "responses": {"200": {"description": "OK"}}}}
            }
        }"#;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(
            req.auth.is_some(),
            "OAuth2 security must map to an auth config"
        );
        match req.auth.as_ref().unwrap() {
            AuthConfig::Bearer { token } => assert_eq!(token, "__access_token__"),
            other => panic!("Expected Bearer placeholder, got {:?}", other),
        }
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
    fn test_xml_body_is_raw_not_json() {
        // An application/xml request body was previously wrapped as
        // Body::Json (serializing the XML string as a quoted JSON string /
        // the object as JSON), corrupting the payload. It must go out as
        // Body::Raw verbatim.
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "XML API", "version": "1.0"},
            "paths": {
                "/widgets": {
                    "post": {
                        "operationId": "createWidget",
                        "requestBody": {
                            "content": {
                                "application/xml": {
                                    "schema": {"type": "object"},
                                    "example": "<widget><name>bolt</name></widget>"
                                }
                            }
                        },
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.body.as_ref().unwrap() {
            Body::Raw(t) => assert_eq!(t, "<widget><name>bolt</name></widget>"),
            other => panic!("Expected Body::Raw XML, got {:?}", other),
        }
        // The media type must be threaded into a Content-Type header — the
        // http client only auto-sets one for multipart, so without this the
        // raw XML body would go out headerless and get 415'd.
        assert_eq!(
            req.headers.get("Content-Type").map(|s| s.as_str()),
            Some("application/xml")
        );
    }

    #[test]
    fn test_json_body_content_type_header() {
        // The application/json media type must also surface as a header.
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "JsonCT", "version": "1.0"},
            "paths": {
                "/pets": {
                    "post": {
                        "operationId": "createPet",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object", "properties": {"name": {"type": "string"}}},
                                    "example": {"name": "Rex"}
                                }
                            }
                        },
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(
            req.headers.get("Content-Type").map(|s| s.as_str()),
            Some("application/json")
        );
    }

    #[test]
    fn test_explicit_content_type_header_wins() {
        // A spec that declares Content-Type as a header parameter must not be
        // overridden by the requestBody media type.
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "ExplicitCT", "version": "1.0"},
            "paths": {
                "/pets": {
                    "post": {
                        "operationId": "createPet",
                        "parameters": [
                            {"name": "Content-Type", "in": "header", "schema": {"type": "string"}, "example": "application/vnd.api+json"}
                        ],
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {"type": "object"},
                                    "example": {"name": "Rex"}
                                }
                            }
                        },
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(
            req.headers.get("Content-Type").map(|s| s.as_str()),
            Some("application/vnd.api+json"),
            "explicit header must not be overridden by media type"
        );
    }

    #[test]
    fn test_object_body_key_order_deterministic() {
        // generate_schema_example previously iterated schema.properties
        // (HashMap) directly — JSON key order in generated bodies varied per
        // run. Two parses of the same spec must produce byte-identical JSON.
        let adapter = OpenApiInputAdapter;
        let spec = br##"{
            "openapi": "3.0.0",
            "info": {"title": "Det", "version": "1.0"},
            "paths": {
                "/things": {
                    "post": {
                        "operationId": "createThing",
                        "requestBody": {
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "zebra": {"type": "string"},
                                            "alpha": {"type": "string"},
                                            "mid": {"type": "integer"}
                                        }
                                    }
                                }
                            }
                        },
                        "responses": {"201": {"description": "Created"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(spec).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.body.as_ref().unwrap() {
            Body::Json(v) => {
                // Assert the EXACT sorted order, not just cross-run equality:
                // serde_json's default Map is BTreeMap-backed and serializes
                // keys alphabetically regardless of insertion order, so a
                // weaker "same across two runs" assertion would pass even if
                // the sort fix were removed. Insertion order (what the sort
                // controls) must be alpha, mid, zebra — this pins the sort
                // for any consumer that iterates the object directly or
                // enables serde_json's preserve_order feature.
                assert_eq!(
                    serde_json::to_string(v).unwrap(),
                    r#"{"alpha":"string","mid":1,"zebra":"string"}"#,
                    "generated JSON body must have sorted property keys"
                );
            }
            other => panic!("Expected Body::Json, got {:?}", other),
        }
    }

    #[test]
    fn test_fallback_content_type_deterministic() {
        // A requestBody offering several media types must pick the same one
        // on every parse — HashMap iteration order is unstable, so the pick
        // is key-sorted (application/xml < text/plain).
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "MultiContent", "version": "1.0"},
            "paths": {
                "/echo": {
                    "post": {
                        "operationId": "echo",
                        "requestBody": {
                            "content": {
                                "text/plain": {"schema": {"type": "string"}, "example": "plain"},
                                "application/xml": {"schema": {"type": "string"}, "example": "<a/>"}
                            }
                        },
                        "responses": {"200": {"description": "OK"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.body.as_ref().unwrap() {
            Body::Raw(t) => assert_eq!(t, "<a/>", "sorted-first content type must win"),
            other => panic!("Expected Body::Raw, got {:?}", other),
        }
    }

    #[test]
    fn test_param_examples_deterministic() {
        // Multiple named examples on a parameter — sorted-first must win so
        // generated requests are stable across runs.
        let adapter = OpenApiInputAdapter;
        let data = br##"{
            "openapi": "3.0.0",
            "info": {"title": "Ex", "version": "1.0"},
            "paths": {
                "/pets": {
                    "get": {
                        "operationId": "listPets",
                        "parameters": [
                            {
                                "name": "status",
                                "in": "query",
                                "examples": {
                                    "zzz": {"value": "zzz-value"},
                                    "aaa": {"value": "aaa-value"}
                                }
                            }
                        ],
                        "responses": {"200": {"description": "OK"}}
                    }
                }
            }
        }"##;
        let scenario = adapter.parse(data).unwrap();
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(
            req.query_params.get("status").unwrap(),
            "aaa-value",
            "sorted-first example must win"
        );
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
