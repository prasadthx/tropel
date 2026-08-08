use crate::error::*;
use crate::model::*;
use std::collections::HashMap;
use tropel_core::scenario::{Scenario, ScenarioInfo, ScenarioItem};
use tropel_core::types::*;

/// Parse a Postman Collection from JSON bytes.
pub fn parse_collection(bytes: &[u8]) -> Result<Collection> {
    let collection: Collection = serde_json::from_slice(bytes)?;
    validate_collection(&collection)?;
    Ok(collection)
}

/// Parse a Postman Collection from a JSON string.
pub fn parse_collection_str(s: &str) -> Result<Collection> {
    let collection: Collection = serde_json::from_str(s)?;
    validate_collection(&collection)?;
    Ok(collection)
}

/// Validate the collection structure.
fn validate_collection(collection: &Collection) -> Result<()> {
    if collection.info.name.is_empty() {
        return Err(CollectionError::MissingField("info.name".into()));
    }
    if collection.info.schema.is_empty() {
        return Err(CollectionError::MissingField("info.schema".into()));
    }
    validate_methods(&collection.item)?;
    Ok(())
}

/// Every request's method must be a valid HTTP token. A genuinely invalid
/// method (empty, whitespace inside, non-tchar chars) fails the whole
/// collection parse loudly instead of silently becoming GET — a write-path
/// request must not degrade into a read-path "test" that reports green.
/// (Valid-but-uncommon tokens like PURGE/LINK parse fine via
/// `Method::Custom`.)
fn validate_methods(items: &[CollectionItem]) -> Result<()> {
    for item in items {
        match item {
            CollectionItem::Request(req) => {
                if Method::parse(&req.request.method).is_none() {
                    return Err(CollectionError::InvalidRequest(format!(
                        "item '{}' has invalid HTTP method {:?}",
                        req.name, req.request.method
                    )));
                }
            }
            CollectionItem::Folder(folder) => validate_methods(&folder.item)?,
        }
    }
    Ok(())
}

/// Convert a Collection into a protocol-agnostic Scenario.
pub fn collection_to_scenario(
    collection: Collection,
    _env_vars: HashMap<String, String>,
) -> Scenario {
    let mut scenario = Scenario {
        info: ScenarioInfo {
            name: collection.info.name.clone(),
            description: collection.info.description.clone(),
            schema: Some(collection.info.schema.clone()),
        },
        items: vec![],
        variables: HashMap::new(),
        auth: convert_auth(collection.auth.as_ref()),
    };

    // Convert collection variables
    for var in &collection.variable {
        if let Some(value) = &var.value {
            scenario.variables.insert(var.key.clone(), value.clone());
        }
    }

    // Convert items, threading collection-level auth down as the inherited
    // scope (Postman inheritance: request > folder > collection).
    scenario.items = convert_items(
        &collection.item,
        &collection.event,
        collection.auth.as_ref(),
    );

    scenario
}

fn convert_items(
    items: &[CollectionItem],
    parent_events: &[Event],
    inherited_auth: Option<&CollectionAuth>,
) -> Vec<ScenarioItem> {
    let mut result = Vec::new();

    for item in items {
        match item {
            CollectionItem::Request(req) => {
                let scenario_item = convert_request_item(req, parent_events, inherited_auth);
                result.push(scenario_item);
            }
            CollectionItem::Folder(folder) => {
                // Folder-level auth overrides the inherited (collection/parent)
                // auth for every request inside the folder. `inherit` passes
                // the parent's auth through; `noauth` explicitly disables it
                // for the whole subtree.
                let folder_auth = match folder.auth.as_ref() {
                    Some(a) if a.auth_type == "inherit" => inherited_auth,
                    Some(a) => Some(a),
                    None => inherited_auth,
                };
                // P0 (backlog): folder events used to REPLACE the inherited
                // chain, so a collection-level prerequest that mints a token
                // ran ZERO times for requests inside folders. Postman is
                // additive — the folder's events APPEND after the inherited
                // collection/parent-folder chain and ALL of them run,
                // outer→inner.
                let mut events = parent_events.to_vec();
                events.extend(folder.event.iter().cloned());
                let scenario_item = ScenarioItem {
                    name: folder.name.clone(),
                    request: None,
                    // The folder's own scripts are ALSO folded into every
                    // descendant leaf below, so they run before/after each
                    // request in the folder (Postman's per-request folder
                    // script semantics). `flatten_execution_items` drops a
                    // folder container, so these fields never run directly.
                    prerequest: find_prerequest_script(&folder.event),
                    test: find_test_script(&folder.event),
                    assertions: vec![],
                    items: convert_items(&folder.item, &events, folder_auth),
                };
                result.push(scenario_item);
            }
        }
    }

    result
}

fn convert_request_item(
    req: &RequestItem,
    parent_events: &[Event],
    inherited_auth: Option<&CollectionAuth>,
) -> ScenarioItem {
    // v2.1 schema location for request-level auth is `item.request.auth`
    // (RequestDetail.auth) — `item.auth` is a position the schema doesn't
    // define, but legacy exports use it, so prefer the schema location and
    // fall back to the legacy one.
    let request_auth = req.request.auth.as_ref().or(req.auth.as_ref());
    let request = convert_request(&req.request, request_auth, inherited_auth);

    // P0 (backlog): request events were EITHER/OR with parent events — a
    // request with its own script dropped the collection/folder scripts
    // entirely. Postman is additive: the request's events concatenate AFTER
    // the inherited chain, outer→inner, and ALL of them run.
    let mut events = parent_events.to_vec();
    events.extend(req.event.iter().cloned());

    ScenarioItem {
        name: req.name.clone(),
        request: Some(request),
        prerequest: find_prerequest_script(&events),
        test: find_test_script(&events),
        assertions: vec![],
        items: vec![],
    }
}

fn convert_request(
    detail: &RequestDetail,
    request_auth: Option<&CollectionAuth>,
    inherited_auth: Option<&CollectionAuth>,
) -> Request {
    // validate_methods() (called from parse_collection) rejects genuinely
    // invalid tokens at parse time. Direct callers of collection_to_scenario
    // bypass that guard, so the fallback here must NOT be a silent GET: the
    // raw token is preserved as Method::Custom, and reqwest::Method::from_bytes
    // (the Custom arm in the HTTP client) rejects non-tchar tokens LOUDLY at
    // request time — the request fails visibly instead of degrading to a
    // read-path GET that reports green.
    let method =
        Method::parse(&detail.method).unwrap_or_else(|| Method::Custom(detail.method.clone()));

    let mut url = build_url(detail);

    let headers: HashMap<String, String> = detail
        .header
        .iter()
        .filter(|h| !h.disabled)
        .map(|h| (h.key.clone(), h.value.clone()))
        .collect();

    let query_params = build_query_params(detail, &mut url);

    let body = convert_body(detail.body.as_ref());

    Request {
        url,
        method,
        headers,
        query_params,
        body,
        auth: resolve_auth(request_auth, inherited_auth),
        ..Default::default()
    }
}

/// Harvest the structured `url.query` list into `query_params` — but ONLY
/// when the URL itself does not already carry a query (same convention as
/// the HAR adapter; the HTTP client re-appends `query_params`, so doing
/// both would send every param twice: `?page=2&page=2`).
///
/// Postman's `url.raw` is the URL exactly as typed and ALWAYS contains the
/// query when one exists — so the common case leaves `query_params` empty
/// and the query rides in the URL. When the URL has no `?` but the
/// structured query has DUPLICATE keys (a HashMap cannot represent
/// `a=1&a=2`), fold the query string into the URL instead, preserving
/// order and duplicates.
fn build_query_params(detail: &RequestDetail, url: &mut String) -> HashMap<String, String> {
    if url.contains('?') {
        return HashMap::new();
    }
    let pairs: Vec<(String, String)> = detail
        .url
        .as_ref()
        .map(|u| {
            u.query
                .iter()
                .filter(|q| !q.disabled)
                .map(|q| (q.key.clone(), q.value.clone().unwrap_or_default()))
                .collect()
        })
        .unwrap_or_default();
    if pairs.is_empty() {
        return HashMap::new();
    }
    if has_duplicate_keys(&pairs) {
        let qs: Vec<String> = pairs.iter().map(|(k, v)| format!("{}={}", k, v)).collect();
        url.push('?');
        url.push_str(&qs.join("&"));
        HashMap::new()
    } else {
        pairs.into_iter().collect()
    }
}

fn has_duplicate_keys(pairs: &[(String, String)]) -> bool {
    let mut seen = std::collections::HashSet::new();
    pairs.iter().any(|(k, _)| !seen.insert(k.clone()))
}

/// Resolve a request's effective auth following Postman inheritance
/// (request > folder > collection):
/// - explicit `noauth` → `Some(AuthConfig::NoAuth)` — disables auth and
///   BLOCKS inheritance (the old `None` mapping made noauth inherit the
///   parent scope, the inverse of Postman);
/// - explicit `inherit` or no request-level auth → the inherited scope auth;
/// - a real auth type → its config.
fn resolve_auth(
    request_auth: Option<&CollectionAuth>,
    inherited_auth: Option<&CollectionAuth>,
) -> Option<AuthConfig> {
    match request_auth {
        None => convert_auth(inherited_auth),
        Some(a) if a.auth_type == "inherit" => convert_auth(inherited_auth),
        Some(a) if a.auth_type == "noauth" => Some(AuthConfig::NoAuth),
        Some(a) => convert_auth(Some(a)),
    }
}

fn build_url(detail: &RequestDetail) -> String {
    let url = match detail.url.as_ref() {
        Some(u) => u,
        None => return String::new(),
    };

    if let Some(raw) = &url.raw {
        if !raw.is_empty() {
            return raw.clone();
        }
    }

    let proto = url.protocol.as_deref().unwrap_or("https");
    let host = url.host.join(".");
    let port = url
        .port
        .as_ref()
        .map(|p| format!(":{}", p))
        .unwrap_or_default();
    let path = if url.path.is_empty() {
        String::new()
    } else {
        format!("/{}", url.path.join("/"))
    };

    format!("{}://{}{}{}", proto, host, port, path)
}

fn convert_body(body: Option<&RequestBody>) -> Option<Body> {
    match body {
        Some(b) => match b.mode.as_str() {
            "raw" => b.raw.clone().map(Body::Raw),
            "urlencoded" => b.urlencoded.as_ref().map(|params| {
                Body::UrlEncoded(
                    params
                        .iter()
                        .filter(|p| !p.disabled)
                        .map(|p| (p.key.clone(), p.value.clone().unwrap_or_default()))
                        .collect(),
                )
            }),
            "formdata" => b.formdata.as_ref().map(|params| {
                Body::FormData(
                    params
                        .iter()
                        .filter(|p| !p.disabled)
                        .map(|p| (p.key.clone(), p.value.clone().unwrap_or_default()))
                        .collect(),
                )
            }),
            "graphql" => b.graphql.as_ref().map(|gql| {
                let variables = gql
                    .variables
                    .as_ref()
                    .and_then(|v| serde_json::from_str(v).ok());
                Body::GraphQL {
                    query: gql.query.clone().unwrap_or_default(),
                    variables,
                }
            }),
            "file" => b
                .file
                .as_ref()
                .and_then(|f| f.content.clone().map(Body::Raw)),
            _ => b.raw.clone().map(Body::Raw),
        },
        None => None,
    }
}

fn convert_auth(auth: Option<&CollectionAuth>) -> Option<AuthConfig> {
    let auth = auth.as_ref()?;
    match auth.auth_type.as_str() {
        // Explicit noauth at ANY scope (request/folder/collection) must
        // yield Some(NoAuth) — never None. None means "no auth configured →
        // inherit the parent scope" and the runner falls back to scenario
        // auth on None, which would silently re-apply collection auth to a
        // folder/request explicitly marked noauth (the inverse of Postman).
        "noauth" => Some(AuthConfig::NoAuth),
        "bearer" => {
            let token = get_auth_attr(&auth.bearer, "token")
                .or_else(|| get_auth_attr(&auth.bearer, "bearerToken"))
                .unwrap_or_default();
            Some(AuthConfig::Bearer { token })
        }
        "basic" => {
            let username = get_auth_attr(&auth.basic, "username").unwrap_or_default();
            let password = get_auth_attr(&auth.basic, "password").unwrap_or_default();
            Some(AuthConfig::Basic { username, password })
        }
        "apikey" => {
            let key = get_auth_attr(&auth.apikey, "key").unwrap_or_default();
            let value = get_auth_attr(&auth.apikey, "value").unwrap_or_default();
            let location_str = get_auth_attr(&auth.apikey, "in").unwrap_or_default();
            let location = if location_str == "query" {
                ApiKeyLocation::Query
            } else {
                ApiKeyLocation::Header
            };
            Some(AuthConfig::ApiKey {
                key,
                value,
                location,
            })
        }
        "digest" => {
            let username = get_auth_attr(&auth.digest, "username").unwrap_or_default();
            let password = get_auth_attr(&auth.digest, "password").unwrap_or_default();
            Some(AuthConfig::Digest { username, password })
        }
        "oauth1" => {
            let consumer_key = get_auth_attr(&auth.oauth1, "consumerKey").unwrap_or_default();
            let consumer_secret = get_auth_attr(&auth.oauth1, "consumerSecret").unwrap_or_default();
            let token = get_auth_attr(&auth.oauth1, "token");
            let token_secret = get_auth_attr(&auth.oauth1, "tokenSecret");
            Some(AuthConfig::OAuth1 {
                consumer_key,
                consumer_secret,
                token,
                token_secret,
            })
        }
        "oauth2" => {
            let access_token = get_auth_attr(&auth.oauth2, "accessToken").unwrap_or_default();
            let token_type = get_auth_attr(&auth.oauth2, "tokenType");
            Some(AuthConfig::OAuth2 {
                access_token,
                token_type,
            })
        }
        "awsv4" => {
            let access_key = get_auth_attr(&auth.awsv4, "accessKey").unwrap_or_default();
            let secret_key = get_auth_attr(&auth.awsv4, "secretKey").unwrap_or_default();
            let region = get_auth_attr(&auth.awsv4, "region");
            let service = get_auth_attr(&auth.awsv4, "service");
            let session_token = get_auth_attr(&auth.awsv4, "sessionToken");
            Some(AuthConfig::AwsSigV4 {
                access_key,
                secret_key,
                region,
                service,
                session_token,
            })
        }
        "hawk" => {
            let auth_id = get_auth_attr(&auth.hawk, "authId").unwrap_or_default();
            let auth_key = get_auth_attr(&auth.hawk, "authKey").unwrap_or_default();
            let algorithm = get_auth_attr(&auth.hawk, "algorithm");
            Some(AuthConfig::Hawk {
                auth_id,
                auth_key,
                algorithm,
            })
        }
        _ => None,
    }
}

fn get_auth_attr(attrs: &[AuthAttribute], key: &str) -> Option<String> {
    attrs.iter().find(|a| a.key == key).map(|a| a.value.clone())
}

/// ALL prerequest scripts in the event chain, outer (collection) → inner
/// (request), concatenated into one script string. Postman is additive: a
/// collection, each enclosing folder, and the request itself may each
/// contribute a prerequest script and EVERY one runs, before the request,
/// in that order. The old find-first behavior silently dropped outer
/// scripts the moment a deeper level had its own (P0).
fn find_prerequest_script(events: &[Event]) -> Option<String> {
    let scripts: Vec<String> = events
        .iter()
        .filter(|e| e.listen == "prerequest")
        .filter_map(|e| e.script.as_ref())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if scripts.is_empty() {
        None
    } else {
        Some(scripts.join("\n;\n"))
    }
}

/// ALL test scripts in the event chain, outer → inner, concatenated into
/// one script string (see `find_prerequest_script`).
fn find_test_script(events: &[Event]) -> Option<String> {
    let scripts: Vec<String> = events
        .iter()
        .filter(|e| e.listen == "test")
        .filter_map(|e| e.script.as_ref())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if scripts.is_empty() {
        None
    } else {
        Some(scripts.join("\n;\n"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_collection() {
        let json = r#"{
            "info": {
                "name": "Test Collection",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": []
        }"#;

        let collection = parse_collection_str(json).unwrap();
        assert_eq!(collection.info.name, "Test Collection");
        assert!(collection.item.is_empty());
    }

    #[test]
    fn test_parse_single_request() {
        let json = r#"{
            "info": {
                "name": "Simple API",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": [
                {
                    "name": "Get Users",
                    "request": {
                        "method": "GET",
                        "header": [
                            {"key": "Accept", "value": "application/json"}
                        ],
                        "url": {
                            "raw": "https://api.example.com/users",
                            "protocol": "https",
                            "host": ["api", "example", "com"],
                            "path": ["users"]
                        }
                    }
                }
            ]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        assert_eq!(collection.item.len(), 1);
    }

    #[test]
    fn test_parse_with_variables() {
        let json = r#"{
            "info": {
                "name": "Environments",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "variable": [
                {"key": "base_url", "value": "https://api.example.com", "type": "string"},
                {"key": "api_key", "value": "secret123", "type": "string"}
            ],
            "item": []
        }"#;

        let collection = parse_collection_str(json).unwrap();
        assert_eq!(collection.variable.len(), 2);
    }

    #[test]
    fn test_convert_to_scenario() {
        let json = r#"{
            "info": {
                "name": "Test",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "variable": [
                {"key": "base_url", "value": "https://api.example.com"}
            ],
            "item": [
                {
                    "name": "Get Users",
                    "request": {
                        "method": "GET",
                        "url": {"raw": "{{base_url}}/users"}
                    }
                }
            ]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());

        assert_eq!(scenario.info.name, "Test");
        assert_eq!(
            scenario.variables.get("base_url").unwrap(),
            "https://api.example.com"
        );
        assert_eq!(scenario.items.len(), 1);
    }

    #[test]
    fn test_parse_graphql_request() {
        let json = r#"{
            "info": {
                "name": "GraphQL",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": [
                {
                    "name": "GraphQL Query",
                    "request": {
                        "method": "POST",
                        "url": {"raw": "https://api.example.com/graphql"},
                        "body": {
                            "mode": "graphql",
                            "graphql": {
                                "query": "query { users { id name } }",
                                "variables": "{\"limit\": 10}"
                            }
                        }
                    }
                }
            ]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());

        if let Some(request) = &scenario.items[0].request {
            assert_eq!(request.method, Method::POST);
            if let Some(Body::GraphQL { query, variables }) = &request.body {
                assert_eq!(query, "query { users { id name } }");
                assert!(variables.is_some());
            } else {
                panic!("Expected GraphQL body");
            }
        }
    }

    #[test]
    fn test_parse_with_events() {
        let json = r#"{
            "info": {
                "name": "With Scripts",
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "item": [
                {
                    "name": "Test Request",
                    "event": [
                        {
                            "listen": "prerequest",
                            "script": {
                                "exec": ["pm.environment.set('key', 'value')"],
                                "type": "text/javascript"
                            }
                        },
                        {
                            "listen": "test",
                            "script": {
                                "exec": [
                                    "pm.test('Status 200', function() {",
                                    "    pm.response.to.have.status(200);",
                                    "});"
                                ],
                                "type": "text/javascript"
                            }
                        }
                    ],
                    "request": {
                        "method": "GET",
                        "url": {"raw": "https://api.example.com/test"}
                    }
                }
            ]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());

        assert!(scenario.items[0].prerequest.is_some());
        assert!(scenario.items[0].test.is_some());
        assert!(scenario.items[0].test.as_ref().unwrap().contains("pm.test"));
    }

    #[test]
    fn test_collection_and_folder_scripts_reach_leaves_in_order() {
        // P0 (backlog): collection- and folder-level scripts never ran for
        // folder-organized collections. Three interacting facts: folder
        // events REPLACED collection events; request events were either/or
        // with parent events (Postman is additive); and
        // flatten_execution_items discards a folder that has children, so
        // its script was dead. A top-level prerequest that mints a token
        // ran zero times → every request sent literal `Bearer {{token}}`.
        //
        // Fix: the inherited event chain (collection → folder → request)
        // is folded into each leaf at convert time, outer→inner, ALL of
        // them concatenated — so per-request scripts run exactly the way
        // Postman runs them.
        let json = r#"{
            "info": {"name": "Scripts", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "event": [
                {"listen": "prerequest", "script": {"exec": ["COLLECTION_PREREQUEST"], "type": "text/javascript"}},
                {"listen": "test", "script": {"exec": ["COLLECTION_TEST"], "type": "text/javascript"}}
            ],
            "item": [{
                "name": "Folder",
                "event": [
                    {"listen": "prerequest", "script": {"exec": ["FOLDER_PREREQUEST"], "type": "text/javascript"}},
                    {"listen": "test", "script": {"exec": ["FOLDER_TEST"], "type": "text/javascript"}}
                ],
                "item": [{
                    "name": "Inner Req",
                    "request": {"method": "GET", "url": {"raw": "https://api.example.com/inner"}},
                    "event": [
                        {"listen": "prerequest", "script": {"exec": ["INNER_PREREQUEST"], "type": "text/javascript"}},
                        {"listen": "test", "script": {"exec": ["INNER_TEST"], "type": "text/javascript"}}
                    ]
                }]
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let folder = &scenario.items[0];
        assert_eq!(folder.name, "Folder");
        let inner = &folder.items[0];

        // Prerequest: ALL three levels concatenated, outer→inner.
        let pre = inner.prerequest.as_ref().expect("leaf must carry prerequest");
        let c = pre.find("COLLECTION_PREREQUEST").expect("collection script folded");
        let f = pre.find("FOLDER_PREREQUEST").expect("folder script folded");
        let i = pre.find("INNER_PREREQUEST").expect("request script kept");
        assert!(c < f && f < i, "prerequest must be outer→inner, got: {pre}");

        // Test: ALL three levels concatenated, outer→inner.
        let t = inner.test.as_ref().expect("leaf must carry test");
        let c = t.find("COLLECTION_TEST").expect("collection test folded");
        let f = t.find("FOLDER_TEST").expect("folder test folded");
        let i = t.find("INNER_TEST").expect("request test kept");
        assert!(c < f && f < i, "test must be outer→inner, got: {t}");
    }

    #[test]
    fn test_request_scripts_append_to_inherited_not_replace() {
        // P0 (backlog): a request with its OWN event dropped the inherited
        // collection/folder scripts (either/or). Postman is additive — the
        // request's script must run IN ADDITION to the outer chain.
        let json = r#"{
            "info": {"name": "Additive", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "event": [
                {"listen": "prerequest", "script": {"exec": ["COLL_PRE"], "type": "text/javascript"}}
            ],
            "item": [{
                "name": "Req",
                "request": {"method": "GET", "url": {"raw": "https://api.example.com/x"}},
                "event": [
                    {"listen": "prerequest", "script": {"exec": ["REQ_PRE"], "type": "text/javascript"}}
                ]
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let pre = scenario.items[0]
            .prerequest
            .as_ref()
            .expect("leaf must carry prerequest");
        let c = pre.find("COLL_PRE").expect("collection script must be inherited");
        let r = pre.find("REQ_PRE").expect("request script must be present");
        assert!(c < r, "inherited script must run before the request's own: {pre}");
    }

    #[test]
    fn test_parse_folder_nesting() {
        // A folder containing a request: the request must surface as a
        // nested ScenarioItem, not be flattened or dropped.
        let json = r#"{
            "info": {"name": "Nested", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Folder",
                "item": [{
                    "name": "Inner Req",
                    "request": {"method": "GET", "url": {"raw": "https://api.example.com/inner"}}
                }]
            }]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());
        assert_eq!(scenario.items.len(), 1);
        assert_eq!(scenario.items[0].name, "Folder");
        assert_eq!(scenario.items[0].items.len(), 1);
        let inner = &scenario.items[0].items[0];
        assert_eq!(inner.name, "Inner Req");
        let req = inner.request.as_ref().expect("inner request parsed");
        assert_eq!(req.url, "https://api.example.com/inner");
    }

    #[test]
    fn test_parse_query_params_and_raw_body() {
        // Structured URL with query params + a raw JSON body + string-form
        // URL variant must all survive the round-trip.
        let json = r#"{
            "info": {"name": "Full", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Create",
                "request": {
                    "method": "POST",
                    "url": {
                        "raw": "https://api.example.com/items",
                        "host": ["api", "example", "com"],
                        "path": ["items"],
                        "query": [
                            {"key": "page", "value": "2"},
                            {"key": "per_page", "value": "50"}
                        ]
                    },
                    "body": {
                        "mode": "raw",
                        "raw": "{\"name\":\"x\"}"
                    }
                }
            }, {
                "name": "StringUrl",
                "request": {"method": "GET", "url": "https://api.example.com/str"}
            }]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());
        assert_eq!(scenario.items.len(), 2);

        let create = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(create.url, "https://api.example.com/items");
        assert_eq!(
            create.query_params.get("page").map(String::as_str),
            Some("2")
        );
        assert_eq!(
            create.query_params.get("per_page").map(String::as_str),
            Some("50")
        );
        assert!(matches!(create.body, Some(Body::Raw(ref s)) if s == "{\"name\":\"x\"}"));

        // String-form URL: the custom UrlDetail deserializer handles it.
        let str_req = scenario.items[1].request.as_ref().unwrap();
        assert_eq!(str_req.url, "https://api.example.com/str");
    }

    #[test]
    fn test_request_level_auth_read_from_request_detail() {
        // Regression (backlog line 69): the v2.1 schema puts request-level
        // auth at `item.request.auth` (RequestDetail.auth), but the parser
        // only read the non-schema `item.auth` position — per-request bearer
        // tokens were silently dropped, so no auth was sent at all.
        let json = r#"{
            "info": {"name": "Auth", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Secure",
                "request": {
                    "method": "GET",
                    "url": {"raw": "https://api.example.com/secure"},
                    "auth": {
                        "type": "bearer",
                        "bearer": [{"key": "token", "value": "tok123", "type": "string"}]
                    }
                }
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.auth.as_ref() {
            Some(AuthConfig::Bearer { token }) => assert_eq!(token, "tok123"),
            other => panic!("expected Bearer auth, got {:?}", other),
        }
    }

    #[test]
    fn test_folder_auth_inherited_by_children() {
        // Folder-level auth must be inherited by every request in the folder
        // (Postman inheritance: request > folder > collection).
        let json = r#"{
            "info": {"name": "Folders", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "auth": {"type": "basic", "basic": [{"key": "username", "value": "coll_user"}, {"key": "password", "value": "coll_pass"}]},
            "item": [{
                "name": "Folder",
                "auth": {"type": "bearer", "bearer": [{"key": "token", "value": "folder_tok"}]},
                "item": [{
                    "name": "Child",
                    "request": {"method": "GET", "url": {"raw": "https://api.example.com/child"}}
                }]
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].items[0].request.as_ref().unwrap();
        match req.auth.as_ref() {
            Some(AuthConfig::Bearer { token }) => assert_eq!(token, "folder_tok"),
            other => panic!("child must inherit folder bearer auth, got {:?}", other),
        }
    }

    #[test]
    fn test_collection_auth_inherited_by_requests() {
        // No folder or request auth → the collection-level auth applies.
        let json = r#"{
            "info": {"name": "Coll", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "auth": {"type": "apikey", "apikey": [{"key": "key", "value": "k1"}, {"key": "value", "value": "v1"}, {"key": "in", "value": "header"}]},
            "item": [{
                "name": "Req",
                "request": {"method": "GET", "url": {"raw": "https://api.example.com/x"}}
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(
            matches!(req.auth.as_ref(), Some(AuthConfig::ApiKey { .. })),
            "request must inherit collection api-key auth, got {:?}",
            req.auth
        );
    }

    #[test]
    fn test_noauth_blocks_inheritance() {
        // Regression (backlog line 69): `{"type":"noauth"}` mapped to None,
        // indistinguishable from inherit, so an explicitly unauthenticated
        // request inherited collection auth — the INVERSE of Postman. noauth
        // must yield AuthConfig::NoAuth so the runner sends no auth.
        let json = r#"{
            "info": {"name": "NoAuth", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "auth": {"type": "bearer", "bearer": [{"key": "token", "value": "coll_tok"}]},
            "item": [{
                "name": "Public",
                "request": {
                    "method": "GET",
                    "url": {"raw": "https://api.example.com/public"},
                    "auth": {"type": "noauth"}
                }
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        assert!(
            matches!(req.auth.as_ref(), Some(AuthConfig::NoAuth)),
            "noauth must block inheritance (AuthConfig::NoAuth), got {:?}",
            req.auth
        );
    }

    #[test]
    fn test_folder_noauth_blocks_collection_inheritance() {
        // Regression: a folder marked noauth inside a bearer-authenticated
        // collection must NOT re-inherit the collection bearer. convert_auth
        // maps "noauth" → Some(AuthConfig::NoAuth) at every scope level so
        // the runner's `.or(scenario.auth)` fallback can't re-apply it.
        let json = r#"{
            "info": {"name": "NoAuthFolder", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "auth": {"type": "bearer", "bearer": [{"key": "token", "value": "coll_tok"}]},
            "item": [{
                "name": "Public Folder",
                "auth": {"type": "noauth"},
                "item": [{
                    "name": "Public Req",
                    "request": {"method": "GET", "url": {"raw": "https://api.example.com/public"}}
                }]
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].items[0].request.as_ref().unwrap();
        assert!(
            matches!(req.auth.as_ref(), Some(AuthConfig::NoAuth)),
            "folder noauth must block collection inheritance, got {:?}",
            req.auth
        );
    }

    #[test]
    fn test_legacy_item_auth_fallback() {
        // Some exports put request auth at the non-schema `item.auth` slot;
        // it must still be honored when `item.request.auth` is absent.
        let json = r#"{
            "info": {"name": "Legacy", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Legacy Auth",
                "auth": {"type": "bearer", "bearer": [{"key": "token", "value": "legacy_tok"}]},
                "request": {"method": "GET", "url": {"raw": "https://api.example.com/legacy"}}
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        match req.auth.as_ref() {
            Some(AuthConfig::Bearer { token }) => assert_eq!(token, "legacy_tok"),
            other => panic!("legacy item.auth must be honored, got {:?}", other),
        }
    }

    #[test]
    fn test_query_not_sent_twice() {
        // Regression (backlog line 72): build_url returns url.raw verbatim
        // (which ALWAYS contains the query) AND query_params harvested the
        // structured url.query list — the HTTP client re-appends
        // query_params, so `GET /items?page=2` went out as
        // `/items?page=2&page=2`. When the URL already carries a query,
        // query_params must stay empty (same convention as the HAR adapter).
        let json = r#"{
            "info": {"name": "Q", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "List",
                "request": {
                    "method": "GET",
                    "url": {
                        "raw": "https://api.example.com/items?page=2",
                        "host": ["api", "example", "com"],
                        "path": ["items"],
                        "query": [{"key": "page", "value": "2"}]
                    }
                }
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/items?page=2");
        assert!(
            req.query_params.is_empty(),
            "query_params must be empty when the URL already has the query — got {:?}",
            req.query_params
        );
    }

    #[test]
    fn test_query_populated_when_url_has_no_query() {
        // Structured query WITHOUT a query in the raw URL must still be
        // harvested into query_params (the client appends them once).
        let json = r#"{
            "info": {"name": "Q", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "List",
                "request": {
                    "method": "GET",
                    "url": {
                        "raw": "https://api.example.com/items",
                        "host": ["api", "example", "com"],
                        "path": ["items"],
                        "query": [{"key": "page", "value": "2"}]
                    }
                }
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/items");
        assert_eq!(req.query_params.get("page").map(String::as_str), Some("2"));
    }

    #[test]
    fn test_query_duplicate_keys_folded_into_url() {
        // A HashMap cannot represent `tag=a&tag=b`; when the URL has no
        // query and the structured list has duplicate keys, fold the full
        // query string into the URL (order + duplicates preserved) and keep
        // query_params empty — mirroring the HAR adapter's convention.
        let json = r#"{
            "info": {"name": "Q", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Tags",
                "request": {
                    "method": "GET",
                    "url": {
                        "raw": "https://api.example.com/search",
                        "host": ["api", "example", "com"],
                        "path": ["search"],
                        "query": [{"key": "tag", "value": "a"}, {"key": "tag", "value": "b"}]
                    }
                }
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        assert_eq!(req.url, "https://api.example.com/search?tag=a&tag=b");
        assert!(req.query_params.is_empty());
    }

    #[test]
    fn test_schema_legal_shapes_parse_as_requests() {
        // Regression (backlog line 93): object-form `description`,
        // string-form `script.exec`, a header with no `value`, a numeric
        // `responseTime`, and a missing response `code` are ALL schema-legal
        // Postman shapes. Before the fix each one made RequestItem fail to
        // parse, the untagged CollectionItem silently fell through to
        // FolderItem (which only requires `name`), and the request was
        // dropped as an empty folder.
        let json = r#"{
            "info": {"name": "Shapes", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Shape Req",
                "request": {
                    "method": "GET",
                    "url": {"raw": "https://api.example.com/shapes"},
                    "description": {"content": "object-form description", "type": "text/plain"},
                    "header": [{"key": "X-No-Value"}]
                },
                "event": [{
                    "listen": "test",
                    "script": {"exec": "pm.test('ok', () => {});", "type": "text/javascript"}
                }],
                "response": [{"name": "r1", "status": "OK", "responseTime": 123}]
            }]
        }"#;

        let scenario = collection_to_scenario(parse_collection_str(json).unwrap(), HashMap::new());
        assert_eq!(
            scenario.items.len(),
            1,
            "request must not fall through to a folder"
        );
        assert_eq!(scenario.items[0].name, "Shape Req");
        let req = scenario.items[0].request.as_ref().expect("request parsed");
        assert_eq!(req.url, "https://api.example.com/shapes");
        assert_eq!(req.headers.get("X-No-Value").map(String::as_str), Some(""));
        // String-form exec must still surface as a test script.
        let test = scenario.items[0].test.as_ref().expect("test script parsed");
        assert!(
            test.contains("pm.test"),
            "string-form exec joined into script"
        );
    }

    #[test]
    fn test_malformed_request_fails_loudly() {
        // Regression (backlog line 93): a request with a genuinely
        // malformed sub-field (here: a non-string header value) must ERROR
        // loudly — not silently become an empty folder that vanishes from
        // the run. The request-key discriminator guarantees RequestItem is
        // attempted whenever a `request` key is present.
        let json = r#"{
            "info": {"name": "Bad", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Broken Req",
                "request": {
                    "method": "GET",
                    "url": {"raw": "https://api.example.com/broken"},
                    "header": [{"key": "X", "value": 42}]
                }
            }]
        }"#;

        assert!(
            parse_collection_str(json).is_err(),
            "malformed request must fail loudly, not become an empty folder"
        );
    }

    #[test]
    fn test_parse_urlencoded_body() {
        let json = r#"{
            "info": {"name": "Form", "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"},
            "item": [{
                "name": "Login",
                "request": {
                    "method": "POST",
                    "url": {"raw": "https://api.example.com/login"},
                    "body": {
                        "mode": "urlencoded",
                        "urlencoded": [
                            {"key": "user", "value": "alice"},
                            {"key": "pass", "value": "secret", "disabled": true}
                        ]
                    }
                }
            }]
        }"#;

        let collection = parse_collection_str(json).unwrap();
        let scenario = collection_to_scenario(collection, HashMap::new());
        let req = scenario.items[0].request.as_ref().unwrap();
        match &req.body {
            Some(Body::UrlEncoded(params)) => {
                assert_eq!(params.len(), 1, "disabled param dropped");
                assert_eq!(params.get("user").map(String::as_str), Some("alice"));
                assert!(params.get("pass").is_none());
            }
            other => panic!("expected UrlEncoded body, got {:?}", other),
        }
    }
}
