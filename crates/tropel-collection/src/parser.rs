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

/// Read and parse a Collection from a file path.
pub fn parse_collection_file(path: &str) -> Result<Collection> {
    let content = std::fs::read_to_string(path)?;
    parse_collection_str(&content)
}

/// Validate the collection structure.
fn validate_collection(collection: &Collection) -> Result<()> {
    if collection.info.name.is_empty() {
        return Err(CollectionError::MissingField("info.name".into()));
    }
    if collection.info.schema.is_empty() {
        return Err(CollectionError::MissingField("info.schema".into()));
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

    // Convert items
    scenario.items = convert_items(&collection.item, &collection.event);

    scenario
}

fn convert_items(items: &[CollectionItem], parent_events: &[Event]) -> Vec<ScenarioItem> {
    let mut result = Vec::new();
    let mut index = 0usize;

    for item in items {
        match item {
            CollectionItem::Request(req) => {
                let scenario_item = convert_request_item(req, parent_events, index);
                result.push(scenario_item);
                index += 1;
            }
            CollectionItem::Folder(folder) => {
                let scenario_item = ScenarioItem {
                    id: format!("folder_{}", index),
                    name: folder.name.clone(),
                    request: None,
                    prerequest: find_prerequest_script(&folder.event),
                    test: find_test_script(&folder.event),
                    assertions: vec![],
                    items: convert_items(&folder.item, &folder.event),
                };
                result.push(scenario_item);
                index += 1;
            }
        }
    }

    result
}

fn convert_request_item(req: &RequestItem, parent_events: &[Event], index: usize) -> ScenarioItem {
    let request = convert_request(&req.request, &req.auth);
    let events = if req.event.is_empty() {
        parent_events
    } else {
        &req.event
    };

    ScenarioItem {
        id: format!("req_{}", index),
        name: req.name.clone(),
        request: Some(request),
        prerequest: find_prerequest_script(events),
        test: find_test_script(events),
        assertions: vec![],
        items: vec![],
    }
}

fn convert_request(detail: &RequestDetail, item_auth: &Option<CollectionAuth>) -> Request {
    let method = Method::parse(&detail.method).unwrap_or(Method::GET);

    let url = build_url(detail);

    let headers: HashMap<String, String> = detail
        .header
        .iter()
        .filter(|h| !h.disabled)
        .map(|h| (h.key.clone(), h.value.clone()))
        .collect();

    let query_params: HashMap<String, String> = detail
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

    let body = convert_body(detail.body.as_ref());

    Request {
        url,
        method,
        headers,
        query_params,
        body,
        auth: convert_auth(item_auth.as_ref()),
        ..Default::default()
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

fn find_prerequest_script(events: &[Event]) -> Option<String> {
    events
        .iter()
        .find(|e| e.listen == "prerequest")
        .and_then(|e| e.script.as_ref())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
}

fn find_test_script(events: &[Event]) -> Option<String> {
    events
        .iter()
        .find(|e| e.listen == "test")
        .and_then(|e| e.script.as_ref())
        .map(|s| s.to_string())
        .filter(|s| !s.is_empty())
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
        assert_eq!(create.query_params.get("page").map(String::as_str), Some("2"));
        assert_eq!(create.query_params.get("per_page").map(String::as_str), Some("50"));
        assert!(matches!(create.body, Some(Body::Raw(ref s)) if s == "{\"name\":\"x\"}"));

        // String-form URL: the custom UrlDetail deserializer handles it.
        let str_req = scenario.items[1].request.as_ref().unwrap();
        assert_eq!(str_req.url, "https://api.example.com/str");
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
