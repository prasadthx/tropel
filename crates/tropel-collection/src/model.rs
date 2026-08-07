use serde::{Deserialize, Serialize};

/// Postman Collection (v2.1/v2.0).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Collection {
    pub info: CollectionInfo,
    #[serde(default)]
    pub item: Vec<CollectionItem>,
    #[serde(default)]
    pub auth: Option<CollectionAuth>,
    #[serde(default)]
    pub variable: Vec<Variable>,
    #[serde(default)]
    pub event: Vec<Event>,
}

/// Collection metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionInfo {
    #[serde(rename = "_postman_id")]
    pub postman_id: Option<String>,
    pub name: String,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
    pub schema: String,
}

/// A single item or folder.
//
// `Folder` is much larger than `Request` (it nests recursively); boxing the
// larger variant keeps the enum small without changing serde's untagged
// shape (Box<T> serializes exactly like T). `Request` remains the largest
// variant, so the size-difference lint is suppressed.
//
// Serialization stays `untagged` (a request item serializes as its
// RequestItem object, a folder as its FolderItem object). Deserialization
// is custom: an object carrying a `request` key is a request item, anything
// else is a folder. This fixes the silent-fallthrough bug where a malformed
// sub-field (object-form description, string-form script.exec, a header
// without value, a numeric responseTime, a missing response code) made
// `RequestItem` fail to parse, and `#[serde(untagged)]` then tried
// `FolderItem` — which only requires `name` — so the request silently
// became an empty folder and was dropped from the run.
#[derive(Debug, Clone, Serialize)]
#[serde(untagged)]
#[allow(clippy::large_enum_variant)]
pub enum CollectionItem {
    Request(RequestItem),
    Folder(Box<FolderItem>),
}

impl<'de> Deserialize<'de> for CollectionItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Discriminate by key presence: folders carry `item`, requests
        // carry `request`. Folder-first: a folder that also carries a stray
        // `request` key (some real exports put `"request": null` next to
        // `"item": [...]`) must keep its children rather than being
        // misclassified as a request. If a request item's sub-fields fail to
        // parse, this errors loudly instead of silently falling through to
        // FolderItem (the pre-fix behavior that turned the request into an
        // empty folder and dropped it).
        let value = serde_json::Value::deserialize(deserializer)?;
        let is_folder = value
            .as_object()
            .map(|o| o.contains_key("item"))
            .unwrap_or(false);
        let is_request = !is_folder
            && value
                .as_object()
                .map(|o| o.contains_key("request"))
                .unwrap_or(false);
        if is_request {
            RequestItem::deserialize(value)
                .map(CollectionItem::Request)
                .map_err(serde::de::Error::custom)
        } else {
            FolderItem::deserialize(value)
                .map(|f| CollectionItem::Folder(Box::new(f)))
                .map_err(serde::de::Error::custom)
        }
    }
}

/// Accept Postman's two schema-legal `description` shapes: a plain string
/// or an object `{"content": …, "type": …}`. Returns the text content.
fn de_opt_description<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum DescriptionForm {
        Str(String),
        Obj { content: Option<String> },
    }
    Ok(
        match Option::<DescriptionForm>::deserialize(deserializer)? {
            Some(DescriptionForm::Str(s)) => Some(s),
            Some(DescriptionForm::Obj { content }) => content,
            None => None,
        },
    )
}

/// Accept Postman's two schema-legal `script.exec` shapes: an array of
/// lines or a single string (wrapped into a one-element array).
fn de_exec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ExecForm {
        Lines(Vec<String>),
        Single(String),
    }
    Ok(match ExecForm::deserialize(deserializer)? {
        ExecForm::Lines(lines) => lines,
        ExecForm::Single(s) => vec![s],
    })
}

/// Accept `response_time` as either a numeric milliseconds value (as
/// exported by Postman) or a string; normalize both to a string.
fn de_opt_response_time<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum TimeForm {
        Num(u64),
        Str(String),
    }
    Ok(match Option::<TimeForm>::deserialize(deserializer)? {
        Some(TimeForm::Num(n)) => Some(n.to_string()),
        Some(TimeForm::Str(s)) => Some(s),
        None => None,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestItem {
    pub name: String,
    pub request: RequestDetail,
    #[serde(default)]
    pub event: Vec<Event>,
    #[serde(default)]
    pub response: Vec<ResponseDetail>,
    #[serde(default)]
    pub auth: Option<CollectionAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FolderItem {
    pub name: String,
    #[serde(default)]
    pub item: Vec<CollectionItem>,
    #[serde(default)]
    pub event: Vec<Event>,
    #[serde(default)]
    pub auth: Option<CollectionAuth>,
    #[serde(default)]
    pub variable: Vec<Variable>,
}

fn default_method() -> String {
    "GET".to_string()
}

/// Request details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestDetail {
    #[serde(default = "default_method")]
    pub method: String,
    #[serde(default)]
    pub header: Vec<Header>,
    pub body: Option<RequestBody>,
    #[serde(default)]
    pub url: Option<UrlDetail>,
    pub auth: Option<CollectionAuth>,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
}

/// URL detail.
///
/// Postman may export a request URL as either the structured object form
/// (`{"raw": "https://…", "host": […], …}`) or as a plain string
/// (`"https://…"`). The custom `Deserialize` accepts both — without it,
/// string-form URLs fail to parse, the untagged `CollectionItem` silently
/// falls through to `FolderItem`, and the request is dropped entirely.
#[derive(Debug, Clone, Serialize)]
pub struct UrlDetail {
    pub raw: Option<String>,
    pub protocol: Option<String>,
    pub host: Vec<String>,
    pub port: Option<String>,
    pub path: Vec<String>,
    pub query: Vec<QueryParam>,
    pub variable: Vec<UrlVariable>,
    pub hash: Option<String>,
}

impl<'de> Deserialize<'de> for UrlDetail {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // Match either the structured object or a bare URL string.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum UrlForm {
            Raw(String),
            Object(UrlDetailFields),
        }

        #[derive(Deserialize)]
        struct UrlDetailFields {
            raw: Option<String>,
            protocol: Option<String>,
            #[serde(default)]
            host: Vec<String>,
            port: Option<String>,
            #[serde(default)]
            path: Vec<String>,
            #[serde(default)]
            query: Vec<QueryParam>,
            #[serde(default)]
            variable: Vec<UrlVariable>,
            hash: Option<String>,
        }

        let form = UrlForm::deserialize(deserializer)?;
        Ok(match form {
            UrlForm::Raw(raw) => UrlDetail {
                raw: Some(raw),
                protocol: None,
                host: Vec::new(),
                port: None,
                path: Vec::new(),
                query: Vec::new(),
                variable: Vec::new(),
                hash: None,
            },
            UrlForm::Object(fields) => UrlDetail {
                raw: fields.raw,
                protocol: fields.protocol,
                host: fields.host,
                port: fields.port,
                path: fields.path,
                query: fields.query,
                variable: fields.variable,
                hash: fields.hash,
            },
        })
    }
}

/// URL query parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
    pub key: String,
    pub value: Option<String>,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: bool,
}

/// URL variable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlVariable {
    pub key: String,
    pub value: Option<String>,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
}

/// HTTP header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub key: String,
    // A header with no `value` is schema-legal in exports; default to empty
    // so it cannot fail RequestItem parsing (which used to silently turn the
    // whole request into an empty folder).
    #[serde(default)]
    pub value: String,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: bool,
}

/// Request body.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestBody {
    pub mode: String,
    pub raw: Option<String>,
    pub urlencoded: Option<Vec<FormParameter>>,
    pub formdata: Option<Vec<FormParameter>>,
    pub file: Option<FileSpec>,
    pub graphql: Option<GraphQLSpec>,
    pub options: Option<BodyOptions>,
}

/// Form parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormParameter {
    pub key: String,
    pub value: Option<String>,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub param_type: Option<String>,
    pub src: Option<String>,
    #[serde(default)]
    pub disabled: bool,
}

/// File specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSpec {
    pub src: Option<String>,
    pub content: Option<String>,
}

/// GraphQL specification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphQLSpec {
    pub query: Option<String>,
    pub variables: Option<String>,
}

/// Body options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BodyOptions {
    pub raw: Option<RawOptions>,
}

/// Raw body options.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawOptions {
    pub language: Option<String>,
}

/// Event (script).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub listen: String,
    pub script: Option<Script>,
}

/// Script.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Script {
    // Postman exports `exec` as either an array of lines or a single string;
    // accept both so a string-form exec cannot fail RequestItem parsing.
    #[serde(default, deserialize_with = "de_exec")]
    pub exec: Vec<String>,
    #[serde(rename = "type")]
    pub script_type: Option<String>,
    pub src: Option<String>,
}

impl std::fmt::Display for Script {
    /// Join exec lines into a single script string.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.exec.join("\n"))
    }
}

/// Variable definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Variable {
    pub key: String,
    pub value: Option<serde_json::Value>,
    #[serde(rename = "type")]
    pub var_type: Option<String>,
    #[serde(default, deserialize_with = "de_opt_description")]
    pub description: Option<String>,
}

/// Auth configuration in Postman format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionAuth {
    #[serde(rename = "type")]
    pub auth_type: String,
    #[serde(default)]
    pub bearer: Vec<AuthAttribute>,
    #[serde(default)]
    pub basic: Vec<AuthAttribute>,
    #[serde(default)]
    pub apikey: Vec<AuthAttribute>,
    #[serde(default)]
    pub digest: Vec<AuthAttribute>,
    #[serde(default)]
    pub oauth1: Vec<AuthAttribute>,
    #[serde(default)]
    pub oauth2: Vec<AuthAttribute>,
    #[serde(default)]
    pub awsv4: Vec<AuthAttribute>,
    #[serde(default)]
    pub hawk: Vec<AuthAttribute>,
}

/// Auth attribute (key-value pair).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthAttribute {
    pub key: String,
    pub value: String,
    #[serde(rename = "type")]
    pub attr_type: Option<String>,
}

/// Response detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseDetail {
    pub name: Option<String>,
    pub status: Option<String>,
    // Missing `code` (or a numeric `response_time`) must not fail parsing —
    // exports omit it; before the fix that silently dropped the request.
    #[serde(default)]
    pub code: u16,
    #[serde(default)]
    pub header: Vec<Header>,
    pub body: Option<String>,
    pub content_type: Option<String>,
    #[serde(default, deserialize_with = "de_opt_response_time")]
    pub response_time: Option<String>,
    #[serde(default)]
    pub cookie: Vec<ResponseCookie>,
}

/// Response cookie.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseCookie {
    pub key: String,
    pub value: String,
    pub domain: Option<String>,
    pub path: Option<String>,
    pub http_only: Option<bool>,
    pub secure: Option<bool>,
    pub same_site: Option<String>,
    pub expires: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse_collection(json: serde_json::Value) -> Collection {
        serde_json::from_value(json).expect("collection must parse")
    }

    fn minimal_info() -> serde_json::Value {
        json!({
            "name": "t",
            "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
        })
    }

    #[test]
    fn folder_first_discrimination_with_stray_request_key() {
        // Regression (backlog line 146): some real exports put `"request":
        // null` next to `"item": [...]`. Folder-first: the folder keeps its
        // children rather than being misclassified as a request (which
        // deserializes null request and drops the children).
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "folder",
                "request": null,
                "item": [{ "name": "child", "request": { "method": "GET", "url": "https://x.test/" } }]
            }]
        }));
        assert_eq!(col.item.len(), 1);
        match &col.item[0] {
            CollectionItem::Folder(f) => {
                assert_eq!(f.name, "folder");
                assert_eq!(f.item.len(), 1, "folder must keep its child");
                assert!(matches!(f.item[0], CollectionItem::Request(_)));
            }
            CollectionItem::Request(_) => panic!("folder misclassified as request"),
        }
    }

    #[test]
    fn malformed_request_errors_loudly_instead_of_silent_folder() {
        // Regression (backlog line 146): a malformed request sub-field used
        // to make untagged fall through to FolderItem — the request silently
        // became an EMPTY FOLDER and was dropped from the run. Now it errors.
        // Here the URL is an object with an invalid port type (string port
        // is fine, but a nested nonsense field isn't the trigger) — use a
        // body with an unknown shape instead: a `mode` that isn't handled is
        // still schema-legal; instead force a malformed URL form.
        let bad = json!({
            "info": minimal_info(),
            "item": [{ "name": "req", "request": { "url": { "raw": 123 } } }]
        });
        let err = serde_json::from_value::<Collection>(bad);
        assert!(
            err.is_err(),
            "malformed request sub-fields must error loudly, not become an empty folder: {:?}",
            err
        );
    }

    #[test]
    fn description_accepts_string_and_object_forms() {
        // Both schema-legal `description` shapes must parse: plain string or
        // `{"content": ..., "type": ...}` — returning the text content.
        // `description` is a field of RequestDetail (INSIDE `request`) — the
        // item-level key is not part of this model and would be dropped.
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": {
                    "description": "plain string desc",
                    "method": "GET",
                    "url": "https://x.test/"
                }
            }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            assert_eq!(r.request.description.as_deref(), Some("plain string desc"));
        } else {
            panic!("expected request");
        }

        let col2 = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": {
                    "description": { "content": "obj desc", "type": "text/plain" },
                    "method": "GET",
                    "url": "https://x.test/"
                }
            }]
        }));
        if let CollectionItem::Request(r) = &col2.item[0] {
            assert_eq!(r.request.description.as_deref(), Some("obj desc"));
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn script_exec_accepts_array_and_single_string() {
        // `script.exec` may be an array of lines or a single string; the
        // string form is wrapped into a one-element array so a string-form
        // exec cannot fail RequestItem parsing (the pre-fix silent-drop bug).
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": { "method": "GET", "url": "https://x.test/" },
                "event": [{ "listen": "test", "script": { "exec": "pm.test('a', () => true);" } }]
            }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            let script = &r.event[0].script.as_ref().expect("script");
            assert_eq!(script.exec.len(), 1);
            assert_eq!(script.exec[0], "pm.test('a', () => true);");
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn response_time_accepts_number_and_string() {
        // Postman exports `response_time` as a NUMBER (ms); some exporters
        // emit a string. Both normalize to a string.
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": { "method": "GET", "url": "https://x.test/" },
                "response": [{ "name": "saved", "code": 200, "response_time": 123 }]
            }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            assert_eq!(r.response[0].response_time.as_deref(), Some("123"));
            assert_eq!(r.response[0].code, 200);
        } else {
            panic!("expected request");
        }

        let col2 = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": { "method": "GET", "url": "https://x.test/" },
                "response": [{ "name": "saved", "response_time": "456" }]
            }]
        }));
        if let CollectionItem::Request(r) = &col2.item[0] {
            assert_eq!(r.response[0].response_time.as_deref(), Some("456"));
            assert_eq!(r.response[0].code, 0, "missing code defaults to 0");
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn url_accepts_object_and_plain_string_forms() {
        // Postman may export a URL as the structured object or a bare string.
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": { "method": "GET", "url": "https://x.test/path?a=1" }
            }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            let u = r.request.url.as_ref().expect("url");
            assert_eq!(u.raw.as_deref(), Some("https://x.test/path?a=1"));
            assert!(u.host.is_empty());
        } else {
            panic!("expected request");
        }

        let col2 = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": {
                    "method": "GET",
                    "url": {
                        "raw": "https://x.test/path?a=1",
                        "protocol": "https",
                        "host": ["x", "test"],
                        "path": ["path"],
                        "query": [{ "key": "a", "value": "1" }]
                    }
                }
            }]
        }));
        if let CollectionItem::Request(r) = &col2.item[0] {
            let u = r.request.url.as_ref().expect("url");
            assert_eq!(u.protocol.as_deref(), Some("https"));
            assert_eq!(u.host, vec!["x", "test"]);
            assert_eq!(u.query.len(), 1);
            assert_eq!(u.query[0].key, "a");
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn header_without_value_defaults_to_empty() {
        // A header with no `value` is schema-legal; it must not fail parsing.
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": { "method": "GET", "url": "https://x.test/", "header": [{ "key": "X-Empty" }] }
            }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            assert_eq!(r.request.header.len(), 1);
            assert_eq!(r.request.header[0].key, "X-Empty");
            assert_eq!(r.request.header[0].value, "");
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn method_defaults_to_get() {
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{ "name": "r", "request": { "url": "https://x.test/" } }]
        }));
        if let CollectionItem::Request(r) = &col.item[0] {
            assert_eq!(r.request.method, "GET");
        } else {
            panic!("expected request");
        }
    }

    #[test]
    fn collection_roundtrip_preserves_forms() {
        // A parsed collection must re-serialize to the same shape (round-trip
        // stability for distributed workers / spool / replay).
        let col = parse_collection(json!({
            "info": minimal_info(),
            "item": [{
                "name": "r",
                "request": {
                    "method": "POST",
                    "url": "https://x.test/submit",
                    "header": [{ "key": "Content-Type", "value": "application/json" }]
                }
            }]
        }));
        let back = serde_json::to_value(&col).expect("serialize");
        let item = &back["item"][0];
        assert_eq!(item["name"], "r");
        assert_eq!(item["request"]["method"], "POST");
        // UrlDetail has derived Serialize — a string-form URL re-serializes
        // as the object form with `raw` carrying the original string.
        assert_eq!(
            item["request"]["url"]["raw"],
            "https://x.test/submit",
            "url must round-trip (object form, raw preserved)"
        );
        // And it re-parses.
        let again: Collection = serde_json::from_value(back).expect("re-parse");
        assert_eq!(again.item.len(), 1);
    }
}
