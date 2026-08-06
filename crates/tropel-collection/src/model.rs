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
    Ok(match Option::<DescriptionForm>::deserialize(deserializer)? {
        Some(DescriptionForm::Str(s)) => Some(s),
        Some(DescriptionForm::Obj { content }) => content,
        None => None,
    })
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
