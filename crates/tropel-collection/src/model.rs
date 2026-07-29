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
    pub description: Option<String>,
    pub schema: String,
}

/// A single item or folder.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CollectionItem {
    Request(RequestItem),
    Folder(FolderItem),
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

/// Request details.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestDetail {
    pub method: String,
    pub header: Vec<Header>,
    pub body: Option<RequestBody>,
    pub url: UrlDetail,
    pub auth: Option<CollectionAuth>,
    pub description: Option<String>,
}

/// URL detail.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlDetail {
    pub raw: Option<String>,
    pub protocol: Option<String>,
    pub host: Vec<String>,
    pub port: Option<String>,
    pub path: Vec<String>,
    #[serde(default)]
    pub query: Vec<QueryParam>,
    pub variable: Vec<UrlVariable>,
    pub hash: Option<String>,
}

/// URL query parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryParam {
    pub key: String,
    pub value: Option<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub disabled: bool,
}

/// URL variable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UrlVariable {
    pub key: String,
    pub value: Option<String>,
    pub description: Option<String>,
}

/// HTTP header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub key: String,
    pub value: String,
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
    #[serde(default)]
    pub exec: Vec<String>,
    #[serde(rename = "type")]
    pub script_type: Option<String>,
    pub src: Option<String>,
}

impl Script {
    /// Join exec lines into a single script string.
    pub fn to_string(&self) -> String {
        self.exec.join("\n")
    }
}

/// Variable definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Variable {
    pub key: String,
    pub value: Option<serde_json::Value>,
    #[serde(rename = "type")]
    pub var_type: Option<String>,
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
    pub code: u16,
    pub header: Vec<Header>,
    pub body: Option<String>,
    pub content_type: Option<String>,
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
