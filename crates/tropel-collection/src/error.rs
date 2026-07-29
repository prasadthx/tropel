use thiserror::Error;

#[derive(Error, Debug)]
pub enum CollectionError {
    #[error("Unsupported collection version: {0}")]
    UnsupportedVersion(String),

    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, CollectionError>;
