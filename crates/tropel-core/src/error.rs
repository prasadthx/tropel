use thiserror::Error;

/// Top-level error type for Tropel operations.
#[derive(Error, Debug)]
pub enum TropelError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Collection error: {0}")]
    Collection(String),

    #[error("Variable resolution error: {0}")]
    Variable(String),

    #[error("JavaScript error: {0}")]
    Js(String),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Extension error: {0}")]
    Extension(String),

    #[error("Metric error: {0}")]
    Metric(String),

    #[error("Report error: {0}")]
    Report(String),

    #[error("{0}")]
    Other(String),
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, TropelError>;
