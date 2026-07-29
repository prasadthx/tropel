use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsError {
    #[error("Runtime error: {0}")]
    Runtime(String),

    #[error("Evaluation error: {0}")]
    Eval(String),

    #[error("Module error: {0}")]
    Module(String),

    #[error("Timeout exceeded")]
    Timeout,

    #[error("Memory limit exceeded")]
    MemoryLimit,

    #[error("Context creation failed: {0}")]
    ContextCreation(String),

    #[error("Bootstrap failed: {0}")]
    Bootstrap(String),

    #[error("Value conversion error: {0}")]
    Conversion(String),
}

pub type Result<T> = std::result::Result<T, JsError>;
