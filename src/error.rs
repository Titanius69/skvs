use thiserror::Error;

#[derive(Error, Debug)]
pub enum SkvsError {
    #[error("Config error: {0}")]
    Config(#[from] anyhow::Error),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    Serialization(#[from] bincode::Error),
    #[error("SQL parse error: {0}")]
    SqlParse(String),
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),
    #[error("Unsupported operation: {0}")]
    Unsupported(String),
    #[error("Transaction error: {0}")]
    Transaction(String),
    #[error("Schema error: {0}")]
    Schema(String),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Trigger error: {0}")]
    Trigger(String),
    #[error("View error: {0}")]
    View(String),
    #[error("JSON error: {0}")]
    Json(String),
    #[error("FTS error: {0}")]
    Fts(String),
}

impl From<sqlparser::parser::ParserError> for SkvsError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        SkvsError::SqlParse(e.to_string())
    }
}

impl From<serde_json::Error> for SkvsError {
    fn from(e: serde_json::Error) -> Self {
        SkvsError::Json(e.to_string())
    }
}

// Alias kept for backwards compatibility with older code that referred to `KvsError`.
pub use SkvsError as KvsError;