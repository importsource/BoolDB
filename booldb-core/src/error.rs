use thiserror::Error;

pub type Result<T> = std::result::Result<T, BoolDBError>;

#[derive(Debug, Error)]
pub enum BoolDBError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Page {page_id} not found")]
    PageNotFound { page_id: u32 },

    #[error("Buffer pool is full, no evictable pages")]
    BufferPoolFull,

    #[error("Page {page_id} is full, cannot insert tuple of {tuple_size} bytes")]
    PageFull { page_id: u32, tuple_size: usize },

    #[error("Tuple not found: page {page_id}, slot {slot_id}")]
    TupleNotFound { page_id: u32, slot_id: u16 },

    #[error("Table '{0}' not found")]
    TableNotFound(String),

    #[error("Table '{0}' already exists")]
    TableAlreadyExists(String),

    #[error("Index '{0}' not found")]
    IndexNotFound(String),

    #[error("Column '{0}' not found")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {got}")]
    TypeMismatch { expected: String, got: String },

    #[error("SQL error: {0}")]
    Sql(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Lock conflict: {0}")]
    LockConflict(String),

    #[error("Internal error: {0}")]
    Internal(String),
}
