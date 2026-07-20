/// All errors that can occur during MemQ batch/message parsing.
#[derive(Debug, thiserror::Error)]
pub enum MemqError {
    #[error("CRC mismatch: expected {expected}, got {actual}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("Unknown compression type: {0}")]
    UnknownCompression(u8),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Batch too small to contain header: {0} bytes < {1}")]
    BatchTooSmall(usize, usize),

    #[error("Message stream ended unexpectedly")]
    UnexpectedEof,

    #[error("Notification JSON missing field: {0}")]
    MissingNotificationField(String),

    #[error("Invalid batch header: {0}")]
    InvalidHeader(String),

    // --- S3 / Storage errors ---

    #[error("S3 object not found: {bucket}/{key}")]
    S3NotFound { bucket: String, key: String },

    #[error("S3 access forbidden: {bucket}/{key}")]
    S3Forbidden { bucket: String, key: String },

    #[error("S3 error: {0}")]
    S3Client(String),

    #[error("Failed to generate presigned URL: {0}")]
    PresignError(String),

    #[error("HTTP error fetching from S3: {status} {msg}")]
    HttpError { status: u16, msg: String },

    // --- Kafka / Notification errors ---

    #[error("Kafka error: {0}")]
    Kafka(String),

    #[error("Failed to parse Kafka notification JSON: {0}")]
    KafkaNotificationParse(String),

    #[error("Consumer closed")]
    ConsumerClosed,

    #[error("No topics subscribed")]
    NoTopicsSubscribed,

    #[error("No more messages to read")]
    NoMoreMessages,
}

/// Result type for MemQ operations.
pub type Result<T> = std::result::Result<T, MemqError>;
