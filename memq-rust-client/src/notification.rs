use crate::error::{MemqError, Result};

/// Kafka notification JSON fields.
///
/// This is the JSON payload published by KafkaNotificationSink after
/// a batch is uploaded to S3.
#[derive(Debug, Clone)]
pub struct Notification {
    pub bucket: String,
    pub key: String,
    pub object_size: u64,
    pub topic: String,
    pub header_size: u64,
    pub num_batch_messages: u64,
    pub num_attempts: u64,
}

impl Notification {
    /// Parse a notification from a JSON string.
    ///
    /// Supports two formats:
    /// 1. Rust test format: {"bucket": "...", "key": "...", ...}
    /// 2. Java MemQ broker format: {"path": "/absolute/path/...", ...}
    ///    (bucket/key derived from path, numAttempts defaults to 1)
    pub fn from_json(json: &str) -> Result<Self> {
        let obj: serde_json::Value = serde_json::from_str(json).map_err(|e| {
            MemqError::MissingNotificationField(format!("JSON parse: {}", e))
        })?;

        // Java broker uses "objectSize", Rust tests use "size"
        let object_size = obj.get("size")
            .or_else(|| obj.get("objectSize"))
            .ok_or_else(|| MemqError::MissingNotificationField("size or objectSize".into()))?
            .as_u64()
            .ok_or_else(|| MemqError::MissingNotificationField("size".into()))?;

        let topic = obj.get("topic")
            .ok_or_else(|| MemqError::MissingNotificationField("topic".into()))?
            .as_str()
            .ok_or_else(|| MemqError::MissingNotificationField("topic".into()))?
            .to_string();

        let header_size = obj.get("headerSize")
            .ok_or_else(|| MemqError::MissingNotificationField("headerSize".into()))?
            .as_u64()
            .ok_or_else(|| MemqError::MissingNotificationField("headerSize".into()))?;

        let num_batch_messages = obj.get("numBatchMessages")
            .ok_or_else(|| MemqError::MissingNotificationField("numBatchMessages".into()))?
            .as_u64()
            .ok_or_else(|| MemqError::MissingNotificationField("numBatchMessages".into()))?;

        // Handle both Java broker format (path-based) and Rust test format (bucket/key)
        let (bucket, key) = if let Some(path) = obj.get("path").and_then(|v| v.as_str()) {
            // Java MemQ broker format: path is an absolute path
            // Derive bucket from the directory containing the file, key from the filename
            let path_buf = std::path::Path::new(path);
            let filename = path_buf
                .file_name()
                .map(|f| f.to_string_lossy().to_string())
                .ok_or_else(|| MemqError::MissingNotificationField("path (no filename)".into()))?;
            let bucket = path_buf
                .parent()
                .map(|p| p.to_string_lossy().to_string())
                .unwrap_or_default();
            (bucket, filename)
        } else {
            // Rust test format: explicit bucket and key
            let bucket = obj.get("bucket")
                .ok_or_else(|| MemqError::MissingNotificationField("bucket or path".into()))?
                .as_str()
                .ok_or_else(|| MemqError::MissingNotificationField("bucket".into()))?
                .to_string();

            let key = obj.get("key")
                .ok_or_else(|| MemqError::MissingNotificationField("key".into()))?
                .as_str()
                .ok_or_else(|| MemqError::MissingNotificationField("key".into()))?
                .to_string();

            (bucket, key)
        };

        let num_attempts = obj.get("numAttempts")
            .and_then(|v| v.as_u64())
            .unwrap_or(1);

        Ok(Notification {
            bucket,
            key,
            object_size,
            topic,
            header_size,
            num_batch_messages,
            num_attempts,
        })
    }

    /// Parse a notification from a serde_json Value.
    pub fn from_value(obj: &serde_json::Value) -> Result<Self> {
        Self::from_json(&obj.to_string())
    }
}
