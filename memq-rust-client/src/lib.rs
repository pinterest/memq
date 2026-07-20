//! MemQ Rust client: protocol parsing and storage format handling.
//!
//! This crate provides the ability to parse MemQ batch files as they are
//! stored on S3. The batch format consists of:
//!
//! 1. **BatchHeader** (8 + 12*N bytes) — seek index mapping message index to byte offset/size
//! 2. **Zero or more MessageHeader + compressed payload pairs** — each header contains metadata
//!    (compression, CRC, message count), followed by message_length bytes of compressed data

pub mod batch_header;
pub mod compression;
pub mod consumer;
pub mod deserializer;
pub mod error;
pub mod kafka;
pub mod message;
pub mod notification;
pub mod storage;

pub use batch_header::BatchHeader;
pub use compression::Compression;
pub use consumer::{MemqConsumer, MemqConsumerConfig};
pub use kafka::AutoOffsetReset;
pub use deserializer::{ByteArrayDeserializer, Deserializer, StringDeserializer};
pub use error::{MemqError, Result};
pub use kafka::{EnrichedNotification, KafkaNotificationConfig, KafkaNotificationSource};
pub use message::{Message, MessageIterator};
pub use message_header::MessageHeader;
pub use notification::Notification;
pub use storage::{
    BatchData, FsStorageConfig, FsStorageHandler, MemqStorageConfig, S3StorageConfig, S3StorageHandler,
    StorageHandler, create_storage_handler,
};

/// Parsed message header + decompressed payload pair.
#[derive(Debug)]
pub struct MessageBatch {
    pub header: message_header::MessageHeader,
    pub decompressed_payload: Vec<u8>,
}

/// Parse a complete batch from raw bytes (as stored on S3).
///
/// Batch layout:
///   BatchHeader (8 + 12*N bytes)
///   MessageHeader + compressed payload (repeated N times)
///
/// CRC32 is computed over the compressed payload bytes of each message header,
/// exactly as the Java producer does.
pub fn parse_batch(raw: &[u8]) -> Result<(BatchHeader, Vec<MessageBatch>)> {
    use crate::error::MemqError;

    if raw.len() < 12 {
        return Err(MemqError::BatchTooSmall(raw.len(), 12));
    }

    // Parse BatchHeader
    let batch_header = BatchHeader::from_bytes(raw)?;

    // Determine where the message headers start
    let mut pos = 8 + 12 * batch_header.entries.len();
    let mut batches = Vec::new();

    while pos < raw.len() {
        // Need at least the fixed portion of the message header (6 bytes)
        if raw.len() - pos < 6 {
            break;
        }

        // Parse MessageHeader
        let msg_header = message_header::MessageHeader::from_bytes(&raw[pos..])?;
        let msg_len = msg_header.message_length as usize;
        let mh_total = msg_header.total_bytes();

        // Determine where the compressed payload starts
        let payload_start = pos + mh_total;
        if raw.len() < payload_start + msg_len {
            return Err(MemqError::BatchTooSmall(raw.len(), payload_start + msg_len));
        }

        // CRC32 is over the raw compressed payload bytes
        let compressed_payload = &raw[payload_start..payload_start + msg_len];
        let actual_crc = crc32fast::hash(compressed_payload);
        if actual_crc != msg_header.crc {
            return Err(MemqError::CrcMismatch {
                expected: msg_header.crc,
                actual: actual_crc,
            });
        }

        // Decompress the payload
        let decompressed = msg_header.compression.decompress(compressed_payload)?;

        batches.push(MessageBatch {
            header: msg_header,
            decompressed_payload: decompressed,
        });

        // Move to the next message header
        pos = payload_start + msg_len;
    }

    Ok((batch_header, batches))
}

/// Message header module — kept separate for clarity.
pub mod message_header;

/// Convenience: parse a batch and iterate over all messages across all sub-batches.
pub fn parse_batch_messages(raw: &[u8]) -> Result<Vec<Message>> {
    let (_batch_header, batches) = parse_batch(raw)?;
    let mut messages = Vec::new();
    for batch in batches {
        let mut iter = MessageIterator::new(&batch.decompressed_payload, batch.header.log_message_count);
        while iter.has_next() {
            messages.push(iter.next()?);
        }
    }
    Ok(messages)
}
