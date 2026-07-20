/// MemQ consumer: polls Kafka notifications, fetches batches from storage, parses messages.
///
/// Mirrors the Java `MemqConsumer` API with builder pattern, poll iterator,
/// and offset commit methods.

use crate::error::{MemqError, Result};
use crate::kafka::{EnrichedNotification, KafkaNotificationConfig, KafkaNotificationSource};
use crate::kafka::AutoOffsetReset;
use crate::notification::Notification;
use crate::storage::{create_storage_handler, MemqStorageConfig, StorageHandler};
use crate::{parse_batch, Message};
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for the MemQ consumer.
#[derive(Debug, Clone)]
pub struct MemqConsumerConfig {
    /// Kafka bootstrap servers.
    pub bootstrap_servers: Vec<String>,
    /// Consumer group ID.
    pub group_id: String,
    /// Notification topic name.
    pub notification_topic: String,
    /// Storage backend configuration.
    pub storage_config: MemqStorageConfig,
    /// Auto offset reset strategy.
    pub auto_offset_reset: AutoOffsetReset,
}

/// An iterator over messages from a single notification (one batch).
struct NotificationMessageIterator {
    /// Raw decompressed payload bytes (preserves wire format for MessageIterator).
    data: Vec<u8>,
    pos: usize,
    remaining: u32,
}

impl NotificationMessageIterator {
    fn new(data: Vec<u8>, remaining: u32) -> Self {
        NotificationMessageIterator { data, pos: 0, remaining }
    }

    fn has_next(&self) -> bool {
        self.remaining > 0 && self.pos < self.data.len()
    }

    fn next(&mut self) -> Result<Message> {
        if !self.has_next() {
            return Err(MemqError::NoMoreMessages);
        }

        let pos = self.pos;

        // Read internalFieldsLength
        if pos + 2 > self.data.len() {
            return Err(MemqError::UnexpectedEof);
        }
        let internal_fields_length = u16::from_be_bytes([self.data[pos], self.data[pos + 1]]) as usize;
        self.pos += 2;

        let mut write_timestamp = None;
        let mut message_id = None;
        let mut headers = Vec::new();

        if internal_fields_length > 0 {
            if self.pos + 8 > self.data.len() {
                return Err(MemqError::UnexpectedEof);
            }
            write_timestamp = Some(i64::from_be_bytes([
                self.data[self.pos], self.data[self.pos + 1], self.data[self.pos + 2],
                self.data[self.pos + 3], self.data[self.pos + 4], self.data[self.pos + 5],
                self.data[self.pos + 6], self.data[self.pos + 7],
            ]));
            self.pos += 8;

            if self.pos > self.data.len() {
                return Err(MemqError::UnexpectedEof);
            }
            let message_id_length = self.data[self.pos] as usize;
            self.pos += 1;

            if message_id_length > 0 {
                if self.pos + message_id_length > self.data.len() {
                    return Err(MemqError::UnexpectedEof);
                }
                message_id = Some(self.data[self.pos..self.pos + message_id_length].to_vec());
                self.pos += message_id_length;
            }

            if self.pos + 2 > self.data.len() {
                return Err(MemqError::UnexpectedEof);
            }
            let mut headers_length = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
            self.pos += 2;

            while headers_length > 0 && self.pos < self.data.len() {
                if self.pos + 4 > self.data.len() {
                    return Err(MemqError::UnexpectedEof);
                }
                let key_length = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
                self.pos += 2;
                if self.pos + key_length > self.data.len() {
                    return Err(MemqError::UnexpectedEof);
                }
                let key = String::from_utf8_lossy(&self.data[self.pos..self.pos + key_length]).to_string();
                self.pos += key_length;
                if self.pos + 2 > self.data.len() {
                    return Err(MemqError::UnexpectedEof);
                }
                let value_length = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]) as usize;
                self.pos += 2;
                if self.pos + value_length > self.data.len() {
                    return Err(MemqError::UnexpectedEof);
                }
                let value = self.data[self.pos..self.pos + value_length].to_vec();
                self.pos += value_length;
                headers.push((key, value));
                headers_length -= 4 + key_length + value_length;
            }
        }

        // Read key
        if self.pos + 4 > self.data.len() {
            return Err(MemqError::UnexpectedEof);
        }
        let key_length = i32::from_be_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
        ]) as usize;
        self.pos += 4;
        let key = if key_length > 0 && self.pos + key_length <= self.data.len() {
            Some(self.data[self.pos..self.pos + key_length].to_vec())
        } else {
            None
        };
        if key_length > 0 {
            self.pos += key_length;
        }

        // Read value
        if self.pos + 4 > self.data.len() {
            return Err(MemqError::UnexpectedEof);
        }
        let value_length = i32::from_be_bytes([
            self.data[self.pos], self.data[self.pos + 1],
            self.data[self.pos + 2], self.data[self.pos + 3],
        ]) as usize;
        self.pos += 4;
        let value = if value_length > 0 && self.pos + value_length <= self.data.len() {
            Some(self.data[self.pos..self.pos + value_length].to_vec())
        } else {
            None
        };
        if value_length > 0 {
            self.pos += value_length;
        }

        self.remaining -= 1;

        Ok(Message {
            key,
            value,
            write_timestamp,
            message_id,
            headers,
        })
    }
}

/// A stream of messages from a single batch.
struct BatchMessageStream {
    iter: NotificationMessageIterator,
}

impl BatchMessageStream {
    fn has_next(&self) -> bool {
        self.iter.has_next()
    }

    fn next(&mut self) -> Result<Message> {
        self.iter.next()
    }
}

/// A notification batch iterator that yields messages from batches.
///
/// Wraps the notification queue and storage fetch logic. Mirrors Java's
/// `NotificationBatchIterator` inner class.
struct NotificationBatchIterator {
    /// Queue of enriched notifications from Kafka.
    notification_queue: VecDeque<EnrichedNotification>,
    /// Current message iterator for the active batch.
    current: Option<BatchMessageStream>,
    /// Retry count for current notification.
    retry_count: u32,
    max_retries: u32,
}

impl NotificationBatchIterator {
    fn new(notifications: Vec<EnrichedNotification>) -> Self {
        NotificationBatchIterator {
            notification_queue: notifications.into_iter().collect(),
            current: None,
            retry_count: 0,
            max_retries: 2,
        }
    }

    fn has_next(&mut self, storage: &dyn StorageHandler) -> bool {
        loop {
            match &self.current {
                Some(stream) => {
                    if stream.has_next() {
                        return true;
                    }
                    // Stream exhausted, load next
                    self.current = None;
                    self.retry_count = 0;
                }
                None => {
                    // Try to get next notification
                    if let Some(notification) = self.notification_queue.pop_front() {
                        if let Ok(stream) = load_batch(storage, &notification.notification) {
                            self.current = Some(stream);
                            self.retry_count = 0;
                            return true;
                        } else {
                            self.retry_count += 1;
                            if self.retry_count > self.max_retries {
                                // Skip this notification after retries
                                self.retry_count = 0;
                            }
                        }
                    } else {
                        return false;
                    }
                }
            }
        }
    }

    fn next(&mut self) -> Result<Message> {
        if let Some(ref mut stream) = self.current {
            stream.next()
        } else {
            Err(MemqError::NoMoreMessages)
        }
    }
}

/// Load a batch from storage and create a message stream.
fn load_batch(storage: &dyn StorageHandler, notification: &Notification) -> Result<BatchMessageStream> {
    let batch_data = storage.fetch_batch(&notification.bucket, &notification.key)?;
    let (_batch_header, message_batches) = parse_batch(&batch_data.data)?;

    // Collect all messages from all sub-batches into one iterator
    let mut all_data = Vec::new();
    for msg_batch in &message_batches {
        all_data.extend_from_slice(&msg_batch.decompressed_payload);
    }

    let total_count: u32 = message_batches.iter().map(|b| b.header.log_message_count).sum();
    let iter = NotificationMessageIterator::new(all_data, total_count);

    Ok(BatchMessageStream { iter })
}

/// The MemQ consumer.
///
/// Polls Kafka for notification topics, fetches batches from storage,
/// and yields messages via an iterator. Supports offset commits.
///
/// # Example
/// ```ignore
/// let config = MemqConsumerConfig {
///     bootstrap_servers: vec!["localhost:9092".to_string()],
///     group_id: "my-consumer-group".to_string(),
///     notification_topic: "TestTopicNotifications".to_string(),
///     storage_config: MemqStorageConfig::Fs(FsStorageConfig {
///         root_dir: PathBuf::from("/tmp/memq-storage"),
///     }),
///     auto_offset_reset: AutoOffsetReset::Earliest,
/// };
///
/// let mut consumer = MemqConsumer::new(config)?;
///
/// loop {
///     let messages = consumer.poll(Duration::from_secs(1))?;
///     for msg in messages {
///         println!("key={:?} value={:?}", msg.key, msg.value);
///     }
///     consumer.commit_offset()?;
/// }
/// ```
pub struct MemqConsumer {
    _config: MemqConsumerConfig,
    kafka_source: KafkaNotificationSource,
    storage: Arc<dyn StorageHandler>,
    /// Queue of enriched notifications ready to be processed.
    notification_queue: VecDeque<EnrichedNotification>,
    /// Current batch iterator.
    batch_iterator: Option<NotificationBatchIterator>,
    /// Whether the consumer is closed.
    closed: bool,
    /// Dry run mode (skip offset commits).
    dry_run: bool,
}

impl MemqConsumer {
    /// Create a new MemqConsumer with the given config.
    pub fn new(config: MemqConsumerConfig) -> Result<Self> {
        let kafka_config = KafkaNotificationConfig {
            bootstrap_servers: config.bootstrap_servers.clone(),
            group_id: config.group_id.clone(),
            notification_topic: config.notification_topic.clone(),
            auto_offset_reset: config.auto_offset_reset,
            max_poll_records: 100,
        };

        let kafka_source = KafkaNotificationSource::new(kafka_config)?;
        let storage: Arc<dyn StorageHandler> = Arc::from(create_storage_handler(config.storage_config.clone())?);

        Ok(MemqConsumer {
            _config: config,
            kafka_source,
            storage,
            notification_queue: VecDeque::new(),
            batch_iterator: None,
            closed: false,
            dry_run: false,
        })
    }

    /// Set dry run mode (don't commit offsets).
    #[must_use]
    pub fn dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    /// Poll Kafka for new notifications and return messages from all available batches.
    ///
    /// Blocks for at most `timeout`. Returns messages from all notifications
    /// that were available within the timeout.
    pub fn poll(&mut self, timeout: Duration) -> Result<Vec<Message>> {
        if self.closed {
            return Err(MemqError::ConsumerClosed);
        }

        // Poll Kafka for new notifications
        let notifications = self.kafka_source.poll(timeout)?;

        // Add to notification queue
        self.notification_queue.extend(notifications);

        // Create batch iterator if we have notifications
        if self.notification_queue.is_empty() {
            return Ok(Vec::new());
        }

        if self.batch_iterator.is_none() {
            self.batch_iterator = Some(NotificationBatchIterator::new(
                self.notification_queue.drain(..).collect(),
            ));
        }

        // Collect all messages from the batch iterator
        let mut messages = Vec::new();
        if let Some(ref mut iter) = self.batch_iterator {
            while iter.has_next(self.storage.as_ref()) {
                messages.push(iter.next()?);
            }
            // If iterator is exhausted, reset it
            if self.batch_iterator.as_ref().unwrap().notification_queue.is_empty() {
                self.batch_iterator = None;
            }
        }

        Ok(messages)
    }

    /// Commit consumed offsets to Kafka synchronously.
    pub fn commit_offset(&mut self) -> Result<()> {
        if self.dry_run {
            return Ok(());
        }
        self.kafka_source.commit()
    }

    /// Commit specific partition offsets.
    pub fn commit_offsets(&mut self, offsets: &[(i32, i64)]) -> Result<()> {
        if self.dry_run {
            return Ok(());
        }
        self.kafka_source.commit_offsets(offsets)
    }

    /// Close the consumer.
    pub fn close(&mut self) {
        self.closed = true;
    }

    /// Whether the consumer is closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Push an enriched notification directly into the consumer's queue.
    ///
    /// Useful for testing — bypasses Kafka entirely.
    pub fn push_notification(&mut self, enriched: EnrichedNotification) {
        self.notification_queue.push_back(enriched);
        // Reset batch iterator so the new notification gets processed
        self.batch_iterator = None;
    }
}
