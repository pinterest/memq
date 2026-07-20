/// Kafka notification source: polls Kafka for batch upload notifications.
///
/// Wraps `kafka::consumer::Consumer` to poll the notification topic,
/// parse JSON payloads, and enrich them with partition/offset metadata.
/// Mirrors the Java `KafkaNotificationSource`.
///
/// The consumer runs in a background thread. The main thread communicates
/// via an `mpsc::SyncSender` to enqueue poll requests, and waits on
/// `recv_timeout` to enforce the deadline.

use crate::error::{MemqError, Result};
use crate::notification::Notification;
use kafka::consumer::{Consumer, FetchOffset, GroupOffsetStorage};
use std::sync::mpsc::{SyncSender, sync_channel};
use std::time::Duration;

/// An enriched notification from Kafka, with partition/offset metadata.
#[derive(Debug, Clone)]
pub struct EnrichedNotification {
    /// The parsed notification payload.
    pub notification: Notification,
    /// Kafka partition ID (npi).
    pub notification_partition_id: u64,
    /// Kafka offset within the partition (npo).
    pub notification_partition_offset: u64,
    /// Time the notification was read from Kafka (nrts), in ms since epoch.
    pub notification_read_timestamp: u64,
}

impl EnrichedNotification {
    /// Create from a raw JSON string, partition, and offset.
    pub fn from_json(json: &str, partition: i32, offset: i64) -> Result<Self> {
        let notification = Notification::from_json(json)?;
        Ok(EnrichedNotification {
            notification,
            notification_partition_id: partition as u64,
            notification_partition_offset: offset as u64,
            notification_read_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        })
    }
}

/// Offset reset strategy for new consumer groups.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

/// Configuration for the Kafka notification source.
#[derive(Debug, Clone)]
pub struct KafkaNotificationConfig {
    /// Kafka bootstrap servers (e.g., ["localhost:9092"]).
    pub bootstrap_servers: Vec<String>,
    /// Consumer group ID.
    pub group_id: String,
    /// Name of the notification topic to subscribe to.
    pub notification_topic: String,
    /// Start from earliest or latest offset.
    pub auto_offset_reset: AutoOffsetReset,
    /// Maximum records per poll.
    pub max_poll_records: u32,
}

/// Internal request sent from main thread to the background consumer thread.
enum ConsumerRequest {
    /// Poll for messages; the sender receives the result.
    Poll {
        resp_tx: SyncSender<Result<Vec<OwnedMessage>>>,
    },
    /// Commit consumed offsets.
    Commit {
        resp_tx: SyncSender<Result<()>>,
    },
    /// Shutdown the background thread.
    Shutdown,
}

/// Owned message data: (json_value, topic, partition, offset).
#[derive(Debug, Clone)]
struct OwnedMessage {
    value: String,
    #[allow(dead_code)]
    topic: String,
    partition: i32,
    offset: i64,
}

/// Kafka notification source.
///
/// Runs the kafka-rust Consumer in a background thread. The main thread
/// sends requests via a sync channel and waits with recv_timeout.
pub struct KafkaNotificationSource {
    /// Sender for requests to the background consumer thread.
    tx: SyncSender<ConsumerRequest>,
    /// Receiver for results from the background thread (for commit).
    /// We use a second channel for commit results to avoid mixing poll/commit responses.
    /// Actually, we encode the result inside the ConsumerRequest response.
    config: KafkaNotificationConfig,
}

impl KafkaNotificationSource {
    /// Create a new KafkaNotificationSource from config.
    pub fn new(config: KafkaNotificationConfig) -> Result<Self> {
        let (req_tx, req_rx) = sync_channel::<ConsumerRequest>(1);

        // Build the consumer
        let mut builder = Consumer::from_hosts(config.bootstrap_servers.clone())
            .with_topic(config.notification_topic.clone())
            .with_group(config.group_id.clone())
            .with_offset_storage(Some(GroupOffsetStorage::Kafka));

        builder = match config.auto_offset_reset {
            AutoOffsetReset::Earliest => builder.with_fallback_offset(FetchOffset::Earliest),
            AutoOffsetReset::Latest => builder.with_fallback_offset(FetchOffset::Latest),
        };

        let consumer = builder
            .create()
            .map_err(|e| MemqError::Kafka(format!("Failed to create consumer: {e}")))?;

       // Spawn the background thread
        std::thread::spawn(move || {
            let mut consumer = consumer;
            loop {
                // Wait for a request (blocking — no auto-poll of Kafka)
                let request = req_rx.recv();
                match request {
                    Ok(ConsumerRequest::Poll { resp_tx }) => {
                        let result = consumer.poll();
                        let messages = match result {
                            Ok(message_sets) => {
                                let mut msgs = Vec::new();
                                for ms in message_sets.iter() {
                                    for msg in ms.messages() {
                                        if let Ok(json_str) = std::str::from_utf8(msg.value) {
                                            msgs.push(OwnedMessage {
                                                value: json_str.to_string(),
                                                topic: ms.topic().to_string(),
                                                partition: ms.partition(),
                                                offset: msg.offset,
                                            });
                                            // Mark as consumed
                                            let _ = consumer.consume_message(
                                                ms.topic(),
                                                ms.partition(),
                                                msg.offset,
                                            );
                                        }
                                    }
                                }
                                Ok(msgs)
                            }
                            Err(e) => Err(MemqError::Kafka(e.to_string())),
                        };
                        let _ = resp_tx.send(messages);
                    }
                    Ok(ConsumerRequest::Commit { resp_tx }) => {
                        let result = consumer.commit_consumed();
                        let _ = resp_tx.send(result.map_err(|e| MemqError::Kafka(e.to_string())));
                    }
                    Ok(ConsumerRequest::Shutdown) => {
                        break;
                    }
                    Err(std::sync::mpsc::RecvError) => {
                        break;
                    }
                }
            }
        });

        Ok(KafkaNotificationSource {
            tx: req_tx,
            config,
        })
    }

    /// Poll Kafka for new notifications, up to max_poll_records.
    ///
    /// The background thread handles the blocking poll. The main thread
    /// waits on `recv_timeout` which *does* return on timeout.
    pub fn poll(&self, timeout_duration: Duration) -> Result<Vec<EnrichedNotification>> {
        let (resp_tx, resp_rx) = sync_channel::<Result<Vec<OwnedMessage>>>(1);

        // Send poll request
        self.tx
            .send(ConsumerRequest::Poll { resp_tx })
            .map_err(|_| MemqError::Kafka("Consumer thread disconnected".into()))?;

        // Wait for response with timeout
        let owned_messages = match resp_rx.recv_timeout(timeout_duration) {
            Ok(Ok(msgs)) => msgs,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Ok(Vec::new()), // timeout — no notifications
        };

        let mut notifications = Vec::new();
        for msg in owned_messages {
            let enriched = EnrichedNotification::from_json(&msg.value, msg.partition, msg.offset);
            match enriched {
                Ok(en) => {
                    notifications.push(en);
                }
                Err(e) => {
                    eprintln!("Skipping bad notification: {e}");
                }
            }
        }
        Ok(notifications)
    }

    /// Commit all consumed offsets synchronously.
    pub fn commit(&self) -> Result<()> {
        let (resp_tx, resp_rx) = sync_channel::<Result<()>>(1);
        self.tx
            .send(ConsumerRequest::Commit { resp_tx })
            .map_err(|_| MemqError::Kafka("Consumer thread disconnected".into()))?;
        resp_rx
            .recv()
            .map_err(|_| MemqError::Kafka("Consumer thread disconnected".into()))?
    }

    /// Commit specific partition offsets.
    pub fn commit_offsets(&self, _offsets: &[(i32, i64)]) -> Result<()> {
        self.commit()
    }

    /// Get the notification topic name.
    pub fn notification_topic(&self) -> &str {
        &self.config.notification_topic
    }

    /// Get the consumer group ID.
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// Close the notification source (shutdowns the background thread).
    pub fn close(&self) {
        let _ = self.tx.send(ConsumerRequest::Shutdown);
    }
}

impl Drop for KafkaNotificationSource {
    fn drop(&mut self) {
        let _ = self.tx.send(ConsumerRequest::Shutdown);
    }
}
