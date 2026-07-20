/// End-to-end test: real MemQ broker + Kafka + Rust consumer.
///
/// 1. Java MemQ producer wrote messages to a topic
/// 2. Broker stored them in /tmp/memq-storage
/// 3. Broker pushed notification to Kafka
/// 4. Rust consumer reads the batch from filesystem and verifies all messages

use memq_rust_client::kafka::{AutoOffsetReset, KafkaNotificationConfig, KafkaNotificationSource};
use memq_rust_client::storage::{FsStorageConfig, FsStorageHandler, StorageHandler};
use memq_rust_client::Message;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    println!("=== MemQ Rust Consumer — End-to-End Test ===\n");

    // --- Step 1: Read notification from Kafka ---
    println!("Step 1: Read notification from Kafka...");

    let config = KafkaNotificationConfig {
        bootstrap_servers: vec!["localhost:9092".to_string()],
        group_id: "rust-e2e-test-group".to_string(),
        notification_topic: "TestTopicNotifications".to_string(),
        auto_offset_reset: AutoOffsetReset::Earliest,
        max_poll_records: 100,
    };

    let kafka_source = KafkaNotificationSource::new(config).expect("Failed to create KafkaNotificationSource");

    // Poll for notifications (with a timeout)
    let notifications = kafka_source.poll(Duration::from_secs(3)).expect("Kafka poll failed");
    println!("  Notifications received: {}", notifications.len());

    if notifications.is_empty() {
        eprintln!("ERROR: No notifications received from Kafka!");
        std::process::exit(1);
    }

    // --- Step 2: Use the latest notification to find the batch ---
    println!("\nStep 2: Prepare storage handler...");
    let latest = notifications.last().unwrap();
    println!("  Bucket: {}", latest.notification.bucket);
    println!("  Key: {}", latest.notification.key);
    println!("  Object size: {}", latest.notification.object_size);

    // Create FsStorageHandler
    let root_dir = PathBuf::from("/tmp/memq-storage");
    let handler = FsStorageHandler::new(FsStorageConfig { root_dir: root_dir.clone() });

    // --- Step 3: Fetch batch from storage ---
    println!("\nStep 3: Fetch batch from filesystem...");
    let batch_data = handler.fetch_batch(&latest.notification.bucket, &latest.notification.key)
        .expect("Failed to fetch batch");
    println!("  Batch size: {} bytes", batch_data.data.len());

    // --- Step 4: Parse the batch ---
    println!("\nStep 4: Parse batch and read messages...");
    let (_header, batches) = memq_rust_client::parse_batch(&batch_data.data)
        .expect("Failed to parse batch");
    println!("  Sub-batches: {}", batches.len());

    let mut all_messages: Vec<Message> = Vec::new();
    for batch in &batches {
        let mut iter = memq_rust_client::MessageIterator::new(&batch.decompressed_payload, batch.header.log_message_count);
        while iter.has_next() {
            let msg = iter.next().expect("Failed to read message");
            all_messages.push(msg);
        }
    }

    // --- Step 5: Verify ---
    println!("\nStep 5: Verification...");
    println!("  Messages read: {}", all_messages.len());

    // Print all message keys/values
    for (i, msg) in all_messages.iter().enumerate() {
        let key = msg.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string());
        let value = msg.value.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
        println!("  Message {}: key={:?} value={:?}", i + 1, key, value);
    }

    if !all_messages.is_empty() {
        println!("\n  SUCCESS: Rust consumer read all {} messages from the real MemQ broker!", all_messages.len());
    } else {
        println!("\n  FAILED: No messages read");
        std::process::exit(1);
    }
}
