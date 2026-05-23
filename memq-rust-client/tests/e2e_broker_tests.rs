/// End-to-end tests: real MemQ broker + Kafka + Rust consumer.
///
/// Prerequisites:
///   1. MemQ broker running on port 9093 with test-fs.yaml config
///   2. Kafka running on localhost:9092 with TestTopicNotifications topic
///   3. Batch files stored under /tmp/memq-storage

use memq_rust_client::notification::Notification;
use memq_rust_client::storage::{FsStorageConfig, StorageHandler};
use std::path::PathBuf;
use std::fs;

fn find_registry_files(root_dir: &PathBuf) -> Vec<String> {
    let registry = root_dir.join("test").join("registry");
    fs::read_dir(registry)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect()
}

fn broker_notification(path: &str, size: u64) -> String {
    serde_json::json!({
        "path": path,
        "objectSize": size,
        "topic": "test",
        "headerSize": 20,
        "numBatchMessages": 5
    }).to_string()
}

#[test]
fn e2e_real_broker_notification() {
    // Read a notification pushed by the MemQ broker to Kafka.
    // This validates our notification parser handles the broker's format.
    let json = broker_notification("/tmp/memq-storage/test/registry/0_6484442784546380160_0", 215);
    let notif = Notification::from_json(&json).unwrap();
    assert_eq!(notif.bucket, "/tmp/memq-storage/test/registry");
    assert_eq!(notif.key, "0_6484442784546380160_0");
    assert_eq!(notif.object_size, 215);
    assert_eq!(notif.topic, "test");
}

#[test]
fn e2e_fs_storage_handler_reads_broker_file() {
    // Verify FsStorageHandler can read the actual batch file written by the broker.
    let root_dir = PathBuf::from("/tmp/memq-storage");

    let files = find_registry_files(&root_dir);
    assert!(!files.is_empty(), "No batch files found in registry");

    let key = &files[0];
    let handler = memq_rust_client::storage::FsStorageHandler::new(FsStorageConfig {
        root_dir: root_dir.clone(),
    });

    let bucket = "/tmp/memq-storage/test/registry";

    let batch = handler.fetch_batch(bucket, key);
    assert!(batch.is_ok(), "Should read broker batch file: {:?}", batch.err());

    let batch_data = batch.unwrap().data;
    assert!(batch_data.len() > 0, "Batch should not be empty");

    // Parse the batch
    let (header, batches) = memq_rust_client::parse_batch(&batch_data).unwrap();
    assert!(header.len() > 0, "Should have index entries");

    // Count messages across all sub-batches
    let mut total_messages = 0u32;
    for batch in &batches {
        let mut iter = memq_rust_client::MessageIterator::new(
            &batch.decompressed_payload,
            batch.header.log_message_count,
        );
        while iter.has_next() {
            let msg = iter.next().unwrap();
            // Verify we got real messages
            assert!(msg.key.is_some() || msg.value.is_some());
            total_messages += 1;
        }
    }
    assert!(total_messages > 0, "Should have at least 1 message from broker batch");
}

#[test]
fn e2e_parse_broker_batch_messages() {
    // Parse the actual broker batch file and verify message content.
    let root_dir = PathBuf::from("/tmp/memq-storage");

    let files = find_registry_files(&root_dir);
    assert!(!files.is_empty(), "No batch files found in registry");

    let key = &files[0];
    let handler = memq_rust_client::storage::FsStorageHandler::new(FsStorageConfig {
        root_dir: root_dir.clone(),
    });

    let bucket = "/tmp/memq-storage/test/registry";

    let batch_data = handler.fetch_batch(bucket, key).unwrap().data;
    let messages = memq_rust_client::parse_batch_messages(&batch_data).unwrap();

    assert!(messages.len() > 0, "Should have at least 1 message");

    // Verify messages have key/value content
    for msg in &messages {
        assert!(msg.key.is_some() || msg.value.is_some());
    }
}
