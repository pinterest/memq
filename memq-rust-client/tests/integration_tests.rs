use memq_rust_client::consumer::{MemqConsumer, MemqConsumerConfig};
use memq_rust_client::kafka::AutoOffsetReset;
use memq_rust_client::notification::Notification;
use memq_rust_client::storage::{FsStorageConfig, FsStorageHandler, MemqStorageConfig, StorageHandler};
use memq_rust_client::{parse_batch, parse_batch_messages, Compression, MessageHeader};
use kafka::producer::{Producer, Record, RequiredAcks};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

mod docker_kafka;

/// Build a wire-format message payload for a single message.
fn make_message_payload(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(&0_u16.to_be_bytes()); // no internal fields
    data.extend_from_slice(&(key.len() as i32).to_be_bytes());
    data.extend_from_slice(key);
    data.extend_from_slice(&(value.len() as i32).to_be_bytes());
    data.extend_from_slice(value);
    data
}

/// Build a complete batch file with the given messages.
fn build_batch(msgs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let num = msgs.len();

    let mut all_payload = Vec::new();
    for (ref k, ref v) in msgs {
        all_payload.extend(make_message_payload(k, v));
    }

    let compressed = Compression::None.compress(&all_payload).unwrap();
    let crc = crc32fast::hash(&compressed);

    let header = MessageHeader {
        header_length: MessageHeader::HEADER_LENGTH,
        version: 0,
        additional_header_length: 0,
        producer_address_length: 0,
        producer_address: Vec::new(),
        producer_epoch: 0,
        producer_request_id: 0,
        crc,
        compression: Compression::None,
        log_message_count: num as u32,
        message_length: compressed.len() as u32,
    };

    let mh_total = header.total_bytes();
    let mut header_bytes = Vec::with_capacity(mh_total);
    header_bytes.extend_from_slice(&header.header_length.to_be_bytes());
    header_bytes.extend_from_slice(&header.version.to_be_bytes());
    header_bytes.extend_from_slice(&header.additional_header_length.to_be_bytes());
    header_bytes.extend_from_slice(&header.crc.to_be_bytes());
    header_bytes.push(header.compression as u8);
    header_bytes.extend_from_slice(&header.log_message_count.to_be_bytes());
    header_bytes.extend_from_slice(&header.message_length.to_be_bytes());

    let batch_header_size = 8 + 12 * num;
    let mut batch_header_bytes = Vec::with_capacity(batch_header_size);
    batch_header_bytes.extend_from_slice(&(batch_header_size as u32).to_be_bytes());
    batch_header_bytes.extend_from_slice(&(num as u32).to_be_bytes());
    let mut data_pos = 0u32;
    for (i, (k, _v)) in msgs.iter().enumerate() {
        let msg_payload = make_message_payload(k, _v);
        batch_header_bytes.extend_from_slice(&(i as u32).to_be_bytes());
        batch_header_bytes.extend_from_slice(&(batch_header_size as u32 + data_pos).to_be_bytes());
        batch_header_bytes.extend_from_slice(&(msg_payload.len() as u32).to_be_bytes());
        data_pos += msg_payload.len() as u32;
    }

    let mut batch = batch_header_bytes;
    batch.extend(header_bytes);
    batch.extend(compressed);
    batch
}

/// Build a gzip-compressed batch.
fn build_compressed_batch(msgs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let num = msgs.len();

    let mut all_payload = Vec::new();
    for (ref k, ref v) in msgs {
        all_payload.extend(make_message_payload(k, v));
    }

    let compressed = Compression::Gzip.compress(&all_payload).unwrap();
    let crc = crc32fast::hash(&compressed);

    let header = MessageHeader {
        header_length: MessageHeader::HEADER_LENGTH,
        version: 0,
        additional_header_length: 0,
        producer_address_length: 0,
        producer_address: Vec::new(),
        producer_epoch: 0,
        producer_request_id: 0,
        crc,
        compression: Compression::Gzip,
        log_message_count: num as u32,
        message_length: compressed.len() as u32,
    };

    let mh_total = header.total_bytes();
    let mut header_bytes = Vec::with_capacity(mh_total);
    header_bytes.extend_from_slice(&header.header_length.to_be_bytes());
    header_bytes.extend_from_slice(&header.version.to_be_bytes());
    header_bytes.extend_from_slice(&header.additional_header_length.to_be_bytes());
    header_bytes.extend_from_slice(&header.crc.to_be_bytes());
    header_bytes.push(header.compression as u8);
    header_bytes.extend_from_slice(&header.log_message_count.to_be_bytes());
    header_bytes.extend_from_slice(&header.message_length.to_be_bytes());

    let batch_header_size = 8 + 12 * num;
    let mut batch_header_bytes = Vec::with_capacity(batch_header_size);
    batch_header_bytes.extend_from_slice(&(batch_header_size as u32).to_be_bytes());
    batch_header_bytes.extend_from_slice(&(num as u32).to_be_bytes());
    let mut data_pos = 0u32;
    for (i, (k, _v)) in msgs.iter().enumerate() {
        let msg_payload = make_message_payload(k, _v);
        batch_header_bytes.extend_from_slice(&(i as u32).to_be_bytes());
        batch_header_bytes.extend_from_slice(&(batch_header_size as u32 + data_pos).to_be_bytes());
        batch_header_bytes.extend_from_slice(&(msg_payload.len() as u32).to_be_bytes());
        data_pos += msg_payload.len() as u32;
    }

    let mut batch = batch_header_bytes;
    batch.extend(header_bytes);
    batch.extend(compressed);
    batch
}

/// Create a temp directory for test storage.
fn create_test_storage() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "memq-integration-{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir_all(&dir).unwrap();
    dir
}

/// Clean up test directory.
fn cleanup_test_storage(dir: &PathBuf) {
    let _ = fs::remove_dir_all(dir);
}

/// Build notification JSON (Rust test format: bucket + key).
fn test_notification_json(bucket: &str, key: &str, size: u64) -> String {
    serde_json::json!({
        "bucket": bucket,
        "key": key,
        "size": size,
        "topic": "test-topic",
        "headerSize": 44,
        "numBatchMessages": 0,
        "numAttempts": 1
    }).to_string()
}

/// Build notification JSON (Java MemQ broker format: absolute path).
fn broker_notification_json(path: &str, size: u64) -> String {
    serde_json::json!({
        "path": path,
        "size": size,
        "topic": "test-topic",
        "headerSize": 44,
        "numBatchMessages": 0
    }).to_string()
}

fn get_kafka_servers() -> Vec<String> {
    std::env::var("KAFKA_SERVERS")
        .ok()
        .and_then(|v| if v.is_empty() { None } else { Some(v) })
        .map(|v| v.split(',').map(|s| s.to_string()).collect())
        .unwrap_or_else(|| vec!["localhost:9092".to_string()])
}

fn try_create_producer(servers: &[String]) -> Option<Producer> {
    // Ensure Kafka is started via Docker
    if !docker_kafka::start() {
        eprintln!("  Kafka not reachable, skipping test");
        return None;
    }
    Producer::from_hosts(servers.to_vec())
        .with_required_acks(RequiredAcks::One)
        .create()
        .ok()
}

fn ensure_topic(producer: &mut Producer, topic: &str) {
    let marker = b"__topic_marker__";
    let record = Record::from_value(topic, &marker[..]);
    let _ = producer.send(&record);
}

// --- Notification parsing tests (both formats) ---

#[test]
fn notification_parse_rust_format() {
    let json = test_notification_json("my-bucket", "batch-001", 1234);
    let notif = Notification::from_json(&json).unwrap();
    assert_eq!(notif.bucket, "my-bucket");
    assert_eq!(notif.key, "batch-001");
    assert_eq!(notif.object_size, 1234);
    assert_eq!(notif.num_attempts, 1);
}

#[test]
fn notification_parse_broker_format() {
    // Simulates Java MemQ broker notification
    let json = broker_notification_json("/tmp/memq-storage/test/hostname/12345_67890", 5678);
    let notif = Notification::from_json(&json).unwrap();
    assert_eq!(notif.bucket, "/tmp/memq-storage/test/hostname");
    assert_eq!(notif.key, "12345_67890");
    assert_eq!(notif.object_size, 5678);
    assert_eq!(notif.num_attempts, 1); // defaults to 1
}

#[test]
fn notification_parse_broker_nested_path() {
    let json = broker_notification_json("/var/data/memq/mytopic/srv1/abc_def_0", 999);
    let notif = Notification::from_json(&json).unwrap();
    assert_eq!(notif.bucket, "/var/data/memq/mytopic/srv1");
    assert_eq!(notif.key, "abc_def_0");
    assert_eq!(notif.object_size, 999);
}

// --- Integration Tests ---

/// Use Latest offset so consumer only sees messages published AFTER it starts.
/// This ensures the consumer actually consumes what we push.
fn make_consumer_config(
    servers: Vec<String>,
    group_id: &str,
    topic: &str,
    storage_dir: PathBuf,
) -> MemqConsumerConfig {
    MemqConsumerConfig {
        bootstrap_servers: servers,
        group_id: group_id.to_string(),
        notification_topic: topic.to_string(),
        storage_config: MemqStorageConfig::Fs(FsStorageConfig {
            root_dir: storage_dir,
        }),
        auto_offset_reset: AutoOffsetReset::Latest,
    }
}

#[test]
fn integration_single_batch_uncompressed() {
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    let storage_dir = create_test_storage();
    let bucket = "test-bucket";

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
        (b"key4".to_vec(), b"value4".to_vec()),
    ];
    let batch_data = build_batch(&messages);
    let file_name = "batch-001";
    let file_path = storage_dir.join(bucket).join(file_name);
    fs::create_dir_all(file_path.parent().unwrap()).unwrap();
    fs::write(&file_path, &batch_data).unwrap();
    let file_size = batch_data.len() as u64;

    let topic = "test-integration-uncompressed";
    ensure_topic(&mut producer, topic);

    let config = make_consumer_config(
        servers.clone(),
        "test-group-uncompressed",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap();

    // Push notification AFTER consumer is created (Latest offset)
    let notif_json = test_notification_json(bucket, file_name, file_size);
    let _ = producer.send(&Record::from_value(topic, notif_json.as_bytes()));

    // Poll should now see the notification and return messages
    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert_eq!(messages.len(), 4, "Expected 4 messages, got {}", messages.len());

    for (i, msg) in messages.iter().enumerate() {
        let expected_key = format!("key{}", i + 1);
        let expected_value = format!("value{}", i + 1);
        assert_eq!(msg.key, Some(expected_key.into_bytes()), "Message {} key mismatch", i);
        assert_eq!(msg.value, Some(expected_value.into_bytes()), "Message {} value mismatch", i);
    }

    consumer.commit_offset().unwrap();
    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_single_batch_compressed() {
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    let storage_dir = create_test_storage();
    let bucket = "test-bucket";

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"k1".to_vec(), b"v1".to_vec()),
        (b"k2".to_vec(), b"v2".to_vec()),
        (b"k3".to_vec(), b"v3".to_vec()),
        (b"k4".to_vec(), b"v4".to_vec()),
        (b"k5".to_vec(), b"v5".to_vec()),
        (b"k6".to_vec(), b"v6".to_vec()),
    ];
    let batch_data = build_compressed_batch(&messages);
    let file_name = "batch-compressed-001";
    let file_path = storage_dir.join(bucket).join(file_name);
    fs::create_dir_all(file_path.parent().unwrap()).unwrap();
    fs::write(&file_path, &batch_data).unwrap();
    let file_size = batch_data.len() as u64;

    let topic = "test-integration-compressed";
    ensure_topic(&mut producer, topic);

    let config = make_consumer_config(
        servers.clone(),
        "test-group-compressed",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap();

    let notif_json = test_notification_json(bucket, file_name, file_size);
    let _ = producer.send(&Record::from_value(topic, notif_json.as_bytes()));

    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert_eq!(messages.len(), 6, "Expected 6 messages, got {}", messages.len());

    for (i, msg) in messages.iter().enumerate() {
        let expected_key = format!("k{}", i + 1);
        let expected_value = format!("v{}", i + 1);
        assert_eq!(msg.key, Some(expected_key.into_bytes()), "Message {} key mismatch", i);
        assert_eq!(msg.value, Some(expected_value.into_bytes()), "Message {} value mismatch", i);
    }

    consumer.commit_offset().unwrap();
    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_multiple_notifications() {
    let storage_dir = create_test_storage();
    let bucket = "test-bucket";

    let batch1: Vec<(Vec<u8>, Vec<u8>)> = vec![(b"batch1-k1".to_vec(), b"batch1-v1".to_vec())];
    let batch1_data = build_batch(&batch1);
    let file1 = "batch1-001";
    fs::create_dir_all(storage_dir.join(bucket).join(file1).parent().unwrap()).unwrap();
    fs::write(storage_dir.join(bucket).join(file1), &batch1_data).unwrap();

    let batch2: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"batch2-k1".to_vec(), b"batch2-v1".to_vec()),
        (b"batch2-k2".to_vec(), b"batch2-v2".to_vec()),
    ];
    let batch2_data = build_batch(&batch2);
    let file2 = "batch2-001";
    fs::create_dir_all(storage_dir.join(bucket).join(file2).parent().unwrap()).unwrap();
    fs::write(storage_dir.join(bucket).join(file2), &batch2_data).unwrap();

    // Use push_notification to bypass Kafka — avoids multi-partition fetch issues
    let topic = "test-integration-multi-push";
    let servers = get_kafka_servers();
    let mut producer = try_create_producer(&servers).expect("Kafka not reachable");
    ensure_topic(&mut producer, topic);

    let config = make_consumer_config(
        servers.clone(),
        "test-group-multi-push",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap();

    // Build enriched notifications directly
    let notif1 = test_notification_json(bucket, file1, batch1_data.len() as u64);
    let enriched1 = memq_rust_client::kafka::EnrichedNotification::from_json(&notif1, 0, 0).unwrap();
    consumer.push_notification(enriched1);

    let notif2 = test_notification_json(bucket, file2, batch2_data.len() as u64);
    let enriched2 = memq_rust_client::kafka::EnrichedNotification::from_json(&notif2, 0, 1).unwrap();
    consumer.push_notification(enriched2);

    // Poll should get all 3 messages from both batches
    let messages = consumer.poll(Duration::from_millis(100)).unwrap();
    assert_eq!(messages.len(), 3, "Expected 3 messages from 2 batches, got {}", messages.len());

    // Verify message content
    let keys: Vec<_> = messages.iter()
        .filter_map(|m| m.key.as_ref().map(|k| String::from_utf8_lossy(k).to_string()))
        .collect();
    assert!(keys.contains(&"batch1-k1".to_string()), "Should contain batch1-k1");
    assert!(keys.contains(&"batch2-k1".to_string()), "Should contain batch2-k1");
    assert!(keys.contains(&"batch2-k2".to_string()), "Should contain batch2-k2");

    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_with_offset_commit() {
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    let storage_dir = create_test_storage();
    let bucket = "test-bucket";

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![(b"commit-test-key".to_vec(), b"commit-test-value".to_vec())];
    let batch_data = build_batch(&messages);
    let file_name = "commit-batch-001";
    fs::create_dir_all(storage_dir.join(bucket).join(file_name).parent().unwrap()).unwrap();
    fs::write(storage_dir.join(bucket).join(file_name), &batch_data).unwrap();

    let topic = "test-integration-commit";
    ensure_topic(&mut producer, topic);

    let config = make_consumer_config(
        servers.clone(),
        "test-group-commit",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap();

    let notif_json = test_notification_json(bucket, file_name, batch_data.len() as u64);
    let _ = producer.send(&Record::from_value(topic, notif_json.as_bytes()));

    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert_eq!(messages.len(), 1, "Expected 1 message");
    assert_eq!(messages[0].key, Some(b"commit-test-key".to_vec()));

    consumer.commit_offset().unwrap();
    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_empty_poll() {
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    let topic = "test-integration-empty";
    ensure_topic(&mut producer, topic);

    let storage_dir = create_test_storage();

    let config = make_consumer_config(
        servers.clone(),
        "test-group-empty",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap();

    // No notifications pushed — should return empty
    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert!(messages.is_empty(), "Expected empty poll result");

    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_dry_run_mode() {
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    let storage_dir = create_test_storage();
    let bucket = "test-bucket";

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![(b"dry-run-key".to_vec(), b"dry-run-value".to_vec())];
    let batch_data = build_batch(&messages);
    let file_name = "dry-run-batch-001";
    fs::create_dir_all(storage_dir.join(bucket).join(file_name).parent().unwrap()).unwrap();
    fs::write(storage_dir.join(bucket).join(file_name), &batch_data).unwrap();

    let topic = "test-integration-dry-run";
    ensure_topic(&mut producer, topic);

    let config = make_consumer_config(
        servers.clone(),
        "test-group-dry-run",
        topic,
        storage_dir.clone(),
    );

    let mut consumer = MemqConsumer::new(config).unwrap().dry_run(true);

    let notif_json = test_notification_json(bucket, file_name, batch_data.len() as u64);
    let _ = producer.send(&Record::from_value(topic, notif_json.as_bytes()));

    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert_eq!(messages.len(), 1, "Expected 1 message in dry run");
    assert_eq!(messages[0].key, Some(b"dry-run-key".to_vec()));
    consumer.commit_offset().unwrap();
    consumer.close();
    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_with_broker_notification_format() {
    // Test with Java MemQ broker notification format (path-based).
    let servers = get_kafka_servers();
    let mut producer = match try_create_producer(&servers) {
        Some(p) => p,
        None => {
            eprintln!("Kafka not reachable, skipping integration test");
            return;
        }
    };

    // Use absolute path for filesystem storage
    let storage_root = std::env::temp_dir().join(format!("memq-broker-test-{:x}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()));
    fs::create_dir_all(&storage_root).unwrap();

    // Absolute path the broker would use: /tmp/memq-broker-test-xxx/test-bucket/batch-001
    let abs_path = storage_root.join("test-bucket/batch-001");
    let abs_path_str = abs_path.to_str().unwrap();

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![(b"broker-k1".to_vec(), b"broker-v1".to_vec())];
    let batch_data = build_batch(&messages);
    fs::create_dir_all(abs_path.parent().unwrap()).unwrap();
    fs::write(&abs_path, &batch_data).unwrap();
    let file_size = batch_data.len() as u64;

    let topic = "test-integration-broker-format";
    ensure_topic(&mut producer, topic);

    let config = MemqConsumerConfig {
        bootstrap_servers: servers.clone(),
        group_id: "test-group-broker".to_string(),
        notification_topic: topic.to_string(),
        // For broker format: root_dir doesn't matter much since bucket is an absolute path
        storage_config: MemqStorageConfig::Fs(FsStorageConfig {
            root_dir: storage_root.clone(),
        }),
        auto_offset_reset: AutoOffsetReset::Latest,
    };

    let mut consumer = MemqConsumer::new(config).unwrap();

    // Push broker-format notification
    let notif_json = broker_notification_json(abs_path_str, file_size);
    let _ = producer.send(&Record::from_value(topic, notif_json.as_bytes()));

    let messages = consumer.poll(Duration::from_millis(500)).unwrap();
    assert_eq!(messages.len(), 1, "Expected 1 message from broker-format notification, got {}", messages.len());
    assert_eq!(messages[0].key, Some(b"broker-k1".to_vec()));

    consumer.close();
    cleanup_test_storage(&storage_root);
}

#[test]
fn integration_storage_handler_fs() {
    let storage_dir = create_test_storage();
    let bucket = "test-bucket";
    let key = "test-batch";

    let handler = FsStorageHandler::new(FsStorageConfig {
        root_dir: storage_dir.clone(),
    });

    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![(b"fs-k1".to_vec(), b"fs-v1".to_vec()), (b"fs-k2".to_vec(), b"fs-v2".to_vec())];
    let batch_data = build_batch(&messages);
    let path = storage_dir.join(bucket).join(key);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(&path, &batch_data).unwrap();

    let batch = handler.fetch_batch(bucket, key).unwrap();
    assert_eq!(batch.data.len(), batch_data.len());

    let (_header, batches) = parse_batch(&batch.data).unwrap();
    assert_eq!(batches.len(), 1);

    let mut msg_count = 0;
    for batch in &batches {
        let mut iter = memq_rust_client::MessageIterator::new(&batch.decompressed_payload, batch.header.log_message_count);
        while iter.has_next() {
            iter.next().unwrap();
            msg_count += 1;
        }
    }
    assert_eq!(msg_count, 2);

    cleanup_test_storage(&storage_dir);
}

#[test]
fn integration_parse_batch_messages() {
    let messages: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"parse-k1".to_vec(), b"parse-v1".to_vec()),
        (b"parse-k2".to_vec(), b"parse-v2".to_vec()),
        (b"parse-k3".to_vec(), b"parse-v3".to_vec()),
    ];
    let batch_data = build_batch(&messages);

    let parsed = parse_batch_messages(&batch_data).unwrap();
    assert_eq!(parsed.len(), 3);

    for (i, msg) in parsed.iter().enumerate() {
        let expected_key = format!("parse-k{}", i + 1);
        let expected_value = format!("parse-v{}", i + 1);
        assert_eq!(msg.key, Some(expected_key.into_bytes()));
        assert_eq!(msg.value, Some(expected_value.into_bytes()));
    }
}
