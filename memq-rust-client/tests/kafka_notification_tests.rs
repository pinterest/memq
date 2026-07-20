use memq_rust_client::kafka::EnrichedNotification;

#[test]
fn enriched_notification_from_valid_json() {
    let json = r#"{
        "bucket": "my-bucket",
        "key": "path/to/batch",
        "size": 1024,
        "topic": "my-topic",
        "headerSize": 120,
        "numBatchMessages": 50,
        "numAttempts": 1
    }"#;

    let enriched = EnrichedNotification::from_json(json, 0, 42).unwrap();
    assert_eq!(enriched.notification.bucket, "my-bucket");
    assert_eq!(enriched.notification.key, "path/to/batch");
    assert_eq!(enriched.notification.object_size, 1024);
    assert_eq!(enriched.notification.topic, "my-topic");
    assert_eq!(enriched.notification.header_size, 120);
    assert_eq!(enriched.notification.num_batch_messages, 50);
    assert_eq!(enriched.notification.num_attempts, 1);
    assert_eq!(enriched.notification_partition_id, 0);
    assert_eq!(enriched.notification_partition_offset, 42);
    assert!(enriched.notification_read_timestamp > 0);
}

#[test]
fn enriched_notification_different_partitions() {
    let json = r#"{
        "bucket": "bucket",
        "key": "key",
        "size": 100,
        "topic": "t",
        "headerSize": 10,
        "numBatchMessages": 1,
        "numAttempts": 1
    }"#;

    let e1 = EnrichedNotification::from_json(json, 2, 100).unwrap();
    let e2 = EnrichedNotification::from_json(json, 3, 200).unwrap();
    assert_eq!(e1.notification_partition_id, 2);
    assert_eq!(e2.notification_partition_id, 3);
    assert_eq!(e1.notification_partition_offset, 100);
    assert_eq!(e2.notification_partition_offset, 200);
}

#[test]
fn enriched_notification_missing_field() {
    let json = r#"{ "bucket": "b" }"#;
    let result = EnrichedNotification::from_json(json, 0, 0);
    assert!(result.is_err());
}

#[test]
fn enriched_notification_invalid_json() {
    let result = EnrichedNotification::from_json("not json", 0, 0);
    assert!(result.is_err());
}
