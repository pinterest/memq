use memq_rust_client::{Message, MessageIterator, parse_batch_messages};

/// Build a wire-format message payload for testing.
fn make_message_payload(
    has_internal_fields: bool,
    key: &[u8],
    value: &[u8],
) -> Vec<u8> {
    let mut data = Vec::new();

    // internalFieldsLength
    if has_internal_fields {
        // writeTimestamp (8) + messageIdLength (1) + headersLength (2) = 11
        let internal_fields_length: u16 = 8 + 1 + 2;
        data.extend_from_slice(&internal_fields_length.to_be_bytes());
        // writeTimestamp
        data.extend_from_slice(&1234567890_i64.to_be_bytes());
        // messageIdLength = 0
        data.push(0);
        // headersLength = 0
        data.extend_from_slice(&0_u16.to_be_bytes());
    } else {
        data.extend_from_slice(&0_u16.to_be_bytes());
    }

    // key
    data.extend_from_slice(&(key.len() as i32).to_be_bytes());
    data.extend_from_slice(key);

    // value
    data.extend_from_slice(&(value.len() as i32).to_be_bytes());
    data.extend_from_slice(value);

    data
}

/// Build a message with timestamp, message_id, and headers.
fn make_message_with_metadata(
    timestamp: i64,
    message_id: &[u8],
    key: &[u8],
    value: &[u8],
    headers: &[(&str, &[u8])],
) -> Vec<u8> {
    let mut data = Vec::new();

    let msg_id_len = message_id.len() as u8;
    let mut headers_len = 0usize;
    for (hk, hv) in headers {
        headers_len += 4 + hk.len() + hv.len();
    }
    let internal_fields_length = 8 + 1 + msg_id_len as usize + 2 + headers_len;

    // internalFieldsLength
    data.extend_from_slice(&(internal_fields_length as u16).to_be_bytes());

    // writeTimestamp
    data.extend_from_slice(&timestamp.to_be_bytes());

    // messageIdLength + messageId
    data.push(msg_id_len);
    data.extend_from_slice(message_id);

    // headersLength + header entries
    data.extend_from_slice(&(headers_len as u16).to_be_bytes());
    for (hk, hv) in headers {
        data.extend_from_slice(&(hk.len() as u16).to_be_bytes());
        data.extend_from_slice(hk.as_bytes());
        data.extend_from_slice(&(hv.len() as u16).to_be_bytes());
        data.extend_from_slice(hv);
    }

    // key
    data.extend_from_slice(&(key.len() as i32).to_be_bytes());
    data.extend_from_slice(key);

    // value
    data.extend_from_slice(&(value.len() as i32).to_be_bytes());
    data.extend_from_slice(value);

    data
}

// --- MessageIterator tests (same logic as NotificationMessageIterator) ---

#[test]
fn message_iterator_single_message() {
    let payload = make_message_payload(false, b"key1", b"value1");
    let mut iter = MessageIterator::new(&payload, 1);
    assert!(iter.has_next());
    let msg = iter.next().unwrap();
    assert_eq!(msg.key, Some(b"key1".to_vec()));
    assert_eq!(msg.value, Some(b"value1".to_vec()));
}

#[test]
fn message_iterator_no_more_messages() {
    let payload = make_message_payload(false, b"key", b"value");
    let mut iter = MessageIterator::new(&payload, 1);
    let _ = iter.next().unwrap();
    assert!(!iter.has_next());
    assert!(iter.next().is_err());
}

#[test]
fn message_iterator_with_timestamp() {
    let payload = make_message_payload(true, b"key", b"value");
    let mut iter = MessageIterator::new(&payload, 1);
    let msg = iter.next().unwrap();
    assert_eq!(msg.write_timestamp, Some(1234567890));
}

#[test]
fn message_iterator_multiple_messages() {
    let mut payload = Vec::new();
    payload.extend(make_message_payload(false, b"k1", b"v1"));
    payload.extend(make_message_payload(false, b"k2", b"v2"));
    payload.extend(make_message_payload(false, b"k3", b"v3"));
    let mut iter = MessageIterator::new(&payload, 3);

    let m1 = iter.next().unwrap();
    assert_eq!(m1.key, Some(b"k1".to_vec()));
    let m2 = iter.next().unwrap();
    assert_eq!(m2.key, Some(b"k2".to_vec()));
    let m3 = iter.next().unwrap();
    assert_eq!(m3.key, Some(b"k3".to_vec()));
    assert!(!iter.has_next());
}

#[test]
fn message_iterator_with_message_id_and_headers() {
    let payload = make_message_with_metadata(
        999,
        b"mid-123",
        b"my-key",
        b"my-value",
        &[("header-key", b"header-val"), ("content-type", b"application/json")],
    );
    let mut iter = MessageIterator::new(&payload, 1);
    let msg = iter.next().unwrap();
    assert_eq!(msg.write_timestamp, Some(999));
    assert_eq!(msg.message_id, Some(b"mid-123".to_vec()));
    assert_eq!(msg.headers.len(), 2);
}

#[test]
fn message_iterator_null_key() {
    // key_length=0 means null key (no key bytes follow)
    let mut data = Vec::new();
    data.extend_from_slice(&0_u16.to_be_bytes());
    data.extend_from_slice(&(0_i32).to_be_bytes()); // null key (length 0)
    data.extend_from_slice(&(5_i32).to_be_bytes());
    data.extend_from_slice(b"hello");

    let mut iter = MessageIterator::new(&data, 1);
    let msg = iter.next().unwrap();
    assert_eq!(msg.key, None);
    assert_eq!(msg.value, Some(b"hello".to_vec()));
}

#[test]
fn message_iterator_null_value() {
    // value_length=0 means null value (no value bytes follow)
    let mut data = Vec::new();
    data.extend_from_slice(&0_u16.to_be_bytes());
    data.extend_from_slice(&(3_i32).to_be_bytes());
    data.extend_from_slice(b"key");
    data.extend_from_slice(&(0_i32).to_be_bytes()); // null value (length 0)

    let mut iter = MessageIterator::new(&data, 1);
    let msg = iter.next().unwrap();
    assert_eq!(msg.key, Some(b"key".to_vec()));
    assert_eq!(msg.value, None);
}

#[test]
fn message_iterator_empty_payload() {
    let mut iter = MessageIterator::new(&[], 1);
    assert!(!iter.has_next());
    assert!(iter.next().is_err());
}

#[test]
fn message_iterator_zero_count() {
    let mut iter = MessageIterator::new(&[1, 2, 3], 0);
    assert!(!iter.has_next());
}

// --- Integration test: parse batch messages ---

#[test]
fn parse_batch_messages_empty() {
    // Minimal batch header (0 entries) + no messages
    let batch: Vec<u8> = vec![0, 0]; // entries_len = 0
    let result = parse_batch_messages(&batch);
    // Should return empty or error (no entries)
    if let Ok(msgs) = result {
        assert!(msgs.is_empty());
    }
}

#[test]
fn message_iterator_truncated_internal_fields() {
    // Truncated: internalFieldsLength says 2 bytes but only 1 byte for writeTimestamp
    let mut data = Vec::new();
    data.extend_from_slice(&3_u16.to_be_bytes()); // internalFieldsLength=3
    data.push(1); // only 1 byte of 8-byte writeTimestamp
    // Then keyLength + value - will likely fail or read garbage
    data.extend_from_slice(&(3_i32).to_be_bytes());
    data.extend_from_slice(b"key1");
    data.extend_from_slice(&(5_i32).to_be_bytes());
    data.extend_from_slice(b"value");

    let mut iter = MessageIterator::new(&data, 1);
    // Should error due to truncated internal fields
    assert!(iter.next().is_err());
}
