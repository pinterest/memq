use memq_rust_client::{
    parse_batch, parse_batch_messages, BatchHeader, Compression, Message, MessageHeader, MessageIterator, Notification,
};
use memq_rust_client::error::MemqError;
use memq_rust_client::batch_header::IndexEntry;

// Base64-encoded batch bytes from the Java producer (reused from Python tests).
const UNCOMPRESSED_BATCH_B64: &str =
    "AAAAHAAAAAIAAAAAAAAAIAAAAJIAAAABAAAAsgAAAJIAKABkABUErBAAJQAAAXdqDH1EAAAAAAAAAAGRLVIQAAAAAAIAAABqACAAAAF3agx9QwgAAAAAAAAAAAANAAR0ZXN0AAV2YWx1ZQAAAAAAAAALdGVzdDEyMzEyMzEAIAAAAXdqDH1DCAAAAAAAAAABAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAoAGQAFQSsEAAlAAABd2oMfUUAAAAAAAAAAf6kvmoAAAAAAgAAAGoAIAAAAXdqDH1ECAAAAAAAAAAAAA0ABHRlc3QABXZhbHVlAAAAAAAAAAt0ZXN0MTIzMTIzMQAgAAABd2oMfUUIAAAAAAAAAAEADQAEdGVzdAAFdmFsdWUAAAAAAAAAC3Rlc3QxMjMxMjMx";

const GZIP_BATCH_B64: &str =
    "AAAAKAAAAAMAAAAAAAAALAAAAHYAAAABAAAAogAAAHMAAAACAAABFQAAAHMAKABkABUErBAAJQAAAXdqfK1xAAAAAAAAAAFCystcAQAAAAIAAABOH4sIAAAAAAAAAGJQYGBgLM+qWVvAwQADvAwsJanFJQysZYk5palQQW6QkKGRMQgBAAAA//9igGkqhGliJKwJAAAA//8DAOE0yN1qAAAAACgAZAAVBKwQACUAAAF3anytcgAAAAAAAAABz+4rrQEAAAACAAAASx+LCAAAAAAAAABiUGBgYCzPqllbxMEAA7wMLCWpxSUMrGWJOaWpUEFukJChkTEIAQAAAP//YsDQxEhYEwAAAP//AwBlNttIagAAAAAoAGQAFQSsEAAlAAABd2p8rXMAAAAAAAAAAbP5aWQBAAAAAgAAAEsfiwgAAAAAAAAAYlBgYGAsz6pZW8zBAAO8DCwlqcUlDKxliTmlqVBBbpCQoZExCAEAAAD//2LA0MRIWBMAAAD//wMAFqtGF2oAAAA=";

const ZSTD_BATCH_B64: &str =
    "AAAAQAAAAAUAAAAAAAAARAAAAHMAAAABAAAAtwAAAHAAAAACAAABJwAAAHAAAAADAAABlwAAAHAAAAAEAAACBwAAAHAAKABkABUErBAAJQAAAXdqfduyAAAAAAAAAAFUPvwXAgAAAAIAAABLKLUv/QBYhAEAdAIAIAAAAXdqfduwCAANAAR0ZXN0AAV2YWx1ZQALdGVzdDEyMzEyMzECAGBUAtABZAAAELIBAwDACSCHCxUEAQAAACgAZAAVBKwQACUAAAF3an3bswAAAAAAAAABCOO92wIAAAACAAAASCi1L/0AWIQBAHQCACAAAAF3an3bswgADQAEdGVzdAAFdmFsdWUAC3Rlc3QxMjMxMjMxAgBgVALQAUwAAAgBAgDAEewpIAEAAAAoAGQAFQSsEAAlAAABd2p927QAAAAAAAAAAc70T7kCAAAAAgAAAEgotS/9AFiEAQB0AgAgAAABd2p927QIAA0ABHRlc3QABXZhbHVlAAt0ZXN0MTIzMTIzMQIAYFQC0AFMAAAIAQIAwBHsKSABAAAAKABkABUErBAAJQAAAXdqfdu2AAAAAAAAAAEdbbejAgAAAAIAAABIKLUv/QBYhAEAdAIAIAAAAXdqfdu2CAANAAR0ZXN0AAV2YWx1ZQALdGVzdDEyMzEyMzECAGBUAtABTAAACAECAMAR7CkgAQAAACgAZAAVBKwQACUAAAF3an3btwAAAAAAAAABdKFLrgIAAAACAAAASCi1L/0AWIQBAHQCACAAAAF3an3btwgADQAEdGVzdAAFdmFsdWUAC3Rlc3QxMjMxMjMxAgBgVALQAUwAAAgBAgDAEewpIAEAAA==";

fn decode_b64(s: &str) -> Vec<u8> {
    base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s).expect("valid base64")
}

// ---------------------------------------------------------------------------
// Real batch parsing (reused from Python tests)
// ---------------------------------------------------------------------------

#[test]
fn parse_uncompressed_batch() {
    let raw = decode_b64(UNCOMPRESSED_BATCH_B64);
    let messages = parse_batch_messages(&raw).expect("parse uncompressed batch");
    assert_eq!(messages.len(), 4, "should have 4 messages");

    for msg in &messages {
        assert_eq!(msg.value, Some(b"test1231231".to_vec()));
    }
}

#[test]
fn parse_gzip_batch() {
    let raw = decode_b64(GZIP_BATCH_B64);
    let messages = parse_batch_messages(&raw).expect("parse gzip batch");
    assert_eq!(messages.len(), 6, "should have 6 messages");

    for msg in &messages {
        assert_eq!(msg.value, Some(b"test1231231".to_vec()));
    }
}

#[test]
fn parse_zstd_batch() {
    let raw = decode_b64(ZSTD_BATCH_B64);
    let messages = parse_batch_messages(&raw).expect("parse zstd batch");
    assert_eq!(messages.len(), 10, "should have 10 messages");

    for msg in &messages {
        assert_eq!(msg.value, Some(b"test1231231".to_vec()));
    }
}

#[test]
fn parse_batch_returns_header_and_message_batches() {
    let raw = decode_b64(UNCOMPRESSED_BATCH_B64);
    let (batch_header, batches) = parse_batch(&raw).expect("parse batch");

    assert_eq!(batch_header.len(), 2); // 2 sub-batches
    assert_eq!(batch_header.get(0).unwrap().offset, 32);
    assert_eq!(batches.len(), 2); // 2 MessageHeader+Payload pairs
    assert_eq!(batches[0].header.compression, Compression::None);
    assert_eq!(batches[0].header.log_message_count, 2);
    assert!(batches[0].header.has_additional_header());
}

// ---------------------------------------------------------------------------
// BatchHeader roundtrip
// ---------------------------------------------------------------------------

#[test]
fn batch_header_roundtrip_single_entry() {
    let entries = vec![
        (0u32, IndexEntry { offset: 0, size: 50 }),
        (1u32, IndexEntry { offset: 50, size: 30 }),
        (2u32, IndexEntry { offset: 80, size: 20 }),
    ];
    let header = BatchHeader {
        header_length: 44,
        entries,
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.header_length.to_be_bytes());
    buf.extend_from_slice(&(header.entries.len() as u32).to_be_bytes());
    for (idx, entry) in &header.entries {
        buf.extend_from_slice(&idx.to_be_bytes());
        buf.extend_from_slice(&entry.offset.to_be_bytes());
        buf.extend_from_slice(&entry.size.to_be_bytes());
    }

    let parsed = BatchHeader::from_bytes(&buf).expect("parse roundtrip");
    assert_eq!(parsed, header);
}

#[test]
fn batch_header_roundtrip_empty() {
    let header = BatchHeader {
        header_length: 12,
        entries: vec![],
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.header_length.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());

    let parsed = BatchHeader::from_bytes(&buf).expect("parse empty");
    assert_eq!(parsed, header);
    assert!(parsed.is_empty());
}

#[test]
fn batch_header_empty_batch() {
    let raw = decode_b64(UNCOMPRESSED_BATCH_B64);
    let (batch_header, _) = parse_batch(&raw).expect("parse");
    assert!(!batch_header.is_empty());
    assert_eq!(
        batch_header.get(0),
        Some(&IndexEntry { offset: 32, size: 146 })
    );
}

#[test]
fn batch_header_too_small() {
    let result = BatchHeader::from_bytes(&[1, 2]);
    assert!(result.is_err());
}

#[test]
fn batch_header_truncated_entry() {
    let raw: Vec<u8> = vec![0, 0, 0, 12, 0, 0, 0, 1, 0, 0, 0, 0];
    let result = BatchHeader::from_bytes(&raw);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// MessageHeader roundtrip
// ---------------------------------------------------------------------------

#[test]
fn message_header_roundtrip_with_additional() {
    let header = MessageHeader {
        header_length: MessageHeader::HEADER_LENGTH,
        version: 100,
        additional_header_length: 26,
        producer_address_length: 4,
        producer_address: vec![127, 0, 0, 1],
        producer_epoch: 12345,
        producer_request_id: 67890,
        crc: 0x12345678,
        compression: Compression::Zstd,
        log_message_count: 10,
        message_length: 512,
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.header_length.to_be_bytes());
    buf.extend_from_slice(&header.version.to_be_bytes());
    buf.extend_from_slice(&header.additional_header_length.to_be_bytes());
    buf.push(header.producer_address_length);
    buf.extend_from_slice(&header.producer_address);
    buf.extend_from_slice(&header.producer_epoch.to_be_bytes());
    buf.extend_from_slice(&header.producer_request_id.to_be_bytes());
    buf.extend_from_slice(&header.crc.to_be_bytes());
    buf.push(header.compression as u8);
    buf.extend_from_slice(&header.log_message_count.to_be_bytes());
    buf.extend_from_slice(&header.message_length.to_be_bytes());

    let parsed = MessageHeader::from_bytes(&buf).expect("parse roundtrip");
    assert_eq!(parsed, header);
}

#[test]
fn message_header_roundtrip_without_additional() {
    let header = MessageHeader {
        header_length: MessageHeader::HEADER_LENGTH,
        version: 100,
        additional_header_length: 0,
        producer_address_length: 0,
        producer_address: vec![],
        producer_epoch: 0,
        producer_request_id: 0,
        crc: 0xABCDEF00,
        compression: Compression::None,
        log_message_count: 3,
        message_length: 100,
    };

    let mut buf = Vec::new();
    buf.extend_from_slice(&header.header_length.to_be_bytes());
    buf.extend_from_slice(&header.version.to_be_bytes());
    buf.extend_from_slice(&header.additional_header_length.to_be_bytes());
    buf.extend_from_slice(&header.crc.to_be_bytes());
    buf.push(header.compression as u8);
    buf.extend_from_slice(&header.log_message_count.to_be_bytes());
    buf.extend_from_slice(&header.message_length.to_be_bytes());

    let parsed = MessageHeader::from_bytes(&buf).expect("parse roundtrip");
    assert_eq!(parsed, header);
}

#[test]
fn message_header_unknown_compression() {
    let mut buf: Vec<u8> = vec![0, 27, 0, 100, 0, 0];
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.push(99);
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());

    let result = MessageHeader::from_bytes(&buf);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// Message roundtrip helpers
// ---------------------------------------------------------------------------

/// Serialize a Message into the wire format for roundtrip testing.
fn serialize_message(msg: &Message) -> Vec<u8> {
    let mut buf = Vec::new();

    let mut internal_len: usize = 0;
    let has_internal = msg.write_timestamp.is_some()
        || msg.message_id.is_some()
        || !msg.headers.is_empty();

    if has_internal {
        if let Some(_ts) = msg.write_timestamp {
            internal_len += 8;
        }
        if let Some(ref mid) = msg.message_id {
            internal_len += 1 + mid.len();
        }
        for (key, val) in &msg.headers {
            internal_len += 2 + key.len() + 2 + val.len();
        }
    }

    buf.extend_from_slice(&(internal_len as u16).to_be_bytes());

    if has_internal {
        if let Some(ts) = msg.write_timestamp {
            buf.extend_from_slice(&ts.to_be_bytes());
        }
        if let Some(ref mid) = msg.message_id {
            buf.push(mid.len() as u8);
            buf.extend_from_slice(mid);
        }
        let mut headers_len: usize = 0;
        for (key, val) in &msg.headers {
            headers_len += 2 + key.len() + 2 + val.len();
        }
        buf.extend_from_slice(&(headers_len as u16).to_be_bytes());
        for (key, val) in &msg.headers {
            buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&(val.len() as u16).to_be_bytes());
            buf.extend_from_slice(val);
        }
    }

    if let Some(ref key) = msg.key {
        buf.extend_from_slice(&(key.len() as i32).to_be_bytes());
        buf.extend_from_slice(key);
    } else {
        buf.extend_from_slice(&0i32.to_be_bytes());
    }

    if let Some(ref value) = msg.value {
        buf.extend_from_slice(&(value.len() as i32).to_be_bytes());
        buf.extend_from_slice(value);
    } else {
        buf.extend_from_slice(&0i32.to_be_bytes());
    }

    buf
}

#[test]
fn message_roundtrip_full() {
    let msg = Message {
        key: Some(b"mykey".to_vec()),
        value: Some(b"myvalue".to_vec()),
        write_timestamp: Some(1234567890),
        message_id: Some(vec![0u8, 1, 2, 3]),
        headers: vec![
            ("content-type".to_string(), b"application/json".to_vec()),
            ("x-priority".to_string(), b"high".to_vec()),
        ],
    };

    let serialized = serialize_message(&msg);
    let mut iter = MessageIterator::new(&serialized, 1);
    let parsed = iter.next().expect("parse message");
    assert_eq!(parsed, msg);
}

#[test]
fn message_roundtrip_no_key() {
    let msg = Message {
        key: None,
        value: Some(b"hello".to_vec()),
        write_timestamp: None,
        message_id: None,
        headers: vec![],
    };

    let serialized = serialize_message(&msg);
    let mut iter = MessageIterator::new(&serialized, 1);
    let parsed = iter.next().expect("parse message");
    assert_eq!(parsed, msg);
}

#[test]
fn message_roundtrip_no_headers() {
    let msg = Message {
        key: Some(b"key".to_vec()),
        value: Some(b"value".to_vec()),
        write_timestamp: None,
        message_id: None,
        headers: vec![],
    };

    let serialized = serialize_message(&msg);
    let mut iter = MessageIterator::new(&serialized, 1);
    let parsed = iter.next().expect("parse message");
    assert_eq!(parsed, msg);
}

#[test]
fn message_roundtrip_multiple_messages() {
    let msgs = vec![
        Message {
            key: Some(b"k1".to_vec()),
            value: Some(b"v1".to_vec()),
            write_timestamp: None,
            message_id: None,
            headers: vec![],
        },
        Message {
            key: Some(b"k2".to_vec()),
            value: Some(b"v2".to_vec()),
            write_timestamp: Some(999),
            message_id: Some(vec![42]),
            headers: vec![("h".to_string(), b"v".to_vec())],
        },
        Message {
            key: None,
            value: None,
            write_timestamp: None,
            message_id: None,
            headers: vec![],
        },
    ];

    let mut buf = Vec::new();
    for msg in &msgs {
        buf.extend_from_slice(&serialize_message(msg));
    }

    let mut iter = MessageIterator::new(&buf, msgs.len() as u32);
    for expected in &msgs {
        let parsed = iter.next().expect("parse message");
        assert_eq!(parsed, *expected);
    }
    assert!(!iter.has_next());
}

// ---------------------------------------------------------------------------
// Notification JSON parsing
// ---------------------------------------------------------------------------

#[test]
fn parse_notification_json() {
    let json = r#"{
        "bucket": "my-bucket",
        "key": "topics/my-topic/abc123",
        "size": 1024,
        "topic": "my-topic",
        "headerSize": 48,
        "numBatchMessages": 5,
        "numAttempts": 1
    }"#;

    let notification = Notification::from_json(json).expect("parse notification");
    assert_eq!(notification.bucket, "my-bucket");
    assert_eq!(notification.key, "topics/my-topic/abc123");
    assert_eq!(notification.object_size, 1024);
    assert_eq!(notification.topic, "my-topic");
    assert_eq!(notification.header_size, 48);
    assert_eq!(notification.num_batch_messages, 5);
    assert_eq!(notification.num_attempts, 1);
}

#[test]
fn parse_notification_missing_field() {
    let json = r#"{"bucket": "test"}"#;
    let result = Notification::from_json(json);
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// CRC validation
// ---------------------------------------------------------------------------

#[test]
fn crc_mismatch_error() {
    let raw = decode_b64(UNCOMPRESSED_BATCH_B64);
    let mut corrupted = raw.clone();

    let (batch_header, batches) = parse_batch(&raw).expect("parse original");
    let pos = 8 + 12 * batch_header.entries.len();
    let mh_total = batches[0].header.total_bytes();
    let payload_start = pos + mh_total;
    corrupted[payload_start] ^= 0xFF;

    let result = parse_batch(&corrupted);
    assert!(result.is_err());

    match result.unwrap_err() {
        MemqError::CrcMismatch { .. } => {}
        other => panic!("expected CrcMismatch error, got {:?}", other),
    }
}

#[test]
fn valid_crc_succeeds() {
    let raw = decode_b64(UNCOMPRESSED_BATCH_B64);
    let (batch_header, batches) = parse_batch(&raw).expect("parse batch");

    // CRC is computed over the compressed payload (not decompressed)
    let pos = 8 + 12 * batch_header.entries.len();
    let mh_total = batches[0].header.total_bytes();
    let payload_start = pos + mh_total;
    let compressed = &raw[payload_start..payload_start + batches[0].header.message_length as usize];
    let actual = crc32fast::hash(compressed);
    assert_eq!(batches[0].header.crc, actual);
}

// ---------------------------------------------------------------------------
// Full roundtrip: construct → compress → parse → verify
// ---------------------------------------------------------------------------

#[test]
fn roundtrip_uncompressed_full() {
    let msgs = vec![
        Message {
            key: Some(b"test".to_vec()),
            value: Some(b"value".to_vec()),
            write_timestamp: None,
            message_id: None,
            headers: vec![],
        },
        Message {
            key: Some(b"test1231231".to_vec()),
            value: Some(b"test1231231".to_vec()),
            write_timestamp: None,
            message_id: None,
            headers: vec![],
        },
    ];

    let batch = build_batch(&msgs, Compression::None);
    let parsed_msgs = parse_batch_messages(&batch).expect("parse synthetic batch");
    assert_eq!(parsed_msgs.len(), msgs.len());
    for (expected, actual) in msgs.iter().zip(parsed_msgs.iter()) {
        assert_eq!(actual.key, expected.key);
        assert_eq!(actual.value, expected.value);
        assert_eq!(actual.headers, expected.headers);
    }
}

#[test]
fn roundtrip_gzip_full() {
    let msgs = vec![
        Message {
            key: Some(b"key1".to_vec()),
            value: Some(b"value1".to_vec()),
            write_timestamp: None,
            message_id: None,
            headers: vec![],
        },
    ];

    let batch = build_batch(&msgs, Compression::Gzip);
    let parsed_msgs = parse_batch_messages(&batch).expect("parse gzip batch");
    assert_eq!(parsed_msgs.len(), 1);
    assert_eq!(parsed_msgs[0].key, Some(b"key1".to_vec()));
    assert_eq!(parsed_msgs[0].value, Some(b"value1".to_vec()));
}

#[test]
fn roundtrip_zstd_full() {
    let msgs = vec![
        Message {
            key: Some(b"key2".to_vec()),
            value: Some(b"value2".to_vec()),
            write_timestamp: Some(99999),
            message_id: Some(vec![0, 1, 2, 3]),
            headers: vec![("content-type".to_string(), b"application/json".to_vec())],
        },
    ];

    let batch = build_batch(&msgs, Compression::Zstd);
    let parsed_msgs = parse_batch_messages(&batch).expect("parse zstd batch");
    assert_eq!(parsed_msgs.len(), 1);
    assert_eq!(parsed_msgs[0].key, Some(b"key2".to_vec()));
    assert_eq!(parsed_msgs[0].value, Some(b"value2".to_vec()));
    assert_eq!(parsed_msgs[0].write_timestamp, Some(99999));
    assert_eq!(parsed_msgs[0].message_id, Some(vec![0, 1, 2, 3]));
}

/// Build a complete synthetic batch from messages.
fn build_batch(msgs: &[Message], compression: Compression) -> Vec<u8> {
    // Serialize messages into payload
    let mut payload_buf = Vec::new();
    let mut msg_offsets = Vec::with_capacity(msgs.len());
    let mut offset = 0usize;
    for msg in msgs {
        let ser = serialize_message(msg);
        msg_offsets.push(offset);
        offset += ser.len();
        payload_buf.extend_from_slice(&ser);
    }

    // Compress
    let compressed = compression.compress(&payload_buf).unwrap();

    // CRC over compressed
    let crc = crc32fast::hash(&compressed);

    // MessageHeader (21 bytes with no additional header)
    let msg_header_size = 6 + 4 + 1 + 4 + 1 + 4 + 4; // 24 bytes

    // BatchHeader
    let batch_header_size = 8 + 12 * msgs.len();
    let header_length = batch_header_size as u32 + msg_header_size as u32 + compressed.len() as u32;

    let mut entries = Vec::with_capacity(msgs.len());
    for (i, &off) in msg_offsets.iter().enumerate() {
        entries.push((
            i as u32,
            IndexEntry {
                offset: (batch_header_size + msg_header_size + off) as u32,
                size: ser_size_at(&payload_buf, i) as u32,
            },
        ));
    }

    let batch_header = BatchHeader {
        header_length,
        entries,
    };

    // Serialize batch header
    let mut batch_buf = Vec::new();
    batch_buf.extend_from_slice(&batch_header.header_length.to_be_bytes());
    batch_buf.extend_from_slice(&(batch_header.entries.len() as u32).to_be_bytes());
    for (idx, entry) in &batch_header.entries {
        batch_buf.extend_from_slice(&idx.to_be_bytes());
        batch_buf.extend_from_slice(&entry.offset.to_be_bytes());
        batch_buf.extend_from_slice(&entry.size.to_be_bytes());
    }

    // Serialize message header
    let msg_header = MessageHeader {
        header_length: MessageHeader::HEADER_LENGTH,
        version: 100,
        additional_header_length: 0,
        producer_address_length: 0,
        producer_address: vec![],
        producer_epoch: 0,
        producer_request_id: 0,
        crc,
        compression,
        log_message_count: msgs.len() as u32,
        message_length: compressed.len() as u32,
    };

    batch_buf.extend_from_slice(&msg_header.header_length.to_be_bytes());
    batch_buf.extend_from_slice(&msg_header.version.to_be_bytes());
    batch_buf.extend_from_slice(&msg_header.additional_header_length.to_be_bytes());
    batch_buf.extend_from_slice(&msg_header.crc.to_be_bytes());
    batch_buf.push(msg_header.compression as u8);
    batch_buf.extend_from_slice(&msg_header.log_message_count.to_be_bytes());
    batch_buf.extend_from_slice(&msg_header.message_length.to_be_bytes());

    batch_buf.extend_from_slice(&compressed);

    batch_buf
}

/// Calculate the byte size of the message at a given index in the serialized payload.
fn ser_size_at(payload: &[u8], index: usize) -> usize {
    let mut pos = 0;
    for i in 0..=index {
        if pos + 2 > payload.len() {
            return 0;
        }
        let internal_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
        pos += 2;

        if internal_len > 0 {
            if pos + 8 > payload.len() { return 0; }
            pos += 8;
            if pos > payload.len() { return 0; }
            let mid_len = payload[pos] as usize;
            pos += 1 + mid_len;
            if pos + 2 > payload.len() { return 0; }
            let headers_len = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
            pos += 2;
            let mut remaining = headers_len;
            while remaining > 0 && pos < payload.len() {
                if pos + 4 > payload.len() { return 0; }
                let kl = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2 + kl;
                if pos + 2 > payload.len() { return 0; }
                let vl = u16::from_be_bytes([payload[pos], payload[pos + 1]]) as usize;
                pos += 2 + vl;
                remaining -= 4 + kl + vl;
            }
        }

        if pos + 4 > payload.len() { return 0; }
        let kl = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]) as usize;
        pos += 4 + kl;

        if pos + 4 > payload.len() { return 0; }
        let vl = i32::from_be_bytes([payload[pos], payload[pos + 1], payload[pos + 2], payload[pos + 3]]) as usize;
        pos += 4 + vl;

        if i == index {
            return pos;
        }
    }
    0
}
