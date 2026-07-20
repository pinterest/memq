use memq_rust_client::deserializer::{ByteArrayDeserializer, Deserializer, StringDeserializer};

#[test]
fn string_deserialize_ascii() {
    let d = StringDeserializer;
    let result = d.deserialize(b"hello world");
    assert_eq!(result, "hello world");
}

#[test]
fn string_deserialize_utf8() {
    let d = StringDeserializer;
    let result = d.deserialize("héllo wörld".as_bytes());
    assert_eq!(result, "héllo wörld");
}

#[test]
fn string_deserialize_empty() {
    let d = StringDeserializer;
    let result = d.deserialize(&[]);
    assert_eq!(result, "");
}

#[test]
fn byte_array_deserialize_returns_owned() {
    let d = ByteArrayDeserializer;
    let input = b"test data";
    let result = d.deserialize(input);
    assert_eq!(result, b"test data");
    assert_eq!(result.len(), input.len());
    // Verify it's an owned Vec<u8>, not a reference
    let _modified = result; // owned, can be moved
}

#[test]
fn byte_array_deserialize_empty() {
    let d = ByteArrayDeserializer;
    let result = d.deserialize(&[]);
    assert!(result.is_empty());
}
