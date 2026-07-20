/// Deserializer trait and implementations for key/value bytes.
///
/// Mirrors the Java `Deserializer` interface.
///
/// Note: The trait uses `&[u8]` input and returns owned types or
/// references with lifetimes tied to the input (not self), since
/// Rust cannot express "return T where T is a reference with the
/// input's lifetime" via a simple type parameter.
pub trait Deserializer<T> {
    /// Initialize the deserializer with configuration properties.
    fn init(&mut self, _props: &std::collections::HashMap<String, String>) {}

    /// Deserialize a byte slice into the target type.
    fn deserialize(&self, bytes: &[u8]) -> T;

    /// Called once at startup to allow the deserializer to read config.
    fn close(&mut self) {}
}

/// Returns raw bytes unchanged (owned copy).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteArrayDeserializer;

impl Deserializer<Vec<u8>> for ByteArrayDeserializer {
    fn deserialize(&self, bytes: &[u8]) -> Vec<u8> {
        bytes.to_vec()
    }
}

/// Deserializes bytes to a String (UTF-8).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).to_string()
    }
}
