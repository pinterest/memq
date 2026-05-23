use crate::error::{MemqError, Result};

/// A single message from a MemQ batch.
///
/// Wire format within the (decompressed) message payload:
///   internalFieldsLength:  short (2 bytes)
///   [if internalFieldsLength > 0:]
///     writeTimestamp:      long (8 bytes)
///     messageIdLength:     byte (1 byte)
///     [if messageIdLength > 0:]
///       messageId:         bytes(messageIdLength)
///     headersLength:       short (2 bytes)
///     [while headersLength > 0:]
///       headerKeyLength:   short (2 bytes)
///       headerKey:         bytes(headerKeyLength)
///       headerValueLength: short (2 bytes)
///       headerValue:       bytes(headerValueLength)
///       headersLength -= 4 + keyLen + valLen
///   keyLength:               int (4 bytes)
///   [key(keyLength bytes)]
///   valueLength:             int (4 bytes)
///   [value(valueLength bytes)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
    pub write_timestamp: Option<i64>,
    pub message_id: Option<Vec<u8>>,
    pub headers: Vec<(String, Vec<u8>)>,
}

/// Iterator over messages in a (decompressed) batch payload stream.
///
/// Yields messages one at a time until the stream is exhausted or
/// log_message_count messages have been read.
pub struct MessageIterator<'a> {
    data: &'a [u8],
    pos: usize,
    remaining: u32,
    #[allow(dead_code)]
    log_message_count: u32,
}

impl<'a> MessageIterator<'a> {
    /// Create a new MessageIterator from decompressed batch payload bytes.
    pub fn new(data: &'a [u8], log_message_count: u32) -> Self {
        MessageIterator {
            data,
            pos: 0,
            remaining: log_message_count,
            log_message_count,
        }
    }

    /// Whether there are more messages to read.
    pub fn has_next(&self) -> bool {
        self.remaining > 0 && self.pos < self.data.len()
    }

    /// Read the next message from the stream.
    pub fn next(&mut self) -> Result<Message> {
        if !self.has_next() {
            return Err(MemqError::UnexpectedEof);
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
            // Read writeTimestamp
            if self.pos + 8 > self.data.len() {
                return Err(MemqError::UnexpectedEof);
            }
            write_timestamp = Some(i64::from_be_bytes([
                self.data[self.pos], self.data[self.pos + 1], self.data[self.pos + 2],
                self.data[self.pos + 3], self.data[self.pos + 4], self.data[self.pos + 5],
                self.data[self.pos + 6], self.data[self.pos + 7],
            ]));
            self.pos += 8;

            // Read messageIdLength
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

            // Read headers
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
