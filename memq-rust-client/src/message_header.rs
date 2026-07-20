use crate::compression::Compression;
use crate::error::{MemqError, Result};

/// MemqMessageHeader: wraps the compressed message payload.
///
/// Wire format (all big-endian):
///   headerLength:            short (2 bytes)
///   version:                 short (2 bytes)
///   additionalHeaderLength:  short (2 bytes)
///   [if additionalHeaderLength > 0:]
///     producerAddressLength: byte (1 byte)
///     producerAddress:       bytes(producerAddressLength)
///     producerEpoch:         long (8 bytes)
///     producerRequestId:     long (8 bytes)
///   crc:                     int (4 bytes)
///   compression:             byte (1 byte)
///   logmessageCount:         int (4 bytes)
///   messageLength:           int (4 bytes)
///
/// Fixed portion: 10 bytes
/// With additional header: + 1 + 4 + 8 + 8 = 21 bytes
/// Total with additional: 31 bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    pub header_length: u16,
    pub version: u16,
    pub additional_header_length: u16,
    pub producer_address_length: u8,
    pub producer_address: Vec<u8>,
    pub producer_epoch: i64,
    pub producer_request_id: i64,
    pub crc: u32,
    pub compression: Compression,
    pub log_message_count: u32,
    pub message_length: u32,
}

impl MessageHeader {
    /// The fixed header length (without additional header).
    pub const HEADER_LENGTH: u16 = 27;

    /// Parse a MessageHeader from raw bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut pos = 0;

        let header_length = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        pos += 2;

        let version = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        pos += 2;

        let additional_header_length = u16::from_be_bytes([bytes[pos], bytes[pos + 1]]);
        pos += 2;

        let mut producer_address_length = 0;
        let mut producer_address = Vec::new();
        let mut producer_epoch = 0i64;
        let mut producer_request_id = 0i64;

        if additional_header_length > 0 {
            producer_address_length = bytes[pos];
            pos += 1;

            if pos + producer_address_length as usize > bytes.len() {
                return Err(MemqError::InvalidHeader("truncated producer address".into()));
            }
            producer_address = bytes[pos..pos + producer_address_length as usize].to_vec();
            pos += producer_address_length as usize;

            producer_epoch = i64::from_be_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7],
            ]);
            pos += 8;

            producer_request_id = i64::from_be_bytes([
                bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3],
                bytes[pos + 4], bytes[pos + 5], bytes[pos + 6], bytes[pos + 7],
            ]);
            pos += 8;
        }

        let crc = u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
        pos += 4;

        let compression = Compression::from_u8(bytes[pos])?;
        pos += 1;

        let log_message_count = u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);
        pos += 4;

        let message_length = u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]]);

        Ok(MessageHeader {
            header_length,
            version,
            additional_header_length,
            producer_address_length,
            producer_address,
            producer_epoch,
            producer_request_id,
            crc,
            compression,
            log_message_count,
            message_length,
        })
    }

    /// Whether this header has additional fields.
    pub fn has_additional_header(&self) -> bool {
        self.additional_header_length > 0
    }

    /// Total bytes consumed by this header on the wire.
    /// The additional_header_length already includes the producer address length + address bytes.
    pub fn total_bytes(&self) -> usize {
        6 + self.additional_header_length as usize + 4 + 1 + 4 + 4
    }
}
