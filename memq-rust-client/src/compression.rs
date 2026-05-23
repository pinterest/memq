use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use std::io::{Read, Write};

use crate::error::MemqError;

/// Compression scheme used for batch payloads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0,
    Gzip = 1,
    Zstd = 2,
}

impl Compression {
    /// Parse a compression byte from the MemqMessageHeader.
    pub fn from_u8(v: u8) -> Result<Self, MemqError> {
        match v {
            0 => Ok(Compression::None),
            1 => Ok(Compression::Gzip),
            2 => Ok(Compression::Zstd),
            _ => Err(MemqError::UnknownCompression(v)),
        }
    }

    /// Decompress a compressed payload (used when reading batches from S3).
    pub fn decompress(self, compressed: &[u8]) -> std::result::Result<Vec<u8>, MemqError> {
        match self {
            Compression::None => Ok(compressed.to_vec()),
            Compression::Gzip => {
                let mut decoder = GzDecoder::new(compressed);
                let mut out = Vec::new();
                decoder.read_to_end(&mut out)?;
                Ok(out)
            }
            Compression::Zstd => {
                let decompressed = zstd::decode_all(compressed)?;
                Ok(decompressed)
            }
        }
    }

    /// Compress a payload (used when writing batches to S3).
    pub fn compress(self, data: &[u8]) -> std::result::Result<Vec<u8>, MemqError> {
        match self {
            Compression::None => Ok(data.to_vec()),
            Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder.write_all(data)?;
                Ok(encoder.finish()?)
            }
            Compression::Zstd => {
                let compressed = zstd::encode_all(data, 3)?;
                Ok(compressed)
            }
        }
    }
}
