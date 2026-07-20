use crate::error::{MemqError, Result};

/// Index entry mapping a message index to its byte offset and size within the batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub offset: u32,
    pub size: u32,
}

/// BatchHeader: enables O(1) seeks into a stored batch file to find a specific message.
///
/// Wire format:
///   headerLength:  int (4 bytes)
///   numIndexEntries: int (4 bytes)
///   entries[N]:    idx(int) offset(int) size(int) -- 12 bytes each
///
/// The index entries are sorted by message index (0, 1, 2, ...).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchHeader {
    /// Total header length (headerLength field, stored as u32 for convenience).
    pub header_length: u32,
    /// Map of message index -> (offset, size) within the batch.
    pub entries: Vec<(u32, IndexEntry)>,
}

impl BatchHeader {
    /// Parse a BatchHeader from raw bytes.
    ///
    /// The stream starts at the beginning of the header.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 8 {
            return Err(MemqError::BatchTooSmall(bytes.len(), 8));
        }

        let header_length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let num_entries = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]) as usize;

        // Minimum size: 8 (header) + 12 * num_entries
        let expected_min = 8 + 12 * num_entries;
        if bytes.len() < expected_min {
            return Err(MemqError::BatchTooSmall(bytes.len(), expected_min));
        }

        let mut entries = Vec::with_capacity(num_entries);
        let mut offset = 8;

        for _ in 0..num_entries {
            if offset + 12 > bytes.len() {
                return Err(MemqError::InvalidHeader("truncated entry".into()));
            }

            let idx = u32::from_be_bytes([bytes[offset], bytes[offset + 1], bytes[offset + 2], bytes[offset + 3]]);
            let entry_offset = u32::from_be_bytes([bytes[offset + 4], bytes[offset + 5], bytes[offset + 6], bytes[offset + 7]]);
            let entry_size = u32::from_be_bytes([bytes[offset + 8], bytes[offset + 9], bytes[offset + 10], bytes[offset + 11]]);

            entries.push((idx, IndexEntry { offset: entry_offset, size: entry_size }));
            offset += 12;
        }

        Ok(BatchHeader {
            header_length,
            entries,
        })
    }

    /// Look up the IndexEntry for a given message index.
    pub fn get(&self, idx: u32) -> Option<&IndexEntry> {
        self.entries.iter().find(|(i, _)| *i == idx).map(|(_, e)| e)
    }

    /// Return the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the header has no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
