/// Storage handler trait and implementations for fetching batch data.
///
/// Mirrors the Java `AbstractS3StorageHandler` / `FileSystemStorageHandler`
/// abstraction: fetch full batches, headers (range), and individual messages (range).

use crate::batch_header::IndexEntry;
use crate::error::{MemqError, Result};
use crate::notification::Notification;
use std::path::PathBuf;

/// A batch fetched from storage.
#[derive(Debug, Clone)]
pub struct BatchData {
    /// Raw batch bytes (BatchHeader + MessageHeader + payload).
    pub data: Vec<u8>,
}

/// Storage handler trait — abstracts S3 and filesystem backends.
pub trait StorageHandler {
    /// Fetch a full batch from storage.
    fn fetch_batch(&self, bucket: &str, key: &str) -> Result<BatchData>;

    /// Fetch only the BatchHeader portion of a batch.
    fn fetch_header(&self, bucket: &str, key: &str, header_size: u64) -> Result<Vec<u8>>;

    /// Fetch a single message from a batch via range request.
    fn fetch_message(&self, bucket: &str, key: &str, entry: &IndexEntry) -> Result<Vec<u8>>;

    /// Get the object size from a notification's storage metadata.
    fn get_batch_size(&self, notification: &Notification) -> u64;
}

// ---------------------------------------------------------------------------
// Filesystem storage handler (for testing / local deployment)
// ---------------------------------------------------------------------------

/// Filesystem storage handler configuration.
#[derive(Debug, Clone)]
pub struct FsStorageConfig {
    /// Root directory where batch files are stored.
    pub root_dir: PathBuf,
}

use std::io::{Read, Seek};
///
/// Maps notification `bucket` → root directory, `key` → file path under root.
/// Mirrors Java's `FileSystemStorageHandler`.
pub struct FsStorageHandler {
    config: FsStorageConfig,
}

impl FsStorageHandler {
    /// Create a new FsStorageHandler from config.
    pub fn new(config: FsStorageConfig) -> Self {
        FsStorageHandler { config }
    }

    /// Resolve a bucket/key to a file path.
    ///
    /// For Rust test format: root_dir/bucket/key
    /// For Java broker format (where bucket is a directory path): root_dir/bucket/key
    ///   The bucket already contains the full path prefix from the broker's "path" field.
    fn resolve_path(&self, bucket: &str, key: &str) -> PathBuf {
        self.config.root_dir.join(bucket).join(key)
    }

 }

impl StorageHandler for FsStorageHandler {
    fn fetch_batch(&self, bucket: &str, key: &str) -> Result<BatchData> {
        let path = self.resolve_path(bucket, key);
        let data = std::fs::read(&path).map_err(|e| MemqError::Io(e))?;
        Ok(BatchData { data })
    }

    fn fetch_header(&self, bucket: &str, key: &str, header_size: u64) -> Result<Vec<u8>> {
        let path = self.resolve_path(bucket, key);
        let file = std::fs::File::open(&path).map_err(|e| MemqError::Io(e))?;
        let mut reader = std::io::BufReader::new(file);
        let mut data = Vec::with_capacity(header_size as usize);
        reader.read_to_end(&mut data).map_err(|e| MemqError::Io(e))?;
        // Truncate to header_size if larger
        data.truncate(header_size as usize);
        Ok(data)
    }

    fn fetch_message(&self, bucket: &str, key: &str, entry: &IndexEntry) -> Result<Vec<u8>> {
        let path = self.resolve_path(bucket, key);
        let file = std::fs::File::open(&path).map_err(|e| MemqError::Io(e))?;
        let mut reader = std::io::BufReader::new(file);
        let offset = entry.offset as u64;
        let size = entry.size as u64;
        // Seek to offset
        reader.seek(std::io::SeekFrom::Start(offset)).map_err(|e| MemqError::Io(e))?;
        let mut data = vec![0u8; size as usize];
        reader.read_exact(&mut data).map_err(|e| MemqError::Io(e))?;
        Ok(data)
    }

    fn get_batch_size(&self, notification: &Notification) -> u64 {
        notification.object_size
    }
}

// ---------------------------------------------------------------------------
// S3 storage handler (production)
// ---------------------------------------------------------------------------

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::Client;
use std::time::Duration;
use tokio::runtime::Runtime;

/// S3 storage handler configuration.
#[derive(Debug, Clone)]
pub struct S3StorageConfig {
    /// AWS region (e.g., "us-east-1").
    pub region: String,
}

/// S3 storage handler for reading batches.
///
/// Generates presigned URLs client-side using the AWS SDK, then fetches
/// data via HTTP. Mirrors Java's `AbstractS3StorageHandler`.
pub struct S3StorageHandler {
    client: Client,
    runtime: Runtime,
    _config: S3StorageConfig,
}

impl S3StorageHandler {
    /// Create a new S3StorageHandler from config.
    ///
    /// Uses AWS credential chain (environment variables, ~/.aws/credentials,
    /// IAM roles, etc.).
    pub fn new(config: S3StorageConfig) -> Result<Self> {
        let runtime = Runtime::new().map_err(|e| MemqError::S3Client(e.to_string()))?;
        let region = config.region.clone();

        let sdk_config = runtime.block_on(async move {
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new(region))
                .load()
                .await
        });

        let client = Client::new(&sdk_config);

        Ok(S3StorageHandler {
            client,
            runtime,
            _config: config,
        })
    }

    /// Generate a presigned GET URL for an S3 object.
    fn presign_get_url(
        &self,
        bucket: &str,
        key: &str,
        expires_in: Duration,
    ) -> Result<String> {
        self.runtime.block_on(async {
            let presigned_req = self
                .client
                .get_object()
                .bucket(bucket)
                .key(key)
                .presigned(PresigningConfig::expires_in(expires_in)
                    .map_err(|e| MemqError::PresignError(e.to_string()))?)
                .await
                .map_err(|e| MemqError::PresignError(e.to_string()))?;

            Ok(presigned_req.uri().to_string())
        })
    }
}

impl StorageHandler for S3StorageHandler {
    fn fetch_batch(&self, bucket: &str, key: &str) -> Result<BatchData> {
        let url = self.presign_get_url(bucket, key, Duration::from_secs(3600))?;

        let response = reqwest::blocking::Client::new()
            .get(&url)
            .send()
            .map_err(|e| MemqError::HttpError {
                status: 0,
                msg: e.to_string(),
            })?;

        let status = response.status().as_u16();
        if status == 404 {
            return Err(MemqError::S3NotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if status == 403 {
            return Err(MemqError::S3Forbidden {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MemqError::HttpError { status, msg: response.status().to_string() });
        }

        let data = response
            .bytes()
            .map_err(|e| MemqError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .to_vec();

        Ok(BatchData { data })
    }

    fn fetch_header(&self, bucket: &str, key: &str, header_size: u64) -> Result<Vec<u8>> {
        let url = self.presign_get_url(bucket, key, Duration::from_secs(3600))?;

        let response = reqwest::blocking::Client::new()
            .get(&url)
            .header("Range", format!("bytes=0-{}", header_size - 1))
            .send()
            .map_err(|e| MemqError::HttpError {
                status: 0,
                msg: e.to_string(),
            })?;

        let status = response.status().as_u16();
        if status == 404 {
            return Err(MemqError::S3NotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if status == 403 {
            return Err(MemqError::S3Forbidden {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MemqError::HttpError { status, msg: response.status().to_string() });
        }

        let data = response
            .bytes()
            .map_err(|e| MemqError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .to_vec();

        Ok(data)
    }

    fn fetch_message(&self, bucket: &str, key: &str, entry: &IndexEntry) -> Result<Vec<u8>> {
        let end = entry.offset + entry.size - 1;
        let url = self.presign_get_url(bucket, key, Duration::from_secs(3600))?;

        let response = reqwest::blocking::Client::new()
            .get(&url)
            .header("Range", format!("bytes={}-{}", entry.offset, end))
            .send()
            .map_err(|e| MemqError::HttpError {
                status: 0,
                msg: e.to_string(),
            })?;

        let status = response.status().as_u16();
        if status == 404 {
            return Err(MemqError::S3NotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if status == 403 {
            return Err(MemqError::S3Forbidden {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }
        if !response.status().is_success() {
            return Err(MemqError::HttpError { status, msg: response.status().to_string() });
        }

        let data = response
            .bytes()
            .map_err(|e| MemqError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?
            .to_vec();

        Ok(data)
    }

    fn get_batch_size(&self, notification: &Notification) -> u64 {
        notification.object_size
    }
}

/// Configuration enum for choosing between S3 and filesystem storage.
#[derive(Debug, Clone)]
pub enum MemqStorageConfig {
    S3(S3StorageConfig),
    Fs(FsStorageConfig),
}

/// Create a storage handler from the configuration.
pub fn create_storage_handler(config: MemqStorageConfig) -> Result<Box<dyn StorageHandler>> {
    match config {
        MemqStorageConfig::S3(s3_config) => {
            let handler = S3StorageHandler::new(s3_config)?;
            Ok(Box::new(handler))
        }
        MemqStorageConfig::Fs(fs_config) => {
            let handler = FsStorageHandler::new(fs_config);
            Ok(Box::new(handler))
        }
    }
}
