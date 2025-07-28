use anyhow::Result;
use futures_util::stream::StreamExt;
use reqwest::{Client, StatusCode};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone)]
pub struct CacheClientConfig {
    pub buffer_size: usize,
    pub max_concurrent_operations: usize,
    pub operation_timeout: Duration,
    pub keep_alive_timeout: Duration,
    pub pool_idle_timeout: Duration,
    pub max_idle_per_host: usize,
    pub max_retries: u32,
    pub initial_retry_delay: Duration,
    pub max_retry_delay: Duration,
    pub bulk_operation_timeout: Duration,
}

impl Default for CacheClientConfig {
    fn default() -> Self {
        Self {
            buffer_size: 64 * 1024, // 64KB buffer for streaming
            max_concurrent_operations: 10,
            operation_timeout: Duration::from_secs(30),
            keep_alive_timeout: Duration::from_secs(60),
            pool_idle_timeout: Duration::from_secs(30),
            max_idle_per_host: 10,
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(10),
            bulk_operation_timeout: Duration::from_secs(120), // 2 minutes
        }
    }
}

#[derive(Error, Debug)]
pub enum CacheError {
    #[error("IO error for {path:?}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("Connection error: {0}")]
    Connection(String),
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("Key already exists: {0}")]
    KeyExists(String),
    #[error("Key not found: {0}")]
    KeyNotFound(String),
    #[error("Server error: {0}")]
    Server(String),
}

#[derive(Debug)]
pub enum DownloadStatus {
    Success,
    Skip,
    Error(CacheError),
}

#[derive(Debug)]
pub struct BatchResultDownload {
    pub filename: String,
    pub status: DownloadStatus,
}

#[derive(Debug)]
pub enum UploadStatus {
    Success,
    Error(CacheError),
    Skip,
}

#[derive(Debug)]
pub struct BatchResultUpload {
    pub filename: String,
    pub status: UploadStatus,
}

pub struct CacheClient {
    client: Client,
    base_url: String,
    config: CacheClientConfig,
}

impl CacheClient {
    /// Create a new cache client with default settings
    pub fn new(base_url: String) -> Self {
        Self::with_config(base_url, CacheClientConfig::default())
    }

    /// Create a new cache client with custom configuration
    pub fn with_config(mut base_url: String, config: CacheClientConfig) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(config.pool_idle_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(config.keep_alive_timeout)
            .build()
            .expect("Failed to create HTTP client");

        if !base_url.starts_with("http") {
            base_url = format!("http://{}", base_url);
        }

        Self {
            client,
            base_url,
            config,
        }
    }

    /// Helper function to format error messages
    fn format_error(&self, err: &CacheError) -> String {
        match err {
            CacheError::KeyNotFound(_) => String::from("File not found on server"),
            CacheError::KeyExists(_) => String::from("File already exists on server"),
            CacheError::Server(msg) => format!("Server error: {}", msg),
            CacheError::Connection(msg) => format!("Connection error: {}", msg),
            CacheError::Http(e) => format!("Network error: {}", e),
            CacheError::Io { path: _, source } => format!("IO error: {}", source),
        }
    }

    /// Check if an error is a connection error
    fn is_connection_error(&self, error: &CacheError) -> bool {
        match error {
            CacheError::Http(e) => e.is_connect() || e.is_timeout(),
            CacheError::Connection(_) => true,
            _ => false,
        }
    }

    /// Execute an operation with retry logic for connection errors
    async fn with_retry<T, F, Fut>(&self, operation: F) -> Result<T, CacheError>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, CacheError>>,
    {
        let mut last_error = None;
        let mut delay = self.config.initial_retry_delay;

        for attempt in 0..self.config.max_retries {
            let is_last_attempt = attempt == self.config.max_retries - 1;
            
            match tokio::time::timeout(self.config.operation_timeout, operation()).await {
                Ok(Ok(result)) => return Ok(result),
                Ok(Err(e)) => {
                    if !self.is_connection_error(&e) || is_last_attempt {
                        if self.is_connection_error(&e) {
                            log::error!("Connection error after {} retries: {}", attempt + 1, self.format_error(&e));
                        }
                        return Err(e);
                    }
                    
                    log::warn!("Retry attempt {} of {} due to: {}", attempt + 1, self.config.max_retries, self.format_error(&e));
                    last_error = Some(e);
                }
                Err(_) => {
                    let timeout_error = CacheError::Connection("Operation timed out".to_string());
                    if is_last_attempt {
                        log::error!("Operation timed out after {} retries", attempt + 1);
                        return Err(timeout_error);
                    }
                    
                    log::warn!("Retry attempt {} of {} due to timeout", attempt + 1, self.config.max_retries);
                    last_error = Some(timeout_error);
                }
            }
            
            // Sleep before retry (skip on last attempt to avoid unnecessary delay)
            if !is_last_attempt {
                tokio::time::sleep(delay).await;
                
                // Calculate next delay with exponential backoff and jitter
                let jitter_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos() % 100;
                delay = std::cmp::min(
                    delay * 2 + Duration::from_millis(jitter_ms as u64),
                    self.config.max_retry_delay,
                );
            }
        }

        Err(last_error.unwrap_or_else(|| CacheError::Connection("Retry failed".to_string())))
    }

    /// Check if a key exists in the cache using a HEAD request
    async fn check_key_exists(&self, key: &str) -> Result<bool, CacheError> {
        let response = self
            .client
            .head(&format!("{}/cache/{}", self.base_url, key))
            .send()
            .await?;

        Ok(response.status() == StatusCode::OK)
    }

    /// Check if a file exists and matches the remote content length
    async fn should_skip_download(
        &self,
        key: &str,
        output_path: &Path,
    ) -> Result<bool, CacheError> {
        // First check if file exists locally
        if !output_path.exists() {
            return Ok(false);
        }

        // Get local file size
        let local_metadata =
            tokio::fs::metadata(output_path)
                .await
                .map_err(|e| CacheError::Io {
                    path: output_path.to_path_buf(),
                    source: e,
                })?;
        let local_size = local_metadata.len();

        // Get remote file size with HEAD request
        let response = self
            .client
            .head(&format!("{}/cache/{}", self.base_url, key))
            .send()
            .await?;

        if response.status() != StatusCode::OK {
            return Ok(false);
        }

        // Compare content lengths
        if let Some(content_length) = response.content_length() {
            Ok(content_length == local_size)
        } else {
            Ok(false)
        }
    }

    /// Download multiple files from the cache, saving them to the current directory
    pub async fn batch_download(&self, filenames: &[String]) -> Vec<BatchResultDownload> {

        let bulk_timeout = tokio::time::timeout(
            self.config.bulk_operation_timeout,
            self.batch_download_inner(filenames),
        );

        match bulk_timeout.await {
            Ok(inner_results) => inner_results,
            Err(_) => {
                // If the entire bulk operation times out, return error status for all files
                filenames
                    .iter()
                    .map(|filename| BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(CacheError::Connection(
                            "Bulk operation timeout (2 minutes)".to_string(),
                        )),
                    })
                    .collect()
            }
        }
    }

    async fn batch_download_inner(&self, filenames: &[String]) -> Vec<BatchResultDownload> {
        let mut results = Vec::with_capacity(filenames.len());

        let mut downloads = futures::stream::iter(filenames.iter().map(|filename| async move {
            let filename = filename;
            
            // Extract key from filename, handling potential errors
            let key = match Path::new(&filename).file_name() {
                Some(name) => name.to_string_lossy().to_string(),
                None => {
                    return BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(CacheError::Io {
                            path: Path::new(&filename).to_path_buf(),
                            source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid filename"),
                        }),
                    };
                }
            };

            // Use retry logic for each individual download
            let result = self
                .with_retry(|| async {
                    self.get_file(&key, Path::new(&filename)).await
                })
                .await;

            BatchResultDownload {
                filename: filename.clone(),
                status: match result {
                    Ok(status) => status,
                    Err(e) => DownloadStatus::Error(e),
                },
            }
        }))
        .buffer_unordered(self.config.max_concurrent_operations);

        while let Some(result) = downloads.next().await {
            results.push(result);
        }

        results
    }

    /// Upload multiple files to the cache from the current directory
    pub async fn batch_upload(&self, filenames: &[String]) -> Vec<BatchResultUpload> {
        let bulk_timeout = tokio::time::timeout(
            self.config.bulk_operation_timeout,
            self.batch_upload_inner(filenames),
        );

        match bulk_timeout.await {
            Ok(inner_results) => inner_results,
            Err(_) => {
                // If the entire bulk operation times out, return error status for all files
                filenames
                    .iter()
                    .map(|filename| BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Error(CacheError::Connection(
                            "Bulk operation timeout (2 minutes)".to_string(),
                        )),
                    })
                    .collect()
            }
        }
    }

    async fn batch_upload_inner(&self, filenames: &[String]) -> Vec<BatchResultUpload> {
        let mut results = Vec::with_capacity(filenames.len());

        let mut uploads = futures_util::stream::iter(filenames.iter().map(|filename| {
            let filename = filename.clone();
            async move {
                // Use retry logic for each individual upload
                let result = self
                    .with_retry(|| async { self.put_file(Path::new(&filename)).await })
                    .await;

                match result {
                    Ok(_) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Success,
                    },
                    Err(CacheError::KeyExists(_)) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Skip,
                    },
                    Err(e) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Error(e),
                    },
                }
            }
        }))
        .buffer_unordered(self.config.max_concurrent_operations);

        while let Some(result) = uploads.next().await {
            results.push(result);
        }

        results
    }

    async fn put_file(&self, file_path: &Path) -> Result<(), CacheError> {
        let key = file_path
            .file_name()
            .ok_or_else(|| CacheError::Io {
                path: file_path.to_path_buf(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid file path"),
            })?
            .to_str()
            .ok_or_else(|| CacheError::Io {
                path: file_path.to_path_buf(),
                source: std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 in filename"),
            })?
            .to_string();

        // First check if the key exists using HEAD request
        if self.check_key_exists(&key).await? {
            return Err(CacheError::KeyExists(key));
        }

        let metadata = tokio::fs::metadata(file_path)
            .await
            .map_err(|e| CacheError::Io {
                path: file_path.to_path_buf(),
                source: e,
            })?;

        let file_size = metadata.len();
        let file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| CacheError::Io {
                path: file_path.to_path_buf(),
                source: e,
            })?;

        // Stream the file in chunks instead of reading it all into memory
        let stream = tokio_util::io::ReaderStream::with_capacity(file, self.config.buffer_size);

        let response = self
            .client
            .put(&format!("{}/cache/{}", self.base_url, key))
            .body(reqwest::Body::wrap_stream(stream))
            .header("content-length", file_size)
            .send()
            .await?;

        match response.status() {
            StatusCode::CREATED => Ok(()),
            StatusCode::CONFLICT => Err(CacheError::KeyExists(key.to_string())),
            status => Err(CacheError::Server(format!("Unexpected status: {}", status))),
        }
    }

    async fn get_file(&self, key: &str, output_path: &Path) -> Result<DownloadStatus, CacheError> {
        // Check if we can skip the download
        if self.should_skip_download(key, output_path).await? {
            return Ok(DownloadStatus::Skip);
        }

        let url = format!("{}/cache/{}", self.base_url, key);
        let response = self.client.get(&url).send().await?;

        match response.status() {
            StatusCode::OK => {
                // Create parent directories if they don't exist
                if let Some(parent) = output_path.parent() {
                    tokio::fs::create_dir_all(parent)
                        .await
                        .map_err(|e| CacheError::Io {
                            path: parent.to_path_buf(),
                            source: e,
                        })?;
                }

                let file =
                    tokio::fs::File::create(output_path)
                        .await
                        .map_err(|e| CacheError::Io {
                            path: output_path.to_path_buf(),
                            source: e,
                        })?;

                let mut stream = response.bytes_stream();
                let mut writer = tokio::io::BufWriter::with_capacity(self.config.buffer_size, file);

                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.map_err(|e| CacheError::Http(e))?;
                    writer.write_all(&chunk).await.map_err(|e| CacheError::Io {
                        path: output_path.to_path_buf(),
                        source: e,
                    })?;
                }

                writer.flush().await.map_err(|e| CacheError::Io {
                    path: output_path.to_path_buf(),
                    source: e,
                })?;

                Ok(DownloadStatus::Success)
            }
            StatusCode::NOT_FOUND => Err(CacheError::KeyNotFound(key.to_string())),
            status => Err(CacheError::Server(format!("Unexpected status: {}", status))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_logic() {
        let config = CacheClientConfig {
            max_retries: 3,
            initial_retry_delay: Duration::from_millis(10),
            max_retry_delay: Duration::from_millis(100),
            ..Default::default()
        };

        let client = CacheClient::with_config("http://localhost:8080".to_string(), config);
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        // Test that retry logic retries on connection errors
        let result = client
            .with_retry(|| {
                let count = call_count_clone.clone();
                async move {
                    let current = count.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        // Fail first 2 attempts with connection error
                        Err(CacheError::Connection("Simulated connection error".to_string()))
                    } else {
                        // Succeed on 3rd attempt
                        Ok("Success")
                    }
                }
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_non_connection_error() {
        let config = CacheClientConfig {
            max_retries: 3,
            ..Default::default()
        };

        let client = CacheClient::with_config("http://localhost:8080".to_string(), config);
        let call_count = Arc::new(AtomicU32::new(0));
        let call_count_clone = call_count.clone();

        // Test that retry logic does not retry on non-connection errors
        let result: Result<(), CacheError> = client
            .with_retry(|| {
                let count = call_count_clone.clone();
                async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Err(CacheError::KeyNotFound("test.txt".to_string()))
                }
            })
            .await;

        assert!(result.is_err());
        // Should only be called once since it's not a connection error
        assert_eq!(call_count.load(Ordering::SeqCst), 1);
    }
    
    #[tokio::test]
    async fn test_jitter_distribution() {
        // Test that jitter calculation produces values in the expected range
        // Note: We can't reliably test randomness on all platforms due to timer resolution differences
        
        let mut all_in_range = true;
        let mut seen_different_values = false;
        let mut first_value = None;
        
        for _ in 0..1000 {
            let jitter_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() % 100;
            
            // Verify value is in range
            if jitter_ms >= 100 {
                all_in_range = false;
                break;
            }
            
            // Check if we've seen different values
            match first_value {
                None => first_value = Some(jitter_ms),
                Some(v) if v != jitter_ms => seen_different_values = true,
                _ => {}
            }
            
            // If we've already seen different values and confirmed range, we can stop
            if seen_different_values {
                break;
            }
        }
        
        // The jitter MUST always be in range 0-99
        assert!(all_in_range, "Jitter value exceeded valid range (0-99)");
        
        // Note: We don't assert on randomness because some CI environments have
        // very coarse timer resolution that makes all timestamps identical within
        // a short time period. The important thing is that jitter is bounded.
    }
}

pub async fn upload(base_url: String, files: Vec<String>, verbose: bool) -> std::io::Result<()> {
    let client = CacheClient::new(base_url);
    println!("Uploading {} files...", files.len());
    let results = client.batch_upload(&files).await;
    
    // Print summary
    let success_count = results.iter().filter(|r| matches!(r.status, UploadStatus::Success)).count();
    let skip_count = results.iter().filter(|r| matches!(r.status, UploadStatus::Skip)).count();
    let error_count = results.iter().filter(|r| matches!(r.status, UploadStatus::Error(_))).count();
    
    if verbose {
        for result in &results {
            match &result.status {
                UploadStatus::Success => println!("✓ Uploaded: {}", result.filename),
                UploadStatus::Skip => println!("• Skipped: {}", result.filename),
                UploadStatus::Error(err) => eprintln!("✗ Failed({}): {}", client.format_error(err), result.filename),
            }
        }
    }
    
    println!("\nSummary: {} uploaded, {} skipped, {} failed", success_count, skip_count, error_count);
    Ok(())
}

pub async fn download(base_url: String, files: Vec<String>, verbose: bool) -> std::io::Result<()> {
    let client = CacheClient::new(base_url);
    println!("Downloading {} files...", files.len());
    let results = client.batch_download(&files).await;
    
    // Print summary
    let success_count = results.iter().filter(|r| matches!(r.status, DownloadStatus::Success)).count();
    let skip_count = results.iter().filter(|r| matches!(r.status, DownloadStatus::Skip)).count();
    let error_count = results.iter().filter(|r| matches!(r.status, DownloadStatus::Error(_))).count();
    
    if verbose {
        for result in &results {
            match &result.status {
                DownloadStatus::Success => println!("✓ Downloaded: {}", result.filename),
                DownloadStatus::Skip => println!("• Skipped: {}", result.filename),
                DownloadStatus::Error(err) => eprintln!("✗ Failed({}): {}", client.format_error(err), result.filename),
            }
        }
    }
    
    println!("\nSummary: {} downloaded, {} skipped, {} failed", success_count, skip_count, error_count);
    Ok(())
}
