use anyhow::Result;
use futures_util::stream::StreamExt;
use reqwest::{Client, StatusCode};
use std::path::{Path, PathBuf};
use std::time::Duration;
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
    /// Create a new cache client with optimized settings
    pub fn new(base_url: String) -> Self {
        Self::with_config(base_url, CacheClientConfig::default())
    }

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
        let mut results = Vec::with_capacity(filenames.len());

        let mut downloads = futures::stream::iter(filenames.iter().map(|filename| async move {
            let filename = filename;
            let key = Path::new(&filename)
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string();

            let result = match tokio::time::timeout(
                self.config.operation_timeout,
                self.get_file(&key, Path::new(&filename)),
            )
            .await
            {
                Ok(Ok(status)) => {
                    match status {
                        DownloadStatus::Success => println!("✓ Downloaded: {}", filename),
                        DownloadStatus::Skip => println!("• Skipped: {}", filename),
                        DownloadStatus::Error(ref err) => {
                            eprintln!("✗ Failed: {} {:?}", filename, err)
                        }
                    }
                    BatchResultDownload {
                        filename: filename.clone(),
                        status,
                    }
                }
                Ok(Err(e)) => {
                    eprintln!("✗ Failed: {} {:?}", filename, e);
                    BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(e),
                    }
                }
                Err(_) => {
                    eprintln!("✗ Failed: {} Operation timed out", filename);
                    BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(CacheError::Server(
                            "Operation timed out".to_string(),
                        )),
                    }
                }
            };
            result
        }))
        .buffer_unordered(self.config.max_concurrent_operations);

        while let Some(result) = downloads.next().await {
            results.push(result);
        }

        results
    }

    /// Upload multiple files to the cache from the current directory
    pub async fn batch_upload(&self, filenames: &[String]) -> Vec<BatchResultUpload> {
        let mut results = Vec::with_capacity(filenames.len());

        let mut uploads = futures_util::stream::iter(filenames.iter().map(|filename| {
            let filename = filename.clone();
            async move {
                let result = match tokio::time::timeout(
                    self.config.operation_timeout,
                    self.put_file(Path::new(&filename)),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        println!("✓ Uploaded: {}", filename);
                        BatchResultUpload {
                            filename: filename.clone(),
                            status: UploadStatus::Success,
                        }
                    }
                    Ok(Err(CacheError::KeyExists(_))) => {
                        println!("• Skipped: {}", filename);
                        BatchResultUpload {
                            filename: filename.clone(),
                            status: UploadStatus::Skip,
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!("✗ Failed: {} {:?}", filename, e);
                        BatchResultUpload {
                            filename: filename.clone(),
                            status: UploadStatus::Error(e),
                        }
                    }
                    Err(_) => {
                        eprintln!("✗ Failed: {} Operation timed out", filename);
                        BatchResultUpload {
                            filename: filename.clone(),
                            status: UploadStatus::Error(CacheError::Server(
                                "Operation timed out".to_string(),
                            )),
                        }
                    }
                };
                result
            }
        }))
        .buffer_unordered(self.config.max_concurrent_operations);

        while let Some(result) = uploads.next().await {
            results.push(result);
        }

        results
    }

    async fn put_file(&self, file_path: &Path) -> Result<(), CacheError> {
        let key = file_path.file_name().unwrap().to_str().unwrap().to_string();

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
            StatusCode::CONFLICT => {
                // This should rarely happen now since we check first
                Err(CacheError::KeyExists(key.to_string()))
            }
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

pub async fn upload(base_url: String, files: Vec<String>) -> std::io::Result<()> {
    let client = CacheClient::new(base_url);
    println!("Uploading {} files...", files.len());
    let results = client.batch_upload(&files).await;
    Ok(())
}

pub async fn download(base_url: String, files: Vec<String>) -> std::io::Result<()> {
    let client = CacheClient::new(base_url);
    println!("Downloading {} files...", files.len());
    let results = client.batch_download(&files).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_batch_operations() -> Result<()> {
        // Create a temporary directory for our test files
        let temp_dir = TempDir::new()?;
        std::env::set_current_dir(temp_dir.path())?;

        // Create some test files
        let test_files = vec![
            ("file1.txt", b"Hello World 1"),
            ("file2.txt", b"Hello World 2"),
            ("file3.txt", b"Hello World 3"),
        ];

        for (filename, content) in &test_files {
            tokio::fs::write(filename, content).await?;
        }

        let client = CacheClient::new("http://localhost:8080".to_string());

        // Test batch upload
        let filenames: Vec<String> = test_files
            .iter()
            .map(|(name, _)| name.to_string())
            .collect();

        let upload_results = client.batch_upload(&filenames).await;
        assert_eq!(upload_results.len(), test_files.len());
        for result in &upload_results {
            assert!(
                matches!(result.status, UploadStatus::Success),
                "Upload failed for {}: {:?}",
                result.filename,
                result.status
            );
        }

        // Delete the original files
        for filename in &filenames {
            tokio::fs::remove_file(filename).await?;
        }

        // Test batch download
        let download_results = client.batch_download(&filenames).await;
        assert_eq!(download_results.len(), test_files.len());

        // Verify the downloaded files
        for ((filename, original_content), result) in test_files.iter().zip(&download_results) {
            assert!(
                matches!(result.status, DownloadStatus::Success),
                "                Download failed for {}: {:?}",
                filename,
                result.status
            );

            let content = tokio::fs::read(filename).await?;
            assert_eq!(&content, original_content);
        }

        // Test skipping existing files
        let second_download = client.batch_download(&filenames).await;
        assert_eq!(second_download.len(), test_files.len());

        // Verify all files were skipped
        for result in &second_download {
            assert!(
                matches!(result.status, DownloadStatus::Skip),
                "File should have been skipped: {}: {:?}",
                result.filename,
                result.status
            );
        }

        Ok(())
    }
}
