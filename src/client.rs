use anyhow::Result;
use futures_util::stream::StreamExt;
use reqwest::{Client, StatusCode};
use std::path::{Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};

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

    pub fn with_config(base_url: String, config: CacheClientConfig) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(config.max_idle_per_host)
            .pool_idle_timeout(config.pool_idle_timeout)
            .tcp_nodelay(true)
            .tcp_keepalive(config.keep_alive_timeout)
            .build()
            .expect("Failed to create HTTP client");

        Self {
            client,
            base_url,
            config,
        }
    }

    /// Download multiple files from the cache, saving them to the current directory
    pub async fn batch_download(&self, filenames: &[String]) -> Vec<BatchResultDownload> {
        let mut results = Vec::with_capacity(filenames.len());
        let total_files = filenames.len();
        let mut processed = 0;

        let mut downloads = futures::stream::iter(filenames.iter().map(|filename| {
            let filename = filename.clone();
            async move {
                let result = match tokio::time::timeout(
                    Duration::from_secs(30), // Add timeout
                    self.get_file(&filename, Path::new(&filename)),
                )
                .await
                {
                    Ok(Ok(_)) => BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Success,
                    },
                    Ok(Err(e)) => BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(e),
                    },
                    Err(_) => BatchResultDownload {
                        filename: filename.clone(),
                        status: DownloadStatus::Error(CacheError::Server(
                            "Operation timed out".to_string(),
                        )),
                    },
                };
                processed += 1;
                if processed % 10 == 0 || processed == total_files {
                    println!("Progress: {}/{} files processed", processed, total_files);
                }
                result
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
        let mut results = Vec::with_capacity(filenames.len());
        let total_files = filenames.len();
        let mut processed = 0;

        let mut uploads = futures_util::stream::iter(filenames.iter().map(|filename| {
            let filename = filename.clone();
            async move {
                let result = match tokio::time::timeout(
                    Duration::from_secs(30), // Add timeout
                    self.put_file(Path::new(&filename)),
                )
                .await
                {
                    Ok(Ok(_)) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Success,
                    },
                    Ok(Err(CacheError::KeyExists(_))) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Skip,
                    },
                    Ok(Err(e)) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Error(e),
                    },
                    Err(_) => BatchResultUpload {
                        filename: filename.clone(),
                        status: UploadStatus::Error(CacheError::Server(
                            "Operation timed out".to_string(),
                        )),
                    },
                };
                processed += 1;
                if processed % 10 == 0 || processed == total_files {
                    println!("Progress: {}/{} files processed", processed, total_files);
                }
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
        let file = File::open(file_path).await.map_err(|e| CacheError::Io {
            path: file_path.to_path_buf(),
            source: e,
        })?;

        let file_size = file
            .metadata()
            .await
            .map_err(|e| CacheError::Io {
                path: file_path.to_path_buf(),
                source: e,
            })?
            .len();

        let mut buf_reader = BufReader::with_capacity(self.config.buffer_size, file);
        let mut buffer = Vec::with_capacity(self.config.buffer_size);
        buf_reader
            .read_to_end(&mut buffer)
            .await
            .map_err(|e| CacheError::Io {
                path: file_path.to_path_buf(),
                source: e,
            })?;

        let response = self
            .client
            .put(&format!("{}/cache/{}", self.base_url, key))
            .body(buffer)
            .header("content-length", file_size)
            .send()
            .await?;

        match response.status() {
            StatusCode::CREATED => Ok(()),
            StatusCode::CONFLICT => {
                // Treat conflict as a skip.
                Err(CacheError::KeyExists(key.to_string()))
            }
            status => Err(CacheError::Server(format!("Unexpected status: {}", status))),
        }
    }

    async fn get_file(&self, key: &str, output_path: &Path) -> Result<(), CacheError> {
        let response = self
            .client
            .get(&format!("{}/cache/{}", self.base_url, key))
            .send()
            .await?;

        match response.status() {
            StatusCode::OK => {
                let mut file = File::create(output_path)
                    .await
                    .map_err(|e| CacheError::Io {
                        path: output_path.to_path_buf(),
                        source: e,
                    })?;

                let mut bytes = response.bytes().await?;
                file.write_all_buf(&mut bytes)
                    .await
                    .map_err(|e| CacheError::Io {
                        path: output_path.to_path_buf(),
                        source: e,
                    })?;
                Ok(())
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

    for result in results {
        match result.status {
            UploadStatus::Success => {
                println!("✓ Successfully uploaded: {}", result.filename);
            }
            UploadStatus::Skip => {
                println!("• Skipped (already exists): {}", result.filename);
            }
            UploadStatus::Error(err) => {
                eprintln!("✗ Failed to upload {}: {:?}", result.filename, err);
            }
        }
    }
    Ok(())
}

pub async fn download(base_url: String, files: Vec<String>) -> std::io::Result<()> {
    let client = CacheClient::new(base_url);
    println!("Downloading {} files...", files.len());
    let results = client.batch_download(&files).await;
    for result in results {
        match result.status {
            DownloadStatus::Success => {
                println!("✓ Successfully downloaded: {}", result.filename);
            }
            DownloadStatus::Error(err) => {
                eprintln!("✗ Failed to download {}: {:?}", result.filename, err);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
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
            let mut file = File::create(filename).await?;
            file.write_all(*content).await?;
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
            fs::remove_file(filename)?;
        }

        // Test batch download
        let download_results = client.batch_download(&filenames).await;
        assert_eq!(download_results.len(), test_files.len());

        // Verify the downloaded files
        for ((filename, original_content), result) in test_files.iter().zip(&download_results) {
            assert!(
                matches!(result.status, DownloadStatus::Success),
                "Download failed for {}: {:?}",
                filename,
                result.status
            );

            let content = fs::read(filename)?;
            assert_eq!(&content, original_content);
        }

        Ok(())
    }
}
