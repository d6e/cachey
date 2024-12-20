use actix_web::{web, App, Error, HttpResponse, HttpServer, Result};
use futures_util::StreamExt;
use log::{error, info, warn};
use num_cpus;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

const BUFFER_SIZE: usize = 64 * 1024; // 64KB chunks for streaming

// Function to determine optimal number of workers
fn calculate_workers() -> usize {
    let cpu_count = num_cpus::get();

    // Reserve at least one CPU for the OS and other processes
    let available_cpus = if cpu_count > 1 { cpu_count - 1 } else { 1 };

    // Cap the maximum number of workers
    // This prevents excessive resource usage on many-core systems
    let max_workers = 32;

    // Use the minimum of available CPUs and max_workers
    // Ensure at least one worker
    std::cmp::min(available_cpus, max_workers).max(1)
}

struct AppState {
    cache_dir: PathBuf,
}

fn sanitize_key(key: &str) -> String {
    // Remove any path separators and potentially dangerous characters
    key.replace(['/', '\\', ':', '*', '?', '"', '<', '>', '|'], "_")
}

fn get_file_path(cache_dir: &Path, key: &str) -> PathBuf {
    cache_dir.join(sanitize_key(key))
}

async fn create_cache_dir_if_needed(cache_dir: &Path) -> Result<(), std::io::Error> {
    match tokio::fs::metadata(cache_dir).await {
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            tokio::fs::create_dir_all(cache_dir).await
        }
        Err(e) => Err(e),
    }
}

async fn put_cache(
    key: web::Path<String>,
    mut payload: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let file_path = get_file_path(&data.cache_dir, &key);

    // Create cache directory if it doesn't exist
    if let Err(e) = create_cache_dir_if_needed(&data.cache_dir).await {
        error!("Failed to create directory for key {}: {}", key, e);
        return Err(actix_web::error::ErrorInternalServerError(format!(
            "Failed to create directory: {}",
            e
        )));
    }

    // Try to create the file with OpenOptions to handle race conditions
    let file = match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true) // This ensures atomic create-if-not-exists
        .open(&file_path)
        .await
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            warn!("PUT conflict - key already exists: {}", key);
            return Ok(HttpResponse::Conflict()
                .content_type("text/plain")
                .body("Key already exists"));
        }
        Err(e) => {
            error!("Failed to create file for key {}: {}", key, e);
            return Err(actix_web::error::ErrorInternalServerError(format!(
                "Failed to create file: {}",
                e
            )));
        }
    };

    let mut writer = tokio::io::BufWriter::with_capacity(BUFFER_SIZE, file);

    // Stream the file contents
    let mut total_bytes = 0;
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| {
            error!("Failed to read chunk for key {}: {}", key, e);
            actix_web::error::ErrorInternalServerError(format!("Failed to read chunk: {}", e))
        })?;

        total_bytes += chunk.len();
        writer.write_all(&chunk).await.map_err(|e| {
            error!("Failed to write chunk for key {}: {}", key, e);
            actix_web::error::ErrorInternalServerError(format!("Failed to write chunk: {}", e))
        })?;
    }

    // Ensure all data is written to disk
    writer.flush().await.map_err(|e| {
        error!("Failed to flush file for key {}: {}", key, e);
        actix_web::error::ErrorInternalServerError(format!("Failed to flush file: {}", e))
    })?;

    info!("Successfully stored key {} ({} bytes)", key, total_bytes);
    Ok(HttpResponse::Created().finish())
}

async fn head_cache(
    key: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let file_path = get_file_path(&data.cache_dir, &key);

    match tokio::fs::metadata(&file_path).await {
        Ok(metadata) => Ok(HttpResponse::Ok()
            .append_header(("content-length", metadata.len().to_string()))
            .finish()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(HttpResponse::NotFound().finish()),
        Err(e) => {
            error!("Failed to read metadata for key {}: {}", key, e);
            Err(actix_web::error::ErrorInternalServerError(format!(
                "Failed to read file metadata: {}",
                e
            )))
        }
    }
}

async fn get_cache(
    key: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let file_path = get_file_path(&data.cache_dir, &key);

    // Get metadata and handle not found case
    let metadata = match tokio::fs::metadata(&file_path).await {
        Ok(m) => m,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            warn!("GET not found - key: {}", key);
            return Ok(HttpResponse::NotFound()
                .content_type("text/plain")
                .body("Key not found"));
        }
        Err(e) => {
            error!("Failed to read metadata for key {}: {}", key, e);
            return Err(actix_web::error::ErrorInternalServerError(format!(
                "Failed to read file metadata: {}",
                e
            )));
        }
    };

    // Open the file
    let file = match tokio::fs::File::open(&file_path).await {
        Ok(f) => f,
        Err(e) => {
            error!("Failed to open file for key {}: {}", key, e);
            return Err(actix_web::error::ErrorInternalServerError(format!(
                "Failed to open file: {}",
                e
            )));
        }
    };

    info!(
        "Successfully retrieved key {} ({} bytes)",
        key,
        metadata.len()
    );
    Ok(HttpResponse::Ok().streaming(tokio_util::io::ReaderStream::new(file)))
}

pub async fn run(base_url: String) -> std::io::Result<()> {
    // Initialize the logger with environment variables
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let url_no_scheme = base_url.replace("https://", "").replace("http://", "");
    let cache_dir = PathBuf::from("cachey_cache");
    let num_workers = calculate_workers();

    info!("Starting server at {}", url_no_scheme);
    info!("Cache directory: {}", cache_dir.display());
    info!("Number of workers: {}", num_workers);
    info!("Total CPU cores: {}", num_cpus::get());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                cache_dir: cache_dir.clone(),
            }))
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024 * 1024))
            .service(
                web::resource("/cache/{key}")
                    .route(web::put().to(put_cache))
                    .route(web::get().to(get_cache))
                    .route(web::head().to(head_cache)),
            )
    })
    .bind(url_no_scheme)?
    .workers(num_workers)
    .run()
    .await
}
