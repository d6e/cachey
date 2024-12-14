use actix_web::{web, App, Error, HttpResponse, HttpServer, Result};
use futures_util::StreamExt;
use serde::Serialize;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use num_cpus;

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

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
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

async fn put_cache(
    key: web::Path<String>,
    mut payload: web::Payload,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let file_path = get_file_path(&data.cache_dir, &key);

    // Check if file already exists
    if file_path.exists() {
        return Ok(HttpResponse::Conflict().json(ErrorResponse {
            error: "Key already exists".to_string(),
        }));
    }

    // Create cache directory if it doesn't exist
    if !data.cache_dir.exists() {
        fs::create_dir_all(&data.cache_dir).map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to create directory: {}", e))
        })?;
    }

    // Open file with async writer
    let file = tokio::fs::File::create(&file_path).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to create file: {}", e))
    })?;
    let mut writer = tokio::io::BufWriter::with_capacity(BUFFER_SIZE, file);

    // Stream the file contents
    while let Some(chunk) = payload.next().await {
        let chunk = chunk.map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to read chunk: {}", e))
        })?;
        writer.write_all(&chunk).await.map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Failed to write chunk: {}", e))
        })?;
    }

    // Ensure all data is written to disk
    writer.flush().await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to flush file: {}", e))
    })?;

    Ok(HttpResponse::Created().finish())
}

async fn get_cache(
    key: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<HttpResponse, Error> {
    let file_path = get_file_path(&data.cache_dir, &key);

    if !file_path.exists() {
        return Ok(HttpResponse::NotFound().json(ErrorResponse {
            error: "Key not found".to_string(),
        }));
    }

    // Open the file
    let file = tokio::fs::File::open(&file_path).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to open file: {}", e))
    })?;

    // Create a streaming response
    Ok(HttpResponse::Ok()
        .streaming(tokio_util::io::ReaderStream::new(file)))
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cache_dir = PathBuf::from("cache");
    let num_workers = calculate_workers();
    
    println!("Starting server at http://127.0.0.1:8080");
    println!("Cache directory: {}", cache_dir.display());
    println!("Number of workers: {}", num_workers);
    println!("Total CPU cores: {}", num_cpus::get());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                cache_dir: cache_dir.clone(),
            }))
            .app_data(web::PayloadConfig::new(10 * 1024 * 1024 * 1024))
            .service(
                web::resource("/cache/{key}")
                    .route(web::put().to(put_cache))
                    .route(web::get().to(get_cache)),
            )
    })
    .bind("127.0.0.1:8080")?
    .workers(num_workers)
    .run()
    .await
}