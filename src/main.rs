use actix_web::{web, App, HttpResponse, HttpServer, Result};
use serde::Serialize;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::fs as tokio_fs;

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
    body: web::Bytes,
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

    // Write the file
    let mut file = fs::File::create(&file_path).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to create file: {}", e))
    })?;

    file.write_all(&body).map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to write file: {}", e))
    })?;

    Ok(HttpResponse::Created().finish())
}

async fn get_cache(
    key: web::Path<String>,
    data: web::Data<AppState>,
) -> Result<HttpResponse> {
    let file_path = get_file_path(&data.cache_dir, &key);

    if !file_path.exists() {
        return Ok(HttpResponse::NotFound().json(ErrorResponse {
            error: "Key not found".to_string(),
        }));
    }

    // Read the file
    let contents = tokio_fs::read(&file_path).await.map_err(|e| {
        actix_web::error::ErrorInternalServerError(format!("Failed to read file: {}", e))
    })?;

    Ok(HttpResponse::Ok().body(contents))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let cache_dir = PathBuf::from("cache");
    
    println!("Starting server at http://127.0.0.1:8080");
    println!("Cache directory: {}", cache_dir.display());

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(AppState {
                cache_dir: cache_dir.clone(),
            }))
            .service(
                web::resource("/cache/{key}")
                    .route(web::put().to(put_cache))
                    .route(web::get().to(get_cache)),
            )
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}