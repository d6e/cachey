use clap::{Arg, Command};
mod client;
mod server;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let matches = Command::new("cachey")
        .about("An arbitrary file caching system")
        .arg(
            Arg::new("base-url")
                .short('b')
                .long("base-url")
                .default_value("http://localhost:8080")
                .help("Base URL for the cache server")
        )
        .subcommand(
            Command::new("client")
                .about("Client operations")
                .subcommand(
                    Command::new("upload")
                        .about("Upload files to cache")
                        .arg(
                            Arg::new("files")
                                .help("Files to upload")
                                .required(true)
                                .num_args(1..)
                        )
                )
                .subcommand(
                    Command::new("download")
                        .about("Download files from cache")
                        .arg(
                            Arg::new("files")
                                .help("Files to download")
                                .required(true)
                                .num_args(1..)
                        )
                )
        )
        .subcommand(
            Command::new("server")
                .about("Server operations")
        )
        .get_matches();

    let base_url = matches.get_one::<String>("base-url").unwrap().to_string();

    match matches.subcommand() {
        Some(("client", client_matches)) => {
            match client_matches.subcommand() {
                Some(("upload", upload_matches)) => {
                    let files: Vec<String> = upload_matches.get_many::<String>("files")
                        .unwrap()
                        .map(|s| s.to_string())
                        .collect();
                    client::upload(base_url, files).await
                }
                Some(("download", download_matches)) => {
                    let files: Vec<String> = download_matches.get_many::<String>("files")
                        .unwrap()
                        .map(|s| s.to_string())
                        .collect();
                    client::download(base_url, files).await
                }
                _ => Ok(()),
            }
        }
        Some(("server", _)) => server::run(base_url).await,
        _ => Ok(()),
    }
}