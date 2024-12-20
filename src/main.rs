use cachey::{client, server};
use clap::{Arg, Command};

fn build_cli() -> Command {
    Command::new("cachey")
        .about("An arbitrary file caching system")
        .arg(
            Arg::new("url")
                .short('u')
                .long("url")
                .default_value("http://127.0.0.1:8080")
                .help("Base URL for the cache server"),
        )
        .subcommand(
            Command::new("client")
                .about("Client operations")
                .subcommand(
                    Command::new("upload").about("Upload files to cache").arg(
                        Arg::new("files")
                            .help("Files to upload")
                            .required(true)
                            .num_args(1..),
                    ),
                )
                .subcommand(
                    Command::new("download")
                        .about("Download files from cache")
                        .arg(
                            Arg::new("files")
                                .help("Files to download")
                                .required(true)
                                .num_args(1..),
                        ),
                ),
        )
        .subcommand(Command::new("server").about("Server operations"))
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut main_cmd = build_cli();
    let matches = main_cmd.clone().get_matches();

    let base_url = matches.get_one::<String>("url").unwrap().clone();

    match matches.subcommand() {
        Some(("client", client_matches)) => {
            match client_matches.subcommand() {
                Some(("upload", upload_matches)) => {
                    let files: Vec<_> = upload_matches
                        .get_many::<String>("files")
                        .unwrap()
                        .cloned()
                        .collect();
                    client::upload(base_url, files).await
                }
                Some(("download", download_matches)) => {
                    let files: Vec<_> = download_matches
                        .get_many::<String>("files")
                        .unwrap()
                        .cloned()
                        .collect();
                    client::download(base_url, files).await
                }
                // If 'client' is used without specifying upload/download,
                // print the help for the 'client' subcommand.
                _ => {
                    let mut client_cmd = build_cli();
                    client_cmd
                        .find_subcommand_mut("client")
                        .unwrap()
                        .print_help()?;
                    println!();
                    Ok(())
                }
            }
        }
        Some(("server", _)) => {
            // If server had sub-subcommands, handle them here
            // For now, just run the server.
            server::run(base_url).await
        }
        // If no subcommand is provided at all, print the main help.
        _ => {
            main_cmd.print_help()?;
            println!();
            Ok(())
        }
    }
}
