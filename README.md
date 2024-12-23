# Cachey

Cachey is a simple file caching system that lets you store files on a remote server and retrieve them across multiple machines. It's useful for sharing build artifacts, dependencies, or any other files that need to be accessed by multiple computers.

## Installation

### From Source
```bash
cargo install --path .
```

### From Release
Download the latest release for your platform from the [releases page](https://github.com/yourusername/cachey/releases).

## Usage

### Starting the Server
```bash
# Start server on default port (8080)
cachey server

# Start on custom port/interface
cachey --url 0.0.0.0:9000 server
```

### Using the Client

Upload files:
```bash
# Upload files
cachey client upload file1.txt file2.txt

# Upload with detailed progress
cachey client upload -v file1.txt file2.txt
```

Download files:
```bash
# Download files
cachey client download file1.txt file2.txt

# Download with detailed progress
cachey client download -v file1.txt file2.txt
```

### Options
- `-u, --url <URL>` - Set server URL (default: http://127.0.0.1:8080)
- `-v, --verbose` - Show detailed progress information
- `--help` - Show help information

### Running as a Service

To run the server as a system service:

1. Copy the service file:
```bash
sudo cp cachey.service /etc/systemd/system/
```

2. Start the service:
```bash
sudo systemctl enable cachey
sudo systemctl start cachey
```
