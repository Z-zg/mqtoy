# Rust Message Queue (MQ)

A high-performance, distributed message queue system written in Rust, featuring gRPC support, message compression, encryption, and monitoring capabilities.

## Features

- **gRPC-based Communication**: Fast and efficient message passing using gRPC
- **Message Compression**: Automatic compression for large messages
- **Message Encryption**: AES-256-GCM encryption support
- **Dead Letter Queues (DLQ)**: Handle failed message processing
- **Message Replay**: Support for replaying messages from specific offsets or timestamps
- **Monitoring Dashboard**: REST API and Prometheus metrics
- **Retention Policies**: Configurable message retention based on age and size
- **Partitioning**: Topic partitioning for better scalability
- **Concurrent Operations**: Thread-safe message handling

## Prerequisites

- Rust 1.70 or higher
- Protocol Buffers compiler (protoc)
- gRPC tools (grpcurl for testing)

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/mq.git
cd mq

# Build the project
cargo build --release
```

## Usage

### Starting the Server

```bash
# Start with default settings
cargo run --release

# Start with custom settings
cargo run --release -- --addr "[::1]:50051" --data-dir "data/mq"
```

### Using the Client

```rust
use mq::client::MQClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new client
    let mut client = MQClient::new("http://[::1]:50051".to_string()).await?;

    // Create a topic
    client.create_topic("my-topic").await?;

    // Publish a message
    let message = "Hello, World!".as_bytes().to_vec();
    let headers = std::collections::HashMap::new();
    client.publish("my-topic", message, headers, None).await?;

    // Subscribe to messages
    let mut rx = client.subscribe("my-topic", None, None, None, None).await?;
    while let Some(result) = rx.recv().await {
        match result {
            Ok(message) => println!("Received: {:?}", message),
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}
```

### Monitoring

The server exposes two monitoring endpoints:

1. REST API (default: http://127.0.0.1:8080):
   - `GET /metrics`: System-wide metrics
   - `GET /metrics/topic/{topic_name}`: Topic-specific metrics

2. Prometheus Metrics (default: http://127.0.0.1:8080/prometheus)

## Testing

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name
```

## Configuration

The server can be configured using command-line arguments:

- `--addr`: Server address (default: "[::1]:50051")
- `--data-dir`: Data directory (default: "data/mq")

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.