# Message Queue System

A high-performance, distributed message queue system written in Rust, featuring compression, encryption, persistence, and concurrent message handling.

## Features

- **Message Compression**: Automatic compression for messages larger than 1KB
- **Message Encryption**: AES-256-GCM encryption for secure message transmission
- **Persistence**: Message storage with sled database
- **Concurrent Operations**: Support for multiple publishers and subscribers
- **Consumer Groups**: Group-based message consumption with offset tracking
- **gRPC Interface**: Modern RPC interface for client-server communication
- **Distributed Architecture**: Support for multiple nodes with leader election

## Prerequisites

- Rust (latest stable version)
- Protocol Buffers compiler (protoc)
- grpcurl (for testing with CLI)

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd mq
```

2. Build the project:
```bash
cargo build --release
```

## Running the Server

Start the server with default settings:
```bash
cargo run --release
```

Or with custom settings:
```bash
cargo run --release -- --addr "[::1]:50051" --data-dir "./data"
```

## Using the Client

### Rust Client

```rust
use mq::client::MQClient;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new client
    let mut client = MQClient::new("http://[::1]:50051".to_string()).await?;

    // Set encryption key (32 bytes for AES-256)
    let mut key = [0u8; 32];
    getrandom::getrandom(&mut key)?;
    client.set_encryption_key(key.to_vec());

    // Create a topic
    client.create_topic("secure-topic").await?;

    // Publish an encrypted message
    let mut headers = HashMap::new();
    headers.insert("key".to_string(), "value".to_string());
    client.publish(
        "secure-topic",
        b"Sensitive data".to_vec(),
        headers,
    ).await?;

    // Subscribe to messages (they will be automatically decrypted)
    let mut rx = client.subscribe("secure-topic", "secure-group").await?;
    
    // Receive and process decrypted messages
    while let Some(result) = rx.recv().await {
        match result {
            Ok(message) => {
                println!("Received: {}", String::from_utf8_lossy(&message.payload));
            }
            Err(e) => eprintln!("Error: {}", e),
        }
    }

    Ok(())
}
```

The encryption is transparent to the application - messages are automatically encrypted when published and decrypted when received, as long as the same encryption key is used.

### Using grpcurl

Create a topic:
```bash
grpcurl -plaintext -d '{"name": "test-topic"}' [::1]:50051 mq.MessageQueueService/CreateTopic
```

Publish a message:
```bash
grpcurl -plaintext -d '{
  "topic": "test-topic",
  "payload": "SGVsbG8sIFdvcmxkIQ==",
  "headers": {"key": "value"}
}' [::1]:50051 mq.MessageQueueService/Publish
```

Subscribe to messages:
```bash
grpcurl -plaintext -d '{
  "topic": "test-topic",
  "consumer_group": "test-group",
  "consumer_id": "consumer-1"
}' [::1]:50051 mq.MessageQueueService/Subscribe
```

## Testing

Run all tests:
```bash
cargo test
```

Run specific test:
```bash
cargo test test_name
```

## Project Structure

```
src/
├── client.rs         # Client implementation
├── coordinator.rs    # Distributed coordination
├── message_queue.rs  # Core message queue logic
├── server.rs        # gRPC server implementation
└── storage.rs       # Message persistence
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.