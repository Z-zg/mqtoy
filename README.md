# Distributed Message Queue

A high-performance, distributed message queue system written in Rust. This project implements a message queue with the following features:

- Topic-based message routing
- Consumer groups support
- Message persistence using Sled
- gRPC-based communication
- Distributed architecture with leader election
- Message ordering and delivery guarantees

## Features

- **Topic-based Routing**: Messages are organized into topics, allowing for flexible message routing
- **Consumer Groups**: Support for consumer groups with offset tracking
- **Persistence**: Messages are persisted using Sled, an embedded key-value database
- **gRPC API**: Modern, efficient communication using Protocol Buffers and gRPC
- **Distributed**: Support for distributed deployment with leader election
- **Message Ordering**: Guaranteed message ordering within topics
- **Delivery Guarantees**: At-least-once delivery semantics

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Protocol Buffers compiler (protoc)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/mq.git
cd mq
```

2. Build the project:
```bash
cargo build --release
```

3. Run the server:
```bash
./target/release/mq-server
```

### Configuration

The server can be configured using command-line arguments:

- `--addr`: The address to bind the server to (default: "[::1]:50051")
- `--data-dir`: The path to store the message queue data (default: "data/mq")

Example:
```bash
./target/release/mq-server --addr "127.0.0.1:50051" --data-dir "/var/lib/mq"
```

## API

The message queue exposes a gRPC API with the following operations:

- `Publish`: Send a message to a topic
- `Subscribe`: Subscribe to messages from a topic
- `CreateTopic`: Create a new topic
- `CommitOffset`: Commit the current offset for a consumer group

## Development

### Building

```bash
cargo build
```

### Testing

```bash
cargo test
```

### Running with Docker

```bash
docker build -t mq-server .
docker run -p 50051:50051 mq-server
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.
