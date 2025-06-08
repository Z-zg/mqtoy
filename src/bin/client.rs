use std::collections::HashMap;
use std::error::Error;
use clap::Parser;
use tokio::time::{sleep, Duration};
use mq::client::MQClient;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "http://[::1]:50051")]
    server_addr: String,

    /// Topic name
    #[arg(short, long)]
    topic: String,

    /// Operation to perform (create, publish, subscribe)
    #[arg(short, long)]
    operation: String,

    /// Message payload (for publish operation)
    #[arg(short, long)]
    message: Option<String>,

    /// Partition number (optional)
    #[arg(short, long)]
    partition: Option<usize>,

    /// Replay offset (optional)
    #[arg(short, long)]
    replay_offset: Option<usize>,

    /// Replay timestamp (optional)
    #[arg(short, long)]
    replay_timestamp: Option<i64>,

    /// Use DLQ (Dead Letter Queue)
    #[arg(short, long)]
    dlq: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args = Args::parse();

    // Create a new client
    let mut client = MQClient::new(args.server_addr).await?;
    println!("Connected to message queue server");

    match args.operation.as_str() {
        "create" => {
            client.create_topic(&args.topic).await?;
            println!("Created topic: {}", args.topic);
        }
        "publish" => {
            if let Some(message) = args.message {
                let headers = HashMap::new();
                let message_id = client
                    .publish(&args.topic, message.as_bytes().to_vec(), headers, Some(args.dlq))
                    .await?;
                println!("Published message with ID: {}", message_id);
            } else {
                println!("Error: Message payload required for publish operation");
            }
        }
        "subscribe" => {
            let mut rx = client
                .subscribe(
                    &args.topic,
                    args.partition,
                    args.replay_offset,
                    args.replay_timestamp,
                    Some(args.dlq),
                )
                .await?;
            println!("Subscribed to topic: {}", args.topic);

            while let Some(result) = rx.recv().await {
                match result {
                    Ok(message) => {
                        println!(
                            "Received message: {}",
                            String::from_utf8_lossy(&message.payload)
                        );
                    }
                    Err(e) => {
                        eprintln!("Error receiving message: {}", e);
                    }
                }
            }
        }
        _ => {
            println!("Unknown operation: {}", args.operation);
            println!("Available operations: create, publish, subscribe");
        }
    }

    Ok(())
} 