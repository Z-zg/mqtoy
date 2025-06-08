use std::path::PathBuf;
use clap::Parser;
use std::error::Error;
use tokio;
use mq::server::run_server;
use mq::message_queue::MessageQueue;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Server address
    #[arg(short, long, default_value = "[::1]:50051")]
    addr: String,

    /// Data directory
    #[arg(short, long, default_value = "data/mq")]
    data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(&args.data_dir)?;

    // Initialize message queue
    let storage_path = args.data_dir.to_str().unwrap();
    let message_queue = Arc::new(MessageQueue::new(storage_path)?);

    // Start gRPC server
    println!("Starting MQ server on {}...", args.addr);
    run_server(message_queue, &args.addr).await?;

    Ok(())
} 