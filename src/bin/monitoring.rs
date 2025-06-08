use std::path::PathBuf;
use clap::Parser;
use std::error::Error;
use tokio;
use mq::monitoring::{MonitoringServer, start_monitoring_server};
use mq::message_queue::MessageQueue;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Monitoring server host
    #[arg(short, long, default_value = "127.0.0.1")]
    host: String,

    /// Monitoring server port
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Data directory
    #[arg(short, long, default_value = "data/mq")]
    data_dir: PathBuf,

    /// Update interval in seconds
    #[arg(short, long, default_value = "5")]
    update_interval: u64,
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

    // Create monitoring server
    let monitoring_server = Arc::new(MonitoringServer::new(message_queue));

    // Start monitoring server
    println!("Starting monitoring server at http://{}:{}", args.host, args.port);
    println!("Metrics will be updated every {} seconds", args.update_interval);
    start_monitoring_server(monitoring_server, &args.host, args.port).await?;

    Ok(())
} 