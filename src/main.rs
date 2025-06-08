mod coordinator;
// mod network;
mod storage;
mod transaction;
mod message_queue;
mod server;
mod client;
mod monitoring;
mod metrics;

use std::net::SocketAddr;
use std::time::Duration;
use tracing_subscriber;
use std::path::PathBuf;
use structopt::StructOpt;
use clap::Parser;
use std::error::Error;
use tokio;
use crate::server::run_server;
use crate::message_queue::MessageQueue;
use crate::monitoring::{MonitoringServer, start_monitoring_server};
use std::sync::Arc;
use actix_rt;

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

#[derive(StructOpt)]
#[structopt(name = "mq-server", about = "A distributed message queue server")]
struct Opt {
    /// The address to bind the server to
    #[structopt(short, long, default_value = "[::1]:50051")]
    addr: String,

    /// The path to store the message queue data
    #[structopt(short, long, default_value = "data/mq")]
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

    // Create monitoring server
    let monitoring_server = Arc::new(MonitoringServer::new(message_queue.clone()));

    // Start monitoring server in a separate task
    let monitoring_server_clone = monitoring_server.clone();
    actix_rt::spawn(async move {
        if let Err(e) = start_monitoring_server(monitoring_server_clone, "127.0.0.1", 8080).await {
            eprintln!("Monitoring server error: {}", e);
        }
    });

    // Start gRPC server
    println!("Starting MQ server...");
    run_server(message_queue, &args.addr).await?;

    Ok(())
}
