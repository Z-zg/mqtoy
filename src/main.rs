mod coordinator;
// mod network;
mod storage;
mod transaction;
mod message_queue;
mod server;

use std::net::SocketAddr;
use std::time::Duration;
use tracing_subscriber;
use std::path::PathBuf;
use structopt::StructOpt;

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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let opt = Opt::from_args();

    // Create data directory if it doesn't exist
    std::fs::create_dir_all(&opt.data_dir)?;

    // Initialize message queue
    let message_queue = message_queue::MessageQueue::new(
        opt.data_dir.to_str().unwrap(),
    )?;

    // Run the server
    server::run_server(message_queue, &opt.addr).await?;

    Ok(())
}
