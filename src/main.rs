mod coordinator;
mod network;
mod storage;
mod transaction;

use std::net::SocketAddr;
use std::time::Duration;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    // Initialize components
    let storage = storage::MessageStorage::new("data")?;
    let transaction_manager = transaction::TransactionManager::new(storage, Duration::from_secs(30));
    let coordinator = coordinator::Coordinator::new();

    // Start transaction checking in the background
    let transaction_manager_clone = transaction_manager.clone();
    tokio::spawn(async move {
        transaction_manager_clone.start_checking().await;
    });

    // Start the server
    let addr = SocketAddr::from(([127, 0, 0, 1], 50051));
    network::start_server(addr, coordinator, transaction_manager).await?;

    Ok(())
}
