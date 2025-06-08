use std::collections::HashMap;
use std::error::Error;
use tokio::time::{sleep, Duration};

use mq::client::MQClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Create a new client
    let mut client = MQClient::new("http://[::1]:50051".to_string()).await?;
    println!("Connected to message queue server");

    // Create a topic
    let topic = "example-topic";
    client.create_topic(topic).await?;
    println!("Created topic: {}", topic);

    // Subscribe to messages
    let mut rx = client.subscribe(topic, None, None, None, None).await?;
    println!("Subscribed to topic: {}", topic);

    // Spawn a task to receive messages
    let mut received_count = 0;
    tokio::spawn(async move {
        while let Some(result) = rx.recv().await {
            match result {
                Ok(message) => {
                    println!(
                        "Received message: {}",
                        String::from_utf8_lossy(&message.payload)
                    );
                    received_count += 1;
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                }
            }
        }
    });

    // Publish some messages
    for i in 0..5 {
        let mut headers = HashMap::new();
        headers.insert("sequence".to_string(), i.to_string());
        
        let message = format!("Hello, World! Message {}", i);
        let message_id = client
            .publish(topic, message.as_bytes().to_vec(), headers, None)
            .await?;
        
        println!("Published message {} with ID: {}", i, message_id);
        sleep(Duration::from_millis(100)).await;
    }

    // Wait for messages to be received
    sleep(Duration::from_secs(1)).await;
    println!("Received {} messages", received_count);

    // Commit offset
    client.commit_offset(topic, 0, received_count as u64).await?;
    println!("Committed offset: {}", received_count);

    Ok(())
} 