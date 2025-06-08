use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use uuid::Uuid;
use std::error::Error as StdError;

use crate::message_queue::Message;
use mq::message_queue_service_client::MessageQueueServiceClient;
use mq::{
    CommitOffsetRequest, CreateTopicRequest, PublishRequest, SubscribeRequest,
};

pub mod mq {
    tonic::include_proto!("mq");
}

#[derive(Clone)]
pub struct MQClient {
    client: MessageQueueServiceClient<Channel>,
    consumer_id: String,
}

impl MQClient {
    pub async fn new(addr: String) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        let client = MessageQueueServiceClient::connect(addr).await?;
        let consumer_id = Uuid::new_v4().to_string();
        Ok(Self {
            client,
            consumer_id,
        })
    }

    pub async fn create_topic(&mut self, name: &str) -> Result<bool, Box<dyn StdError + Send + Sync>> {
        let request = CreateTopicRequest {
            name: name.to_string(),
        };
        let response = self.client.create_topic(request).await?;
        Ok(response.into_inner().success)
    }

    pub async fn publish(
        &mut self,
        topic: &str,
        payload: Vec<u8>,
        headers: HashMap<String, String>,
    ) -> Result<String, Box<dyn StdError + Send + Sync>> {
        let request = PublishRequest {
            topic: topic.to_string(),
            payload,
            headers,
        };
        let response = self.client.publish(request).await?;
        Ok(response.into_inner().message_id)
    }

    pub async fn subscribe(
        &mut self,
        topic: &str,
        consumer_group: &str,
    ) -> Result<mpsc::Receiver<Result<Message, Box<dyn StdError + Send + Sync>>>, Box<dyn StdError + Send + Sync>> {
        let request = SubscribeRequest {
            topic: topic.to_string(),
            consumer_group: consumer_group.to_string(),
            consumer_id: self.consumer_id.clone(),
        };

        let response = self.client.subscribe(request).await?;
        let mut stream = response.into_inner();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                let message = Message {
                    id: Uuid::parse_str(&msg.id).unwrap(),
                    topic: msg.topic,
                    payload: msg.payload,
                    timestamp: msg.timestamp,
                    headers: msg.headers.clone(),
                    is_compressed: msg.headers.contains_key("compression"),
                };
                if tx.send(Ok(message)).await.is_err() {
                    break;
                }
            }
        });

        Ok(rx)
    }

    pub async fn commit_offset(
        &mut self,
        topic: &str,
        consumer_group: &str,
        offset: u64,
    ) -> Result<bool, Box<dyn StdError + Send + Sync>> {
        let request = CommitOffsetRequest {
            topic: topic.to_string(),
            consumer_group: consumer_group.to_string(),
            offset,
        };
        let response = self.client.commit_offset(request).await?;
        Ok(response.into_inner().success)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};
    use std::sync::Once;
    use tokio::task::JoinHandle;
    use crate::message_queue::MessageQueue;
    use crate::server::run_server;
    use std::sync::atomic::{AtomicBool, Ordering};

    static INIT: Once = Once::new();
    static mut SERVER_HANDLE: Option<JoinHandle<()>> = None;
    static SERVER_READY: AtomicBool = AtomicBool::new(false);

    async fn start_test_server() {
        INIT.call_once(|| {
            let dir = tempfile::tempdir().unwrap();
            let storage_path = dir.path().to_str().unwrap().to_string();
            let message_queue = MessageQueue::new(&storage_path).unwrap();
            
            let server_handle = tokio::spawn(async move {
                println!("Starting test server...");
                run_server(message_queue, "[::1]:50051").await.unwrap();
            });
            
            unsafe {
                SERVER_HANDLE = Some(server_handle);
            }
        });

        // Wait for server to be ready
        let mut retries = 0;
        while !SERVER_READY.load(Ordering::Relaxed) && retries < 10 {
            match MQClient::new("http://[::1]:50051".to_string()).await {
                Ok(_) => {
                    // Try to create a topic to ensure server is fully operational
                    let mut client = MQClient::new("http://[::1]:50051".to_string()).await.unwrap();
                    if client.create_topic("test-ready").await.is_ok() {
                        SERVER_READY.store(true, Ordering::Relaxed);
                        break;
                    }
                }
                Err(_) => {
                    sleep(Duration::from_millis(100)).await;
                    retries += 1;
                }
            }
        }
        assert!(SERVER_READY.load(Ordering::Relaxed), "Server failed to start");
    }

    async fn setup_test_client() -> MQClient {
        start_test_server().await;
        MQClient::new("http://[::1]:50051".to_string())
            .await
            .expect("Failed to create client")
    }

    #[tokio::test]
    async fn test_client_basic_operations() {
        let mut client = setup_test_client().await;

        // Create topic
        let result = client.create_topic("test-topic").await;
        assert!(result.unwrap());

        // Publish message
        let mut headers = HashMap::new();
        headers.insert("key".to_string(), "value".to_string());
        let message_id = client
            .publish(
                "test-topic",
                b"Hello, World!".to_vec(),
                headers.clone(),
            )
            .await
            .unwrap();
        assert!(!message_id.is_empty());

        // Subscribe to messages
        let mut rx = client
            .subscribe("test-topic", "test-group")
            .await
            .unwrap();

        // Receive message
        let message = rx.recv().await.unwrap().unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.payload, b"Hello, World!");
        assert_eq!(message.headers.get("key").unwrap(), "value");

        // Commit offset
        let result = client.commit_offset("test-topic", "test-group", 1).await;
        assert!(result.unwrap());
    }

    #[tokio::test]
    async fn test_client_error_handling() {
        let mut client = setup_test_client().await;

        // Try to publish to non-existent topic
        let result = client
            .publish(
                "non-existent",
                b"test".to_vec(),
                HashMap::new(),
            )
            .await;
        assert!(result.is_err());

        // Try to subscribe to non-existent topic
        let result = client.subscribe("non-existent", "test-group").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_client_concurrent_operations() {
        let mut client = setup_test_client().await;
        let topic = format!("test-topic-{}", uuid::Uuid::new_v4());
        let group = format!("test-group-{}", uuid::Uuid::new_v4());
        client.create_topic(&topic).await.unwrap();

        // Spawn multiple publishers
        let mut handles = vec![];
        for i in 0..5 {
            let mut client = client.clone();
            let topic = topic.clone();
            let handle = tokio::spawn(async move {
                let message = format!("Message {}", i);
                client
                    .publish(
                        &topic,
                        message.as_bytes().to_vec(),
                        HashMap::new(),
                    )
                    .await
            });
            handles.push(handle);
        }

        // Wait for all publishers to complete
        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Subscribe and verify messages
        let mut rx = client
            .subscribe(&topic, &group)
            .await
            .unwrap();

        let mut received_messages = Vec::new();
        for _ in 0..5 {
            if let Ok(message) = rx.recv().await.unwrap() {
                received_messages.push(String::from_utf8(message.payload).unwrap());
            }
        }

        assert_eq!(received_messages.len(), 5);
        // Sort both vectors to ensure order doesn't matter
        received_messages.sort();
        let mut expected_messages: Vec<String> = (0..5).map(|i| format!("Message {}", i)).collect();
        expected_messages.sort();
        assert_eq!(received_messages, expected_messages);
    }

    #[tokio::test]
    async fn test_message_compression() {
        let mut client = setup_test_client().await;
        client.create_topic("compression-test").await.unwrap();

        // Publish a large message that should be compressed
        let large_payload = vec![0u8; 2048]; // 2KB payload
        let message_id = client
            .publish(
                "compression-test",
                large_payload.clone(),
                HashMap::new(),
            )
            .await
            .unwrap();

        // Subscribe and verify the message
        let mut rx = client
            .subscribe("compression-test", "test-group")
            .await
            .unwrap();

        let message = rx.recv().await.unwrap().unwrap();
        assert_eq!(message.topic, "compression-test");
        assert_eq!(message.payload.len(), large_payload.len()); // Length should match after decompression
        assert!(!message.is_compressed); // Should be decompressed by the time we receive it
    }
} 