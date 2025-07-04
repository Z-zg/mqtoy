use std::collections::HashMap;
use tokio::sync::mpsc;
use tonic::transport::Channel;
use uuid::Uuid;
use std::error::Error as StdError;
use std::sync::Arc;

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
    encryption_key: Option<Vec<u8>>,
}

impl MQClient {
    pub async fn new(addr: String) -> Result<Self, Box<dyn StdError + Send + Sync>> {
        let client = MessageQueueServiceClient::connect(addr).await?;
        let consumer_id = Uuid::new_v4().to_string();
        Ok(Self {
            client,
            consumer_id,
            encryption_key: None,
        })
    }

    pub fn set_encryption_key(&mut self, key: Vec<u8>) {
        self.encryption_key = Some(key);
    }

    pub async fn create_topic(&mut self, name: &str) -> Result<bool, Box<dyn StdError + Send + Sync>> {
        let request = CreateTopicRequest {
            name: name.to_string(),
            partition_count: Some(3), // Default to 3 partitions
            max_age_seconds: None,    // Use default retention policy
            max_size_bytes: None,     // Use default retention policy
        };
        let response = self.client.create_topic(request).await?;
        Ok(response.into_inner().success)
    }

    pub async fn publish(
        &mut self,
        topic: &str,
        payload: Vec<u8>,
        headers: HashMap<String, String>,
        dlq: Option<bool>,
    ) -> Result<String, Box<dyn StdError + Send + Sync>> {
        let mut message = Message {
            id: Uuid::new_v4(),
            topic: topic.to_string(),
            payload,
            timestamp: chrono::Utc::now().timestamp(),
            headers,
            is_compressed: false,
            is_encrypted: false,
        };

        // Encrypt if key is set
        if let Some(ref key) = self.encryption_key {
            message.encrypt(key)
                .map_err(|e| Box::new(e) as Box<dyn StdError + Send + Sync>)?;
        }

        let request = PublishRequest {
            topic: topic.to_string(),
            payload: message.payload,
            headers: message.headers,
            partition: None, // Let server choose partition
            dlq,
        };
        let response = self.client.publish(request).await?;
        Ok(response.into_inner().message_id)
    }

    pub async fn subscribe(
        &mut self,
        topic: &str,
        partition: Option<usize>,
        replay_offset: Option<usize>,
        replay_timestamp: Option<i64>,
        dlq: Option<bool>,
    ) -> Result<mpsc::Receiver<Result<Message, Box<dyn StdError + Send + Sync>>>, Box<dyn StdError + Send + Sync>> {
        let request = SubscribeRequest {
            topic: topic.to_string(),
            partition: partition.map(|p| p as u32),
            replay_offset: replay_offset.map(|o| o as u64),
            replay_timestamp,
            dlq,
        };

        let response = self.client.subscribe(request).await?;
        let mut stream = response.into_inner();
        let encryption_key = self.encryption_key.clone();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            while let Ok(Some(msg)) = stream.message().await {
                let mut message = Message {
                    id: Uuid::parse_str(&msg.id).unwrap(),
                    topic: msg.topic,
                    payload: msg.payload,
                    timestamp: msg.timestamp,
                    headers: msg.headers.clone(),
                    is_compressed: msg.headers.contains_key("compression"),
                    is_encrypted: msg.headers.contains_key("encryption"),
                };

                // Decrypt if encrypted and key is set
                if message.is_encrypted {
                    if let Some(ref key) = encryption_key {
                        if let Err(e) = message.decrypt(key) {
                            if tx.send(Err(Box::new(e) as Box<dyn StdError + Send + Sync>)).await.is_err() {
                                break;
                            }
                            continue;
                        }
                    }
                }

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
        partition: usize,
        offset: u64,
    ) -> Result<bool, Box<dyn StdError + Send + Sync>> {
        let request = CommitOffsetRequest {
            topic: topic.to_string(),
            partition: partition as u32,
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
    use tokio::task::JoinHandle;
    use crate::message_queue::MessageQueue;
    use crate::server::run_server;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::net::TcpListener;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicU16;

    static PORT: AtomicU16 = AtomicU16::new(50051);

    fn get_next_port() -> String {
        let port = PORT.fetch_add(1, Ordering::SeqCst);
        format!("127.0.0.1:{}", port)
    }

    async fn start_test_server() -> (String, JoinHandle<()>) {
        let addr = get_next_port();
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_string();
        let message_queue = MessageQueue::new(&storage_path).unwrap();
        let addr_clone = addr.clone();
        
        let server_handle = tokio::spawn(async move {
            println!("Starting test server on {}...", addr_clone);
            run_server(Arc::new(message_queue), &addr_clone).await.unwrap();
        });

        // 等待服务器启动
        let mut retries = 0;
        while retries < 10 {
            match MQClient::new(format!("http://{}", addr)).await {
                Ok(mut client) => {
                    // 尝试创建主题来验证服务器是否完全就绪
                    if client.create_topic("test-ready").await.is_ok() {
                        break;
                    }
                }
                Err(e) => {
                    println!("Server not ready: {:?}", e);
                    sleep(Duration::from_millis(500)).await;
                    retries += 1;
                }
            }
        }
        assert!(retries < 10, "Server failed to start");
        (addr, server_handle)
    }

    async fn setup_test_client() -> (MQClient, JoinHandle<()>) {
        let (addr, server_handle) = start_test_server().await;
        
        // 等待服务器完全就绪
        sleep(Duration::from_secs(2)).await;
        
        // 尝试连接
        let mut retries = 0;
        while retries < 5 {
            match MQClient::new(format!("http://{}", addr)).await {
                Ok(mut client) => {
                    // 验证连接是否真的可用
                    if client.create_topic("test-connection").await.is_ok() {
                        return (client, server_handle);
                    }
                }
                Err(e) => {
                    println!("Failed to connect: {:?}", e);
                    sleep(Duration::from_secs(2)).await;
                    retries += 1;
                }
            }
        }
        panic!("Failed to create client after multiple retries");
    }

    #[tokio::test]
    async fn test_client_basic_operations() {
        let (mut client, server_handle) = setup_test_client().await;

        // Create topic
        let result = client.create_topic("test-topic").await;
        assert!(result.is_ok(), "Failed to create topic: {:?}", result.err());

        // Publish message
        let mut headers = HashMap::new();
        headers.insert("key".to_string(), "value".to_string());
        let message_id = client
            .publish(
                "test-topic",
                b"Hello, World!".to_vec(),
                headers.clone(),
                None,
            )
            .await
            .expect("Failed to publish message");
        assert!(!message_id.is_empty());

        // Subscribe to messages
        let mut rx = client
            .subscribe("test-topic", None, None, None, None)
            .await
            .expect("Failed to subscribe");

        // Receive message
        let message = rx.recv().await
            .expect("Failed to receive message")
            .expect("Received error message");
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.payload, b"Hello, World!");
        assert_eq!(message.headers.get("key").unwrap(), "value");

        // Commit offset
        let result = client.commit_offset("test-topic", 0, 1).await;
        assert!(result.is_ok(), "Failed to commit offset: {:?}", result.err());

        // 清理服务器
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_client_error_handling() {
        let (mut client, server_handle) = setup_test_client().await;

        // Try to publish to non-existent topic
        let result = client
            .publish(
                "non-existent",
                b"test".to_vec(),
                HashMap::new(),
                None,
            )
            .await;
        assert!(result.is_err());

        // Try to subscribe to non-existent topic
        let result = client.subscribe("non-existent", None, None, None, None).await;
        assert!(result.is_err());

        // 清理服务器
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_message_compression() {
        let (mut client, server_handle) = setup_test_client().await;
        let result = client.create_topic("compression-test").await;
        assert!(result.is_ok(), "Failed to create topic: {:?}", result.err());

        // Publish a large message that should be compressed
        let large_payload = vec![0u8; 2048]; // 2KB payload
        let message_id = client
            .publish(
                "compression-test",
                large_payload.clone(),
                HashMap::new(),
                None,
            )
            .await
            .expect("Failed to publish message");
        assert!(!message_id.is_empty());

        // Subscribe and verify the message
        let mut rx = client
            .subscribe("compression-test", None, None, None, None)
            .await
            .expect("Failed to subscribe");

        let message = rx.recv().await
            .expect("Failed to receive message")
            .expect("Received error message");
        assert_eq!(message.topic, "compression-test");
        assert_eq!(message.payload.len(), large_payload.len()); // Length should match after decompression
        assert!(!message.is_compressed); // Should be decompressed by the time we receive it

        // 清理服务器
        server_handle.abort();
    }

    #[tokio::test]
    async fn test_message_encryption() {
        let (mut client, server_handle) = setup_test_client().await;
        let result = client.create_topic("encryption-test").await;
        assert!(result.is_ok(), "Failed to create topic: {:?}", result.err());

        // Generate a random key
        let mut key = [0u8; 32];
        getrandom::getrandom(&mut key).unwrap();
        client.set_encryption_key(key.to_vec());

        // Publish an encrypted message
        let payload = b"Hello, encrypted world!".to_vec();
        let message_id = client
            .publish(
                "encryption-test",
                payload.clone(),
                HashMap::new(),
                None,
            )
            .await
            .expect("Failed to publish message");
        assert!(!message_id.is_empty());

        // Subscribe and verify the message
        let mut rx = client
            .subscribe("encryption-test", None, None, None, None)
            .await
            .expect("Failed to subscribe");

        let message = rx.recv().await
            .expect("Failed to receive message")
            .expect("Received error message");
        assert_eq!(message.topic, "encryption-test");
        assert_eq!(message.payload, payload);
        assert!(!message.is_encrypted); // Should be decrypted by the time we receive it

        // 清理服务器
        server_handle.abort();
    }
} 