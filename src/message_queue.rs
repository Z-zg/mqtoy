use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use thiserror::Error;
use flate2::Compression;
use std::io::Write;
use flate2::write::ZlibEncoder;
use flate2::read::ZlibDecoder;
use std::io::Read;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};

#[derive(Debug, Error)]
pub enum MQError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Consumer group not found: {0}")]
    ConsumerGroupNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(String),
    #[error("Compression error: {0}")]
    CompressionError(String),
    #[error("Encryption error: {0}")]
    EncryptionError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    #[serde(with = "uuid::serde::compact")]
    pub id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub headers: HashMap<String, String>,
    pub is_compressed: bool,
    pub is_encrypted: bool,
}

impl Message {
    pub fn new(topic: String, payload: Vec<u8>, headers: HashMap<String, String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            topic,
            payload,
            timestamp: chrono::Utc::now().timestamp(),
            headers,
            is_compressed: false,
            is_encrypted: false,
        }
    }

    pub fn compress(&mut self) -> Result<(), MQError> {
        if self.is_compressed {
            return Ok(());
        }
        if self.payload.len() <= 1024 {
            // Do not compress small messages
            return Ok(());
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::best());
        encoder.write_all(&self.payload)
            .map_err(|e| MQError::CompressionError(format!("Failed to compress message: {}", e)))?;
        let compressed = encoder.finish()
            .map_err(|e| MQError::CompressionError(format!("Failed to finish compression: {}", e)))?;

        if compressed.len() < self.payload.len() {
            self.payload = compressed;
            self.is_compressed = true;
            self.headers.insert("compression".to_string(), "deflate".to_string());
        }
        Ok(())
    }

    pub fn decompress(&mut self) -> Result<(), MQError> {
        if !self.is_compressed {
            return Ok(());
        }

        let mut decoder = ZlibDecoder::new(&self.payload[..]);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)
            .map_err(|e| MQError::CompressionError(format!("Failed to decompress message: {}", e)))?;

        self.payload = decompressed;
        self.is_compressed = false;
        self.headers.remove("compression");
        Ok(())
    }

    pub fn encrypt(&mut self, key: &[u8]) -> Result<(), MQError> {
        if self.is_encrypted {
            return Ok(());
        }

        // Generate a random nonce
        let mut nonce_bytes = [0u8; 12];
        getrandom::getrandom(&mut nonce_bytes)
            .map_err(|e| MQError::EncryptionError(format!("Failed to generate nonce: {}", e)))?;
        let nonce = Nonce::from_slice(&nonce_bytes);

        // Create cipher
        let cipher_key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(cipher_key);

        // Encrypt the payload
        let encrypted = cipher
            .encrypt(nonce, self.payload.as_ref())
            .map_err(|e| MQError::EncryptionError(format!("Failed to encrypt message: {}", e)))?;

        // Store nonce and encrypted data
        let mut encrypted_data = Vec::new();
        encrypted_data.extend_from_slice(&nonce_bytes);
        encrypted_data.extend_from_slice(&encrypted);

        // Update message
        self.payload = encrypted_data;
        self.is_encrypted = true;
        self.headers.insert("encryption".to_string(), "aes-256-gcm".to_string());

        Ok(())
    }

    pub fn decrypt(&mut self, key: &[u8]) -> Result<(), MQError> {
        if !self.is_encrypted {
            return Ok(());
        }

        // Extract nonce from the beginning of the payload
        if self.payload.len() < 12 {
            return Err(MQError::EncryptionError("Invalid encrypted payload".to_string()));
        }
        let (nonce_bytes, encrypted) = self.payload.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);

        // Create cipher
        let cipher_key = Key::<Aes256Gcm>::from_slice(key);
        let cipher = Aes256Gcm::new(cipher_key);

        // Decrypt the payload
        let decrypted = cipher
            .decrypt(nonce, encrypted)
            .map_err(|e| MQError::EncryptionError(format!("Failed to decrypt message: {}", e)))?;

        // Update message
        self.payload = decrypted;
        self.is_encrypted = false;
        self.headers.remove("encryption");

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: String,
    pub offset: u64,
    pub consumers: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Partition {
    pub id: usize,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    pub max_age_seconds: Option<u64>,
    pub max_size_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
    pub dlq: Partition, // Dead Letter Queue partition
    pub partition_count: usize,
    pub retention_policy: RetentionPolicy,
}

#[derive(Clone)]
pub struct MessageQueue {
    topics: Arc<RwLock<HashMap<String, Topic>>>,
    storage: Arc<RwLock<sled::Db>>,
}

impl MessageQueue {
    pub fn new(storage_path: &str) -> Result<Self, MQError> {
        let db = sled::open(storage_path)
            .map_err(|e| MQError::StorageError(e.to_string()))?;
        
        let mq = Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(db)),
        };

        // Start retention policy enforcement task
        let mq_clone = mq.clone();
        tokio::spawn(async move {
            loop {
                mq_clone.enforce_retention_policies().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
            }
        });

        Ok(mq)
    }

    pub async fn topic_exists(&self, name: &str) -> bool {
        let topics = self.topics.read().await;
        topics.contains_key(name)
    }

    pub async fn create_topic_with_partitions(
        &self,
        name: &str,
        partition_count: usize,
        retention_policy: Option<RetentionPolicy>,
    ) {
        let mut topics = self.topics.write().await;
        if !topics.contains_key(name) {
            let partitions = (0..partition_count)
                .map(|id| Partition { id, messages: Vec::new() })
                .collect();
            let dlq = Partition { id: partition_count, messages: Vec::new() };
            topics.insert(
                name.to_string(),
                Topic {
                    name: name.to_string(),
                    partitions,
                    dlq,
                    partition_count,
                    retention_policy: retention_policy.unwrap_or(RetentionPolicy {
                        max_age_seconds: Some(7 * 24 * 60 * 60), // Default 7 days
                        max_size_bytes: Some(1024 * 1024 * 1024), // Default 1GB
                    }),
                },
            );
        }
    }

    pub async fn create_topic(&self, name: &str) {
        self.create_topic_with_partitions(name, 3, None).await;
    }

    pub async fn publish(&self, topic: &str, message: Message) -> Result<(), MQError> {
        let mut topics = self.topics.write().await;
        let topic_obj = topics.get_mut(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        let partition_id = (message.id.as_u128() as usize) % topic_obj.partition_count;
        let partition = topic_obj.partitions.get_mut(partition_id).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        partition.messages.push(message);
        Ok(())
    }

    pub async fn publish_dlq(&self, topic: &str, message: Message) -> Result<(), MQError> {
        let mut topics = self.topics.write().await;
        let topic_obj = topics.get_mut(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        topic_obj.dlq.messages.push(message);
        Ok(())
    }

    pub async fn subscribe(&self, topic: &str, partition: Option<usize>, replay_offset: Option<usize>, replay_timestamp: Option<i64>, dlq: bool) -> Result<Vec<Message>, MQError> {
        if dlq {
            return self.subscribe_dlq(topic).await;
        }
        let topics = self.topics.read().await;
        let topic_obj = topics.get(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        let mut messages = Vec::new();
        match partition {
            Some(pid) => {
                if let Some(partition) = topic_obj.partitions.get(pid) {
                    let mut msgs = partition.messages.clone();
                    if let Some(offset) = replay_offset {
                        if offset < msgs.len() {
                            msgs = msgs[offset..].to_vec();
                        } else {
                            msgs.clear();
                        }
                    }
                    if let Some(ts) = replay_timestamp {
                        msgs = msgs.into_iter().filter(|m| m.timestamp >= ts).collect();
                    }
                    messages.extend(msgs);
                }
            }
            None => {
                for partition in &topic_obj.partitions {
                    let mut msgs = partition.messages.clone();
                    if let Some(offset) = replay_offset {
                        if offset < msgs.len() {
                            msgs = msgs[offset..].to_vec();
                        } else {
                            msgs.clear();
                        }
                    }
                    if let Some(ts) = replay_timestamp {
                        msgs = msgs.into_iter().filter(|m| m.timestamp >= ts).collect();
                    }
                    messages.extend(msgs);
                }
            }
        }
        Ok(messages)
    }

    pub async fn subscribe_dlq(&self, topic: &str) -> Result<Vec<Message>, MQError> {
        let topics = self.topics.read().await;
        let topic_obj = topics.get(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        Ok(topic_obj.dlq.messages.clone())
    }

    pub async fn replay_from_offset(&self, topic: &str, partition: usize, offset: usize) -> Result<Vec<Message>, MQError> {
        let topics = self.topics.read().await;
        let topic_obj = topics.get(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        if let Some(partition) = topic_obj.partitions.get(partition) {
            if offset < partition.messages.len() {
                return Ok(partition.messages[offset..].to_vec());
            } else {
                return Ok(vec![]);
            }
        }
        Err(MQError::TopicNotFound(topic.to_string()))
    }

    pub async fn replay_from_timestamp(&self, topic: &str, partition: usize, timestamp: i64) -> Result<Vec<Message>, MQError> {
        let topics = self.topics.read().await;
        let topic_obj = topics.get(topic).ok_or(MQError::TopicNotFound(topic.to_string()))?;
        if let Some(partition) = topic_obj.partitions.get(partition) {
            let msgs: Vec<_> = partition.messages.iter().filter(|m| m.timestamp >= timestamp).cloned().collect();
            return Ok(msgs);
        }
        Err(MQError::TopicNotFound(topic.to_string()))
    }

    pub async fn commit_offset(
        &self,
        topic: &str,
        partition: usize,
        offset: u64,
    ) -> Result<(), MQError> {
        let mut topics = self.topics.write().await;
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| MQError::TopicNotFound(topic.to_string()))?;

        if partition >= topic.partition_count {
            return Err(MQError::TopicNotFound(format!("Partition {} not found", partition)));
        }

        // Truncate messages up to the offset
        if let Some(partition) = topic.partitions.get_mut(partition) {
            if offset as usize <= partition.messages.len() {
                partition.messages.drain(..offset as usize);
            }
        }
        Ok(())
    }

    async fn enforce_retention_policies(&self) {
        let mut topics = self.topics.write().await;
        let now = chrono::Utc::now().timestamp();

        for topic in topics.values_mut() {
            for partition in &mut topic.partitions {
                // Enforce time-based retention
                if let Some(max_age) = topic.retention_policy.max_age_seconds {
                    partition.messages.retain(|msg| {
                        (now - msg.timestamp) <= max_age as i64
                    });
                }

                // Enforce size-based retention
                if let Some(max_size) = topic.retention_policy.max_size_bytes {
                    let mut current_size = 0;
                    let mut i = partition.messages.len();
                    
                    // Calculate size from newest to oldest messages
                    while i > 0 {
                        i -= 1;
                        current_size += partition.messages[i].payload.len();
                        if current_size > max_size as usize {
                            partition.messages.drain(..i);
                            break;
                        }
                    }
                }
            }
        }
    }

    pub async fn get_all_topics(&self) -> Vec<Topic> {
        let topics = self.topics.read().await;
        topics.values().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_basic_operations() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Create topic
        mq.create_topic("test-topic").await;

        // Publish message
        let message = Message {
            id: Uuid::new_v4(),
            topic: "test-topic".to_string(),
            payload: b"test message".to_vec(),
            timestamp: chrono::Utc::now().timestamp(),
            headers: HashMap::new(),
            is_compressed: false,
            is_encrypted: false,
        };
        mq.publish("test-topic", message.clone()).await.unwrap();

        // Subscribe and receive message
        let messages = mq
            .subscribe("test-topic", None, None, None, false)
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"test message");

        // Commit offset
        mq.commit_offset("test-topic", 0, 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_multiple_consumers() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Create topic and publish messages
        mq.create_topic_with_partitions("test-topic", 3, None).await;
        for i in 0..3 {
            let message = Message {
                id: Uuid::new_v4(),
                topic: "test-topic".to_string(),
                payload: format!("message {}", i).into_bytes(),
                timestamp: chrono::Utc::now().timestamp(),
                headers: HashMap::new(),
                is_compressed: false,
                is_encrypted: false,
            };
            mq.publish("test-topic", message).await.unwrap();
        }

        // Subscribe with multiple consumers to different partitions
        let messages1 = mq
            .subscribe("test-topic", Some(0), None, None, false)
            .await
            .unwrap();
        let messages2 = mq
            .subscribe("test-topic", Some(1), None, None, false)
            .await
            .unwrap();
        let messages3 = mq
            .subscribe("test-topic", Some(2), None, None, false)
            .await
            .unwrap();

        // Total messages across all partitions should be 3
        assert_eq!(messages1.len() + messages2.len() + messages3.len(), 3);

        // Commit offset for first partition
        mq.commit_offset("test-topic", 0, messages1.len() as u64)
            .await
            .unwrap();

        // Subscribe again to first partition
        let messages1_after = mq
            .subscribe("test-topic", Some(0), None, None, false)
            .await
            .unwrap();
        assert_eq!(messages1_after.len(), 0); // Should have no messages after commit
    }

    #[tokio::test]
    async fn test_error_cases() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Test publishing to non-existent topic
        let message = Message {
            id: Uuid::new_v4(),
            topic: "non-existent".to_string(),
            payload: b"test".to_vec(),
            timestamp: chrono::Utc::now().timestamp(),
            headers: HashMap::new(),
            is_compressed: false,
            is_encrypted: false,
        };
        assert!(matches!(
            mq.publish("non-existent", message).await,
            Err(MQError::TopicNotFound(_))
        ));

        // Test subscribing to non-existent topic
        assert!(matches!(
            mq.subscribe("non-existent", None, None, None, false).await,
            Err(MQError::TopicNotFound(_))
        ));

        // Test committing offset for non-existent topic
        assert!(matches!(
            mq.commit_offset("non-existent", 0, 0).await,
            Err(MQError::TopicNotFound(_))
        ));

        // Create topic and test committing offset for non-existent partition
        mq.create_topic("test-topic").await;
        assert!(matches!(
            mq.commit_offset("test-topic", 10, 0).await,
            Err(MQError::TopicNotFound(_))
        ));
    }

    #[tokio::test]
    async fn test_message_headers() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Create topic
        mq.create_topic("test-topic").await;

        // Create message with headers
        let mut headers = HashMap::new();
        headers.insert("key1".to_string(), "value1".to_string());
        headers.insert("key2".to_string(), "value2".to_string());

        let message = Message {
            id: Uuid::new_v4(),
            topic: "test-topic".to_string(),
            payload: b"test message".to_vec(),
            timestamp: chrono::Utc::now().timestamp(),
            headers: headers.clone(),
            is_compressed: false,
            is_encrypted: false,
        };

        // Publish and verify headers
        mq.publish("test-topic", message).await.unwrap();
        let messages = mq
            .subscribe("test-topic", None, None, None, false)
            .await
            .unwrap();
        assert_eq!(messages[0].headers, headers);
    }

    #[tokio::test]
    async fn test_concurrent_operations() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();
        mq.create_topic("test-topic").await;

        // Spawn multiple tasks to publish messages concurrently
        let mut handles = vec![];
        for i in 0..5 {
            let mq_clone = mq.clone();
            let handle = tokio::spawn(async move {
                let message = Message {
                    id: Uuid::new_v4(),
                    topic: "test-topic".to_string(),
                    payload: format!("message {}", i).into_bytes(),
                    timestamp: chrono::Utc::now().timestamp(),
                    headers: HashMap::new(),
                    is_compressed: false,
                    is_encrypted: false,
                };
                mq_clone.publish("test-topic", message).await
            });
            handles.push(handle);
        }

        // Wait for all publish operations to complete
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Verify all messages were published
        let messages = mq
            .subscribe("test-topic", None, None, None, false)
            .await
            .unwrap();
        assert_eq!(messages.len(), 5);
    }

    #[tokio::test]
    async fn test_message_compression() {
        let dir = tempfile::tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_string();
        let _mq = MessageQueue::new(&storage_path).unwrap();

        // Create a large message that should be compressed
        let large_payload = vec![0u8; 2048]; // 2KB of zeros
        let mut message = Message::new(
            "test-topic".to_string(),
            large_payload.clone(),
            HashMap::new(),
        );

        // Test compression
        message.compress().unwrap();
        assert!(message.is_compressed);
        assert!(message.payload.len() < large_payload.len()); // Compressed size should be smaller
        assert_eq!(message.headers.get("compression").unwrap(), "deflate");

        // Test decompression
        message.decompress().unwrap();
        assert!(!message.is_compressed);
        assert_eq!(message.payload.len(), large_payload.len()); // Length should match after decompression
        assert!(!message.headers.contains_key("compression"));

        // Test that small messages aren't compressed
        let small_payload = vec![0u8; 100]; // 100 bytes
        let mut small_message = Message::new(
            "test-topic".to_string(),
            small_payload.clone(),
            HashMap::new(),
        );
        small_message.compress().unwrap();
        assert!(!small_message.is_compressed);
        assert_eq!(small_message.payload, small_payload);
    }

    #[tokio::test]
    async fn test_compression_error_handling() {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_string();
        let _mq = MessageQueue::new(&storage_path).unwrap();

        // Create a message with invalid compression flag
        let mut message = Message::new(
            "test-topic".to_string(),
            vec![0u8; 100],
            HashMap::new(),
        );
        message.is_compressed = true; // Set compression flag without actually compressing

        // Attempting to decompress should fail
        assert!(matches!(
            message.decompress(),
            Err(MQError::CompressionError(_))
        ));
    }

    #[tokio::test]
    async fn test_message_encryption() {
        let storage_path = tempfile::tempdir().unwrap().path().to_path_buf();
        let _mq = MessageQueue::new(storage_path.to_str().unwrap()).unwrap();

        // Create a test message
        let mut message = Message {
            id: Uuid::new_v4(),
            topic: "test-topic".to_string(),
            payload: "Hello, encrypted world!".as_bytes().to_vec(),
            headers: HashMap::new(),
            is_compressed: false,
            is_encrypted: false,
            timestamp: chrono::Utc::now().timestamp(),
        };

        // Test encryption
        let key = [1u8; 32]; // 256-bit key
        message.encrypt(&key).unwrap();
        assert!(message.is_encrypted);
        assert!(message.headers.contains_key("encryption"));
        assert_eq!(message.headers.get("encryption").unwrap(), "aes-256-gcm");
        assert!(message.payload.len() > 12); // Should have nonce (12 bytes) + encrypted data

        // Test decryption
        message.decrypt(&key).unwrap();
        assert!(!message.is_encrypted);
        assert!(!message.headers.contains_key("encryption"));
        assert_eq!(message.payload, "Hello, encrypted world!".as_bytes());
    }

    #[tokio::test]
    async fn test_retention_policies() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Create topic with custom retention policy
        let retention_policy = RetentionPolicy {
            max_age_seconds: Some(60), // 1 minute
            max_size_bytes: Some(1000), // 1KB
        };
        mq.create_topic_with_partitions("test-topic", 1, Some(retention_policy)).await;

        // Publish messages
        for _i in 0..5 {
            let message = Message {
                id: Uuid::new_v4(),
                topic: "test-topic".to_string(),
                payload: vec![0u8; 200], // 200 bytes per message
                timestamp: chrono::Utc::now().timestamp(),
                headers: HashMap::new(),
                is_compressed: false,
                is_encrypted: false,
            };
            mq.publish("test-topic", message).await.unwrap();
        }

        // Wait for retention policy to be enforced
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        mq.enforce_retention_policies().await;

        // Verify messages are retained based on size
        let messages = mq.subscribe("test-topic", Some(0), None, None, false).await.unwrap();
        assert!(messages.len() <= 5); // Should have at most 5 messages (1000 bytes / 200 bytes per message)

        // Create topic with time-based retention
        let retention_policy = RetentionPolicy {
            max_age_seconds: Some(1), // 1 second
            max_size_bytes: None,
        };
        mq.create_topic_with_partitions("time-topic", 1, Some(retention_policy)).await;

        // Publish message
        let message = Message {
            id: Uuid::new_v4(),
            topic: "time-topic".to_string(),
            payload: b"test".to_vec(),
            timestamp: chrono::Utc::now().timestamp(),
            headers: HashMap::new(),
            is_compressed: false,
            is_encrypted: false,
        };
        mq.publish("time-topic", message).await.unwrap();

        // Wait for retention policy to be enforced
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
        mq.enforce_retention_policies().await;

        // Verify message is removed due to age
        let messages = mq.subscribe("time-topic", Some(0), None, None, false).await.unwrap();
        assert_eq!(messages.len(), 0);
    }

    #[test]
    fn test_invalid_storage_path() {
        // 使用无效的存储路径
        let invalid_path = if cfg!(windows) {
            "C:\\invalid\\path\\with\\invalid\\chars\\*\\?"
        } else {
            "/invalid/path/with/invalid/chars/*/?"
        };
        
        let result = MessageQueue::new(invalid_path);
        assert!(result.is_err());
    }
} 