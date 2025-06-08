use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MQError {
    #[error("Topic not found: {0}")]
    TopicNotFound(String),
    #[error("Consumer group not found: {0}")]
    ConsumerGroupNotFound(String),
    #[error("Storage error: {0}")]
    StorageError(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub topic: String,
    pub payload: Vec<u8>,
    pub timestamp: i64,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    pub id: String,
    pub offset: u64,
    pub consumers: Vec<String>,
}

#[derive(Debug)]
pub struct Topic {
    pub name: String,
    pub messages: Vec<Message>,
    pub consumer_groups: HashMap<String, ConsumerGroup>,
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
        
        Ok(Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(db)),
        })
    }

    pub async fn topic_exists(&self, name: &str) -> bool {
        let topics = self.topics.read().await;
        topics.contains_key(name)
    }

    pub async fn create_topic(&self, name: &str) {
        let mut topics = self.topics.write().await;
        if !topics.contains_key(name) {
            topics.insert(
                name.to_string(),
                Topic {
                    name: name.to_string(),
                    messages: Vec::new(),
                    consumer_groups: HashMap::new(),
                },
            );
        }
    }

    pub async fn publish(&self, topic: &str, message: Message) -> Result<(), MQError> {
        let mut topics = self.topics.write().await;
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| MQError::TopicNotFound(topic.to_string()))?;
        
        topic.messages.push(message);
        Ok(())
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        consumer_group: &str,
        consumer_id: &str,
    ) -> Result<Vec<Message>, MQError> {
        let mut topics = self.topics.write().await;
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| MQError::TopicNotFound(topic.to_string()))?;

        let group = topic
            .consumer_groups
            .entry(consumer_group.to_string())
            .or_insert_with(|| ConsumerGroup {
                id: consumer_group.to_string(),
                offset: 0,
                consumers: Vec::new(),
            });

        if !group.consumers.contains(&consumer_id.to_string()) {
            group.consumers.push(consumer_id.to_string());
        }

        let messages = topic.messages
            .iter()
            .skip(group.offset as usize)
            .cloned()
            .collect();

        Ok(messages)
    }

    pub async fn commit_offset(
        &self,
        topic: &str,
        consumer_group: &str,
        offset: u64,
    ) -> Result<(), MQError> {
        let mut topics = self.topics.write().await;
        let topic = topics
            .get_mut(topic)
            .ok_or_else(|| MQError::TopicNotFound(topic.to_string()))?;

        let group = topic
            .consumer_groups
            .get_mut(consumer_group)
            .ok_or_else(|| MQError::ConsumerGroupNotFound(consumer_group.to_string()))?;

        group.offset = offset;
        Ok(())
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
        };
        mq.publish("test-topic", message.clone()).await.unwrap();

        // Subscribe and receive message
        let messages = mq
            .subscribe("test-topic", "test-group", "consumer-1")
            .await
            .unwrap();
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].payload, b"test message");

        // Commit offset
        mq.commit_offset("test-topic", "test-group", 1)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_multiple_consumers() {
        let dir = tempdir().unwrap();
        let mq = MessageQueue::new(dir.path().to_str().unwrap()).unwrap();

        // Create topic and publish messages
        mq.create_topic("test-topic").await;
        for i in 0..3 {
            let message = Message {
                id: Uuid::new_v4(),
                topic: "test-topic".to_string(),
                payload: format!("message {}", i).into_bytes(),
                timestamp: chrono::Utc::now().timestamp(),
                headers: HashMap::new(),
            };
            mq.publish("test-topic", message).await.unwrap();
        }

        // Subscribe with multiple consumers in the same group
        let messages1 = mq
            .subscribe("test-topic", "test-group", "consumer-1")
            .await
            .unwrap();
        let messages2 = mq
            .subscribe("test-topic", "test-group", "consumer-2")
            .await
            .unwrap();

        assert_eq!(messages1.len(), 3);
        assert_eq!(messages2.len(), 3);

        // Commit offset for first consumer
        mq.commit_offset("test-topic", "test-group", 2)
            .await
            .unwrap();

        // Subscribe again with first consumer
        let messages1_after = mq
            .subscribe("test-topic", "test-group", "consumer-1")
            .await
            .unwrap();
        assert_eq!(messages1_after.len(), 1); // Should only get the last message
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
        };
        assert!(matches!(
            mq.publish("non-existent", message).await,
            Err(MQError::TopicNotFound(_))
        ));

        // Test subscribing to non-existent topic
        assert!(matches!(
            mq.subscribe("non-existent", "group", "consumer").await,
            Err(MQError::TopicNotFound(_))
        ));

        // Test committing offset for non-existent topic
        assert!(matches!(
            mq.commit_offset("non-existent", "group", 0).await,
            Err(MQError::TopicNotFound(_))
        ));

        // Create topic and test committing offset for non-existent consumer group
        mq.create_topic("test-topic").await;
        assert!(matches!(
            mq.commit_offset("test-topic", "non-existent", 0).await,
            Err(MQError::ConsumerGroupNotFound(_))
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
        };

        // Publish and verify headers
        mq.publish("test-topic", message).await.unwrap();
        let messages = mq
            .subscribe("test-topic", "test-group", "consumer-1")
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
            .subscribe("test-topic", "test-group", "consumer-1")
            .await
            .unwrap();
        assert_eq!(messages.len(), 5);
    }
} 