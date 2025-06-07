use crate::storage::{Message, MessageStatus, MessageStorage, StorageError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct TransactionManager {
    storage: MessageStorage,
    check_interval: Duration,
}

impl TransactionManager {
    pub fn new(storage: MessageStorage, check_interval: Duration) -> Self {
        Self {
            storage,
            check_interval,
        }
    }

    pub async fn start_checking(&self) {
        let mut interval = time::interval(self.check_interval);
        loop {
            interval.tick().await;
            if let Err(e) = self.check_pending_messages().await {
                tracing::error!("Error checking pending messages: {:?}", e);
            }
        }
    }

    async fn check_pending_messages(&self) -> Result<(), StorageError> {
        // TODO: Implement actual message checking logic
        // This would involve checking for messages that have been in Pending state
        // for too long and need to be either committed or rolled back
        Ok(())
    }

    pub fn prepare_message(&self, topic: String, body: Vec<u8>) -> Result<Uuid, StorageError> {
        let message = Message {
            id: Uuid::new_v4(),
            topic,
            body,
            status: MessageStatus::Pending,
            created_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
        };

        self.storage.store_message(&message)?;
        Ok(message.id)
    }

    pub fn commit_message(&self, id: Uuid) -> Result<(), StorageError> {
        self.storage
            .update_message_status(id, MessageStatus::Committed)
    }

    pub fn rollback_message(&self, id: Uuid) -> Result<(), StorageError> {
        self.storage
            .update_message_status(id, MessageStatus::Rollback)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_transaction_manager() {
        let temp_dir = tempdir().unwrap();
        let storage = MessageStorage::new(temp_dir.path()).unwrap();
        let manager = TransactionManager::new(storage, Duration::from_secs(1));

        // Test preparing a message
        let topic = "test".to_string();
        let body = vec![1, 2, 3];
        let message_id = manager.prepare_message(topic.clone(), body.clone()).unwrap();

        // Verify message is in pending state
        let message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(message.topic, topic);
        assert_eq!(message.body, body);
        assert_eq!(message.status, MessageStatus::Pending);

        // Test committing a message
        manager.commit_message(message_id).unwrap();
        let committed_message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(committed_message.status, MessageStatus::Committed);

        // Test rolling back a message
        let message_id = manager.prepare_message(topic, body).unwrap();
        manager.rollback_message(message_id).unwrap();
        let rolled_back_message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(rolled_back_message.status, MessageStatus::Rollback);
    }
} 