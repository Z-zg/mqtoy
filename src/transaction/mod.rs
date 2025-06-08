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
    use tokio::time::sleep;

    async fn setup_test_manager() -> (TransactionManager, String) {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path().to_str().unwrap().to_string();
        let storage = MessageStorage::new(temp_dir.path()).unwrap();
        let manager = TransactionManager::new(storage, Duration::from_millis(100));
        (manager, storage_path)
    }

    #[tokio::test]
    async fn test_transaction_manager() {
        let (manager, _) = setup_test_manager().await;

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

    #[tokio::test]
    async fn test_concurrent_transactions() {
        let (manager, _) = setup_test_manager().await;

        // Prepare multiple messages concurrently
        let mut handles = vec![];
        for i in 0..5 {
            let manager_clone = manager.clone();
            let handle = tokio::spawn(async move {
                let topic = format!("topic-{}", i);
                let body = vec![i as u8];
                let message_id = manager_clone.prepare_message(topic, body).unwrap();
                sleep(Duration::from_millis(50)).await;
                manager_clone.commit_message(message_id).unwrap();
                message_id
            });
            handles.push(handle);
        }

        // Wait for all transactions to complete
        let message_ids: Vec<Uuid> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all messages are committed
        for message_id in message_ids {
            let message = manager.storage.get_message(message_id).unwrap();
            assert_eq!(message.status, MessageStatus::Committed);
        }
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let (manager, _) = setup_test_manager().await;

        // Prepare a message
        let topic = "test".to_string();
        let body = vec![1, 2, 3];
        let message_id = manager.prepare_message(topic.clone(), body.clone()).unwrap();

        // Verify initial state
        let message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(message.status, MessageStatus::Pending);

        // Rollback the message
        manager.rollback_message(message_id).unwrap();

        // Verify rollback state
        let message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(message.status, MessageStatus::Rollback);

        // Try to commit after rollback
        manager.commit_message(message_id).unwrap();
        let message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(message.status, MessageStatus::Committed);
    }

    #[tokio::test]
    async fn test_message_checking() {
        let (manager, _) = setup_test_manager().await;

        // Prepare a message
        let topic = "test".to_string();
        let body = vec![1, 2, 3];
        let message_id = manager.prepare_message(topic, body).unwrap();

        // Start the checking process
        let manager_clone = manager.clone();
        let check_handle = tokio::spawn(async move {
            manager_clone.start_checking().await;
        });

        // Wait for a short time to allow checking to occur
        sleep(Duration::from_millis(200)).await;

        // Cancel the checking process
        check_handle.abort();

        // Verify the message is still in pending state
        let message = manager.storage.get_message(message_id).unwrap();
        assert_eq!(message.status, MessageStatus::Pending);
    }

    #[tokio::test]
    async fn test_invalid_message_id() {
        let (manager, _) = setup_test_manager().await;
        let invalid_id = Uuid::new_v4();

        // Try to commit non-existent message
        assert!(matches!(
            manager.commit_message(invalid_id),
            Err(StorageError::MessageNotFound)
        ));

        // Try to rollback non-existent message
        assert!(matches!(
            manager.rollback_message(invalid_id),
            Err(StorageError::MessageNotFound)
        ));
    }
} 