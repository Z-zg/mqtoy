use serde::{Deserialize, Serialize};
use sled::Db;
use std::path::Path;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Storage operation failed: {0}")]
    OperationError(String),
    #[error("Message not found")]
    MessageNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    pub id: Uuid,
    pub topic: String,
    pub body: Vec<u8>,
    pub status: MessageStatus,
    pub created_at: i64,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum MessageStatus {
    Pending,
    Committed,
    Rollback,
}

#[derive(Debug, Clone)]
pub struct MessageStorage {
    db: Db,
}

impl MessageStorage {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = sled::open(path).map_err(|e| StorageError::OperationError(e.to_string()))?;
        Ok(Self { db })
    }

    pub fn store_message(&self, message: &Message) -> Result<(), StorageError> {
        let key = format!("msg:{}", message.id);
        let value = serde_json::to_vec(message)
            .map_err(|e| StorageError::OperationError(e.to_string()))?;
        
        self.db
            .insert(key, value)
            .map_err(|e| StorageError::OperationError(e.to_string()))?;
        
        Ok(())
    }

    pub fn get_message(&self, id: Uuid) -> Result<Message, StorageError> {
        let key = format!("msg:{}", id);
        let value = self
            .db
            .get(key)
            .map_err(|e| StorageError::OperationError(e.to_string()))?
            .ok_or(StorageError::MessageNotFound)?;

        serde_json::from_slice(&value)
            .map_err(|e| StorageError::OperationError(e.to_string()))
    }

    pub fn update_message_status(&self, id: Uuid, status: MessageStatus) -> Result<(), StorageError> {
        let mut message = self.get_message(id)?;
        message.status = status;
        self.store_message(&message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_message_storage() {
        let temp_dir = tempdir().unwrap();
        let storage = MessageStorage::new(temp_dir.path()).unwrap();

        // Test storing and retrieving a message
        let message = Message {
            id: Uuid::new_v4(),
            topic: "test".to_string(),
            body: vec![1, 2, 3],
            status: MessageStatus::Pending,
            created_at: 1234567890,
        };

        // Store message
        storage.store_message(&message).unwrap();

        // Retrieve message
        let retrieved = storage.get_message(message.id).unwrap();
        assert_eq!(retrieved.id, message.id);
        assert_eq!(retrieved.topic, message.topic);
        assert_eq!(retrieved.body, message.body);
        assert_eq!(retrieved.status, message.status);
        assert_eq!(retrieved.created_at, message.created_at);

        // Test updating message status
        storage
            .update_message_status(message.id, MessageStatus::Committed)
            .unwrap();
        let updated = storage.get_message(message.id).unwrap();
        assert_eq!(updated.status, MessageStatus::Committed);

        // Test non-existent message
        let non_existent_id = Uuid::new_v4();
        assert!(matches!(
            storage.get_message(non_existent_id),
            Err(StorageError::MessageNotFound)
        ));
    }

    #[test]
    fn test_message_status_transitions() {
        let temp_dir = tempdir().unwrap();
        let storage = MessageStorage::new(temp_dir.path()).unwrap();

        let message = Message {
            id: Uuid::new_v4(),
            topic: "test".to_string(),
            body: vec![1, 2, 3],
            status: MessageStatus::Pending,
            created_at: 1234567890,
        };

        storage.store_message(&message).unwrap();

        // Test all possible status transitions
        storage
            .update_message_status(message.id, MessageStatus::Committed)
            .unwrap();
        let committed = storage.get_message(message.id).unwrap();
        assert_eq!(committed.status, MessageStatus::Committed);

        storage
            .update_message_status(message.id, MessageStatus::Rollback)
            .unwrap();
        let rolled_back = storage.get_message(message.id).unwrap();
        assert_eq!(rolled_back.status, MessageStatus::Rollback);

        storage
            .update_message_status(message.id, MessageStatus::Pending)
            .unwrap();
        let pending = storage.get_message(message.id).unwrap();
        assert_eq!(pending.status, MessageStatus::Pending);
    }

    #[test]
    fn test_multiple_messages() {
        let temp_dir = tempdir().unwrap();
        let storage = MessageStorage::new(temp_dir.path()).unwrap();

        // Store multiple messages
        let messages: Vec<Message> = (0..5)
            .map(|i| Message {
                id: Uuid::new_v4(),
                topic: format!("topic-{}", i),
                body: vec![i as u8],
                status: MessageStatus::Pending,
                created_at: 1234567890 + i as i64,
            })
            .collect();

        for message in &messages {
            storage.store_message(message).unwrap();
        }

        // Verify all messages can be retrieved
        for message in &messages {
            let retrieved = storage.get_message(message.id).unwrap();
            assert_eq!(retrieved.id, message.id);
            assert_eq!(retrieved.topic, message.topic);
            assert_eq!(retrieved.body, message.body);
        }
    }

    #[test]
    fn test_storage_persistence() {
        let temp_dir = tempdir().unwrap();
        let storage_path = temp_dir.path();

        // Create and store a message
        let storage = MessageStorage::new(storage_path).unwrap();
        let message = Message {
            id: Uuid::new_v4(),
            topic: "test".to_string(),
            body: vec![1, 2, 3],
            status: MessageStatus::Pending,
            created_at: 1234567890,
        };
        storage.store_message(&message).unwrap();

        // Drop the first storage instance to release the lock
        drop(storage);

        // Create a new storage instance with the same path
        let new_storage = MessageStorage::new(storage_path).unwrap();
        let retrieved = new_storage.get_message(message.id).unwrap();
        assert_eq!(retrieved.id, message.id);
        assert_eq!(retrieved.topic, message.topic);
        assert_eq!(retrieved.body, message.body);
        assert_eq!(retrieved.status, message.status);
    }

    #[test]
    fn test_invalid_storage_path() {
        let result = MessageStorage::new("/invalid/path/that/does/not/exist");
        assert!(result.is_err());
    }
} 