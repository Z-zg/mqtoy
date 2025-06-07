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
} 