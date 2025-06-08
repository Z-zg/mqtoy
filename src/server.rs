use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;
use tokio_stream::wrappers::ReceiverStream;
use tokio::sync::mpsc;
use std::collections::HashMap;

use crate::message_queue::{Message, MessageQueue, MQError};
use mq::message_queue_service_server::{MessageQueueService, MessageQueueServiceServer};
use mq::{
    CommitOffsetRequest, CommitOffsetResponse, CreateTopicRequest, CreateTopicResponse,
    PublishRequest, PublishResponse, SubscribeRequest,
};

pub mod mq {
    tonic::include_proto!("mq");
}

#[derive(Clone)]
pub struct MQServer {
    message_queue: MessageQueue,
}

#[tonic::async_trait]
impl MessageQueueService for MQServer {
    type SubscribeStream = ReceiverStream<Result<mq::Message, Status>>;

    async fn publish(
        &self,
        request: Request<PublishRequest>,
    ) -> Result<Response<PublishResponse>, Status> {
        let req = request.into_inner();
        
        // First check if the topic exists
        if !self.message_queue.topic_exists(&req.topic).await {
            return Err(Status::not_found(format!("Topic '{}' not found", req.topic)));
        }

        let mut message = Message {
            id: Uuid::new_v4(),
            topic: req.topic.clone(),
            payload: req.payload,
            timestamp: chrono::Utc::now().timestamp(),
            headers: req.headers,
            is_compressed: false,
        };

        // Compress the message if it's large enough
        if message.payload.len() > 1024 { // Only compress messages larger than 1KB
            message.compress()
                .map_err(|e| Status::internal(format!("Failed to compress message: {}", e)))?;
        }

        self.message_queue
            .publish(&req.topic, message.clone())
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PublishResponse {
            message_id: message.id.to_string(),
        }))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let req = request.into_inner();

        // First check if the topic exists
        if !self.message_queue.topic_exists(&req.topic).await {
            return Err(Status::not_found(format!("Topic '{}' not found", req.topic)));
        }

        let (tx, rx) = mpsc::channel(8);
        let message_queue = self.message_queue.clone();
        tokio::spawn(async move {
            let messages = message_queue
                .subscribe(&req.topic, &req.consumer_group, &req.consumer_id)
                .await;
            if let Ok(messages) = messages {
                for mut msg in messages {
                    // Decompress the message if it's compressed
                    if msg.is_compressed {
                        if let Err(e) = msg.decompress() {
                            eprintln!("Failed to decompress message: {}", e);
                            continue;
                        }
                    }

                    let proto_msg = mq::Message {
                        id: msg.id.to_string(),
                        topic: msg.topic,
                        payload: msg.payload,
                        timestamp: msg.timestamp,
                        headers: msg.headers,
                    };
                    if tx.send(Ok(proto_msg)).await.is_err() {
                        break;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn create_topic(
        &self,
        request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();
        self.message_queue.create_topic(&req.name).await;
        Ok(Response::new(CreateTopicResponse { success: true }))
    }

    async fn commit_offset(
        &self,
        request: Request<CommitOffsetRequest>,
    ) -> Result<Response<CommitOffsetResponse>, Status> {
        let req = request.into_inner();
        self.message_queue
            .commit_offset(&req.topic, &req.consumer_group, req.offset)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CommitOffsetResponse { success: true }))
    }
}

impl MQServer {
    pub fn new(message_queue: MessageQueue) -> Self {
        Self { message_queue }
    }
}

pub async fn run_server(message_queue: MessageQueue, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    let addr = addr.parse()?;
    let server = MQServer::new(message_queue);
    println!("MQ Server listening on {}", addr);
    Server::builder()
        .add_service(MessageQueueServiceServer::new(server))
        .serve(addr)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use tokio::sync::mpsc;
    use tokio_stream::StreamExt;

    async fn setup_test_server() -> (MQServer, String) {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_string();
        let message_queue = MessageQueue::new(&storage_path).unwrap();
        let server = MQServer::new(message_queue);
        (server, storage_path)
    }

    #[tokio::test]
    async fn test_publish_and_subscribe() {
        let (server, _) = setup_test_server().await;

        // Create topic
        let create_req = Request::new(CreateTopicRequest {
            name: "test-topic".to_string(),
        });
        let response = server.create_topic(create_req).await.unwrap();
        assert!(response.into_inner().success);

        // Publish message
        let mut headers = HashMap::new();
        headers.insert("key1".to_string(), "value1".to_string());
        let publish_req = Request::new(PublishRequest {
            topic: "test-topic".to_string(),
            payload: b"test message".to_vec(),
            headers,
        });
        let response = server.publish(publish_req).await.unwrap();
        let message_id = response.into_inner().message_id;
        assert!(!message_id.is_empty());

        // Subscribe to messages
        let subscribe_req = Request::new(SubscribeRequest {
            topic: "test-topic".to_string(),
            consumer_group: "test-group".to_string(),
            consumer_id: "consumer-1".to_string(),
        });
        let response = server.subscribe(subscribe_req).await.unwrap();
        let mut stream = response.into_inner();

        // Receive message
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.topic, "test-topic");
        assert_eq!(message.payload, b"test message");
        assert_eq!(message.headers.get("key1").unwrap(), "value1");
    }

    #[tokio::test]
    async fn test_error_handling() {
        let (server, _) = setup_test_server().await;

        // Try to publish to non-existent topic
        let publish_req = Request::new(PublishRequest {
            topic: "non-existent".to_string(),
            payload: b"test".to_vec(),
            headers: HashMap::new(),
        });
        let result = server.publish(publish_req).await;
        assert!(result.is_err());

        // Try to subscribe to non-existent topic
        let subscribe_req = Request::new(SubscribeRequest {
            topic: "non-existent".to_string(),
            consumer_group: "test-group".to_string(),
            consumer_id: "consumer-1".to_string(),
        });
        let result = server.subscribe(subscribe_req).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_commit_offset() {
        let (server, _) = setup_test_server().await;

        // Create topic
        let create_req = Request::new(CreateTopicRequest {
            name: "test-topic".to_string(),
        });
        server.create_topic(create_req).await.unwrap();

        // Publish some messages
        for i in 0..3 {
            let publish_req = Request::new(PublishRequest {
                topic: "test-topic".to_string(),
                payload: format!("message {}", i).into_bytes(),
                headers: HashMap::new(),
            });
            server.publish(publish_req).await.unwrap();
        }

        // Subscribe and commit offset
        let subscribe_req = Request::new(SubscribeRequest {
            topic: "test-topic".to_string(),
            consumer_group: "test-group".to_string(),
            consumer_id: "consumer-1".to_string(),
        });
        let response = server.subscribe(subscribe_req).await.unwrap();
        let mut stream = response.into_inner();

        // Receive first two messages
        let _ = stream.next().await.unwrap().unwrap();
        let _ = stream.next().await.unwrap().unwrap();

        // Commit offset
        let commit_req = Request::new(CommitOffsetRequest {
            topic: "test-topic".to_string(),
            consumer_group: "test-group".to_string(),
            offset: 2,
        });
        let response = server.commit_offset(commit_req).await.unwrap();
        assert!(response.into_inner().success);

        // Subscribe again and verify only one message is received
        let subscribe_req = Request::new(SubscribeRequest {
            topic: "test-topic".to_string(),
            consumer_group: "test-group".to_string(),
            consumer_id: "consumer-1".to_string(),
        });
        let response = server.subscribe(subscribe_req).await.unwrap();
        let mut stream = response.into_inner();
        let message = stream.next().await.unwrap().unwrap();
        assert_eq!(message.payload, b"message 2");
    }

    #[tokio::test]
    async fn test_concurrent_subscribers() {
        let (server, _) = setup_test_server().await;

        // Create topic
        let create_req = Request::new(CreateTopicRequest {
            name: "test-topic".to_string(),
        });
        server.create_topic(create_req).await.unwrap();

        // Publish messages
        for i in 0..5 {
            let publish_req = Request::new(PublishRequest {
                topic: "test-topic".to_string(),
                payload: format!("message {}", i).into_bytes(),
                headers: HashMap::new(),
            });
            server.publish(publish_req).await.unwrap();
        }

        // Create multiple subscribers
        let mut handles = vec![];
        for i in 0..3 {
            let server_clone = server.clone();
            let handle = tokio::spawn(async move {
                let subscribe_req = Request::new(SubscribeRequest {
                    topic: "test-topic".to_string(),
                    consumer_group: "test-group".to_string(),
                    consumer_id: format!("consumer-{}", i),
                });
                let response = server_clone.subscribe(subscribe_req).await.unwrap();
                let mut stream = response.into_inner();
                let mut count = 0;
                while let Some(_) = stream.next().await {
                    count += 1;
                }
                count
            });
            handles.push(handle);
        }

        // Verify all subscribers received all messages
        for handle in handles {
            let count = handle.await.unwrap();
            assert_eq!(count, 5);
        }
    }
} 