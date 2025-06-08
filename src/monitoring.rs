use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use crate::message_queue::{MessageQueue, Topic};
use crate::metrics::{METRICS, REGISTRY};
use prometheus::Encoder;
use std::time::Instant;

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetrics {
    name: String,
    partition_count: usize,
    total_messages: usize,
    messages_per_partition: Vec<usize>,
    dlq_message_count: usize,
    retention_policy: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SystemMetrics {
    total_topics: usize,
    total_messages: usize,
    topics: Vec<TopicMetrics>,
}

pub struct MonitoringServer {
    message_queue: Arc<MessageQueue>,
    metrics: Arc<RwLock<SystemMetrics>>,
}

impl MonitoringServer {
    pub fn new(message_queue: Arc<MessageQueue>) -> Self {
        Self {
            message_queue: message_queue.clone(),
            metrics: Arc::new(RwLock::new(SystemMetrics {
                total_topics: 0,
                total_messages: 0,
                topics: Vec::new(),
            })),
        }
    }

    pub async fn update_metrics(&self) {
        let mut metrics = self.metrics.write().await;
        let topics = self.message_queue.get_all_topics().await;
        
        metrics.total_topics = topics.len();
        metrics.total_messages = 0;
        metrics.topics.clear();

        let mut total_messages = 0;
        let mut total_dlq_messages = 0;

        for topic in topics {
            let mut topic_metrics = TopicMetrics {
                name: topic.name.clone(),
                partition_count: topic.partition_count,
                total_messages: 0,
                messages_per_partition: Vec::new(),
                dlq_message_count: topic.dlq.messages.len(),
                retention_policy: format!(
                    "max_age: {:?}s, max_size: {:?}B",
                    topic.retention_policy.max_age_seconds,
                    topic.retention_policy.max_size_bytes
                ),
            };

            for partition in &topic.partitions {
                let message_count = partition.messages.len();
                topic_metrics.messages_per_partition.push(message_count);
                topic_metrics.total_messages += message_count;
            }

            total_messages += topic_metrics.total_messages;
            total_dlq_messages += topic_metrics.dlq_message_count;
            metrics.topics.push(topic_metrics);
        }

        metrics.total_messages = total_messages;

        // Update Prometheus metrics
        let prom_metrics = METRICS.read().await;
        prom_metrics.update_topic_metrics(
            metrics.total_topics as i64,
            metrics.total_messages as i64,
            total_dlq_messages as i64,
        );
    }
}

async fn get_system_metrics(
    server: web::Data<Arc<MonitoringServer>>,
) -> impl Responder {
    let metrics = server.metrics.read().await;
    HttpResponse::Ok().json(&*metrics)
}

async fn get_topic_metrics(
    server: web::Data<Arc<MonitoringServer>>,
    topic_name: web::Path<String>,
) -> impl Responder {
    let metrics = server.metrics.read().await;
    if let Some(topic) = metrics.topics.iter().find(|t| t.name == *topic_name) {
        HttpResponse::Ok().json(topic)
    } else {
        HttpResponse::NotFound().finish()
    }
}

// Add new endpoint for Prometheus metrics
async fn metrics() -> impl Responder {
    let mut buffer = vec![];
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    HttpResponse::Ok()
        .content_type("text/plain; version=0.0.4; charset=utf-8")
        .body(buffer)
}

pub async fn start_monitoring_server(
    monitoring_server: Arc<MonitoringServer>,
    host: &str,
    port: u16,
) -> std::io::Result<()> {
    let server = monitoring_server.clone();
    
    // Start metrics update task
    let update_server = monitoring_server.clone();
    tokio::spawn(async move {
        loop {
            update_server.update_metrics().await;
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        }
    });

    println!("Monitoring server starting at http://{}:{}", host, port);
    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(server.clone()))
            .route("/metrics", web::get().to(get_system_metrics))
            .route("/metrics/topic/{topic_name}", web::get().to(get_topic_metrics))
            .route("/prometheus", web::get().to(metrics))
    })
    .bind((host, port))?
    .run()
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use crate::message_queue::MessageQueue;
    use tempfile::tempdir;
    use uuid::Uuid;

    async fn setup_test_server() -> (Arc<MonitoringServer>, String) {
        let dir = tempdir().unwrap();
        let storage_path = dir.path().to_str().unwrap().to_string();
        let message_queue = Arc::new(MessageQueue::new(&storage_path).unwrap());
        let server = Arc::new(MonitoringServer::new(message_queue.clone()));
        (server, storage_path)
    }

    #[actix_rt::test]
    async fn test_system_metrics() {
        let (server, _) = setup_test_server().await;
        
        // Create test topic and publish messages
        let message_queue = server.message_queue.clone();
        message_queue.create_topic("test-topic").await;
        
        for _ in 0..3 {
            let message = crate::message_queue::Message {
                id: Uuid::new_v4(),
                topic: "test-topic".to_string(),
                payload: b"test".to_vec(),
                timestamp: chrono::Utc::now().timestamp(),
                headers: HashMap::new(),
                is_compressed: false,
                is_encrypted: false,
            };
            message_queue.publish("test-topic", message).await.unwrap();
        }

        // Update metrics
        server.update_metrics().await;

        // Test metrics endpoint
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server.clone()))
                .route("/metrics", web::get().to(get_system_metrics))
        ).await;

        let req = test::TestRequest::get().uri("/metrics").to_request();
        let resp: SystemMetrics = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.total_topics, 1);
        assert_eq!(resp.total_messages, 3);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "test-topic");
    }

    #[actix_rt::test]
    async fn test_topic_metrics() {
        let (server, _) = setup_test_server().await;
        
        // Create test topic and publish messages
        let message_queue = server.message_queue.clone();
        message_queue.create_topic("test-topic").await;
        
        for _ in 0..2 {
            let message = crate::message_queue::Message {
                id: Uuid::new_v4(),
                topic: "test-topic".to_string(),
                payload: b"test".to_vec(),
                timestamp: chrono::Utc::now().timestamp(),
                headers: HashMap::new(),
                is_compressed: false,
                is_encrypted: false,
            };
            message_queue.publish("test-topic", message).await.unwrap();
        }

        // Update metrics
        server.update_metrics().await;

        // Test topic metrics endpoint
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server.clone()))
                .route("/metrics/topic/{topic_name}", web::get().to(get_topic_metrics))
        ).await;

        let req = test::TestRequest::get()
            .uri("/metrics/topic/test-topic")
            .to_request();
        let resp: TopicMetrics = test::call_and_read_body_json(&app, req).await;

        assert_eq!(resp.name, "test-topic");
        assert_eq!(resp.total_messages, 2);
        assert_eq!(resp.partition_count, 3);
        assert_eq!(resp.messages_per_partition.len(), 3);
    }

    #[actix_rt::test]
    async fn test_nonexistent_topic_metrics() {
        let (server, _) = setup_test_server().await;
        
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(server.clone()))
                .route("/metrics/topic/{topic_name}", web::get().to(get_topic_metrics))
        ).await;

        let req = test::TestRequest::get()
            .uri("/metrics/topic/nonexistent")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), 404);
    }
} 