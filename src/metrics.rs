use lazy_static::lazy_static;
use prometheus::{
    Counter, Gauge, Histogram, HistogramOpts, IntCounter, IntGauge, Opts, Registry,
};
use std::sync::Arc;
use tokio::sync::RwLock;

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();
}

#[derive(Clone)]
pub struct Metrics {
    // Topic metrics
    pub topic_count: IntGauge,
    pub total_messages: IntGauge,
    pub messages_per_topic: IntGauge,
    pub dlq_messages: IntGauge,

    // Message operation metrics
    pub publish_operations: IntCounter,
    pub subscribe_operations: IntCounter,
    pub publish_latency: Histogram,
    pub subscribe_latency: Histogram,

    // Error metrics
    pub publish_errors: IntCounter,
    pub subscribe_errors: IntCounter,
    pub compression_errors: IntCounter,
    pub encryption_errors: IntCounter,
}

impl Metrics {
    pub fn new() -> Self {
        let topic_count = IntGauge::new("mq_topic_count", "Total number of topics").unwrap();
        let total_messages = IntGauge::new("mq_total_messages", "Total number of messages").unwrap();
        let messages_per_topic = IntGauge::new("mq_messages_per_topic", "Messages per topic").unwrap();
        let dlq_messages = IntGauge::new("mq_dlq_messages", "Number of messages in DLQ").unwrap();

        let publish_operations = IntCounter::new("mq_publish_operations_total", "Total publish operations").unwrap();
        let subscribe_operations = IntCounter::new("mq_subscribe_operations_total", "Total subscribe operations").unwrap();
        
        let publish_latency = Histogram::with_opts(
            HistogramOpts::new("mq_publish_latency_seconds", "Publish operation latency")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        ).unwrap();
        
        let subscribe_latency = Histogram::with_opts(
            HistogramOpts::new("mq_subscribe_latency_seconds", "Subscribe operation latency")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        ).unwrap();

        let publish_errors = IntCounter::new("mq_publish_errors_total", "Total publish errors").unwrap();
        let subscribe_errors = IntCounter::new("mq_subscribe_errors_total", "Total subscribe errors").unwrap();
        let compression_errors = IntCounter::new("mq_compression_errors_total", "Total compression errors").unwrap();
        let encryption_errors = IntCounter::new("mq_encryption_errors_total", "Total encryption errors").unwrap();

        // Register all metrics
        REGISTRY.register(Box::new(topic_count.clone())).unwrap();
        REGISTRY.register(Box::new(total_messages.clone())).unwrap();
        REGISTRY.register(Box::new(messages_per_topic.clone())).unwrap();
        REGISTRY.register(Box::new(dlq_messages.clone())).unwrap();
        REGISTRY.register(Box::new(publish_operations.clone())).unwrap();
        REGISTRY.register(Box::new(subscribe_operations.clone())).unwrap();
        REGISTRY.register(Box::new(publish_latency.clone())).unwrap();
        REGISTRY.register(Box::new(subscribe_latency.clone())).unwrap();
        REGISTRY.register(Box::new(publish_errors.clone())).unwrap();
        REGISTRY.register(Box::new(subscribe_errors.clone())).unwrap();
        REGISTRY.register(Box::new(compression_errors.clone())).unwrap();
        REGISTRY.register(Box::new(encryption_errors.clone())).unwrap();

        Self {
            topic_count,
            total_messages,
            messages_per_topic,
            dlq_messages,
            publish_operations,
            subscribe_operations,
            publish_latency,
            subscribe_latency,
            publish_errors,
            subscribe_errors,
            compression_errors,
            encryption_errors,
        }
    }

    pub fn update_topic_metrics(&self, topic_count: i64, total_messages: i64, dlq_messages: i64) {
        self.topic_count.set(topic_count);
        self.total_messages.set(total_messages);
        self.dlq_messages.set(dlq_messages);
    }

    pub fn record_publish(&self, latency: f64) {
        self.publish_operations.inc();
        self.publish_latency.observe(latency);
    }

    pub fn record_subscribe(&self, latency: f64) {
        self.subscribe_operations.inc();
        self.subscribe_latency.observe(latency);
    }

    pub fn record_publish_error(&self) {
        self.publish_errors.inc();
    }

    pub fn record_subscribe_error(&self) {
        self.subscribe_errors.inc();
    }

    pub fn record_compression_error(&self) {
        self.compression_errors.inc();
    }

    pub fn record_encryption_error(&self) {
        self.encryption_errors.inc();
    }
}

lazy_static! {
    pub static ref METRICS: Arc<RwLock<Metrics>> = Arc::new(RwLock::new(Metrics::new()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_metrics_recording() {
        let metrics = METRICS.read().await;
        
        // Test basic metrics
        metrics.update_topic_metrics(5, 100, 2);
        assert_eq!(metrics.topic_count.get(), 5);
        assert_eq!(metrics.total_messages.get(), 100);
        assert_eq!(metrics.dlq_messages.get(), 2);

        // Test operation metrics
        metrics.record_publish(0.1);
        metrics.record_subscribe(0.2);
        assert_eq!(metrics.publish_operations.get(), 1);
        assert_eq!(metrics.subscribe_operations.get(), 1);

        // Test error metrics
        metrics.record_publish_error();
        metrics.record_subscribe_error();
        metrics.record_compression_error();
        metrics.record_encryption_error();
        assert_eq!(metrics.publish_errors.get(), 1);
        assert_eq!(metrics.subscribe_errors.get(), 1);
        assert_eq!(metrics.compression_errors.get(), 1);
        assert_eq!(metrics.encryption_errors.get(), 1);
    }
} 