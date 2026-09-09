use std::{sync::Arc, time::Duration};

use chrono::Utc;
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde_json::json;
use tracing::warn;

use crate::{config::Config, domain::KafkaEvent};

#[derive(Clone)]
pub struct EventPublisher {
    producer: FutureProducer,
    config: Arc<Config>,
}

impl EventPublisher {
    pub fn connect(config: Arc<Config>) -> anyhow::Result<Self> {
        let producer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("enable.idempotence", "true")
            .set("acks", "all")
            .set("retries", "2147483647")
            .set("max.in.flight.requests.per.connection", "5")
            .set("compression.type", "zstd")
            .set("delivery.timeout.ms", "30000")
            .create()?;
        Ok(Self { producer, config })
    }

    pub async fn publish(&self, event: &KafkaEvent) -> anyhow::Result<()> {
        let payload = serde_json::to_string(event)?;
        self.send(
            &self.config.topic(event.event_type),
            &event.partition_key(),
            &payload,
        )
        .await
    }

    pub async fn publish_dlq(&self, reason: &str, payload: &[u8]) -> anyhow::Result<()> {
        let body = json!({
            "reason": reason,
            "received_at": Utc::now(),
            "payload": String::from_utf8_lossy(payload),
        });
        self.send(
            &self.config.dlq_topic(),
            "consumer-decode-error",
            &serde_json::to_string(&body)?,
        )
        .await
    }

    async fn send(&self, topic: &str, key: &str, payload: &str) -> anyhow::Result<()> {
        self.producer
            .send(
                FutureRecord::to(topic).key(key).payload(payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(error, _)| {
                warn!(?error, topic, "Kafka publish failed");
                anyhow::anyhow!(error)
            })?;
        Ok(())
    }
}
