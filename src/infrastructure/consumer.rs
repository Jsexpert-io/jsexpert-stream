use std::sync::Arc;

use futures::StreamExt;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    ClientConfig,
};
use tracing::{info, warn};

use crate::{
    config::Config,
    domain::{EventType, KafkaEvent},
    infrastructure::clickhouse::ClickHouseEventStore,
    transport::kafka::EventPublisher,
};

pub struct EventConsumer {
    consumer: StreamConsumer,
    event_store: ClickHouseEventStore,
    publisher: EventPublisher,
    topics: Vec<String>,
}

impl EventConsumer {
    pub async fn connect(config: Arc<Config>) -> anyhow::Result<Self> {
        let event_store = ClickHouseEventStore::connect(&config.clickhouse_url).await?;
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &config.kafka_brokers)
            .set("group.id", &config.consumer_group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "45000")
            .set("max.poll.interval.ms", "300000")
            .create()?;
        let topics = [EventType::Trace, EventType::Error, EventType::Activity]
            .into_iter()
            .map(|event_type| config.topic(event_type))
            .collect();
        let publisher = EventPublisher::connect(config)?;
        Ok(Self {
            consumer,
            event_store,
            publisher,
            topics,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let topic_refs: Vec<_> = self.topics.iter().map(String::as_str).collect();
        self.consumer.subscribe(&topic_refs)?;
        info!(topics = ?self.topics, "Kafka consumer started");

        let mut stream = self.consumer.stream();
        while let Some(message) = stream.next().await {
            let message = match message {
                Ok(message) => message,
                Err(error) => {
                    warn!(?error, "Kafka consumer error");
                    continue;
                }
            };
            let payload = match message.payload_view::<str>() {
                Some(Ok(payload)) => payload,
                _ => {
                    self.publisher
                        .publish_dlq("message had no UTF-8 payload", &[])
                        .await?;
                    self.consumer.commit_message(&message, CommitMode::Async)?;
                    continue;
                }
            };
            let event: KafkaEvent = match serde_json::from_str(payload) {
                Ok(event) => event,
                Err(error) => {
                    self.publisher
                        .publish_dlq(&error.to_string(), payload.as_bytes())
                        .await?;
                    self.consumer.commit_message(&message, CommitMode::Async)?;
                    continue;
                }
            };

            match self.event_store.persist(&event).await {
                Ok(()) => self.consumer.commit_message(&message, CommitMode::Async)?,
                Err(error) => {
                    // An uncommitted offset is retried after a rebalance or restart.
                    warn!(?error, event_id = %event.event_id, "event persistence failed; retrying later");
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
        Ok(())
    }
}
