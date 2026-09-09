use std::env;

use crate::domain::EventType;

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: String,
    pub database_url: String,
    pub clickhouse_url: String,
    pub kafka_brokers: String,
    pub topic_prefix: String,
    pub consumer_group: String,
    pub max_event_bytes: usize,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        Ok(Self {
            listen_addr: required_env("LISTEN_ADDR")?,
            database_url: required_env("DATABASE_URL")?,
            clickhouse_url: required_env("CLICKHOUSE_URL")?,
            kafka_brokers: required_env("KAFKA_BROKERS")?,
            topic_prefix: env::var("KAFKA_TOPIC_PREFIX")
                .unwrap_or_else(|_| "jsexpert.events".into()),
            consumer_group: env::var("KAFKA_CONSUMER_GROUP")
                .unwrap_or_else(|_| "jsexpert-stream-v1".into()),
            max_event_bytes: env::var("MAX_EVENT_BYTES")
                .unwrap_or_else(|_| "1048576".into())
                .parse()?,
        })
    }

    pub fn topic(&self, event_type: EventType) -> String {
        format!("{}.{}", self.topic_prefix, event_type)
    }

    pub fn dlq_topic(&self) -> String {
        format!("{}.dlq", self.topic_prefix)
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).map_err(|_| anyhow::anyhow!("{name} must be configured"))
}
