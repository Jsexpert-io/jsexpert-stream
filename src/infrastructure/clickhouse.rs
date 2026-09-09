use chrono::{DateTime, Utc};
use clickhouse::{Client, Row};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::domain::{EventType, KafkaEvent};

#[derive(Clone)]
pub struct ClickHouseEventStore {
    client: Client,
}

impl ClickHouseEventStore {
    pub async fn connect(clickhouse_url: &str) -> anyhow::Result<Self> {
        let admin = Client::default().with_url(clickhouse_url);
        create_schema(&admin).await?;
        Ok(Self {
            client: admin.with_database("jsexpertdb"),
        })
    }

    pub async fn persist(&self, event: &KafkaEvent) -> anyhow::Result<()> {
        let stored = StoredEvent::from_event(event)?;
        let mut insert = self.client.insert("ingest_events")?;
        insert.write(&stored).await?;
        insert.end().await?;
        Ok(())
    }
}

#[derive(Clone, Debug, Row, Serialize)]
struct StoredEvent {
    event_id: Uuid,
    tenant_id: String,
    project_id: String,
    event_type: String,
    occurred_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    event_name: String,
    actor_id: String,
    trace_id: String,
    error_fingerprint: String,
    payload: String,
}

impl StoredEvent {
    fn from_event(event: &KafkaEvent) -> anyhow::Result<Self> {
        Ok(Self {
            event_id: event.event_id,
            tenant_id: event.tenant_id.clone(),
            project_id: event.project_id.clone(),
            event_type: event.event_type.to_string(),
            occurred_at: event.occurred_at,
            received_at: event.received_at,
            event_name: event.event_name.clone().unwrap_or_default(),
            actor_id: event.actor_id.clone().unwrap_or_default(),
            trace_id: event.trace_id.clone().unwrap_or_default(),
            error_fingerprint: error_fingerprint(event),
            payload: serde_json::to_string(&event.payload)?,
        })
    }
}

async fn create_schema(client: &Client) -> anyhow::Result<()> {
    client
        .query("CREATE DATABASE IF NOT EXISTS jsexpertdb")
        .execute()
        .await?;
    client
        .query(
            "
        CREATE TABLE IF NOT EXISTS jsexpertdb.ingest_events (
            event_id UUID,
            tenant_id String,
            project_id String,
            event_type LowCardinality(String),
            occurred_at DateTime64(3, 'UTC'),
            received_at DateTime64(3, 'UTC'),
            event_name String,
            actor_id String,
            trace_id String,
            error_fingerprint String,
            payload String
        ) ENGINE = ReplacingMergeTree(received_at)
        PARTITION BY toYYYYMM(occurred_at)
        ORDER BY (tenant_id, project_id, event_type, event_id)
        ",
        )
        .execute()
        .await?;
    Ok(())
}

fn error_fingerprint(event: &KafkaEvent) -> String {
    if !matches!(event.event_type, EventType::Error) {
        return String::new();
    }
    let source = format!(
        "{}|{}|{}",
        payload_string(&event.payload, "name"),
        payload_string(&event.payload, "message"),
        payload_string(&event.payload, "stack"),
    );
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

fn payload_string<'a>(payload: &'a Value, key: &str) -> &'a str {
    payload.get(key).and_then(Value::as_str).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::error_fingerprint;
    use crate::domain::{EventType, KafkaEvent};

    #[test]
    fn error_fingerprint_is_stable() {
        let event = KafkaEvent {
            schema_version: 1,
            event_id: Uuid::nil(),
            tenant_id: "tenant".into(),
            project_id: "project".into(),
            event_type: EventType::Error,
            occurred_at: Utc::now(),
            received_at: Utc::now(),
            event_name: None,
            actor_id: None,
            trace_id: None,
            payload: json!({ "name": "TypeError", "message": "bad", "stack": "at one" }),
        };

        assert_eq!(error_fingerprint(&event), error_fingerprint(&event));
    }
}
