use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Trace,
    Error,
    Activity,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Trace => "trace",
            Self::Error => "error",
            Self::Activity => "activity",
        })
    }
}

#[derive(Debug, Deserialize)]
pub struct IncomingEvent {
    #[serde(default)]
    pub event_id: Option<Uuid>,
    pub event_type: EventType,
    #[serde(default)]
    pub occurred_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub event_name: Option<String>,
    #[serde(default)]
    pub actor_id: Option<String>,
    #[serde(default)]
    pub trace_id: Option<String>,
    pub payload: Value,
}

impl IncomingEvent {
    pub fn validate(&self) -> Result<(), String> {
        if self.payload.is_null() {
            return Err("payload must not be null".into());
        }
        validate_length("event_name", self.event_name.as_deref())?;
        validate_length("actor_id", self.actor_id.as_deref())?;
        Ok(())
    }

    pub fn into_kafka_event(self, identity: ProjectIdentity) -> KafkaEvent {
        KafkaEvent {
            schema_version: 1,
            event_id: self.event_id.unwrap_or_else(Uuid::new_v4),
            tenant_id: identity.tenant_id,
            project_id: identity.project_id,
            event_type: self.event_type,
            occurred_at: self.occurred_at.unwrap_or_else(Utc::now),
            received_at: Utc::now(),
            event_name: self.event_name,
            actor_id: self.actor_id,
            trace_id: self.trace_id,
            payload: self.payload,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct KafkaEvent {
    pub schema_version: u8,
    pub event_id: Uuid,
    pub tenant_id: String,
    pub project_id: String,
    pub event_type: EventType,
    pub occurred_at: DateTime<Utc>,
    pub received_at: DateTime<Utc>,
    pub event_name: Option<String>,
    pub actor_id: Option<String>,
    pub trace_id: Option<String>,
    pub payload: Value,
}

impl KafkaEvent {
    pub fn partition_key(&self) -> String {
        format!("{}:{}:{}", self.tenant_id, self.project_id, self.event_type)
    }

    pub fn otlp_trace(identity: ProjectIdentity, payload: Value) -> Self {
        Self {
            schema_version: 1,
            event_id: Uuid::new_v4(),
            tenant_id: identity.tenant_id,
            project_id: identity.project_id,
            event_type: EventType::Trace,
            occurred_at: Utc::now(),
            received_at: Utc::now(),
            event_name: Some("otlp.trace.export".into()),
            actor_id: None,
            trace_id: None,
            payload,
        }
    }
}

#[derive(Clone, Debug, FromRow)]
pub struct ProjectIdentity {
    pub project_id: String,
    pub tenant_id: String,
}

fn validate_length(field_name: &str, value: Option<&str>) -> Result<(), String> {
    if value.is_some_and(|value| value.len() > 200) {
        return Err(format!("{field_name} must be 200 characters or fewer"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde_json::json;
    use uuid::Uuid;

    use super::{EventType, KafkaEvent};

    #[test]
    fn partition_key_keeps_a_project_event_stream_together() {
        let event = KafkaEvent {
            schema_version: 1,
            event_id: Uuid::nil(),
            tenant_id: "tenant-a".into(),
            project_id: "project-b".into(),
            event_type: EventType::Activity,
            occurred_at: Utc::now(),
            received_at: Utc::now(),
            event_name: None,
            actor_id: None,
            trace_id: None,
            payload: json!({}),
        };

        assert_eq!(event.partition_key(), "tenant-a:project-b:activity");
    }
}
