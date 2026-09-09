use std::{env, sync::Arc, time::Duration};

use axum::{
    extract::State,
    http::{header::HeaderName, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use clickhouse::{Client as ClickHouseClient, Row};
use futures::StreamExt;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    message::Message,
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{postgres::PgPoolOptions, FromRow, PgPool};
use thiserror::Error;
use tokio::task::JoinSet;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::{error, info, warn};
use uuid::Uuid;

const CLIENT_ID_HEADER: HeaderName = HeaderName::from_static("clientid");
const CLIENT_SECRET_HEADER: HeaderName = HeaderName::from_static("clientsecret");

#[derive(Clone, Debug)]
struct Config {
    listen_addr: String,
    database_url: String,
    clickhouse_url: String,
    kafka_brokers: String,
    topic_prefix: String,
    consumer_group: String,
    max_event_bytes: usize,
}

impl Config {
    fn from_env() -> anyhow::Result<Self> {
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

    fn topic(&self, event_type: EventType) -> String {
        format!("{}.{}", self.topic_prefix, event_type)
    }

    fn dlq_topic(&self) -> String {
        format!("{}.dlq", self.topic_prefix)
    }
}

fn required_env(name: &str) -> anyhow::Result<String> {
    env::var(name).map_err(|_| anyhow::anyhow!("{name} must be configured"))
}

#[derive(Clone)]
struct AppState {
    projects: PgPool,
    producer: FutureProducer,
    config: Arc<Config>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventType {
    Trace,
    Error,
    Activity,
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Trace => "trace",
            Self::Error => "error",
            Self::Activity => "activity",
        })
    }
}

#[derive(Debug, Deserialize)]
struct IncomingEvent {
    #[serde(default)]
    event_id: Option<Uuid>,
    event_type: EventType,
    #[serde(default)]
    occurred_at: Option<DateTime<Utc>>,
    #[serde(default)]
    event_name: Option<String>,
    #[serde(default)]
    actor_id: Option<String>,
    #[serde(default)]
    trace_id: Option<String>,
    payload: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct KafkaEvent {
    schema_version: u8,
    event_id: Uuid,
    tenant_id: String,
    project_id: String,
    event_type: EventType,
    occurred_at: DateTime<Utc>,
    received_at: DateTime<Utc>,
    event_name: Option<String>,
    actor_id: Option<String>,
    trace_id: Option<String>,
    payload: Value,
}

impl KafkaEvent {
    fn partition_key(&self) -> String {
        format!("{}:{}:{}", self.tenant_id, self.project_id, self.event_type)
    }
}

#[derive(FromRow)]
struct ProjectIdentity {
    project_id: String,
    tenant_id: String,
}

#[derive(Debug, Error)]
enum ApiError {
    #[error("missing or invalid project credentials")]
    Unauthorized,
    #[error("invalid event: {0}")]
    BadRequest(String),
    #[error("event pipeline is temporarily unavailable")]
    Unavailable,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

async fn project_identity(headers: &HeaderMap, pool: &PgPool) -> Result<ProjectIdentity, ApiError> {
    let client_id = header_value(headers, &CLIENT_ID_HEADER)?;
    let client_secret = header_value(headers, &CLIENT_SECRET_HEADER)?;

    sqlx::query_as::<_, ProjectIdentity>(
        r#"
        SELECT id AS project_id, "userId" AS tenant_id
        FROM "Project"
        WHERE "clientId" = $1 AND "clientSecret" = $2 AND "isActive" = true
        "#,
    )
    .bind(client_id)
    .bind(client_secret)
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        error!(?error, "project credential lookup failed");
        ApiError::Unavailable
    })?
    .ok_or(ApiError::Unauthorized)
}

fn header_value(headers: &HeaderMap, name: &HeaderName) -> Result<&str, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::Unauthorized)
}

async fn ingest_event(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(input): Json<IncomingEvent>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let identity = project_identity(&headers, &state.projects).await?;
    validate_event(&input)?;
    let event = KafkaEvent {
        schema_version: 1,
        event_id: input.event_id.unwrap_or_else(Uuid::new_v4),
        tenant_id: identity.tenant_id,
        project_id: identity.project_id,
        event_type: input.event_type,
        occurred_at: input.occurred_at.unwrap_or_else(Utc::now),
        received_at: Utc::now(),
        event_name: input.event_name,
        actor_id: input.actor_id,
        trace_id: input.trace_id,
        payload: input.payload,
    };
    publish(
        &state.producer,
        &state.config.topic(event.event_type),
        &event,
    )
    .await?;

    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "event_id": event.event_id, "status": "accepted" })),
    ))
}

async fn ingest_otlp_trace(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(payload): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let identity = project_identity(&headers, &state.projects).await?;
    let event = KafkaEvent {
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
    };
    publish(
        &state.producer,
        &state.config.topic(EventType::Trace),
        &event,
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "event_id": event.event_id, "status": "accepted" })),
    ))
}

fn validate_event(input: &IncomingEvent) -> Result<(), ApiError> {
    if input.payload.is_null() {
        return Err(ApiError::BadRequest("payload must not be null".into()));
    }
    if input
        .event_name
        .as_ref()
        .is_some_and(|name| name.len() > 200)
    {
        return Err(ApiError::BadRequest(
            "event_name must be 200 characters or fewer".into(),
        ));
    }
    if input
        .actor_id
        .as_ref()
        .is_some_and(|actor| actor.len() > 200)
    {
        return Err(ApiError::BadRequest(
            "actor_id must be 200 characters or fewer".into(),
        ));
    }
    Ok(())
}

async fn publish(
    producer: &FutureProducer,
    topic: &str,
    event: &KafkaEvent,
) -> Result<(), ApiError> {
    let payload = serde_json::to_string(event).map_err(|_| ApiError::Unavailable)?;
    producer
        .send(
            FutureRecord::to(topic)
                .key(&event.partition_key())
                .payload(&payload),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(error, _)| {
            warn!(?error, topic, "Kafka publish failed");
            ApiError::Unavailable
        })?;
    Ok(())
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

async fn create_event_store(client: &ClickHouseClient) -> anyhow::Result<()> {
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

async fn persist_event(client: &ClickHouseClient, event: &KafkaEvent) -> anyhow::Result<()> {
    let payload = serde_json::to_string(&event.payload)?;
    let stored = StoredEvent {
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
        payload,
    };
    let mut insert = client.insert("ingest_events")?;
    insert.write(&stored).await?;
    insert.end().await?;
    Ok(())
}

fn error_fingerprint(event: &KafkaEvent) -> String {
    if !matches!(event.event_type, EventType::Error) {
        return String::new();
    }
    let source = format!(
        "{}|{}|{}",
        event
            .payload
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        event
            .payload
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or_default(),
        event
            .payload
            .get("stack")
            .and_then(Value::as_str)
            .unwrap_or_default()
    );
    format!("{:x}", Sha256::digest(source.as_bytes()))
}

async fn consume(config: Arc<Config>) -> anyhow::Result<()> {
    let admin = ClickHouseClient::default().with_url(&config.clickhouse_url);
    create_event_store(&admin).await?;
    let clickhouse = admin.with_database("jsexpertdb");
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("group.id", &config.consumer_group)
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("auto.offset.reset", "earliest")
        .set("session.timeout.ms", "45000")
        .set("max.poll.interval.ms", "300000")
        .create()?;

    let trace = config.topic(EventType::Trace);
    let error = config.topic(EventType::Error);
    let activity = config.topic(EventType::Activity);
    consumer.subscribe(&[&trace, &error, &activity])?;
    info!(topics = ?[trace, error, activity], "Kafka consumer started");

    let mut stream = consumer.stream();
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
                publish_dlq(&config, "message had no UTF-8 payload", &[]).await?;
                consumer.commit_message(&message, CommitMode::Async)?;
                continue;
            }
        };
        let event: KafkaEvent = match serde_json::from_str(payload) {
            Ok(event) => event,
            Err(error) => {
                publish_dlq(&config, &error.to_string(), payload.as_bytes()).await?;
                consumer.commit_message(&message, CommitMode::Async)?;
                continue;
            }
        };

        match persist_event(&clickhouse, &event).await {
            Ok(()) => consumer.commit_message(&message, CommitMode::Async)?,
            Err(error) => {
                // Do not commit: Kafka will redeliver after a rebalance or restart.
                warn!(?error, event_id = %event.event_id, "event persistence failed; retrying later");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
    Ok(())
}

async fn publish_dlq(config: &Config, reason: &str, payload: &[u8]) -> anyhow::Result<()> {
    let producer = kafka_producer(config)?;
    let body = json!({
        "reason": reason,
        "received_at": Utc::now(),
        "payload": String::from_utf8_lossy(payload),
    });
    producer
        .send(
            FutureRecord::to(&config.dlq_topic())
                .key("consumer-decode-error")
                .payload(&serde_json::to_string(&body)?),
            Duration::from_secs(10),
        )
        .await
        .map_err(|(error, _)| anyhow::anyhow!(error))?;
    Ok(())
}

fn kafka_producer(config: &Config) -> anyhow::Result<FutureProducer> {
    Ok(ClientConfig::new()
        .set("bootstrap.servers", &config.kafka_brokers)
        .set("enable.idempotence", "true")
        .set("acks", "all")
        .set("retries", "2147483647")
        .set("max.in.flight.requests.per.connection", "5")
        .set("compression.type", "zstd")
        .set("delivery.timeout.ms", "30000")
        .create()?)
}

async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn ready(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    sqlx::query("SELECT 1")
        .execute(&state.projects)
        .await
        .map_err(|_| ApiError::Unavailable)?;
    Ok(StatusCode::NO_CONTENT)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let config = Arc::new(Config::from_env()?);
    let projects = PgPoolOptions::new()
        .max_connections(20)
        .connect(&config.database_url)
        .await?;
    let producer = kafka_producer(&config)?;
    let state = AppState {
        projects,
        producer,
        config: config.clone(),
    };

    let mut tasks = JoinSet::new();
    tasks.spawn(consume(config.clone()));

    let app = Router::new()
        .route("/health/live", get(live))
        .route("/health/ready", get(ready))
        .route("/v1/events", post(ingest_event))
        .route("/v1/traces", post(ingest_otlp_trace))
        .with_state(state)
        .layer(ConcurrencyLimitLayer::new(1_000))
        .layer(axum::extract::DefaultBodyLimit::max(config.max_event_bytes))
        .layer(PropagateRequestIdLayer::new(HeaderName::from_static(
            "x-request-id",
        )))
        .layer(SetRequestIdLayer::new(
            HeaderName::from_static("x-request-id"),
            MakeRequestUuid,
        ))
        .layer(TraceLayer::new_for_http());

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    info!(address = %config.listen_addr, "JsExpert stream ingestion service started");
    tokio::select! {
        result = axum::serve(listener, app) => result?,
        result = tasks.join_next() => {
            if let Some(Err(error)) = result {
                return Err(anyhow::anyhow!("consumer task failed: {error}"));
            }
            return Err(anyhow::anyhow!("consumer task stopped unexpectedly"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
