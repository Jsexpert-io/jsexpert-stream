use axum::{extract::State, http::StatusCode, Json};
use serde_json::{json, Value};

use crate::{
    auth::ProjectAuthenticator,
    domain::{IncomingEvent, KafkaEvent},
    error::ApiError,
    transport::kafka::EventPublisher,
};

#[derive(Clone)]
pub struct AppState {
    authenticator: ProjectAuthenticator,
    publisher: EventPublisher,
}

impl AppState {
    pub fn new(authenticator: ProjectAuthenticator, publisher: EventPublisher) -> Self {
        Self {
            authenticator,
            publisher,
        }
    }
}

pub async fn ingest_event(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(input): Json<IncomingEvent>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    input.validate().map_err(ApiError::BadRequest)?;
    let identity = state.authenticator.authenticate(&headers).await?;
    let event = input.into_kafka_event(identity);
    accept_event(&state.publisher, event).await
}

pub async fn ingest_otlp_trace(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let identity = state.authenticator.authenticate(&headers).await?;
    accept_event(&state.publisher, KafkaEvent::otlp_trace(identity, payload)).await
}

pub async fn live() -> StatusCode {
    StatusCode::NO_CONTENT
}

pub async fn ready(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
    state
        .authenticator
        .is_ready()
        .await
        .then_some(StatusCode::NO_CONTENT)
        .ok_or(ApiError::Unavailable)
}

async fn accept_event(
    publisher: &EventPublisher,
    event: KafkaEvent,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    publisher
        .publish(&event)
        .await
        .map_err(|_| ApiError::Unavailable)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({ "event_id": event.event_id, "status": "accepted" })),
    ))
}
