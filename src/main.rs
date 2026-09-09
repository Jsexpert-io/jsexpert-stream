mod auth;
mod config;
mod domain;
mod error;
mod infrastructure;
mod transport;

use std::sync::Arc;

use auth::ProjectAuthenticator;
use axum::{
    http::header::HeaderName,
    routing::{get, post},
    Router,
};
use config::Config;
use infrastructure::consumer::EventConsumer;
use tokio::task::JoinSet;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::{
    request_id::{MakeRequestUuid, PropagateRequestIdLayer, SetRequestIdLayer},
    trace::TraceLayer,
};
use tracing::info;
use transport::{
    http::{ingest_event, ingest_otlp_trace, live, ready, AppState},
    kafka::EventPublisher,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .json()
        .init();

    let config = Arc::new(Config::from_env()?);
    let authenticator = ProjectAuthenticator::connect(&config.database_url).await?;
    let publisher = EventPublisher::connect(config.clone())?;
    let state = AppState::new(authenticator, publisher);
    let consumer = EventConsumer::connect(config.clone()).await?;

    let mut tasks = JoinSet::new();
    tasks.spawn(consumer.run());

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
