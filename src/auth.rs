use axum::http::{header::HeaderName, HeaderMap};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tracing::error;

use crate::{domain::ProjectIdentity, error::ApiError};

const CLIENT_ID_HEADER: HeaderName = HeaderName::from_static("clientid");
const CLIENT_SECRET_HEADER: HeaderName = HeaderName::from_static("clientsecret");

#[derive(Clone)]
pub struct ProjectAuthenticator {
    projects: PgPool,
}

impl ProjectAuthenticator {
    pub async fn connect(database_url: &str) -> anyhow::Result<Self> {
        let projects = PgPoolOptions::new()
            .max_connections(20)
            .connect(database_url)
            .await?;
        Ok(Self { projects })
    }

    pub async fn authenticate(&self, headers: &HeaderMap) -> Result<ProjectIdentity, ApiError> {
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
        .fetch_optional(&self.projects)
        .await
        .map_err(|error| {
            error!(?error, "project credential lookup failed");
            ApiError::Unavailable
        })?
        .ok_or(ApiError::Unauthorized)
    }

    pub async fn is_ready(&self) -> bool {
        sqlx::query("SELECT 1")
            .execute(&self.projects)
            .await
            .is_ok()
    }
}

fn header_value<'a>(headers: &'a HeaderMap, name: &HeaderName) -> Result<&'a str, ApiError> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or(ApiError::Unauthorized)
}
