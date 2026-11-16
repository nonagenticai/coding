use axum::Json;
use axum::extract::State;
use serde::Serialize;
use utoipa::ToSchema;

use crate::AppState;
use crate::error::AppError;

#[derive(Debug, Serialize, ToSchema)]
pub struct Health {
    /// Always `ok` when the process is serving HTTP.
    pub status: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Readiness {
    pub status: String,
    /// Result of `SELECT 1` against the runtime pool.
    pub database: String,
}

/// Liveness probe.
///
/// Answers 200 while the HTTP server runs. It does not touch the database, so a DB outage does
/// not make the orchestrator restart a healthy process.
#[utoipa::path(
    get,
    path = "/health",
    tag = "health",
    responses((status = 200, description = "Service is up", body = Health))
)]
pub async fn health() -> Json<Health> {
    Json(Health {
        status: "ok".to_owned(),
    })
}

/// Liveness under the API prefix, so generated clients have one root.
#[utoipa::path(
    get,
    path = "/api/health",
    tag = "health",
    responses((status = 200, description = "Service is up", body = Health))
)]
pub async fn api_health() -> Json<Health> {
    health().await
}

/// Readiness. Fails with `503` when the database is unreachable, which is what a rollout should
/// gate on (unlike liveness).
#[utoipa::path(
    get,
    path = "/api/health/ready",
    tag = "health",
    responses(
        (status = 200, description = "Database reachable", body = Readiness),
        (status = 503, description = "Database unreachable"),
    )
)]
pub async fn ready(State(state): State<AppState>) -> Result<Json<Readiness>, AppError> {
    sqlx::query("SELECT 1")
        .execute(&state.pool)
        .await
        .map_err(|e| AppError::Unavailable(format!("database: {e}")))?;
    Ok(Json(Readiness {
        status: "ok".to_owned(),
        database: "ok".to_owned(),
    }))
}
