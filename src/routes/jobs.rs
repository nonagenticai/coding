//! Job routes. The async contract conveyor calls:
//!
//! * `POST /api/jobs`          — create (202) — `coding_jobs.create`
//! * `POST /api/jobs/status`   — read by id in a body (200) — `coding_jobs.get`
//!
//! Both paths and methods are exactly what conveyor's ActionSpec declares; the bodies are this
//! service's own contract, frozen by the HTTP tests.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::AppState;
use crate::domain::{
    CreateJobRequest, ErrorBody, ErrorDetail, InputError, Job, JobStatusRequest, Source,
};
use crate::error::AppError;
use crate::store;
use crate::tenant::{TenantHeader, TenantId};

/// Create a job.
#[utoipa::path(
    post,
    path = "/api/jobs",
    tag = "jobs",
    params(TenantHeader),
    request_body = CreateJobRequest,
    responses(
        (status = 202, description = "Job accepted", body = Job),
        (status = 400, description = "Invalid request"),
        (status = 401, description = "Missing or invalid tenant"),
        (status = 404, description = "Referenced workspace not found"),
    )
)]
pub async fn create_job(
    State(state): State<AppState>,
    tenant: TenantId,
    Json(req): Json<CreateJobRequest>,
) -> Response {
    if req.timeout_seconds == 0 || req.timeout_seconds > 86_400 {
        return AppError::BadRequest("timeout_seconds must be between 1 and 86400".into())
            .into_response();
    }
    if let Err(error) = req.input.validate(req.kind) {
        return invalid_input(error);
    }
    // An uploaded source must already belong to this tenant: naming another tenant's id would
    // otherwise let a job read an archive the caller never uploaded.
    if let Some(Source::Upload { id, .. }) = &req.input.source {
        match store::owns_source(&state.pool, tenant.as_str(), id).await {
            Ok(true) => {}
            Ok(false) => return AppError::NotFound(format!("source {id}")).into_response(),
            Err(error) => return error.into_response(),
        }
    }
    // `JobInput` is a closed shape of strings, integers and vectors, so this cannot fail; it is
    // still handled rather than unwrapped because a panic in a handler is a dropped connection.
    let input = match serde_json::to_value(&req.input) {
        Ok(input) => input,
        Err(error) => {
            tracing::error!(error = %error, "job input could not be serialized");
            return AppError::BadRequest("input is not serializable".into()).into_response();
        }
    };
    match store::create_job(
        &state.pool,
        tenant.as_str(),
        req.kind,
        req.workspace_id.as_deref(),
        &input,
        req.idempotency_key.as_deref(),
        req.timeout_seconds,
    )
    .await
    {
        Ok(job) => (StatusCode::ACCEPTED, Json(job)).into_response(),
        Err(error) => error.into_response(),
    }
}

/// Render an [`InputError`] as the service's one error envelope. The shared [`AppError`] has no
/// variant for a caller-specific code, so the envelope is built here instead of widening it.
fn invalid_input(error: InputError) -> Response {
    let body = ErrorBody {
        error: ErrorDetail {
            code: error.code.to_owned(),
            message: error.message,
            details: None,
            retryable: false,
        },
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}

/// Read a job by id carried in the body. A POST because conveyor's action engine cannot template
/// a path and a GET carries no body.
#[utoipa::path(
    post,
    path = "/api/jobs/status",
    tag = "jobs",
    params(TenantHeader),
    request_body = JobStatusRequest,
    responses(
        (status = 200, description = "Current job state", body = Job),
        (status = 400, description = "Invalid job id"),
        (status = 401, description = "Missing or invalid tenant"),
        (status = 404, description = "Job not found for this tenant"),
    )
)]
pub async fn job_status(
    State(state): State<AppState>,
    tenant: TenantId,
    Json(req): Json<JobStatusRequest>,
) -> Result<Json<Job>, AppError> {
    Ok(Json(
        store::get_job(&state.pool, tenant.as_str(), &req.id).await?,
    ))
}

/// Read a job by path id (human/UI use; same body as `/api/jobs/status`).
#[utoipa::path(
    get,
    path = "/api/jobs/{id}",
    tag = "jobs",
    params(TenantHeader, ("id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Current job state", body = Job),
        (status = 400, description = "Invalid job id"),
        (status = 404, description = "Job not found for this tenant"),
    )
)]
pub async fn get_job(
    State(state): State<AppState>,
    tenant: TenantId,
    Path(id): Path<String>,
) -> Result<Json<Job>, AppError> {
    Ok(Json(
        store::get_job(&state.pool, tenant.as_str(), &id).await?,
    ))
}

/// Request cancellation. Queued jobs cancel immediately; running jobs are asked to stop and their
/// sandbox Pod is deleted by the worker.
#[utoipa::path(
    post,
    path = "/api/jobs/{id}/cancel",
    tag = "jobs",
    params(TenantHeader, ("id" = String, Path, description = "Job id")),
    responses(
        (status = 200, description = "Job after the cancellation request", body = Job),
        (status = 404, description = "Job not found for this tenant"),
    )
)]
pub async fn cancel_job(
    State(state): State<AppState>,
    tenant: TenantId,
    Path(id): Path<String>,
) -> Result<Json<Job>, AppError> {
    Ok(Json(
        store::cancel_job(&state.pool, tenant.as_str(), &id).await?,
    ))
}

#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    #[serde(default)]
    pub after: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JobEvent {
    pub seq: i64,
    pub kind: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JobEventPage {
    pub events: Vec<JobEvent>,
    /// Pass as `after` to fetch the next page. Unchanged when there is nothing new.
    pub next: i64,
}

/// Read the job's event log after a cursor.
///
/// M0 returns a JSON page; the streaming (SSE) form lands with the sandbox milestone, when a
/// streaming dependency is added deliberately rather than by transitive accident.
#[utoipa::path(
    get,
    path = "/api/jobs/{id}/events",
    tag = "jobs",
    params(
        TenantHeader,
        ("id" = String, Path, description = "Job id"),
        ("after" = Option<i64>, Query, description = "Return events with seq greater than this"),
    ),
    responses(
        (status = 200, description = "Event page", body = JobEventPage),
        (status = 404, description = "Job not found for this tenant"),
    )
)]
pub async fn job_events(
    State(state): State<AppState>,
    tenant: TenantId,
    Path(id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<JobEventPage>, AppError> {
    // Tenant-scope first: an unknown job is 404, never an empty page.
    let _ = store::get_job(&state.pool, tenant.as_str(), &id).await?;
    let rows = store::events_since(&state.pool, tenant.as_str(), &id, query.after, 500).await?;
    let next = rows.last().map(|(seq, _, _)| *seq).unwrap_or(query.after);
    Ok(Json(JobEventPage {
        events: rows
            .into_iter()
            .map(|(seq, kind, payload)| JobEvent { seq, kind, payload })
            .collect(),
        next,
    }))
}
