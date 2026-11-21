//! Source uploads: how a working tree that does not fit in a 64 KiB Pod spec reaches a runner.
//!
//! * `POST /api/sources`   — store one gzip tar of a working tree, tenant-scoped, and return its
//!   id. The body is read here rather than through an extractor, so the 32 MiB cap is enforced by
//!   this handler instead of axum's 2 MiB default.
//! * `GET /api/sources/{id}/blob` — the archive, for a Pod that holds the capability token the
//!   worker minted. No tenant header: the token *is* the capability, and it is the only thing
//!   that makes an id readable.
//!
//! Neither route lets a caller name a URL, a token or a hash — those belong to the server.

use std::sync::OnceLock;

use axum::Json;
use axum::extract::{Path, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use tokio::sync::Semaphore;
use utoipa::ToSchema;

use crate::AppState;
use crate::error::AppError;
use crate::source_archive;
use crate::store;
use crate::tenant::{TenantHeader, TenantId};

/// How many uploads one process buffers at once. The handler holds the compressed archive in
/// memory to hash it, validate it and store it, so N concurrent uploads are N × 32 MiB.
const MAX_CONCURRENT_UPLOADS: usize = 2;
/// How long an upload waits for a slot before the app says "busy". Waiting is right: the slot is
/// a memory guard, not a refusal, and a caller that is told to retry would only send 32 MiB again.
const UPLOAD_SLOT_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
/// How long the body may take to arrive once the slot is held. A slot is a memory guard, so a
/// client that connects and trickles must not hold one forever; 180 s still lets a 32 MiB archive
/// arrive over a ~1.5 Mbit/s link, and the client's own upload timeout is longer.
const UPLOAD_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(180);

/// The process-wide upload gate. A `OnceLock` because the limit is a property of the process's
/// memory, not of a request or of the database handle.
static UPLOAD_SLOTS: OnceLock<Semaphore> = OnceLock::new();

fn upload_slots() -> &'static Semaphore {
    UPLOAD_SLOTS.get_or_init(|| Semaphore::new(MAX_CONCURRENT_UPLOADS))
}

/// What an upload returns.
#[derive(Debug, Serialize, ToSchema)]
pub struct SourceCreated {
    /// Name this id in `input.source` as `{"kind": "upload", "id": "…"}`.
    pub id: String,
    /// sha256 of the archive, exactly as stored. The runner verifies what it downloads.
    pub sha256: String,
    /// Compressed size in bytes.
    pub bytes: i64,
    /// Number of files in the archive.
    pub files: usize,
}

/// Upload one gzip tar of a working tree.
#[utoipa::path(
    post,
    path = "/api/sources",
    tag = "sources",
    params(TenantHeader),
    request_body(content = String, content_type = "application/gzip",
                 description = "gzip-compressed tar; sorted relative paths, no links"),
    responses(
        (status = 201, description = "Archive stored", body = SourceCreated),
        (status = 400, description = "Not a gzip tar, or it breaks a path/entry rule"),
        (status = 401, description = "Missing or invalid tenant"),
        (status = 413, description = "Larger than the archive cap"),
        (status = 503, description = "Too many uploads in flight right now; retry"),
    )
)]
pub async fn create_source(
    State(state): State<AppState>,
    tenant: TenantId,
    request: Request,
) -> Response {
    let permit = match tokio::time::timeout(UPLOAD_SLOT_WAIT, upload_slots().acquire()).await {
        Ok(Ok(permit)) => permit,
        Ok(Err(_)) => {
            // The semaphore is never closed; this arm exists so the handler cannot panic.
            return AppError::Unavailable("the upload gate is closed".to_owned()).into_response();
        }
        Err(_) => {
            return AppError::Unavailable(format!(
                "at most {MAX_CONCURRENT_UPLOADS} uploads are buffered at a time; retry shortly"
            ))
            .into_response();
        }
    };

    let limit = source_archive::MAX_COMPRESSED_BYTES;
    let read = tokio::time::timeout(
        UPLOAD_READ_TIMEOUT,
        axum::body::to_bytes(request.into_body(), limit + 1),
    )
    .await;
    let Ok(read) = read else {
        return AppError::RequestTimeout(format!(
            "the archive did not arrive within {}s",
            UPLOAD_READ_TIMEOUT.as_secs()
        ))
        .into_response();
    };
    let Ok(body) = read else {
        return AppError::PayloadTooLarge(format!(
            "the archive could not be read in full, or is over the {limit}-byte cap"
        ))
        .into_response();
    };
    if body.len() > limit {
        return AppError::PayloadTooLarge(format!(
            "the archive is {} bytes, over the {limit}-byte cap",
            body.len()
        ))
        .into_response();
    }

    // Validation inflates the archive (up to 128 MiB) and hashes it: pure CPU that must not run
    // on a Tokio worker. Two such uploads on one 2-vCPU API pod would otherwise stall every
    // request, including the k8s probes, for the duration of the inflate.
    let validating = body.clone();
    let validated = tokio::task::spawn_blocking(move || {
        let manifest = source_archive::validate(&validating)?;
        let sha256 = source_archive::sha256_hex(&validating);
        Ok::<_, source_archive::ArchiveError>((manifest, sha256))
    })
    .await;
    let (manifest, sha256) = match validated {
        Ok(Ok(validated)) => validated,
        Ok(Err(error)) => return AppError::BadRequest(error.to_string()).into_response(),
        Err(error) => {
            tracing::error!(error = %error, "the archive validator task failed");
            return AppError::Unavailable("the archive validator stopped unexpectedly".to_owned())
                .into_response();
        }
    };
    let Ok(files) = i32::try_from(manifest.files) else {
        return AppError::BadRequest("the archive carries too many files".to_owned())
            .into_response();
    };

    // The body was capped by `limit + 1` bytes above, far inside `i64`; a length that still
    // overflowed is answered as too large rather than panicking.
    let Ok(bytes) = i64::try_from(body.len()) else {
        return AppError::PayloadTooLarge("the archive is too large to record".to_owned())
            .into_response();
    };
    let created =
        store::create_source(&state.pool, tenant.as_str(), &sha256, bytes, files, &body).await;
    drop(permit);

    match created {
        Ok(id) => (
            StatusCode::CREATED,
            Json(SourceCreated {
                id,
                sha256,
                bytes,
                files: manifest.files,
            }),
        )
            .into_response(),
        Err(error) => error.into_response(),
    }
}

/// The archive behind a capability token.
///
/// The token travels in the `x-source-token` header — never in the URI, which the trace layer
/// logs. An unknown id, a wrong token and an expired token all answer `404`: the id alone must
/// not tell a caller whether a source exists.
#[utoipa::path(
    get,
    path = "/api/sources/{id}/blob",
    tag = "sources",
    params(("id" = String, Path, description = "Source id")),
    responses(
        (status = 200, description = "The stored archive", content_type = "application/gzip"),
        (status = 400, description = "Malformed source id"),
        (status = 404, description = "Unknown source, wrong token, or expired token"),
    )
)]
pub async fn source_blob(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let token = headers
        .get("x-source-token")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if token.is_empty() {
        // Same answer as a wrong token: an absent credential is not a different kind of no.
        return AppError::NotFound(format!("source {id}")).into_response();
    }
    match store::source_blob(&state.pool, &id, token).await {
        Ok(Some(blob)) => {
            let mut response = blob.into_response();
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/gzip"),
            );
            response
        }
        Ok(None) => AppError::NotFound(format!("source {id}")).into_response(),
        Err(error) => error.into_response(),
    }
}
