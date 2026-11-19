#![forbid(unsafe_code)]
//! Service library: router, `OpenAPI` document, configuration, database and the reusable
//! trusted-header tenant extractor. `main.rs` is a thin wrapper so integration tests can
//! build the exact router the binary serves.

pub mod client;
pub mod config;
pub mod db;
pub mod deliver;
pub mod domain;
pub mod error;
pub mod github_app;
pub mod routes;
pub mod runner;
pub mod sandbox;
pub mod source_archive;
pub mod store;
pub mod telemetry;
pub mod tenant;
pub mod worker;

use axum::Router;
use axum::routing::get;
use sqlx::PgPool;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

/// Shared state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
    /// `CODING_DEFAULT_TENANT`: used when a request carries no `X-Tenant-Id`. Set only for
    /// in-cluster callers that have no trusted edge in front of them (conveyor). Unset is
    /// fail-closed: a missing header is 401.
    pub default_tenant: Option<String>,
}

/// Document-level `OpenAPI` metadata. `info` (title, version, description) comes from
/// Cargo.toml, so renaming the crate renames the spec.
#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "health", description = "Liveness and readiness"),
        (name = "jobs", description = "Asynchronous work: create, poll, cancel, follow"),
        (name = "sources", description = "Upload a working tree a job can run against")
    )
)]
struct ApiDoc;

/// Every route is registered here, through `routes!`, so the served router and the
/// `OpenAPI` document cannot drift: a handler without `#[utoipa::path]` does not compile here.
fn api_router() -> OpenApiRouter<AppState> {
    OpenApiRouter::with_openapi(ApiDoc::openapi())
        .routes(routes!(routes::health::health))
        .routes(routes!(routes::health::api_health))
        .routes(routes!(routes::health::ready))
        .routes(routes!(routes::jobs::create_job))
        .routes(routes!(routes::jobs::job_status))
        .routes(routes!(routes::jobs::get_job))
        .routes(routes!(routes::jobs::cancel_job))
        .routes(routes!(routes::jobs::job_events))
        .routes(routes!(routes::sources::create_source))
        .routes(routes!(routes::sources::source_blob))
}

/// The `OpenAPI` document of this service (what `--print-openapi` writes). Needs no env and no DB.
#[must_use]
pub fn openapi() -> utoipa::openapi::OpenApi {
    let (_, spec) = api_router().split_for_parts();
    spec
}

/// The HTTP application the binary serves.
pub fn app(state: AppState) -> Router {
    let (router, _) = api_router().split_for_parts();
    router
        .route("/api/openapi.json", get(routes::openapi_json))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}
