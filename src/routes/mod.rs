pub mod health;
pub mod jobs;
pub mod sources;

use axum::Json;

/// The served OpenAPI document. Not registered through `routes!` (that would document the
/// document endpoint recursively); clients fetch it from here.
#[utoipa::path(
    get,
    path = "/api/openapi.json",
    tag = "meta",
    responses((status = 200, description = "OpenAPI 3 document"))
)]
pub async fn openapi_json() -> Json<utoipa::openapi::OpenApi> {
    Json(crate::openapi())
}
