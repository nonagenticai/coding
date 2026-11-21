//! Errors. Every HTTP failure renders as one envelope so a client parses one shape:
//!
//! ```json
//! {"error": {"code": "job_not_found", "message": "...", "details": null, "retryable": false}}
//! ```
//!
//! This is deliberately NOT the template's FastAPI-style `{"detail": "..."}`: the new API is a
//! fresh contract (see the plan), and every caller is generated from the `OpenAPI` document.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

use crate::domain::{ErrorBody, ErrorDetail};

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    Unavailable(String),
    /// A body larger than the route accepts. Kept apart from `BadRequest` so a client can tell
    /// "your tree is too big" from "your tree is malformed" without parsing the message.
    #[error("{0}")]
    PayloadTooLarge(String),
    /// The request did not finish arriving inside the route's read budget. Distinct from
    /// `PayloadTooLarge`: the archive may be well within the cap, the sender was too slow.
    #[error("{0}")]
    RequestTimeout(String),
    #[error("database error")]
    Database(#[from] sqlx::Error),
}

impl AppError {
    fn code(&self) -> &'static str {
        match self {
            Self::Unauthorized(_) => "unauthorized",
            Self::BadRequest(_) => "invalid_request",
            Self::NotFound(_) => "not_found",
            Self::Conflict(_) => "conflict",
            Self::Unavailable(_) => "unavailable",
            Self::PayloadTooLarge(_) => "payload_too_large",
            Self::RequestTimeout(_) => "request_timeout",
            Self::Database(_) => "internal",
        }
    }

    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Unavailable(_) | Self::RequestTimeout(_) | Self::Database(_)
        )
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RequestTimeout(_) => StatusCode::REQUEST_TIMEOUT,
            Self::Database(e) => {
                tracing::error!(error = %e, "database error");
                StatusCode::INTERNAL_SERVER_ERROR
            }
        };
        let body = ErrorBody {
            error: ErrorDetail {
                code: self.code().to_owned(),
                message: self.to_string(),
                details: None,
                retryable: self.retryable(),
            },
        };
        (status, Json(body)).into_response()
    }
}

/// Failures before the server starts serving.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("config: {0}")]
    Config(#[from] crate::config::ConfigError),
    #[error("database: {0}")]
    Database(#[from] sqlx::Error),
    #[error("migrations: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("worker: {0}")]
    Worker(#[from] AppError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn renders_the_single_envelope() {
        let response = AppError::NotFound("job nope".to_owned()).into_response();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let bytes = to_bytes(response.into_body(), 4096).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["error"]["code"], "not_found");
        assert_eq!(value["error"]["message"], "job nope");
        assert_eq!(value["error"]["retryable"], false);
    }

    #[test]
    fn database_errors_are_retryable() {
        assert!(AppError::Database(sqlx::Error::PoolClosed).retryable());
    }
}
