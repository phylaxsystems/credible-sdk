use axum::{
    Json,
    http::StatusCode,
    response::{
        IntoResponse,
        Response,
    },
};
use serde::Serialize;
use utoipa::ToSchema;

/// Generic error response body for non-2xx handler responses.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
}

/// HTTP error type returned by handlers.
#[derive(Debug, Clone)]
pub struct HttpError {
    status: StatusCode,
    error: String,
}

impl HttpError {
    pub fn new(status: StatusCode, error: impl Into<String>) -> Self {
        Self {
            status,
            error: error.into(),
        }
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> Response {
        (self.status, Json(ErrorResponse { error: self.error })).into_response()
    }
}

/// Reusable HTTP error descriptor for handler-level constants.
#[derive(Debug, Clone, Copy)]
pub struct HttpErrorDef {
    status: StatusCode,
    message: &'static str,
}

impl HttpErrorDef {
    #[must_use]
    pub const fn new(status: StatusCode, message: &'static str) -> Self {
        Self { status, message }
    }

    pub fn with_message(self, message: impl Into<String>) -> HttpError {
        HttpError::new(self.status, message)
    }

    #[must_use]
    pub fn into_error(self) -> HttpError {
        HttpError::new(self.status, self.message)
    }
}

/// Standard handler result for this API.
pub type AppResult<T> = Result<T, HttpError>;

#[macro_export]
macro_rules! http_error {
    ($name:ident, $status:ident, $message:expr $(,)?) => {
        pub const $name: $crate::server::models::error::HttpErrorDef =
            $crate::server::models::error::HttpErrorDef::new(
                axum::http::StatusCode::$status,
                $message,
            );
    };
    ($status:ident, $message:expr $(,)?) => {
        $crate::server::models::error::HttpError::new(axum::http::StatusCode::$status, $message)
    };
    ($status:expr, $message:expr $(,)?) => {
        $crate::server::models::error::HttpError::new($status, $message)
    };
}

#[cfg(test)]
mod tests {
    use super::HttpError;
    use axum::{
        body::to_bytes,
        http::StatusCode,
        response::IntoResponse,
    };

    #[tokio::test]
    async fn http_error_into_response_contains_status_and_body() {
        let response = HttpError::new(StatusCode::BAD_REQUEST, "Invalid payload").into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("error body should be readable");
        let body = std::str::from_utf8(&body).expect("response body should be valid utf-8");
        assert_eq!(body, r#"{"error":"Invalid payload"}"#);
    }
}
