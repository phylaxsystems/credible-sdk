use axum::http::StatusCode;

/// Handles `GET /health` requests.
pub async fn health_handler() -> StatusCode {
    StatusCode::OK
}
