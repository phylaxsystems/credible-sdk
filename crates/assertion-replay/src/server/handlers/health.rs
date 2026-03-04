use axum::http::StatusCode;

/// Handles `GET /health` requests.
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy")
    ),
    tag = "assertion-replay"
)]
pub async fn health_handler() -> StatusCode {
    StatusCode::OK
}
