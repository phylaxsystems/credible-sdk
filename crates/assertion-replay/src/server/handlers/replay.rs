use crate::{
    server::{
        models::{
            error::ErrorResponse,
            replay::{
                ReplayRequest,
                ReplayResponse,
            },
        },
        state::AppState,
    },
    services::replay::run_replay,
};
use axum::{
    Json,
    extract::State,
    http::StatusCode,
};

/// Handles `POST /replay` requests.
///
/// Returns:
/// - `200` with `{"status":"OK"}` when scheduling/execution completes
/// - `4xx` when JSON extraction/validation fails
/// - `500` for internal replay/runtime failures
pub async fn replay_handler(
    State(state): State<AppState>,
    Json(request): Json<ReplayRequest>,
) -> Result<(StatusCode, Json<ReplayResponse>), (StatusCode, Json<ErrorResponse>)> {
    match run_replay(state.config.as_ref(), &request).await {
        Ok(()) => Ok((StatusCode::OK, Json(ReplayResponse::ok()))),
        Err(source) => {
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: source.to_string(),
                }),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ReplayRequest;

    #[test]
    fn replay_request_default_has_no_assertion_ids() {
        let request = ReplayRequest::default();
        assert!(request.assertion_ids.is_empty());
    }

    #[test]
    fn replay_request_deserializes_valid_payload() {
        let payload = br#"{
                "assertion_ids": [
                    "0x1111111111111111111111111111111111111111111111111111111111111111"
                ]
            }"#;
        let request: ReplayRequest =
            serde_json::from_slice(payload).expect("valid payload should parse");
        assert_eq!(request.assertion_ids.len(), 1);
    }
}
