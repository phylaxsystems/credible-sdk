use crate::{
    server::{
        models::{
            error::{
                AppResult,
                HttpError,
            },
            replay::ReplayRequest,
        },
        state::AppState,
    },
    services::replay::{
        ReplayError,
        run_replay,
    },
};
use axum::{
    extract::{
        Json,
        State,
        rejection::JsonRejection,
    },
    http::StatusCode,
};
use thiserror::Error;

/// Handles `POST /replay` requests.
///
/// Returns:
/// - `200` when scheduling/execution completes
/// - `4xx` when JSON extraction/validation fails
/// - `500` for internal replay/runtime failures
pub async fn replay_handler(
    State(state): State<AppState>,
    payload: Result<Json<ReplayRequest>, JsonRejection>,
) -> AppResult<StatusCode> {
    let Json(request) = payload.map_err(ReplayHandlerError::from)?;
    run_replay(state.config.as_ref(), &request)
        .await
        .map_err(ReplayHandlerError::from)?;

    Ok(StatusCode::OK)
}

#[derive(Debug, Error)]
enum ReplayHandlerError {
    #[error("invalid payload: {0}")]
    InvalidPayload(String),
    #[error(transparent)]
    Replay(#[from] ReplayError),
}

impl From<JsonRejection> for ReplayHandlerError {
    fn from(source: JsonRejection) -> Self {
        Self::InvalidPayload(source.body_text())
    }
}

impl From<ReplayHandlerError> for HttpError {
    fn from(source: ReplayHandlerError) -> Self {
        match source {
            ReplayHandlerError::InvalidPayload(message) => {
                super::INVALID_PAYLOAD_ERROR.with_message(message)
            }
            ReplayHandlerError::Replay(_error) => super::REPLAY_FAILED_ERROR.into_error(),
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
