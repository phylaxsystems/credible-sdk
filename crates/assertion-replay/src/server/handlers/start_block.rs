use crate::{
    server::{
        models::{
            error::{
                AppResult,
                HttpError,
            },
            replay::ReplayStartBlockResponse,
        },
        state::AppState,
    },
    services::replay::{
        ReplayError,
        preview_replay_start_block,
    },
};
use axum::{
    Json,
    extract::State,
};
use thiserror::Error;

/// Handles `GET /replay/start-block` requests.
///
/// # Errors
///
/// Returns [`HttpError`] when replay start-block preview computation fails.
pub async fn replay_start_block_handler(
    State(state): State<AppState>,
) -> AppResult<Json<ReplayStartBlockResponse>> {
    let preview =
        preview_replay_start_block(state.head_provider.as_ref(), state.replay_window.as_ref())
            .await
            .map_err(StartBlockHandlerError::from)?;

    Ok(Json(ReplayStartBlockResponse {
        start_block: preview.start_block,
        head_block: preview.head_block,
        replay_window: preview.replay_window,
    }))
}

#[derive(Debug, Error)]
enum StartBlockHandlerError {
    #[error(transparent)]
    Replay(#[from] ReplayError),
}

impl From<StartBlockHandlerError> for HttpError {
    fn from(source: StartBlockHandlerError) -> Self {
        match source {
            StartBlockHandlerError::Replay(error) => {
                super::START_BLOCK_QUERY_FAILED_ERROR.with_message(error.to_string())
            }
        }
    }
}
