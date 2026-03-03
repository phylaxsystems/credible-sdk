pub mod handlers;
pub mod models;
pub mod state;

pub use state::AppState;

use axum::Router;
use handlers::{
    health::health_handler,
    replay::replay_handler,
    start_block::replay_start_block_handler,
};

/// Builds the HTTP router for the replaying API.
pub fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/health", axum::routing::get(health_handler))
        .route(
            "/replay/start-block",
            axum::routing::get(replay_start_block_handler),
        )
        .route("/replay", axum::routing::post(replay_handler))
        .with_state(state)
}
