pub mod handlers;
pub mod models;
pub mod state;

pub use state::AppState;

use axum::Router;
use handlers::replay::replay_handler;

/// Builds the HTTP router for the replaying API.
pub fn app_router(state: AppState) -> Router {
    Router::new()
        .route("/replay", axum::routing::post(replay_handler))
        .with_state(state)
}
