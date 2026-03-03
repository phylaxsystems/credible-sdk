use crate::config::Config;
use std::sync::Arc;

/// Shared application state for request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
}
