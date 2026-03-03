use crate::{
    config::Config,
    services::replay::ReplayDurationTuning,
};
use alloy_provider::RootProvider;
use std::sync::{
    Arc,
    atomic::AtomicU64,
};

/// Shared application state for request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub head_provider: Arc<RootProvider>,
    pub replay_window: Arc<AtomicU64>,
    pub replay_duration_tuning: ReplayDurationTuning,
}
