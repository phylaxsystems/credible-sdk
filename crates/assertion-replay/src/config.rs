use clap::Parser;
use std::net::SocketAddr;

/// Runtime configuration for the replaying API service.
///
/// CLI flags and env vars are both supported to match sidecar/shadow-driver
/// deployment ergonomics.
#[derive(Debug, Clone, Parser)]
#[command(author, version, about, long_about = None)]
pub struct Config {
    #[arg(long, env = "REPLAY_BIND_ADDR", default_value = "0.0.0.0:8080")]
    pub bind_addr: SocketAddr,

    #[arg(long, env = "REPLAY_ARCHIVE_WS_URL")]
    pub archive_ws_url: String,

    #[arg(long, env = "REPLAY_ARCHIVE_HTTP_URL")]
    pub archive_http_url: String,

    #[arg(long, env = "REPLAY_WINDOW")]
    pub replay_window: u64,

    #[arg(long, env = "REPLAY_CHAIN_ID", default_value_t = 1)]
    pub chain_id: u64,

    #[arg(
        long,
        env = "REPLAY_ASSERTION_GAS_LIMIT",
        default_value_t = 1_000_000_000
    )]
    pub assertion_gas_limit: u64,

    #[arg(long, env = "REPLAY_DURATION_MIN_MINUTES", default_value_t = 10.0)]
    pub replay_duration_min_minutes: f64,

    #[arg(long, env = "REPLAY_DURATION_TARGET_MINUTES", default_value_t = 12.5)]
    pub replay_duration_target_minutes: f64,

    #[arg(long, env = "REPLAY_DURATION_MAX_MINUTES", default_value_t = 15.0)]
    pub replay_duration_max_minutes: f64,
}

impl Config {
    /// Loads runtime configuration from CLI args and environment variables.
    pub fn load() -> Self {
        Self::parse()
    }
}
