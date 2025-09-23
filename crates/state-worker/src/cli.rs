//! Command-line configuration for the state worker.

use clap::Parser;

pub const DEFAULT_TRACE_TIMEOUT_SECS: u64 = 30;

/// Runtime configuration flags for the state worker.
///
/// We expose each parameter as both a long-form CLI flag and an env var so the
/// worker can be configured through deployment manifests without shell args.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// WebSocket endpoint for the Ethereum node.
    #[arg(long, env = "STATE_WORKER_WS_URL")]
    pub ws_url: String,

    /// Redis connection string.
    #[arg(long, env = "STATE_WORKER_REDIS_URL")]
    pub redis_url: String,

    /// Namespace prefix for Redis keys.
    #[arg(long, env = "STATE_WORKER_REDIS_NAMESPACE", default_value = "state")]
    pub redis_namespace: String,

    /// Optional block number to start syncing from.
    #[arg(long, env = "STATE_WORKER_START_BLOCK")]
    pub start_block: Option<u64>,

    /// Timeout in seconds for debug trace requests.
    #[arg(long, env = "STATE_WORKER_TRACE_TIMEOUT_SECS", default_value_t = DEFAULT_TRACE_TIMEOUT_SECS)]
    pub trace_timeout_secs: u64,
}
