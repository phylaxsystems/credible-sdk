//! Command-line configuration for the state worker.

use clap::Parser;
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

    /// Optional chain identifier used to select an embedded genesis snapshot.
    #[arg(long, env = "STATE_WORKER_CHAIN_ID")]
    pub chain_id: Option<u64>,

    /// Optional state depth (how many blocks behind head Redis will have the data from)
    #[arg(long, env = "STATE_WORKER_STATE_DEPTH", default_value = "3")]
    pub state_depth: usize,

    /// Optional file to read genesis state from. If specified, overrides the embedded genesis snapshot.
    #[arg(long, env = "STATE_WORKER_FILE_TO_GENESIS")]
    pub file_to_genesis: Option<String>,
}
