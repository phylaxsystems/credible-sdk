//! Command-line configuration for the state checker.

use clap::Parser;

/// Runtime configuration flags for the state checker.
///
/// We expose each parameter as both a long-form CLI flag and an env var so the
/// worker can be configured through deployment manifests without shell args.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// RPC endpoint URL for the Ethereum node.
    #[arg(long, env = "STATE_CHECKER_RPC_URL")]
    pub rpc_url: String,

    /// Redis connection string.
    #[arg(long, env = "STATE_CHECKER_REDIS_URL")]
    pub redis_url: String,

    /// Namespace prefix for Redis keys.
    #[arg(long, env = "STATE_CHECKER_REDIS_NAMESPACE", default_value = "state")]
    pub redis_namespace: String,

    /// Optional state depth (how many blocks behind head Redis will have the data from)
    #[arg(long, env = "STATE_CHECKER_STATE_DEPTH", default_value = "3")]
    pub state_depth: usize,
}
