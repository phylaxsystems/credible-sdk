//! Command-line configuration for the state worker.

use clap::Parser;
use std::str::FromStr;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(long, env = "STATE_WORKER_WS_URL")]
    pub ws_url: String,

    #[arg(long, env = "STATE_WORKER_PROVIDER_TYPE")]
    pub provider_type: ProviderType,

    #[arg(long, env = "STATE_WORKER_REDIS_URL")]
    pub redis_url: String,

    #[arg(long, env = "STATE_WORKER_REDIS_NAMESPACE", default_value = "state")]
    pub redis_namespace: String,

    #[arg(long, env = "STATE_WORKER_START_BLOCK")]
    pub start_block: Option<u64>,

    #[arg(long, env = "STATE_WORKER_STATE_DEPTH", default_value = "3")]
    pub state_depth: usize,

    /// File to read genesis state from. Required to seed initial state.
    #[arg(long, env = "STATE_WORKER_FILE_TO_GENESIS")]
    pub file_to_genesis: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ProviderType {
    Geth,
    Parity,
}

impl FromStr for ProviderType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "geth" => Ok(Self::Geth),
            "parity" => Ok(Self::Parity),
            _ => Err(format!("Invalid provider type: {s}")),
        }
    }
}
