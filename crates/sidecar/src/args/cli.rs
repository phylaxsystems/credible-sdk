//! Sidecar command arguments
use assertion_executor::primitives::SpecId;
use std::{
    path::PathBuf,
    str::FromStr,
};

fn parse_spec_id(s: &str) -> Result<SpecId, String> {
    SpecId::from_str(s).map_err(|_| format!("Invalid spec id: {s}"))
}

/// Parameters for the chain we receive tx from
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
#[command(next_help_heading = "Rollup")]
pub struct ChainArgs {
    /// What EVM specification to use. Only latest for now
    #[arg(
        long = "chain.spec-id",
        env = "CHAIN_SPEC_ID",
        required = true,
        value_parser = parse_spec_id,
        value_enum
    )]
    pub spec_id: SpecId,

    // Chain ID
    #[arg(long = "chain.chain-id", env = "CHAIN_CHAIN_ID", required = true)]
    pub chain_id: u64,
}

/// Main sidecar arguments
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    /// Path to the configuration file
    #[arg(long = "config-file-path", env = "CONFIG_FILE_PATH")]
    pub config_file_path: Option<PathBuf>,

    #[command(flatten)]
    pub chain: ChainArgs,
}
