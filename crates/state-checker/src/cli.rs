//! Command-line configuration for the state checker.

use clap::Parser;

/// Runtime configuration flags for the state checker.
///
/// We expose each parameter as both a long-form CLI flag and an env var so the
/// worker can be configured through deployment manifests without shell args.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// MDBX path.
    #[arg(long, env = "STATE_CHECKER_MDBX_PATH")]
    pub mdbx_path: String,

    /// Optional state depth (how many blocks behind head in the state worker will have the data from)
    #[arg(long, env = "STATE_CHECKER_STATE_DEPTH", default_value = "3")]
    pub state_depth: u8,
}
