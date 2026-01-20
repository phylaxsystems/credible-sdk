//! Sidecar command arguments
use assertion_executor::primitives::SpecId;
use std::{
    path::PathBuf,
    str::FromStr,
};

/// Main sidecar arguments
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    /// Path to the configuration file
    #[arg(long = "config-file-path", env = "CONFIG_FILE_PATH")]
    pub config_file_path: Option<PathBuf>,
    /// Path to write dhat memory profile output (only available with dhat-heap feature)
    #[cfg(feature = "dhat-heap")]
    #[arg(long = "dhat-output-path", env = "DHAT_OUTPUT_PATH")]
    pub dhat_output_path: Option<PathBuf>,
}
