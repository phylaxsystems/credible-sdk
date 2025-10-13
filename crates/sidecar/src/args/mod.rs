//! Unified configuration combining CLI args and file config
pub mod cli;
pub mod file;

use crate::args::{
    cli::{
        ChainArgs,
        SidecarArgs,
    },
    file::{
        ConfigError,
        CredibleConfig,
        FileConfig,
        StateConfig,
        TransportConfig,
    },
};
use assertion_executor::primitives::SpecId;
use clap::Parser;
use std::str::FromStr;

const DEFAULT_CONFIG: &str = include_str!("../../default_config.json");

/// Complete configuration for the sidecar
#[derive(Debug, Clone)]
pub struct Config {
    pub credible: CredibleConfig,
    pub transport: TransportConfig,
    pub state: StateConfig,
    pub chain: ChainArgs,
}

impl Config {
    /// Load configuration by merging CLI args and file config
    ///
    /// Precedence: CLI args > config file
    pub fn load() -> Result<Self, ConfigError> {
        let args = SidecarArgs::parse();

        // Load file-based config
        let file_config = match &args.config_file_path {
            Some(path) => {
                // Explicit path provided - load from file
                FileConfig::from_file(path)?
            }
            None => {
                // No path provided - use embedded default
                FileConfig::from_str(DEFAULT_CONFIG)?
            }
        };

        // Merge with CLI args (CLI takes precedence)
        Ok(Self {
            credible: file_config.credible,
            transport: file_config.transport,
            state: file_config.state,
            chain: args.chain,
        })
    }
}
