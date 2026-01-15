//! Command-line configuration for the state worker.

use std::str::FromStr;

#[derive(Clone, Debug)]
pub struct Config {
    pub ws_url: String,
    pub provider_type: ProviderType,
    pub mdbx_path: String,
    pub start_block: Option<u64>,
    pub state_depth: u8,
    /// File to read genesis state from. Required to seed initial state.
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
