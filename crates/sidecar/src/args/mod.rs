//! Unified configuration combining CLI args and file config
pub mod cli;

use crate::args::cli::SidecarArgs;
use assertion_executor::{
    primitives::{
        Address,
        SpecId,
    },
    store::BlockTag,
};
use clap::Parser;
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    fs,
    path::Path,
    str::FromStr,
};

const DEFAULT_CONFIG: &str = include_str!("../../default_config.json");

/// Configuration loaded from JSON file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Config {
    pub chain: ChainConfig,
    pub credible: CredibleConfig,
    pub transport: TransportConfig,
    pub state: StateConfig,
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
                Self::from_file(path)?
            }
            None => {
                // No path provided - use embedded default
                Self::from_str(DEFAULT_CONFIG)?
            }
        };

        // Merge with CLI args (CLI takes precedence)
        Ok(Self {
            credible: file_config.credible,
            transport: file_config.transport,
            state: file_config.state,
            chain: file_config.chain,
        })
    }

    /// Load configuration from a JSON file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let contents = fs::read_to_string(path).map_err(|e| {
            ConfigError::ReadError(format!("Failed to read {}: {e}", path.display()))
        })?;

        serde_json::from_str(&contents).map_err(|e| {
            ConfigError::ParseError(format!("Failed to parse {}: {e}", path.display()))
        })
    }
}

impl FromStr for Config {
    type Err = ConfigError;

    /// Load configuration from a JSON string
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
            .map_err(|e| ConfigError::ParseError(format!("Failed to parse JSON: {e}")))
    }
}

/// Parameters for the chain we receive tx from
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ChainConfig {
    /// What EVM specification to use
    pub spec_id: SpecId,
    /// Chain ID
    pub chain_id: u64,
}

/// Credible configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CredibleConfig {
    /// Gas limit for assertion execution
    pub assertion_gas_limit: u64,
    /// Whether the overlay cache has to be invalidated every block
    pub overlay_cache_invalidation_every_block: Option<bool>,
    /// Sled cache capacity, used in the `FsDb`, 256mb default
    pub cache_capacity_bytes: Option<usize>,
    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    pub flush_every_ms: Option<usize>,
    /// HTTP URL of the assertion DA
    pub assertion_da_rpc_url: String,
    /// WS URL the RPC store will use to index assertions
    pub indexer_rpc_url: String,
    /// Path to the indexer database (separate from main assertion store)
    pub indexer_db_path: String,
    /// Path to the rpc store db
    pub assertion_store_db_path: String,
    /// Block tag to use for indexing assertions.
    pub block_tag: BlockTag,
    /// Contract address of the state oracle contract, used to query assertion info
    pub state_oracle: Address,
    /// Block number of the state oracle deployment
    pub state_oracle_deployment_block: u64,
    /// Maximum capacity for transaction results
    pub transaction_results_max_capacity: usize,
    /// Cache checker client websocket url
    #[cfg(feature = "cache_validation")]
    pub cache_checker_ws_url: String,
}

/// Select which transport protocol to run
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportProtocol {
    Http,
    Grpc,
}

/// Transport configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct TransportConfig {
    /// Select which transport protocol to run
    pub protocol: TransportProtocol,
    /// Server bind address and port
    pub bind_addr: String,
}

fn default_use_debug_code_by_hash() -> bool {
    false
}

/// State configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StateConfig {
    /// Sequencer bind address and port
    pub sequencer_url: Option<String>,
    /// Besu client websocket bind address and port
    pub besu_client_ws_url: Option<String>,
    /// Besu client HTTP bind address and port
    pub besu_client_http_url: Option<String>,
    /// Redis bind address and port
    pub redis_url: Option<String>,
    /// Namespace prefix for Redis keys.
    pub redis_namespace: Option<String>,
    /// Redis state depth (how many blocks behind head Redis will have the data from)
    pub redis_depth: Option<usize>,
    /// Minimum state diff to consider a cache synced
    pub minimum_state_diff: u64,
    /// Maximum time (ms) the engine will wait for a state source to report as  synced before
    /// failing a transaction.
    pub sources_sync_timeout_ms: u64,
    /// Period (ms) the engine will check if the state sources are synced.
    pub sources_monitoring_period_ms: u64,
    /// Whether to use the `debug_codeByHash` RPC method when fetching bytecode.
    #[serde(default = "default_use_debug_code_by_hash")]
    pub use_debug_code_by_hash: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("{0}")]
    ReadError(String),
    #[error("{0}")]
    ParseError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // Helper function to create a valid test config JSON
    fn valid_config_json() -> String {
        r#"{
  "chain": {
    "spec_id": "CANCUN",
    "chain_id": 1
  },
  "credible": {
    "assertion_gas_limit": 30000000,
    "overlay_cache_capacity": 1000,
    "overlay_cache_invalidation_every_block": true,
    "cache_capacity_bytes": 268435456,
    "flush_every_ms": 5000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "indexer_rpc_url": "ws://localhost:8546",
    "indexer_db_path": "/tmp/indexer.db",
    "assertion_store_db_path": "/tmp/store.db",
    "block_tag": "latest",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  },
  "transport": {
    "protocol": "http",
    "bind_addr": "127.0.0.1:3000"
  },
  "state": {
    "sequencer_url": "http://localhost:8547",
    "besu_client_ws_url": "ws://localhost:8548",
    "besu_client_http_url": "http://localhost:8545",
    "redis_url": "redis://localhost:6379",
    "redis_namespace": "sidecar",
    "redis_depth": 100,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000,
    "use_debug_code_by_hash": false
  }
}"#
        .to_string()
    }

    #[test]
    fn test_from_file_success() {
        // Create a temporary file with valid config
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "{}", valid_config_json()).unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        // Verify chain config
        assert_eq!(config.chain.spec_id, SpecId::CANCUN);
        assert_eq!(config.chain.chain_id, 1);

        // Verify credible config
        assert_eq!(config.credible.assertion_gas_limit, 30000000);
        assert_eq!(config.credible.cache_capacity_bytes, Some(268435456));
        assert_eq!(config.credible.flush_every_ms, Some(5000));
        assert_eq!(
            config.credible.assertion_da_rpc_url,
            "http://localhost:8545"
        );
        assert_eq!(config.credible.indexer_rpc_url, "ws://localhost:8546");
        assert_eq!(config.credible.indexer_db_path, "/tmp/indexer.db");
        assert_eq!(config.credible.assertion_store_db_path, "/tmp/store.db");
        assert_eq!(config.credible.state_oracle_deployment_block, 100);
        assert_eq!(config.credible.transaction_results_max_capacity, 10000);

        // Verify transport config
        assert_eq!(config.transport.protocol, TransportProtocol::Http);
        assert_eq!(config.transport.bind_addr, "127.0.0.1:3000");

        // Verify state config
        assert_eq!(
            config.state.sequencer_url,
            Some("http://localhost:8547".to_string())
        );
        assert_eq!(
            config.state.besu_client_ws_url,
            Some("ws://localhost:8548".to_string())
        );
        assert_eq!(
            config.state.besu_client_http_url,
            Some("http://localhost:8545".to_string())
        );
        assert_eq!(
            config.state.redis_url,
            Some("redis://localhost:6379".to_string())
        );
        assert_eq!(config.state.redis_namespace, Some("sidecar".to_string()));
        assert_eq!(config.state.redis_depth, Some(100));
        assert_eq!(config.state.minimum_state_diff, 10);
        assert_eq!(config.state.sources_sync_timeout_ms, 30000);
        assert_eq!(config.state.sources_monitoring_period_ms, 1000);
        assert!(!config.state.use_debug_code_by_hash);
    }

    #[test]
    fn test_from_file_not_found() {
        let result = Config::from_file("/nonexistent/path/config.json");

        assert!(result.is_err());
        match result {
            Err(ConfigError::ReadError(msg)) => {
                assert!(msg.contains("Failed to read"));
            }
            _ => panic!("Expected ReadError"),
        }
    }

    #[test]
    fn test_from_file_invalid_json() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, "this is not valid JSON {{{{").unwrap();
        temp_file.flush().unwrap();

        let result = Config::from_file(temp_file.path());

        assert!(result.is_err());
        match result {
            Err(ConfigError::ParseError(msg)) => {
                assert!(msg.contains("Failed to parse"));
            }
            _ => panic!("Expected ParseError"),
        }
    }

    #[test]
    fn test_from_file_missing_required_fields() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
  "credible": {{
    "assertion_gas_limit": 30000000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let result = Config::from_file(temp_file.path());

        assert!(result.is_err());
        match result {
            Err(ConfigError::ParseError(_)) => {}
            _ => panic!("Expected ParseError for missing fields"),
        }
    }

    #[test]
    fn test_chain_config_different_spec_ids() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
  "chain": {{
    "spec_id": "SHANGHAI",
    "chain_id": 11155111
  }},
  "credible": {{
    "assertion_gas_limit": 30000000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "indexer_rpc_url": "ws://localhost:8546",
    "indexer_db_path": "/tmp/indexer.db",
    "assertion_store_db_path": "/tmp/store.db",
    "block_tag": "latest",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "protocol": "grpc",
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "redis_namespace": "test",
    "redis_depth": 50,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        assert_eq!(config.chain.spec_id, SpecId::SHANGHAI);
        assert_eq!(config.chain.chain_id, 11155111);
        assert_eq!(config.transport.protocol, TransportProtocol::Grpc);
    }

    #[test]
    fn test_invalid_type_values() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
  "chain": {{
    "spec_id": "CANCUN",
    "chain_id": 1
  }},
  "credible": {{
    "assertion_gas_limit": "not a number",
    "overlay_cache_capacity": 1000,
    "cache_capacity_bytes": 268435456,
    "flush_every_ms": 5000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "indexer_rpc_url": "ws://localhost:8546",
    "indexer_db_path": "/tmp/indexer.db",
    "assertion_store_db_path": "/tmp/store.db",
    "block_tag": "Latest",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "protocol": "http",
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "redis_namespace": "sidecar",
    "redis_depth": 100,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let result = Config::from_file(temp_file.path());
        assert!(result.is_err());
        assert!(matches!(result, Err(ConfigError::ParseError(_))));
    }

    #[test]
    fn test_from_file_with_optional_none_values() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
  "chain": {{
    "spec_id": "CANCUN",
    "chain_id": 1
  }},
  "credible": {{
    "assertion_gas_limit": 30000000,
    "assertion_da_rpc_url": "http://localhost:8545",
    "indexer_rpc_url": "ws://localhost:8546",
    "indexer_db_path": "/tmp/indexer.db",
    "assertion_store_db_path": "/tmp/store.db",
    "block_tag": "latest",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "protocol": "http",
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        assert_eq!(config.state.sequencer_url, None);
        assert_eq!(config.state.besu_client_ws_url, None);
        assert_eq!(config.state.redis_url, None);
    }
}
