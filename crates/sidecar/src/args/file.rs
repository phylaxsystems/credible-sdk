//! File-based configuration structures
use assertion_executor::{
    primitives::{
        Address,
        SpecId,
    },
    store::BlockTag,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::{
    fs,
    path::Path,
    str::FromStr,
};

/// Configuration loaded from JSON file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct FileConfig {
    pub credible: CredibleConfig,
    pub transport: TransportConfig,
    pub state: StateConfig,
}

impl FileConfig {
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

impl FromStr for FileConfig {
    type Err = ConfigError;

    /// Load configuration from a JSON string
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
            .map_err(|e| ConfigError::ParseError(format!("Failed to parse JSON: {e}")))
    }
}

/// Credible configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CredibleConfig {
    /// Gas limit for assertion execution
    pub assertion_gas_limit: u64,
    /// Overlay cache capacity in elements
    pub overlay_cache_capacity: Option<usize>,
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

/// State configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StateConfig {
    /// Sequencer bind address and port
    pub sequencer_url: Option<String>,
    /// Besu client bind address and port
    pub besu_client_ws_url: Option<String>,
    /// Redis bind address and port
    pub redis_url: Option<String>,
    /// Minimum state diff to consider a cache synced
    pub minimum_state_diff: u64,
    /// Maximum time (ms) the engine will wait for a state source to report as  synced before
    /// failing a transaction.
    pub sources_sync_timeout_ms: u64,
    /// Period (ms) the engine will check if the state sources are synced.
    pub sources_monitoring_period_ms: u64,
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
  "credible": {
    "assertion_gas_limit": 30000000,
    "overlay_cache_capacity": 1000,
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
    "redis_url": "redis://localhost:6379",
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
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

        let config = FileConfig::from_file(temp_file.path()).unwrap();

        // Verify credible config
        assert_eq!(config.credible.assertion_gas_limit, 30000000);
        assert_eq!(config.credible.overlay_cache_capacity, Some(1000));
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
            config.state.redis_url,
            Some("redis://localhost:6379".to_string())
        );
        assert_eq!(config.state.minimum_state_diff, 10);
        assert_eq!(config.state.sources_sync_timeout_ms, 30000);
        assert_eq!(config.state.sources_monitoring_period_ms, 1000);
    }

    #[test]
    fn test_from_file_not_found() {
        let result = FileConfig::from_file("/nonexistent/path/config.json");

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

        let result = FileConfig::from_file(temp_file.path());

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

        let result = FileConfig::from_file(temp_file.path());

        assert!(result.is_err());
        match result {
            Err(ConfigError::ParseError(_)) => {}
            _ => panic!("Expected ParseError for missing fields"),
        }
    }

    #[test]
    fn test_from_file_with_optional_none_values() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
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

        let config = FileConfig::from_file(temp_file.path()).unwrap();

        assert_eq!(config.state.sequencer_url, None);
        assert_eq!(config.state.besu_client_ws_url, None);
        assert_eq!(config.state.redis_url, None);
    }

    #[test]
    fn test_config_error_display() {
        let read_error = ConfigError::ReadError("test read error".to_string());
        assert_eq!(read_error.to_string(), "test read error");

        let parse_error = ConfigError::ParseError("test parse error".to_string());
        assert_eq!(parse_error.to_string(), "test parse error");
    }

    #[test]
    fn test_invalid_type_values() {
        let mut temp_file = NamedTempFile::new().unwrap();
        write!(
            temp_file,
            r#"{{
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
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let result = FileConfig::from_file(temp_file.path());
        assert!(result.is_err());
        assert!(matches!(result, Err(ConfigError::ParseError(_))));
    }
}
