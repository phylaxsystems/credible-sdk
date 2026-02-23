//! Unified configuration combining CLI args and file config
pub mod cli;

use crate::{
    args::cli::SidecarArgs,
    transport::grpc::config::GrpcTransportConfig,
};
use assertion_executor::primitives::{
    Address,
    SpecId,
};
use clap::Parser;
use serde::{
    Deserialize,
    de::DeserializeOwned,
};
use serde_with::{
    DurationMilliSeconds,
    serde_as,
};
use std::{
    env,
    fmt,
    fs,
    path::{
        Path,
        PathBuf,
    },
    str::FromStr,
    time::Duration,
};

const DEFAULT_CONFIG: &str = include_str!("../../default_config.json");

fn default_accepted_txs_ttl_ms() -> Duration {
    Duration::from_secs(2)
}

fn default_poll_interval() -> Duration {
    Duration::from_secs(1)
}

#[derive(Clone, PartialEq, Eq, Default, Deserialize)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl From<String> for SecretString {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SecretString {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl FromStr for SecretString {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

/// Configuration loaded from JSON file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct ConfigFile {
    pub chain: Option<ChainConfigFile>,
    pub credible: Option<CredibleConfigFile>,
    pub transport: Option<TransportConfigFile>,
    pub state: Option<StateConfigFile>,
    pub dhat_output_path: Option<PathBuf>,
}

impl ConfigFile {
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

    /// Resolve the config file with environment fallbacks
    pub fn resolve(self) -> Result<Config, ConfigError> {
        resolve_config(self)
    }
}

impl FromStr for ConfigFile {
    type Err = ConfigError;

    /// Load configuration from a JSON string
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s)
            .map_err(|e| ConfigError::ParseError(format!("Failed to parse JSON: {e}")))
    }
}

/// Resolved configuration used by the sidecar runtime
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Config {
    pub chain: ChainConfig,
    pub credible: CredibleConfig,
    pub transport: GrpcTransportConfig,
    pub state: StateConfig,
    pub dhat_output_path: Option<PathBuf>,
}

impl Config {
    /// Load configuration by merging CLI args, file config, and environment variables
    ///
    /// Precedence: CLI args > config file > environment
    pub fn load() -> Result<Self, ConfigError> {
        let args = SidecarArgs::parse();

        let file_config = match &args.config_file_path {
            Some(path) => ConfigFile::from_file(path)?,
            None => ConfigFile::from_str(DEFAULT_CONFIG)?,
        };

        file_config.resolve()
    }

    /// Load configuration from a JSON file and resolve environment fallbacks
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        ConfigFile::from_file(path)?.resolve()
    }
}

/// Parameters for the chain we receive tx from
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct ChainConfigFile {
    /// What EVM specification to use
    pub spec_id: Option<SpecId>,
    /// Chain ID
    pub chain_id: Option<u64>,
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
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct CredibleConfigFile {
    /// Gas limit for assertion execution
    pub assertion_gas_limit: Option<u64>,
    /// Whether the overlay cache has to be invalidated every block
    pub overlay_cache_invalidation_every_block: Option<bool>,
    /// Sled cache capacity, used in the `FsDb`, 256mb default
    pub cache_capacity_bytes: Option<usize>,
    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    pub flush_every_ms: Option<usize>,
    /// HTTP URL of the assertion DA
    pub assertion_da_rpc_url: Option<String>,
    /// URL of the event source for assertion events
    pub event_source_url: Option<String>,
    /// Poll interval for the event source syncer
    #[serde_as(as = "Option<DurationMilliSeconds<u64>>")]
    pub poll_interval: Option<Duration>,
    /// Path to the rpc store db
    pub assertion_store_db_path: Option<String>,
    /// Path to the transaction observer database
    pub transaction_observer_db_path: Option<String>,
    /// Dapp API endpoint for incident publishing
    pub transaction_observer_endpoint: Option<String>,
    /// Dapp API auth token for incident publishing
    pub transaction_observer_auth_token: Option<SecretString>,
    /// Max incident publish requests per poll interval
    pub transaction_observer_endpoint_rps_max: Option<usize>,
    /// Poll interval for incident publishing in milliseconds
    pub transaction_observer_poll_interval_ms: Option<u64>,
    /// Contract address of the state oracle contract, used to query assertion info
    pub state_oracle: Option<Address>,
    /// Block number of the state oracle deployment
    pub state_oracle_deployment_block: Option<u64>,
    /// Maximum capacity for transaction results
    pub transaction_results_max_capacity: Option<usize>,
    /// Maximum time (ms) to keep accepted transactions without results.
    #[serde_as(as = "Option<DurationMilliSeconds<u64>>")]
    pub accepted_txs_ttl_ms: Option<Duration>,
    /// Cache checker client websocket url
    #[cfg(feature = "cache_validation")]
    pub cache_checker_ws_url: Option<String>,
    /// Interval between prune runs in milliseconds for the assertion store
    pub assertion_store_prune_config_interval_ms: Option<u64>,
    /// Number of blocks to keep after inactivation (buffer for reorgs) for the assertion store
    pub assertion_store_prune_config_retention_blocks: Option<u64>,
}

#[serde_as]
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
    /// URL of the event source for assertion events
    pub event_source_url: String,
    /// Poll interval for the event source syncer
    pub poll_interval: Duration,
    /// Path to the rpc store db
    pub assertion_store_db_path: String,
    /// Path to the transaction observer database
    pub transaction_observer_db_path: Option<String>,
    /// Dapp API endpoint for incident publishing
    pub transaction_observer_endpoint: Option<String>,
    /// Dapp API auth token for incident publishing
    pub transaction_observer_auth_token: Option<SecretString>,
    /// Max incident publish requests per poll interval
    pub transaction_observer_endpoint_rps_max: Option<usize>,
    /// Poll interval for incident publishing in milliseconds
    pub transaction_observer_poll_interval_ms: Option<u64>,
    /// Contract address of the state oracle contract, used to query assertion info
    pub state_oracle: Address,
    /// Block number of the state oracle deployment
    pub state_oracle_deployment_block: u64,
    /// Maximum capacity for transaction results
    pub transaction_results_max_capacity: usize,
    /// Maximum time (ms) to keep accepted transactions without results.
    #[serde(default = "default_accepted_txs_ttl_ms")]
    #[serde_as(as = "DurationMilliSeconds<u64>")]
    pub accepted_txs_ttl_ms: Duration,
    /// Cache checker client websocket url
    #[cfg(feature = "cache_validation")]
    pub cache_checker_ws_url: String,
    /// Interval between prune runs in milliseconds for the assertion store
    pub assertion_store_prune_config_interval_ms: Option<u64>,
    /// Number of blocks to keep after inactivation (buffer for reorgs) for the assertion store
    pub assertion_store_prune_config_retention_blocks: Option<u64>,
}

/// Transport configuration from file
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct TransportConfigFile {
    /// Server bind address and port
    pub bind_addr: Option<String>,
    /// Health server bind address and port
    pub health_bind_addr: Option<String>,
    /// Maximum number of events ID in the transport layer buffer before dropping new events.
    pub event_id_buffer_capacity: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "type")]
pub enum StateSourceConfig {
    #[serde(rename = "mdbx")]
    Mdbx {
        /// State worker MDBX path
        mdbx_path: String,
        /// State worker depth (how many blocks behind head state worker will have the data from)
        depth: usize,
    },
    #[serde(rename = "eth-rpc")]
    EthRpc {
        /// Eth RPC source websocket bind address and port
        ws_url: String,
        /// Eth RPC source HTTP bind address and port
        http_url: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct LegacyStateConfig {
    /// Eth RPC source websocket bind address and port
    pub eth_rpc_source_ws_url: Option<String>,
    /// Eth RCP source HTTP bind address and port
    pub eth_rpc_source_http_url: Option<String>,
    /// State worker MDBX path
    pub state_worker_mdbx_path: Option<String>,
    /// State worker depth (how many blocks behind head state worker will have the data from)
    pub state_worker_depth: Option<usize>,
}

impl LegacyStateConfig {
    pub fn has_any(&self) -> bool {
        self.eth_rpc_source_ws_url.is_some()
            || self.eth_rpc_source_http_url.is_some()
            || self.state_worker_mdbx_path.is_some()
            || self.state_worker_depth.is_some()
    }
}

/// State configuration from file
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct StateConfigFile {
    /// State sources
    pub sources: Option<Vec<StateSourceConfig>>,
    /// Legacy state source fields (deprecated).
    #[serde(flatten)]
    pub legacy: LegacyStateConfig,
    /// Minimum state diff to consider a cache synced
    pub minimum_state_diff: Option<u64>,
    /// Maximum time (ms) the engine will wait for a state source to report as  synced before
    /// failing a transaction.
    pub sources_sync_timeout_ms: Option<u64>,
    /// Period (ms) the engine will check if the state sources are synced.
    pub sources_monitoring_period_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StateConfig {
    /// State sources
    #[serde(default)]
    pub sources: Vec<StateSourceConfig>,
    /// Legacy state source fields (deprecated).
    #[serde(flatten)]
    pub legacy: LegacyStateConfig,
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
    #[error("{0}")]
    MissingRequired(String),
}

fn resolve_config(file: ConfigFile) -> Result<Config, ConfigError> {
    let chain_file = file.chain.unwrap_or_default();
    let credible_file = file.credible.unwrap_or_default();
    let transport_file = file.transport.unwrap_or_default();
    let state_file = file.state.unwrap_or_default();

    let chain = resolve_chain(&chain_file)?;
    let credible = resolve_credible(&credible_file)?;
    let transport = resolve_transport(&transport_file)?;
    let state = resolve_state(&state_file)?;

    Ok(Config {
        chain,
        credible,
        transport,
        state,
        dhat_output_path: file.dhat_output_path,
    })
}

fn resolve_chain(chain_file: &ChainConfigFile) -> Result<ChainConfig, ConfigError> {
    Ok(ChainConfig {
        spec_id: required_or_env_with(
            chain_file.spec_id,
            "SIDECAR_CHAIN_SPEC_ID",
            "chain.spec_id",
            parse_spec_id,
        )?,
        chain_id: required_or_env(chain_file.chain_id, "SIDECAR_CHAIN_ID", "chain.chain_id")?,
    })
}

fn resolve_credible(credible_file: &CredibleConfigFile) -> Result<CredibleConfig, ConfigError> {
    let required = resolve_credible_required(credible_file)?;
    let optional = resolve_credible_optional(credible_file)?;
    let ttls = resolve_credible_ttls(credible_file)?;
    let prune = resolve_credible_prune(credible_file)?;

    Ok(CredibleConfig {
        assertion_gas_limit: required.assertion_gas_limit,
        overlay_cache_invalidation_every_block: optional.overlay_cache_invalidation_every_block,
        cache_capacity_bytes: optional.cache_capacity_bytes,
        flush_every_ms: optional.flush_every_ms,
        assertion_da_rpc_url: required.assertion_da_rpc_url,
        event_source_url: required.event_source_url,
        poll_interval: optional.poll_interval,
        assertion_store_db_path: required.assertion_store_db_path,
        transaction_observer_db_path: optional.transaction_observer_db_path,
        transaction_observer_endpoint: optional.transaction_observer_endpoint,
        transaction_observer_auth_token: optional.transaction_observer_auth_token,
        transaction_observer_endpoint_rps_max: optional.transaction_observer_endpoint_rps_max,
        transaction_observer_poll_interval_ms: optional.transaction_observer_poll_interval_ms,
        state_oracle: required.state_oracle,
        state_oracle_deployment_block: required.state_oracle_deployment_block,
        transaction_results_max_capacity: required.transaction_results_max_capacity,
        accepted_txs_ttl_ms: ttls.accepted_txs_ttl_ms,
        #[cfg(feature = "cache_validation")]
        cache_checker_ws_url: required.cache_checker_ws_url,
        assertion_store_prune_config_interval_ms: prune.assertion_store_prune_config_interval_ms,
        assertion_store_prune_config_retention_blocks: prune
            .assertion_store_prune_config_retention_blocks,
    })
}

fn resolve_transport(
    transport_file: &TransportConfigFile,
) -> Result<GrpcTransportConfig, ConfigError> {
    Ok(GrpcTransportConfig {
        bind_addr: required_or_env(
            transport_file.bind_addr.clone(),
            "SIDECAR_TRANSPORT_BIND_ADDR",
            "transport.bind_addr",
        )?,
        health_bind_addr: parse_env("SIDECAR_HEALTH_BIND_ADDR")?
            .or_else(|| transport_file.health_bind_addr.clone())
            .unwrap_or_else(|| GrpcTransportConfig::default().health_bind_addr),
        event_id_buffer_capacity: parse_env("SIDECAR_EVENT_ID_BUFFER_CAPACITY")?
            .or(transport_file.event_id_buffer_capacity)
            .unwrap_or_else(|| GrpcTransportConfig::default().event_id_buffer_capacity),
    })
}

fn resolve_state(state_file: &StateConfigFile) -> Result<StateConfig, ConfigError> {
    let sources = parse_env_json("SIDECAR_STATE_SOURCES")?
        .or_else(|| state_file.sources.clone())
        .unwrap_or_default();

    Ok(StateConfig {
        sources,
        legacy: LegacyStateConfig {
            eth_rpc_source_ws_url: parse_env("SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL")?
                .or_else(|| state_file.legacy.eth_rpc_source_ws_url.clone()),
            eth_rpc_source_http_url: parse_env("SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL")?
                .or_else(|| state_file.legacy.eth_rpc_source_http_url.clone()),
            state_worker_mdbx_path: parse_env("SIDECAR_STATE_WORKER_MDBX_PATH")?
                .or_else(|| state_file.legacy.state_worker_mdbx_path.clone()),
            state_worker_depth: state_file
                .legacy
                .state_worker_depth
                .or(parse_env("SIDECAR_STATE_WORKER_DEPTH")?),
        },
        minimum_state_diff: required_or_env(
            state_file.minimum_state_diff,
            "SIDECAR_STATE_MINIMUM_STATE_DIFF",
            "state.minimum_state_diff",
        )?,
        sources_sync_timeout_ms: required_or_env(
            state_file.sources_sync_timeout_ms,
            "SIDECAR_STATE_SOURCES_SYNC_TIMEOUT_MS",
            "state.sources_sync_timeout_ms",
        )?,
        sources_monitoring_period_ms: required_or_env(
            state_file.sources_monitoring_period_ms,
            "SIDECAR_STATE_SOURCES_MONITORING_PERIOD_MS",
            "state.sources_monitoring_period_ms",
        )?,
    })
}

struct CredibleRequired {
    assertion_gas_limit: u64,
    assertion_da_rpc_url: String,
    event_source_url: String,
    assertion_store_db_path: String,
    state_oracle: Address,
    state_oracle_deployment_block: u64,
    transaction_results_max_capacity: usize,
    #[cfg(feature = "cache_validation")]
    cache_checker_ws_url: String,
}

struct CredibleOptional {
    overlay_cache_invalidation_every_block: Option<bool>,
    cache_capacity_bytes: Option<usize>,
    flush_every_ms: Option<usize>,
    poll_interval: Duration,
    transaction_observer_db_path: Option<String>,
    transaction_observer_endpoint: Option<String>,
    transaction_observer_auth_token: Option<SecretString>,
    transaction_observer_endpoint_rps_max: Option<usize>,
    transaction_observer_poll_interval_ms: Option<u64>,
}

struct CredibleTtls {
    accepted_txs_ttl_ms: Duration,
}

struct CrediblePrune {
    assertion_store_prune_config_interval_ms: Option<u64>,
    assertion_store_prune_config_retention_blocks: Option<u64>,
}

fn resolve_credible_required(
    credible_file: &CredibleConfigFile,
) -> Result<CredibleRequired, ConfigError> {
    Ok(CredibleRequired {
        assertion_gas_limit: required_or_env(
            credible_file.assertion_gas_limit,
            "SIDECAR_ASSERTION_GAS_LIMIT",
            "credible.assertion_gas_limit",
        )?,
        assertion_da_rpc_url: required_or_env(
            credible_file.assertion_da_rpc_url.clone(),
            "SIDECAR_ASSERTION_DA_RPC_URL",
            "credible.assertion_da_rpc_url",
        )?,
        event_source_url: required_or_env(
            credible_file.event_source_url.clone(),
            "SIDECAR_EVENT_SOURCE_URL",
            "credible.event_source_url",
        )?,
        assertion_store_db_path: required_or_env(
            credible_file.assertion_store_db_path.clone(),
            "SIDECAR_ASSERTION_STORE_DB_PATH",
            "credible.assertion_store_db_path",
        )?,
        state_oracle: required_or_env(
            credible_file.state_oracle,
            "SIDECAR_STATE_ORACLE",
            "credible.state_oracle",
        )?,
        state_oracle_deployment_block: required_or_env(
            credible_file.state_oracle_deployment_block,
            "SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK",
            "credible.state_oracle_deployment_block",
        )?,
        transaction_results_max_capacity: required_or_env(
            credible_file.transaction_results_max_capacity,
            "SIDECAR_TRANSACTION_RESULTS_MAX_CAPACITY",
            "credible.transaction_results_max_capacity",
        )?,
        #[cfg(feature = "cache_validation")]
        cache_checker_ws_url: required_or_env(
            credible_file.cache_checker_ws_url.clone(),
            "SIDECAR_CACHE_CHECKER_WS_URL",
            "credible.cache_checker_ws_url",
        )?,
    })
}

fn resolve_credible_optional(
    credible_file: &CredibleConfigFile,
) -> Result<CredibleOptional, ConfigError> {
    Ok(CredibleOptional {
        overlay_cache_invalidation_every_block: optional_or_env(
            credible_file.overlay_cache_invalidation_every_block,
            "SIDECAR_OVERLAY_CACHE_INVALIDATION_EVERY_BLOCK",
        )?,
        cache_capacity_bytes: optional_or_env(
            credible_file.cache_capacity_bytes,
            "SIDECAR_CACHE_CAPACITY_BYTES",
        )?,
        flush_every_ms: optional_or_env(credible_file.flush_every_ms, "SIDECAR_FLUSH_EVERY_MS")?,
        poll_interval: parse_env_duration_ms("SIDECAR_POLL_INTERVAL_MS")?
            .or(credible_file.poll_interval)
            .unwrap_or_else(default_poll_interval),
        transaction_observer_db_path: optional_or_env(
            credible_file.transaction_observer_db_path.clone(),
            "SIDECAR_TRANSACTION_OBSERVER_DB_PATH",
        )?,
        transaction_observer_endpoint: optional_or_env(
            credible_file.transaction_observer_endpoint.clone(),
            "SIDECAR_TRANSACTION_OBSERVER_ENDPOINT",
        )?,
        transaction_observer_auth_token: optional_or_env(
            credible_file.transaction_observer_auth_token.clone(),
            "SIDECAR_TRANSACTION_OBSERVER_AUTH_TOKEN",
        )?,
        transaction_observer_endpoint_rps_max: optional_or_env(
            credible_file.transaction_observer_endpoint_rps_max,
            "SIDECAR_TRANSACTION_OBSERVER_ENDPOINT_RPS_MAX",
        )?,
        transaction_observer_poll_interval_ms: optional_or_env(
            credible_file.transaction_observer_poll_interval_ms,
            "SIDECAR_TRANSACTION_OBSERVER_POLL_INTERVAL_MS",
        )?,
    })
}

fn resolve_credible_ttls(credible_file: &CredibleConfigFile) -> Result<CredibleTtls, ConfigError> {
    Ok(CredibleTtls {
        accepted_txs_ttl_ms: parse_env_duration_ms("SIDECAR_ACCEPTED_TXS_TTL_MS")?
            .or(credible_file.accepted_txs_ttl_ms)
            .unwrap_or_else(default_accepted_txs_ttl_ms),
    })
}

fn resolve_credible_prune(
    credible_file: &CredibleConfigFile,
) -> Result<CrediblePrune, ConfigError> {
    Ok(CrediblePrune {
        assertion_store_prune_config_interval_ms: optional_or_env(
            credible_file.assertion_store_prune_config_interval_ms,
            "SIDECAR_ASSERTION_STORE_PRUNE_INTERVAL_MS",
        )?,
        assertion_store_prune_config_retention_blocks: optional_or_env(
            credible_file.assertion_store_prune_config_retention_blocks,
            "SIDECAR_ASSERTION_STORE_PRUNE_RETENTION_BLOCKS",
        )?,
    })
}

fn env_value(key: &str) -> Option<String> {
    env::var(key)
        .ok()
        .and_then(|value| (!value.trim().is_empty()).then_some(value))
}

fn parse_env<T>(key: &str) -> Result<Option<T>, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let Some(raw) = env_value(key) else {
        return Ok(None);
    };

    T::from_str(&raw)
        .map(Some)
        .map_err(|e| ConfigError::ParseError(format!("Failed to parse env {key}: {e}")))
}

fn parse_env_duration_ms(key: &str) -> Result<Option<Duration>, ConfigError> {
    parse_env::<u64>(key).map(|value| value.map(Duration::from_millis))
}

fn parse_env_json<T>(key: &str) -> Result<Option<T>, ConfigError>
where
    T: DeserializeOwned,
{
    let Some(raw) = env_value(key) else {
        return Ok(None);
    };

    serde_json::from_str(&raw)
        .map(Some)
        .map_err(|e| ConfigError::ParseError(format!("Failed to parse env {key} as JSON: {e}")))
}

fn required_or_env<T>(
    file_value: Option<T>,
    env_key: &'static str,
    field_name: &'static str,
) -> Result<T, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    if let Some(value) = parse_env(env_key)? {
        return Ok(value);
    }

    if let Some(value) = file_value {
        return Ok(value);
    }

    Err(ConfigError::MissingRequired(format!(
        "Missing required config value: {field_name} (env {env_key})"
    )))
}

fn optional_or_env<T>(
    file_value: Option<T>,
    env_key: &'static str,
) -> Result<Option<T>, ConfigError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    let env_value = parse_env(env_key)?;
    Ok(env_value.or(file_value))
}

fn required_or_env_with<T>(
    file_value: Option<T>,
    env_key: &'static str,
    field_name: &'static str,
    parse: fn(&str) -> Result<T, String>,
) -> Result<T, ConfigError> {
    if let Some(raw) = env_value(env_key) {
        return parse(&raw)
            .map_err(|e| ConfigError::ParseError(format!("Failed to parse env {env_key}: {e}")));
    }

    if let Some(value) = file_value {
        return Ok(value);
    }

    Err(ConfigError::MissingRequired(format!(
        "Missing required config value: {field_name} (env {env_key})"
    )))
}

fn parse_spec_id(value: &str) -> Result<SpecId, String> {
    let trimmed = value.trim();
    let normalized = trimmed.to_ascii_uppercase();
    let json = if trimmed.starts_with('\"') && trimmed.ends_with('\"') {
        trimmed.to_string()
    } else {
        format!("\"{normalized}\"")
    };

    serde_json::from_str(&json).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env,
        io::Write,
        sync::Mutex,
    };
    use tempfile::NamedTempFile;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct EnvGuard {
        key: &'static str,
        prev: Option<String>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            if let Some(value) = &self.prev {
                unsafe {
                    env::set_var(self.key, value);
                }
            } else {
                unsafe {
                    env::remove_var(self.key);
                }
            }
        }
    }

    fn unset_env_var(key: &'static str) -> EnvGuard {
        let prev = env::var(key).ok();
        unsafe {
            env::remove_var(key);
        }
        EnvGuard { key, prev }
    }

    fn set_env_var(key: &'static str, value: &str) -> EnvGuard {
        let prev = env::var(key).ok();
        unsafe {
            env::set_var(key, value);
        }
        EnvGuard { key, prev }
    }

    const REQUIRED_ENV_KEYS: [&str; 11] = [
        "SIDECAR_CHAIN_SPEC_ID",
        "SIDECAR_CHAIN_ID",
        "SIDECAR_ASSERTION_GAS_LIMIT",
        "SIDECAR_ASSERTION_DA_RPC_URL",
        "SIDECAR_EVENT_SOURCE_URL",
        "SIDECAR_ASSERTION_STORE_DB_PATH",
        "SIDECAR_STATE_ORACLE",
        "SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK",
        "SIDECAR_TRANSACTION_RESULTS_MAX_CAPACITY",
        "SIDECAR_TRANSPORT_BIND_ADDR",
        "SIDECAR_STATE_MINIMUM_STATE_DIFF",
    ];

    const REQUIRED_ENV_KEYS_STATE: [&str; 2] = [
        "SIDECAR_STATE_SOURCES_SYNC_TIMEOUT_MS",
        "SIDECAR_STATE_SOURCES_MONITORING_PERIOD_MS",
    ];

    fn clear_required_envs() -> Vec<EnvGuard> {
        let mut guards = Vec::new();
        for key in REQUIRED_ENV_KEYS.iter().copied() {
            guards.push(unset_env_var(key));
        }
        for key in REQUIRED_ENV_KEYS_STATE.iter().copied() {
            guards.push(unset_env_var(key));
        }
        guards
    }

    fn set_required_envs_defaults() -> Vec<EnvGuard> {
        vec![
            set_env_var("SIDECAR_CHAIN_SPEC_ID", "CANCUN"),
            set_env_var("SIDECAR_CHAIN_ID", "1"),
            set_env_var("SIDECAR_ASSERTION_GAS_LIMIT", "30000000"),
            set_env_var("SIDECAR_ASSERTION_DA_RPC_URL", "http://localhost:8545"),
            set_env_var("SIDECAR_EVENT_SOURCE_URL", "http://localhost:4350/graphql"),
            set_env_var("SIDECAR_ASSERTION_STORE_DB_PATH", "/tmp/store.db"),
            set_env_var(
                "SIDECAR_STATE_ORACLE",
                "0x1234567890123456789012345678901234567890",
            ),
            set_env_var("SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK", "100"),
            set_env_var("SIDECAR_TRANSACTION_RESULTS_MAX_CAPACITY", "10000"),
            set_env_var("SIDECAR_TRANSPORT_BIND_ADDR", "127.0.0.1:3000"),
            set_env_var("SIDECAR_STATE_MINIMUM_STATE_DIFF", "10"),
            set_env_var("SIDECAR_STATE_SOURCES_SYNC_TIMEOUT_MS", "30000"),
            set_env_var("SIDECAR_STATE_SOURCES_MONITORING_PERIOD_MS", "1000"),
        ]
    }

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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  },
  "transport": {
    "bind_addr": "127.0.0.1:3000",
    "health_bind_addr": "127.0.0.1:3001"
  },
  "state": {
    "sources": [
      {
        "type": "eth-rpc",
        "ws_url": "ws://localhost:8548",
        "http_url": "http://localhost:8545"
      },
      {
        "type": "mdbx",
        "mdbx_path": "/data/state_worker.mdbx",
        "depth": 100
      }
    ],
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
        assert_eq!(
            config.credible.event_source_url,
            "http://localhost:4350/graphql"
        );
        assert_eq!(config.credible.assertion_store_db_path, "/tmp/store.db");
        assert_eq!(
            config.credible.transaction_observer_db_path,
            Some("/tmp/observer.db".to_string())
        );
        assert_eq!(
            config.credible.transaction_observer_endpoint,
            Some("http://localhost:3001/api/v1/enforcer/incidents".to_string())
        );
        assert_eq!(
            config.credible.transaction_observer_auth_token,
            Some(SecretString::new("test-token"))
        );
        assert_eq!(
            config.credible.transaction_observer_endpoint_rps_max,
            Some(50)
        );
        assert_eq!(
            config.credible.transaction_observer_poll_interval_ms,
            Some(1000)
        );
        assert_eq!(config.credible.state_oracle_deployment_block, 100);
        assert_eq!(config.credible.transaction_results_max_capacity, 10000);

        // Verify transport config
        assert_eq!(config.transport.bind_addr, "127.0.0.1:3000");

        // Verify state config
        assert_eq!(
            config.state.sources,
            vec![
                StateSourceConfig::EthRpc {
                    ws_url: "ws://localhost:8548".to_string(),
                    http_url: "http://localhost:8545".to_string(),
                },
                StateSourceConfig::Mdbx {
                    mdbx_path: "/data/state_worker.mdbx".to_string(),
                    depth: 100,
                }
            ]
        );
        assert_eq!(config.state.minimum_state_diff, 10);
        assert_eq!(config.state.sources_sync_timeout_ms, 30000);
        assert_eq!(config.state.sources_monitoring_period_ms, 1000);
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
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();

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
            Err(ConfigError::MissingRequired(msg)) => {
                assert!(msg.contains("Missing required config value"));
            }
            _ => panic!("Expected MissingRequired for missing fields"),
        }
    }

    #[test]
    fn test_from_file_env_fallback_for_required_fields() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let _spec_id = set_env_var("SIDECAR_CHAIN_SPEC_ID", "CANCUN");
        let _chain_id = set_env_var("SIDECAR_CHAIN_ID", "1");
        let _gas_limit = set_env_var("SIDECAR_ASSERTION_GAS_LIMIT", "30000000");
        let _da_url = set_env_var("SIDECAR_ASSERTION_DA_RPC_URL", "http://localhost:8545");
        let _graphql_url = set_env_var("SIDECAR_EVENT_SOURCE_URL", "http://localhost:4350/graphql");
        let _store_db = set_env_var("SIDECAR_ASSERTION_STORE_DB_PATH", "/tmp/store.db");
        let _state_oracle = set_env_var(
            "SIDECAR_STATE_ORACLE",
            "0x1234567890123456789012345678901234567890",
        );
        let _state_oracle_block = set_env_var("SIDECAR_STATE_ORACLE_DEPLOYMENT_BLOCK", "100");
        let _tx_results = set_env_var("SIDECAR_TRANSACTION_RESULTS_MAX_CAPACITY", "10000");
        let _bind_addr = set_env_var("SIDECAR_TRANSPORT_BIND_ADDR", "127.0.0.1:3000");
        let _min_state_diff = set_env_var("SIDECAR_STATE_MINIMUM_STATE_DIFF", "10");
        let _sync_timeout = set_env_var("SIDECAR_STATE_SOURCES_SYNC_TIMEOUT_MS", "30000");
        let _monitoring_period = set_env_var("SIDECAR_STATE_SOURCES_MONITORING_PERIOD_MS", "1000");

        let config = Config::from_file(temp_file.path()).unwrap();

        assert_eq!(config.chain.spec_id, SpecId::CANCUN);
        assert_eq!(config.chain.chain_id, 1);
    }

    #[test]
    fn test_env_overrides_file_values() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();

        let _envs = set_required_envs_defaults();
        let _env_override = set_env_var("SIDECAR_TRANSPORT_BIND_ADDR", "127.0.0.1:9999");
        let _env_tag = set_env_var("SIDECAR_BLOCK_TAG", "safe");

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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3001"
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
        assert_eq!(config.transport.bind_addr, "127.0.0.1:9999");
    }

    #[test]
    fn test_optional_env_fallbacks() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();
        let _envs = set_required_envs_defaults();

        let _optional_endpoint = set_env_var(
            "SIDECAR_TRANSACTION_OBSERVER_ENDPOINT",
            "http://example.com/incidents",
        );
        let _optional_token = set_env_var("SIDECAR_TRANSACTION_OBSERVER_AUTH_TOKEN", "token-123");
        let _optional_rps = set_env_var("SIDECAR_TRANSACTION_OBSERVER_ENDPOINT_RPS_MAX", "42");

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();
        assert_eq!(
            config.credible.transaction_observer_endpoint,
            Some("http://example.com/incidents".to_string())
        );
        assert_eq!(
            config.credible.transaction_observer_auth_token,
            Some(SecretString::new("token-123"))
        );
        assert_eq!(
            config.credible.transaction_observer_endpoint_rps_max,
            Some(42)
        );
    }

    #[test]
    fn test_optional_missing_remains_none() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();
        let _envs = set_required_envs_defaults();

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();
        assert!(config.credible.transaction_observer_endpoint.is_none());
        assert!(config.credible.transaction_observer_auth_token.is_none());
        assert!(
            config
                .credible
                .transaction_observer_endpoint_rps_max
                .is_none()
        );
    }

    #[test]
    fn test_invalid_env_value_returns_parse_error() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();
        let _envs = set_required_envs_defaults();
        let _bad_chain_id = set_env_var("SIDECAR_CHAIN_ID", "not-a-number");

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let result = Config::from_file(temp_file.path());
        assert!(result.is_err());
        match result {
            Err(ConfigError::ParseError(msg)) => {
                assert!(msg.contains("SIDECAR_CHAIN_ID"));
            }
            _ => panic!("Expected ParseError for invalid env value"),
        }
    }

    #[test]
    fn test_state_sources_from_env_json() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();
        let _envs = set_required_envs_defaults();

        let _sources = set_env_var(
            "SIDECAR_STATE_SOURCES",
            r#"[{"type":"mdbx","mdbx_path":"/data/state.mdbx","depth":7}]"#,
        );

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();
        assert_eq!(
            config.state.sources,
            vec![StateSourceConfig::Mdbx {
                mdbx_path: "/data/state.mdbx".to_string(),
                depth: 7,
            }]
        );
    }

    #[test]
    fn test_legacy_state_fields_from_env() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _guards = clear_required_envs();
        let _envs = set_required_envs_defaults();

        let _legacy_ws = set_env_var("SIDECAR_STATE_ETH_RPC_SOURCE_WS_URL", "ws://legacy:8546");
        let _legacy_http = set_env_var(
            "SIDECAR_STATE_ETH_RPC_SOURCE_HTTP_URL",
            "http://legacy:8545",
        );
        let _legacy_mdbx = set_env_var("SIDECAR_STATE_WORKER_MDBX_PATH", "/data/legacy_state.mdbx");
        let _legacy_depth = set_env_var("SIDECAR_STATE_WORKER_DEPTH", "5");

        let mut temp_file = NamedTempFile::new().unwrap();
        write!(temp_file, r"{{}}").unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();
        assert_eq!(
            config.state.legacy.eth_rpc_source_ws_url,
            Some("ws://legacy:8546".to_string())
        );
        assert_eq!(
            config.state.legacy.eth_rpc_source_http_url,
            Some("http://legacy:8545".to_string())
        );
        assert_eq!(
            config.state.legacy.state_worker_mdbx_path,
            Some("/data/legacy_state.mdbx".to_string())
        );
        assert_eq!(config.state.legacy.state_worker_depth, Some(5));
        assert!(config.state.legacy.has_any());
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "sources": [
      {{
        "type": "mdbx",
        "mdbx_path": "/data/state_worker.mdbx",
        "depth": 50
      }}
    ],
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "sources": [
      {{
        "type": "mdbx",
        "mdbx_path": "/data/state_worker.mdbx",
        "depth": 3
      }}
    ],
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
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

        assert!(config.state.sources.is_empty());
        assert!(!config.state.legacy.has_any());
    }

    #[test]
    fn test_from_file_with_legacy_state_fields() {
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "eth_rpc_source_ws_url": "ws://legacy.example:8546",
    "eth_rpc_source_http_url": "http://legacy.example:8545",
    "state_worker_mdbx_path": "/data/legacy_state_worker.mdbx",
    "state_worker_depth": 7,
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        assert!(config.state.sources.is_empty());
        assert_eq!(
            config.state.legacy.eth_rpc_source_ws_url,
            Some("ws://legacy.example:8546".to_string())
        );
        assert_eq!(
            config.state.legacy.eth_rpc_source_http_url,
            Some("http://legacy.example:8545".to_string())
        );
        assert_eq!(
            config.state.legacy.state_worker_mdbx_path,
            Some("/data/legacy_state_worker.mdbx".to_string())
        );
        assert_eq!(config.state.legacy.state_worker_depth, Some(7));
    }

    #[test]
    fn test_from_file_with_multiple_sources_preserves_order() {
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "sources": [
      {{
        "type": "eth-rpc",
        "ws_url": "ws://first.example:8546",
        "http_url": "http://first.example:8545"
      }},
      {{
        "type": "eth-rpc",
        "ws_url": "ws://second.example:8546",
        "http_url": "http://second.example:8545"
      }},
      {{
        "type": "mdbx",
        "mdbx_path": "/data/state_worker.mdbx",
        "depth": 42
      }}
    ],
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        assert_eq!(
            config.state.sources,
            vec![
                StateSourceConfig::EthRpc {
                    ws_url: "ws://first.example:8546".to_string(),
                    http_url: "http://first.example:8545".to_string(),
                },
                StateSourceConfig::EthRpc {
                    ws_url: "ws://second.example:8546".to_string(),
                    http_url: "http://second.example:8545".to_string(),
                },
                StateSourceConfig::Mdbx {
                    mdbx_path: "/data/state_worker.mdbx".to_string(),
                    depth: 42,
                }
            ]
        );
    }

    #[test]
    fn test_from_file_with_mixed_sources_including_duplicates() {
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
    "event_source_url": "http://localhost:4350/graphql",
    "assertion_store_db_path": "/tmp/store.db",
    "transaction_observer_db_path": "/tmp/observer.db",
    "transaction_observer_endpoint": "http://localhost:3001/api/v1/enforcer/incidents",
    "transaction_observer_auth_token": "test-token",
    "transaction_observer_endpoint_rps_max": 50,
    "transaction_observer_poll_interval_ms": 1000,
    "state_oracle": "0x1234567890123456789012345678901234567890",
    "state_oracle_deployment_block": 100,
    "transaction_results_max_capacity": 10000
  }},
  "transport": {{
    "bind_addr": "127.0.0.1:3000"
  }},
  "state": {{
    "sources": [
      {{
        "type": "mdbx",
        "mdbx_path": "/data/state_worker_a.mdbx",
        "depth": 10
      }},
      {{
        "type": "mdbx",
        "mdbx_path": "/data/state_worker_b.mdbx",
        "depth": 20
      }},
      {{
        "type": "eth-rpc",
        "ws_url": "ws://rpc.example:8546",
        "http_url": "http://rpc.example:8545"
      }}
    ],
    "minimum_state_diff": 10,
    "sources_sync_timeout_ms": 30000,
    "sources_monitoring_period_ms": 1000
  }}
}}"#
        )
        .unwrap();
        temp_file.flush().unwrap();

        let config = Config::from_file(temp_file.path()).unwrap();

        assert_eq!(
            config.state.sources,
            vec![
                StateSourceConfig::Mdbx {
                    mdbx_path: "/data/state_worker_a.mdbx".to_string(),
                    depth: 10,
                },
                StateSourceConfig::Mdbx {
                    mdbx_path: "/data/state_worker_b.mdbx".to_string(),
                    depth: 20,
                },
                StateSourceConfig::EthRpc {
                    ws_url: "ws://rpc.example:8546".to_string(),
                    http_url: "http://rpc.example:8545".to_string(),
                }
            ]
        );
    }
}
