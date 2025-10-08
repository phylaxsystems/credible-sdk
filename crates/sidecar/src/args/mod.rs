//! Sidecar command arguments
use assertion_executor::{
    primitives::{
        Address,
        SpecId,
    },
    store::BlockTag,
};
use std::{
    path::PathBuf,
    str::FromStr,
};

fn parse_spec_id(s: &str) -> Result<SpecId, String> {
    SpecId::from_str(s).map_err(|_| format!("Invalid spec id: {s}"))
}

/// Default contract address for the state oracle contract. Used for indexing assertions
pub const DEFAULT_STATE_ORACLE_ADDRESS: &str = "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b";

/// Parameters for the chain we receive tx from
#[derive(Default, Debug, Clone, PartialEq, Eq, clap::Args)]
#[command(next_help_heading = "Rollup")]
pub struct ChainArgs {
    /// What EVM specification to use. Only latest for now
    #[arg(
        long = "chain.spec-id",
        env = "CHAIN_SPEC_ID",
        default_value = "latest",
        value_parser = parse_spec_id,
        value_enum
    )]
    pub spec_id: SpecId,

    // Chain ID
    #[arg(long = "chain.chain-id", default_value = "1", env = "CHAIN_CHAIN_ID")]
    pub chain_id: u64,
}

/// Parameters for Credible configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CredibleArgs {
    /// Gas limit for assertion execution
    #[arg(
        long = "credible.assertion-gas-limit",
        env = "CREDIBLE_ASSERTION_GAS_LIMIT",
        default_value = "3000000"
    )]
    pub assertion_gas_limit: u64,

    /// Overlay cache capacity in elements
    #[arg(
        long = "credible.overlay-cache-capacity",
        env = "CREDIBLE_OVERLAY_CACHE_CAPACITY",
        default_value = "100"
    )]
    pub overlay_cache_capacity: Option<usize>,

    /// Sled cache capacity, used in the `FsDb`, 256mb default
    #[arg(
        long = "credible.cache-capacity-bytes",
        env = "CREDIBLE_CACHE_CAPACITY_BYTES",
        default_value = "256"
    )]
    pub cache_capacity_bytes: Option<usize>,

    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    #[arg(
        long = "credible.flush-every-ms",
        env = "CREDIBLE_FLUSH_EVERY_MS",
        default_value = "5000"
    )]
    pub flush_every_ms: Option<usize>,

    /// HTTP URL of the assertion DA
    #[arg(
        long = "credible.assertion-da-rpc-url",
        env = "CREDIBLE_ASSERTION_DA_RPC_URL",
        default_value = "http://localhost:5001"
    )]
    pub assertion_da_rpc_url: String,

    /// WS URL the RPC store will use to index assertions
    #[arg(
        long = "credible.indexer-rpc-url",
        env = "CREDIBLE_INDEXER_RPC_URL",
        default_value = "ws://localhost:8546"
    )]
    pub indexer_rpc_url: String,
    /// Path to the indexer database (separate from main assertion store)
    #[arg(
        long = "credible.indexer-db-path",
        env = "CREDIBLE_INDEXER_DB_PATH",
        default_value = "indexer_database"
    )]
    pub indexer_db_path: PathBuf,

    /// Path to the rpc store db
    #[arg(
        long = "credible.assertion-store-db-path",
        env = "CREDIBLE_ASSERTION_STORE_DB_PATH",
        default_value = "assertion_store_database"
    )]
    pub assertion_store_db_path: PathBuf,

    /// Block tag to use for indexing assertions.
    #[arg(
        long = "credible.block-tag",
        env = "CREDIBLE_BLOCK_TAG",
        default_value = "finalized",
        value_enum
    )]
    pub block_tag: BlockTag,

    /// Contract address of the state oracle contract, used to query assertion info
    #[arg(
        long = "credible.state-oracle",
        env = "CREDIBLE_STATE_ORACLE",
        default_value = DEFAULT_STATE_ORACLE_ADDRESS
    )]
    pub state_oracle: Address,

    /// Block number of the state oracle deployment
    #[arg(
        long = "credible.state-oracle-deployment-block",
        env = "CREDIBLE_STATE_ORACLE_DEPLOYMENT_BLOCK",
        default_value = "0"
    )]
    pub state_oracle_deployment_block: u64,

    /// Maximum capacity for transaction results
    #[arg(
        long = "credible.transaction-results-max-capacity",
        env = "CREDIBLE_TRANSACTION_RESULTS_MAX_CAPACITY",
        default_value = "100"
    )]
    pub transaction_results_max_capacity: usize,
}

/// Select which transport protocol to run
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum TransportProtocolArg {
    #[value(name = "http")]
    Http,
    #[value(name = "grpc")]
    Grpc,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct HttpTransportArgs {
    /// Server bind address and port
    #[arg(
        long = "transport.bind-addr",
        env = "TRANSPORT_BIND_ADDR",
        default_value = "127.0.0.1:8080"
    )]
    pub bind_addr: String,
}

/// State sources configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct StateArgs {
    /// Sequencer bind address and port
    #[arg(long = "state.sequencer-url", env = "STATE_SEQUENCER_URL")]
    pub sequencer_url: Option<String>,

    /// Besu client bind address and port
    #[arg(long = "state.besu-client-ws-url", env = "STATE_BESU_CLIENT_WS_URL")]
    pub besu_client_ws_url: Option<String>,

    /// Redis bind address and port
    #[arg(long = "state.redis-url", env = "STATE_REDIS_URL")]
    pub redis_url: Option<String>,

    /// Minimum state diff to consider a cache synced
    #[arg(
        long = "state.minimum-state-diff",
        default_value = "100",
        env = "STATE_MINIMUM_STATE_DIFF"
    )]
    pub minimum_state_diff: u64,

    /// Maximum time (ms) the engine will wait for a state source to report as
    /// synced before failing a transaction.
    #[arg(
        long = "state.sources-sync-timeout-ms",
        default_value = "1000",
        env = "STATE_SOURCES_SYNC_TIMEOUT_MS"
    )]
    pub sources_sync_timeout_ms: u64,
}

impl Default for StateArgs {
    fn default() -> Self {
        Self {
            sequencer_url: None,
            besu_client_ws_url: None,
            redis_url: None,
            minimum_state_diff: 100,
            sources_sync_timeout_ms: 100,
        }
    }
}

/// Main sidecar arguments that extend `TelemetryArgs` and `CredibleArgs`
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    /// Which transport protocol to run
    #[arg(
        long = "transport.protocol",
        env = "TRANSPORT_PROTOCOL",
        default_value = "http",
        value_enum
    )]
    pub transport_protocol: TransportProtocolArg,
    #[command(flatten)]
    pub chain: ChainArgs,
    #[command(flatten)]
    pub credible: CredibleArgs,
    #[command(flatten)]
    pub transport: HttpTransportArgs,
    #[command(flatten)]
    pub state: StateArgs,
}
