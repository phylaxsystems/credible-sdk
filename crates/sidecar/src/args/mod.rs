//! Sidecar command arguments
use assertion_executor::{
    primitives::{
        Address,
        SpecId,
    },
    store::BlockTag,
};
use std::path::PathBuf;

/// Wrapper for `SpecId` that implements `clap::ValueEnum`
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum, Default)]
pub enum SpecIdArg {
    #[default]
    #[value(name = "latest")]
    Latest,
}

impl From<SpecIdArg> for SpecId {
    fn from(_arg: SpecIdArg) -> Self {
        SpecId::default()
    }
}

/// Default contract address for the state oracle contract. Used for indexing assertions
pub const DEFAULT_STATE_ORACLE_ADDRESS: &str = "0x6dD3f12ce435f69DCeDA7e31605C02Bb5422597b";

/// Parameters for the chain we receive tx from
#[derive(Default, Debug, Clone, PartialEq, Eq, clap::Args)]
#[command(next_help_heading = "Rollup")]
pub struct ChainArgs {
    /// chain block time in milliseconds
    #[arg(
        long = "chain.block-time",
        env = "CHAIN_BLOCK_TIME",
        default_value = "1000"
    )]
    pub block_time: u64,

    /// How much time extra to wait for the block building job to complete and not get garbage collected
    #[arg(
        long = "chain.extra-block-deadline-secs",
        env = "CHAIN_EXTRA_BLOCK_DEADLINE_SECS",
        default_value = "20"
    )]
    pub extra_block_deadline_secs: u64,

    /// What EVM specification to use. Only latest for now
    #[arg(
        long = "chain.spec-id",
        env = "CHAIN_SPEC_ID",
        default_value = "latest",
        value_enum
    )]
    pub spec_id: SpecIdArg,

    // Chain ID
    #[arg(long = "chain.chain-id", default_value = "1", env = "CHAIN_CHAIN_ID")]
    pub chain_id: u64,

    /// RPC node URL and port
    #[arg(
        long = "chain.rpc-url",
        env = "CHAIN_RPC_URL",
        default_value = "http://127.0.0.1:8545"
    )]
    pub rpc_url: String,

    /// Besu client websocket URL
    #[arg(
        long = "chain.besu-client-ws-url",
        default_value = "ws://127.0.0.1:8546",
        env = "CHAIN_BESU_CLIENT_WS_URL"
    )]
    pub besu_client_ws_url: String,
}

/// Parameters for Credible configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CredibleArgs {
    /// Soft timeout for credible block building in milliseconds
    #[arg(
        long = "credible.soft-timeout-ms",
        env = "CREDIBLE_SOFT_TIMEOUT_MS",
        default_value = "650"
    )]
    pub soft_timeout_ms: u64,

    /// Gas limit for assertion execution
    #[arg(
        long = "credible.assertion-gas-limit",
        env = "CREDIBLE_ASSERTION_GAS_LIMIT",
        default_value = "3000000"
    )]
    pub assertion_gas_limit: u64,

    /// Overlay cache capacity, 1gb default
    #[arg(
        long = "credible.overlay-cache-capacity-bytes",
        env = "CREDIBLE_OVERLAY_CACHE_CAPACITY_BYTES",
        default_value = "1024000000"
    )]
    pub overlay_cache_capacity_bytes: Option<usize>,

    /// Path to the `assertion-executor` database.
    #[arg(
        long = "credible.assertion-executor-db-path",
        env = "CREDIBLE_ASSERTION_EXECUTOR_DB_PATH",
        default_value = "ae_database"
    )]
    pub assertion_executor_db_path: PathBuf,

    /// Sled cache capacity, used in the `FsDb`, 256mb default
    #[arg(
        long = "credible.cache-capacity-bytes",
        env = "CREDIBLE_CACHE_CAPACITY_BYTES",
        default_value = "256000000"
    )]
    pub cache_capacity_bytes: Option<usize>,

    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    #[arg(
        long = "credible.flush-every-ms",
        env = "CREDIBLE_FLUSH_EVERY_MS",
        default_value = "5000"
    )]
    pub flush_every_ms: Option<usize>,

    /// `FsDb` compression level, default to 3
    #[arg(
        long = "credible.zstd-compression-level",
        env = "CREDIBLE_ZSTD_COMPRESSION_LEVEL",
        default_value = "3"
    )]
    pub zstd_compression_level: Option<i32>,

    /// WS URL the RPC store will use to index assertions
    #[arg(
        long = "credible.indexer-rpc-url",
        env = "CREDIBLE_INDEXER_RPC_URL",
        default_value = "ws://localhost:8546"
    )]
    pub indexer_rpc_url: String,

    /// HTTP URL of the assertion DA
    #[arg(
        long = "credible.assertion-da-rpc-url",
        env = "CREDIBLE_ASSERTION_DA_RPC_URL",
        default_value = "http://localhost:5001"
    )]
    pub assertion_da_rpc_url: String,

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

    /// Path to the indexer database (separate from main assertion store)
    #[arg(
        long = "credible.indexer-db-path",
        env = "CREDIBLE_INDEXER_DB_PATH",
        default_value = "indexer_database"
    )]
    pub indexer_db_path: PathBuf,

    #[arg(
        long = "credible.transaction-results-max-capacity",
        env = "CREDIBLE_TRANSACTION_RESULTS_MAX_CAPACITY",
        default_value = "1000000"
    )]
    pub transaction_results_max_capacity: usize,
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

/// Parameters for telemetry configuration
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct TelemetryArgs {
    /// Inverted sampling frequency in blocks. 1 - each block, 100 - every 100th block.
    #[arg(
        long = "telemetry.sampling-ratio",
        env = "TELEMETRY_SAMPLING_RATIO",
        default_value = "100"
    )]
    pub sampling_ratio: u64,
}

/// Main sidecar arguments that extend `TelemetryArgs` and `CredibleArgs`
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    #[command(flatten)]
    pub chain: ChainArgs,
    #[command(flatten)]
    pub credible: CredibleArgs,
    #[command(flatten)]
    pub transport: HttpTransportArgs,
    #[command(flatten)]
    pub telemetry: TelemetryArgs,
}
