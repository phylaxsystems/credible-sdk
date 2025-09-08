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
        default_value = "1000",
        env = "CHAIN_BLOCK_TIME"
    )]
    pub block_time: u64,

    /// How much time extra to wait for the block building job to complete and not get garbage collected
    #[arg(
        long = "chain.extra-block-deadline-secs",
        default_value = "20",
        env = "CHAIN_EXTRA_BLOCK_DEADLINE_SECS"
    )]
    pub extra_block_deadline_secs: u64,

    /// What EVM specification to use. Only latest for now
    #[arg(
        long = "chain.spec-id",
        default_value = "latest",
        env = "CHAIN_SPEC_ID",
        value_enum
    )]
    pub spec_id: SpecIdArg,

    // Chain ID
    #[arg(long = "chain.chain-id", default_value = "1", env = "CHAIN_CHAIN_ID")]
    pub chain_id: u64,

    /// RPC node URL and port
    #[arg(
        long = "chain.rpc-url",
        default_value = "http://127.0.0.1:8545",
        env = "CHAIN_RPC_URL"
    )]
    pub rpc_url: String,
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

/// Parameters for Credible configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CredibleArgs {
    /// Soft timeout for credible block building in milliseconds
    #[arg(
        long = "credible.soft-timeout-ms",
        default_value = "650",
        env = "CREDIBLE_SOFT_TIMEOUT_MS"
    )]
    pub soft_timeout_ms: u64,

    /// Gas limit for assertion execution
    #[arg(
        long = "credible.assertion-gas-limit",
        default_value = "3000000",
        env = "CREDIBLE_ASSERTION_GAS_LIMIT"
    )]
    pub assertion_gas_limit: u64,

    /// Overlay cache capacity, 1gb default
    #[arg(
        long = "credible.overlay-cache-capacity-bytes",
        default_value = "1024000000",
        env = "CREDIBLE_OVERLAY_CACHE_CAPACITY_BYTES"
    )]
    pub overlay_cache_capacity_bytes: Option<usize>,

    /// Path to the `assertion-executor` database.
    #[arg(
        long = "credible.assertion-executor-db-path",
        default_value = "ae_database",
        env = "CREDIBLE_ASSERTION_EXECUTOR_DB_PATH"
    )]
    pub assertion_executor_db_path: PathBuf,

    /// Sled cache capacity, used in the `FsDb`, 256mb default
    #[arg(
        long = "credible.cache-capacity-bytes",
        default_value = "256000000",
        env = "CREDIBLE_CACHE_CAPACITY_BYTES"
    )]
    pub cache_capacity_bytes: Option<usize>,

    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    #[arg(
        long = "credible.flush-every-ms",
        default_value = "5000",
        env = "CREDIBLE_FLUSH_EVERY_MS"
    )]
    pub flush_every_ms: Option<usize>,

    /// `FsDb` compression level, default to 3
    #[arg(
        long = "credible.zstd-compression-level",
        default_value = "3",
        env = "CREDIBLE_ZSTD_COMPRESSION_LEVEL"
    )]
    pub zstd_compression_level: Option<i32>,

    /// WS URL the RPC store will use to index assertions
    #[arg(
        long = "credible.indexer-rpc-url",
        default_value = "ws://localhost:8546",
        env = "CREDIBLE_INDEXER_RPC_URL"
    )]
    pub indexer_rpc_url: String,

    /// HTTP URL of the assertion DA
    #[arg(
        long = "credible.assertion-da-rpc-url",
        default_value = "http://localhost:5001",
        env = "CREDIBLE_ASSERTION_DA_RPC_URL"
    )]
    pub assertion_da_rpc_url: String,

    /// Path to the rpc store db
    #[arg(
        long = "credible.assertion-store-db-path",
        default_value = "assertion_store_database",
        env = "CREDIBLE_ASSERTION_STORE_DB_PATH"
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
        default_value = DEFAULT_STATE_ORACLE_ADDRESS,
        env = "CREDIBLE_STATE_ORACLE",
        required = true
    )]
    pub state_oracle: Address,

    /// Path to the indexer database (separate from main assertion store)
    #[arg(
        long = "credible.indexer-db-path",
        default_value = "indexer_database",
        env = "CREDIBLE_INDEXER_DB_PATH"
    )]
    pub indexer_db_path: PathBuf,

    #[arg(
        long = "credible.transaction-results-max-capacity",
        default_value = "1000000",
        env = "CREDIBLE_TRANSACTION_RESULTS_MAX_CAPACITY"
    )]
    pub transaction_results_max_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct HttpTransportArgs {
    /// Server bind address and port
    #[arg(
        long = "transport.bind-addr",
        default_value = "127.0.0.1:8080",
        env = "TRANSPORT_BIND_ADDR"
    )]
    pub bind_addr: String,
}

/// Main sidecar arguments that extend `TelemetryArgs` and `CredibleArgs`
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    #[command(flatten)]
    pub telemetry: TelemetryArgs,

    #[command(flatten)]
    pub credible: CredibleArgs,

    #[command(flatten)]
    pub chain: ChainArgs,

    #[command(flatten)]
    pub transport: HttpTransportArgs,
}
