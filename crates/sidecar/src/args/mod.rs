//! Sidecar command arguments
use assertion_executor::{primitives::Address, store::BlockTag,};
use assertion_executor::primitives::SpecId;
use std::path::PathBuf;

/// Wrapper for SpecId that implements clap::ValueEnum
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum SpecIdArg {
    #[value(name = "latest")]
    Latest,
}

impl Default for SpecIdArg {
    fn default() -> Self {
        Self::Latest
    }
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
        long = "chain.chain-block-time",
        default_value = "1000",
        env = "CHAIN_BLOCK_TIME"
    )]
    pub chain_block_time: u64,

    /// How much time extra to wait for the block building job to complete and not get garbage collected
    #[arg(long = "chain.extra-block-deadline-secs", default_value = "20")]
    pub extra_block_deadline_secs: u64,

    /// What EVM specification to use. Only latest for now
    #[arg(long = "chain.spec-id", default_value = "latest", env = "CHAIN_SPEC_ID", value_enum)]
    pub spec_id: SpecIdArg,

    /// Transport JSON-RPC server URL and port
    #[arg(long = "chain.transport-url", default_value = "http://127.0.0.1:8545", env = "CHAIN_RPC_URL")]
    pub rpc_url: String,
}

/// Parameters for telemetry configuration
#[derive(Debug, Clone, Default, PartialEq, Eq, clap::Args)]
pub struct TelemetryArgs {
    /// Inverted sampling frequency in blocks. 1 - each block, 100 - every 100th block.
    #[arg(
        long = "telemetry.sampling-ratio",
        env = "SAMPLING_RATIO",
        default_value = "100"
    )]
    pub sampling_ratio: u64,
}

/// Parameters for Credible configuration
#[derive(Debug, Clone, PartialEq, Eq, clap::Args)]
pub struct CredibleArgs {
    /// Soft timeout for credible block building in milliseconds
    #[arg(
        long = "ae.soft-timeout-ms",
        default_value = "650",
        env = "AE_BLOCK_TIME_LIMIT"
    )]
    pub soft_timeout_ms: u64,

    /// Gas limit for assertion execution
    #[arg(
        long = "ae.assertion-gas-limit",
        default_value = "3000000",
        env = "AE_ASSERTION_GAS_LIMIT"
    )]
    pub assertion_gas_limit: u64,

    /// Overlay cache capacity, 1gb default
    #[arg(
        long = "ae.overlay_cache_capacity_bytes",
        default_value = "1024000000",
        env = "AE_CACHE_CAPACITY_BYTES"
    )]
    pub overlay_cache_capacity_bytes: Option<usize>,

    /// Path to the `assertion-executor` database.
    #[arg(long = "ae.db_path", default_value = "ae_database", env = "AE_DB_PATH")]
    pub db_path: PathBuf,

    /// Sled cache capacity, used in the `FsDb`, 256mb default
    #[arg(
        long = "ae.cache_capacity_bytes",
        default_value = "256000000",
        env = "AE_CACHE_CAPACITY_BYTES"
    )]
    pub cache_capacity_bytes: Option<usize>,

    /// How often in ms will the `FsDb` be flushed to disk, 5 sec default
    #[arg(
        long = "ae.flush_every_ms",
        default_value = "5000",
        env = "AE_FLUSH_EVERY_MS"
    )]
    pub flush_every_ms: Option<usize>,

    /// `FsDb` compression level, default to 3
    #[arg(
        long = "ae.fs_compression_level",
        default_value = "3",
        env = "AE_FS_COMPRESSION_LEVEL"
    )]
    pub zstd_compression_level: Option<i32>,

    /// WS URL the RPC store will use to index assertions
    #[arg(
        long = "ae.rpc_url",
        default_value = "ws://localhost:8546",
        env = "AE_RPC_STORE_URL"
    )]
    pub indexer_rpc: String,

    /// HTTP URL of the assertion DA
    #[arg(
        long = "ae.rpc_da_url",
        default_value = "http://localhost:5001",
        env = "AE_RPC_DA_URL"
    )]
    pub rpc_da_url: String,

    /// Path to the rpc store db
    #[arg(
        long = "ae.rpc_store_db_path",
        default_value = "rpc_store_database",
        env = "AE_RPC_STORE_DB_PATH"
    )]
    pub rpc_store_db: PathBuf,

    /// Block tag to use for indexing assertions.
    #[arg(
        long = "ae.block_tag",
        env = "AE_BLOCK_TAG",
        default_value = "finalized",
        value_enum
    )]
    pub block_tag: BlockTag,

    /// Contract address of the state oracle contract, used to query assertion info
    #[arg(
        long = "ae.oracle_contract",
        default_value = DEFAULT_STATE_ORACLE_ADDRESS,
        env = "AE_ORACLE_CONTRACT"
    )]
    pub oracle_contract: Address,
}

/// Main sidecar arguments that extend TelemetryArgs and CredibleArgs
#[derive(Debug, Clone, PartialEq, Eq, clap::Parser)]
#[command(name = "sidecar", about = "Credible layer sidecar")]
pub struct SidecarArgs {
    #[command(flatten)]
    pub telemetry: TelemetryArgs,

    #[command(flatten)]
    pub credible: CredibleArgs,

    #[command(flatten)]
    pub rollup: ChainArgs,
}
