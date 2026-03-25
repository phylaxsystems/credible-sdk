use crate::geth_version::GethVersionError;
use alloy::primitives::Address;
use mdbx::common::error::StateError;
use thiserror::Error;

pub type Result<T> = std::result::Result<T, StateWorkerError>;

type BoxDynError = Box<dyn std::error::Error + Send + Sync>;

pub fn boxed_error(error: impl std::error::Error + Send + Sync + 'static) -> BoxDynError {
    Box::new(error)
}

#[derive(Debug, Error)]
pub enum StateWorkerError {
    #[error("failed to connect to websocket provider {ws_url}")]
    ConnectProvider {
        ws_url: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to get client version via web3_clientVersion")]
    ClientVersion {
        #[source]
        source: BoxDynError,
    },
    #[error(transparent)]
    GethVersion(#[from] GethVersionError),
    #[error("failed to initialize database client at {path}")]
    DatabaseInit {
        path: String,
        #[source]
        source: StateError,
    },
    #[error("failed to read current block from the database")]
    DatabaseReadCurrentBlock {
        #[source]
        source: BoxDynError,
    },
    #[error("failed to read current block from database during genesis hydration")]
    DatabaseReadGenesisStatus {
        #[source]
        source: BoxDynError,
    },
    #[error("failed to persist block {block_number}")]
    PersistBlock {
        block_number: u64,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to persist genesis block")]
    PersistGenesisBlock {
        #[source]
        source: BoxDynError,
    },
    #[error("failed to get latest block number from provider")]
    GetBlockNumber {
        #[source]
        source: BoxDynError,
    },
    #[error("failed to subscribe to newHeads")]
    SubscribeBlocks {
        #[source]
        source: BoxDynError,
    },
    #[error("block subscription completed")]
    BlockSubscriptionCompleted,
    #[error("missing block {block_number} (next block: {next_block})")]
    MissingBlock { block_number: u64, next_block: u64 },
    #[error("failed to fetch block {block_number}")]
    GetBlockByNumber {
        block_number: u64,
        #[source]
        source: BoxDynError,
    },
    #[error("block {block_number} not found")]
    BlockNotFound { block_number: u64 },
    #[error("failed to trace block {block_number}")]
    TraceBlock {
        block_number: u64,
        #[source]
        source: BoxDynError,
    },
    #[error("trace failed or unexpected tracer type (tx: {tx_hash})")]
    UnexpectedTraceResult { tx_hash: String },
    #[error("failed to deserialize genesis JSON")]
    GenesisJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to parse alloc entry for address {address}")]
    GenesisAllocEntry {
        address: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse address {value}")]
    ParseAddress {
        value: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse numeric value {value}")]
    ParseNumericValue {
        value: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse nonce for address {address}")]
    ParseNonce {
        address: Address,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse u64 value {value}")]
    ParseU64Value {
        value: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse storage slot key {slot}")]
    ParseStorageSlotKey {
        slot: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to parse storage slot value {value}")]
    ParseStorageSlotValue {
        value: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to decode hex value {value}")]
    DecodeHexValue {
        value: String,
        #[source]
        source: BoxDynError,
    },
    #[error("failed to read genesis file {path}")]
    ReadGenesisFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse genesis file {path}")]
    ParseGenesisFile {
        path: String,
        #[source]
        source: BoxDynError,
    },
    #[error("missing parent block hash for EIP-2935 at block {block_number}")]
    MissingParentBlockHash { block_number: u64 },
    #[error("missing parent beacon block root for EIP-4788 at block {block_number}")]
    MissingParentBeaconBlockRoot { block_number: u64 },
    #[error("genesis block cannot have non-zero parent beacon root")]
    GenesisParentBeaconRootNonZero,
}
