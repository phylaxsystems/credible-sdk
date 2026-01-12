//! Error types for MDBX state management.

use crate::AddressHash;
use alloy::primitives::{
    Address,
    B256,
};

/// Errors that can occur during state operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Database error
    #[error("Database error: {0}")]
    Database(#[source] reth_db::DatabaseError),

    /// Failed to open the database
    #[error("Failed to open database at {path}: {message}")]
    DatabaseOpen { path: String, message: String },

    /// Failed to create a database directory
    #[error("Failed to create database directory at {path}: {source}")]
    CreateDir {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// Failed to create a table
    #[error("Failed to create table '{table}': {message}")]
    CreateTable { table: String, message: String },

    /// Transaction commit failed
    #[error("Failed to commit transaction: {0}")]
    CommitFailed(String),

    /// Failed to decode hex string
    #[error("Failed to decode hex value '{0}'")]
    HexDecode(String, #[source] hex::FromHexError),

    /// Failed to parse U256
    #[error("Failed to parse U256 from '{0}'")]
    ParseU256(String, #[source] alloy::primitives::ruint::ParseError),

    /// Block not found in expected namespace
    #[error("Block {block_number} not found in namespace {namespace_idx}")]
    BlockNotFound {
        block_number: u64,
        namespace_idx: u8,
    },

    /// Block is not available in the circular buffer
    #[error("Block {0} is not available in the circular buffer (oldest available: {1})")]
    BlockNotAvailable(u64, u64),

    /// Metadata is not available
    #[error("Metadata is not available")]
    MetadataNotAvailable,

    /// Missing state diff needed for reconstruction
    #[error(
        "Missing state diff for block {needed_block} (required to reconstruct state at block {target_block})"
    )]
    MissingStateDiff {
        needed_block: u64,
        target_block: u64,
    },

    /// Failed to serialize state diff
    #[error("Failed to serialize state diff for block {0}")]
    SerializeDiff(u64, #[source] serde_json::Error),

    /// Failed to deserialize state diff
    #[error("Failed to deserialize state diff for block {0}")]
    DeserializeDiff(u64, #[source] serde_json::Error),

    /// Invalid namespace calculation
    #[error("Invalid namespace index {0} for buffer size {1}")]
    InvalidNamespace(u8, u8),

    /// Invalid buffer size
    #[error("Buffer size must be greater than 0")]
    InvalidBufferSize,

    /// Invalid B256 length
    #[error("Invalid B256 length: expected 32 bytes, got {0}")]
    InvalidB256Length(usize),

    /// Integer conversion error
    #[error("Integer conversion error")]
    IntConversion(#[source] std::num::TryFromIntError),

    /// Account not found
    #[error("Account {0} not found at block {1}")]
    AccountNotFound(Address, u64),

    /// Code not found for hash
    #[error("Code not found for hash {0} at block {1}")]
    CodeNotFound(B256, u64),

    /// No data in database
    #[error("Database is empty - no blocks have been written")]
    EmptyDatabase,

    /// Codec error
    #[error("Codec error: {0}")]
    Codec(String),

    /// Duplicate account in block state update
    #[error("Duplicate account in BlockStateUpdate: {0:x}")]
    DuplicateAccount(AddressHash),
}

/// Result type alias for state operations.
pub type StateResult<T> = Result<T, StateError>;

impl From<std::num::TryFromIntError> for StateError {
    fn from(err: std::num::TryFromIntError) -> Self {
        StateError::IntConversion(err)
    }
}
