//! Error types for Redis state management.

use alloy::primitives::{
    Address,
    B256,
};
use std::num::TryFromIntError;

/// Errors that can occur during Redis state operations.
#[derive(Debug, thiserror::Error)]
pub enum StateError {
    /// Redis connection or query error
    #[error("Redis error")]
    Redis(#[source] redis::RedisError),

    /// Failed to decode hex string
    #[error("Failed to decode hex value '{0}'")]
    HexDecode(String, #[source] hex::FromHexError),

    /// Failed to parse U256
    #[error("Failed to parse U256 from '{0}'")]
    ParseU256(String, #[source] alloy::primitives::ruint::ParseError),

    /// Failed to parse integer
    #[error("Failed to parse integer from '{0}'")]
    ParseInt(String, #[source] std::num::ParseIntError),

    /// Missing required account field
    #[error("Missing required account field: {0}")]
    MissingField(&'static str),

    /// Block not found in expected namespace
    #[error("Block {block_number} not found in namespace")]
    BlockNotFound { block_number: u64 },

    /// Block is not available in the circular buffer
    #[error("Block {0} is not available in the circular buffer")]
    BlockNotAvailable(u64),

    /// Block is not available in Redis
    #[error("Block {0} is not available in Redis (may have been evicted from circular buffer)")]
    BlockNotInRedis(u64),

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
    #[error("Invalid namespace calculation for block {0} with buffer size {1}")]
    InvalidNamespace(u64, usize),

    /// Invalid buffer size
    #[error("Buffer size must be greater than 0")]
    InvalidBufferSize,

    /// Invalid B256 length
    #[error("Invalid B256 length: expected 32 bytes, got {0}")]
    InvalidB256Length(usize),

    /// Integer conversion error
    #[error("Integer conversion error")]
    IntConversion(#[source] std::num::TryFromIntError),

    /// Conflicting namespace rotation size in Redis metadata
    #[error(
        "State dump index count mismatch: state worker configured for {existing} indices but requested {requested}"
    )]
    StateDumpIndexMismatch { existing: usize, requested: usize },

    /// Account not found
    #[error("Account {0} not found at block {1}")]
    AccountNotFound(Address, u64),

    /// Code not found for hash
    #[error("Code not found for hash {0} at block {1}")]
    CodeNotFound(B256, u64),

    /// Namespace is locked for writing
    #[error(
        "Namespace {namespace} is locked for writing block {target_block} (started at {started_at})"
    )]
    NamespaceLocked {
        namespace: String,
        target_block: u64,
        started_at: i64,
    },

    /// Failed to acquire write lock
    #[error(
        "Failed to acquire write lock for namespace {namespace}: already locked by {existing_writer}"
    )]
    LockAcquisitionFailed {
        namespace: String,
        existing_writer: String,
    },

    /// Failed to serialize lock data
    #[error("Failed to serialize lock data")]
    SerializeLock(#[source] serde_json::Error),

    /// Failed to deserialize lock data
    #[error("Failed to deserialize lock data")]
    DeserializeLock(#[source] serde_json::Error),

    /// Invalid block length
    #[error("Invalid block length")]
    InvalidLength(#[source] TryFromIntError),

    /// Stale lock detected during recovery
    #[error(
        "Stale lock detected for namespace {namespace}: writer {writer_id} started at {started_at}"
    )]
    StaleLockDetected {
        namespace: String,
        writer_id: String,
        started_at: i64,
    },

    /// Lock was lost during write operation (another process cleared it)
    #[error("Write lock was lost for namespace {namespace} during block {block_number} commit")]
    LockLost {
        namespace: String,
        block_number: u64,
    },
}

/// Result type alias for state operations.
pub type StateResult<T> = Result<T, StateError>;

// Conversion implementations for automatic error conversion
impl From<redis::RedisError> for StateError {
    fn from(err: redis::RedisError) -> Self {
        StateError::Redis(err)
    }
}

impl From<std::num::TryFromIntError> for StateError {
    fn from(err: std::num::TryFromIntError) -> Self {
        StateError::IntConversion(err)
    }
}
