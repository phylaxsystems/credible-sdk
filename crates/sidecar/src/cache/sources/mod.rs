use assertion_executor::db::DatabaseRef;
use revm::context::DBErrorMarker;
use state_store::common::error::StateError;
use std::fmt::{
    Debug,
    Display,
};
use thiserror::Error;

pub mod besu_client;
mod json_rpc_db;
pub mod redis;
pub mod sequencer;

/// A data source that provides blockchain state information.
///
/// Sources are components that can retrieve blockchain data such as account
/// information, block hashes, bytecode, and storage values. Each source
/// maintains its own synchronization state and can report whether it's
/// ready to serve requests for a given block number.
///
/// # Implementation Requirements
///
/// Implementors must:
/// 1. Implement the `DatabaseRef` trait to provide data access
/// 2. Implement `is_synced` to report synchronization status
/// 3. Provide a unique name for identification and debugging
/// 4. Be thread-safe (`Send + Sync`)
///
/// # Thread Safety
///
/// All trait methods must be thread-safe as sources may be accessed
/// concurrently from multiple threads.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// #[derive(Debug)]
/// struct MySource {
///     name: &'static str,
///     max_block: AtomicU64,
/// }
///
/// impl Source for MySource {
///     fn is_synced(&self, current_block: u64) -> bool {
///         let max = self.max_block.load(Ordering::Acquire);
///         current_block <= max
///     }
///
///     fn name(&self) -> &'static str {
///         self.name
///     }
///
///     fn update_target_block(&self, _block_number: u64) {
///         // No-op for this example
///     }
/// }
///
/// // Also implement DatabaseRef trait methods...
/// ```
pub trait Source: DatabaseRef<Error = SourceError> + Debug + Sync + Send {
    /// Checks if this source is synchronized up to the specified block number.
    ///
    /// A source is considered synchronized if it can reliably provide data
    /// for the given block number. Sources that are not synchronized should
    /// not be queried as they may return stale or incorrect data.
    ///
    /// # Arguments
    ///
    /// * `current_block_number` - The block number to check synchronization against
    ///
    /// # Returns
    ///
    /// `true` if the source can provide data for the specified block number,
    /// `false` otherwise.
    ///
    /// # Implementation Notes
    ///
    /// Implementations should use atomic operations when accessing the block
    /// number to ensure thread safety. The typical pattern is:
    ///
    /// ```rust,ignore
    /// fn is_synced(&self, current_block: u64) -> bool {
    ///     // Check if this source has data for `current`
    /// }
    /// ```
    fn is_synced(&self, current_block_number: u64) -> bool;

    /// Returns a unique identifier for this source.
    ///
    /// The name is used for debugging and logging purposes. It should be
    /// unique among all sources in a cache to allow for clear identification
    /// in logs and error messages.
    ///
    /// # Returns
    ///
    /// A `SourceName` that uniquely identifies this source.
    fn name(&self) -> SourceName;

    /// Updates the block number that queries should target.
    ///
    /// Implementations may use this hint to issue RPC calls against a specific
    /// block rather than the latest head. The default implementation is a
    /// no-op for sources that do not depend on block context.
    fn update_target_block(&self, block_number: u64);
}

/// Names for a particular source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceName {
    BesuClient,
    Redis,
    Sequencer,
}

// FIXME: Derive `strum`
impl Display for SourceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceName::BesuClient => write!(f, "BesuClient"),
            SourceName::Redis => write!(f, "Redis"),
            SourceName::Sequencer => write!(f, "Sequencer"),
        }
    }
}

#[derive(Error, Debug)]
pub enum SourceError {
    #[error("Block not found")]
    BlockNotFound,
    #[error("Code by hash not found")]
    CodeByHashNotFound,
    #[error("Cache miss")]
    CacheMiss,
    #[error("Request failed")]
    Request(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Failed to fetch code by hash from Redis")]
    RedisCodeByHash(#[source] StateError),
    #[error("Failed to fetch storage from Redis")]
    RedisStorage(#[source] StateError),
    #[error("Failed to fetch account info from Redis")]
    RedisAccount(#[source] StateError),
    #[error("Failed to fetch the block hash from Redis")]
    RedisBlockHash(#[source] StateError),
    #[error("Other error: {0}")]
    Other(String),
    #[error("Storage not found")]
    StorageNotFound,
}

impl DBErrorMarker for SourceError {}
