use alloy::primitives::U256;
use assertion_executor::db::DatabaseRef;
use revm::context::DBErrorMarker;
use state_store::mdbx::common::error::StateError;
use std::fmt::{
    Debug,
    Display,
};
use thiserror::Error;

pub mod eth_rpc_source;
mod json_rpc_db;
pub mod state_worker;

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
///     fn is_synced(&self, latest_head: u64) -> bool {
///         let max = self.max_block.load(Ordering::Acquire);
///         latest_head <= max
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
    /// Checks if this source is synchronized up to the specified block number knowing the minimum
    /// synced block needed by the cache.
    ///
    /// A source is considered synchronized if it can reliably provide data
    /// for the given block number. Sources that are not synchronized should
    /// not be queried as they may return stale or incorrect data.
    ///
    /// # Arguments
    ///
    /// * `min_synced_block` - The minimum block needed to be synced to
    /// * `latest_head` - The latest head to check synchronization against
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
    /// fn is_synced(&self, min_synced_block: u64, latest_head: u64) -> bool {
    ///     // Check if this source has data for `current`
    /// }
    /// ```
    fn is_synced(&self, min_synced_block: U256, latest_head: U256) -> bool;

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

    /// Updates the current cache status
    ///
    /// Updates the current cache status for the Source by passing the minimum block needed
    /// to be synced to (`min_synced_block`) and the latest head in the cache (`latest_head`)
    ///
    /// The two inputs represent the range of blocks from which the source can fetch the data
    fn update_cache_status(&self, min_synced_block: U256, latest_head: U256);
}

/// Names for a particular source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceName {
    EthRpcSource,
    StateWorker,
    #[cfg(test)]
    Other,
}

// FIXME: Derive `strum`
impl Display for SourceName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceName::EthRpcSource => write!(f, "EthRpcSource"),
            SourceName::StateWorker => write!(f, "StateWorker"),
            #[cfg(test)]
            SourceName::Other => write!(f, "Other"),
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
    #[error("Failed to fetch code by hash from state worker")]
    StateWorkerCodeByHash(#[source] StateError),
    #[error("Failed to fetch storage from state worker")]
    StateWorkerStorage(#[source] StateError),
    #[error("Failed to fetch account info from state worker")]
    StateWorkerAccount(#[source] StateError),
    #[error("Failed to fetch the block hash from state worker")]
    StateWorkerBlockHash(#[source] StateError),
    #[error("Other error: {0}")]
    Other(String),
    #[error("Storage not found")]
    StorageNotFound,
}

impl DBErrorMarker for SourceError {}
