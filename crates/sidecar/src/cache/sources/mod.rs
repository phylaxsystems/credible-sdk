use assertion_executor::db::DatabaseRef;
use revm::context::DBErrorMarker;
use std::fmt::Debug;
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
/// ```rust
/// #[derive(Debug)]
/// struct MySource {
///     name: &'static str,
///     max_block: AtomicU64,
/// }
///
/// impl Source for MySource {
///     fn is_synced(&self, current_block: &AtomicU64) -> bool {
///         let current = current_block.load(Ordering::Acquire);
///         let max = self.max_block.load(Ordering::Acquire);
///         current <= max
///     }
///
///     fn name(&self) -> &'static str {
///         self.name
///     }
/// }
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
    /// ```rust
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
    /// A static string that uniquely identifies this source.
    fn name(&self) -> &'static str;
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
    #[error("Other error: {0}")]
    Other(String),
    #[cfg(test)]
    #[error("Storage not found")]
    StorageNotFound,
}

impl DBErrorMarker for SourceError {}
