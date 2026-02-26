use crate::primitives::{
    Address,
    B256,
};
use serde::{
    Deserialize,
    Serialize,
};
use std::future::Future;

/// Trait abstracting how assertion events are fetched.
///
/// Implementations may query a GraphQL API, a direct database, a mock,
/// or any other data source.
///
/// Both methods accept a `since_block` parameter: they return all
/// events with `block > since_block`, ordered by `block` ascending.
pub trait EventSource: Send + Sync {
    /// Verify that the event source is reachable and healthy.
    ///
    /// The default implementation calls [`get_indexer_head`](Self::get_indexer_head)
    /// and succeeds if the call completes without error.
    fn health_check(&self) -> impl Future<Output = Result<(), EventSourceError>> + Send {
        async { self.get_indexer_head().await.map(|_| ()) }
    }

    /// Fetch all `AssertionAdded` events with block number strictly greater
    /// than `since_block`.
    fn fetch_added_events(
        &self,
        since_block: u64,
    ) -> impl Future<Output = Result<Vec<AssertionAddedEvent>, EventSourceError>> + Send;

    /// Fetch all `AssertionRemoved` events with block number strictly greater
    /// than `since_block`.
    fn fetch_removed_events(
        &self,
        since_block: u64,
    ) -> impl Future<Output = Result<Vec<AssertionRemovedEvent>, EventSourceError>> + Send;

    /// Returns the latest block number the event source has indexed up to.
    /// Used to know when the sidecar is "caught up" with the external indexer.
    fn get_indexer_head(
        &self,
    ) -> impl Future<Output = Result<Option<u64>, EventSourceError>> + Send;
}

/// A raw `AssertionAdded` event as returned by the event source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionAddedEvent {
    /// Block number in which the event was emitted
    pub block: u64,
    /// Log index within the block
    pub log_index: u64,
    /// The assertion adopter address
    pub assertion_adopter: Address,
    /// The unique assertion id (keccak of deployment bytecode)
    pub assertion_id: B256,
    /// The block number when this assertion becomes active
    pub activation_block: u64,
}

/// A raw `AssertionRemoved` event as returned by the event source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionRemovedEvent {
    /// Block number in which the event was emitted
    pub block: u64,
    /// Log index within the block
    pub log_index: u64,
    /// The assertion adopter address
    pub assertion_adopter: Address,
    /// The unique assertion id
    pub assertion_id: B256,
    /// The block number when this assertion becomes inactive
    pub deactivation_block: u64,
}

/// Error type for event source operations.
#[derive(Debug, thiserror::Error)]
pub enum EventSourceError {
    #[error("Request failed: {0}")]
    RequestFailed(String),
    #[error("Response parse error: {0}")]
    ParseError(String),
}
