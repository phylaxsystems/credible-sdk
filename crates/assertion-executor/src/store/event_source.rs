use crate::primitives::{
    Address,
    B256,
};
use async_trait::async_trait;
use serde::{
    Deserialize,
    Serialize,
};

/// Trait abstracting how assertion events are fetched.
///
/// Implementations may query a GraphQL API, a direct database, a mock,
/// or any other data source.
///
/// Both methods accept a `since_block` parameter: they return all
/// events with `block > since_block`, ordered by `block` ascending.
#[async_trait]
pub trait EventSource: Send + Sync {
    /// Verify that the event source is reachable and healthy.
    ///
    /// The default implementation calls [`get_indexer_head`](Self::get_indexer_head)
    /// and succeeds if the call completes without error.
    async fn health_check(&self) -> Result<(), EventSourceError> {
        self.get_indexer_head().await?;
        Ok(())
    }

    /// Fetch all `AssertionAdded` events with block number strictly greater
    /// than `since_block`.
    async fn fetch_added_events(
        &self,
        since_block: u64,
    ) -> Result<Vec<AssertionAddedEvent>, EventSourceError>;

    /// Fetch all `AssertionRemoved` events with block number strictly greater
    /// than `since_block`.
    async fn fetch_removed_events(
        &self,
        since_block: u64,
    ) -> Result<Vec<AssertionRemovedEvent>, EventSourceError>;

    /// Returns the latest block number the event source has indexed up to.
    /// Used to know when the sidecar is "caught up" with the external indexer.
    async fn get_indexer_head(&self) -> Result<Option<u64>, EventSourceError>;
}

/// A raw `AssertionAdded` event as returned by the event source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionAddedEvent {
    /// Block number in which the event was emitted
    pub block: u64,
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
    #[error("Event source unavailable: {0}")]
    Unavailable(String),
}
