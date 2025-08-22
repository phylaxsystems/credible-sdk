//! # `indexer`
//!
//! Contains setup functions and types to initialize the assertion-executor indexer.

use assertion_executor::store::{
    Indexer,
    IndexerCfg,
    IndexerError,
};

/// Runs the indexer to monitor blockchain events from the Credible Layer state oracle contract
///
/// This function initializes and runs the assertion indexer, which continuously monitors
/// the configured blockchain for events emitted by the state oracle contract. When assertion
/// events are detected, it retrieves the corresponding assertion data from the DA layer
/// and updates the local assertion store to keep the sidecar's validation logic current.
///
/// The indexer syncs to the current blockchain head before entering continuous monitoring mode.
pub async fn run_indexer(indexer_cfg: IndexerCfg) -> Result<(), IndexerError> {
    // First, we create an indexer and sync it to the head.
    let indexer = Indexer::new_synced(indexer_cfg).await?;
    // We can then run the indexer and update the assertion store as new
    // assertions come in
    indexer.run().await
}
