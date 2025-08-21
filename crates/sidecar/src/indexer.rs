//! # `indexer`
//!
//! Contains setup functions and types to initialize the assertion-executor indexer.

use assertion_executor::store::{
    Indexer, IndexerCfg, IndexerError
};

/// Runs the indexer
pub async fn run_indexer(indexer_cfg: IndexerCfg) -> Result<(), IndexerError> {
    // First, we create an indexer and sync it to the head.
    let indexer = Indexer::new_synced(indexer_cfg).await?;
    // We can then run the indexer and update the assertion store as new
    // assertions come in
    indexer.run().await
}
