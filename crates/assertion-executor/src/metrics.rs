//! Prometheus metrics for the assertion-executor indexer.

#![allow(clippy::cast_precision_loss)]

use metrics::{
    counter,
    gauge,
};

/// Metrics for the assertion indexer.
#[derive(Debug, Default)]
pub struct IndexerMetrics;

impl IndexerMetrics {
    pub fn new() -> Self {
        Self {}
    }

    /// Record the latest head block the indexer is aware of.
    ///
    /// Committed as a `Gauge`: `assertion_executor_indexer_head_block`
    #[allow(clippy::unused_self)]
    pub fn set_head_block(&self, block_number: u64) {
        gauge!("assertion_executor_indexer_head_block").set(block_number as f64);
    }

    /// Record how many assertions the indexer has seen.
    ///
    /// Committed as a `Counter`: `assertion_executor_indexer_assertions_seen_total`
    #[allow(clippy::unused_self)]
    pub fn record_assertions_seen(&self, count: u64) {
        if count > 0 {
            counter!("assertion_executor_indexer_assertions_seen_total").increment(count);
        }
    }

    /// Record how many assertions the indexer has moved into the store.
    ///
    /// Committed as a `Counter`: `assertion_executor_indexer_assertions_moved_total`
    #[allow(clippy::unused_self)]
    pub fn record_assertions_moved(&self, count: u64) {
        if count > 0 {
            counter!("assertion_executor_indexer_assertions_moved_total").increment(count);
        }
    }
}
