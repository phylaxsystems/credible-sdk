//! Prometheus metrics for the assertion-executor indexer.

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
    pub fn set_head_block(&self, block_number: u64) {
        let _ = self;
        gauge!("assertion_executor_indexer_head_block").set(lossy_u64_to_f64(block_number));
    }

    /// Record how many assertions the indexer has seen.
    ///
    /// Committed as a `Counter`: `assertion_executor_indexer_assertions_seen_total`
    pub fn record_assertions_seen(&self, count: u64) {
        let _ = self;
        if count > 0 {
            counter!("assertion_executor_indexer_assertions_seen_total").increment(count);
        }
    }

    /// Record how many assertions the indexer has moved into the store.
    ///
    /// Committed as a `Counter`: `assertion_executor_indexer_assertions_moved_total`
    pub fn record_assertions_moved(&self, count: u64) {
        let _ = self;
        if count > 0 {
            counter!("assertion_executor_indexer_assertions_moved_total").increment(count);
        }
    }
}

fn lossy_u64_to_f64(value: u64) -> f64 {
    u64_to_f64_lossy(value)
}

fn u64_to_f64_lossy(value: u64) -> f64 {
    let high = u32::try_from(value >> 32).unwrap_or(0);
    let low = u32::try_from(value & 0xFFFF_FFFF).unwrap_or(0);
    f64::from(high) * 4_294_967_296.0 + f64::from(low)
}
