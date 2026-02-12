//! Prometheus metrics for the assertion-executor indexer.

use crate::utils::cast::lossy_u64_to_f64;
use metrics::{
    counter,
    gauge,
};

/// Record the latest head block the indexer is aware of.
///
/// Committed as a `Gauge`: `assertion_executor_indexer_head_block`
pub fn set_head_block(block_number: u64) {
    gauge!("assertion_executor_indexer_head_block").set(lossy_u64_to_f64(block_number));
}

/// Record how many assertions the indexer has seen.
///
/// Committed as a `Counter`: `assertion_executor_indexer_assertions_seen_total`
pub fn record_assertions_seen(count: u64) {
    if count > 0 {
        counter!("assertion_executor_indexer_assertions_seen_total").increment(count);
    }
}

/// Record how many assertions the indexer has moved into the store.
///
/// Committed as a `Counter`: `assertion_executor_indexer_assertions_moved_total`
pub fn record_assertions_moved(count: u64) {
    if count > 0 {
        counter!("assertion_executor_indexer_assertions_moved_total").increment(count);
    }
}
