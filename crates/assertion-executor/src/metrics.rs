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

/// Record whether the indexer is currently catching up to chain head.
///
/// This is `1` when replaying a backlog (for example after downtime), and `0`
/// during normal steady-state operation.
///
/// Committed as a `Gauge`: `assertion_executor_indexer_is_syncing`
pub fn set_syncing(is_syncing: bool) {
    gauge!("assertion_executor_indexer_is_syncing").set(if is_syncing { 1.0 } else { 0.0 });
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

/// Record that the external event source head moved backward.
///
/// This indicates the upstream indexer is handling a reorg. The syncer
/// skips the cycle and waits for the event source to stabilise.
///
/// Committed as a `Counter`: `assertion_executor_event_source_head_regression_total`
pub fn record_event_source_head_regression() {
    counter!("assertion_executor_event_source_head_regression_total").increment(1);
}
