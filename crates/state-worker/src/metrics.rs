//! Prometheus metrics for the state-worker service.
//!
//! Tracks statistics from `commit_block` operations including account/storage
//! changes, bytecode writes, and timing information.

#![allow(clippy::cast_precision_loss)]

use mdbx::CommitStats;
use metrics::{
    counter,
    gauge,
    histogram,
};

/// Record metrics from a block commit operation.
///
/// ## Metrics recorded
///
/// ### Gauges (latest values per block)
/// - `state_worker_accounts_written`: Number of accounts written
/// - `state_worker_accounts_deleted`: Number of accounts deleted
/// - `state_worker_storage_slots_written`: Number of storage slots written
/// - `state_worker_storage_slots_deleted`: Number of storage slots deleted
/// - `state_worker_full_storage_deletes`: Number of full account storage wipes
/// - `state_worker_bytecodes_written`: Number of bytecodes written
/// - `state_worker_diffs_applied`: Number of intermediate diffs applied during rotation
/// - `state_worker_diff_bytes`: Size of the serialized diff in bytes
/// - `state_worker_largest_account_storage`: Largest storage count for any single account
/// - `state_worker_current_block`: Current block number
///
/// ### Histograms (timing distributions)
/// - `state_worker_preprocess_duration_seconds`: Time spent in parallel preprocessing
/// - `state_worker_diff_application_duration_seconds`: Time spent applying intermediate diffs
/// - `state_worker_batch_write_duration_seconds`: Time spent executing batch writes
/// - `state_worker_commit_duration_seconds`: Time spent in `tx.commit()` call
/// - `state_worker_total_duration_seconds`: Total wall-clock time for the commit operation
///
/// ### Counters (cumulative)
/// - `state_worker_blocks_committed_total`: Total number of blocks committed
/// - `state_worker_total_accounts_written`: Cumulative accounts written
/// - `state_worker_total_storage_slots_written`: Cumulative storage slots written
pub fn record_commit(block_number: u64, stats: &CommitStats) {
    // Gauges for current block stats
    gauge!("state_worker_accounts_written").set(stats.accounts_written as f64);
    gauge!("state_worker_accounts_deleted").set(stats.accounts_deleted as f64);
    gauge!("state_worker_storage_slots_written").set(stats.storage_slots_written as f64);
    gauge!("state_worker_storage_slots_deleted").set(stats.storage_slots_deleted as f64);
    gauge!("state_worker_full_storage_deletes").set(stats.full_storage_deletes as f64);
    gauge!("state_worker_bytecodes_written").set(stats.bytecodes_written as f64);
    gauge!("state_worker_diffs_applied").set(stats.diffs_applied as f64);
    gauge!("state_worker_diff_bytes").set(stats.diff_bytes as f64);
    gauge!("state_worker_largest_account_storage").set(stats.largest_account_storage as f64);
    gauge!("state_worker_current_block").set(block_number as f64);

    // Histograms for timing distributions
    histogram!("state_worker_preprocess_duration_seconds").record(stats.preprocess_duration);
    histogram!("state_worker_diff_application_duration_seconds")
        .record(stats.diff_application_duration);
    histogram!("state_worker_batch_write_duration_seconds").record(stats.batch_write_duration);
    histogram!("state_worker_commit_duration_seconds").record(stats.commit_duration);
    histogram!("state_worker_total_duration_seconds").record(stats.total_duration);

    // Counters for cumulative totals
    counter!("state_worker_blocks_committed_total").increment(1);
    counter!("state_worker_total_accounts_written").increment(stats.accounts_written as u64);
    counter!("state_worker_total_storage_slots_written")
        .increment(stats.storage_slots_written as u64);
}

/// Record a failed block processing attempt.
///
/// Committed as a `Counter`: `state_worker_block_failures_total`
pub fn record_block_failure() {
    counter!("state_worker_block_failures_total").increment(1);
}
