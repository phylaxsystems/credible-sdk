//! Prometheus metrics for the state-worker service.
//!
//! Tracks statistics from `commit_block` operations including account/storage
//! changes, bytecode writes, and timing information.

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
///
/// ### Health / sync gauges
/// - `state_worker_db_healthy`: Whether DB access is healthy (`1` healthy, `0` unhealthy)
/// - `state_worker_head_block`: Latest chain head observed by the worker
/// - `state_worker_sync_lag_blocks`: Number of blocks the worker is behind head
/// - `state_worker_is_syncing`: Whether the worker is catching up to head
/// - `state_worker_is_following_head`: Whether the worker is tailing the head in steady-state
pub fn record_commit(block_number: u64, stats: &CommitStats) {
    // Gauges for current block stats
    gauge!("state_worker_accounts_written").set(usize_to_f64(stats.accounts_written));
    gauge!("state_worker_accounts_deleted").set(usize_to_f64(stats.accounts_deleted));
    gauge!("state_worker_storage_slots_written").set(usize_to_f64(stats.storage_slots_written));
    gauge!("state_worker_storage_slots_deleted").set(usize_to_f64(stats.storage_slots_deleted));
    gauge!("state_worker_full_storage_deletes").set(usize_to_f64(stats.full_storage_deletes));
    gauge!("state_worker_bytecodes_written").set(usize_to_f64(stats.bytecodes_written));
    gauge!("state_worker_diffs_applied").set(usize_to_f64(stats.diffs_applied));
    gauge!("state_worker_diff_bytes").set(usize_to_f64(stats.diff_bytes));
    gauge!("state_worker_largest_account_storage").set(usize_to_f64(stats.largest_account_storage));
    gauge!("state_worker_current_block").set(u64_to_f64(block_number));

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

/// Record whether DB access is currently healthy.
///
/// Committed as a `Gauge`: `state_worker_db_healthy`
pub fn set_db_healthy(is_healthy: bool) {
    gauge!("state_worker_db_healthy").set(bool_to_f64(is_healthy));
}

/// Record the latest head block observed from the execution client.
///
/// Committed as a `Gauge`: `state_worker_head_block`
pub fn set_head_block(block_number: u64) {
    gauge!("state_worker_head_block").set(u64_to_f64(block_number));
}

/// Record how far the worker is behind the observed chain head.
///
/// Committed as a `Gauge`: `state_worker_sync_lag_blocks`
pub fn set_sync_lag_blocks(lag_blocks: u64) {
    gauge!("state_worker_sync_lag_blocks").set(u64_to_f64(lag_blocks));
}

/// Record whether the worker is currently catching up to chain head.
///
/// Committed as a `Gauge`: `state_worker_is_syncing`
pub fn set_syncing(is_syncing: bool) {
    gauge!("state_worker_is_syncing").set(bool_to_f64(is_syncing));
}

/// Record whether the worker is currently following the chain head in
/// steady-state streaming mode.
///
/// Committed as a `Gauge`: `state_worker_is_following_head`
pub fn set_following_head(is_following_head: bool) {
    gauge!("state_worker_is_following_head").set(bool_to_f64(is_following_head));
}

fn usize_to_f64(value: usize) -> f64 {
    let capped = u32::try_from(value).unwrap_or(u32::MAX);
    f64::from(capped)
}

fn u64_to_f64(value: u64) -> f64 {
    let capped = u32::try_from(value).unwrap_or(u32::MAX);
    f64::from(capped)
}

fn bool_to_f64(value: bool) -> f64 {
    if value { 1.0 } else { 0.0 }
}
