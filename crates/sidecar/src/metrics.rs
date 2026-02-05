//! Prometheus `metrics`
//!
//! Contains data types for containing metrics before sending them to the
//! global prometheus server spawned in `main.rs`
//!
//! The prometheus exporter lives by default at `0.0.0.0:9000`.
//! The port on which to bind it to can be specified with the
//! `TRACING_METRICS_PORT` env variable.

// For converting to f64 from u64. Its metrics so not important to get full precision
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::unused_self)]

use alloy::primitives::U256;
use metrics::{
    counter,
    gauge,
    histogram,
};
use parking_lot::RwLock;
use std::sync::atomic::{
    AtomicBool,
    AtomicU64,
    AtomicUsize,
    Ordering,
};

/// Individual block metrics we commit to the prometheus exporter.
///
/// Will commit metrics when dropped.
///
/// ## Additional metrics
///
/// Metrics `sidecar_cache_invalidations` (counter), `sidecar_cache_min_required_height` (counter),
/// and `sidecar_cache_invalidations_time_seconds` (gauge) are committed in a different way to the
/// metrics below, but can be accessed the same way in prometheus.
#[derive(Clone, Debug, Default)]
pub struct BlockMetrics {
    /// Duration elapsed from receiving one blockenv to a new one.
    /// Does not necessarily equate to how much active time was spent
    /// working on transactions.
    ///
    /// Committed as a `Histogram`.
    pub block_processing_duration: std::time::Duration,
    /// Time spent idling and not building blocks
    ///
    /// Committed as a `Gauge`.
    pub idle_time: std::time::Duration,
    /// Time spent processing events
    ///
    /// Committed as a `Gauge` and `Histogram`.
    pub event_processing_time: std::time::Duration,
    /// How many transactions the engine has seen
    ///
    /// Committed as a `Gauge`.
    pub transactions_considered: u64,
    /// How many txs were executed
    ///
    /// Committed as a `Gauge`.
    pub transactions_simulated: u64,
    /// How many transactions we have executed successfully
    ///
    /// Committed as a `Gauge`.
    pub transactions_simulated_success: u64,
    /// How many transactions we have executed unsuccessfully
    ///
    /// Committed as a `Gauge`.
    pub transactions_simulated_failure: u64,
    /// How many transactions we have executed successfully,
    /// which ended up invalidating assertions
    ///
    /// Committed as a `Gauge`.
    pub invalidated_transactions: u64,
    /// How much gas was used in a block
    ///
    /// Committed as a `Gauge`.
    pub block_gas_used: u64,
    /// How many assertions we have executed in the block
    ///
    /// Committed as a `Gauge`.
    pub assertions_per_block: u64,
    /// How much assertion gas we executed in a block
    ///
    /// Committed as a `Gauge`.
    pub assertion_gas_per_block: u64,
    /// Current block height
    ///
    /// Committed as a `Gauge`.
    pub current_height: U256,
}

impl BlockMetrics {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Increments the `sidecar_cache_invalidations` counter, commit its duration
    /// `sidecar_cache_invalidations_time_seconds` and set the min required height counter.
    pub fn increment_cache_invalidation(&self, duration: std::time::Duration, height: U256) {
        counter!("sidecar_cache_invalidations").increment(1);
        let height_u64: u64 = height.try_into().unwrap_or(u64::MAX);
        counter!("sidecar_cache_min_required_height").absolute(height_u64);
        gauge!("sidecar_cache_invalidations_time_seconds").set(duration);
    }

    /// Commits the metrics
    pub fn commit(&self) {
        histogram!("sidecar_block_processing_duration_seconds")
            .record(self.block_processing_duration);
        histogram!("sidecar_idle_time_distribution").record(self.idle_time);
        gauge!("sidecar_idle_time_seconds").set(self.idle_time.as_secs_f64());
        histogram!("sidecar_event_processing_time_distribution").record(self.event_processing_time);
        gauge!("sidecar_event_processing_time_seconds")
            .set(self.event_processing_time.as_secs_f64());
        gauge!("sidecar_transactions_considered").set(self.transactions_considered as f64);
        gauge!("sidecar_transactions_simulated").set(self.transactions_simulated as f64);
        gauge!("sidecar_transactions_simulated_success")
            .set(self.transactions_simulated_success as f64);
        gauge!("sidecar_transactions_simulated_failure")
            .set(self.transactions_simulated_failure as f64);
        gauge!("sidecar_invalidated_transactions").set(self.invalidated_transactions as f64);
        gauge!("sidecar_block_gas_used").set(self.block_gas_used as f64);
        gauge!("sidecar_assertions_per_block").set(self.assertions_per_block as f64);
        gauge!("sidecar_assertion_gas_per_block").set(self.assertion_gas_per_block as f64);
        gauge!("sidecar_current_height").set(f64::from(self.current_height));
    }

    /// Resets all values inside of `&mut Self` back to their defaults
    pub fn reset(&mut self) {
        self.block_processing_duration = std::time::Duration::default();
        self.idle_time = std::time::Duration::default();
        self.event_processing_time = std::time::Duration::default();
        self.transactions_considered = 0;
        self.transactions_simulated = 0;
        self.transactions_simulated_success = 0;
        self.transactions_simulated_failure = 0;
        self.invalidated_transactions = 0;
        self.block_gas_used = 0;
        self.assertions_per_block = 0;
        self.assertion_gas_per_block = 0;
        self.current_height = U256::ZERO;
    }
}

impl Drop for BlockMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}

/// Individual metrics that we commit on a per transaction basis.
///
/// Will commit metrics when dropped.
#[derive(Clone, Debug, Default)]
pub struct TransactionMetrics {
    /// How much assertion gas a transaction spent
    pub assertion_gas_per_transaction: u64,
    /// How many assertions we have executed per transaction
    pub assertions_per_transaction: u64,
    /// Duration we spent processing a transaction in microseconds
    pub transaction_processing_duration: std::time::Duration,
    /// Duration spent executing assertions for a transaction
    pub assertion_execution_duration: std::time::Duration,
    /// How much gas we have executed per assertion
    pub gas_per_assertion: u64,
}

impl TransactionMetrics {
    pub fn new() -> Self {
        Self {
            assertion_gas_per_transaction: 0,
            assertions_per_transaction: 0,
            transaction_processing_duration: std::time::Duration::default(),
            assertion_execution_duration: std::time::Duration::default(),
            gas_per_assertion: 0,
        }
    }

    /// Commits the per tx metrics
    pub fn commit(&self) {
        histogram!("sidecar_assertion_gas_per_transaction")
            .record(self.assertion_gas_per_transaction as f64);
        histogram!("sidecar_assertions_per_transaction")
            .record(self.assertions_per_transaction as f64);
        histogram!("sidecar_transaction_processing_duration")
            .record(self.transaction_processing_duration);
        histogram!("sidecar_assertion_execution_duration")
            .record(self.assertion_execution_duration);
        histogram!("sidecar_gas_per_assertion").record(self.gas_per_assertion as f64);
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}

/// State metrics. The metrics are committed to prometheus on write. The fields in this struct are
/// for tracking purposes.
#[derive(Debug)]
pub struct StateMetrics {
    /// Latest unprocessed block number
    pub latest_unprocessed_block: RwLock<U256>,
    /// Latest head block number
    pub latest_head: RwLock<U256>,
    /// Counter for reset operations
    pub reset_latest_unprocessed_block: AtomicU64,
}

impl Default for StateMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl StateMetrics {
    pub fn new() -> Self {
        Self {
            latest_unprocessed_block: RwLock::new(U256::ZERO),
            latest_head: RwLock::new(U256::ZERO),
            reset_latest_unprocessed_block: AtomicU64::new(0),
        }
    }

    /// Set the state sources latest unprocessed block number (`sidecar_cache_latest_unprocessed_block`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_latest_unprocessed_block(&self, block_number: U256) {
        *self.latest_unprocessed_block.write() = block_number;
        // For metrics, we convert to f64. Large U256 values will lose precision,
        // but this is acceptable for metrics purposes.
        gauge!("sidecar_cache_latest_unprocessed_block").set(u256_to_f64(block_number));
    }

    /// Set the state sources min synced head (`sidecar_cache_min_synced_head`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_min_synced_head(&self, number: U256) {
        gauge!("sidecar_cache_min_synced_head").set(u256_to_f64(number));
    }

    /// Set the state sources latest head (`sidecar_cache_latest_head`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_latest_head(&self, block_number: U256) {
        *self.latest_head.write() = block_number;
        gauge!("sidecar_cache_latest_head").set(u256_to_f64(block_number));
    }

    /// Track the number of times the `basic_ref` was called successfully
    /// (`sidecar_cache_basic_ref_success_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_basic_ref_success(&self, source: &impl ToString) {
        counter!("sidecar_cache_basic_ref_success_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track the number of times the `code_by_hash_ref` was called successfully
    /// (`sidecar_cache_code_by_hash_ref_success_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_code_by_hash_ref_success(&self, source: &impl ToString) {
        counter!("sidecar_cache_code_by_hash_ref_success_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track the number of times the `block_hash_ref` was called successfully
    /// (`sidecar_cache_block_hash_ref_success_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_block_hash_ref_success(&self, source: &impl ToString) {
        counter!("sidecar_cache_block_hash_ref_success_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track the number of times the `storage_ref` was called successfully
    /// (`sidecar_cache_storage_ref_success_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_storage_ref_success(&self, source: &impl ToString) {
        counter!("sidecar_cache_storage_ref_success_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track `basic_ref` failures per source
    /// (`sidecar_cache_basic_ref_failure_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_basic_ref_failure(&self, source: &impl ToString) {
        counter!("sidecar_cache_basic_ref_failure_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track `storage_ref` failures per source
    /// (`sidecar_cache_storage_ref_failure_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_storage_ref_failure(&self, source: &impl ToString) {
        counter!("sidecar_cache_storage_ref_failure_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track `block_hash_ref` failures per source
    /// (`sidecar_cache_block_hash_ref_failure_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_block_hash_ref_failure(&self, source: &impl ToString) {
        counter!("sidecar_cache_block_hash_ref_failure_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track `code_by_hash_ref` failures per source
    /// (`sidecar_cache_code_by_hash_ref_failure_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_code_by_hash_ref_failure(&self, source: &impl ToString) {
        counter!("sidecar_cache_code_by_hash_ref_failure_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track which source ultimately served the `basic_ref` request
    /// (`sidecar_cache_basic_ref_serving_source_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn record_basic_ref_serving_source(&self, source: &impl ToString) {
        counter!("sidecar_cache_basic_ref_serving_source_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track which source ultimately served the `storage_ref` request
    /// (`sidecar_cache_storage_ref_serving_source_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn record_storage_ref_serving_source(&self, source: &impl ToString) {
        counter!("sidecar_cache_storage_ref_serving_source_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track which source ultimately served the `block_hash_ref` request
    /// (`sidecar_cache_block_hash_ref_serving_source_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn record_block_hash_ref_serving_source(&self, source: &impl ToString) {
        counter!("sidecar_cache_block_hash_ref_serving_source_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track which source ultimately served the `code_by_hash_ref` request
    /// (`sidecar_cache_code_by_hash_ref_serving_source_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn record_code_by_hash_ref_serving_source(&self, source: &impl ToString) {
        counter!("sidecar_cache_code_by_hash_ref_serving_source_counter", "source" => source.to_string())
            .increment(1);
    }

    /// Track the number of times the required last unprocessed block was reset
    /// (`sidecar_cache_reset_latest_unprocessed_block_counter`)
    ///
    /// Committed as a `Counter`.
    pub fn increase_reset_latest_unprocessed_block(&self) {
        self.reset_latest_unprocessed_block
            .fetch_add(1, Ordering::Relaxed);
        counter!("sidecar_cache_reset_latest_unprocessed_block_counter").increment(1);
    }

    /// Track the duration of the `is_sync` call
    /// (`sidecar_cache_is_sync_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn is_sync_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_is_sync_duration").record(duration);
    }

    /// Track the total duration of the `basic_ref` call
    /// (`sidecar_cache_total_basic_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn total_basic_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_basic_ref_duration").record(duration);
    }

    /// Track the duration of the `basic_ref` call per source
    /// (`sidecar_cache_basic_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn basic_ref_duration(&self, source: &impl ToString, duration: std::time::Duration) {
        histogram!("sidecar_cache_basic_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `block_hash_ref` call
    /// (`sidecar_cache_total_block_hash_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn total_block_hash_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_block_hash_ref_duration").record(duration);
    }

    /// Track the duration of the `block_hash_ref` call per source
    /// (`sidecar_cache_block_hash_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn block_hash_ref_duration(&self, source: &impl ToString, duration: std::time::Duration) {
        histogram!("sidecar_cache_block_hash_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `code_by_hash_ref` call
    /// (`sidecar_cache_total_code_by_hash_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn total_code_by_hash_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_code_by_hash_ref_duration").record(duration);
    }

    /// Track the duration of the `code_by_hash_ref` call per source
    /// (`sidecar_cache_code_by_hash_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn code_by_hash_ref_duration(&self, source: &impl ToString, duration: std::time::Duration) {
        histogram!("sidecar_cache_code_by_hash_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `storage_ref` call
    /// (`sidecar_cache_total_storage_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn total_storage_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_storage_ref_duration").record(duration);
    }

    /// Track the duration of the `storage_ref` call per source
    /// (`sidecar_cache_storage_ref_duration`)
    ///
    /// Committed as a `Histogram`.
    pub fn storage_ref_duration(&self, source: &impl ToString, duration: std::time::Duration) {
        histogram!("sidecar_cache_storage_ref_duration", "source" => source.to_string())
            .record(duration);
    }
}

/// Convert U256 to f64 for metrics.
///
/// Note: This will lose precision for very large values (> 2^53),
/// but this is acceptable for metrics purposes since block numbers
/// in practice fit well within f64's precise range.
#[inline]
fn u256_to_f64(value: U256) -> f64 {
    // For values that fit in u64, use direct conversion for precision
    if value <= U256::from(u64::MAX) {
        let limbs = value.as_limbs();
        limbs[0] as f64
    } else {
        // For larger values, convert via string parsing (slower but handles full range)
        // In practice, block numbers should never reach this path
        value.to_string().parse::<f64>().unwrap_or(f64::MAX)
    }
}

/// Metrics for an individual source
///
/// Tracks per-source synchronization status and statistics including
/// sync state, check timestamps, and transition counts.
#[derive(Debug, Default)]
pub struct SourceMetrics {
    /// Whether this source is currently synced
    pub is_synced: AtomicBool,
    /// Last time this source was checked (seconds since monitoring started)
    pub last_checked: AtomicU64,
    /// Number of times this source has transitioned to synced state
    pub sync_count: AtomicUsize,
    /// Number of times this source has transitioned to unsynced state
    pub unsync_count: AtomicUsize,
}

impl SourceMetrics {
    /// Updates the synchronization status
    ///
    /// Tracks state transitions internally without committing to Prometheus.
    /// Call `commit()` to export metrics.
    ///
    /// # Arguments
    ///
    /// * `is_synced` - Whether the source is currently synced
    /// * `timestamp` - Current timestamp in seconds
    pub fn update_sync_status(&self, is_synced: bool, timestamp: u64) {
        let was_synced = self.is_synced.swap(is_synced, Ordering::SeqCst);
        self.last_checked.store(timestamp, Ordering::Relaxed);

        // Track transitions
        if is_synced && !was_synced {
            self.sync_count.fetch_add(1, Ordering::Relaxed);
        } else if !is_synced && was_synced {
            self.unsync_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Commits the per-source metrics to Prometheus
    ///
    /// Exports the following metrics with the source label:
    /// - `sidecar_source_is_synced`: Current sync status (0 or 1)
    /// - `sidecar_source_last_checked`: Timestamp of last check
    /// - `sidecar_source_total_syncs`: Total number of sync transitions
    /// - `sidecar_source_total_unsyncs`: Total number of unsync transitions
    ///
    /// # Arguments
    ///
    /// * `source_name` - The name of the source (used as a label in metrics)
    pub fn commit(&self, source_name: &impl ToString) {
        let is_synced = self.is_synced.load(Ordering::Acquire);
        let last_checked = self.last_checked.load(Ordering::Relaxed);
        let sync_count = self.sync_count.load(Ordering::Relaxed);
        let unsync_count = self.unsync_count.load(Ordering::Relaxed);

        gauge!("sidecar_source_is_synced", "source" => source_name.to_string()).set(if is_synced {
            1.0
        } else {
            0.0
        });
        gauge!("sidecar_source_last_checked", "source" => source_name.to_string())
            .set(last_checked as f64);
        gauge!("sidecar_source_total_syncs", "source" => source_name.to_string())
            .set(sync_count as f64);
        gauge!("sidecar_source_total_unsyncs", "source" => source_name.to_string())
            .set(unsync_count as f64);
    }
}

/// Metrics for the engine transactions result
#[derive(Debug, Default)]
pub struct EngineTransactionsResultMetrics {}

impl EngineTransactionsResultMetrics {
    /// Set the current engine `transactions` length (`sidecar_engine_transactions_results_transactions_length`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_engine_transaction_length(&self, len: usize) {
        gauge!("sidecar_engine_transactions_results_transactions_length").set(len as f64);
    }

    /// Set the current engine `accepted_txs` length (`sidecar_engine_transactions_state_accepted_txs_length`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_engine_transactions_state_accepted_txs_length(&self, len: usize) {
        gauge!("sidecar_engine_transactions_state_accepted_txs_length").set(len as f64);
    }

    /// Set the current engine `transaction_results_pending_requests` length (`sidecar_engine_transactions_state_transaction_results_pending_requests_length`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_engine_transactions_state_transaction_results_pending_requests_length(
        &self,
        len: usize,
    ) {
        gauge!("sidecar_engine_transactions_state_transaction_results_pending_requests_length")
            .set(len as f64);
    }

    /// Set the current engine `transaction_results` length (`sidecar_engine_transactions_state_transaction_results_length`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_engine_transactions_state_transaction_results_length(&self, len: usize) {
        gauge!("sidecar_engine_transactions_state_transaction_results_length").set(len as f64);
    }
}

/// Metrics for the transport transactions result
#[derive(Clone, Debug, Default)]
pub struct TransportTransactionsResultMetrics {}

impl TransportTransactionsResultMetrics {
    /// Set the current transport `transactions` length (`sidecar_transport_transactions_result_pending_receives_length`)
    ///
    /// Committed as a `Gauge`.
    pub fn set_transport_pending_receives_length(&self, len: usize) {
        gauge!("sidecar_transport_transactions_result_pending_receives_length").set(len as f64);
    }
}
