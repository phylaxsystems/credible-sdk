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

use metrics::{
    counter,
    gauge,
    histogram,
};
use std::sync::atomic::{
    AtomicU64,
    Ordering,
};

/// Individual block metrics we commit to the prometheus exporter.
///
/// Will commit metrics when dropped.
///
/// ## Additional metrics
///
/// Metrics `sidecar_cache_invalidations` (counter), `sidecar_cache_min_required_height` (counter),
/// and `sidecar_cache_invalidations_time_seconds` (gauge) are commited in a different way to the
/// metrics below, but are can be accessed the same way in prometheus.
#[derive(Clone, Debug, Default)]
pub struct BlockMetrics {
    /// Duration elapsed from receving one blockenv to a new one.
    /// Does not necessarily equate to how much active time was spent
    /// working on transactions.
    ///
    /// Commited as a `Histogram`.
    pub block_processing_duration: std::time::Duration,
    /// Time spent idling and not building blocks
    ///
    /// Commited as a `Gauge`.
    pub idle_time: std::time::Duration,
    /// Time spent processing events
    ///
    /// Commited as a `Gauge` and `Histogram`.
    pub event_processing_time: std::time::Duration,
    /// How many transactions the engine has seen
    ///
    /// Commited as a `Gauge`.
    pub transactions_considered: u64,
    /// How many txs were executed
    ///
    /// Commited as a `Gauge`.
    pub transactions_simulated: u64,
    /// How many transactions we have executed successfully
    ///
    /// Commited as a `Gauge`.
    pub transactions_simulated_success: u64,
    /// How many transactions we have executed unsuccessfully
    ///
    /// Commited as a `Gauge`.
    pub transactions_simulated_failure: u64,
    /// How many transactions we have executed successfully,
    /// which ended up invalidating assertions
    ///
    /// Commited as a `Gauge`.
    pub invalidated_transactions: u64,
    /// How much gas was used in a block
    ///
    /// Commited as a `Gauge`.
    pub block_gas_used: u64,
    /// How many assertions we have executed in the block
    ///
    /// Commited as a `Gauge`.
    pub assertions_per_block: u64,
    /// How much assertion gas we executed in a block
    ///
    /// Commited as a `Gauge`.
    pub assertion_gas_per_block: u64,
    /// Current block height
    ///
    /// Commited as a `Gauge`.
    pub current_height: u64,
}

impl BlockMetrics {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Increments the `sidecar_cache_invalidations` counter, commit its duration
    /// `sidecar_cache_invalidations_time_seconds` and set the min required height counter.
    pub fn increment_cache_invalidation(&self, duration: std::time::Duration, height: u64) {
        counter!("sidecar_cache_invalidations").increment(1);
        counter!("sidecar_cache_min_required_height").absolute(height);
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
        gauge!("sidecar_current_height").set(self.current_height as f64);
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
        self.current_height = 0;
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
    /// How much gas we have executed per assertion
    pub gas_per_assertion: u64,
}

impl TransactionMetrics {
    pub fn new() -> Self {
        Self {
            assertion_gas_per_transaction: 0,
            assertions_per_transaction: 0,
            transaction_processing_duration: std::time::Duration::default(),
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
        histogram!("sidecar_gas_per_assertion").record(self.gas_per_assertion as f64);
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}

/// Cache metrics. The metrics are committed to prometheus on write. The fields in this struct are
/// for tracking purposes
#[derive(Debug, Default)]
pub struct CacheMetrics {
    pub required_block_number: AtomicU64,
    pub current_block_number: AtomicU64,
    pub reset_required_block_number_counter: AtomicU64,
}

impl CacheMetrics {
    pub fn new() -> Self {
        Self {
            required_block_number: AtomicU64::new(0),
            current_block_number: AtomicU64::new(0),
            reset_required_block_number_counter: AtomicU64::new(0),
        }
    }

    /// Set the cache required block number (`sidecar_cache_required_block_number`)
    ///
    /// Commited as a `Gauge`.
    pub fn set_required_block_number(&self, block_number: u64) {
        self.required_block_number
            .store(block_number, Ordering::Relaxed);
        gauge!("sidecar_cache_required_block_number").set(block_number as f64);
    }

    /// Set the cache current block number (`sidecar_cache_current_block_number`)
    ///
    /// Commited as a `Gauge`.
    pub fn set_current_block_number(&self, block_number: u64) {
        self.current_block_number
            .store(block_number, Ordering::Relaxed);
        gauge!("sidecar_cache_current_block_number").set(block_number as f64);
    }

    /// Track the number of times the `basic_ref` was called
    /// (`sidecar_cache_basic_ref_counter`)
    ///
    /// Commited as a `Counter`.
    pub fn increase_basic_ref_counter(&self) {
        counter!("sidecar_cache_basic_ref_counter").increment(1);
    }

    /// Track the number of times the `code_by_hash_ref` was called
    /// (`sidecar_cache_code_by_hash_ref_counter`)
    ///
    /// Commited as a `Counter`.
    pub fn increase_code_by_hash_ref_counter(&self) {
        counter!("sidecar_cache_code_by_hash_ref_counter").increment(1);
    }

    /// Track the number of times the `block_hash_ref` was called
    /// (`sidecar_cache_block_hash_ref_counter`)
    ///
    /// Commited as a `Counter`.
    pub fn increase_block_hash_ref_counter(&self) {
        counter!("sidecar_cache_block_hash_ref_counter").increment(1);
    }

    /// Track the number of times the `storage_ref` was called
    /// (`sidecar_cache_storage_ref_counter`)
    ///
    /// Commited as a `Counter`.
    pub fn increase_storage_ref_counter_counter(&self) {
        counter!("sidecar_cache_storage_ref_counter").increment(1);
    }

    /// Track the number of times the required block number was reset
    /// (`sidecar_cache_reset_required_block_number_counter`)
    ///
    /// Commited as a `Counter`.
    pub fn increase_reset_required_block_number_counter(&self) {
        let reset_required_block_number_counter = self
            .reset_required_block_number_counter
            .load(Ordering::Relaxed)
            + 1;
        self.reset_required_block_number_counter
            .store(reset_required_block_number_counter, Ordering::Relaxed);
        counter!("sidecar_cache_reset_required_block_number_counter").increment(1);
    }

    /// Track the duration of the `is_sync` call
    /// (`sidecar_cache_is_sync_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn is_sync_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_is_sync_duration").record(duration);
    }

    /// Track the total duration of the `basic_ref` call
    /// (`sidecar_cache_total_basic_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn total_basic_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_basic_ref_duration").record(duration);
    }

    /// Track the duration of the `basic_ref` call per source
    /// (`sidecar_cache_basic_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn basic_ref_duration(&self, source: &str, duration: std::time::Duration) {
        histogram!("sidecar_cache_basic_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `block_hash_ref` call
    /// (`sidecar_cache_total_block_hash_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn total_block_hash_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_block_hash_ref_duration").record(duration);
    }

    /// Track the duration of the `block_hash_ref` call per source
    /// (`sidecar_cache_block_hash_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn block_hash_ref_duration(&self, source: &str, duration: std::time::Duration) {
        histogram!("sidecar_cache_block_hash_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `code_by_hash_ref` call
    /// (`sidecar_cache_total_code_by_hash_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn total_code_by_hash_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_code_by_hash_ref_duration").record(duration);
    }

    /// Track the duration of the `code_by_hash_ref` call per source
    /// (`sidecar_cache_code_by_hash_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn code_by_hash_ref_duration(&self, source: &str, duration: std::time::Duration) {
        histogram!("sidecar_cache_code_by_hash_ref_duration", "source" => source.to_string())
            .record(duration);
    }

    /// Track the total duration of the `storage_ref` call
    /// (`sidecar_cache_total_storage_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn total_storage_ref_duration(&self, duration: std::time::Duration) {
        histogram!("sidecar_cache_total_storage_ref_duration").record(duration);
    }

    /// Track the duration of the `storage_ref` call per source
    /// (`sidecar_cache_storage_ref_duration`)
    ///
    /// Commited as a `Histogram`.
    pub fn storage_ref_duration(&self, source: &str, duration: std::time::Duration) {
        histogram!("sidecar_cache_storage_ref_duration", "source" => source.to_string())
            .record(duration);
    }
}
