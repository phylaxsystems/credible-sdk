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

use assertion_executor::primitives::FixedBytes;
use metrics::{
    counter,
    gauge,
    histogram,
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
    #[allow(clippy::unused_self)]
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
#[derive(Clone, Debug)]
pub struct TransactionMetrics {
    pub hash: FixedBytes<32>,
    pub block_number: u64,
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
    pub fn new(hash: FixedBytes<32>, block_number: u64) -> Self {
        Self {
            hash,
            block_number,
            assertion_gas_per_transaction: 0,
            assertions_per_transaction: 0,
            transaction_processing_duration: std::time::Duration::default(),
            gas_per_assertion: 0,
        }
    }

    /// Commits the per tx metrics
    pub fn commit(&self) {
        histogram!("sidecar_assertion_gas_per_transaction", "tx_hash" => self.hash.to_string(), "block_number" => self.block_number.to_string())
            .record(self.assertion_gas_per_transaction as f64);
        histogram!("sidecar_assertions_per_transaction", "tx_hash" => self.hash.to_string(), "block_number" => self.block_number.to_string())
            .record(self.assertions_per_transaction as f64);
        histogram!("sidecar_transaction_processing_duration", "tx_hash" => self.hash.to_string(), "block_number" => self.block_number.to_string())
            .record(self.transaction_processing_duration);
        histogram!("sidecar_gas_per_assertion", "tx_hash" => self.hash.to_string(), "block_number" => self.block_number.to_string())
            .record(self.gas_per_assertion as f64);
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}
