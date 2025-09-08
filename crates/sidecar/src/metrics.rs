use assertion_executor::primitives::FixedBytes;
use metrics::{
    counter,
    gauge,
    histogram,
};

/// Individual block metrics we commit to the prometheus exporter.
///
/// Will commit metrics when dropped.
#[derive(Clone, Debug, Default)]
pub struct BlockMetrics {
    /// Entire duration from blockenv to blockenv
    pub block_processing_duration: std::time::Duration,
    /// How many transactions the engine has seen
    pub transactions_considered_total: u64, //
    /// How many txs were executed
    pub transactions_simulated_total: u64, //
    /// How many transactions we have executed successfully
    pub transactions_simulated_success_total: u64, //
    /// How many transactions we have executed unsuccessfully
    pub transactions_simulated_failure_total: u64, //
    /// How many transactions we have executed successfully,
    /// which ended up invalidating assertions
    pub invalidated_transactions_total: u64, //
    /// How much gas was used in a block
    pub block_gas_used: u64, //
    /// How many assertions we have executed in the block
    pub assertions_per_block: u64, //
    /// How much assertion gas we executed in a block
    pub assertion_gas_per_block: u64, //
    /// Current block height
    pub current_height: u64, //
}

impl BlockMetrics {
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Commits the metrics
    pub fn commit(&self) {
        histogram!("block_processing_duration_seconds").record(self.block_processing_duration);
        counter!("transactions_considered_total").increment(self.transactions_considered_total);
        counter!("transactions_simulated_total").increment(self.transactions_simulated_total);
        counter!("transactions_simulated_success_total").increment(self.transactions_simulated_success_total);
        counter!("transactions_simulated_failure_total").increment(self.transactions_simulated_failure_total);
        counter!("invalidated_transactions_total").increment(self.invalidated_transactions_total);
        gauge!("block_gas_used").set(self.block_gas_used as f64);
        gauge!("assertions_per_block").set(self.assertions_per_block as f64);
        gauge!("assertion_gas_per_block").set(self.assertion_gas_per_block as f64);
        gauge!("current_height").set(self.current_height as f64);
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
    pub fn new(hash: FixedBytes<32>) -> Self {
        Self {
            hash,
            assertion_gas_per_transaction: 0,
            assertions_per_transaction: 0,
            transaction_processing_duration: std::time::Duration::default(),
            gas_per_assertion: 0,
        }
    }

    /// Commits the per tx metrics
    pub fn commit(&self) {
        histogram!("assertion_gas_per_transaction", "tx_hash" => self.hash.to_string()).record(self.assertion_gas_per_transaction as f64);
        histogram!("assertions_per_transaction", "tx_hash" => self.hash.to_string()).record(self.assertions_per_transaction as f64);
        histogram!("transaction_processing_duration", "tx_hash" => self.hash.to_string()).record(self.transaction_processing_duration);
        histogram!("gas_per_assertion", "tx_hash" => self.hash.to_string()).record(self.gas_per_assertion as f64);
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}
