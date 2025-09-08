use metrics::{
    counter,
    gauge,
    histogram,
};

/// Individual block metrics we commit to the prometheus exporter.
///
/// Will commit metrics when dropped.
#[derive(Clone, Debug)]
pub struct BlockMetrics {
    /// Duration we spent processing transactions
    pub transaction_processing_duration: std::time::Duration,
    /// Entire duration from blockenv to blockenv
    pub block_processing_duration: std::time::Duration,
    /// How many transactions the engine has seen
    pub transactions_considered_total: u64,
    /// How many txs were executed
    pub transactions_simulated_total: u64,
    /// How many transactions we have executed successfully
    pub transactions_simulated_success_total: u64,
    /// How many transactions we have executed unsuccessfully
    pub transactions_simulated_failure_total: u64,
    /// How many transactions we have executed successfully,
    /// which ended up invalidating assertions
    pub invalidated_transactions_total: u64,
    /// How much gas was used in a block
    pub block_gas_used: u64,
    /// How many assertions we have executed in the block
    pub assertions_per_block: u64,
    /// How much assertion gas we executed in a block
    pub assertion_gas_per_block: u64,
    /// Current block height
    pub current_height: u64,
}

impl BlockMetrics {
    /// Commits the metrics
    pub fn commit(&self) {
        gauge!("transaction_processing_duration").set(self.transaction_processing_duration);
        gauge!("block_processing_duration").set(self.block_processing_duration);
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
    pub hash: String,
    /// How much assertion gas a transaction spent
    pub assertion_gas_per_transaction: u64,
    /// How many assertions we have executed per transaction
    pub assertions_per_transaction: u64,
    /// How much gas we have executed per assertion
    pub gas_per_assertion: u64,
}

impl TransactionMetrics {
    /// Commits the per tx metrics
    pub fn commit(&self) {
        histogram!("assertion_gas_per_transaction", "tx_hash" => self.hash.clone()).record(self.assertion_gas_per_transaction as f64);
        histogram!("assertions_per_transaction", "tx_hash" => self.hash.clone()).record(self.assertions_per_transaction as f64);
        histogram!("gas_per_assertion", "tx_hash" => self.hash.clone()).record(self.gas_per_assertion as f64);
    }
}

impl Drop for TransactionMetrics {
    fn drop(&mut self) {
        self.commit();
    }
}
