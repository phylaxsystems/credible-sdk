//! # `engine`
//!
//! The engine is responsible for executing transactions and verifying against
//! assertions. It does this by receiving transactions over a channel and
//! executes them in order. New blocks are marked by new `BlockEnv` objects
//! being received over a channel.
//!
//! When processing a new block(by receiving a new `BlockEnv`) and executing
//! associated trasnactions, the engine will advance its state and verify that
//! txs pass assertions.
//!
//! ```no_run
//! ┌─────────────────────────────────────────────────────────────┐
//! │                        CORE ENGINE                          │
//! ├─────────────────────────────────────────────────────────────┤
//! │                                                             │
//! │  ┌────────────────┐                        ┌─────────────┐  │
//! │  │  BlockEnv/TXs  │                        │ TX Results  │  │
//! │  └─────┬──────────┘                        └─────────────┘  │
//! │        │                                          ^         │
//! │        │                                          │         │
//! │        v                                          │         │
//! │  ┌─────────────┐         ┌─────────────┐          │         │
//! │  │Transaction  │  ────>  │   PhEVM     │  ────────┘         │
//! │  │   Queue     │         │             │                    │
//! │  └─────────────┘         └─────────────┘                    │
//! │                                 ^                           │
//! │                                 │                           │
//! │                                 v                           │
//! │                          ┌──────────────┐                   │
//! │                          │ State Access │                   │
//! │                          └──────────────┘                   │
//! │                                                             │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! Assertions are EVM code executed in parallel after every transaction.
//! This is possible due to assertions being read only. We must verify that no
//! assertion reverts before approving a transaction.

pub mod queue;
mod transactions_results;

use super::engine::queue::{
    QueueBlockEnv,
    TransactionQueueReceiver,
    TxQueueContents,
};
use crate::{
    TransactionsState,
    critical,
    metrics::{
        BlockMetrics,
        TransactionMetrics,
    },
};
use assertion_executor::primitives::EVMError;
use std::fmt::Debug;

#[allow(unused_imports)]
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    ExecutorError,
    db::overlay::OverlayDb,
    primitives::ExecutionResult,
    store::{
        AssertionState,
        AssertionStore,
        AssertionStoreError,
    },
};
use revm::state::EvmState;

use crate::{
    cache::Cache,
    engine::transactions_results::TransactionsResults,
    utils::ErrorRecoverability,
};
use assertion_executor::{
    ForkTxExecutionError,
    db::Database,
};
#[allow(unused_imports)]
use revm::{
    DatabaseCommit,
    DatabaseRef,
    context::{
        BlockEnv,
        TxEnv,
    },
    primitives::{
        Address,
        B256,
    },
};
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

/// Contains the last two executed transaction hashes and resulting states.
/// Stores up to 2 transactions in a stack-allocated array.
#[derive(Debug)]
struct LastExecutedTx {
    hashes: [Option<(B256, Option<EvmState>)>; 2],
    len: usize,
}

impl LastExecutedTx {
    fn new() -> Self {
        Self {
            hashes: [None, None],
            len: 0,
        }
    }

    fn push(&mut self, hash: B256, state: Option<EvmState>) {
        if self.len == 2 {
            // Shift elements to make room for new one
            self.hashes[0] = self.hashes[1].take();
            self.hashes[1] = Some((hash, state));
        } else {
            self.hashes[self.len] = Some((hash, state));
            self.len += 1;
        }
    }

    fn remove_last(&mut self) -> Option<(B256, Option<EvmState>)> {
        if self.len == 0 {
            return None;
        }

        let result = self.hashes[self.len - 1].take();
        self.len -= 1;
        result
    }

    fn current(&self) -> Option<&(B256, Option<EvmState>)> {
        if self.len == 0 {
            None
        } else {
            self.hashes[self.len - 1].as_ref()
        }
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum EngineError {
    #[error("Database error")]
    DatabaseError,
    #[error("Transaction error")]
    TransactionError,
    #[error("Assertion error")]
    AssertionError,
    #[error("Transaction queue channel closed")]
    ChannelClosed,
    #[error("Get transaction result oneshot channel closed")]
    GetTxResultChannelClosed,
    #[error("Hash supplied by the reorg event does not match the last executed transaction")]
    BadReorgHash,
    #[error(
        "Nothing to commit. We expect a reorg for a failed transaction due to an internal EVM error."
    )]
    NothingToCommit,
}

impl From<&EngineError> for ErrorRecoverability {
    fn from(e: &EngineError) -> Self {
        match e {
            EngineError::DatabaseError
            | EngineError::AssertionError
            | EngineError::NothingToCommit
            | EngineError::BadReorgHash => ErrorRecoverability::Unrecoverable,
            EngineError::TransactionError
            | EngineError::ChannelClosed
            | EngineError::GetTxResultChannelClosed => ErrorRecoverability::Recoverable,
        }
    }
}

/// Represents either a successful transaction validation or an internal validation error
#[derive(Debug, Clone, PartialEq)]
pub enum TransactionResult {
    /// Transaction was processed successfully (may have reverted/halted, but validation completed)
    ValidationCompleted {
        execution_result: ExecutionResult,
        is_valid: bool,
    },
    /// Internal error occurred during validation process
    ValidationError(String),
}

/// The engine processes blocks and appends transactions to them.
/// It accepts transaction events sent from a transport via the `TransactionQueueReceiver`
/// and processes them accordingly.
#[derive(Debug)]
pub struct CoreEngine<DB> {
    state: OverlayDb<DB>,
    cache: Arc<Cache>,
    tx_receiver: TransactionQueueReceiver,
    assertion_executor: AssertionExecutor,
    block_env: Option<BlockEnv>,
    transaction_results: TransactionsResults,
    block_metrics: BlockMetrics,
    last_executed_tx: LastExecutedTx,
    block_env_transaction_counter: u64,
}

impl<DB: DatabaseRef + Send + Sync> CoreEngine<DB> {
    #[instrument(name = "engine::new", skip_all, level = "debug")]
    pub fn new(
        state: OverlayDb<DB>,
        cache: Arc<Cache>,
        tx_receiver: TransactionQueueReceiver,
        assertion_executor: AssertionExecutor,
        state_results: Arc<TransactionsState>,
        transaction_results_max_capacity: usize,
    ) -> Self {
        Self {
            state,
            cache,
            tx_receiver,
            assertion_executor,
            block_env: None,
            transaction_results: TransactionsResults::new(
                state_results,
                transaction_results_max_capacity,
            ),
            block_metrics: BlockMetrics::new(),
            last_executed_tx: LastExecutedTx::new(),
            block_env_transaction_counter: 0,
        }
    }

    /// Creates a new `CoreEngine` for testing purposes.
    /// Not to be used for anything but tests.
    #[cfg(test)]
    #[allow(dead_code)]
    #[allow(clippy::missing_panics_doc)]
    pub fn new_test() -> Self {
        let (_, tx_receiver) = crossbeam::channel::unbounded();
        Self {
            state: OverlayDb::new(None, 64),
            tx_receiver,
            assertion_executor: AssertionExecutor::new(
                ExecutorConfig::default(),
                AssertionStore::new_ephemeral().expect("REASON"),
            ),
            block_env: None,
            cache: Arc::new(Cache::new(vec![], 10)),
            transaction_results: TransactionsResults::new(TransactionsState::new(), 10),
            block_metrics: BlockMetrics::new(),
            last_executed_tx: LastExecutedTx::new(),
            block_env_transaction_counter: 0,
        }
    }

    /// Inserts an assertion directly into the assertion store of the engine.
    #[cfg(test)]
    pub fn insert_into_store(
        &self,
        address: Address,
        assertion: AssertionState,
    ) -> Result<(), AssertionStoreError> {
        self.assertion_executor
            .store
            .insert(address, assertion)
            .map(|_| ())
    }

    /// Execute transaction with the core engines blockenv.
    // FIXME: Break this function down into smaller pieces
    #[allow(clippy::too_many_lines)]
    #[instrument(
        name = "engine::execute_transaction",
        skip(self, tx_env),
        fields(tx_hash = %tx_hash, caller = %tx_env.caller, gas_limit = tx_env.gas_limit),
        level = "debug"
    )]
    fn execute_transaction(&mut self, tx_hash: B256, tx_env: &TxEnv) -> Result<(), EngineError> {
        self.block_env_transaction_counter += 1;
        let mut tx_metrics = TransactionMetrics::new(tx_hash);
        let instant = std::time::Instant::now();

        trace!(
            target = "engine",
            tx_hash = %tx_hash,
            tx_env = ?tx_env,
            "Executing transaction with environment"
        );

        let mut fork_db = self.state.fork();
        let block_env = self.block_env.as_ref().ok_or_else(|| {
            error!("No block environment set for transaction execution");
            EngineError::TransactionError
        })?;

        debug!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = block_env.number,
            "Validating transaction against assertions"
        );

        #[cfg(feature = "linea")]
        if check_recepient_address(tx_env).is_none() {
            // if `None`, we can just skip this transaction as it failed
            // linea execution requirements
            return Ok(());
        }

        // Validate transaction and run assertions
        let rax = self.assertion_executor.validate_transaction_ext_db(
            block_env.clone(),
            tx_env.clone(),
            &mut fork_db,
            &mut self.state,
        );

        tx_metrics.transaction_processing_duration = instant.elapsed();

        let rax = match rax {
            Ok(rax) => rax,
            Err(e) => {
                if !ErrorRecoverability::from(&e).is_recoverable() {
                    critical!(error = ?e, "Failed to execute a transaction");
                }
                self.last_executed_tx.push(tx_hash, None);
                match e {
                    ExecutorError::ForkTxExecutionError(_) => {
                        // Transaction validation errors (nonce, gas, funds, etc.)
                        debug!(
                            target = "engine",
                            error = ?e,
                            tx_hash = %tx_hash,
                            "Transaction validation failed"
                        );
                        trace!(
                            target = "engine",
                            tx_hash = %tx_hash,
                            tx_env = ?tx_env,
                            "Transaction validation environment"
                        );
                        self.transaction_results.add_transaction_result(
                            tx_hash,
                            &TransactionResult::ValidationError(format!("{e:?}")),
                        );
                        return Ok(());
                    }
                    ExecutorError::AssertionExecutionError(_) => {
                        // Assertion system failures (database corruption, invalid bytecode, etc.)
                        // These should crash the engine as they indicate system-level problems
                        error!(
                            target = "engine",
                            error = ?e,
                            tx_hash = %tx_hash,
                            tx_env= ?tx_env,
                            "Fatal assertion execution error occurred"
                        );

                        self.transaction_results.add_transaction_result(
                            tx_hash,
                            &TransactionResult::ValidationError(format!("{e:?}")),
                        );

                        return Err(EngineError::AssertionError);
                    }
                }
            }
        };

        tx_metrics.assertions_per_transaction = rax.total_assertion_funcs_ran();
        self.block_metrics.assertions_per_block += rax.total_assertion_funcs_ran();
        tx_metrics.assertion_gas_per_transaction = rax.total_assertions_gas();
        self.block_metrics.assertion_gas_per_block += rax.total_assertions_gas();

        let is_valid = rax.is_valid();
        let execution_result = rax.result_and_state.result.clone();

        info!(
            target = "engine",
            tx_hash = %tx_hash,
            is_valid,
            execution_result = ?execution_result,
            "Transaction processed"
        );

        self.block_metrics.transactions_simulated += 1;

        if is_valid {
            // Transaction valid, passed assertions, commit state for successful transactions
            debug!(
                target = "engine",
                tx_hash = %tx_hash,
                "Transaction does not invalidate assertions, processing result"
            );
            trace!(
                target = "engine",
                tx_hash = %tx_hash,
                tx_env = ?tx_env,
                "Transaction processing environment"
            );
            trace!(
                target = "engine",
                tx_hash = %tx_hash,
                assertions_ran = ?rax.assertions_executions,
                "Assertions execution details"
            );

            if execution_result.is_success() {
                trace!(
                    target = "engine",
                    tx_hash = %tx_hash,
                    "Commiting state of successful tx to buffer"
                );
                self.block_metrics.block_gas_used += rax.result_and_state.result.gas_used();
                self.block_metrics.transactions_simulated_success += 1;
            } else {
                self.block_metrics.transactions_simulated_failure += 1;
            }
        } else {
            warn!(
                target = "engine",
                tx_hash = %tx_hash,
                "Transaction failed assertion validation"
            );
            trace!(
                target = "engine",
                tx_hash = %tx_hash,
                tx_env = ?tx_env,
                assertions_executions = ?rax.assertions_executions,
                "Transaction validation details"
            );

            self.block_metrics.invalidated_transactions += 1;
        }

        self.last_executed_tx
            .push(tx_hash, Some(rax.result_and_state.state));

        // Store the transaction result
        self.transaction_results.add_transaction_result(
            tx_hash,
            &TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            },
        );

        trace!("Transaction execution completed");
        Ok(())
    }

    /// Get the state of the engine's overlay database for testing purposes.
    #[cfg(test)]
    pub fn get_state(&self) -> &OverlayDb<DB> {
        &self.state
    }

    /// Get a reference to the block environment for testing purposes.
    #[cfg(test)]
    pub fn get_block_env(&self) -> Option<&BlockEnv> {
        self.block_env.as_ref()
    }

    /// Get transaction result by hash.
    #[cfg(test)]
    pub fn get_transaction_result(
        &self,
        tx_hash: &B256,
    ) -> Option<dashmap::mapref::one::Ref<'_, B256, TransactionResult>> {
        self.transaction_results.get_transaction_result(tx_hash)
    }

    /// Get transaction result by hash, returning a cloned value for test compatibility.
    #[cfg(test)]
    pub fn get_transaction_result_cloned(&self, tx_hash: &B256) -> Option<TransactionResult> {
        self.transaction_results
            .get_transaction_result(tx_hash)
            .map(|r| r.clone())
    }

    /// Get all transaction results for testing purposes.
    #[cfg(test)]
    pub fn get_all_transaction_results(&self) -> &dashmap::DashMap<B256, TransactionResult> {
        self.transaction_results.get_all_transaction_result()
    }

    /// Clone all transaction results for testing purposes.
    #[cfg(test)]
    pub fn clone_transaction_results(&self) -> HashMap<B256, TransactionResult> {
        self.transaction_results
            .get_all_transaction_result()
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    fn check_cache(&mut self, queue_block_env: &QueueBlockEnv) {
        // If the block env is not +1 from the previous block env, invalidate the cache
        if let Some(prev_block_env) = self.block_env.as_ref()
            && prev_block_env.number != queue_block_env.block_env.number - 1
        {
            warn!(prev_block_env = %prev_block_env.number, current_block_env = %queue_block_env.block_env.number, "BlockEnv received is not +1 from the previous block env, invalidating cache");
            self.cache
                .reset_required_block_number(queue_block_env.block_env.number);
            self.state.invalidate_all();
        }

        // If the last tx hash from the block env is different from the last tx hash from the
        // queue, invalidate the cache
        if let Some((prev_tx_hash, _)) = self.last_executed_tx.current()
            && Some(prev_tx_hash) != queue_block_env.last_tx_hash.as_ref()
        {
            warn!(prev_tx_hash = ?prev_tx_hash, current_tx_hash = ?queue_block_env.last_tx_hash, "The last transaction hash in the BlockEnv does not match the last transaction processed, invalidating cache");
            self.cache
                .reset_required_block_number(queue_block_env.block_env.number);
            self.state.invalidate_all();
        }

        // If the number of transactions in the block env is different from the number of
        // transactions received, invalidate the cache
        if self.block_env_transaction_counter != queue_block_env.n_transactions {
            warn!(
                sidecar_n_transactions = self.block_env_transaction_counter,
                block_env_n_transactions = queue_block_env.n_transactions,
                "The number of transactions in the BlockEnv does not much the transactions processed, invalidating cache"
            );
            self.cache
                .reset_required_block_number(queue_block_env.block_env.number);
            self.state.invalidate_all();
        }

        self.block_env_transaction_counter = 0;
    }

    /// Run the engine and process transactions and blocks received
    /// via the transaction queue.
    // TODO: fn should probably not be async but we do it because
    // so we can easily select on result in main. too bad!
    #[instrument(name = "engine::run", skip_all, level = "info")]
    pub async fn run(&mut self) -> Result<(), EngineError> {
        let mut processed_blocks = 0u64;
        let mut processed_txs = 0u64;
        let mut block_processing_time = std::time::Instant::now();

        loop {
            // Use try_recv and yield when empty to be async-friendly
            let event = match self.tx_receiver.try_recv() {
                Ok(event) => event,
                Err(crossbeam::channel::TryRecvError::Empty) => {
                    // Channel is empty, yield to allow other tasks to run
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(crossbeam::channel::TryRecvError::Disconnected) => {
                    error!(target = "engine", "Transaction queue channel disconnected");
                    return Err(EngineError::ChannelClosed);
                }
            };

            match event {
                TxQueueContents::Block(queue_block_env, current_span) => {
                    let block_env = &queue_block_env.block_env;
                    let _guard = current_span.enter();

                    self.check_cache(&queue_block_env);

                    // Apply the previously executed transaction state changes
                    self.apply_state_buffer()?;

                    processed_blocks += 1;
                    info!(
                        target = "engine",
                        block_number = block_env.number,
                        processed_blocks,
                        "Processing new block",
                    );
                    debug!(
                        target = "engine",
                        timestamp = block_env.timestamp,
                        number = block_env.number,
                        gas_limit = block_env.gas_limit,
                        base_fee = ?block_env.basefee,
                        "Block details"
                    );

                    self.cache.set_block_number(block_env.number);

                    self.block_metrics.block_processing_duration = block_processing_time.elapsed();
                    self.block_metrics.current_height = block_env.number;
                    // Commit all values inside of `block_metrics` to prometheus collector
                    self.block_metrics.commit();
                    // Reset the values inside to their defaults
                    self.block_metrics.reset();
                    block_processing_time = std::time::Instant::now();

                    self.block_env = Some(queue_block_env.block_env);
                }
                TxQueueContents::Tx(queue_transaction, current_span) => {
                    let _guard = current_span.enter();

                    let tx_hash = queue_transaction.tx_hash;
                    let tx_env = queue_transaction.tx_env;
                    processed_txs += 1;
                    self.block_metrics.transactions_considered += 1;

                    if self.block_env.is_none() {
                        error!(
                            target = "engine",
                            tx_hash = %tx_hash,
                            caller = %tx_env.caller,
                            processed_txs,
                            "Received transaction without first receiving a BlockEnv"
                        );
                        return Err(EngineError::TransactionError);
                    }

                    debug!(
                        target = "engine",
                        tx_hash = %tx_hash,
                        caller = %tx_env.caller,
                        gas_limit = tx_env.gas_limit,
                        processed_txs,
                        current_block = self.block_env.as_ref().map(|b| b.number),
                        "Processing transaction"
                    );

                    // Apply the previously executed transaction state changes
                    self.apply_state_buffer()?;

                    // Process the transaction with the current block environment
                    self.execute_transaction(tx_hash, &tx_env)?;
                }
                TxQueueContents::Reorg(hash, current_span) => {
                    let _guard = current_span.enter();
                    self.execute_reorg(hash)?;
                }
            }

            if processed_blocks > 0 && processed_blocks.is_multiple_of(100) {
                info!(
                    target = "engine",
                    blocks = processed_blocks,
                    transactions = processed_txs,
                    cache_entries = self.state.cache_entry_count(),
                    "Engine processing stats"
                );
            }
        }
    }

    /// Applies the state inside `self.last_executed_tx` to `self.state`.
    ///
    /// If `self.last_executed_tx` is empty, we dont do anything.
    fn apply_state_buffer(&mut self) -> Result<(), EngineError> {
        if let Some((_, state)) = self.last_executed_tx.current() {
            let changes = state.clone();
            let changes = changes.ok_or(EngineError::NothingToCommit)?;
            self.state.commit(changes);
        }
        self.last_executed_tx = LastExecutedTx::new();
        Ok(())
    }

    /// Processes a reorg event. Checks if the hash of the last executed tx
    /// matches the hash supplied by the reorg event.
    /// If yes, we throw out the last executed tx buffer. If not, we throw
    /// an error.
    ///
    /// This function is needed because we don't know if a transaction was
    /// fully included in a block because a besu plugin might unselect it.
    /// Because we are receiving transactions one-by-one for now, this is
    /// an acceptable solution.
    // TODO: when we star receiving tx bundles this should be expanded such
    // that we can go `n` transactions deep inside of a block.
    fn execute_reorg(&mut self, tx_hash: B256) -> Result<(), EngineError> {
        trace!(
            target = "engine",
            tx_hash = %tx_hash,
            "Checking reorg validity for hash"
        );

        // Check if we have received a transaction at all
        if let Some((last_hash, _)) = self.last_executed_tx.current()
            && tx_hash == *last_hash
        {
            info!(
                target = "engine",
                tx_hash = %tx_hash,
                "Executing reorg for hash"
            );

            // Remove the last transaction from buffer, preserving the previous one if it exists
            self.last_executed_tx.remove_last();

            // Remove transaction from results
            self.transaction_results.remove_transaction_result(tx_hash);

            // Only decrement the counter if we haven't processed a new block yet
            if self.block_env_transaction_counter > 0 {
                self.block_env_transaction_counter -= 1;
            }

            return Ok(());
        }

        // If we received a reorg event before executing a tx,
        // or if the tx hashes dont match something bad happened and we need to exit
        Err(EngineError::BadReorgHash)
    }
}

/// This is a linea specific function that checks that transaction recepients
/// are not precompiles, or fall in the reserved address range of `0x01`-`0x09`.
///
/// We call this function on txenvs before we execute them, and return a `None`
/// if the recepient falls on this address range.
#[cfg(feature = "linea")]
fn check_recepient_address(tx: &TxEnv) -> Option<()> {
    // tx is create, not calling range
    if tx.kind.is_create() {
        return Some(());
    }

    let address = tx.kind.to().unwrap().0;
    let is_precompile_range = address[0..19] == [0; 19] && (1..=9).contains(&address[19]);

    // Not in the bad precompile range, return some
    if !is_precompile_range {
        return Some(());
    }

    // Bad precompile range, return none
    None
}

impl<DBError> From<&EVMError<DBError>> for ErrorRecoverability {
    fn from(value: &EVMError<DBError>) -> Self {
        match value {
            EVMError::Transaction(_) | EVMError::Header(_) | EVMError::Custom(_) => {
                Self::Recoverable
            }
            EVMError::Database(_) => Self::Unrecoverable,
        }
    }
}

impl<ExtDb: revm::Database> From<&ForkTxExecutionError<ExtDb>> for ErrorRecoverability {
    fn from(value: &ForkTxExecutionError<ExtDb>) -> Self {
        match value {
            ForkTxExecutionError::TxEvmError(e) => e.into(),
            ForkTxExecutionError::CallTracerError(_) => Self::Recoverable,
        }
    }
}

impl<Active, ExtDb> From<&ExecutorError<Active, ExtDb>> for ErrorRecoverability
where
    Active: DatabaseRef,
    ExtDb: Database,
    ExtDb::Error: Debug,
{
    fn from(error: &ExecutorError<Active, ExtDb>) -> Self {
        match error {
            ExecutorError::ForkTxExecutionError(_) => ErrorRecoverability::Recoverable,
            ExecutorError::AssertionExecutionError(_) => ErrorRecoverability::Unrecoverable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::TestDbError;
    use assertion_executor::{
        ExecutorConfig,
        store::AssertionStore,
    };
    use revm::{
        context::{
            BlockEnv,
            TxEnv,
        },
        database::{
            CacheDB,
            EmptyDBTyped,
        },
        primitives::{
            Address,
            B256,
            Bytes,
            TxKind,
            U256,
            uint,
        },
    };
    use std::time::Duration;

    fn create_test_engine() -> (
        CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
        crossbeam::channel::Sender<TxQueueContents>,
    ) {
        let (tx_sender, tx_receiver) = crossbeam::channel::unbounded();
        let underlying_db = CacheDB::new(EmptyDBTyped::default());
        let state = OverlayDb::new(Some(std::sync::Arc::new(underlying_db)), 1024);
        let assertion_store =
            AssertionStore::new_ephemeral().expect("Failed to create assertion store");
        let assertion_executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);

        let state_results = TransactionsState::new();
        let cache = Arc::new(Cache::new(vec![], 10));
        let engine = CoreEngine::new(
            state,
            cache,
            tx_receiver,
            assertion_executor,
            state_results,
            10,
        );
        (engine, tx_sender)
    }

    #[test]
    fn test_last_executed_tx_single_push_and_pop() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x11; 32]);
        let s1: EvmState = EvmState::default();

        txs.push(h1, Some(s1));

        let cur = txs.current().expect("should contain the pushed tx");
        assert_eq!(cur.0, h1, "current should be the last pushed hash");

        let popped = txs.remove_last().expect("should pop the only element");
        assert_eq!(popped.0, h1, "popped should be the same hash");
        assert!(txs.current().is_none(), "should be empty after pop");
    }

    #[test]
    fn test_last_executed_tx_two_elements_lifo() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x21; 32]);
        let h2 = B256::from([0x22; 32]);
        txs.push(h1, Some(EvmState::default()));
        txs.push(h2, Some(EvmState::default()));

        // LIFO: current is h2
        assert_eq!(txs.current().unwrap().0, h2);
        // Pop h2, current becomes h1
        assert_eq!(txs.remove_last().unwrap().0, h2);
        assert_eq!(txs.current().unwrap().0, h1);
        // Pop h1, now empty
        assert_eq!(txs.remove_last().unwrap().0, h1);
        assert!(txs.current().is_none());
    }

    #[test]
    fn test_last_executed_tx_overflow_discards_oldest() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x31; 32]);
        let h2 = B256::from([0x32; 32]);
        let h3 = B256::from([0x33; 32]);

        // Fill to capacity
        txs.push(h1, Some(EvmState::default()));
        txs.push(h2, Some(EvmState::default()));
        assert_eq!(txs.current().unwrap().0, h2);

        // Push over capacity; should drop h1 and keep [h2, h3]
        txs.push(h3, Some(EvmState::default()));
        assert_eq!(
            txs.current().unwrap().0,
            h3,
            "current should be newest after overflow"
        );

        // Removing last returns h3, and now current should be h2 (h1 was discarded)
        assert_eq!(txs.remove_last().unwrap().0, h3);
        assert_eq!(
            txs.current().unwrap().0,
            h2,
            "previous should be preserved after pop"
        );

        // Removing last again returns h2 and leaves empty
        assert_eq!(txs.remove_last().unwrap().0, h2);
        assert!(txs.current().is_none());
    }

    fn create_test_block_env() -> BlockEnv {
        BlockEnv {
            number: 1,
            basefee: 0, // Set basefee to 0 to avoid balance issues
            ..Default::default()
        }
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_functionality(mut instance: crate::utils::LocalInstance) {
        // Send an empty block to verify we can advance the chain with empty blocks
        instance.new_block().await.unwrap();

        // Send and verify a reverting CREATE transaction
        let tx_hash = instance.send_reverting_create_tx().await.unwrap();

        // Verify transaction reverted but was still valid (passed assertions)
        assert!(
            instance
                .is_transaction_reverted_but_valid(&tx_hash)
                .await
                .unwrap(),
            "Transaction should revert but still be valid (pass assertions)"
        );

        // Send and verify a successful CREATE transaction
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Verify transaction was successful
        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        // Send Block 1 with Transaction 1
        let tx1_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Send Block 2 with Transaction 2
        let tx2_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Verify both transactions were processed successfully
        assert!(
            instance.is_transaction_successful(&tx1_hash).await.unwrap(),
            "Transaction 1 should be successful"
        );
        assert!(
            instance.is_transaction_successful(&tx2_hash).await.unwrap(),
            "Transaction 2 should be successful"
        );

        instance
            .send_assertion_passing_failing_pair()
            .await
            .unwrap();

        instance
            .send_and_verify_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();
        instance
            .send_and_verify_reverting_create_tx()
            .await
            .unwrap();
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reject_tx_before_blockenv(mut instance: crate::utils::LocalInstance) {
        // Send and verify a successful CREATE transaction
        let rax = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await;

        // Verify transaction was successful
        assert!(
            rax.is_err(),
            "Transaction did not error when sending before blockenv!"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_real(mut instance: crate::utils::LocalInstance) {
        // 1. run tx + reorg

        // Send and verify a successful CREATE transaction
        tracing::error!("1.");
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Verify transaction was successful
        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        // Send reorg and unwrap on the result, verifying if the core engine
        // processed tx or exited with error
        instance.send_reorg(tx_hash).await.unwrap();

        // 2. tx + reorg + tx
        tracing::error!("2.");

        // Send and verify a successful CREATE transaction
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Verify transaction was successful
        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        // Send reorg and unwrap on the result, verifying if the core engine
        // processed tx or exited with error
        instance.send_reorg(tx_hash).await.unwrap();

        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        // 3. tx + tx + reorg
        tracing::error!("3.");

        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        instance.send_reorg(tx_hash).await.unwrap();
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_bad_tx(mut instance: crate::utils::LocalInstance) {
        // Send and verify a successful CREATE transaction
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Verify transaction was successful
        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        // Send reorg and unwrap on the result, verifying if the core engine
        // processed tx or exited with error
        assert!(
            instance.send_reorg(B256::random()).await.is_err(),
            "not an error, core engine should have exited!"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_before_blockenv_rejected(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send reorg without any prior blockenv or transaction
        assert!(
            instance.send_reorg(B256::random()).await.is_err(),
            "Reorg before any blockenv should be rejected and exit engine"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_after_blockenv_before_tx_rejected(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send a blockenv with no transactions
        instance
            .send_block_with_txs(Vec::new())
            .await
            .expect("should send empty blockenv");

        // Now send a reorg before any transaction in this block
        assert!(
            instance.send_reorg(B256::random()).await.is_err(),
            "Reorg after blockenv but before any tx should be rejected"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_valid_then_previous_rejected(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Execute two successful transactions
        let tx1 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .expect("tx1 should be sent successfully");
        let tx2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .expect("tx2 should be sent successfully");

        // Valid reorg for the last executed tx should succeed (engine keeps running)
        instance
            .send_reorg(tx2)
            .await
            .expect("reorg of last executed tx should succeed");

        // Reorg for the previous tx (tx1) should be rejected
        // Because the engine only keeps the last executed tx in the buffer
        assert!(
            instance.send_reorg(tx1).await.is_err(),
            "Reorg with wrong hash should be rejected and exit engine"
        );
    }

    #[test]
    fn test_database_commit_verification() {
        use revm::primitives::address;

        let (mut engine, _) = create_test_engine();
        let block_env = create_test_block_env();

        // Create a simple create transaction that will succeed
        let tx_env = TxEnv {
            caller: Address::from([0x03; 20]),
            gas_limit: 100000,
            gas_price: 0,
            kind: TxKind::Create,
            value: uint!(0_U256),
            data: Bytes::from(vec![0x60, 0x00, 0x60, 0x00]),
            nonce: 0,
            ..Default::default()
        };

        // Generate a random transaction hash for testing
        let tx_hash = B256::from([0x33; 32]);

        // Get initial cache state
        let initial_cache_count = engine.get_state().cache_entry_count();

        engine.block_env = Some(block_env);

        // Execute the transaction
        let result = engine.execute_transaction(tx_hash, &tx_env);
        assert!(result.is_ok(), "Transaction should execute successfully");
        // We now need to advance the state by one block so we commit the transaction state
        engine.apply_state_buffer().unwrap();

        // Verify the caller's account state was updated
        let caller_account = engine
            .get_state()
            .basic_ref(tx_env.caller)
            .expect("Should be able to read caller account");
        assert!(
            caller_account.is_some(),
            "Caller account should exist after CREATE transaction"
        );
        let caller_info = caller_account.unwrap();
        assert_eq!(
            caller_info.nonce, 1,
            "Caller nonce should be incremented from 0 to 1"
        );
        assert_eq!(
            caller_info.balance,
            uint!(0_U256),
            "Caller balance should remain 0"
        );

        // Verify the created contract exists at the expected address
        // From the cache output, we know the contract was created at this address
        let contract_address = address!("76cae8af66cb2488933e640ba08650a3a8e7ae19");

        let contract_account = engine
            .get_state()
            .basic_ref(contract_address)
            .expect("Should be able to read contract account");
        assert!(
            contract_account.is_some(),
            "Contract account should exist at the expected address"
        );
        let contract_info = contract_account.unwrap();
        assert_eq!(
            contract_info.nonce, 1,
            "Contract nonce should be 1 for CREATE transactions"
        );
        assert_eq!(
            contract_info.balance,
            uint!(0_U256),
            "Contract balance should be 0"
        );

        // Verify the code hash matches empty bytecode hash (keccak256 of empty bytes)
        assert_eq!(
            contract_info.code_hash,
            revm::primitives::KECCAK_EMPTY,
            "Contract should have empty code hash"
        );

        // Verify that data has been committed by checking the cache count increases when we read data
        // (The overlay cache gets populated when data is read from the underlying database)
        let final_cache_count = engine.get_state().cache_entry_count();
        assert!(
            final_cache_count >= initial_cache_count,
            "Transaction executed and state is readable - data was committed. Initial: {initial_cache_count}, Final: {final_cache_count}"
        );

        // Verify we can read storage from the state after commit
        let state_result = engine.get_state().storage_ref(tx_env.caller, U256::ZERO);
        assert!(
            state_result.is_ok(),
            "Should be able to read from committed state"
        );

        // Verify transaction result is stored and succeeded
        let tx_result = engine.get_transaction_result_cloned(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        match tx_result.unwrap() {
            TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            } => {
                assert!(is_valid, "Transaction should pass assertions");
                match execution_result {
                    ExecutionResult::Success { .. } => {
                        // Expected - transaction succeeded
                    }
                    other => panic!("Expected Success result, got {other:?}"),
                }
            }
            TransactionResult::ValidationError(e) => {
                panic!("Unexpected validation error: {e:?}");
            }
        }
    }

    #[test]
    fn test_engine_requires_block_env_before_tx() {
        let (mut engine, _) = create_test_engine();
        let tx_env = TxEnv {
            caller: Address::from([0x04; 20]),
            gas_limit: 100000,
            gas_price: 0,
            kind: TxKind::Create,
            value: uint!(0_U256),
            data: Bytes::new(),
            nonce: 0,
            ..Default::default()
        };

        // Generate a random transaction hash for testing
        let tx_hash = B256::from([0x44; 32]);

        // Execute transaction without block environment
        let result = engine.execute_transaction(tx_hash, &tx_env);

        assert!(
            result.is_err(),
            "Engine should require block environment before processing transactions"
        );
        match result.unwrap_err() {
            EngineError::TransactionError => {
                // This is the expected error when no block environment is set
            }
            other => panic!("Expected TransactionError, got {other:?}"),
        }
    }

    #[tracing_test::traced_test]
    #[crate::utils::engine_test(all)]
    async fn test_block_env_wrong_transaction_number(mut instance: crate::utils::LocalInstance) {
        // Send and verify a reverting CREATE transaction
        let _tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.transport.set_n_transactions(2);

        // Send a blockEnv with the wrong number of transactions
        instance.new_block().await.unwrap();

        instance.wait_for_processing(Duration::from_millis(2)).await;

        assert!(logs_contain(
            "The number of transactions in the BlockEnv does not much the transactions processed, invalidating cache"
        ));
    }

    #[tracing_test::traced_test]
    #[crate::utils::engine_test(all)]
    async fn test_block_env_wrong_last_tx_hash(mut instance: crate::utils::LocalInstance) {
        // Send and verify a reverting CREATE transaction
        let tx_hash_1 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Send and verify a reverting CREATE transaction
        let _tx_hash_2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.transport.set_last_tx_hash(Some(tx_hash_1));

        // Send a blockEnv with the wrong last tx hash
        instance.new_block().await.unwrap();

        instance.wait_for_processing(Duration::from_millis(2)).await;

        assert!(logs_contain(
            "The last transaction hash in the BlockEnv does not match the last transaction processed, invalidating cache"
        ));
    }

    #[tracing_test::traced_test]
    #[crate::utils::engine_test(http)]
    async fn test_block_env_transaction_number_greater_than_zero_and_no_last_tx_hash(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send and verify a reverting CREATE transaction
        let _tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.transport.set_last_tx_hash(None);

        // Send a blockEnv with the wrong number of transactions
        let _ = instance.new_block().await;

        instance.wait_for_processing(Duration::from_millis(2)).await;

        // The blockEnv is not accepted at http level
        assert!(logs_contain("Failed to decode transactions"));
    }

    #[tracing_test::traced_test]
    #[crate::utils::engine_test(http)]
    async fn test_block_env_transaction_number_zero_and_last_tx_hash(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send and verify a reverting CREATE transaction
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.transport.set_last_tx_hash(Some(tx_hash));
        instance.transport.set_n_transactions(0);

        // Send a blockEnv with the wrong number of transactions
        let _ = instance.new_block().await;

        instance.wait_for_processing(Duration::from_millis(2)).await;

        // The blockEnv is not accepted at http level
        assert!(logs_contain("Failed to decode transactions"));
    }

    #[test]
    fn test_failed_transaction_commit() {
        let (mut engine, _) = create_test_engine();
        let tx_hash = B256::from([0x44; 32]);

        engine.last_executed_tx.push(tx_hash, None);

        let result = engine.apply_state_buffer();
        assert!(matches!(result, Err(EngineError::NothingToCommit)));
    }
}
