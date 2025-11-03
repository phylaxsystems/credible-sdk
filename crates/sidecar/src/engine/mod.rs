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
//! ```text
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

mod monitoring;
pub mod queue;
mod transactions_results;

use super::engine::queue::{
    CommitHead as QueueCommitHead,
    NewIteration as QueueNewIteration,
    QueueBlockEnv,
    QueueTransaction,
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
use assertion_executor::primitives::{
    EVMError,
    TxValidationResult,
};
use std::{
    fmt::Debug,
    sync::Arc,
    time::{
        Duration,
        Instant,
    },
};
use tokio::time::sleep;

#[allow(unused_imports)]
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
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
    execution_ids::{
        BlockExecutionId,
        TxExecutionId,
    },
    utils::ErrorRecoverability,
};
use alloy::primitives::TxHash;
use assertion_executor::{
    ExecutorError,
    ForkTxExecutionError,
    TxExecutionError,
    db::{
        Database,
        fork_db::ForkDb,
    },
};
#[cfg(feature = "cache_validation")]
use monitoring::cache::CacheChecker;
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
use std::collections::HashMap;
#[cfg(feature = "cache_validation")]
use tokio::task::AbortHandle;
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

/// Contains the last two executed transaction identifiers and resulting states.
/// Stores up to 2 transactions in a stack-allocated array.
#[derive(Debug)]
struct LastExecutedTx {
    execution_results: [Option<(TxExecutionId, Option<EvmState>)>; 2],
    len: usize,
}

impl LastExecutedTx {
    fn new() -> Self {
        Self {
            execution_results: [None, None],
            len: 0,
        }
    }

    fn push(&mut self, tx_execution_id: TxExecutionId, state: Option<EvmState>) {
        if self.len == 2 {
            // Shift elements to make room for new one
            self.execution_results[0] = self.execution_results[1].take();
            self.execution_results[1] = Some((tx_execution_id, state));
        } else {
            self.execution_results[self.len] = Some((tx_execution_id, state));
            self.len += 1;
        }
    }

    fn remove_last(&mut self) -> Option<(TxExecutionId, Option<EvmState>)> {
        if self.len == 0 {
            return None;
        }

        let result = self.execution_results[self.len - 1].take();
        self.len -= 1;
        result
    }

    fn current(&self) -> Option<&(TxExecutionId, Option<EvmState>)> {
        if self.len == 0 {
            None
        } else {
            self.execution_results[self.len - 1].as_ref()
        }
    }

    fn clear(&mut self) {
        self.execution_results = [None, None];
        self.len = 0;
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
    #[error("No sources are synced!")]
    NoSyncedSources,
    #[error("Infallible: Missing current block data")]
    MissingCurrentBlockData,
    #[error("Block number specified by transaction and block number currently built do not match!")]
    TxBlockMismatch,
    #[error("commitHead not received before newIteration")]
    MissingCommitHead,
    #[error("commitHead iteration {commit_iteration} does not match newIteration {new_iteration}")]
    CommitIterationMismatch {
        commit_iteration: u64,
        new_iteration: u64,
    },
}

impl From<&EngineError> for ErrorRecoverability {
    fn from(e: &EngineError) -> Self {
        match e {
            EngineError::DatabaseError
            | EngineError::AssertionError
            | EngineError::MissingCurrentBlockData => ErrorRecoverability::Unrecoverable,
            EngineError::TransactionError
            | EngineError::ChannelClosed
            | EngineError::GetTxResultChannelClosed
            | EngineError::NothingToCommit
            | EngineError::TxBlockMismatch
            | EngineError::BadReorgHash
            | EngineError::NoSyncedSources
            | EngineError::MissingCommitHead
            | EngineError::CommitIterationMismatch { .. } => ErrorRecoverability::Recoverable,
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

/// The `BlockIterationData` is a unique identifier for a block iteration state. It contains
/// the state of the current block for a given iteration (e.i., `fork_db`, `n_transactions`,
/// `last_executed_tx`)
#[derive(Debug)]
struct BlockIterationData<DB> {
    /// Current block's fork. It is created once per block, accumulates all transaction changes
    fork_db: ForkDb<OverlayDb<DB>>,
    /// How many transactions we have seen for each iteration in the `blockEnv` we are currently working on
    n_transactions: u64,
    /// Stores last executed transactions for reorging
    last_executed_tx: LastExecutedTx,
}

/// The engine processes blocks and appends transactions to them.
/// It accepts transaction events sent from a transport via the `TransactionQueueReceiver`
/// and processes them accordingly.
#[derive(Debug)]
pub struct CoreEngine<DB> {
    /// In-memory `revm::Database` cache. Populated as the sidecar executes blocks.
    ///
    /// This is the state the `CoreEngine` is executing transactions against.
    state: OverlayDb<DB>,
    /// Current block iteration data per block execution id.
    current_block_iterations: HashMap<BlockExecutionId, BlockIterationData<DB>>,
    /// External providers of state we use when we do not have a piece of state cached in our in memory db.
    /// External state providers implement a trait that we use to query databaseref-like data and populate `state: OverlayDb<DB>`
    /// for execution with it.
    ///
    /// Some state providers may be slow which is why we use them as a fallback for `state: OverlayDb<DB>`.
    cache: Arc<Cache>,
    /// Channel on which the core engine receives events. Events include new transactions, blocks(block envs), reorgs.
    tx_receiver: TransactionQueueReceiver,
    /// Core engines instance of the assertion executor, executes transactions and assertions
    assertion_executor: AssertionExecutor,
    /// Block env we are currently working on
    block_env: Option<BlockEnv>,
    /// Commit head metadata pending a new iteration event.
    pending_commit_head: Option<QueueCommitHead>,
    /// Stores results of executed transactions
    transaction_results: TransactionsResults,
    /// Core engine related block building metrics
    block_metrics: BlockMetrics,
    /// How long to wait to get a response for if the `cache: Arc<Cache>` state sources are synced.
    state_sources_sync_timeout: Duration,
    /// Used to indicate that we dont have any state sources available to avoid checking if they are synced.
    check_sources_available: bool,
    sources_monitoring: Arc<monitoring::sources::Sources>,
    /// Option to invalidate the cache on every block
    overlay_cache_invalidation_every_block: bool,
    #[cfg(feature = "cache_validation")]
    processed_transactions: Arc<moka::sync::Cache<TxHash, Option<EvmState>>>,
    #[cfg(feature = "cache_validation")]
    cache_checker: Option<AbortHandle>,
}

#[cfg(feature = "cache_validation")]
impl<DB> Drop for CoreEngine<DB> {
    fn drop(&mut self) {
        if let Some(handle) = self.cache_checker.take() {
            handle.abort();
        }
    }
}

impl<DB: DatabaseRef + Send + Sync> CoreEngine<DB> {
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "engine::new", skip_all, level = "debug")]
    pub async fn new(
        state: OverlayDb<DB>,
        cache: Arc<Cache>,
        tx_receiver: TransactionQueueReceiver,
        assertion_executor: AssertionExecutor,
        state_results: Arc<TransactionsState>,
        transaction_results_max_capacity: usize,
        state_sources_sync_timeout: Duration,
        source_monitoring_period: Duration,
        overlay_cache_invalidation_every_block: bool,
        #[cfg(feature = "cache_validation")] provider_ws_url: Option<&str>,
    ) -> Self {
        #[cfg(feature = "cache_validation")]
        let (processed_transactions, cache_checker) = {
            let processed_transactions: Arc<moka::sync::Cache<TxHash, Option<EvmState>>> =
                Arc::new(moka::sync::Cache::builder().max_capacity(100).build());
            let handle = if let Some(provider_ws_url) = provider_ws_url
                && let Ok(cache_checker) =
                    CacheChecker::try_new(provider_ws_url, processed_transactions.clone()).await
            {
                let handle = tokio::spawn(cache_checker.run());
                Some(handle.abort_handle())
            } else {
                None
            };

            (processed_transactions, handle)
        };
        Self {
            state,
            current_block_iterations: HashMap::new(),
            cache: cache.clone(),
            tx_receiver,
            assertion_executor,
            block_env: None,
            pending_commit_head: None,
            transaction_results: TransactionsResults::new(
                state_results,
                transaction_results_max_capacity,
            ),
            block_metrics: BlockMetrics::new(),
            state_sources_sync_timeout,
            check_sources_available: true,
            sources_monitoring: monitoring::sources::Sources::new(cache, source_monitoring_period),
            overlay_cache_invalidation_every_block,
            #[cfg(feature = "cache_validation")]
            processed_transactions,
            #[cfg(feature = "cache_validation")]
            cache_checker,
        }
    }

    /// Creates a new `CoreEngine` for testing purposes.
    /// Not to be used for anything but tests.
    #[cfg(test)]
    #[allow(dead_code)]
    #[allow(clippy::missing_panics_doc)]
    pub fn new_test() -> Self {
        let (_, tx_receiver) = crossbeam::channel::unbounded();
        let cache = Arc::new(Cache::new(vec![], 10));
        Self {
            state: OverlayDb::new(None),
            current_block_iterations: HashMap::new(),
            tx_receiver,
            assertion_executor: AssertionExecutor::new(
                ExecutorConfig::default(),
                AssertionStore::new_ephemeral().expect("REASON"),
            ),
            block_env: None,
            pending_commit_head: None,
            cache: cache.clone(),
            transaction_results: TransactionsResults::new(TransactionsState::new(), 10),
            block_metrics: BlockMetrics::new(),
            state_sources_sync_timeout: Duration::from_millis(100),
            check_sources_available: true,
            overlay_cache_invalidation_every_block: false,
            #[cfg(feature = "cache_validation")]
            processed_transactions: Arc::new(
                moka::sync::Cache::builder().max_capacity(100).build(),
            ),
            #[cfg(feature = "cache_validation")]
            cache_checker: None,
            sources_monitoring: monitoring::sources::Sources::new(cache, Duration::from_millis(20)),
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

    fn process_transaction_validation_error<ErrType>(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: &TxEnv,
        e: &ExecutorError<ErrType>,
    ) -> Result<(), EngineError>
    where
        ErrType: Debug,
    {
        let tx_hash = tx_execution_id.tx_hash;

        if !ErrorRecoverability::from(e).is_recoverable() {
            critical!(error = ?e, "Failed to execute a transaction");
        }
        match e {
            ExecutorError::ForkTxExecutionError(_) => {
                // Transaction validation errors (nonce, gas, funds, etc.)
                debug!(
                    target = "engine",
                    error = ?e,
                    tx_execution_id = %tx_execution_id.to_json_string(),
                    "Transaction validation failed"
                );
                trace!(
                    target = "engine",
                    tx_execution_id = %tx_execution_id.to_json_string(),
                    tx_env = ?tx_env,
                    "Transaction validation environment"
                );
                self.add_transaction_result(
                    tx_execution_id,
                    &TransactionResult::ValidationError(format!("{e:?}")),
                    None,
                )?;
                Ok(())
            }
            ExecutorError::AssertionExecutionError(state, _) => {
                // Assertion system failures (database corruption, invalid bytecode, etc.)
                // These should crash the engine as they indicate system-level problems
                error!(
                    target = "engine",
                    error = ?e,
                    tx_hash = %tx_hash,
                    tx_env= ?tx_env,
                    "Fatal assertion execution error occurred"
                );
                self.add_transaction_result(
                    tx_execution_id,
                    &TransactionResult::ValidationError(format!("{e:?}")),
                    Some(state.clone()),
                );
                Err(EngineError::AssertionError)
            }
            _ => Err(EngineError::AssertionError),
        }
    }

    /// Adds the result of a transaction to the transaction results and updates
    /// the last executed transaction accordingly.
    fn add_transaction_result(
        &mut self,
        tx_execution_id: TxExecutionId,
        result: &TransactionResult,
        state: Option<EvmState>,
    ) -> Result<(), EngineError> {
        let current_block_iteration = self
            .current_block_iterations
            .get_mut(&tx_execution_id.as_block_execution_id())
            .ok_or(EngineError::MissingCurrentBlockData)?;
        #[cfg(feature = "cache_validation")]
        // FIXME: needs to be iteration aware
        self.processed_transactions
            .insert(tx_execution_id.tx_hash, state.clone());

        current_block_iteration
            .last_executed_tx
            .push(tx_execution_id, state);
        self.transaction_results
            .add_transaction_result(tx_execution_id, result);
        Ok(())
    }

    fn trace_execute_transaction_result(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: &TxEnv,
        rax: &TxValidationResult,
    ) {
        let tx_hash = tx_execution_id.tx_hash;
        let is_valid = rax.is_valid();
        let execution_result = rax.result_and_state.result.clone();

        info!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = tx_execution_id.block_number,
            iteration_id = tx_execution_id.iteration_id,
            is_valid,
            execution_result = ?execution_result,
            "Transaction processed"
        );

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
    }

    /// Execute transaction with the core engines blockenv.
    #[instrument(
        name = "engine::execute_transaction",
        skip(self, tx_env),
        fields(
            tx_execution_id = %tx_execution_id.to_json_string(),
            caller = %tx_env.caller,
            gas_limit = tx_env.gas_limit
        ),
        level = "debug"
    )]
    fn execute_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: &TxEnv,
    ) -> Result<(), EngineError> {
        let tx_hash = tx_execution_id.tx_hash;
        let block_env = self.block_env.clone().ok_or_else(|| {
            error!("No block environment set for transaction execution");
            EngineError::TransactionError
        })?;

        let current_block_iteration = self
            .current_block_iterations
            .get_mut(&tx_execution_id.as_block_execution_id())
            .ok_or(EngineError::MissingCurrentBlockData)?;
        current_block_iteration.n_transactions += 1;

        let mut tx_metrics = TransactionMetrics::new();
        let instant = std::time::Instant::now();

        trace!(
            target = "engine",
            tx_hash = %tx_hash,
            tx_env = ?tx_env,
            "Executing transaction with environment"
        );

        debug!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = block_env.number,
            "Validating transaction against assertions"
        );

        #[cfg(feature = "linea")]
        if check_recepient_address(tx_env).is_none() {
            return Ok(());
        }

        // Validate transaction and run assertions
        // Execute directly on the block fork
        // Note: block_fork is ForkDb<OverlayDb<DB>>, so the error type will be
        // ExecutorError<<ForkDb<OverlayDb<DB>> as DatabaseRef>::Error>
        // which is ExecutorError<NotFoundError>
        let rax = self.assertion_executor.validate_transaction(
            block_env.clone(),
            tx_env,
            &mut current_block_iteration.fork_db,
            false,
        );

        tx_metrics.transaction_processing_duration = instant.elapsed();

        let rax = match rax {
            Ok(rax) => rax,
            Err(e) => {
                return self.process_transaction_validation_error(tx_execution_id, tx_env, &e);
            }
        };

        // Update metrics
        tx_metrics.assertions_per_transaction = rax.total_assertion_funcs_ran();
        self.block_metrics.assertions_per_block += rax.total_assertion_funcs_ran();
        tx_metrics.assertion_gas_per_transaction = rax.total_assertions_gas();
        self.block_metrics.assertion_gas_per_block += rax.total_assertions_gas();
        self.block_metrics.transactions_simulated += 1;
        tx_metrics.commit();

        // Log the result of the transaction execution
        self.trace_execute_transaction_result(tx_execution_id, tx_env, &rax);

        self.add_transaction_result(
            tx_execution_id,
            &TransactionResult::ValidationCompleted {
                is_valid: rax.is_valid(),
                execution_result: rax.result_and_state.result,
            },
            Some(rax.result_and_state.state),
        )?;

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

    /// Get transaction result by `TxExecutionId`.
    #[cfg(test)]
    pub fn get_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<dashmap::mapref::one::Ref<'_, TxExecutionId, TransactionResult>> {
        self.transaction_results
            .get_transaction_result(tx_execution_id)
    }

    /// Get transaction result by `TxExecutionId`, returning a cloned value for test compatibility.
    #[cfg(test)]
    pub fn get_transaction_result_cloned(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<TransactionResult> {
        self.transaction_results
            .get_transaction_result(tx_execution_id)
            .map(|r| r.clone())
    }

    /// Get all transaction results for testing purposes.
    #[cfg(test)]
    pub fn get_all_transaction_results(
        &self,
    ) -> &dashmap::DashMap<TxExecutionId, TransactionResult> {
        self.transaction_results.get_all_transaction_result()
    }

    /// Clone all transaction results for testing purposes.
    #[cfg(test)]
    pub fn clone_transaction_results(&self) -> HashMap<TxExecutionId, TransactionResult> {
        self.transaction_results
            .get_all_transaction_result()
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect()
    }

    /// Invalidates the state, cache and last executed tx
    fn invalidate_all(&mut self, queue_block_env: &QueueBlockEnv) {
        self.cache
            .reset_required_block_number(queue_block_env.block_env.number);

        // Measure cache invalidation time and set its new min required driver height
        let instant = Instant::now();
        self.check_sources_available = true;
        self.state.invalidate_all();
        self.current_block_iterations.clear();
        self.block_metrics
            .increment_cache_invalidation(instant.elapsed(), queue_block_env.block_env.number);
    }

    /// Checks if the cache should be cleared and clears it.
    fn check_cache(
        &mut self,
        queue_block_env: &QueueBlockEnv,
        block_execution_id: Option<BlockExecutionId>,
    ) {
        // If block execution ID is not present, it means that the block may be empty
        // (no transactions), therefore, set the last executed tx to None and the n_transactions to 0
        let (current_last_executed_tx, n_transactions) = if let Some(block_execution_id) =
            block_execution_id
            && let Some(current_block_iteration) =
                self.current_block_iterations.get(&block_execution_id)
        {
            (
                current_block_iteration.last_executed_tx.current().cloned(),
                current_block_iteration.n_transactions,
            )
        } else {
            (None, 0)
        };

        // If the block env is not +1 from the previous block env, invalidate the cache
        if let Some(prev_block_env) = self.block_env.as_ref()
            && prev_block_env.number != queue_block_env.block_env.number - 1
        {
            warn!(prev_block_env = %prev_block_env.number, current_block_env = %queue_block_env.block_env.number, "BlockEnv received is not +1 from the previous block env, invalidating cache");
            self.invalidate_all(queue_block_env);
        }

        // If the last tx hash from the block env is different from the last tx hash from the
        // queue, invalidate the cache
        if let Some((prev_tx_execution_id, _)) = current_last_executed_tx
            && Some(prev_tx_execution_id.tx_hash) != queue_block_env.last_tx_hash
        {
            warn!(
                prev_tx_hash = %prev_tx_execution_id.tx_hash,
                prev_block_number = prev_tx_execution_id.block_number,
                prev_iteration_id = prev_tx_execution_id.iteration_id,
                current_tx_hash = ?queue_block_env.last_tx_hash,
                "The last transaction hash in the BlockEnv does not match the last transaction processed, invalidating cache"
            );
            self.invalidate_all(queue_block_env);
        }

        // If the number of transactions in the block env is different from the number of
        // transactions received, invalidate the cache
        if n_transactions != queue_block_env.n_transactions {
            warn!(
                sidecar_n_transactions = n_transactions,
                block_env_n_transactions = queue_block_env.n_transactions,
                "The number of transactions in the BlockEnv does not match the transactions processed, invalidating cache"
            );
            self.invalidate_all(queue_block_env);
        }
    }

    /// Verifies that all state sources are synced, and if not stall until they are.
    ///
    /// If the sources do not become synced after a set amount of time, the function
    /// errors.
    async fn verify_state_sources_synced_for_tx(
        &mut self,
        block_number: u64,
    ) -> Result<(), EngineError> {
        const RETRY_INTERVAL: Duration = Duration::from_millis(10);

        if !self.check_sources_available {
            return Ok(());
        }
        self.check_sources_available = false;

        let start = Instant::now();
        loop {
            if self
                .cache
                .iter_synced_sources()
                .into_iter()
                .any(|a| a.is_synced(block_number))
            {
                return Ok(());
            }

            let waited = start.elapsed();
            if waited >= self.state_sources_sync_timeout {
                error!(
                    target = "engine",
                    waited_ms = waited.as_millis(),
                    timeout_ms = self.state_sources_sync_timeout.as_millis(),
                    "No synced sources within timeout"
                );
                return Err(EngineError::NoSyncedSources);
            }

            debug!(
                target = "engine",
                waited_ms = waited.as_millis(),
                next_retry_ms = RETRY_INTERVAL.as_millis(),
                timeout_ms = self.state_sources_sync_timeout.as_millis(),
                "No synced sources, retrying"
            );
            sleep(RETRY_INTERVAL).await;
        }
    }

    /// Run the engine and process transactions and blocks received
    /// via the transaction queue.
    // TODO: fn should probably not be async but we do it because
    // so we can easily select on result in main. too bad!
    pub async fn run(&mut self) -> Result<(), EngineError> {
        let mut processed_blocks = 0u64;
        let mut block_processing_time = Instant::now();
        let mut idle_start = Instant::now();

        loop {
            // Use try_recv and yield when empty to be async-friendly
            let event = match self.tx_receiver.try_recv() {
                Ok(event) => {
                    // We received an event, accumulate time spent idle
                    self.block_metrics.idle_time += idle_start.elapsed();
                    event
                }
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

            // Track event processing time
            let event_start = Instant::now();

            match event {
                TxQueueContents::Block(queue_block_env, current_span) => {
                    let _guard = current_span.enter();
                    self.process_block_event(
                        queue_block_env,
                        &mut processed_blocks,
                        &mut block_processing_time,
                    );
                }
                TxQueueContents::Tx(queue_transaction, current_span) => {
                    let _guard = current_span.enter();
                    self.process_transaction_event(queue_transaction).await?;
                }
                TxQueueContents::Reorg(tx_execution_id, current_span) => {
                    let _guard = current_span.enter();
                    self.execute_reorg(tx_execution_id)?;
                }
                TxQueueContents::CommitHead(commit_head, current_span) => {
                    let _guard = current_span.enter();
                    self.process_commit_head_event(commit_head)?;
                }
                TxQueueContents::NewIteration(new_iteration, current_span) => {
                    let _guard = current_span.enter();
                    self.process_new_iteration_event(
                        new_iteration,
                        &mut processed_blocks,
                        &mut block_processing_time,
                    )?;
                }
            }

            // Accumulate event processing time
            self.block_metrics.event_processing_time += event_start.elapsed();

            // Reset idle timer after processing event
            idle_start = Instant::now();

            if processed_blocks > 0 && processed_blocks.is_multiple_of(100) {
                info!(
                    target = "engine",
                    blocks = processed_blocks,
                    cache_entries = self.state.cache_entry_count(),
                    "Engine processing stats"
                );
            }
        }
    }

    #[instrument(name = "engine::process_commit_head_event", skip_all, level = "info")]
    fn process_commit_head_event(
        &mut self,
        commit_head: QueueCommitHead,
    ) -> Result<(), EngineError> {
        if let Some(existing) = &self.pending_commit_head {
            warn!(
                target = "engine",
                pending_iteration = existing.selected_iteration_id,
                new_iteration = commit_head.selected_iteration_id,
                "Overwriting pending commitHead"
            );
        }

        self.pending_commit_head = Some(commit_head);
        Ok(())
    }

    #[instrument(
        name = "engine::process_new_iteration_event",
        skip_all,
        fields(iteration_id = new_iteration.iteration_id),
        level = "info"
    )]
    fn process_new_iteration_event(
        &mut self,
        new_iteration: QueueNewIteration,
        processed_blocks: &mut u64,
        block_processing_time: &mut Instant,
    ) -> Result<(), EngineError> {
        let commit_head = self
            .pending_commit_head
            .take()
            .ok_or(EngineError::MissingCommitHead)?;

        if commit_head.selected_iteration_id != new_iteration.iteration_id {
            warn!(
                target = "engine",
                commit_iteration = commit_head.selected_iteration_id,
                new_iteration = new_iteration.iteration_id,
                "commitHead iteration mismatch"
            );
            return Err(EngineError::CommitIterationMismatch {
                commit_iteration: commit_head.selected_iteration_id,
                new_iteration: new_iteration.iteration_id,
            });
        }

        let queue_block_env = QueueBlockEnv {
            block_env: new_iteration.block_env,
            last_tx_hash: commit_head.last_tx_hash,
            n_transactions: commit_head.n_transactions,
            selected_iteration_id: Some(commit_head.selected_iteration_id),
        };

        self.process_block_event(queue_block_env, processed_blocks, block_processing_time)
    }

    #[instrument(name = "engine::process_block_event", skip_all, level = "info")]
    fn process_block_event(
        &mut self,
        queue_block_env: QueueBlockEnv,
        processed_blocks: &mut u64,
        block_processing_time: &mut Instant,
    ) -> Result<(), EngineError> {
        // Legacy block events take precedence; clear any pending commit head metadata.
        self.pending_commit_head = None;

        let block_env = &queue_block_env.block_env;

        let block_execution_id =
            queue_block_env
                .selected_iteration_id
                .map(|selected_iteration_id| {
                    BlockExecutionId {
                        block_number: block_env.number,
                        iteration_id: selected_iteration_id,
                    }
                });

        // If it is configured to invalidate the cache every block, do so
        if self.overlay_cache_invalidation_every_block {
            self.invalidate_all(&queue_block_env);
        } else {
            // If not, check if the cache should be invalidated.
            self.check_cache(&queue_block_env, block_execution_id);
        }

        // Finalize the previous block by committing its fork to the underlying state
        if let Some(block_execution_id) = block_execution_id {
            // Apply the last transaction's state to the current block fork
            self.apply_state_buffer_to_fork(block_execution_id)?;
            self.finalize_previous_block(block_execution_id);
        } else {
            warn!(
                target = "engine",
                block_number = %block_env.number,
                "blockEnv missing selected iteration id"
            );
        }

        *processed_blocks += 1;

        info!(
            target = "engine",
            block_number = block_env.number,
            processed_blocks = *processed_blocks,
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
        *block_processing_time = Instant::now();

        self.block_env = Some(queue_block_env.block_env);
        self.current_block_iterations = HashMap::new();

        Ok(())
    }

    #[instrument(name = "engine::process_transaction_event", skip_all, level = "info")]
    async fn process_transaction_event(
        &mut self,
        queue_transaction: QueueTransaction,
    ) -> Result<(), EngineError> {
        let tx_execution_id = queue_transaction.tx_execution_id;
        let tx_hash = tx_execution_id.tx_hash;
        let tx_env = queue_transaction.tx_env;
        self.block_metrics.transactions_considered += 1;

        let Some(ref block) = self.block_env else {
            error!(
                target = "engine",
                tx_hash = %tx_hash,
                block_number = tx_execution_id.block_number,
                iteration_id = tx_execution_id.iteration_id,
                caller = %tx_env.caller,
                "Received transaction without first receiving a BlockEnv"
            );
            return Err(EngineError::TransactionError);
        };

        // Checks if the received `TxExecutionId` matches blockenv requirements.
        // `tx_execution_id` must be `self.block_env.number + 1`, otherwise we should
        // drop the event.
        let expected_block_number = block.number + 1;
        if expected_block_number != tx_execution_id.block_number {
            warn!(
                target = "engine",
                tx_hash = %tx_hash,
                blockenv_block_number = block.number,
                tx_block_number = tx_execution_id.block_number,
                iteration_id = tx_execution_id.iteration_id,
                caller = %tx_env.caller,
                "Requested transaction block number does not match block number currently built in engine!"
            );

            let error_message = format!(
                "Transaction targeted block {} but engine is building block {}",
                tx_execution_id.block_number, expected_block_number
            );
            self.transaction_results.add_transaction_result(
                tx_execution_id,
                &TransactionResult::ValidationError(error_message),
            );

            return Ok(());
        }

        // Initialize the current block iteration id if it does not exist
        self.current_block_iterations
            .entry(tx_execution_id.as_block_execution_id())
            .or_insert(BlockIterationData {
                fork_db: self.state.fork(),
                n_transactions: 0,
                last_executed_tx: LastExecutedTx::new(),
            });

        self.verify_state_sources_synced_for_tx(block.number)
            .await?;

        debug!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = tx_execution_id.block_number,
            iteration_id = tx_execution_id.iteration_id,
            caller = %tx_env.caller,
            gas_limit = tx_env.gas_limit,
            current_block = self.block_env.as_ref().map(|b| b.number),
            "Processing transaction"
        );

        // Apply the previously executed transaction state changes to the block fork
        self.apply_state_buffer_to_fork(tx_execution_id.as_block_execution_id())?;

        // Process the transaction with the current block environment
        self.execute_transaction(tx_execution_id, &tx_env)?;

        Ok(())
    }

    /// Applies the state inside `self.last_executed_tx` to the current block fork.
    fn apply_state_buffer_to_fork(
        &mut self,
        block_execution_id: BlockExecutionId,
    ) -> Result<(), EngineError> {
        let Some(current_block_iteration) =
            self.current_block_iterations.get_mut(&block_execution_id)
        else {
            return Ok(());
        };

        if let Some((tx_execution_id, state)) = current_block_iteration.last_executed_tx.current() {
            let changes = state.clone();
            let changes = changes.ok_or(EngineError::NothingToCommit)?;

            // Commit to the current block fork
            current_block_iteration.fork_db.commit(changes);
        }
        current_block_iteration.last_executed_tx = LastExecutedTx::new();
        Ok(())
    }

    /// Finalizes the previous block by committing the block fork to the underlying state
    fn finalize_previous_block(&mut self, block_execution_id: BlockExecutionId) {
        let Some(current_block_iteration) =
            self.current_block_iterations.get_mut(&block_execution_id)
        else {
            return;
        };

        debug!(
            target = "engine",
            "Finalizing previous block by committing fork to underlying state"
        );

        // Commit the entire block's state to the underlying OverlayDb
        self.state
            .commit_overlay_fork_db(current_block_iteration.fork_db.clone());
    }

    /// Applies the state inside `self.last_executed_tx` to `self.state`.
    ///
    /// If `self.last_executed_tx` is empty, we dont do anything.
    #[cfg(test)]
    fn apply_state_buffer(
        &mut self,
        block_execution_id: BlockExecutionId,
    ) -> Result<(), EngineError> {
        let Some(current_block_iteration) =
            self.current_block_iterations.get_mut(&block_execution_id)
        else {
            return Ok(());
        };

        #[allow(clippy::used_underscore_binding)]
        if let Some((_tx_execution_id, state)) = current_block_iteration.last_executed_tx.current()
        {
            let changes = state.clone();
            let changes = changes.ok_or(EngineError::NothingToCommit)?;
            self.state.commit(changes);
        }
        current_block_iteration.last_executed_tx = LastExecutedTx::new();
        Ok(())
    }

    /// Processes a reorg event. Checks if the execution id of the last executed tx
    /// matches the identifier supplied by the reorg event.
    /// If yes, we throw out the last executed tx buffer. If not, we throw
    /// an error.
    ///
    /// This function is needed because we don't know if a transaction was
    /// fully included in a block because a besu plugin might unselect it.
    /// Because we are receiving transactions one-by-one for now, this is
    /// an acceptable solution.
    // TODO: when we star receiving tx bundles this should be expanded such
    // that we can go `n` transactions deep inside of a block.
    #[instrument(
        name = "engine::execute_reorg",
        skip_all,
        fields(
            tx_hash = %tx_execution_id.tx_hash,
            block_number = tx_execution_id.block_number,
            iteration_id = tx_execution_id.iteration_id
        ),
        level = "info"
    )]
    fn execute_reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), EngineError> {
        trace!(
            target = "engine",
            tx_execution_id = %tx_execution_id.to_json_string(),
            "Checking reorg validity for hash"
        );

        let Some(ref block) = self.block_env else {
            error!(
                target = "engine",
                tx_execution_id = %tx_execution_id.to_json_string(),
                "Received reorg without first receiving a BlockEnv"
            );
            return Err(EngineError::TransactionError);
        };

        // Checks if the received `TxExecutionId` matches blockenv requirements.
        // `tx_execution_id` must be `self.block_env.number + 1`, otherwise we should
        // drop the event.
        if block.number + 1 != tx_execution_id.block_number {
            warn!(
                target = "engine",
                tx_hash = %tx_execution_id.tx_hash,
                blockenv_block_number = block.number,
                tx_execution_id = %tx_execution_id.to_json_string(),
                "Requested reorg block number does not match block number currently built in engine!"
            );

            return Ok(());
        }

        let current_block_iteration = self
            .current_block_iterations
            .get_mut(&tx_execution_id.as_block_execution_id())
            .ok_or(EngineError::MissingCurrentBlockData)?;

        // Check if we have received a transaction at all
        if let Some((last_execution_id, _)) = current_block_iteration.last_executed_tx.current()
            && tx_execution_id == *last_execution_id
        {
            info!(
                target = "engine",
                tx_execution_id = %tx_execution_id.to_json_string(),
                "Executing reorg for hash"
            );

            // Remove the last transaction from buffer, preserving the previous one if it exists
            current_block_iteration.last_executed_tx.remove_last();

            // Remove transaction from results
            self.transaction_results
                .remove_transaction_result(tx_execution_id);

            // Only decrement the counter if we haven't processed a new block yet
            if current_block_iteration.n_transactions > 0 {
                current_block_iteration.n_transactions -= 1;
            }

            return Ok(());
        }

        error!(
            target = "engine",
            tx_execution_id = %tx_execution_id.to_json_string(),
            current_block_iteration_tx_hash = ?current_block_iteration.last_executed_tx.current().map(|(tx_id, _)| tx_id.tx_hash),
            current_block_iteration_tx_block_number = current_block_iteration.last_executed_tx.current().map(|(tx_id, _)| tx_id.block_number),
            "Reorg not found"
        );

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

impl<DbErr> From<&TxExecutionError<DbErr>> for ErrorRecoverability
where
    DbErr: Debug,
{
    fn from(error: &TxExecutionError<DbErr>) -> Self {
        match error {
            TxExecutionError::TxEvmError(e) => e.into(),
            TxExecutionError::CallTracerError(_) => ErrorRecoverability::Recoverable,
        }
    }
}

impl<ActiveDbErr, ExtDbErr> From<&ExecutorError<ActiveDbErr, ExtDbErr>> for ErrorRecoverability
where
    ActiveDbErr: Debug,
    ExtDbErr: Debug,
{
    fn from(error: &ExecutorError<ActiveDbErr, ExtDbErr>) -> Self {
        match error {
            ExecutorError::ForkTxExecutionError(e) => e.into(), // ← This calls TxExecutionError::into()
            ExecutorError::AssertionExecutionError(..) => ErrorRecoverability::Unrecoverable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::TestDbError;
    use assertion_executor::{
        ExecutorConfig,
        primitives::AccountInfo,
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

    async fn create_test_engine_with_timeout(
        timeout: Duration,
    ) -> (
        CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
        crossbeam::channel::Sender<TxQueueContents>,
    ) {
        let (tx_sender, tx_receiver) = crossbeam::channel::unbounded();
        let underlying_db = CacheDB::new(EmptyDBTyped::default());
        let state = OverlayDb::new(Some(std::sync::Arc::new(underlying_db)));
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
            timeout,
            timeout / 2, // We divide by 2 to ensure we read the cache status before we timeout
            false,
            #[cfg(feature = "cache_validation")]
            None,
        )
        .await;
        (engine, tx_sender)
    }

    async fn create_test_engine() -> (
        CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
        crossbeam::channel::Sender<TxQueueContents>,
    ) {
        create_test_engine_with_timeout(Duration::from_millis(100)).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_core_engine_errors_when_no_synced_sources() {
        let (mut engine, tx_sender) =
            create_test_engine_with_timeout(Duration::from_millis(10)).await;

        let engine_handle = tokio::spawn(async move { engine.run().await });

        let queue_block_env = queue::QueueBlockEnv {
            block_env: BlockEnv::default(),
            last_tx_hash: None,
            n_transactions: 0,
            selected_iteration_id: Some(0),
        };
        let queue_tx = queue::QueueTransaction {
            tx_execution_id: TxExecutionId::new(1, 0, B256::from([0x11; 32])),
            tx_env: TxEnv::default(),
        };

        tx_sender
            .send(TxQueueContents::Block(
                queue_block_env,
                tracing::Span::none(),
            ))
            .expect("queue send should succeed");
        tx_sender
            .send(TxQueueContents::Tx(queue_tx, tracing::Span::none()))
            .expect("queue send should succeed");

        let result = engine_handle.await.expect("engine task should not panic");
        assert!(
            matches!(result, Err(EngineError::NoSyncedSources)),
            "expected NoSyncedSources error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_tx_block_mismatch_yields_validation_error() {
        let (mut engine, _) = create_test_engine().await;
        let block_env = create_test_block_env();
        let expected_block_number = block_env.number + 1;
        let mismatched_block_number = block_env.number;
        engine.block_env = Some(block_env);

        let tx_execution_id =
            TxExecutionId::new(mismatched_block_number, 0, B256::from([0x42; 32]));
        let queue_transaction = queue::QueueTransaction {
            tx_execution_id,
            tx_env: TxEnv::default(),
        };

        let result = engine.process_transaction_event(queue_transaction).await;
        assert!(
            result.is_ok(),
            "mismatched block number should not return an engine error, got {result:?}"
        );

        let stored_result = engine
            .get_transaction_result_cloned(&tx_execution_id)
            .expect("validation error should be recorded");
        match stored_result {
            TransactionResult::ValidationError(message) => {
                assert!(
                    message.contains(&mismatched_block_number.to_string()),
                    "error message should mention the transaction block number"
                );
                assert!(
                    message.contains(&expected_block_number.to_string()),
                    "error message should mention the expected block number"
                );
            }
            TransactionResult::ValidationCompleted { .. } => {
                panic!("expected validation error, found ValidationCompleted")
            }
        }

        assert!(
            engine.current_block_iterations.is_empty(),
            "transaction with mismatched block number should not create block iteration state"
        );
    }

    #[test]
    fn test_last_executed_tx_single_push_and_pop() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x11; 32]);
        let id1 = TxExecutionId::from_hash(h1);
        let s1: EvmState = EvmState::default();

        txs.push(id1, Some(s1));

        let cur = txs.current().expect("should contain the pushed tx");
        assert_eq!(cur.0, id1, "current should be the last pushed hash");

        let popped = txs.remove_last().expect("should pop the only element");
        assert_eq!(popped.0, id1, "popped should be the same hash");
        assert!(txs.current().is_none(), "should be empty after pop");
    }

    #[test]
    fn test_last_executed_tx_two_elements_lifo() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x21; 32]);
        let h2 = B256::from([0x22; 32]);
        let id1 = TxExecutionId::from_hash(h1);
        let id2 = TxExecutionId::from_hash(h2);
        txs.push(id1, Some(EvmState::default()));
        txs.push(id2, Some(EvmState::default()));

        // LIFO: current is h2
        assert_eq!(txs.current().unwrap().0, id2);
        // Pop h2, current becomes h1
        assert_eq!(txs.remove_last().unwrap().0, id2);
        assert_eq!(txs.current().unwrap().0, id1);
        // Pop h1, now empty
        assert_eq!(txs.remove_last().unwrap().0, id1);
        assert!(txs.current().is_none());
    }

    #[test]
    fn test_last_executed_tx_overflow_discards_oldest() {
        let mut txs = LastExecutedTx::new();
        let h1 = B256::from([0x31; 32]);
        let h2 = B256::from([0x32; 32]);
        let h3 = B256::from([0x33; 32]);
        let id1 = TxExecutionId::from_hash(h1);
        let id2 = TxExecutionId::from_hash(h2);
        let id3 = TxExecutionId::from_hash(h3);

        // Fill to capacity
        txs.push(id1, Some(EvmState::default()));
        txs.push(id2, Some(EvmState::default()));
        assert_eq!(txs.current().unwrap().0, id2);

        // Push over capacity; should drop h1 and keep [h2, h3]
        txs.push(id3, Some(EvmState::default()));
        assert_eq!(
            txs.current().unwrap().0,
            id3,
            "current should be newest after overflow"
        );

        // Removing last returns h3, and now current should be h2 (h1 was discarded)
        assert_eq!(txs.remove_last().unwrap().0, id3);
        assert_eq!(
            txs.current().unwrap().0,
            id2,
            "previous should be preserved after pop"
        );

        // Removing last again returns h2 and leaves empty
        assert_eq!(txs.remove_last().unwrap().0, id2);
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

        // If the response is successful, it means the engine answered before it was taken down
        if let Ok(rax) = rax
            && let Ok(successful) = instance.is_transaction_successful(&rax).await
        {
            assert!(
                !successful,
                "Transaction should not be successful before blockenv"
            );
        }
        // If the response is not successful, the test passes since the engine is down due to the error
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
            instance
                .send_reorg(TxExecutionId::from_hash(B256::random()))
                .await
                .is_err(),
            "not an error, core engine should have exited!"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_before_blockenv_rejected(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send reorg without any prior blockenv or transaction
        assert!(
            instance
                .send_reorg(TxExecutionId::from_hash(B256::random()))
                .await
                .is_err(),
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
            instance
                .send_reorg(TxExecutionId::from_hash(B256::random()))
                .await
                .is_err(),
            "Reorg after blockenv but before any tx should be rejected"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_valid_then_previous_rejected(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Execute two successful transactions
        let mut tx1 = instance
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
        tx1.block_number += 1;
        assert!(
            instance.send_reorg(tx1).await.is_err(),
            "Reorg with wrong hash should be rejected and exit engine"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_core_engine_reorg_followed_by_blockenv_with_last_tx_hash(
        mut instance: crate::utils::LocalInstance,
    ) {
        let initial_cache_resets = instance.cache_reset_count();

        // Start by sending a block environment so subsequent dry transactions share the same block.
        instance
            .new_block()
            .await
            .expect("initial blockenv should be accepted");

        // Send two transactions without new blockenvs so they belong to the same block.
        let tx1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .expect("first tx should be sent successfully");
        assert!(
            instance
                .is_transaction_successful(&tx1)
                .await
                .expect("first tx should be processed successfully"),
            "First transaction should execute successfully"
        );

        let tx2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .expect("second tx should be sent successfully");
        assert!(
            instance
                .is_transaction_successful(&tx2)
                .await
                .expect("second tx should be processed successfully"),
            "Second transaction should execute successfully"
        );

        // Reorg the second transaction; this should succeed and remove it from the buffer.
        instance
            .send_reorg(tx2)
            .await
            .expect("reorg of the last tx should succeed");

        // Sending a new blockenv should reference tx1 as the last transaction hash and succeed.
        //
        // last_tx_hash are accounted for inside of the mock transports.
        instance
            .new_block()
            .await
            .expect("blockenv referencing the remaining tx should be accepted");

        // Engine should still accept new transactions after the blockenv.
        let tx3 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .expect("engine should accept new tx after blockenv");
        assert!(
            instance
                .is_transaction_successful(&tx3)
                .await
                .expect("third tx should be processed successfully"),
            "Engine should remain healthy after processing the blockenv"
        );

        instance
            .wait_for_processing(Duration::from_millis(25))
            .await;
        assert_eq!(
            instance.cache_reset_count(),
            initial_cache_resets,
            "Cache should remain valid throughout this scenario"
        );
    }

    #[allow(clippy::too_many_lines)]
    #[tokio::test]
    async fn test_database_commit_verification() {
        use revm::primitives::address;

        let (mut engine, _) = create_test_engine().await;
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
        let tx_execution_id = TxExecutionId::from_hash(tx_hash);

        // Get initial cache state
        let initial_cache_count = engine.get_state().cache_entry_count();

        engine.block_env = Some(block_env);

        let current_block_iteration_id = BlockIterationData {
            fork_db: engine.state.fork(),
            n_transactions: 0,
            last_executed_tx: LastExecutedTx::new(),
        };

        // Execute the transaction
        let tx_execution_id = TxExecutionId::from_hash(tx_hash);
        engine
            .current_block_iterations
            .entry(tx_execution_id.as_block_execution_id())
            .or_insert(current_block_iteration_id);

        let result = engine.execute_transaction(tx_execution_id, &tx_env);
        assert!(result.is_ok(), "Transaction should execute successfully");
        // We now need to advance the state by one block so we commit the transaction state
        engine
            .apply_state_buffer(tx_execution_id.as_block_execution_id())
            .unwrap();

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
        let tx_result = engine.get_transaction_result_cloned(&tx_execution_id);
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

    #[tokio::test]
    async fn test_engine_requires_block_env_before_tx() {
        let (mut engine, _) = create_test_engine().await;
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
        let tx_execution_id = TxExecutionId::from_hash(tx_hash);

        // Execute transaction without block environment
        let result = engine.execute_transaction(tx_execution_id, &tx_env);

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

    #[crate::utils::engine_test(all)]
    async fn test_block_env_wrong_transaction_number(mut instance: crate::utils::LocalInstance) {
        // Send and verify a reverting CREATE transaction
        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        instance.transport.set_n_transactions(2);

        // Send a blockEnv with the wrong number of transactions
        instance
            .expect_cache_flush(|instance| {
                Box::pin(async move {
                    instance.new_block().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let tx_hash = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx_hash).await.unwrap(),
            "Transaction should execute successfully and pass assertions"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_block_env_wrong_last_tx_hash(mut instance: crate::utils::LocalInstance) {
        tracing::info!("test_block_env_wrong_last_tx_hash: sending first tx");
        // Send and verify a reverting CREATE transaction
        let tx_execution_id_1 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        tracing::info!("test_block_env_wrong_last_tx_hash: sending second tx (dry)");
        // Send and verify a reverting CREATE transaction
        let tx_execution_id_2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id_1)
                .await
                .unwrap(),
            "Transaction should execute successfully and pass assertions"
        );
        assert!(
            instance
                .is_transaction_successful(&tx_execution_id_2)
                .await
                .unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        tracing::info!("test_block_env_wrong_last_tx_hash: overriding last tx hash");
        instance
            .transport
            .set_last_tx_hash(Some(tx_execution_id_1.tx_hash));

        assert!(
            instance
                .sequencer_http_mock
                .eth_balance_counter
                .get(&instance.default_account)
                .is_none()
        );
        assert!(
            instance
                .besu_client_http_mock
                .eth_balance_counter
                .get(&instance.default_account)
                .is_none()
        );

        // Send a blockEnv with the wrong last tx hash
        tracing::info!("test_block_env_wrong_last_tx_hash: forcing block env flush");
        instance
            .expect_cache_flush(|instance| {
                Box::pin(async move {
                    tracing::info!("test_block_env_wrong_last_tx_hash: calling new_block inside expect_cache_flush");
                    instance.new_block().await?;
                    tracing::info!("test_block_env_wrong_last_tx_hash: new_block inside expect_cache_flush finished");
                    Ok(())
                })
            })
            .await
            .unwrap();

        // Send and verify a successful CREATE transaction
        tracing::info!("test_block_env_wrong_last_tx_hash: sending post-flush tx");
        let tx_execution_id = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id)
                .await
                .unwrap(),
            "Transaction should execute successfully and pass assertions"
        );
        tracing::info!("test_block_env_wrong_last_tx_hash: test completed");
    }

    #[crate::utils::engine_test(all)]
    async fn test_all_tx_types(mut instance: crate::utils::LocalInstance) {
        instance.send_all_tx_types().await.unwrap();
    }

    #[crate::utils::engine_test(http)]
    async fn test_block_env_transaction_number_greater_than_zero_and_no_last_tx_hash(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send and verify a reverting CREATE transaction
        let tx_execution_id = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id)
                .await
                .unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        instance.transport.set_last_tx_hash(None);

        // Send a blockEnv with the wrong number of transactions
        let res = instance.new_block().await;

        assert!(res.is_err());
    }

    #[crate::utils::engine_test(http)]
    async fn test_block_env_transaction_number_zero_and_last_tx_hash(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Send and verify a reverting CREATE transaction
        let tx_execution_id = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_execution_id)
                .await
                .unwrap(),
            "Transaction should execute successfully and pass assertions"
        );

        instance
            .transport
            .set_last_tx_hash(Some(tx_execution_id.tx_hash));
        instance.transport.set_n_transactions(0);

        // Send a blockEnv with the wrong number of transactions
        let res = instance.new_block().await;

        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_failed_transaction_commit() {
        let (mut engine, _) = create_test_engine().await;
        let tx_hash = B256::from([0x44; 32]);

        let block_execution_id = BlockExecutionId {
            block_number: 1,
            iteration_id: 1,
        };

        let tx_execution_id = TxExecutionId {
            block_number: 1,
            iteration_id: 1,
            tx_hash,
        };
        let mut last_executed_tx = LastExecutedTx::new();
        last_executed_tx.push(tx_execution_id, None);

        let current_block_iteration_id = BlockIterationData {
            fork_db: engine.state.fork(),
            n_transactions: 0,
            last_executed_tx,
        };

        engine
            .current_block_iterations
            .entry(block_execution_id)
            .or_insert(current_block_iteration_id);

        let result = engine.apply_state_buffer(block_execution_id);
        assert!(matches!(result, Err(EngineError::NothingToCommit)));
    }

    #[crate::utils::engine_test(all)]
    async fn test_multiple_iterations_winner_selected(mut instance: crate::utils::LocalInstance) {
        info!("Testing multiple iterations with winner selection");
        instance.new_block().await.unwrap();

        // Send transactions with different iteration IDs in Block 1
        instance.set_current_iteration_id(1);
        let tx_block1_iter1 = instance
            .send_successful_create_tx_dry(uint!(100_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let tx_block1_iter2 = instance
            .send_successful_create_tx_dry(uint!(200_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(3);
        let tx_block1_iter3 = instance
            .send_successful_create_tx_dry(uint!(300_U256), Bytes::new())
            .await
            .unwrap();

        // All transactions should be processed
        assert!(
            instance
                .is_transaction_successful(&tx_block1_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx_block1_iter2)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx_block1_iter3)
                .await
                .unwrap()
        );

        // Block 2: Select iteration 2 as the winner from Block 1
        instance.set_current_iteration_id(2);
        instance.new_block().await.unwrap();

        // Send a transaction to verify state was committed correctly
        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Transaction in Block 2 should succeed after iteration 2 was selected"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_iteration_selection_commits_only_winner(
        mut instance: crate::utils::LocalInstance,
    ) {
        info!("Testing that only winning iteration is committed to state");

        let initial_cache_count = instance.cache_reset_count();

        // Block 1
        instance.new_block().await.unwrap();

        // Iteration 1: Create a contract at value 100
        instance.set_current_iteration_id(1);
        let tx_iter1 = instance
            .send_successful_create_tx_dry(uint!(100_U256), Bytes::new())
            .await
            .unwrap();

        // Iteration 2: Create a contract at value 200
        instance.set_current_iteration_id(2);
        let tx_iter2 = instance
            .send_successful_create_tx_dry(uint!(200_U256), Bytes::new())
            .await
            .unwrap();

        assert!(instance.is_transaction_successful(&tx_iter1).await.unwrap());
        assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

        // Block 2: Select iteration 1 as the winner
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        // The state should reflect iteration 1's changes, not iteration 2's
        let tx_verify = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_verify)
                .await
                .unwrap()
        );

        // No cache flush should occur
        instance
            .wait_for_processing(Duration::from_millis(25))
            .await;
        assert_eq!(
            instance.cache_reset_count(),
            initial_cache_count,
            "Cache should not flush when correct iteration is selected"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_wrong_iteration_selected_triggers_flush(
        mut instance: crate::utils::LocalInstance,
    ) {
        info!("Testing that selecting wrong iteration triggers cache flush");

        // Block 1
        instance.new_block().await.unwrap();

        // Send transaction in iteration 1
        instance.set_current_iteration_id(1);
        let tx_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Send transaction in iteration 2
        instance.set_current_iteration_id(2);
        let tx_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(instance.is_transaction_successful(&tx_iter1).await.unwrap());
        assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

        // Block 2: Select iteration 3 (which doesn't exist)
        instance.set_current_iteration_id(3);
        instance.transport.set_last_tx_hash(Some(B256::random())); // Wrong hash
        instance.transport.set_n_transactions(1);

        instance
            .expect_cache_flush(|instance| {
                Box::pin(async move {
                    instance.new_block().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        // Engine should continue working
        let tx_after_flush = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_after_flush)
                .await
                .unwrap(),
            "Engine should work after cache flush"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_multiple_transactions_same_iteration(mut instance: crate::utils::LocalInstance) {
        info!("Testing multiple transactions in the same iteration");

        // Block 1
        instance.new_block().await.unwrap();

        // Send multiple transactions all in iteration 2
        instance.set_current_iteration_id(2);
        let tx1_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        let tx2_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        let tx3_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // All should succeed
        assert!(
            instance
                .is_transaction_successful(&tx1_iter2)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx2_iter2)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx3_iter2)
                .await
                .unwrap()
        );

        // Block 2: Select iteration 2 with all 3 transactions
        instance.set_current_iteration_id(2);
        instance.new_block().await.unwrap();

        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Should commit all 3 transactions from iteration 2"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_iteration_selection_with_transaction_count_mismatch(
        mut instance: crate::utils::LocalInstance,
    ) {
        info!("Testing iteration selection with wrong transaction count");

        // Block 1
        instance.new_block().await.unwrap();

        // Iteration 1: 2 transactions
        instance.set_current_iteration_id(1);
        let tx1_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        let tx2_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx1_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx2_iter1)
                .await
                .unwrap()
        );

        // Block 2: Select iteration 1 but set the wrong count (3 instead of 2)
        instance.set_current_iteration_id(1);
        instance.transport.set_n_transactions(3);

        instance
            .expect_cache_flush(|instance| {
                Box::pin(async move {
                    instance.new_block().await?;
                    Ok(())
                })
            })
            .await
            .unwrap();
    }

    #[crate::utils::engine_test(all)]
    async fn test_reorg_in_specific_iteration_before_selection(
        mut instance: crate::utils::LocalInstance,
    ) {
        info!("Testing reorg within iteration before block selection");

        // Block 1
        instance.new_block().await.unwrap();

        // Iteration 1: Send 2 transactions
        instance.set_current_iteration_id(1);
        let tx1_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        let tx2_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Iteration 2: Send 1 transaction
        instance.set_current_iteration_id(2);
        let tx1_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx1_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx2_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx1_iter2)
                .await
                .unwrap()
        );

        // Reorg last transaction in iteration 1
        instance.set_current_iteration_id(1);
        instance.send_reorg(tx2_iter1).await.unwrap();

        // Block 2: Select iteration 1 with only 1 transaction (after reorg)
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Should work after reorg and correct iteration selection"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_empty_iteration_selected(mut instance: crate::utils::LocalInstance) {
        info!("Testing selection of empty iteration (no transactions)");

        // Block 1
        instance.new_block().await.unwrap();

        // Only send transactions in iteration 2
        instance.set_current_iteration_id(2);
        let tx_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

        // Block 2: Select iteration 1 which has no transactions
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        // Should still work
        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Should work when empty iteration is selected"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_interleaved_iterations(mut instance: crate::utils::LocalInstance) {
        info!("Testing interleaved iteration IDs");

        // Block 1
        instance.new_block().await.unwrap();

        // Send transactions with alternating iteration IDs
        instance.set_current_iteration_id(1);
        let tx1_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let tx1_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(1);
        let tx2_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let tx2_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // All should succeed
        assert!(
            instance
                .is_transaction_successful(&tx1_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx1_iter2)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx2_iter1)
                .await
                .unwrap()
        );
        assert!(
            instance
                .is_transaction_successful(&tx2_iter2)
                .await
                .unwrap()
        );

        // Block 2: Select iteration 1 (should have 2 transactions)
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Should correctly handle interleaved iterations"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_multiple_blocks_multiple_iterations(mut instance: crate::utils::LocalInstance) {
        info!("Testing multiple blocks each with multiple iterations");

        // Block 1
        instance.new_block().await.unwrap();

        instance.set_current_iteration_id(1);
        let b1_tx_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let b1_tx_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Block 2: Select iteration 2 from Block 1
        instance.set_current_iteration_id(2);
        instance.new_block().await.unwrap();

        instance.set_current_iteration_id(1);
        let b2_tx_iter1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let b2_tx_iter2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Block 3: Select iteration 1 from Block 2
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let tx_block3 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block3)
                .await
                .unwrap(),
            "Should handle multiple blocks with different iteration selections"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_cache_miss_with_iteration_selection(mut instance: crate::utils::LocalInstance) {
        info!("Testing cache miss behavior with iteration selection");

        // Block 1
        instance.new_block().await.unwrap();

        // Iteration 1: Trigger cache miss
        instance.set_current_iteration_id(1);
        let (caller1, tx1_iter1) = instance.send_create_tx_with_cache_miss().await.unwrap();

        // Iteration 2: Trigger another cache miss
        instance.set_current_iteration_id(2);
        let (caller2, tx1_iter2) = instance.send_create_tx_with_cache_miss().await.unwrap();

        instance
            .wait_for_processing(Duration::from_millis(50))
            .await;

        assert!(instance.get_transaction_result(&tx1_iter1).is_some());
        assert!(instance.get_transaction_result(&tx1_iter2).is_some());

        // Block 2: Select iteration 1
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let tx_block2 = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&tx_block2)
                .await
                .unwrap(),
            "Should handle cache misses with iteration selection"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_switching_winning_iterations_across_blocks(
        mut instance: crate::utils::LocalInstance,
    ) {
        info!("Testing switching winning iterations across multiple blocks");

        // Block 1
        instance.new_block().await.unwrap();

        instance.set_current_iteration_id(1);
        let b1_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let b1_i2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Block 2: Select iteration 1
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        instance.set_current_iteration_id(1);
        let b2_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let b2_i2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Block 3: Switch to iteration 2
        instance.set_current_iteration_id(2);
        instance.new_block().await.unwrap();

        instance.set_current_iteration_id(1);
        let b3_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        instance.set_current_iteration_id(2);
        let b3_i2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Block 4: Switch back to iteration 1
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let final_tx = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&final_tx).await.unwrap(),
            "Should handle switching winning iterations across blocks"
        );
    }

    #[crate::utils::engine_test(all)]
    async fn test_reorg_affects_only_target_iteration(mut instance: crate::utils::LocalInstance) {
        info!("Testing that reorg only affects the target iteration");

        // Block 1
        instance.new_block().await.unwrap();

        // Iteration 1: 3 transactions
        instance.set_current_iteration_id(1);
        let tx1_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();
        let tx2_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();
        let tx3_i1 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // Iteration 2: 2 transactions
        instance.set_current_iteration_id(2);
        let tx1_i2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();
        let tx2_i2 = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        // All should be successful
        assert!(instance.is_transaction_successful(&tx1_i1).await.unwrap());
        assert!(instance.is_transaction_successful(&tx2_i1).await.unwrap());
        assert!(instance.is_transaction_successful(&tx3_i1).await.unwrap());
        assert!(instance.is_transaction_successful(&tx1_i2).await.unwrap());
        assert!(instance.is_transaction_successful(&tx2_i2).await.unwrap());

        // Reorg last transaction in iteration 1
        instance.set_current_iteration_id(1);
        instance.send_reorg(tx3_i1).await.unwrap();

        // Verify reorg only affected iteration 1
        assert!(
            instance.is_transaction_removed(&tx3_i1).await.unwrap(),
            "Reorged transaction should be removed"
        );
        assert!(
            instance.get_transaction_result(&tx1_i1).is_some(),
            "Other transactions in iteration 1 should remain"
        );
        assert!(
            instance.get_transaction_result(&tx2_i1).is_some(),
            "Other transactions in iteration 1 should remain"
        );
        assert!(
            instance.get_transaction_result(&tx1_i2).is_some(),
            "Iteration 2 should be unaffected"
        );
        assert!(
            instance.get_transaction_result(&tx2_i2).is_some(),
            "Iteration 2 should be unaffected"
        );

        // Block 2: Select iteration 1 (now with 2 transactions)
        instance.set_current_iteration_id(1);
        instance.new_block().await.unwrap();

        let final_tx = instance
            .send_successful_create_tx(uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        assert!(instance.is_transaction_successful(&final_tx).await.unwrap());
    }

    #[crate::utils::engine_test(all)]
    async fn test_storage_slot_persistence_intra_and_cross_block(
        mut instance: crate::utils::LocalInstance,
    ) {
        // Deploy a contract that can SET and VERIFY arbitrary storage slots
        // Calldata format:
        // - Bytes [0:32]: operation (0=SET, 1=VERIFY)
        // - Bytes [32:64]: slot
        // - Bytes [64:96]: value (for SET) or expected value (for VERIFY)

        let constructor_bytecode = Bytes::from(vec![
            // Constructor: copy runtime code to memory and return it
            0x60, 0x30, // PUSH1 48 (runtime size)
            0x80, // DUP1
            0x60, 0x0b, // PUSH1 11 (runtime code starts here)
            0x60, 0x00, // PUSH1 0 (memory offset)
            0x39, // CODECOPY
            0x60, 0x00, // PUSH1 0
            0xf3, // RETURN
            // Runtime code (48 bytes):
            0x60, 0x00, // PUSH1 0
            0x35, // CALLDATALOAD (load operation)
            0x80, // DUP1
            0x15, // ISZERO (check if operation == 0)
            0x60, 0x13, // PUSH1 19 (jump to SET)
            0x57, // JUMPI
            0x60, 0x01, // PUSH1 1
            0x14, // EQ (check if operation == 1)
            0x60, 0x1c, // PUSH1 28 (jump to VERIFY)
            0x57, // JUMPI
            0x60, 0x00, // PUSH1 0
            0x60, 0x00, // PUSH1 0
            0xfd, // REVERT (invalid operation)
            // SET operation (offset 0x13)
            0x5b, // JUMPDEST
            0x60, 0x40, // PUSH1 64       ← FIXED: load value first
            0x35, // CALLDATALOAD (load value)
            0x60, 0x20, // PUSH1 32       ← FIXED: then load slot
            0x35, // CALLDATALOAD (load slot)
            0x55, // SSTORE (now: value at slot, correct!)
            0x00, // STOP
            // VERIFY operation (offset 0x1c)
            0x5b, // JUMPDEST
            0x60, 0x20, // PUSH1 32
            0x35, // CALLDATALOAD (load slot)
            0x80, // DUP1
            0x54, // SLOAD
            0x60, 0x40, // PUSH1 64
            0x35, // CALLDATALOAD (load expected value)
            0x14, // EQ
            0x60, 0x2e, // PUSH1 46 (jump to success)
            0x57, // JUMPI
            0x60, 0x00, // PUSH1 0
            0x60, 0x00, // PUSH1 0
            0xfd, // REVERT (value mismatch)
            // Success (offset 0x2e)
            0x5b, // JUMPDEST
            0x00, // STOP
        ]);

        // Helper to create SET calldata
        let set_calldata = |slot: U256, value: U256| -> Bytes {
            let mut data = Vec::with_capacity(96);
            data.extend_from_slice(&[0u8; 32]); // operation = 0 (SET)
            data.extend_from_slice(&slot.to_be_bytes::<32>());
            data.extend_from_slice(&value.to_be_bytes::<32>());
            Bytes::from(data)
        };

        // Helper to create VERIFY calldata
        let verify_calldata = |slot: U256, expected: U256| -> Bytes {
            let mut data = Vec::with_capacity(96);
            let mut op = [0u8; 32];
            op[31] = 1; // operation = 1 (VERIFY)
            data.extend_from_slice(&op);
            data.extend_from_slice(&slot.to_be_bytes::<32>());
            data.extend_from_slice(&expected.to_be_bytes::<32>());
            Bytes::from(data)
        };

        // Start Block 1
        instance.new_block().await.unwrap();

        // TX1: Deploy the storage test contract
        let deploy_tx = instance
            .send_successful_create_tx_dry(uint!(0_U256), constructor_bytecode)
            .await
            .unwrap();

        assert!(
            instance
                .is_transaction_successful(&deploy_tx)
                .await
                .unwrap(),
            "Contract deployment should succeed"
        );

        // Calculate contract address
        let sender = instance.default_account();
        let contract_address = sender.create(0);
        info!("Storage test contract deployed at: {}", contract_address);

        // TX2: Write slot 5 = 0x42
        let tx2 = instance
            .send_call_tx_dry(
                contract_address,
                uint!(0_U256),
                set_calldata(uint!(5_U256), uint!(0x42_U256)),
            )
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx2).await.unwrap(),
            "TX2: SET slot 5 = 0x42 should succeed"
        );

        // TX3: Verify slot 5 == 0x42 (intra-block read)
        let tx3 = instance
            .send_call_tx_dry(
                contract_address,
                uint!(0_U256),
                verify_calldata(uint!(5_U256), uint!(0x42_U256)),
            )
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx3).await.unwrap(),
            "TX3: VERIFY slot 5 == 0x42 should succeed (intra-block persistence)"
        );

        // TX4: Write slot 5 = 0x43
        let tx4 = instance
            .send_call_tx_dry(
                contract_address,
                uint!(0_U256),
                set_calldata(uint!(5_U256), uint!(0x43_U256)),
            )
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx4).await.unwrap(),
            "TX4: SET slot 5 = 0x43 should succeed"
        );

        // TX5: Verify slot 5 == 0x43 (intra-block read after update)
        let tx5 = instance
            .send_call_tx_dry(
                contract_address,
                uint!(0_U256),
                verify_calldata(uint!(5_U256), uint!(0x43_U256)),
            )
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx5).await.unwrap(),
            "TX5: VERIFY slot 5 == 0x43 should succeed (intra-block after update)"
        );

        info!("✓ Intra-block storage persistence verified");

        // Start Block 2 (commits Block 1 state)
        instance.new_block().await.unwrap();

        // TX6: Verify slot 5 == 0x43 (cross-block read)
        let tx6 = instance
            .send_call_tx_dry(
                contract_address,
                uint!(0_U256),
                verify_calldata(uint!(5_U256), uint!(0x43_U256)),
            )
            .await
            .unwrap();

        assert!(
            instance.is_transaction_successful(&tx6).await.unwrap(),
            "TX6: VERIFY slot 5 == 0x43 should succeed (cross-block persistence)"
        );
    }
}
