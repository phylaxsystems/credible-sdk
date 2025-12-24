//! # `engine`
//!
//! The engine is responsible for executing transactions and verifying against
//! assertions. It does this by receiving transactions over a channel and
//! executes them in order. New blocks are marked by new `BlockEnv` objects
//! being received over a channel.
//!
//! When processing a new block(by receiving a new `BlockEnv`) and executing
//! associated transactions, the engine will advance its state and verify that
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
pub mod system_calls;
#[cfg(test)]
mod tests;
mod transactions_results;

pub use transactions_results::TransactionsResults;

use self::{
    queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        TransactionQueueReceiver,
        TxQueueContents,
    },
    system_calls::{
        SpecIdExt,
        SystemCalls,
        SystemCallsConfig,
    },
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
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::{
    sync::oneshot,
    time::sleep,
};

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
    cache::Sources,
    engine::system_calls::SystemCallError,
    execution_ids::{
        BlockExecutionId,
        TxExecutionId,
    },
    utils::ErrorRecoverability,
};
use alloy::primitives::{
    TxHash,
    U256,
};
use arrayvec::ArrayVec;
use assertion_executor::{
    ExecutorError,
    ForkTxExecutionError,
    TxExecutionError,
    db::{
        Database,
        fork_db::ForkDb,
    },
};
use dashmap::DashMap;
#[cfg(feature = "cache_validation")]
use monitoring::cache::CacheChecker;
use revm::primitives::hardfork::SpecId;
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
use tokio::task::JoinHandle;
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

/// Timeout for recv - threads will check for the shutdown flag at this interval
const RECV_TIMEOUT: Duration = Duration::from_millis(100);

/// Branch prediction hint for unlikely conditions
#[inline]
#[cold]
fn unlikely(b: bool) -> bool {
    b
}

/// Contains the last two executed transaction identifiers and resulting states.
#[derive(Debug)]
struct LastExecutedTx {
    execution_results: ArrayVec<(TxExecutionId, Option<EvmState>), 2>,
}

impl LastExecutedTx {
    #[inline]
    const fn new() -> Self {
        Self {
            execution_results: ArrayVec::new_const(),
        }
    }

    #[inline]
    fn push(&mut self, tx_execution_id: TxExecutionId, state: Option<EvmState>) {
        if self.execution_results.is_full() {
            self.execution_results.remove(0);
        }
        // Safety: we just ensured there's space by removing if full
        // Using push is safe here, but we can use try_push for extra safety
        let _ = self.execution_results.try_push((tx_execution_id, state));
    }

    #[inline]
    fn remove_last(&mut self) -> Option<(TxExecutionId, Option<EvmState>)> {
        self.execution_results.pop()
    }

    #[inline]
    fn current(&self) -> Option<&(TxExecutionId, Option<EvmState>)> {
        self.execution_results.last()
    }

    #[inline]
    fn clear(&mut self) {
        self.execution_results.clear();
    }
}

#[derive(thiserror::Error, Debug, Clone)]
pub enum EngineError {
    #[error("Database error")]
    DatabaseError,
    #[error("Transaction error")]
    TransactionError,
    #[error("Iteration error")]
    IterationError,
    #[error("Reorg error")]
    ReorgError,
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
    #[error("No iteration data to commit")]
    NoIterationDataToCommit,
    #[error("Block number specified by transaction and block number currently built do not match!")]
    TxBlockMismatch,
    #[error("Shutdown signal received")]
    Shutdown,
    #[error("System call error")]
    SystemCallError(#[source] SystemCallError),
}

impl From<&EngineError> for ErrorRecoverability {
    fn from(e: &EngineError) -> Self {
        match e {
            EngineError::DatabaseError | EngineError::AssertionError => {
                ErrorRecoverability::Unrecoverable
            }
            EngineError::TransactionError
            | EngineError::ReorgError
            | EngineError::IterationError
            | EngineError::ChannelClosed
            | EngineError::GetTxResultChannelClosed
            | EngineError::NothingToCommit
            | EngineError::NoIterationDataToCommit
            | EngineError::TxBlockMismatch
            | EngineError::BadReorgHash
            | EngineError::NoSyncedSources
            | EngineError::SystemCallError(_)
            | EngineError::Shutdown => ErrorRecoverability::Recoverable,
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
    /// Iteration `BlockEnv`
    block_env: BlockEnv,
}

impl<DB> BlockIterationData<DB> {
    /// Checks if the last executed transaction matches the given transaction ID
    #[inline]
    fn has_last_tx(&self, tx_id: TxExecutionId) -> bool {
        self.last_executed_tx
            .current()
            .is_some_and(|(id, _)| *id == tx_id)
    }

    /// Gets the transaction ID of the last executed transaction, if any
    #[inline]
    fn last_tx_id(&self) -> Option<TxExecutionId> {
        self.last_executed_tx.current().map(|(id, _)| *id)
    }
}

/// The engine processes blocks and appends transactions to them.
/// It accepts transaction events sent from a transport via the `TransactionQueueReceiver`
/// and processes them accordingly.
#[derive(Debug)]
pub struct CoreEngine<DB> {
    /// In-memory `revm::Database` cache. Populated as the sidecar executes blocks.
    ///
    /// This is the state the `CoreEngine` is executing transactions against.
    cache: OverlayDb<DB>,
    /// Current block iteration data per block execution id.
    current_block_iterations: HashMap<BlockExecutionId, BlockIterationData<DB>>,
    /// Current head: last committed block.
    current_head: U256,
    /// External providers of state we use when we do not have a piece of state cached in our in memory db.
    /// External state providers implement a trait that we use to query databaseref-like data and populate `state: OverlayDb<DB>`
    /// for execution with it.
    ///
    /// Some state providers may be slow which is why we use them as a fallback for `state: OverlayDb<DB>`.
    sources: Arc<Sources>,
    /// Channel on which the core engine receives events. Events include new transactions, blocks(block envs), reorgs.
    tx_receiver: TransactionQueueReceiver,
    /// Core engines instance of the assertion executor, executes transactions and assertions
    assertion_executor: AssertionExecutor,
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
    /// System calls handler for EIP-2935 and EIP-4788.
    system_calls: SystemCalls,
    #[cfg(feature = "cache_validation")]
    processed_transactions: Arc<moka::sync::Cache<TxHash, Option<EvmState>>>,
    #[cfg(feature = "cache_validation")]
    iteration_pending_processed_transactions:
        HashMap<BlockExecutionId, HashMap<TxHash, Option<EvmState>>>,
    #[cfg(feature = "cache_validation")]
    cache_checker: Option<AbortHandle>,
    cache_metrics_handle: Option<JoinHandle<()>>,
}

#[cfg(feature = "cache_validation")]
impl<DB> Drop for CoreEngine<DB> {
    fn drop(&mut self) {
        if let Some(handle) = self.cache_checker.take() {
            handle.abort();
        }
        if let Some(handle) = self.cache_metrics_handle.take() {
            handle.abort();
        }
    }
}

impl<DB: DatabaseRef + Send + Sync + 'static> CoreEngine<DB> {
    #[allow(clippy::too_many_arguments)]
    #[instrument(name = "engine::new", skip_all, level = "debug")]
    pub async fn new(
        cache: OverlayDb<DB>,
        sources: Arc<Sources>,
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
            cache_metrics_handle: Some(cache.spawn_monitoring_thread()),
            cache,
            current_block_iterations: HashMap::new(),
            current_head: U256::ZERO,
            sources: sources.clone(),
            tx_receiver,
            assertion_executor,
            transaction_results: TransactionsResults::new(
                state_results,
                transaction_results_max_capacity,
            ),
            block_metrics: BlockMetrics::new(),
            state_sources_sync_timeout,
            check_sources_available: true,
            sources_monitoring: monitoring::sources::Sources::new(
                sources,
                source_monitoring_period,
            ),
            overlay_cache_invalidation_every_block,
            system_calls: SystemCalls::new(),
            #[cfg(feature = "cache_validation")]
            processed_transactions,
            #[cfg(feature = "cache_validation")]
            iteration_pending_processed_transactions: HashMap::new(),
            #[cfg(feature = "cache_validation")]
            cache_checker,
        }
    }

    /// Returns the current spec ID from the executor configuration.
    #[inline]
    fn get_spec_id(&self) -> SpecId {
        self.assertion_executor.config.spec_id
    }

    /// Helper function to take an errored transaction, and either:
    ///
    /// - raise its error further if necessary,
    /// - store the execution failure if the transaction itself failed.
    ///
    /// "Transction itself failing" in this contexts means
    /// there was a tx validation error like out of gas.
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
                trace!(
                    target = "engine",
                    error = ?e,
                    tx_execution_id = %tx_execution_id.to_json_string(),
                    tx_env = ?tx_env,
                    "Transaction validation failed"
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
                )?;
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
        let block_id = tx_execution_id.as_block_execution_id();

        let current_block_iteration = self
            .current_block_iterations
            .get_mut(&block_id)
            .ok_or(EngineError::TransactionError)?;

        #[cfg(feature = "cache_validation")]
        {
            self.iteration_pending_processed_transactions
                .entry(block_id)
                .or_insert_with(|| HashMap::with_capacity(16))
                .insert(tx_execution_id.tx_hash, state.clone());
        }

        current_block_iteration
            .last_executed_tx
            .push(tx_execution_id, state);
        self.transaction_results
            .add_transaction_result(tx_execution_id, result);
        Ok(())
    }

    /// Takes a transaction validation result and dispatches metrics according
    /// to it.
    fn trace_execute_transaction_result(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: &TxEnv,
        rax: &TxValidationResult,
    ) {
        let tx_hash = tx_execution_id.tx_hash;
        let is_valid = rax.is_valid();
        let execution_result = &rax.result_and_state.result;
        let status = match execution_result {
            ExecutionResult::Success { .. } => "success",
            ExecutionResult::Revert { .. } => "reverted",
            ExecutionResult::Halt { .. } => "halt",
        };

        info!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = %tx_execution_id.block_number,
            iteration_id = tx_execution_id.iteration_id,
            is_valid,
            status,
            gas_used = execution_result.gas_used(),
            "Transaction processed"
        );

        if is_valid {
            // Transaction valid, passed assertions, commit state for successful transactions
            debug!(
                target = "engine",
                tx_hash = %tx_hash,
                "Transaction does not invalidate assertions, processing result"
            );

            if execution_result.is_success() {
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

            self.block_metrics.invalidated_transactions += 1;
        }
    }

    /// Execute transaction and relted assertions with the core engines blockenv.
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
        let block_id = tx_execution_id.as_block_execution_id();

        let current_block_iteration = self
            .current_block_iterations
            .get_mut(&block_id)
            .ok_or(EngineError::TransactionError)?;
        current_block_iteration.n_transactions += 1;

        let instant = Instant::now();

        trace!(
            target = "engine",
            tx_hash = %tx_execution_id.tx_hash,
            caller = %tx_env.caller,
            gas_limit = tx_env.gas_limit,
            block_number = %current_block_iteration.block_env.number,
            "Executing transaction with environment"
        );

        // Validate transaction and run assertions
        // Execute directly on the block fork
        // Note: block_fork is ForkDb<OverlayDb<DB>>, so the error type will be
        // ExecutorError<<ForkDb<OverlayDb<DB>> as DatabaseRef>::Error>
        // which is ExecutorError<NotFoundError>
        let block_env = current_block_iteration.block_env.clone();

        let rax = self.assertion_executor.validate_transaction(
            block_env,
            tx_env,
            &mut current_block_iteration.fork_db,
            false,
        );

        let processing_duration = instant.elapsed();

        let rax = match rax {
            Ok(rax) => rax,
            Err(e) => {
                return self.process_transaction_validation_error(tx_execution_id, tx_env, &e);
            }
        };

        // Batch metric updates
        let assertions_ran = rax.total_assertion_funcs_ran();
        let assertions_gas = rax.total_assertions_gas();

        self.block_metrics.assertions_per_block += assertions_ran;
        self.block_metrics.assertion_gas_per_block += assertions_gas;
        self.block_metrics.transactions_simulated += 1;

        // Commit transaction metrics
        let mut tx_metrics = TransactionMetrics::new();
        tx_metrics.transaction_processing_duration = processing_duration;
        tx_metrics.assertions_per_transaction = assertions_ran;
        tx_metrics.assertion_gas_per_transaction = assertions_gas;
        tx_metrics.commit();

        self.trace_execute_transaction_result(tx_execution_id, tx_env, &rax);

        self.add_transaction_result(
            tx_execution_id,
            &TransactionResult::ValidationCompleted {
                is_valid: rax.is_valid(),
                execution_result: rax.result_and_state.result,
            },
            Some(rax.result_and_state.state),
        )?;

        Ok(())
    }

    /// Invalidates the state, cache and last executed tx
    fn invalidate_all(&mut self, commit_head: &CommitHead) {
        self.sources
            .reset_latest_unprocessed_block(commit_head.block_number);

        // Measure cache invalidation time and set its new min required driver height
        let instant = Instant::now();
        self.check_sources_available = true;
        self.cache.invalidate_all();
        self.current_block_iterations.clear();
        self.block_metrics
            .increment_cache_invalidation(instant.elapsed(), commit_head.block_number);
    }

    /// Checks if the cache should be cleared and clears it.
    fn check_cache(&mut self, commit_head: &CommitHead, block_execution_id: &BlockExecutionId) {
        // Single HashMap lookup instead of multiple
        let (last_tx_id, n_transactions) =
            match self.current_block_iterations.get(block_execution_id) {
                Some(data) => (data.last_tx_id(), data.n_transactions),
                None => (None, 0),
            };

        // Check cheapest conditions first
        let head_mismatch =
            self.current_head != commit_head.block_number.saturating_sub(U256::from(1));
        let tx_hash_mismatch =
            last_tx_id.is_some() && last_tx_id.map(|id| id.tx_hash) != commit_head.last_tx_hash;
        let count_mismatch = n_transactions != commit_head.n_transactions;

        trace!(
            head_mismatch = head_mismatch,
            tx_hash_mismatch = tx_hash_mismatch,
            count_mismatch = count_mismatch,
            "Checking cache conditions"
        );

        if head_mismatch {
            warn!(
                current_head = %self.current_head,
                commit_head = %commit_head.block_number,
                "CommitHead not +1 from current head"
            );
            self.invalidate_all(commit_head);
        } else if tx_hash_mismatch {
            if let Some(prev_id) = last_tx_id {
                warn!(
                    prev_tx_hash = %prev_id.tx_hash,
                    current_tx_hash = ?commit_head.last_tx_hash,
                    "Last transaction hash mismatch"
                );
            }
            self.invalidate_all(commit_head);
        } else if count_mismatch {
            warn!(
                sidecar_n_transactions = n_transactions,
                block_env_n_transactions = commit_head.n_transactions,
                "Transaction count mismatch"
            );
            self.invalidate_all(commit_head);
        }
    }

    /// Verifies that all state sources are synced, and if not stall until they are.
    ///
    /// If the sources do not become synced after a set amount of time, the function
    /// errors.
    #[cfg(test)]
    async fn verify_state_sources_synced_for_tx(&mut self) -> Result<(), EngineError> {
        const RETRY_INTERVAL: Duration = Duration::from_millis(10);

        if !self.check_sources_available {
            return Ok(());
        }
        self.check_sources_available = false;

        let start = Instant::now();
        loop {
            if self.sources.iter_synced_sources().into_iter().any(|a| {
                // For this case, the min_synced_block is the current head too, meaning that
                // the sources must be synced up to the current head
                a.is_synced(self.current_head, self.current_head)
            }) {
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

            trace!(
                target = "engine",
                waited_ms = waited.as_millis(),
                next_retry_ms = RETRY_INTERVAL.as_millis(),
                timeout_ms = self.state_sources_sync_timeout.as_millis(),
                "No synced sources, retrying"
            );
            sleep(RETRY_INTERVAL).await;
        }
    }

    /// Spawns the engine on a dedicated OS thread with blocking receive.
    #[allow(clippy::type_complexity)]
    pub fn spawn(
        self,
        shutdown: Arc<AtomicBool>,
    ) -> Result<
        (
            std::thread::JoinHandle<Result<(), EngineError>>,
            oneshot::Receiver<Result<(), EngineError>>,
        ),
        std::io::Error,
    > {
        let (tx, rx) = oneshot::channel();

        let handle = std::thread::Builder::new()
            .name("sidecar-engine".into())
            .spawn(move || {
                let mut engine = self;
                let result = engine.run_blocking(shutdown);
                // Notify that we're exiting (ignore send error if receiver dropped)
                let _ = tx.send(result.clone());
                result
            })?;

        Ok((handle, rx))
    }

    /// Blocking run loop with shutdown support
    #[allow(clippy::needless_pass_by_value)]
    fn run_blocking(&mut self, shutdown: Arc<AtomicBool>) -> Result<(), EngineError> {
        let mut processed_blocks = 0u64;
        let mut block_processing_time = Instant::now();
        let mut idle_start = Instant::now();

        loop {
            // Check for the shutdown flag
            if unlikely(shutdown.load(Ordering::Acquire)) {
                info!(target = "engine", "Shutdown signal received");
                return Ok(());
            }

            // Use recv_timeout so we can periodically check the shutdown flag
            let event = match self.tx_receiver.recv_timeout(RECV_TIMEOUT) {
                Ok(event) => {
                    self.block_metrics.idle_time += idle_start.elapsed();
                    event
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // No event, loop back and check for the shutdown flag
                    continue;
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    info!(target = "engine", "Channel disconnected");
                    return Err(EngineError::ChannelClosed);
                }
            };

            // Check shutdown before processing (in case of long processing)
            if unlikely(shutdown.load(Ordering::Acquire)) {
                info!(target = "engine", "Shutdown signal received");
                return Ok(());
            }

            let event_start = Instant::now();

            let result = match event {
                TxQueueContents::NewIteration(new_iteration, current_span) => {
                    let _guard = current_span.enter();
                    self.process_iteration(&new_iteration)
                }
                TxQueueContents::CommitHead(commit_head, current_span) => {
                    let _guard = current_span.enter();
                    self.process_commit_head(
                        &commit_head,
                        &mut processed_blocks,
                        &mut block_processing_time,
                    )
                }
                TxQueueContents::Tx(queue_transaction, current_span) => {
                    let _guard = current_span.enter();
                    self.verify_state_sources_synced_blocking(&shutdown)
                        .and_then(|()| self.process_transaction_event(queue_transaction))
                }
                TxQueueContents::Reorg(tx_execution_id, current_span) => {
                    let _guard = current_span.enter();
                    self.execute_reorg(tx_execution_id)
                }
            };

            if let Err(error) = result {
                match ErrorRecoverability::from(&error) {
                    ErrorRecoverability::Recoverable => {
                        warn!(
                            target = "engine",
                            error = ?error,
                            "Recoverable error occurred during event processing, continuing"
                        );
                        self.cache.invalidate_all();
                        self.sources
                            .reset_latest_unprocessed_block(self.current_head);
                    }
                    ErrorRecoverability::Unrecoverable => {
                        critical!(
                            error = ?error,
                            "Unrecoverable error occurred, stopping engine"
                        );
                        return Err(error);
                    }
                }
            }

            self.block_metrics.event_processing_time += event_start.elapsed();
            idle_start = Instant::now();
        }
    }

    /// Blocking version of state source sync check with shutdown support
    fn verify_state_sources_synced_blocking(
        &mut self,
        shutdown: &AtomicBool,
    ) -> Result<(), EngineError> {
        const RETRY_INTERVAL: Duration = Duration::from_millis(10);

        if !self.check_sources_available {
            return Ok(());
        }
        self.check_sources_available = false;

        let start = Instant::now();
        loop {
            // Check shutdown
            if unlikely(shutdown.load(Ordering::Acquire)) {
                return Err(EngineError::Shutdown);
            }

            if self
                .sources
                .iter_synced_sources()
                .into_iter()
                .any(|a| a.is_synced(self.current_head, self.current_head))
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

            std::thread::sleep(RETRY_INTERVAL);
        }
    }

    /// Run the engine and process transactions and blocks received
    /// via the transaction queue.
    #[cfg(test)]
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
                Err(flume::TryRecvError::Empty) => {
                    // Channel is empty, yield to allow other tasks to run
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    error!(target = "engine", "Transaction queue channel disconnected");
                    return Err(EngineError::ChannelClosed);
                }
            };

            // Track event processing time
            let event_start = Instant::now();

            // Process event and handle errors appropriately
            let result = match event {
                TxQueueContents::NewIteration(new_iteration, current_span) => {
                    let _guard = current_span.enter();
                    self.process_iteration(&new_iteration)
                }
                TxQueueContents::CommitHead(commit_head, current_span) => {
                    let _guard = current_span.enter();
                    self.process_commit_head(
                        &commit_head,
                        &mut processed_blocks,
                        &mut block_processing_time,
                    )
                }
                TxQueueContents::Tx(queue_transaction, current_span) => {
                    let _guard = current_span.enter();
                    // Await the async verification before processing
                    match self.verify_state_sources_synced_for_tx().await {
                        Ok(()) => self.process_transaction_event(queue_transaction),
                        Err(e) => Err(e),
                    }
                }
                TxQueueContents::Reorg(tx_execution_id, current_span) => {
                    let _guard = current_span.enter();
                    self.execute_reorg(tx_execution_id)
                }
            };

            // Handle the result of event processing
            if let Err(error) = result {
                match ErrorRecoverability::from(&error) {
                    ErrorRecoverability::Recoverable => {
                        // Log the error and continue processing
                        warn!(
                            target = "engine",
                            error = ?error,
                            "Recoverable error occurred during event processing, continuing"
                        );
                        // Invalid the cache and reset the latest unprocessed block
                        self.cache.invalidate_all();
                        self.sources
                            .reset_latest_unprocessed_block(self.current_head);
                        self.current_block_iterations.clear();
                    }
                    ErrorRecoverability::Unrecoverable => {
                        // Log the critical error and break the loop
                        critical!(
                            error = ?error,
                            "Unrecoverable error occurred, stopping engine"
                        );
                        return Err(error);
                    }
                }
            }

            // Accumulate event processing time
            self.block_metrics.event_processing_time += event_start.elapsed();

            // Reset idle timer after processing event
            idle_start = Instant::now();
        }
    }

    /// Create a new block iteration for the current block.
    ///
    /// Iterations are *sub-blocks* that are built sequentially under one slot.
    /// The rationale is that when its time to commit a block to a slot an
    /// iteration is selected and its state committed.
    ///
    /// This selection is done entirely by the driver of the sidecar.
    #[instrument(name = "engine::process_iteration", skip_all, level = "info")]
    fn process_iteration(&mut self, new_iteration: &NewIteration) -> Result<(), EngineError> {
        let block_env = &new_iteration.block_env;

        info!(
            target = "engine",
            queue_iteration = ?new_iteration,
            number = %block_env.number,
            "Processing a new iteration",
        );

        // Checks if the received iteration is sequential to the current head, otherwise we should
        // drop the event.
        let expected_block_number = self.current_head + U256::from(1);
        if expected_block_number != block_env.number {
            warn!(
                target = "engine",
                current_head = %self.current_head,
                block_env = ?block_env,
                iteration_id = %new_iteration.iteration_id,
                "Iteration block number does not match block number currently built in engine!"
            );
            return Err(EngineError::IterationError);
        }

        let block_execution_id = BlockExecutionId::from(new_iteration);

        let block_iteration_data = BlockIterationData {
            fork_db: self.cache.fork(),
            n_transactions: 0,
            last_executed_tx: LastExecutedTx::new(),
            block_env: block_env.clone(),
        };

        // NOTE: If the sidecar receives the same iteration twice, the last one prevails
        self.current_block_iterations
            .insert(block_execution_id, block_iteration_data);

        debug!(
            target = "engine",
            timestamp = %block_env.timestamp,
            number = %block_env.number,
            gas_limit = block_env.gas_limit,
            base_fee = ?block_env.basefee,
            iteration_id = block_execution_id.iteration_id,
            "Iteration successfully created"
        );

        Ok(())
    }

    /// Commits an iteration as the chain head and mark the start of a new block.
    ///
    /// A `CommitHead` event must be processed before we can accept an iteration
    /// for a new block. This is so we know what/if we need to commit a specific
    /// iteration as the canonical head of the chain.
    #[instrument(name = "engine::process_commit_head", skip_all, level = "info")]
    fn process_commit_head(
        &mut self,
        commit_head: &CommitHead,
        processed_blocks: &mut u64,
        block_processing_time: &mut Instant,
    ) -> Result<(), EngineError> {
        info!(
            target = "engine",
            commit_head = ?commit_head,
            processed_blocks = *processed_blocks,
            has_block_hash = commit_head.block_hash.is_some(),
            has_parent_beacon_block_root = commit_head.parent_beacon_block_root.is_some(),
            "Processing CommitHead",
        );

        let block_execution_id = BlockExecutionId::from(commit_head);

        // If it is configured to invalidate the cache every block, do so
        if self.overlay_cache_invalidation_every_block {
            self.invalidate_all(commit_head);
        } else {
            // If not, check if the cache should be invalidated.
            self.check_cache(commit_head, &block_execution_id);
        }

        let spec_id = self.get_spec_id();
        let system_calls = self.system_calls.clone();

        // Finalize the previous block by committing its fork to the underlying state
        // Apply the last transaction's state to the current block fork
        if self
            .current_block_iterations
            .contains_key(&block_execution_id)
        {
            // Finalize the previous block by committing its fork to the underlying state
            self.apply_state_buffer_to_fork(block_execution_id)?;

            self.finalize_previous_block(block_execution_id);
        }

        // Apply EIP-2935 and EIP-4788 system calls before finalizing
        let config = SystemCallsConfig::new(
            spec_id,
            commit_head.block_number,
            commit_head.timestamp,
            commit_head.block_hash,
            commit_head.parent_beacon_block_root,
        );

        system_calls
            .apply_system_calls(&config, &mut self.cache)
            .map_err(EngineError::SystemCallError)?;

        *processed_blocks += 1;

        #[cfg(feature = "cache_validation")]
        {
            // Use remove to avoid clone
            if let Some(iteration) = self
                .iteration_pending_processed_transactions
                .remove(&block_execution_id)
            {
                for (tx_hash, state) in iteration {
                    self.processed_transactions.insert(tx_hash, state);
                }
            }
            self.iteration_pending_processed_transactions.clear();
        }

        // Set the block number to the latest applied head
        self.sources.set_block_number(commit_head.block_number);
        self.current_head = commit_head.block_number;

        self.block_metrics.block_processing_duration = block_processing_time.elapsed();
        self.block_metrics.current_height = commit_head.block_number;
        // Commit all values inside of `block_metrics` to prometheus collector
        self.block_metrics.commit();
        // Reset the values inside to their defaults
        self.block_metrics.reset();
        *block_processing_time = Instant::now();

        self.current_block_iterations.clear();

        debug!(
            target = "engine",
            commit_head = ?commit_head,
            processed_blocks = *processed_blocks,
            "CommitHead successfully applied"
        );

        Ok(())
    }

    /// Process an incoming transaction
    ///
    /// Executes a transaction against an iteration, executing and validating all assertions.
    /// This method performs all synchronous work before state verification.
    #[instrument(name = "engine::process_transaction_event", skip_all, level = "info")]
    fn process_transaction_event(
        &mut self,
        queue_transaction: QueueTransaction,
    ) -> Result<(), EngineError> {
        let tx_execution_id = queue_transaction.tx_execution_id;
        let tx_hash = tx_execution_id.tx_hash;
        let tx_env = queue_transaction.tx_env;
        self.block_metrics.transactions_considered += 1;

        info!(
            target = "engine",
            tx_execution_id = ?tx_execution_id,
            tx_hash = ?tx_hash,
            "Processing a new transaction",
        );

        let block_id = tx_execution_id.as_block_execution_id();

        let Some(current_block_iteration) = self.current_block_iterations.get(&block_id) else {
            error!(
                target = "engine",
                tx_hash = %tx_hash,
                block_number = %tx_execution_id.block_number,
                iteration_id = tx_execution_id.iteration_id,
                caller = %tx_env.caller,
                "Received transaction without first receiving a BlockEnv. An iteration for the id must be received first"
            );
            return Err(EngineError::TransactionError);
        };

        let expected_block_number = self.current_head + U256::from(1);
        let iteration_block_number = current_block_iteration.block_env.number;
        if iteration_block_number != expected_block_number {
            warn!(
                target = "engine",
                tx_hash = %tx_hash,
                tx_block = %tx_execution_id.block_number,
                iteration_block = %iteration_block_number,
                expected_block = %expected_block_number,
                "Transaction block number does not match expected block number"
            );
            let message = format!(
                "Transaction targeted block {} but expected block {} based on current head {}",
                tx_execution_id.block_number, expected_block_number, self.current_head
            );
            self.add_transaction_result(
                tx_execution_id,
                &TransactionResult::ValidationError(message),
                None,
            )?;
            return Ok(());
        }

        debug!(
            target = "engine",
            tx_hash = %tx_hash,
            block_number = %tx_execution_id.block_number,
            iteration_id = tx_execution_id.iteration_id,
            caller = %tx_env.caller,
            gas_limit = tx_env.gas_limit,
            current_head = %self.current_head,
            "Processing transaction"
        );

        // Apply the previously executed transaction state changes to the block fork
        self.apply_state_buffer_to_fork(block_id)?;

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

        if let Some((_, state)) = current_block_iteration.last_executed_tx.current() {
            let changes = state.clone().ok_or(EngineError::NothingToCommit)?;
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
        self.cache
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
            let changes = state.clone().ok_or(EngineError::NothingToCommit)?;
            self.cache.commit(changes);
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
            block_number = %tx_execution_id.block_number,
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

        let Some(current_block_iteration) = self
            .current_block_iterations
            .get_mut(&tx_execution_id.as_block_execution_id())
        else {
            error!(
                target = "engine",
                tx_hash = %tx_execution_id.tx_hash,
                block_number = %tx_execution_id.block_number,
                iteration_id = tx_execution_id.iteration_id,
                "Received reorg without first receiving a BlockEnv"
            );
            return Err(EngineError::ReorgError);
        };

        // Check if we have received a transaction at all and if it matches
        if current_block_iteration.has_last_tx(tx_execution_id) {
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
            current_block_iteration_tx_hash = ?current_block_iteration.last_tx_id().map(|id| id.tx_hash),
            current_block_iteration_tx_block_number = ?current_block_iteration.last_tx_id().map(|id| id.block_number),
            "Reorg not found"
        );

        // If we received a reorg event before executing a tx,
        // or if the tx hashes dont match something bad happened and we need to exit
        Err(EngineError::BadReorgHash)
    }
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
            ExecutorError::ForkTxExecutionError(e) => e.into(),
            ExecutorError::AssertionExecutionError(..) => ErrorRecoverability::Unrecoverable,
        }
    }
}
