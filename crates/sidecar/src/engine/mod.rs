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

use super::engine::queue::{
    TransactionQueueReceiver,
    TxQueueContents,
};

use assertion_executor::{
    primitives::ExecutionResult,
    AssertionExecutor,
    db::overlay::OverlayDb,
    store::{
        AssertionState,
        AssertionStoreError,
    },
};
use revm::{
    DatabaseCommit,
    DatabaseRef,
    context::{
        BlockEnv,
        TxEnv,
    },
    primitives::Address,
};
use tracing::{
    debug,
    error,
    info,
    instrument,
    trace,
    warn,
};

#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("Database error")]
    DatabaseError,
    #[error("Transaction error")]
    TransactionError,
    #[error("Assertion error")]
    AssertionError,
    #[error("Transaction queue channel closed")]
    ChannelClosed,
}

/// The engine processes blocks and appends transactions to them.
#[derive(Debug, Clone)]
pub struct CoreEngine<DB> {
    state: OverlayDb<DB>,
    tx_receiver: TransactionQueueReceiver,
    assertion_executor: AssertionExecutor,
    block_env: Option<BlockEnv>,
}

impl<DB: DatabaseRef + Send + Sync> CoreEngine<DB> {
    #[instrument(name = "core_engine_new", skip_all, level = "debug")]
    pub fn new(
        state: OverlayDb<DB>,
        tx_receiver: TransactionQueueReceiver,
        assertion_executor: AssertionExecutor,
    ) -> Self {        
        Self {
            state,
            tx_receiver,
            assertion_executor,
            block_env: None,
        }
    }

    /// Creates a new `CoreEngine` for testing purposes.
    /// Not to be used for anything but tests.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn new_test() -> Self {
        use assertion_executor::{
            ExecutorConfig,
            store::AssertionStore,
        };

        let (_, tx_receiver) = crossbeam::channel::unbounded();
        Self {
            state: OverlayDb::new(None, 64),
            tx_receiver,
            assertion_executor: AssertionExecutor::new(
                ExecutorConfig::default(),
                AssertionStore::new_ephemeral().expect("REASON"),
            ),
            block_env: None,
        }
    }

    /// Inserts an assertion directly into the assertion store of the engine.
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
    #[instrument(name = "execute_transaction", skip(self, tx_env), fields(caller = %tx_env.caller, gas_limit = tx_env.gas_limit), level = "debug")]
    fn execute_transaction(&mut self, tx_env: TxEnv) -> Result<(), EngineError> {
        let mut fork_db = self.state.fork();
        let block_env = self
            .block_env
            .as_ref()
            .ok_or_else(|| {
                error!("No block environment set for transaction execution");
                EngineError::TransactionError
            })?;
        
        debug!("Validating transaction against assertions at block {}", block_env.number);
        
        // Note: does not actually run assertions because we instantiate a test store with no way to add them.
        let rax = self
            .assertion_executor
            .validate_transaction_ext_db(block_env.clone(), tx_env.clone(), &mut fork_db, &mut self.state)
            .map_err(|e| {
                error!("Assertion validation failed: {:?}", e);
                EngineError::AssertionError
            })?;

        if rax.is_valid() {
            debug!("Transaction passed all assertions, processing result");
            // Transaction valid, passed assertions, commit
            match rax.result_and_state.result {
                ExecutionResult::Success { gas_used, .. } => {
                    info!("Transaction executed successfully, gas used: {}", gas_used);
                    self.state.commit(rax.result_and_state.state);
                },
                ExecutionResult::Revert { gas_used, .. } => {
                    warn!("Transaction reverted, gas used: {}", gas_used);
                },
                ExecutionResult::Halt { reason, gas_used } => {
                    error!("Transaction halted with reason: {:?}, gas used: {}", reason, gas_used);
                    return Err(EngineError::TransactionError);
                },
            }
        } else {
            warn!("Transaction failed assertion validation!");
            trace!("Transaction details: {:?} Assertions ran: {:?}", tx_env, rax.assertions_executions,);
        }

        trace!("Transaction execution completed");
        Ok(())
    }

    /// Run the engine and process transactions and blocks received
    /// via the transaction queue.
    // TODO: fn should probably not be async but we do it because
    // so we can easily select on result in main. too bad!
    #[instrument(name = "engine_run", skip_all, level = "info")]
    pub async fn run(&mut self) -> Result<(), EngineError> {
        let mut processed_blocks = 0u64;
        let mut processed_txs = 0u64;
        
        loop {
            let event = self
                .tx_receiver
                .try_recv()
                .map_err(|e| {
                    error!("Transaction queue channel closed: {:?}", e);
                    EngineError::ChannelClosed
                })?;

            match event {
                TxQueueContents::Block(block_env) => {
                    processed_blocks += 1;
                    info!(
                        "Processing new block: number={}, processed_blocks={}",
                        block_env.number, processed_blocks
                    );
                    debug!(
                        "Block details: timestamp={}, gas_limit={}, base_fee={:?}",
                        block_env.timestamp, block_env.gas_limit, block_env.basefee
                    );
                    
                    self.block_env = Some(block_env);
                }
                TxQueueContents::Tx(tx) => {
                    processed_txs += 1;
                    
                    if self.block_env.is_none() {
                        error!(
                            "Received transaction without first receiving a BlockEnv! caller={}, processed_txs={}",
                            tx.caller, processed_txs
                        );
                        return Err(EngineError::TransactionError);
                    }
                    
                    debug!(
                        "Processing transaction: caller={}, gas_limit={}, processed_txs={}, current_block={}",
                        tx.caller, tx.gas_limit, processed_txs, 
                        self.block_env.as_ref().map(|b| b.number.to_string()).unwrap_or_else(|| "None".to_string())
                    );
                    
                    // Process the transaction with the current block environment
                    self.execute_transaction(tx)?;
                }
            }
            
            if processed_blocks > 0 && processed_blocks % 100 == 0 {
                info!(
                    "Engine processing stats: blocks={}, transactions={}, cache_entries={}",
                    processed_blocks, processed_txs, self.state.cache_entry_count()
                );
            }
        }
    }
}
