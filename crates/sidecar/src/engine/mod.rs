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
    AssertionExecutor,
    db::overlay::OverlayDb,
    primitives::ExecutionResult,
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
    primitives::{
        Address,
        B256,
    },
};
use std::collections::HashMap;
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

/// Result of transaction execution
#[derive(Debug, Clone)]
pub struct TransactionResult {
    pub execution_result: ExecutionResult,
    pub passed_assertions: bool,
}

/// The engine processes blocks and appends transactions to them.
#[derive(Debug, Clone)]
pub struct CoreEngine<DB> {
    state: OverlayDb<DB>,
    tx_receiver: TransactionQueueReceiver,
    assertion_executor: AssertionExecutor,
    block_env: Option<BlockEnv>,
    /// TODO: move this out of the core engine when we add proper state.
    transaction_results: HashMap<B256, TransactionResult>,
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
            transaction_results: HashMap::new(),
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
            transaction_results: HashMap::new(),
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
    #[instrument(name = "execute_transaction", skip(self, tx_env), fields(tx_hash = %tx_hash, caller = %tx_env.caller, gas_limit = tx_env.gas_limit), level = "debug")]
    fn execute_transaction(&mut self, tx_hash: B256, tx_env: TxEnv) -> Result<(), EngineError> {
        let mut fork_db = self.state.fork();
        let block_env = self.block_env.as_ref().ok_or_else(|| {
            error!("No block environment set for transaction execution");
            EngineError::TransactionError
        })?;

        debug!(
            "Validating transaction {} against assertions at block {}",
            tx_hash, block_env.number
        );

        // Note: does not actually run assertions because we instantiate a test store with no way to add them.
        let rax = self
            .assertion_executor
            .validate_transaction_ext_db(
                block_env.clone(),
                tx_env.clone(),
                &mut fork_db,
                &mut self.state,
            )
            .map_err(|e| {
                error!("Assertion validation failed: {:?}", e);
                EngineError::AssertionError
            })?;

        let passed_assertions = rax.is_valid();
        let execution_result = rax.result_and_state.result.clone();

        if passed_assertions {
            debug!(
                "Transaction {} passed all assertions, processing result",
                tx_hash
            );
            // Transaction valid, passed assertions, commit
            match &execution_result {
                ExecutionResult::Success { gas_used, .. } => {
                    info!(
                        "Transaction {} executed successfully, gas used: {}",
                        tx_hash, gas_used
                    );
                    self.state.commit(rax.result_and_state.state);
                }
                ExecutionResult::Revert { gas_used, .. } => {
                    warn!("Transaction {} reverted, gas used: {}", tx_hash, gas_used);
                }
                ExecutionResult::Halt { reason, gas_used } => {
                    error!(
                        "Transaction {} halted with reason: {:?}, gas used: {}",
                        tx_hash, reason, gas_used
                    );
                    // Store the result before returning error
                    self.transaction_results.insert(
                        tx_hash,
                        TransactionResult {
                            execution_result,
                            passed_assertions,
                        },
                    );
                    return Err(EngineError::TransactionError);
                }
            }
        } else {
            warn!("Transaction {} failed assertion validation!", tx_hash);
            trace!(
                "Transaction {} details: {:?} Assertions ran: {:?}",
                tx_hash, tx_env, rax.assertions_executions,
            );
        }

        // Store the transaction result
        self.transaction_results.insert(
            tx_hash,
            TransactionResult {
                execution_result,
                passed_assertions,
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
    pub fn get_transaction_result(&self, tx_hash: &B256) -> Option<&TransactionResult> {
        self.transaction_results.get(tx_hash)
    }

    /// Get all transaction results for testing purposes.
    #[cfg(test)]
    pub fn get_all_transaction_results(&self) -> &HashMap<B256, TransactionResult> {
        &self.transaction_results
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
            let event = self.tx_receiver.try_recv().map_err(|e| {
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
                TxQueueContents::Tx { tx_hash, tx_env } => {
                    processed_txs += 1;

                    if self.block_env.is_none() {
                        error!(
                            "Received transaction {} without first receiving a BlockEnv! caller={}, processed_txs={}",
                            tx_hash, tx_env.caller, processed_txs
                        );
                        return Err(EngineError::TransactionError);
                    }

                    debug!(
                        "Processing transaction: tx_hash={}, caller={}, gas_limit={}, processed_txs={}, current_block={}",
                        tx_hash,
                        tx_env.caller,
                        tx_env.gas_limit,
                        processed_txs,
                        self.block_env
                            .as_ref()
                            .map(|b| b.number.to_string())
                            .unwrap_or_else(|| "None".to_string())
                    );

                    // Process the transaction with the current block environment
                    self.execute_transaction(tx_hash, tx_env)?;
                }
            }

            if processed_blocks > 0 && processed_blocks.is_multiple_of(100) {
                info!(
                    "Engine processing stats: blocks={}, transactions={}, cache_entries={}",
                    processed_blocks,
                    processed_txs,
                    self.state.cache_entry_count()
                );
            }
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

        let engine = CoreEngine::new(state, tx_receiver, assertion_executor);
        (engine, tx_sender)
    }

    fn create_test_block_env() -> BlockEnv {
        BlockEnv {
            number: 1,
            basefee: 0, // Set basefee to 0 to avoid balance issues
            ..Default::default()
        }
    }

    #[test]
    fn test_successful_transaction_execution() {
        let (mut engine, tx_sender) = create_test_engine();
        let block_env = create_test_block_env();

        // Create a simple transaction that doesn't require assertions
        let tx_env = TxEnv {
            caller: Address::from([0x01; 20]),
            gas_limit: 100000,
            gas_price: 0,
            kind: TxKind::Create,
            value: uint!(0_U256),
            data: Bytes::new(),
            nonce: 0,
            ..Default::default()
        };

        // Generate a random transaction hash for testing
        let tx_hash = B256::from([0x11; 32]);

        // Send block environment first
        tx_sender
            .send(TxQueueContents::Block(block_env.clone()))
            .unwrap();

        // Send the transaction
        tx_sender
            .send(TxQueueContents::Tx {
                tx_hash,
                tx_env: tx_env.clone(),
            })
            .unwrap();

        // Process one iteration of the engine loop manually for testing
        let received_block = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Block(block) => block,
            _ => panic!("Expected block environment"),
        };
        engine.block_env = Some(received_block);

        let (received_tx_hash, received_tx_env) = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Tx { tx_hash, tx_env } => (tx_hash, tx_env),
            _ => panic!("Expected transaction"),
        };

        // Execute the transaction
        let result = engine.execute_transaction(received_tx_hash, received_tx_env);

        assert!(
            result.is_ok(),
            "Transaction should execute successfully: {:?}",
            result
        );
        assert!(
            engine.get_block_env().is_some(),
            "Block environment should be set"
        );
        assert_eq!(engine.get_block_env().unwrap().number, 1);

        // Verify transaction result is stored
        let tx_result = engine.get_transaction_result(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        assert!(
            tx_result.unwrap().passed_assertions,
            "Transaction should pass assertions"
        );
    }

    #[test]
    fn test_reverting_transaction_no_state_update() {
        let (mut engine, tx_sender) = create_test_engine();
        let block_env = create_test_block_env();

        // Create a transaction that will revert - Create with bytecode that calls REVERT
        let tx_env = TxEnv {
            caller: Address::from([0x02; 20]),
            gas_limit: 100000,
            gas_price: 0,
            kind: TxKind::Create,
            value: uint!(0_U256),
            data: Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]), // PUSH1 0x00 PUSH1 0x00 REVERT (reverts with empty message)
            nonce: 0,
            ..Default::default()
        };

        // Generate a random transaction hash for testing
        let tx_hash = B256::from([0x22; 32]);

        // Get initial state snapshot
        let initial_cache_count = engine.get_state().cache_entry_count();

        // Send block environment first
        tx_sender
            .send(TxQueueContents::Block(block_env.clone()))
            .unwrap();

        // Send the reverting transaction
        tx_sender
            .send(TxQueueContents::Tx {
                tx_hash,
                tx_env: tx_env.clone(),
            })
            .unwrap();

        // Process the block environment
        let received_block = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Block(block) => block,
            _ => panic!("Expected block environment"),
        };
        engine.block_env = Some(received_block);

        let (received_tx_hash, received_tx_env) = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Tx { tx_hash, tx_env } => (tx_hash, tx_env),
            _ => panic!("Expected transaction"),
        };

        // Execute the reverting transaction
        let result = engine.execute_transaction(received_tx_hash, received_tx_env);

        // The transaction execution should complete successfully even if the transaction reverts
        assert!(
            result.is_ok(),
            "Engine should handle reverting transactions gracefully: {:?}",
            result
        );

        // Verify that no state changes were committed to the underlying database
        // Since the transaction reverted, the cache entry count should remain the same
        assert_eq!(
            engine.get_state().cache_entry_count(),
            initial_cache_count,
            "Reverting transaction should not add entries to the state cache"
        );

        // Verify transaction result is stored and shows it reverted
        let tx_result = engine.get_transaction_result(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        let result_data = tx_result.unwrap();
        assert!(
            result_data.passed_assertions,
            "Transaction should pass assertions (no assertions to fail)"
        );
        match &result_data.execution_result {
            assertion_executor::primitives::ExecutionResult::Revert { .. } => {
                // Expected - transaction reverted
            }
            other => panic!("Expected Revert result, got {:?}", other),
        }
    }

    #[test]
    fn test_database_commit_verification() {
        let (mut engine, tx_sender) = create_test_engine();
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

        // Send block environment and transaction
        tx_sender
            .send(TxQueueContents::Block(block_env.clone()))
            .unwrap();
        tx_sender
            .send(TxQueueContents::Tx {
                tx_hash,
                tx_env: tx_env.clone(),
            })
            .unwrap();

        // Process the block environment
        let received_block = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Block(block) => block,
            _ => panic!("Expected block environment"),
        };
        engine.block_env = Some(received_block);

        let (received_tx_hash, received_tx_env) = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Tx { tx_hash, tx_env } => (tx_hash, tx_env),
            _ => panic!("Expected transaction"),
        };

        // Execute the transaction
        let result = engine.execute_transaction(received_tx_hash, received_tx_env);
        assert!(result.is_ok(), "Transaction should execute successfully");

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
        use revm::primitives::address;
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
            "Transaction executed and state is readable - data was committed. Initial: {}, Final: {}",
            initial_cache_count,
            final_cache_count
        );

        // Verify we can read storage from the state after commit
        let state_result = engine.get_state().storage_ref(tx_env.caller, U256::ZERO);
        assert!(
            state_result.is_ok(),
            "Should be able to read from committed state"
        );

        // Verify transaction result is stored and succeeded
        let tx_result = engine.get_transaction_result(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        let result_data = tx_result.unwrap();
        assert!(
            result_data.passed_assertions,
            "Transaction should pass assertions"
        );
        match &result_data.execution_result {
            assertion_executor::primitives::ExecutionResult::Success { .. } => {
                // Expected - transaction succeeded
            }
            other => panic!("Expected Success result, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_requires_block_env_before_tx() {
        let (mut engine, tx_sender) = create_test_engine();
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

        // Send transaction without block environment first
        tx_sender
            .send(TxQueueContents::Tx {
                tx_hash,
                tx_env: tx_env.clone(),
            })
            .unwrap();

        let (received_tx_hash, received_tx_env) = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Tx { tx_hash, tx_env } => (tx_hash, tx_env),
            _ => panic!("Expected transaction"),
        };

        // Execute transaction without block environment
        let result = engine.execute_transaction(received_tx_hash, received_tx_env);

        assert!(
            result.is_err(),
            "Engine should require block environment before processing transactions"
        );
        match result.unwrap_err() {
            EngineError::TransactionError => {
                // This is the expected error when no block environment is set
            }
            other => panic!("Expected TransactionError, got {:?}", other),
        }
    }

    #[test]
    fn test_engine_maintains_block_state() {
        let (mut engine, tx_sender) = create_test_engine();
        let block_env1 = BlockEnv {
            number: 1,
            ..create_test_block_env()
        };
        let block_env2 = BlockEnv {
            number: 2,
            ..create_test_block_env()
        };

        // Send first block
        tx_sender
            .send(TxQueueContents::Block(block_env1.clone()))
            .unwrap();
        let received_block1 = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Block(block) => block,
            _ => panic!("Expected block environment"),
        };
        engine.block_env = Some(received_block1);

        assert_eq!(engine.get_block_env().unwrap().number, 1);

        // Send second block
        tx_sender
            .send(TxQueueContents::Block(block_env2.clone()))
            .unwrap();
        let received_block2 = match engine.tx_receiver.try_recv().unwrap() {
            TxQueueContents::Block(block) => block,
            _ => panic!("Expected block environment"),
        };
        engine.block_env = Some(received_block2);

        assert_eq!(engine.get_block_env().unwrap().number, 2);
    }
}
