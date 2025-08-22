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

#[allow(unused_imports)]
use assertion_executor::{
    AssertionExecutor,
    db::overlay::OverlayDb,
    primitives::ExecutionResult,
    store::{
        AssertionState,
        AssertionStoreError,
    },
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
use std::collections::HashMap;
use tokio::sync::oneshot;
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
    #[error("Get transaction result oneshot channel closed")]
    GetTxResultChannelClosed,
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
    tx_receiver: TransactionQueueReceiver,
    assertion_executor: AssertionExecutor,
    block_env: Option<BlockEnv>,
    /// TODO: move this out of the core engine when we add proper state.
    transaction_results: HashMap<B256, TransactionResult>,
    pending_queries: HashMap<TxHash, oneshot::Sender<TransactionResult>>,
}

impl<DB: DatabaseRef + Send + Sync> CoreEngine<DB> {
    #[instrument(name = "engine::new", skip_all, level = "debug")]
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
            pending_queries: HashMap::new(),
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
            pending_queries: HashMap::new(),
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
    #[instrument(
        name = "engine::execute_transaction",
        skip(self),
        fields(tx_hash = %tx_hash, tx_env = ?tx_env, caller = %tx_env.caller, gas_limit = tx_env.gas_limit),
        level = "debug"
    )]
    fn execute_transaction(&mut self, tx_hash: B256, tx_env: TxEnv) -> Result<(), EngineError> {
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

        // Validate transaction and run assertions
        let rax = match self.assertion_executor.validate_transaction_ext_db(
            block_env.clone(),
            tx_env.clone(),
            &mut fork_db,
            &mut self.state,
        ) {
            Ok(rax) => rax,
            Err(e) => {
                error!(
                    target = "engine",
                    error = ?e,
                    tx_hash = %tx_hash,
                    tx_env= ?tx_env,
                    "Internal validation error occurred"
                );
                // Store the validation error result
                self.transaction_results.insert(
                    tx_hash,
                    TransactionResult::ValidationError(format!("{e:?}")),
                );
                return Err(EngineError::AssertionError);
            }
        };

        let is_valid = rax.is_valid();
        let execution_result = rax.result_and_state.result.clone();

        info!(
            target = "engine",
            tx_hash = %tx_hash,
            is_valid,
            execution_result = ?execution_result,
            "Transaction processed"
        );

        if is_valid {
            // Transaction valid, passed assertions, commit state for successful transactions
            debug!(
                target = "engine",
                tx_hash = %tx_hash,
                tx_env= ?tx_env,
                "Transaction does not invalidate assertions, processing result"
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
                    "Commiting state of successful tx"
                );
                self.state.commit(rax.result_and_state.state);
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
        }

        // Store the transaction result
        self.transaction_results.insert(
            tx_hash,
            TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            },
        );

        self.process_pending_queries(tx_hash)?;
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

    /// Check if there is a pending query for the processed result
    fn process_pending_queries(&mut self, tx_hash: TxHash) -> Result<(), EngineError> {
        if let Some(sender) = self.pending_queries.remove(&tx_hash) {
            let result = self.transaction_results.get(&tx_hash).unwrap();
            sender.send(result.clone()).map_err(|e| {
                error!(
                    target = "engine",
                    error = ?e,
                    tx_hash = %tx_hash,
                    "Failed to send transaction result to query sender"
                );
                EngineError::GetTxResultChannelClosed
            })?;
        }
        Ok(())
    }

    /// Run the engine and process transactions and blocks received
    /// via the transaction queue.
    // TODO: fn should probably not be async but we do it because
    // so we can easily select on result in main. too bad!
    #[instrument(name = "engine::run", skip_all, level = "info")]
    pub async fn run(&mut self) -> Result<(), EngineError> {
        let mut processed_blocks = 0u64;
        let mut processed_txs = 0u64;

        loop {
            let event = self.tx_receiver.try_recv().map_err(|e| {
                error!(
                    target = "engine",
                    error = ?e,
                    "Transaction queue channel closed"
                );
                EngineError::ChannelClosed
            })?;

            match event {
                TxQueueContents::Block(block_env) => {
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
                        gas_limit = block_env.gas_limit,
                        base_fee = ?block_env.basefee,
                        "Block details"
                    );

                    self.block_env = Some(block_env);
                }
                TxQueueContents::Tx(queue_transaction) => {
                    let tx_hash = queue_transaction.tx_hash;
                    let tx_env = queue_transaction.tx_env;
                    processed_txs += 1;

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

                    // Process the transaction with the current block environment
                    self.execute_transaction(tx_hash, tx_env)?;
                }
                TxQueueContents::GetTxResult(query) => {
                    let tx_hash = query.tx_hash;
                    let result = self.get_transaction_result(&tx_hash);
                    match result {
                        Some(result) => {
                            query.sender.send(result.clone()).map_err(|e| {
                                error!(
                                    target = "engine",
                                    error = ?e,
                                    tx_hash = %tx_hash,
                                    "Failed to send transaction result to query sender"
                                );
                                EngineError::GetTxResultChannelClosed
                            })?;
                        }
                        None => {
                            self.pending_queries.insert(query.tx_hash, query.sender);
                        }
                    }
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        transport::{
            Transport,
            mock::MockTransport,
        },
        utils::TestDbError,
    };
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

    /// Create a complete test setup with engine, mock transport, and driver sender
    fn create_test_setup() -> (
        CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
        MockTransport,
        crossbeam::channel::Sender<TxQueueContents>,
    ) {
        let (engine_tx, engine_rx) = crossbeam::channel::unbounded();
        let (mock_tx, mock_rx) = crossbeam::channel::unbounded();

        // Create engine
        let underlying_db = CacheDB::new(EmptyDBTyped::default());
        let state = OverlayDb::new(Some(std::sync::Arc::new(underlying_db)), 1024);
        let assertion_store =
            AssertionStore::new_ephemeral().expect("Failed to create assertion store");
        let assertion_executor = AssertionExecutor::new(ExecutorConfig::default(), assertion_store);
        let engine = CoreEngine::new(state, engine_rx, assertion_executor);

        // Create mock transport with the receiver
        let mock_transport = MockTransport::with_receiver(engine_tx, mock_rx);

        (engine, mock_transport, mock_tx)
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
        let (mut engine, _) = create_test_engine();
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

        engine.block_env = Some(block_env.clone());

        // Execute the transaction
        let result = engine.execute_transaction(tx_hash, tx_env);

        assert!(
            result.is_ok(),
            "Transaction should execute successfully: {result:?}"
        );
        assert!(
            engine.get_block_env().is_some(),
            "Block environment should be set"
        );
        assert_eq!(engine.get_block_env().unwrap().number, 1);

        // Verify transaction result is stored
        let tx_result = engine.get_transaction_result(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        match tx_result.unwrap() {
            TransactionResult::ValidationCompleted { is_valid, .. } => {
                assert!(is_valid, "Transaction should pass assertions");
            }
            TransactionResult::ValidationError(e) => {
                panic!("Unexpected validation error: {e:?}");
            }
        }
    }

    #[test]
    fn test_reverting_transaction_no_state_update() {
        let (mut engine, _) = create_test_engine();
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

        // Capture comprehensive state snapshot before transaction execution
        let initial_cache_count = engine.get_state().cache_entry_count();

        // Verify no initial state exists for caller or contract addresses
        let caller_before = engine.get_state().basic_ref(tx_env.caller).unwrap();
        assert!(
            caller_before.is_none(),
            "Caller account should not exist before transaction"
        );

        // For CREATE transactions, calculate the expected contract address
        use revm::primitives::address;
        let expected_contract_address = address!("76cae8af66cb2488933e640ba08650a3a8e7ae19");
        let contract_before = engine
            .get_state()
            .basic_ref(expected_contract_address)
            .unwrap();
        assert!(
            contract_before.is_none(),
            "Contract account should not exist before transaction"
        );

        engine.block_env = Some(block_env);

        // Execute the reverting transaction
        let result = engine.execute_transaction(tx_hash, tx_env.clone());

        // The transaction execution should complete successfully even if the transaction reverts
        assert!(
            result.is_ok(),
            "Engine should handle reverting transactions gracefully: {result:?}"
        );

        // Verify comprehensive state verification: overlay should be unchanged
        let final_cache_count = engine.get_state().cache_entry_count();
        assert_eq!(
            final_cache_count, initial_cache_count,
            "Reverting transaction should not add entries to the state cache. Initial: {initial_cache_count}, Final: {final_cache_count}"
        );

        // Verify specific account states remain unchanged
        let caller_after = engine.get_state().basic_ref(tx_env.caller).unwrap();
        assert!(
            caller_after.is_none(),
            "Caller account should not exist after reverting transaction"
        );

        let contract_after = engine
            .get_state()
            .basic_ref(expected_contract_address)
            .unwrap();
        assert!(
            contract_after.is_none(),
            "Contract account should not exist after reverting transaction"
        );

        // Verify no storage changes occurred
        let storage_result = engine.get_state().storage_ref(tx_env.caller, U256::ZERO);
        assert!(
            storage_result.is_ok(),
            "Should be able to check storage without errors"
        );
        assert_eq!(
            storage_result.unwrap(),
            U256::ZERO,
            "Storage should remain empty/default"
        );

        // Verify the overlay cache itself shows no contamination
        // by checking that no keys are cached for our transaction addresses
        assert!(
            !engine
                .get_state()
                .is_cached(&assertion_executor::db::overlay::TableKey::Basic(
                    tx_env.caller
                )),
            "Caller account should not be cached in overlay after revert"
        );
        assert!(
            !engine
                .get_state()
                .is_cached(&assertion_executor::db::overlay::TableKey::Basic(
                    expected_contract_address
                )),
            "Contract account should not be cached in overlay after revert"
        );

        // Verify transaction result is stored and shows it reverted
        let tx_result = engine.get_transaction_result(&tx_hash);
        assert!(tx_result.is_some(), "Transaction result should be stored");
        match tx_result.unwrap() {
            TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            } => {
                assert!(
                    *is_valid,
                    "Transaction should pass assertions (no assertions to fail)"
                );
                match execution_result {
                    ExecutionResult::Revert { .. } => {
                        // Expected - transaction reverted
                    }
                    other => panic!("Expected Revert result, got {other:?}"),
                }
            }
            TransactionResult::ValidationError(e) => {
                panic!("Unexpected validation error: {e:?}");
            }
        }
    }

    #[test]
    fn test_database_commit_verification() {
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
        let result = engine.execute_transaction(tx_hash, tx_env.clone());
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
            "Transaction executed and state is readable - data was committed. Initial: {initial_cache_count}, Final: {final_cache_count}"
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
        match tx_result.unwrap() {
            TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            } => {
                assert!(*is_valid, "Transaction should pass assertions");
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
        let result = engine.execute_transaction(tx_hash, tx_env.clone());

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

    #[tokio::test]
    async fn test_end_to_end_transaction_flow() {
        // Use the simpler create_test_engine helper to get direct access to sender
        let (mut engine, queue_sender) = create_test_engine();

        // Create test block and transaction
        let block_env = create_test_block_env();
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
        let tx_hash = B256::from([0x11; 32]);

        // Send block environment and transaction directly to engine queue
        queue_sender
            .send(TxQueueContents::Block(block_env))
            .unwrap();
        queue_sender
            .send(TxQueueContents::Tx(queue::QueueTransaction {
                tx_hash,
                tx_env: tx_env.clone(),
            }))
            .unwrap();

        // Process the items manually (simulating what engine.run() would do)
        // Process block first
        let block_item = engine.tx_receiver.try_recv().unwrap();
        match block_item {
            TxQueueContents::Block(block) => engine.block_env = Some(block),
            _ => panic!("Expected block"),
        }

        // Process transaction
        let tx_item = engine.tx_receiver.try_recv().unwrap();
        match tx_item {
            TxQueueContents::Tx(queue_tx) => {
                engine
                    .execute_transaction(queue_tx.tx_hash, queue_tx.tx_env)
                    .unwrap();
            }
            _ => panic!("Expected transaction"),
        }

        // Verify the transaction was processed
        let result = engine.get_transaction_result(&tx_hash);
        assert!(result.is_some(), "Transaction result should be stored");
        match result.unwrap() {
            TransactionResult::ValidationCompleted { is_valid, .. } => {
                assert!(is_valid, "Transaction should pass assertions");
            }
            TransactionResult::ValidationError(e) => {
                panic!("Unexpected validation error: {e:?}");
            }
        }
    }

    #[tokio::test]
    async fn test_mock_driver_multiple_blocks_and_transactions() {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let (mut engine, mock_transport, mock_sender) = create_test_setup();

            // Prepare test data
            let block1 = BlockEnv {
                number: 1,
                ..create_test_block_env()
            };
            let block2 = BlockEnv {
                number: 2,
                ..create_test_block_env()
            };

            let tx1_env = TxEnv {
                caller: Address::from([0x01; 20]),
                gas_limit: 100000,
                gas_price: 0,
                kind: TxKind::Create,
                value: uint!(0_U256),
                data: Bytes::new(),
                nonce: 0,
                ..Default::default()
            };
            let tx1_hash = B256::from([0x11; 32]);

            let tx2_env = TxEnv {
                caller: Address::from([0x02; 20]),
                gas_limit: 100000,
                gas_price: 0,
                kind: TxKind::Create,
                value: uint!(0_U256),
                data: Bytes::new(),
                nonce: 0,
                ..Default::default()
            };
            let tx2_hash = B256::from([0x22; 32]);

            // Send sequence: Block1 -> Tx1 -> Block2 -> Tx2
            mock_sender.send(TxQueueContents::Block(block1)).unwrap();
            mock_sender
                .send(TxQueueContents::Tx(queue::QueueTransaction {
                    tx_hash: tx1_hash,
                    tx_env: tx1_env,
                }))
                .unwrap();
            mock_sender.send(TxQueueContents::Block(block2)).unwrap();
            mock_sender
                .send(TxQueueContents::Tx(queue::QueueTransaction {
                    tx_hash: tx2_hash,
                    tx_env: tx2_env,
                }))
                .unwrap();

            // Start transport in background
            let transport_handle = tokio::spawn(async move {
                let _ = mock_transport.run().await;
            });

            // Give transport a moment to forward the data
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            // Process items manually in sequence
            // Block 1
            let item = engine.tx_receiver.try_recv().unwrap();
            match item {
                TxQueueContents::Block(block) => engine.block_env = Some(block),
                _ => panic!(),
            }

            // Transaction 1
            let item = engine.tx_receiver.try_recv().unwrap();
            match item {
                TxQueueContents::Tx(queue_tx) => {
                    engine
                        .execute_transaction(queue_tx.tx_hash, queue_tx.tx_env)
                        .unwrap()
                }
                _ => panic!(),
            }

            // Block 2
            let item = engine.tx_receiver.try_recv().unwrap();
            match item {
                TxQueueContents::Block(block) => engine.block_env = Some(block),
                _ => panic!(),
            }

            // Transaction 2
            let item = engine.tx_receiver.try_recv().unwrap();
            match item {
                TxQueueContents::Tx(queue_tx) => {
                    engine
                        .execute_transaction(queue_tx.tx_hash, queue_tx.tx_env)
                        .unwrap()
                }
                _ => panic!(),
            }

            // Clean up - close the channel to stop the transport
            drop(mock_sender);
            transport_handle.abort();
            let _ = transport_handle.await;

            // Verify both transactions were processed
            let tx1_result = engine.get_transaction_result(&tx1_hash);
            let tx2_result = engine.get_transaction_result(&tx2_hash);

            assert!(
                tx1_result.is_some(),
                "Transaction 1 result should be stored"
            );
            assert!(
                tx2_result.is_some(),
                "Transaction 2 result should be stored"
            );
            match tx1_result.unwrap() {
                TransactionResult::ValidationCompleted { is_valid, .. } => {
                    assert!(is_valid, "Transaction 1 should pass assertions");
                }
                TransactionResult::ValidationError(e) => {
                    panic!("Transaction 1 unexpected validation error: {e:?}");
                }
            }
            match tx2_result.unwrap() {
                TransactionResult::ValidationCompleted { is_valid, .. } => {
                    assert!(is_valid, "Transaction 2 should pass assertions");
                }
                TransactionResult::ValidationError(e) => {
                    panic!("Transaction 2 unexpected validation error: {e:?}");
                }
            }

            // Verify final block state
            assert_eq!(
                engine.get_block_env().unwrap().number,
                2,
                "Should be on block 2"
            );
        })
        .await
        .expect("Test timed out");
    }
}
