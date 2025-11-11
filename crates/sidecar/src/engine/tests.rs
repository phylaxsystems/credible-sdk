#![allow(clippy::field_reassign_with_default)]
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

impl<DB> CoreEngine<DB> {
    /// Creates a new `CoreEngine` for testing purposes.
    /// Not to be used for anything but tests.
    #[cfg(test)]
    #[allow(dead_code)]
    #[allow(clippy::missing_panics_doc)]
    pub fn new_test() -> Self {
        let (_, tx_receiver) = crossbeam::channel::unbounded();
        let sources = Arc::new(Sources::new(vec![], 10));
        Self {
            cache: OverlayDb::new(None),
            current_block_iterations: HashMap::new(),
            tx_receiver,
            assertion_executor: AssertionExecutor::new(
                ExecutorConfig::default(),
                AssertionStore::new_ephemeral().expect("REASON"),
            ),
            sources: sources.clone(),
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
            #[cfg(feature = "cache_validation")]
            iteration_pending_processed_transactions: HashMap::new(),
            sources_monitoring: monitoring::sources::Sources::new(
                sources,
                Duration::from_millis(20),
            ),
            current_head: 0,
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

    /// Get the state of the engine's overlay database for testing purposes.
    #[cfg(test)]
    pub fn get_cache(&self) -> &OverlayDb<DB> {
        &self.cache
    }

    /// Get a reference to the block environment for testing purposes.
    #[cfg(test)]
    pub fn get_block_env(&self, block_execution_id: &BlockExecutionId) -> Option<BlockEnv> {
        self.current_block_iterations
            .get(block_execution_id)
            .map(|a| a.block_env.clone())
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
}

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
    let sources = Arc::new(Sources::new(vec![], 10));
    let engine = CoreEngine::new(
        state,
        sources,
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
    let (mut engine, tx_sender) = create_test_engine_with_timeout(Duration::from_millis(10)).await;

    let engine_handle = tokio::spawn(async move { engine.run().await });

    // Create iteration with block number 1
    let mut block_env = BlockEnv::default();
    block_env.number = 1; // Match the transaction's block number

    let new_iteration = queue::NewIteration {
        block_env,
        iteration_id: 0,
    };

    let queue_tx = queue::QueueTransaction {
        tx_execution_id: TxExecutionId::new(1, 0, B256::from([0x11; 32])),
        tx_env: TxEnv::default(),
    };

    tx_sender
        .send(TxQueueContents::NewIteration(
            new_iteration,
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

    // Set up and commit block 1 to establish current_head = 1
    let block_env_1 = BlockEnv {
        number: 1,
        basefee: 0,
        ..Default::default()
    };

    let queue_iteration_1 = queue::NewIteration {
        block_env: block_env_1,
        iteration_id: 0,
    };
    engine.process_iteration(&queue_iteration_1).unwrap();

    let queue_commit_1 = queue::CommitHead::new(1, 0, None, 0);
    engine
        .process_commit_head(&queue_commit_1, &mut 0, &mut Instant::now())
        .unwrap();

    // Now current_head = 1, so expected_block_number = 2
    let expected_block_number = engine.current_head + 1; // = 2

    // Create an iteration for a mismatched block (e.g., block 5)
    let mismatched_block_number = 5;
    let block_env_mismatched = BlockEnv {
        number: mismatched_block_number,
        basefee: 0,
        ..Default::default()
    };

    let queue_iteration_mismatch = queue::NewIteration {
        block_env: block_env_mismatched,
        iteration_id: 0,
    };
    let iteration_result = engine.process_iteration(&queue_iteration_mismatch);
    assert!(
        matches!(iteration_result, Err(EngineError::IterationError)),
        "mismatched block iteration should be rejected"
    );

    // Send transaction for the mismatched block
    let tx_execution_id = TxExecutionId::new(mismatched_block_number, 0, B256::from([0x42; 32]));
    let queue_transaction = queue::QueueTransaction {
        tx_execution_id,
        tx_env: TxEnv::default(),
    };

    let result = engine.process_transaction_event(queue_transaction);
    assert!(
        matches!(result, Err(EngineError::TransactionError)),
        "Transaction for rejected iteration should fail, got {result:?}"
    );

    assert!(
        engine
            .get_transaction_result_cloned(&tx_execution_id)
            .is_none(),
        "Rejected iteration should not record a transaction result"
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
    instance.new_iteration(1).await.unwrap();

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
    let initial_cache_count = engine.get_cache().cache_entry_count();

    let current_block_iteration_id = BlockIterationData {
        fork_db: engine.cache.fork(),
        n_transactions: 0,
        last_executed_tx: LastExecutedTx::new(),
        block_env,
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
        .get_cache()
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
        .get_cache()
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
    let final_cache_count = engine.get_cache().cache_entry_count();
    assert!(
        final_cache_count >= initial_cache_count,
        "Transaction executed and state is readable - data was committed. Initial: {initial_cache_count}, Final: {final_cache_count}"
    );

    // Verify we can read storage from the state after commit
    let state_result = engine.get_cache().storage_ref(tx_env.caller, U256::ZERO);
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
            .eth_rpc_source_http_mock
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
        fork_db: engine.cache.fork(),
        n_transactions: 0,
        last_executed_tx,
        block_env: BlockEnv::default(),
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
    instance.new_iteration(1).await.unwrap();
    let tx_block1_iter1 = instance
        .send_successful_create_tx_dry(uint!(100_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let tx_block1_iter2 = instance
        .send_successful_create_tx_dry(uint!(200_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(3).await.unwrap();
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
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

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
async fn test_iteration_selection_commits_only_winner(mut instance: crate::utils::LocalInstance) {
    info!("Testing that only winning iteration is committed to state");

    let initial_cache_count = instance.cache_reset_count();

    // Block 1
    instance.new_block().await.unwrap();

    // Iteration 1: Create a contract at value 100
    instance.new_iteration(1).await.unwrap();
    let tx_iter1 = instance
        .send_successful_create_tx_dry(uint!(100_U256), Bytes::new())
        .await
        .unwrap();

    // Iteration 2: Create a contract at value 200
    instance.new_iteration(2).await.unwrap();
    let tx_iter2 = instance
        .send_successful_create_tx_dry(uint!(200_U256), Bytes::new())
        .await
        .unwrap();

    assert!(instance.is_transaction_successful(&tx_iter1).await.unwrap());
    assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

    // Block 2: Select iteration 1 as the winner
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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
async fn test_wrong_iteration_selected_triggers_flush(mut instance: crate::utils::LocalInstance) {
    info!("Testing that selecting wrong iteration triggers cache flush");

    // Block 1
    instance.new_block().await.unwrap();

    // Send transaction in iteration 1
    instance.new_iteration(1).await.unwrap();
    let tx_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Send transaction in iteration 2
    instance.new_iteration(2).await.unwrap();
    let tx_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(instance.is_transaction_successful(&tx_iter1).await.unwrap());
    assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

    // Block 2: Select iteration 3 (which doesn't exist)
    instance.new_iteration(3).await.unwrap();
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
    instance.new_iteration(2).await.unwrap();
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
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

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
    instance.new_iteration(1).await.unwrap();
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
    instance.new_iteration(1).await.unwrap();
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
    instance.new_iteration(1).await.unwrap();
    let tx1_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    let tx2_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Iteration 2: Send 1 transaction
    instance.new_iteration(2).await.unwrap();
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
    instance.new_iteration(1).await.unwrap();
    instance.send_reorg(tx2_iter1).await.unwrap();

    // Block 2: Select iteration 1 with only 1 transaction (after reorg)
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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
    instance.new_iteration(2).await.unwrap();
    let tx_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(instance.is_transaction_successful(&tx_iter2).await.unwrap());

    // Block 2: Select iteration 1 which has no transactions
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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
    instance.new_iteration(1).await.unwrap();
    let tx1_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let tx1_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(1).await.unwrap();
    let tx2_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
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
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

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

    instance.new_iteration(1).await.unwrap();
    let b1_tx_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let b1_tx_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Block 2: Select iteration 2 from Block 1
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

    instance.new_iteration(1).await.unwrap();
    let b2_tx_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let b2_tx_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Block 3: Select iteration 1 from Block 2
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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

    instance.new_iteration(1).await.unwrap();
    let b1_i1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let b1_i2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Block 2: Select iteration 1
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    instance.new_iteration(1).await.unwrap();
    let b2_i1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let b2_i2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Block 3: Switch to iteration 2
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

    instance.new_iteration(1).await.unwrap();
    let b3_i1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    instance.new_iteration(2).await.unwrap();
    let b3_i2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Block 4: Switch back to iteration 1
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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
    instance.new_iteration(1).await.unwrap();
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
    instance.new_iteration(2).await.unwrap();
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
    instance.new_iteration(1).await.unwrap();
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
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

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

    info!("Intra-block storage persistence verified");

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
