#![allow(clippy::field_reassign_with_default)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]

use super::*;
use crate::{
    engine::system_calls::HISTORY_BUFFER_LENGTH,
    utils::TestDbError,
};
use alloy::eips::{
    eip2935::{
        HISTORY_SERVE_WINDOW,
        HISTORY_STORAGE_ADDRESS,
    },
    eip4788::BEACON_ROOTS_ADDRESS,
};
use assertion_executor::{
    ExecutorConfig,
    db::{
        BlockHashStore,
        VersionDb,
    },
    primitives::{
        AccountInfo,
        ExecutionResult,
    },
    store::AssertionStore,
    test_utils::{
        bytecode,
        counter_call,
    },
};
use revm::{
    DatabaseRef,
    bytecode::Bytecode,
    context::{
        BlockEnv,
        TxEnv,
    },
    database::{
        CacheDB,
        DBErrorMarker,
        EmptyDBTyped,
    },
    primitives::{
        Address,
        B256,
        Bytes,
        Log,
        TxKind,
        U256,
        keccak256,
        uint,
    },
};
use std::{
    cell::RefCell,
    collections::HashMap,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
};
use thiserror::Error;

impl<DB> CoreEngine<DB> {
    /// Creates a new `CoreEngine` for testing purposes.
    /// Not to be used for anything but tests.
    #[cfg(test)]
    pub fn new_test() -> Self {
        let (_, tx_receiver) = flume::unbounded();
        let sources = Arc::new(Sources::new(vec![], 10));
        Self {
            cache: OverlayDb::new(None),
            canonical_db: None,
            current_block_iterations: HashMap::new(),
            tx_receiver,
            incident_sender: None,
            report_incidents: false,
            assertion_executor: AssertionExecutor::new(
                ExecutorConfig::default(),
                AssertionStore::new_ephemeral(),
            ),
            sources: sources.clone(),
            transaction_results: TransactionsResults::new(TransactionsState::new(), 10),
            block_metrics: BlockMetrics::new(),
            state_sources_sync_timeout: Duration::from_millis(100),
            check_sources_available: true,
            overlay_cache_invalidation_every_block: false,
            system_calls: SystemCalls,
            #[cfg(feature = "cache_validation")]
            processed_transactions: Arc::new(
                moka::sync::Cache::builder().max_capacity(128).build(),
            ),
            #[cfg(feature = "cache_validation")]
            cache_checker: None,
            #[cfg(feature = "cache_validation")]
            iteration_pending_processed_transactions: HashMap::new(),
            sources_monitoring: monitoring::sources::Sources::new(
                sources,
                Duration::from_millis(20),
            ),
            current_head: U256::from(0),
            cache_metrics_handle: None,
            assertion_failure_cache: moka::sync::Cache::builder()
                .max_capacity(super::ASSERTION_FAILURE_CACHE_SIZE)
                .build(),
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

    /// Get the transaction count for a specific block iteration.
    #[cfg(test)]
    pub fn get_n_transactions(&self, block_execution_id: &BlockExecutionId) -> Option<u64> {
        self.current_block_iterations
            .get(block_execution_id)
            .map(super::BlockIterationData::valid_count)
    }
}

async fn create_test_engine_with_timeout(
    timeout: Duration,
) -> (
    CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
    flume::Sender<TxQueueContents>,
) {
    let (tx_sender, tx_receiver) = flume::unbounded();
    let underlying_db = CacheDB::new(EmptyDBTyped::default());
    let state = OverlayDb::new(Some(std::sync::Arc::new(underlying_db)));
    let assertion_store = AssertionStore::new_ephemeral();
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
        None,
        #[cfg(feature = "cache_validation")]
        None,
    )
    .await;
    (engine, tx_sender)
}

async fn create_test_engine() -> (
    CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
    flume::Sender<TxQueueContents>,
) {
    create_test_engine_with_timeout(Duration::from_millis(100)).await
}

fn create_test_block_env() -> BlockEnv {
    BlockEnv {
        number: U256::from(1),
        basefee: 0, // Set basefee to 0 to avoid balance issues
        ..Default::default()
    }
}

fn record_validation_result(
    engine: &mut CoreEngine<CacheDB<EmptyDBTyped<TestDbError>>>,
    tx_execution_id: TxExecutionId,
    is_valid: bool,
) {
    let result = TransactionResult::ValidationCompleted {
        execution_result: ExecutionResult::Success {
            reason: revm::context::result::SuccessReason::Stop,
            gas_used: 0,
            gas_refunded: 0,
            logs: Vec::new(),
            output: revm::context::result::Output::Call(Bytes::new()),
        },
        is_valid,
    };
    engine
        .transaction_results
        .add_transaction_result(tx_execution_id, &result);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_core_engine_errors_when_no_synced_sources() {
    let (engine, tx_sender) = create_test_engine_with_timeout(Duration::from_millis(10)).await;

    let sources = engine.sources.clone();
    let shutdown = Arc::new(AtomicBool::new(false));
    let (engine_handle, _engine_exit_rx) = engine
        .spawn(shutdown.clone())
        .expect("failed to spawn engine thread");

    // Create iteration with block number 1
    let mut block_env = BlockEnv::default();
    block_env.number = U256::from(1); // Match the transaction's block number

    let new_iteration = queue::NewIteration {
        block_env,
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };

    let queue_tx = queue::QueueTransaction {
        tx_execution_id: TxExecutionId::new(U256::from(1), 0, B256::from([0x11; 32]), 0),
        tx_env: TxEnv::default(),
        prev_tx_hash: None,
    };

    tx_sender
        .send(TxQueueContents::NewIteration(new_iteration))
        .expect("queue send should succeed");
    tx_sender
        .send(TxQueueContents::Tx(queue_tx))
        .expect("queue send should succeed");

    assert_eq!(sources.iter_synced_sources().count(), 0);
    shutdown.store(true, Ordering::Release);
    let _ = engine_handle.join();
}

#[tokio::test]
async fn test_tx_block_mismatch_yields_validation_error() {
    let (mut engine, _) = create_test_engine().await;

    // Set up and commit block 1 to establish current_head = 1
    let block_env_1 = BlockEnv {
        number: U256::from(1),
        basefee: 0,
        ..Default::default()
    };

    let queue_iteration_1 = queue::NewIteration {
        block_env: block_env_1,
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };
    engine.process_iteration(&queue_iteration_1).unwrap();

    let queue_commit_1 = queue::CommitHead::new(
        U256::from(1),
        0,
        None,
        0,
        B256::ZERO,
        Some(B256::ZERO),
        U256::from(1),
    );
    engine
        .process_commit_head(&queue_commit_1, &mut 0, &mut Instant::now())
        .unwrap();

    // Now current_head = 1, so expected_block_number = 2
    let expected_block_number = engine.current_head + U256::from(1); // = 2

    // Create an iteration for a mismatched block (e.g., block 5)
    let mismatched_block_number = 5u64;
    let block_env_mismatched = BlockEnv {
        number: U256::from(mismatched_block_number),
        basefee: 0,
        ..Default::default()
    };

    let queue_iteration_mismatch = queue::NewIteration {
        block_env: block_env_mismatched,
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };
    let iteration_result = engine.process_iteration(&queue_iteration_mismatch);
    assert!(
        matches!(iteration_result, Err(EngineError::IterationError)),
        "mismatched block iteration should be rejected"
    );

    // Send transaction for the mismatched block
    let tx_execution_id = TxExecutionId::new(
        U256::from(mismatched_block_number),
        0,
        B256::from([0x42; 32]),
        0,
    );
    let queue_transaction = queue::QueueTransaction {
        tx_execution_id,
        tx_env: TxEnv::default(),
        prev_tx_hash: None,
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
        .send_and_verify_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    instance
        .send_and_verify_reverting_create_tx()
        .await
        .unwrap();
}

#[crate::utils::engine_test(mock)]
async fn test_execute_assertion_passing_failing_pair(mut instance: crate::utils::LocalInstance) {
    instance
        .send_assertion_passing_failing_pair()
        .await
        .unwrap();
}

#[crate::utils::engine_test(mock)]
async fn test_assertion_invalid_tx_count_matches_sent_no_reorg(
    mut instance: crate::utils::LocalInstance,
) {
    let basefee = 10u64;
    let caller = counter_call().caller;
    let tx_block_number = instance.block_number + U256::from(1);
    let block_exec_id = BlockExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
    };

    let mut tx_pass = counter_call();
    tx_pass.nonce = instance.next_nonce(caller, block_exec_id);
    tx_pass.gas_price = basefee.into();

    let mut tx_fail = counter_call();
    tx_fail.nonce = instance.next_nonce(caller, block_exec_id);
    tx_fail.gas_price = basefee.into();

    let hash_pass = B256::random();
    let hash_fail = B256::random();

    instance
        .send_block_with_txs(vec![(hash_pass, tx_pass), (hash_fail, tx_fail)])
        .await
        .unwrap();

    // Capture cache resets after initial setup. The first send_block_with_txs() may cause one reset
    // because CommitHead arrives before any NewIteration exists for that block.
    let initial_cache_resets = instance.cache_reset_count();

    let tx_pass_id = TxExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
        tx_hash: hash_pass,
        index: 0,
    };
    let tx_fail_id = TxExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
        tx_hash: hash_fail,
        index: 1,
    };

    match instance.wait_for_transaction_processed(&tx_pass_id).await {
        Ok(TransactionResult::ValidationCompleted { is_valid, .. }) => {
            assert!(is_valid, "First transaction should pass assertions");
        }
        Ok(TransactionResult::ValidationError(e)) => panic!("{e}"),
        Err(e) => panic!("{e:?}"),
    }

    assert!(
        instance.is_transaction_invalid(&tx_fail_id).await.unwrap(),
        "Second transaction should fail assertions"
    );

    instance
        .new_block_with_hashes(B256::ZERO, None)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(25))
        .await;

    assert_eq!(
        instance.cache_reset_count(),
        initial_cache_resets,
        "Cache should remain valid when the sequencer keeps the assertion-invalid transaction"
    );
}

#[crate::utils::engine_test(mock)]
async fn test_assertion_invalid_tx_reorg_keeps_count_in_sync(
    mut instance: crate::utils::LocalInstance,
) {
    let basefee = 10u64;
    let caller = counter_call().caller;
    let tx_block_number = instance.block_number + U256::from(1);
    let block_exec_id = BlockExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
    };

    let mut tx_pass = counter_call();
    tx_pass.nonce = instance.next_nonce(caller, block_exec_id);
    tx_pass.gas_price = basefee.into();

    let mut tx_fail_1 = counter_call();
    tx_fail_1.nonce = instance.next_nonce(caller, block_exec_id);
    tx_fail_1.gas_price = basefee.into();

    let mut tx_fail_2 = counter_call();
    tx_fail_2.nonce = instance.next_nonce(caller, block_exec_id);
    tx_fail_2.gas_price = basefee.into();

    let hash_pass = B256::random();
    let hash_fail_1 = B256::random();
    let hash_fail_2 = B256::random();

    instance
        .send_block_with_txs(vec![
            (hash_pass, tx_pass),
            (hash_fail_1, tx_fail_1),
            (hash_fail_2, tx_fail_2),
        ])
        .await
        .unwrap();

    // Capture cache resets after initial setup. The first send_block_with_txs() may cause one reset
    // because CommitHead arrives before any NewIteration exists for that block.
    let initial_cache_resets = instance.cache_reset_count();

    let tx_pass_id = TxExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
        tx_hash: hash_pass,
        index: 0,
    };
    let tx_fail_1_id = TxExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
        tx_hash: hash_fail_1,
        index: 1,
    };
    let tx_fail_2_id = TxExecutionId {
        block_number: tx_block_number,
        iteration_id: instance.iteration_id,
        tx_hash: hash_fail_2,
        index: 2,
    };

    match instance.wait_for_transaction_processed(&tx_pass_id).await {
        Ok(TransactionResult::ValidationCompleted { is_valid, .. }) => {
            assert!(is_valid, "First transaction should pass assertions");
        }
        Ok(TransactionResult::ValidationError(e)) => panic!("{e}"),
        Err(e) => panic!("{e:?}"),
    }

    assert!(
        instance
            .is_transaction_invalid(&tx_fail_1_id)
            .await
            .unwrap(),
        "Second transaction should fail assertions"
    );
    assert!(
        instance
            .is_transaction_invalid(&tx_fail_2_id)
            .await
            .unwrap(),
        "Third transaction should fail assertions"
    );

    instance.send_reorg(tx_fail_2_id).await.unwrap();
    assert!(
        instance
            .is_transaction_removed(&tx_fail_2_id)
            .await
            .unwrap(),
        "Reorg should remove the last assertion-invalid transaction"
    );

    instance.transport.set_n_transactions(2);
    instance.transport.set_last_tx_hash(Some(hash_fail_1));

    instance
        .new_block_with_hashes(B256::ZERO, None)
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(25))
        .await;

    assert_eq!(
        instance.cache_reset_count(),
        initial_cache_resets,
        "Cache should remain valid after reorging the last invalid transaction"
    );
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
async fn test_core_engine_reorg_scenarios(mut instance: crate::utils::LocalInstance) {
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

    // Valid reorg for the most recent tx should succeed (engine keeps running)
    instance
        .send_reorg(tx2)
        .await
        .expect("reorg of tip tx should succeed");

    // Reorg for the previous tx (tx1) should be rejected
    // Because reorg hashes must match the tail of executed transactions
    tx1.block_number += U256::from(1);
    assert!(
        instance.send_reorg(tx1).await.is_err(),
        "Reorg with wrong hash should be rejected and exit engine"
    );
}

#[crate::utils::engine_test(all)]
async fn test_core_engine_reorg_followed_by_blockenv_with_last_tx_hash(
    mut instance: crate::utils::LocalInstance,
) {
    // Start by sending a block environment so subsequent dry transactions share the same block.
    instance
        .new_block()
        .await
        .expect("initial blockenv should be accepted");

    // Capture cache resets after initial setup. The first new_block() may cause one reset
    // because CommitHead arrives before any NewIteration exists for that block.
    let initial_cache_resets = instance.cache_reset_count();

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

    // Reorg the second transaction; this should succeed and remove it from the iteration.
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

#[tokio::test]
async fn test_execute_reorg_depth_truncates_tail() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx1_hash = B256::from([0x11; 32]);
    let tx2_hash = B256::from([0x22; 32]);
    let tx3_hash = B256::from([0x33; 32]);

    let tx1 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx2_hash,
        index: 1,
    };
    let tx3 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx3_hash,
        index: 2,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    // Add 3 successful transactions to the version db
    version_db.commit(EvmState::default());
    version_db.commit(EvmState::default());
    version_db.commit(EvmState::default());

    let current_block_iteration_id = BlockIterationData {
        version_db,
        executed_txs: vec![tx1, tx2, tx3],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_id);

    record_validation_result(&mut engine, tx1, true);
    record_validation_result(&mut engine, tx2, true);
    record_validation_result(&mut engine, tx3, true);

    let reorg = queue::ReorgRequest {
        tx_execution_id: tx3,
        tx_hashes: vec![tx2_hash, tx3_hash],
    };

    assert!(engine.execute_reorg(&reorg).is_ok());

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert_eq!(current_block_iteration.executed_txs, vec![tx1]);
    assert_eq!(current_block_iteration.depth(), 1);
    assert_eq!(current_block_iteration.len(), 1);
}

/// Tests that reorg works correctly when the remaining `commit_log` contains
/// a failed transaction (None state). This is a regression test for a bug
/// where `rebuild_fork_db` would fail on None entries.
#[tokio::test]
async fn test_execute_reorg_with_failed_tx_in_remaining_commit_log() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx1_hash = B256::from([0x11; 32]);
    let tx2_hash = B256::from([0x22; 32]);
    let tx3_hash = B256::from([0x33; 32]);

    let tx1 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx2_hash,
        index: 1,
    };
    let tx3 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx3_hash,
        index: 2,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    // TX1 succeeds, TX2 fails (None), TX3 succeeds
    version_db.commit(EvmState::default()); // TX1 succeeded
    version_db.commit_empty(); // TX2 failed (EVM error, no state)
    version_db.commit(EvmState::default()); // TX3 succeeded

    let current_block_iteration_data = BlockIterationData {
        version_db,
        executed_txs: vec![tx1, tx2, tx3],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_data);

    record_validation_result(&mut engine, tx1, true);
    record_validation_result(&mut engine, tx2, false);
    record_validation_result(&mut engine, tx3, true);

    // Reorg TX3 only - should leave TX1 (Some) and TX2 (None) in commit_log
    let reorg = queue::ReorgRequest {
        tx_execution_id: tx3,
        tx_hashes: vec![tx3_hash],
    };

    // This should succeed even though TX2 has None in commit_log
    let result = engine.execute_reorg(&reorg);
    assert!(
        result.is_ok(),
        "Reorg should succeed even with failed tx (None) in remaining commit_log: {result:?}"
    );

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert_eq!(current_block_iteration.executed_txs, vec![tx1, tx2]);
    assert_eq!(current_block_iteration.depth(), 2);
    assert_eq!(current_block_iteration.len(), 2);
}

/// Tests that missing transaction results do not affect `n_transactions` tracking.
#[tokio::test]
async fn test_execute_reorg_missing_transaction_result_not_counted_valid() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx1_hash = B256::from([0x11; 32]);
    let tx2_hash = B256::from([0x22; 32]);

    let tx1 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx2_hash,
        index: 1,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    version_db.commit(EvmState::default());
    version_db.commit(EvmState::default());

    let current_block_iteration_data = BlockIterationData {
        version_db,
        executed_txs: vec![tx1, tx2],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_data);

    record_validation_result(&mut engine, tx1, true);

    let reorg = queue::ReorgRequest {
        tx_execution_id: tx2,
        tx_hashes: vec![tx2_hash],
    };

    let result = engine.execute_reorg(&reorg);
    assert!(
        result.is_ok(),
        "Reorg should succeed even with missing tx results: {result:?}"
    );

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert_eq!(current_block_iteration.executed_txs, vec![tx1]);
    assert_eq!(current_block_iteration.len(), 1);
}

/// Tests that a reorg can remove ALL transactions, resetting to the base state.
/// This is an edge case where depth equals the total number of transactions.
#[tokio::test]
async fn test_execute_reorg_removes_all_transactions() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx1_hash = B256::from([0x11; 32]);
    let tx2_hash = B256::from([0x22; 32]);
    let tx3_hash = B256::from([0x33; 32]);

    let tx1 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx2_hash,
        index: 1,
    };
    let tx3 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx3_hash,
        index: 2,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    version_db.commit(EvmState::default());
    version_db.commit(EvmState::default());
    version_db.commit(EvmState::default());

    let current_block_iteration_data = BlockIterationData {
        version_db,
        executed_txs: vec![tx1, tx2, tx3],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_data);

    record_validation_result(&mut engine, tx1, true);
    record_validation_result(&mut engine, tx2, true);
    record_validation_result(&mut engine, tx3, true);

    // Reorg ALL 3 transactions
    let reorg = queue::ReorgRequest {
        tx_execution_id: tx3,
        tx_hashes: vec![tx1_hash, tx2_hash, tx3_hash],
    };

    let result = engine.execute_reorg(&reorg);
    assert!(
        result.is_ok(),
        "Reorg should succeed when removing all transactions: {result:?}"
    );

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert!(
        current_block_iteration.executed_txs.is_empty(),
        "All transactions should be removed"
    );
    assert_eq!(current_block_iteration.depth(), 0, "Depth should be 0");
    assert_eq!(current_block_iteration.len(), 0);
}

/// Tests reorg when all transactions in the commit log were failures (`commit_empty`).
/// This ensures the rollback handles the case where no state was ever committed.
#[tokio::test]
async fn test_execute_reorg_with_all_failed_transactions() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx1_hash = B256::from([0x11; 32]);
    let tx2_hash = B256::from([0x22; 32]);

    let tx1 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx1_hash,
        index: 0,
    };
    let tx2 = TxExecutionId {
        block_number,
        iteration_id,
        tx_hash: tx2_hash,
        index: 1,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    // Both transactions failed (no state changes)
    version_db.commit_empty();
    version_db.commit_empty();

    let current_block_iteration_data = BlockIterationData {
        version_db,
        executed_txs: vec![tx1, tx2],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_data);

    record_validation_result(&mut engine, tx1, false);
    record_validation_result(&mut engine, tx2, false);

    // Reorg TX2 only
    let reorg = queue::ReorgRequest {
        tx_execution_id: tx2,
        tx_hashes: vec![tx2_hash],
    };

    let result = engine.execute_reorg(&reorg);
    assert!(
        result.is_ok(),
        "Reorg should succeed with all failed transactions: {result:?}"
    );

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert_eq!(current_block_iteration.executed_txs, vec![tx1]);
    assert_eq!(current_block_iteration.depth(), 1);
    assert_eq!(current_block_iteration.len(), 1);
}

/// Tests a deep reorg (depth > 1) with an alternating success/failure pattern.
/// Pattern: [Some, None, Some, None, Some] - reorg the last 4 (depth=4)
/// Expected: Only the first transaction remains (index 0)
#[tokio::test]
async fn test_execute_reorg_deep_with_alternating_success_failure() {
    let (mut engine, _) = create_test_engine().await;

    let block_number = U256::from(1);
    let iteration_id = 1;
    let block_execution_id = BlockExecutionId {
        block_number,
        iteration_id,
    };

    let tx_hashes: Vec<B256> = (0..5).map(|i| B256::from([i as u8 + 0x11; 32])).collect();
    let txs: Vec<TxExecutionId> = tx_hashes
        .iter()
        .enumerate()
        .map(|(i, &tx_hash)| {
            TxExecutionId {
                block_number,
                iteration_id,
                tx_hash,
                index: i as u64,
            }
        })
        .collect();

    let mut version_db = VersionDb::new(engine.cache.clone());
    // Alternating pattern: Some, None, Some, None, Some
    version_db.commit(EvmState::default()); // TX0 succeeded
    version_db.commit_empty(); // TX1 failed
    version_db.commit(EvmState::default()); // TX2 succeeded
    version_db.commit_empty(); // TX3 failed
    version_db.commit(EvmState::default()); // TX4 succeeded

    let current_block_iteration_data = BlockIterationData {
        version_db,
        executed_txs: txs.clone(),
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: block_number,
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .insert(block_execution_id, current_block_iteration_data);

    record_validation_result(&mut engine, txs[0], true);
    record_validation_result(&mut engine, txs[1], false);
    record_validation_result(&mut engine, txs[2], true);
    record_validation_result(&mut engine, txs[3], false);
    record_validation_result(&mut engine, txs[4], true);

    // Deep reorg: remove TX1, TX2, TX3, TX4
    let reorg = queue::ReorgRequest {
        tx_execution_id: txs[4],
        tx_hashes: tx_hashes[1..5].to_vec(),
    };

    let result = engine.execute_reorg(&reorg);
    assert!(
        result.is_ok(),
        "Deep reorg with alternating pattern should succeed: {result:?}"
    );

    let current_block_iteration = engine
        .current_block_iterations
        .get(&block_execution_id)
        .expect("block iteration should exist");
    assert_eq!(
        current_block_iteration.executed_txs,
        vec![txs[0]],
        "Only TX0 should remain"
    );
    assert_eq!(current_block_iteration.depth(), 1, "Depth should be 1");
    assert_eq!(current_block_iteration.len(), 1);
}

#[allow(clippy::too_many_lines)]
#[tokio::test]
async fn test_database_commit_and_block_env_requirements() {
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
        version_db: VersionDb::new(engine.cache.clone()),
        executed_txs: Vec::new(),
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
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
    // Commit the iteration fork into the underlying cache
    engine.finalize_previous_block(tx_execution_id.as_block_execution_id());

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

    // Test engine requires block env before tx
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

#[crate::utils::engine_test(mock)]
async fn test_committed_nonce_persists_across_cache_flush(
    mut instance: crate::utils::LocalInstance,
) {
    // Block 1 (commit empty, start block 2)
    instance.new_block().await.unwrap();

    // Block 2: execute a tx so the canonical nonce increments.
    instance.new_iteration(1).await.unwrap();
    let tx1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(instance.is_transaction_successful(&tx1).await.unwrap());

    // Commit block 2 (start block 3)
    instance.new_block().await.unwrap();

    let account = instance.default_account();
    let nonce_after_commit = instance
        .db()
        .with_read(|db| db.basic_ref(account).unwrap().unwrap().nonce);
    assert_eq!(
        nonce_after_commit, 1,
        "canonical db should reflect the committed nonce"
    );

    // Force a cache flush on the next commit.
    instance.transport.set_n_transactions(999);
    instance
        .expect_cache_flush(|instance| {
            Box::pin(async move {
                instance.new_block().await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    let nonce_after_flush = instance
        .db()
        .with_read(|db| db.basic_ref(account).unwrap().unwrap().nonce);
    assert_eq!(
        nonce_after_flush, 1,
        "canonical db nonce should persist across cache flush"
    );

    // Next tx should use nonce=1 and succeed.
    instance.new_iteration(1).await.unwrap();
    let tx2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(instance.is_transaction_successful(&tx2).await.unwrap());
}

#[derive(Debug)]
struct NullSource;

impl DatabaseRef for NullSource {
    type Error = crate::cache::sources::SourceError;

    fn basic_ref(&self, _address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(None)
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::default())
    }

    fn storage_ref(&self, _address: Address, _index: U256) -> Result<U256, Self::Error> {
        Ok(U256::ZERO)
    }

    fn block_hash_ref(&self, _number: u64) -> Result<B256, Self::Error> {
        Ok(B256::ZERO)
    }
}

impl crate::cache::sources::Source for NullSource {
    fn is_synced(&self, _min_synced_block: U256, _latest_head: U256) -> bool {
        true
    }

    fn name(&self) -> crate::cache::sources::SourceName {
        crate::cache::sources::SourceName::Other
    }

    fn update_cache_status(&self, _min_synced_block: U256, _latest_head: U256) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn test_canonical_db_nonce_committed_on_commit_head() {
    use crate::utils::local_instance_db::LocalInstanceDb;

    let sources: Vec<Arc<dyn crate::cache::sources::Source>> = vec![Arc::new(NullSource)];
    let sources = Arc::new(Sources::new(sources, 10));
    let mut canonical = CacheDB::new(sources.clone());

    let caller = Address::from([0x03; 20]);
    canonical.insert_account_info(
        caller,
        AccountInfo {
            balance: U256::MAX,
            nonce: 0,
            ..Default::default()
        },
    );

    let canonical = Arc::new(LocalInstanceDb::new(canonical));
    let state = OverlayDb::new(Some(canonical.clone()));

    let (tx_sender, tx_receiver) = flume::unbounded();
    drop(tx_sender);

    let assertion_executor =
        AssertionExecutor::new(ExecutorConfig::default(), AssertionStore::new_ephemeral());
    let state_results = TransactionsState::new();

    let mut engine = CoreEngine::new(
        state,
        sources,
        tx_receiver,
        assertion_executor,
        state_results,
        10,
        Duration::from_millis(100),
        Duration::from_millis(20),
        false,
        None,
        #[cfg(feature = "cache_validation")]
        None,
    )
    .await;
    engine.set_canonical_db_for_tests(canonical.clone());

    let block_env = BlockEnv {
        number: U256::from(1),
        basefee: 0,
        ..Default::default()
    };
    engine
        .process_iteration(&NewIteration::new(
            1,
            block_env,
            Some(B256::ZERO),
            Some(B256::ZERO),
        ))
        .unwrap();

    let tx_hash = B256::from([0x42; 32]);
    let tx_execution_id = TxExecutionId::new(U256::from(1), 1, tx_hash, 0);
    let tx_env = TxEnv {
        caller,
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Create,
        value: uint!(0_U256),
        data: Bytes::new(),
        nonce: 0,
        ..Default::default()
    };
    engine
        .execute_transaction(tx_execution_id, &tx_env)
        .unwrap();

    let tx_result = engine
        .get_transaction_result_cloned(&tx_execution_id)
        .expect("engine should store transaction result");
    match tx_result {
        TransactionResult::ValidationCompleted { .. } => {}
        TransactionResult::ValidationError(err) => {
            panic!("transaction validation error: {err}");
        }
    }

    let block_id = tx_execution_id.as_block_execution_id();
    let iteration = engine.current_block_iterations.get(&block_id).unwrap();
    let nonce_in_iteration = iteration
        .version_db
        .basic_ref(caller)
        .unwrap()
        .unwrap()
        .nonce;
    assert_eq!(nonce_in_iteration, 1, "iteration db should increment nonce");

    #[cfg(any(test, feature = "bench-utils"))]
    {
        assert_eq!(
            iteration.executed_state_deltas.len(),
            1,
            "engine should record one state delta"
        );
        let delta = iteration.executed_state_deltas[0].as_ref().unwrap();
        let caller_account = delta
            .get(&caller)
            .expect("delta should include caller account");
        assert!(
            caller_account.is_touched(),
            "delta caller account should be marked touched"
        );
        assert_eq!(
            caller_account.info.nonce, 1,
            "delta should reflect incremented nonce"
        );
    }

    let commit_head = CommitHead::new(
        U256::from(1),
        1,
        Some(tx_hash),
        1,
        B256::random(),
        Some(B256::random()),
        U256::from(1234567890),
    );

    let mut processed_blocks = 0;
    let mut block_processing_time = Instant::now();
    engine
        .process_commit_head(
            &commit_head,
            &mut processed_blocks,
            &mut block_processing_time,
        )
        .unwrap();

    let nonce_after_commit = canonical.with_read(|db| db.basic_ref(caller).unwrap().unwrap().nonce);
    assert_eq!(nonce_after_commit, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[allow(clippy::too_many_lines)]
async fn test_canonical_db_nonce_committed_after_initial_empty_block() {
    use crate::utils::local_instance_db::LocalInstanceDb;

    let sources: Vec<Arc<dyn crate::cache::sources::Source>> = vec![Arc::new(NullSource)];
    let sources = Arc::new(Sources::new(sources, 10));
    let mut canonical = CacheDB::new(sources.clone());

    let caller = Address::from([0x03; 20]);
    canonical.insert_account_info(
        caller,
        AccountInfo {
            balance: U256::MAX,
            nonce: 0,
            ..Default::default()
        },
    );

    let canonical = Arc::new(LocalInstanceDb::new(canonical));
    let state = OverlayDb::new(Some(canonical.clone()));

    let (tx_sender, tx_receiver) = flume::unbounded();
    drop(tx_sender);

    let assertion_executor =
        AssertionExecutor::new(ExecutorConfig::default(), AssertionStore::new_ephemeral());
    let state_results = TransactionsState::new();

    let mut engine = CoreEngine::new(
        state,
        sources,
        tx_receiver,
        assertion_executor,
        state_results,
        10,
        Duration::from_millis(100),
        Duration::from_millis(20),
        false,
        None,
        #[cfg(feature = "cache_validation")]
        None,
    )
    .await;
    engine.set_canonical_db_for_tests(canonical.clone());

    // Commit block 1 without ever having created an iteration (matches `LocalInstance::new_block()` behavior).
    let commit_head_1 = CommitHead::new(
        U256::from(1),
        1,
        None,
        0,
        B256::random(),
        Some(B256::random()),
        U256::from(1234567890),
    );
    let mut processed_blocks = 0;
    let mut block_processing_time = Instant::now();
    engine
        .process_commit_head(
            &commit_head_1,
            &mut processed_blocks,
            &mut block_processing_time,
        )
        .unwrap();

    // Block 2 iteration and tx.
    let block_env_2 = BlockEnv {
        number: U256::from(2),
        basefee: 0,
        ..Default::default()
    };
    engine
        .process_iteration(&NewIteration::new(
            1,
            block_env_2,
            Some(B256::ZERO),
            Some(B256::ZERO),
        ))
        .unwrap();

    let tx_hash = B256::from([0x42; 32]);
    let tx_execution_id = TxExecutionId::new(U256::from(2), 1, tx_hash, 0);
    let tx_env = TxEnv {
        caller,
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Create,
        value: uint!(0_U256),
        data: Bytes::new(),
        nonce: 0,
        ..Default::default()
    };
    engine
        .execute_transaction(tx_execution_id, &tx_env)
        .unwrap();

    let tx_result = engine
        .get_transaction_result_cloned(&tx_execution_id)
        .expect("engine should store transaction result");
    match tx_result {
        TransactionResult::ValidationCompleted { .. } => {}
        TransactionResult::ValidationError(err) => {
            panic!("transaction validation error: {err}");
        }
    }

    let commit_head_2 = CommitHead::new(
        U256::from(2),
        1,
        Some(tx_hash),
        1,
        B256::random(),
        Some(B256::random()),
        U256::from(1234567890),
    );

    engine
        .process_commit_head(
            &commit_head_2,
            &mut processed_blocks,
            &mut block_processing_time,
        )
        .unwrap();

    let nonce_after_commit = canonical.with_read(|db| db.basic_ref(caller).unwrap().unwrap().nonce);
    assert_eq!(nonce_after_commit, 1);
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
            .fallback_eth_rpc_source_http_mock
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

#[tokio::test]
async fn test_failed_transaction_commit() {
    let (mut engine, _) = create_test_engine().await;
    let tx_hash = B256::from([0x44; 32]);

    let block_execution_id = BlockExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
    };

    let tx_execution_id = TxExecutionId {
        block_number: U256::from(1),
        iteration_id: 1,
        tx_hash,
        index: 0,
    };

    let mut version_db = VersionDb::new(engine.cache.clone());
    // Add a failed transaction (commit_empty) to simulate pending state
    version_db.commit_empty();

    let current_block_iteration_id = BlockIterationData {
        version_db,
        executed_txs: vec![tx_execution_id],
        executed_state_deltas: Vec::new(),
        incident_txs: Vec::new(),
        block_env: BlockEnv {
            number: U256::from(1),
            ..Default::default()
        },
    };

    engine
        .current_block_iterations
        .entry(block_execution_id)
        .or_insert(current_block_iteration_id);

    let queue_transaction = queue::QueueTransaction {
        tx_execution_id,
        tx_env: TxEnv::default(),
        prev_tx_hash: None,
    };

    let result = engine.process_transaction_event(queue_transaction);
    assert!(matches!(result, Err(EngineError::NothingToCommit)));
}

#[crate::utils::engine_test(all)]
async fn test_iteration_selection_and_commit(mut instance: crate::utils::LocalInstance) {
    info!("Testing multiple iterations with winner selection");

    instance.new_block().await.unwrap();

    // Capture cache count after initial setup (first new_block may cause one reset)
    let initial_cache_count = instance.cache_reset_count();

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
async fn test_multiple_transactions_and_iterations(mut instance: crate::utils::LocalInstance) {
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

    info!("Testing interleaved iteration IDs");

    // Block 3
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

    // Block 4: Select iteration 1 (should have 2 transactions)
    instance.new_block().await.unwrap();
    instance.new_iteration(2).await.unwrap();

    let tx_block4 = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(
        instance
            .is_transaction_successful(&tx_block4)
            .await
            .unwrap(),
        "Should correctly handle interleaved iterations"
    );
}

#[crate::utils::engine_test(all)]
async fn test_iteration_selection_with_transaction_count_mismatch(
    mut instance: crate::utils::LocalInstance,
) {
    info!("Testing iteration selection with wrong transaction count");

    // Block 1
    instance.new_block().await.unwrap();
    instance
        .wait_for_processing(Duration::from_millis(100))
        .await;

    // Iteration 1: 2 transactions
    instance.new_iteration(1).await.unwrap();
    let tx1_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(100))
        .await;

    let tx2_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    instance
        .wait_for_processing(Duration::from_millis(100))
        .await;

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
        .wait_for_processing(Duration::from_millis(100))
        .await;

    instance
        .expect_cache_flush(|instance| {
            Box::pin(async move {
                instance.new_block().await?;
                instance
                    .wait_for_processing(Duration::from_millis(100))
                    .await;
                Ok(())
            })
        })
        .await
        .unwrap();
}

#[crate::utils::engine_test(all)]
async fn test_reorg_in_specific_iteration(mut instance: crate::utils::LocalInstance) {
    info!("Testing reorg within iteration before block selection");

    // Block 1
    instance.new_block().await.unwrap();

    // Iteration 1: Send 3 transactions
    instance.new_iteration(1).await.unwrap();
    let tx1_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    let tx2_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    let tx3_iter1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Iteration 2: Send 2 transactions
    instance.new_iteration(2).await.unwrap();
    let tx1_iter2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    let tx2_iter2 = instance
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
            .is_transaction_successful(&tx3_iter1)
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
            .is_transaction_successful(&tx2_iter2)
            .await
            .unwrap()
    );

    // Reorg last transaction in iteration 1
    instance.new_iteration(1).await.unwrap();
    instance.send_reorg(tx3_iter1).await.unwrap();

    // Verify reorg only affected iteration 1
    assert!(
        instance.is_transaction_removed(&tx3_iter1).await.unwrap(),
        "Reorged transaction should be removed"
    );
    assert!(
        instance.get_transaction_result(&tx1_iter1).is_some(),
        "Other transactions in iteration 1 should remain"
    );
    assert!(
        instance.get_transaction_result(&tx2_iter1).is_some(),
        "Other transactions in iteration 1 should remain"
    );
    assert!(
        instance.get_transaction_result(&tx1_iter2).is_some(),
        "Iteration 2 should be unaffected"
    );
    assert!(
        instance.get_transaction_result(&tx2_iter2).is_some(),
        "Iteration 2 should be unaffected"
    );

    // Block 2: Select iteration 1 (now with 2 transactions)
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

/// Tests arbitrary depth reorgs using `send_reorg_depth` via LocalInstance.
///
/// This test verifies that the full transport stack (LocalInstance -> Driver -> Engine)
/// correctly handles depth-N reorgs where N > 1.
#[crate::utils::engine_test(mock)]
async fn test_arbitrary_depth_reorg_via_local_instance(mut instance: crate::utils::LocalInstance) {
    info!("Testing arbitrary depth reorg (depth=3) via LocalInstance");

    // Block 1
    instance.new_block().await.unwrap();

    // Execute 4 transactions in the same iteration
    let tx0 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .expect("tx0 should succeed");
    let tx1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .expect("tx1 should succeed");
    let tx2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .expect("tx2 should succeed");
    let tx3 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .expect("tx3 should succeed");

    // Verify all transactions succeeded
    assert!(instance.is_transaction_successful(&tx0).await.unwrap());
    assert!(instance.is_transaction_successful(&tx1).await.unwrap());
    assert!(instance.is_transaction_successful(&tx2).await.unwrap());
    assert!(instance.is_transaction_successful(&tx3).await.unwrap());

    // Reorg the last 3 transactions: removes tx1, tx2, tx3
    // tx_hashes must be in chronological order (oldest first)
    let tx_hashes = vec![tx1.tx_hash, tx2.tx_hash, tx3.tx_hash];
    instance
        .send_reorg_depth(tx3, tx_hashes)
        .await
        .expect("depth-3 reorg should succeed");

    // Verify tx0 is still present, tx1/tx2/tx3 are removed
    assert!(
        instance.get_transaction_result(&tx0).is_some(),
        "tx0 should still exist after reorg"
    );
    assert!(
        instance.is_transaction_removed(&tx1).await.unwrap(),
        "tx1 should be removed by reorg"
    );
    assert!(
        instance.is_transaction_removed(&tx2).await.unwrap(),
        "tx2 should be removed by reorg"
    );
    assert!(
        instance.is_transaction_removed(&tx3).await.unwrap(),
        "tx3 should be removed by reorg"
    );

    // Block 2: Continue with new transactions after reorg
    // Using new_block() to get fresh state and verify engine is healthy
    instance.new_block().await.unwrap();

    let tx_after_reorg = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .expect("tx after reorg should succeed");

    assert!(
        instance
            .is_transaction_successful(&tx_after_reorg)
            .await
            .unwrap(),
        "New transaction after reorg should succeed"
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
    instance.set_current_iteration_id(1);
    let (caller2, tx1_iter2) = instance.send_create_tx_with_cache_miss().await.unwrap();

    instance
        .wait_for_processing(Duration::from_millis(150))
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

fn selector_bytes(signature: &str) -> Bytes {
    let hash = keccak256(signature.as_bytes());
    Bytes::from(hash[..4].to_vec())
}

fn event_topic(signature: &str) -> B256 {
    keccak256(signature.as_bytes())
}

fn decode_u256_word(data: &[u8], index: usize) -> U256 {
    let start = index * 32;
    let end = start + 32;
    let word: [u8; 32] = data[start..end].try_into().expect("word must be 32 bytes");
    U256::from_be_bytes::<32>(word)
}

fn decode_b256_word(data: &[u8], index: usize) -> B256 {
    let start = index * 32;
    let end = start + 32;
    let word: [u8; 32] = data[start..end].try_into().expect("word must be 32 bytes");
    B256::from(word)
}

fn decode_bool_word(data: &[u8], index: usize) -> bool {
    decode_u256_word(data, index) == U256::from(1)
}

fn find_event_log(logs: &[Log], address: Address, topic0: B256) -> Option<&Log> {
    logs.iter().find(|log| {
        log.address == address && log.topics().first().is_some_and(|topic| *topic == topic0)
    })
}

fn find_last_event_log(logs: &[Log], address: Address, topic0: B256) -> Option<&Log> {
    logs.iter().rev().find(|log| {
        log.address == address && log.topics().first().is_some_and(|topic| *topic == topic0)
    })
}

#[crate::utils::engine_test(all)]
async fn test_eip4788_contract_reads_beacon_root(mut instance: crate::utils::LocalInstance) {
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    let constructor_bytecode = bytecode("BeaconRootTiming.sol:BeaconRootTiming");
    let deploy_tx = instance
        .send_successful_create_tx_dry(uint!(0_U256), constructor_bytecode)
        .await
        .unwrap();
    assert!(
        instance
            .is_transaction_successful(&deploy_tx)
            .await
            .unwrap()
    );

    let contract_address = instance.default_account().create(0);

    let expected_root = B256::repeat_byte(0x11);
    let expected_hash = B256::repeat_byte(0xaa);
    instance
        .new_block_with_hashes(expected_hash, Some(expected_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let call_tx = instance
        .send_call_tx_dry(contract_address, uint!(0_U256), selector_bytes("check()"))
        .await
        .unwrap();

    let result = instance
        .wait_for_transaction_processed(&call_tx)
        .await
        .unwrap();
    let (execution_result, is_valid) = match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("Result(uint256,uint256,bool,bytes32)");
    let log = find_event_log(&logs, contract_address, topic0)
        .expect("expected BeaconRootTiming Result event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 4,
        "unexpected BeaconRootTiming event data length"
    );

    let returned_root = decode_b256_word(data, 3);
    let call_success = decode_bool_word(data, 2);

    assert!(call_success, "staticcall to beacon roots should succeed");
    assert_eq!(
        returned_root, expected_root,
        "beacon root should match system call update"
    );

    let call_last_tx = instance
        .send_call_tx_dry(
            contract_address,
            uint!(0_U256),
            selector_bytes("check_last()"),
        )
        .await
        .unwrap();

    let result_last = instance
        .wait_for_transaction_processed(&call_last_tx)
        .await
        .unwrap();
    let (execution_result, is_valid) = match result_last {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("ResultLast(uint256,uint256,uint256,bool,bytes32)");
    let log = find_event_log(&logs, contract_address, topic0)
        .expect("expected BeaconRootTiming ResultLast event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 5,
        "unexpected BeaconRootTiming ResultLast event data length"
    );

    let call_success = decode_bool_word(data, 3);
    let returned_root = decode_b256_word(data, 4);
    assert!(
        !call_success && returned_root.is_zero(),
        "check_last should fail until the ring buffer has filled"
    );
}

#[crate::utils::engine_test(all)]
async fn test_eip2935_contract_reads_block_hash(mut instance: crate::utils::LocalInstance) {
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    let constructor_bytecode = bytecode("BlockHashTiming.sol:BlockHashTiming");
    let deploy_tx = instance
        .send_successful_create_tx_dry(uint!(0_U256), constructor_bytecode)
        .await
        .unwrap();
    assert!(
        instance
            .is_transaction_successful(&deploy_tx)
            .await
            .unwrap()
    );

    let contract_address = instance.default_account().create(0);

    let expected_hash = B256::repeat_byte(0xbb);
    let expected_root = B256::repeat_byte(0x22);
    instance
        .new_block_with_hashes(expected_hash, Some(expected_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let call_tx = instance
        .send_call_tx_dry(contract_address, uint!(0_U256), selector_bytes("check()"))
        .await
        .unwrap();

    let result = instance
        .wait_for_transaction_processed(&call_tx)
        .await
        .unwrap();
    let (execution_result, is_valid) = match result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("Result(uint256,uint256,bool,bytes32,bytes32)");
    let log = find_event_log(&logs, contract_address, topic0)
        .expect("expected BlockHashTiming Result event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 5,
        "unexpected BlockHashTiming event data length"
    );

    let returned_hash = decode_b256_word(data, 3);
    let expected_hash_from_opcode = decode_b256_word(data, 4);
    let call_success = decode_bool_word(data, 2);

    assert!(call_success, "staticcall to history storage should succeed");
    assert_eq!(
        returned_hash, expected_hash,
        "history storage should return the parent block hash"
    );
    assert_eq!(
        expected_hash_from_opcode, expected_hash,
        "BLOCKHASH opcode should match history storage hash"
    );

    let call_last_tx = instance
        .send_call_tx_dry(
            contract_address,
            uint!(0_U256),
            selector_bytes("check_last()"),
        )
        .await
        .unwrap();

    let result_last = instance
        .wait_for_transaction_processed(&call_last_tx)
        .await
        .unwrap();
    let (execution_result, is_valid) = match result_last {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("Result(uint256,uint256,bool,bytes32,bytes32)");
    let log = find_last_event_log(&logs, contract_address, topic0)
        .expect("expected BlockHashTiming Result event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 5,
        "unexpected BlockHashTiming Result event data length"
    );

    let call_success = decode_bool_word(data, 2);
    let returned_hash = decode_b256_word(data, 3);
    let expected_hash_from_opcode = decode_b256_word(data, 4);
    assert!(
        !call_success && returned_hash.is_zero() && expected_hash_from_opcode.is_zero(),
        "check_last should fail until the history window has filled"
    );
}

#[crate::utils::engine_test(all)]
async fn test_eip4788_with_skipped_slot_timestamps(mut instance: crate::utils::LocalInstance) {
    let base_timestamp = U256::from(1_234_567_890u64);
    let block_1_timestamp = base_timestamp + U256::from(10u64);
    let block_2_timestamp = base_timestamp + U256::from(1_000u64);

    instance.set_block_timestamp(U256::from(1u64), block_1_timestamp);
    instance.set_block_timestamp(U256::from(2u64), block_2_timestamp);

    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    let beacon_bytecode = bytecode("BeaconRootTiming.sol:BeaconRootTiming");

    let deploy_beacon = instance
        .send_successful_create_tx_dry(uint!(0_U256), beacon_bytecode)
        .await
        .unwrap();
    assert!(
        instance
            .is_transaction_successful(&deploy_beacon)
            .await
            .unwrap()
    );

    let beacon_address = instance.default_account().create(0);

    let parent_hash = B256::repeat_byte(0x55);
    let parent_root = B256::repeat_byte(0x66);
    instance
        .new_block_with_hashes(parent_hash, Some(parent_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let beacon_check = instance
        .send_call_tx_dry(beacon_address, uint!(0_U256), selector_bytes("check()"))
        .await
        .unwrap();

    let beacon_result = instance
        .wait_for_transaction_processed(&beacon_check)
        .await
        .unwrap();
    let (execution_result, is_valid) = match beacon_result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("Result(uint256,uint256,bool,bytes32)");
    let log = find_event_log(&logs, beacon_address, topic0)
        .expect("expected BeaconRootTiming Result event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 4,
        "unexpected BeaconRootTiming event data length"
    );

    let call_success = decode_bool_word(data, 2);
    let returned_root = decode_b256_word(data, 3);
    assert!(
        call_success,
        "staticcall to beacon roots should succeed even with skipped slots"
    );
    assert_eq!(
        returned_root, parent_root,
        "beacon root should match system call update with skipped slots"
    );

    assert!(
        call_success,
        "staticcall to beacon roots should succeed even with skipped slots"
    );
    assert_eq!(
        returned_root, parent_root,
        "beacon root should match system call update with skipped slots"
    );
}

#[crate::utils::engine_test(all)]
async fn test_eip2935_with_skipped_slot_timestamps(mut instance: crate::utils::LocalInstance) {
    let base_timestamp = U256::from(1_234_567_890u64);
    let block_1_timestamp = base_timestamp + U256::from(10u64);
    let block_2_timestamp = base_timestamp + U256::from(1_000u64);

    instance.set_block_timestamp(U256::from(1u64), block_1_timestamp);
    instance.set_block_timestamp(U256::from(2u64), block_2_timestamp);

    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    let blockhash_bytecode = bytecode("BlockHashTiming.sol:BlockHashTiming");
    let deploy_blockhash = instance
        .send_successful_create_tx_dry(uint!(0_U256), blockhash_bytecode)
        .await
        .unwrap();
    assert!(
        instance
            .is_transaction_successful(&deploy_blockhash)
            .await
            .unwrap()
    );

    let blockhash_address = instance.default_account().create(0);

    let parent_hash = B256::repeat_byte(0x55);
    let parent_root = B256::repeat_byte(0x66);
    instance
        .new_block_with_hashes(parent_hash, Some(parent_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let blockhash_check = instance
        .send_call_tx_dry(blockhash_address, uint!(0_U256), selector_bytes("check()"))
        .await
        .unwrap();

    let blockhash_result = instance
        .wait_for_transaction_processed(&blockhash_check)
        .await
        .unwrap();
    let (execution_result, is_valid) = match blockhash_result {
        TransactionResult::ValidationCompleted {
            execution_result,
            is_valid,
        } => (execution_result, is_valid),
        TransactionResult::ValidationError(error) => {
            panic!("transaction failed validation: {error}")
        }
    };
    assert!(is_valid, "transaction should be valid");

    let logs = match execution_result {
        ExecutionResult::Success { logs, .. } => logs,
        other => panic!("expected success, got {other:?}"),
    };

    let topic0 = event_topic("Result(uint256,uint256,bool,bytes32,bytes32)");
    let log = find_event_log(&logs, blockhash_address, topic0)
        .expect("expected BlockHashTiming Result event");

    let data = log.data.data.as_ref();
    assert_eq!(
        data.len(),
        32 * 5,
        "unexpected BlockHashTiming event data length"
    );

    let call_success = decode_bool_word(data, 2);
    let returned_hash = decode_b256_word(data, 3);
    let expected_hash_from_opcode = decode_b256_word(data, 4);
    assert!(call_success, "staticcall to history storage should succeed");
    assert_eq!(
        returned_hash, parent_hash,
        "history storage should return the parent block hash"
    );
    assert_eq!(
        expected_hash_from_opcode, parent_hash,
        "BLOCKHASH opcode should match history storage hash"
    );
}

#[crate::utils::engine_test(all)]
async fn test_system_calls_configurations(mut instance: crate::utils::LocalInstance) {
    // Block 1: Setup
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(instance.is_transaction_successful(&tx1).await.unwrap());

    // Block 2: No hashes (None, None)
    instance
        .new_block_with_hashes(B256::ZERO, None)
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(
        instance.is_transaction_successful(&tx2).await.unwrap(),
        "Engine should gracefully handle missing parent hash"
    );

    // Block 3: Only parent hash
    let hash = B256::repeat_byte(0xab);
    instance.new_block_with_hashes(hash, None).await.unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx3 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(
        instance.is_transaction_successful(&tx3).await.unwrap(),
        "Engine should gracefully handle missing beacon root"
    );

    // Block 4: Only beacon root
    let beacon_root = B256::repeat_byte(0xcd);
    instance
        .new_block_with_hashes(B256::ZERO, Some(beacon_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx4 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(
        instance.is_transaction_successful(&tx4).await.unwrap(),
        "Engine should continue working after beacon root system call"
    );

    // Block 5: Both parent hash and beacon root
    let hash = B256::repeat_byte(0xef);
    let beacon_root = B256::repeat_byte(0x12);
    instance
        .new_block_with_hashes(hash, Some(beacon_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx5 = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(
        instance.is_transaction_successful(&tx5).await.unwrap(),
        "Both system calls should be applied successfully"
    );
}

#[crate::utils::engine_test(all)]
async fn test_system_calls_sequential_blocks(mut instance: crate::utils::LocalInstance) {
    for i in 0..5 {
        instance.new_block().await.unwrap();
        instance.new_iteration(1).await.unwrap();

        let tx = instance
            .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
            .await
            .unwrap();
        assert!(
            instance.is_transaction_successful(&tx).await.unwrap(),
            "Transaction in block {} should succeed",
            i + 1
        );

        // Apply system calls with unique hashes for each block
        let hash = B256::from([i as u8; 32]);
        let beacon_root = B256::from([(i + 100) as u8; 32]);
        instance
            .new_block_with_hashes(hash, Some(beacon_root))
            .await
            .unwrap();
    }

    // Final verification
    instance.new_iteration(1).await.unwrap();
    let final_tx = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(
        instance.is_transaction_successful(&final_tx).await.unwrap(),
        "System calls should work across many sequential blocks"
    );
}

#[crate::utils::engine_test(all)]
async fn test_system_calls_after_cache_flush(mut instance: crate::utils::LocalInstance) {
    // Block 1
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();
    let tx1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(instance.is_transaction_successful(&tx1).await.unwrap());

    // Force a cache flush by providing wrong transaction count
    instance.transport.set_n_transactions(999);

    instance
        .expect_cache_flush(|instance| {
            Box::pin(async move {
                instance
                    .new_block_with_hashes(B256::repeat_byte(0x11), Some(B256::repeat_byte(0x22)))
                    .await?;
                Ok(())
            })
        })
        .await
        .unwrap();

    // After cache flush, system calls should still work
    instance.new_iteration(1).await.unwrap();
    let tx2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    assert!(instance.is_transaction_successful(&tx2).await.unwrap());

    // Block 3 with system calls
    let hash = B256::repeat_byte(0x33);
    let beacon_root = B256::repeat_byte(0x44);
    instance
        .new_block_with_hashes(hash, Some(beacon_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let tx3 = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(
        instance.is_transaction_successful(&tx3).await.unwrap(),
        "System calls should work after cache flush"
    );
}

#[crate::utils::engine_test(all)]
async fn test_system_calls_with_reorg(mut instance: crate::utils::LocalInstance) {
    // Block 1
    instance.new_block().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    let tx1 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();
    let tx2 = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(instance.is_transaction_successful(&tx1).await.unwrap());
    assert!(instance.is_transaction_successful(&tx2).await.unwrap());

    // Reorg the last transaction
    instance.send_reorg(tx2).await.unwrap();

    // Block 2 with system calls (should handle reorg correctly)
    let hash = B256::repeat_byte(0x55);
    let beacon_root = B256::repeat_byte(0x66);
    instance
        .new_block_with_hashes(hash, Some(beacon_root))
        .await
        .unwrap();
    instance.new_iteration(1).await.unwrap();

    let tx3 = instance
        .send_successful_create_tx(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    assert!(
        instance.is_transaction_successful(&tx3).await.unwrap(),
        "System calls should work after reorg"
    );
}

/// Simple mock database for testing system calls
#[derive(Debug, Default)]
struct MockDb {
    accounts: HashMap<Address, revm::state::Account>,
    block_hash_cache: RefCell<HashMap<u64, B256>>,
}

#[derive(Error, Debug)]
pub enum MockDbError {
    #[error("Block hash not found for block {0}")]
    BlockHashNotFound(u64),
}

impl DBErrorMarker for MockDbError {}

impl DatabaseRef for MockDb {
    type Error = MockDbError;

    fn basic_ref(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        Ok(self.accounts.get(&address).map(|acc| acc.info.clone()))
    }

    fn code_by_hash_ref(&self, _code_hash: B256) -> Result<Bytecode, Self::Error> {
        Ok(Bytecode::default())
    }

    fn storage_ref(&self, address: Address, index: U256) -> Result<U256, Self::Error> {
        Ok(self
            .accounts
            .get(&address)
            .and_then(|acc| acc.storage.get(&index))
            .map_or(U256::ZERO, |slot| slot.present_value))
    }

    fn block_hash_ref(&self, number: u64) -> Result<B256, Self::Error> {
        self.block_hash_cache
            .borrow()
            .get(&number)
            .copied()
            .ok_or(MockDbError::BlockHashNotFound(number))
    }
}

impl BlockHashStore for MockDb {
    fn store_block_hash(&self, number: u64, hash: B256) {
        self.block_hash_cache.borrow_mut().insert(number, hash);
    }
}

impl revm::DatabaseCommit for MockDb {
    fn commit(&mut self, changes: revm::state::EvmState) {
        for (address, account) in changes {
            self.accounts.insert(address, account);
        }
    }
}

#[test]
fn test_ring_buffer_and_slot_calculations() {
    let mut db = MockDb::default();
    let system_calls = SystemCalls::new();

    // EIP-2935: Test with block number that wraps around the ring buffer
    let block_number = HISTORY_SERVE_WINDOW + 99;
    let hash = B256::repeat_byte(0xff);

    let config = SystemCallsConfig {
        spec_id: SpecId::PRAGUE,
        block_number: U256::from(block_number),
        timestamp: U256::from(1234567890),
        block_hash: Some(hash),
        parent_beacon_block_root: None,
    };

    let result = system_calls.apply_eip2935(&config, &mut db);
    assert!(result.is_ok());

    let account = db.accounts.get(&HISTORY_STORAGE_ADDRESS).unwrap();
    // EIP-2935: slot = (block_number - 1) % HISTORY_SERVE_WINDOW = 8289 % 8191 = 98
    let expected_slot = U256::from(98u64);
    assert!(account.storage.contains_key(&expected_slot));

    // EIP-4788: Test with timestamp that wraps around
    let mut db = MockDb::default();
    let timestamp = HISTORY_BUFFER_LENGTH * 2 + 500;
    let beacon_root = B256::repeat_byte(0xee);

    let config = SystemCallsConfig {
        spec_id: SpecId::CANCUN,
        block_number: U256::from(100),
        timestamp: U256::from(timestamp),
        block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(beacon_root),
    };

    let result = system_calls.apply_eip4788(&config, &mut db);
    assert!(result.is_ok());

    let account = db.accounts.get(&BEACON_ROOTS_ADDRESS).unwrap();
    // Slot should be timestamp % HISTORY_BUFFER_LENGTH = 500
    let expected_slot = U256::from(500u64);
    assert!(account.storage.contains_key(&expected_slot));

    // EIP-2935: parent block number 100, slot = 100 % 8191 = 100
    let block_number = 100u64;
    let slot_index = block_number % HISTORY_SERVE_WINDOW as u64;
    assert_eq!(slot_index, 100);

    let number_slot = U256::from(slot_index);
    let hash_slot = U256::from(slot_index + HISTORY_SERVE_WINDOW as u64);
    assert_eq!(number_slot, U256::from(100u64));
    assert_eq!(hash_slot, U256::from(8291u64)); // 100 + 8191

    // EIP-4788: timestamp 1700000000, slot = 1700000000 % 8191 = 7096
    let timestamp = 1700000000u64;
    let timestamp_index = timestamp % HISTORY_BUFFER_LENGTH;
    assert_eq!(timestamp_index, 7096);
}

#[test]
fn test_spec_id_activation_and_behavior() {
    // Test Shanghai (pre-Cancun)
    assert!(!SpecId::SHANGHAI.is_cancun_active());
    assert!(!SpecId::SHANGHAI.is_prague_active());

    // Test Cancun
    assert!(SpecId::CANCUN.is_cancun_active());
    assert!(!SpecId::CANCUN.is_prague_active());

    // Test Prague
    assert!(SpecId::PRAGUE.is_cancun_active());
    assert!(SpecId::PRAGUE.is_prague_active());

    let system_calls = SystemCalls::new();

    // Shanghai: neither contract should be touched
    let mut db = MockDb::default();
    let config = SystemCallsConfig {
        spec_id: SpecId::SHANGHAI,
        block_number: U256::from(100),
        timestamp: U256::from(1234567890),
        block_hash: Some(B256::repeat_byte(0x11)),
        parent_beacon_block_root: Some(B256::repeat_byte(0x22)),
    };

    let result = system_calls.apply_pre_tx_system_calls(&config, &mut db);
    assert!(result.is_ok());
    // Neither contract should be touched for Shanghai
    assert!(!db.accounts.contains_key(&BEACON_ROOTS_ADDRESS));
    assert!(!db.accounts.contains_key(&HISTORY_STORAGE_ADDRESS));

    // Cancun: only beacon roots should be touched
    let mut db = MockDb::default();
    let config = SystemCallsConfig {
        spec_id: SpecId::CANCUN,
        block_number: U256::from(100),
        timestamp: U256::from(1234567890),
        block_hash: Some(B256::repeat_byte(0x11)),
        parent_beacon_block_root: Some(B256::repeat_byte(0x22)),
    };

    let result = system_calls.apply_pre_tx_system_calls(&config, &mut db);
    assert!(result.is_ok());
    // Only beacon roots should be touched for Cancun
    assert!(db.accounts.contains_key(&BEACON_ROOTS_ADDRESS));
    assert!(!db.accounts.contains_key(&HISTORY_STORAGE_ADDRESS));

    // Prague: both contracts should be touched
    let mut db = MockDb::default();
    let config = SystemCallsConfig {
        spec_id: SpecId::PRAGUE,
        block_number: U256::from(100),
        timestamp: U256::from(1234567890),
        block_hash: Some(B256::repeat_byte(0x11)),
        parent_beacon_block_root: Some(B256::repeat_byte(0x22)),
    };

    let result = system_calls.apply_pre_tx_system_calls(&config, &mut db);
    assert!(result.is_ok());
    // Both contracts should be touched for Prague
    assert!(db.accounts.contains_key(&BEACON_ROOTS_ADDRESS));
    assert!(db.accounts.contains_key(&HISTORY_STORAGE_ADDRESS));
}

#[crate::utils::engine_test(mock)]
async fn test_transaction_stalls_until_source_synced(mut instance: crate::utils::LocalInstance) {
    use revm::primitives::uint;
    use std::time::Duration;

    // Block 1: Just advance the chain
    // IMPORTANT: Don't send any transactions so check_sources_available stays true
    instance.new_block().await.unwrap();

    // Advance engine to block 2 WITHOUT syncing the mock RPC
    // After this: engine's current_head = 2, but sources are still at block 1
    instance.new_block_unsynced().await.unwrap();
    instance.new_iteration(1).await.unwrap();

    // The block we need sources to sync to (current_head after new_block_unsynced)
    // Note: current_head is set to commit_head.block_number in process_commit_head
    // After new_block() with block_number=1: current_head=1, block_number becomes 2
    // After new_block_unsynced() with block_number=2: current_head=2, block_number becomes 3
    let current_head_block: u64 = 2;

    // Execute a transaction - should stall because:
    // - check_sources_available is still true (no transactions were processed yet)
    // - sources report synced to block 1, but engine current_head is 2
    let tx = instance
        .send_successful_create_tx_dry(uint!(0_U256), Bytes::new())
        .await
        .unwrap();

    // Verify transaction stalls (short timeout should expire without a result)
    let stall_result = tokio::time::timeout(
        Duration::from_millis(50),
        instance.wait_for_transaction_processed(&tx),
    )
    .await;

    assert!(
        stall_result.is_err(),
        "Transaction should stall while source is not synced to current block"
    );

    // Now update mock newHeads to the current head block
    instance
        .eth_rpc_source_http_mock
        .send_new_head_with_block_number(current_head_block);

    // Transaction should now complete successfully
    assert!(
        instance.is_transaction_successful(&tx).await.unwrap(),
        "Transaction should succeed after source syncs to current block"
    );
}

/// Tests that reverted transactions (EVM reverts) ARE counted in `n_transactions`.
/// Reverts are valid execution outcomes and their state should be committed.
#[tokio::test]
async fn test_reverted_transactions_are_counted() {
    let (mut engine, _) = create_test_engine().await;

    let block_env = create_test_block_env();
    let block_execution_id = BlockExecutionId {
        block_number: block_env.number,
        iteration_id: 0,
    };

    // Create iteration
    let new_iteration = queue::NewIteration {
        block_env: block_env.clone(),
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };
    engine.process_iteration(&new_iteration).unwrap();

    // Create a transaction that will revert (REVERT opcode: 0xfd)
    // Bytecode: PUSH1 0x00, PUSH1 0x00, REVERT (60 00 60 00 fd)
    let reverting_tx_env = TxEnv {
        caller: Address::from([0x01; 20]),
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Create,
        value: uint!(0_U256),
        data: Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]),
        nonce: 0,
        ..Default::default()
    };

    let tx_hash = B256::from([0x11; 32]);
    let tx_execution_id = TxExecutionId::new(block_env.number, 0, tx_hash, 0);

    // Execute the reverting transaction
    let result = engine.execute_transaction(tx_execution_id, &reverting_tx_env);
    assert!(
        result.is_ok(),
        "Reverting transaction should execute without engine error"
    );

    // Verify the transaction result is a revert
    let tx_result = engine
        .get_transaction_result_cloned(&tx_execution_id)
        .expect("Transaction result should exist");

    match tx_result {
        TransactionResult::ValidationCompleted {
            execution_result, ..
        } => {
            assert!(
                !execution_result.is_success(),
                "Transaction should have reverted"
            );
        }
        TransactionResult::ValidationError(_) => {
            panic!("Expected ValidationCompleted result");
        }
    }

    // Verify n_transactions is incremented for reverted transaction
    let n_transactions = engine
        .get_n_transactions(&block_execution_id)
        .expect("Block iteration should exist");
    assert_eq!(
        n_transactions, 1,
        "Reverted transactions SHOULD be counted in n_transactions"
    );
}

/// Tests that invalid transactions (validation errors like insufficient funds, bad nonce)
/// are NOT counted in `n_transactions` because their state is not committed.
#[tokio::test]
async fn test_invalid_transactions_not_counted() {
    let (mut engine, _) = create_test_engine().await;

    let block_env = create_test_block_env();
    let block_execution_id = BlockExecutionId {
        block_number: block_env.number,
        iteration_id: 0,
    };

    // Create iteration
    let new_iteration = queue::NewIteration {
        block_env: block_env.clone(),
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };
    engine.process_iteration(&new_iteration).unwrap();

    // Create a transaction that will fail validation (insufficient funds)
    // Sending 1 ETH from an account with 0 balance
    let invalid_tx_env = TxEnv {
        caller: Address::from([0x99; 20]), // Account with no balance
        gas_limit: 21_000,
        gas_price: 0,
        kind: TxKind::Call(Address::from([0x88; 20])),
        value: uint!(1_000_000_000_000_000_000_U256), // 1 ETH
        data: Bytes::new(),
        nonce: 0,
        ..Default::default()
    };

    let tx_hash = B256::from([0x22; 32]);
    let tx_execution_id = TxExecutionId::new(block_env.number, 0, tx_hash, 0);

    // Execute the invalid transaction - should return Ok but store ValidationError
    let result = engine.execute_transaction(tx_execution_id, &invalid_tx_env);
    assert!(
        result.is_ok(),
        "Invalid transaction should not cause engine error"
    );

    // Verify the transaction result is a ValidationError
    let tx_result = engine
        .get_transaction_result_cloned(&tx_execution_id)
        .expect("Transaction result should exist");
    assert!(
        matches!(tx_result, TransactionResult::ValidationError(_)),
        "Expected ValidationError for insufficient funds, got {tx_result:?}"
    );

    // Verify n_transactions is NOT incremented for invalid transaction
    let n_transactions = engine
        .get_n_transactions(&block_execution_id)
        .expect("Block iteration should exist");
    assert_eq!(
        n_transactions, 0,
        "Invalid transactions should NOT be counted in n_transactions"
    );
}

/// Tests the mixed scenario: valid transactions are counted, invalid are not.
#[tokio::test]
async fn test_mixed_valid_and_invalid_transactions_counting() {
    let (mut engine, _) = create_test_engine().await;

    let block_env = create_test_block_env();
    let block_execution_id = BlockExecutionId {
        block_number: block_env.number,
        iteration_id: 0,
    };

    // Create iteration
    let new_iteration = queue::NewIteration {
        block_env: block_env.clone(),
        iteration_id: 0,
        parent_block_hash: Some(B256::ZERO),
        parent_beacon_block_root: Some(B256::ZERO),
    };
    engine.process_iteration(&new_iteration).unwrap();

    // Transaction 1: Valid successful transaction
    let valid_tx_env = TxEnv {
        caller: Address::from([0x01; 20]),
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Create,
        value: uint!(0_U256),
        data: Bytes::from(vec![0x60, 0x00]), // PUSH1 0x00 - minimal valid bytecode
        nonce: 0,
        ..Default::default()
    };
    let tx1_hash = B256::from([0x11; 32]);
    let tx1_id = TxExecutionId::new(block_env.number, 0, tx1_hash, 0);
    engine.execute_transaction(tx1_id, &valid_tx_env).unwrap();

    // Transaction 2: Invalid transaction (insufficient funds)
    let invalid_tx_env = TxEnv {
        caller: Address::from([0x99; 20]),
        gas_limit: 21_000,
        gas_price: 0,
        kind: TxKind::Call(Address::from([0x88; 20])),
        value: uint!(1_000_000_000_000_000_000_U256), // 1 ETH from empty account
        data: Bytes::new(),
        nonce: 0,
        ..Default::default()
    };
    let tx2_hash = B256::from([0x22; 32]);
    let tx2_id = TxExecutionId::new(block_env.number, 0, tx2_hash, 1);
    engine.execute_transaction(tx2_id, &invalid_tx_env).unwrap();

    // Transaction 3: Valid reverting transaction
    let reverting_tx_env = TxEnv {
        caller: Address::from([0x02; 20]),
        gas_limit: 100_000,
        gas_price: 0,
        kind: TxKind::Create,
        value: uint!(0_U256),
        data: Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]), // REVERT
        nonce: 0,
        ..Default::default()
    };
    let tx3_hash = B256::from([0x33; 32]);
    let tx3_id = TxExecutionId::new(block_env.number, 0, tx3_hash, 2);
    engine
        .execute_transaction(tx3_id, &reverting_tx_env)
        .unwrap();

    // Verify results
    let tx1_result = engine.get_transaction_result_cloned(&tx1_id).unwrap();
    let tx2_result = engine.get_transaction_result_cloned(&tx2_id).unwrap();
    let tx3_result = engine.get_transaction_result_cloned(&tx3_id).unwrap();

    assert!(
        matches!(tx1_result, TransactionResult::ValidationCompleted { .. }),
        "TX1 should be ValidationCompleted"
    );
    assert!(
        matches!(tx2_result, TransactionResult::ValidationError(_)),
        "TX2 should be ValidationError"
    );
    assert!(
        matches!(tx3_result, TransactionResult::ValidationCompleted { .. }),
        "TX3 should be ValidationCompleted (revert)"
    );

    // Only valid transactions (TX1 and TX3) should be counted
    let n_transactions = engine
        .get_n_transactions(&block_execution_id)
        .expect("Block iteration should exist");
    assert_eq!(
        n_transactions, 2,
        "Only valid transactions (success + revert) should be counted, not invalid ones"
    );
}
