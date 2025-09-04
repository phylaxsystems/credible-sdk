//! # `utils`
//!
//! Contains various shared utilities we use across the sidecar `mod`s:
//! - `test_util` test utilites to set up and run tests.
//! - `instance` test instance for running engine tests with mock transport

#[cfg(test)]
#[allow(dead_code)]
pub mod instance;
#[cfg(test)]
mod test_util;

pub use crate::utils::instance::LocalInstance;
use crate::{
    CoreEngine,
    engine::queue::TransactionQueueSender,
    transport::mock::MockTransport,
};
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    primitives::{
        AccountInfo,
        Address,
        B256,
        TxEnv,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::{
        COUNTER_ADDRESS,
        SIMPLE_ASSERTION_COUNTER,
        bytecode,
        counter_acct_info,
        counter_call,
    },
};
use crossbeam::channel;
use std::sync::Arc;
#[cfg(test)]
pub use test_util::{
    TestDbError,
    engine_test,
};
use tracing::{
    error,
    info,
    warn,
};

use crate::transport::Transport;

pub trait TestTransport: Sized {
    /// Creates a `LocalInstance` with a specific transport
    async fn new() -> Result<LocalInstance<Self>, String>;
    /// Advance the core engine block by sending a new blockenv to it
    async fn new_block(&mut self) -> Result<(), String>;
    /// Send a transaction to the core engine via the transport
    async fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String>;
}

/// Wrapper over the LocalInstance that
pub struct LocalInstanceMockDriver {
    /// Channel for sending transactions and blocks to the mock transport
    mock_sender: TransactionQueueSender,
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!("Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Create the database and state
        let mut underlying_db = CacheDB::new(EmptyDBTyped::default());
        // Insert default counter contract into the underlying db (and mock by proxy)
        underlying_db.insert_account_info(COUNTER_ADDRESS, counter_acct_info());

        // Create default account that will be used by this instance
        let default_account = Address::from([0x01; 20]);
        let default_account_info = AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        };
        underlying_db.insert_account_info(default_account, default_account_info);

        // Fund common test accounts with maximum balance
        let default_caller = counter_call().caller;
        let caller_account = AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        };
        underlying_db.insert_account_info(default_caller, caller_account);

        // Fund test caller account
        let test_caller = Address::from([0x02; 20]);
        let test_account = AccountInfo {
            balance: U256::MAX,
            ..Default::default()
        };
        underlying_db.insert_account_info(test_caller, test_account);

        let underlying_db = Arc::new(underlying_db);

        let state = OverlayDb::new(Some(underlying_db.clone()), 1024);

        // Create assertion store and executor
        let assertion_store = Arc::new(
            AssertionStore::new_ephemeral()
                .map_err(|e| format!("Failed to create assertion store: {e}"))?,
        );

        // Insert counter assertion into store
        let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
        assertion_store
            .insert(
                COUNTER_ADDRESS,
                // Assuming AssertionState::new_test takes Bytes or similar
                AssertionState::new_test(assertion_bytecode),
            )
            .unwrap();

        let assertion_executor =
            AssertionExecutor::new(ExecutorConfig::default(), (*assertion_store).clone());

        // Create the engine with TransactionsState
        let state_results = crate::TransactionsState::new();
        let mut engine = CoreEngine::new(
            state,
            engine_rx,
            assertion_executor,
            state_results.clone(),
            10,
        );

        // Spawn the engine task that manually processes items
        // This mimics what the tests do - manually processing items from the queue
        let engine_handle = tokio::spawn(async move {
            info!("Engine task started, waiting for items...");
            info!("Engine about to call run()");
            let result = engine.run().await;
            match result {
                Ok(_) => info!("Engine run() completed successfully"),
                Err(e) => error!("Engine run() failed: {:?}", e),
            }
            info!("Engine task completed");
        });

        // Create mock transport with the channels
        let transport = MockTransport::with_receiver(engine_tx, mock_rx, state_results.clone());

        // Spawn the transport task
        let transport_handle = tokio::spawn(async move {
            info!("Transport task started");
            info!("Transport about to call run()");
            let result = transport.run().await;
            match result {
                Ok(_) => info!("Transport run() completed successfully"),
                Err(e) => warn!("Transport stopped with error: {}", e),
            }
            info!("Transport task completed");
        });

        Ok(LocalInstance {
            mock_sender: mock_tx.clone(),
            db: underlying_db,
            assertion_store,
            transport_handle: Some(transport_handle),
            engine_handle: Some(engine_handle),
            block_bumber: 0,
            transaction_results: state_results,
            default_account: Address::from([0x01; 20]),
            current_nonce: 0,
            transport: LocalInstanceMockDriver {
                mock_sender: mock_tx,
            },
        })
    }

    async fn new_block(&mut self) -> Result<(), String> {
        todo!()
    }

    async fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        todo!()
    }
}
