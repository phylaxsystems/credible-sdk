
use crate::{
    engine::{
        CoreEngine,
        queue::{
            TransactionQueueSender,
            TxQueueContents,
            QueueTransaction,
        },
    },
    transport::{
        Transport,
        mock::MockTransport,
    },
};
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    store::{AssertionState, AssertionStore},
};
use crossbeam::channel;
use revm::{
    context::{BlockEnv, TxEnv},
    database::{CacheDB, EmptyDBTyped},
    primitives::{Address, B256, Bytes, TxKind, U256},
};
use assertion_executor::primitives::AccountInfo;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn, error};

/// Test database error type
type TestDbError = std::convert::Infallible;

/// Creates a test instance of the core engine with mock transport.
/// This struct manages the lifecycle of a test engine instance.
pub struct LocalInstance {
    /// Channel for sending transactions and blocks to the mock transport
    mock_sender: TransactionQueueSender,
    /// The underlying database
    db: Arc<CacheDB<EmptyDBTyped<TestDbError>>>,
    /// The assertion store
    assertion_store: Arc<AssertionStore>,
    /// Transport task handle
    transport_handle: Option<JoinHandle<()>>,
    /// Engine task handle  
    engine_handle: Option<JoinHandle<()>>,
}

impl LocalInstance {
    /// Create a new local instance with mock transport
    pub async fn new() -> Result<Self, String> {
        info!("Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Create the database and state
        let underlying_db = Arc::new(CacheDB::new(EmptyDBTyped::default()));
        let state = OverlayDb::new(Some(underlying_db.clone()), 1024);

        // Create assertion store and executor
        let assertion_store = Arc::new(
            AssertionStore::new_ephemeral()
                .map_err(|e| format!("Failed to create assertion store: {}", e))?
        );
        let assertion_executor = AssertionExecutor::new(
            ExecutorConfig::default(),
            (*assertion_store).clone(),
        );

        // Create the engine
        let mut engine = CoreEngine::new(state, engine_rx, assertion_executor);
        
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
        let transport = MockTransport::with_receiver(engine_tx, mock_rx);
        
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

        Ok(Self {
            mock_sender: mock_tx,
            db: underlying_db,
            assertion_store,
            transport_handle: Some(transport_handle),
            engine_handle: Some(engine_handle),
        })
    }

    /// Send a new block environment to the engine
    pub fn send_block(&self, block_env: BlockEnv) -> Result<(), String> {
        info!("LocalInstance sending block: {:?}", block_env.number);
        info!("About to send to mock_sender channel");
        let result = self.mock_sender
            .send(TxQueueContents::Block(block_env))
            .map_err(|e| format!("Failed to send block: {}", e));
        match &result {
            Ok(_) => info!("Successfully sent block to mock_sender"),
            Err(e) => error!("Failed to send block: {}", e),
        }
        result
    }

    /// Send a transaction to the engine
    pub fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        info!("LocalInstance sending transaction: {:?}", tx_hash);
        let queue_tx = QueueTransaction { tx_hash, tx_env };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx))
            .map_err(|e| format!("Failed to send transaction: {}", e))
    }

    /// Send a block with multiple transactions
    pub fn send_block_with_txs(
        &self,
        block_env: BlockEnv,
        transactions: Vec<(B256, TxEnv)>,
    ) -> Result<(), String> {
        // Send the block environment first
        self.send_block(block_env)?;

        // Then send all transactions
        for (tx_hash, tx_env) in transactions {
            self.send_transaction(tx_hash, tx_env)?;
        }

        Ok(())
    }

    /// Get a reference to the underlying database
    pub fn db(&self) -> &Arc<CacheDB<EmptyDBTyped<TestDbError>>> {
        &self.db
    }

    /// Get a reference to the assertion store
    pub fn assertion_store(&self) -> &Arc<AssertionStore> {
        &self.assertion_store
    }

    /// Wait for a short time to allow transaction processing
    /// Since we can't directly access engine results in this test setup,
    /// tests should verify behavior through other means (e.g., state changes)
    pub async fn wait_for_processing(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    /// Insert an assertion into the store
    pub fn insert_assertion(
        &self,
        address: Address,
        assertion: AssertionState,
    ) -> Result<(), String> {
        self.assertion_store
            .insert(address, assertion)
            .map(|_| ())
            .map_err(|e| format!("Failed to insert assertion: {}", e))
    }

    /// Create a test block environment with default values
    pub fn create_test_block(&self, number: u64) -> BlockEnv {
        BlockEnv {
            number,
            basefee: 0, // Set to 0 to avoid balance issues in tests
            timestamp: 1_000_000 + number * 12,
            gas_limit: 30_000_000,
            ..Default::default()
        }
    }

    /// Create a simple test transaction
    pub fn create_test_transaction(
        &self,
        caller: Address,
        nonce: u64,
        value: U256,
        data: Bytes,
    ) -> TxEnv {
        TxEnv {
            caller,
            gas_limit: 100_000,
            gas_price: 0,
            kind: TxKind::Create,
            value,
            data,
            nonce,
            ..Default::default()
        }
    }

    /// Send a simple test transaction and wait for processing
    pub async fn send_and_wait(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        self.send_transaction(tx_hash, tx_env)?;
        self.wait_for_processing(Duration::from_millis(100)).await;
        Ok(())
    }

    /// Prefund accounts with ETH for testing
    pub fn fund_accounts(&mut self, accounts: &[(Address, U256)]) -> Result<(), String> {
        let db = Arc::get_mut(&mut self.db)
            .ok_or_else(|| "Cannot get mutable reference to database".to_string())?;
        
        for (address, balance) in accounts {
            let mut account_info = AccountInfo::default();
            account_info.balance = *balance;
            db.insert_account_info(*address, account_info);
        }
        
        Ok(())
    }
}

impl Drop for LocalInstance {
    fn drop(&mut self) {
        // Abort tasks if still running
        if let Some(handle) = self.transport_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.engine_handle.take() {
            handle.abort();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::primitives::uint;

    #[tokio::test]
    async fn test_mock_driver_pattern_demonstration() {
        // Create a mock instance for testing
        info!("Creating LocalInstance for test");
        let instance = LocalInstance::new().await.unwrap();
        
        // Send a block environment
        info!("Sending block to engine");
        let block = instance.create_test_block(1);
        instance.send_block(block).unwrap();
        
        // Give transport time to forward the block
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Create and send a transaction
        info!("Sending transaction to engine");
        let tx_env = instance.create_test_transaction(
            Address::from([0x01; 20]),
            0,
            uint!(0_U256),
            Bytes::new(),
        );
        
        let tx_hash = B256::from([0x11; 32]);
        instance.send_transaction(tx_hash, tx_env).unwrap();
        
        // Wait for processing
        info!("Waiting for transaction processing");
        instance.wait_for_processing(Duration::from_millis(100)).await;
        
        info!("Test completed successfully");
        // Test passes if no panic/errors occurred during processing
    }
}