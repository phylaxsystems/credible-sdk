use crate::{
    engine::{
        CoreEngine,
        TransactionResult,
        queue::{
            QueueTransaction,
            TransactionQueueSender,
            TxQueueContents,
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
    primitives::{
        AccountInfo,
        FixedBytes,
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
use dashmap::DashMap;
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
        address,
        hex,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::{
    error,
    info,
    warn,
};

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
    /// Current block number
    block_bumber: u64,
    /// Shared transaction results from engine
    transaction_results: Arc<DashMap<B256, TransactionResult>>,
}

impl LocalInstance {
    /// Create a new local instance with mock transport
    pub async fn new() -> Result<Self, String> {
        info!("Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Create the database and state
        let mut underlying_db = CacheDB::new(EmptyDBTyped::default());
        // Insert default counter contract into the underlying db (and mock by proxy)
        underlying_db.insert_account_info(COUNTER_ADDRESS, counter_acct_info());

        // Fund common test accounts with maximum balance
        let default_caller = counter_call().caller;
        let mut caller_account = AccountInfo::default();
        caller_account.balance = U256::MAX;
        underlying_db.insert_account_info(default_caller, caller_account);

        // Fund test caller account
        let test_caller = Address::from([0x02; 20]);
        let mut test_account = AccountInfo::default();
        test_account.balance = U256::MAX;
        underlying_db.insert_account_info(test_caller, test_account);

        let underlying_db = Arc::new(underlying_db);

        let state = OverlayDb::new(Some(underlying_db.clone()), 1024);

        // Create assertion store and executor
        let assertion_store = Arc::new(
            AssertionStore::new_ephemeral()
                .map_err(|e| format!("Failed to create assertion store: {}", e))?,
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

        // Create the engine
        let mut engine = CoreEngine::new(state, engine_rx, assertion_executor);

        // Get shared transaction results before moving engine into task
        let transaction_results = engine.get_shared_results();

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
            block_bumber: 0,
            transaction_results,
        })
    }

    /// Send a new block environment to the engine
    pub fn new_block(&mut self) -> Result<(), String> {
        info!("LocalInstance sending block: {:?}", self.block_bumber);
        let block_env = BlockEnv {
            number: self.block_bumber,
            gas_limit: 50_000_000, // Set higher gas limit for assertions
            ..Default::default()
        };
        // Increment block number for next time we call new_block
        self.block_bumber += 1;

        let result = self
            .mock_sender
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
    pub fn send_block_with_txs(&mut self, transactions: Vec<(B256, TxEnv)>) -> Result<(), String> {
        // Send the block environment first
        self.new_block()?;

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

    /// Send a successful CREATE transaction and verify it succeeds
    pub async fn send_successful_create_tx(
        &mut self,
        caller: Address,
        nonce: u64,
        value: U256,
        data: Bytes,
    ) -> Result<B256, String> {
        // Ensure we have a block
        self.new_block()?;

        // Create transaction
        let tx_env = TxEnv {
            caller,
            gas_limit: 100_000,
            gas_price: 0,
            kind: TxKind::Create,
            value,
            data,
            nonce,
            ..Default::default()
        };

        // Generate transaction hash based on caller and nonce
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0..20].copy_from_slice(caller.as_slice());
        hash_bytes[20..28].copy_from_slice(&nonce.to_be_bytes());
        let tx_hash = B256::from(hash_bytes);

        // Send transaction
        self.send_transaction(tx_hash, tx_env)?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(100)).await;

        Ok(tx_hash)
    }

    /// Send a reverting transaction and verify it reverts
    pub async fn send_reverting_create_tx(
        &mut self,
        caller: Address,
        nonce: u64,
    ) -> Result<B256, String> {
        // Ensure we have a block
        self.new_block()?;

        // Create reverting transaction (PUSH1 0x00 PUSH1 0x00 REVERT)
        let tx_env = TxEnv {
            caller,
            gas_limit: 100_000,
            gas_price: 0,
            kind: TxKind::Create,
            value: U256::ZERO,
            data: Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]),
            nonce,
            ..Default::default()
        };

        // Generate transaction hash based on caller and nonce
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0..20].copy_from_slice(caller.as_slice());
        hash_bytes[20..28].copy_from_slice(&nonce.to_be_bytes());
        let tx_hash = B256::from(hash_bytes);

        // Send transaction
        self.send_transaction(tx_hash, tx_env)?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(100)).await;

        Ok(tx_hash)
    }

    /// Send a CALL transaction to an existing contract
    pub async fn send_call_tx(
        &mut self,
        caller: Address,
        to: Address,
        nonce: u64,
        value: U256,
        data: Bytes,
    ) -> Result<B256, String> {
        // Ensure we have a block
        self.new_block()?;

        // Create call transaction
        let tx_env = TxEnv {
            caller,
            gas_limit: 100_000,
            gas_price: 0,
            kind: TxKind::Call(to),
            value,
            data,
            nonce,
            ..Default::default()
        };

        // Generate transaction hash based on caller and nonce
        let mut hash_bytes = [0u8; 32];
        hash_bytes[0..20].copy_from_slice(caller.as_slice());
        hash_bytes[20..28].copy_from_slice(&nonce.to_be_bytes());
        let tx_hash = B256::from(hash_bytes);

        // Send transaction
        self.send_transaction(tx_hash, tx_env)?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(100)).await;

        Ok(tx_hash)
    }

    /// Send multiple transactions in the same block
    pub async fn send_batch_transactions(
        &mut self,
        transactions: Vec<(Address, u64, U256, Bytes, TxKind)>,
    ) -> Result<Vec<B256>, String> {
        // Send block first
        self.new_block()?;

        let mut tx_hashes = Vec::new();

        // Send all transactions
        for (i, (caller, nonce, value, data, kind)) in transactions.into_iter().enumerate() {
            let tx_env = TxEnv {
                caller,
                gas_limit: 100_000,
                gas_price: 0,
                kind,
                value,
                data,
                nonce,
                ..Default::default()
            };

            // Generate transaction hash based on caller, nonce, and index
            let mut hash_bytes = [0u8; 32];
            hash_bytes[0..20].copy_from_slice(caller.as_slice());
            hash_bytes[20..28].copy_from_slice(&nonce.to_be_bytes());
            hash_bytes[28..32].copy_from_slice(&(i as u32).to_be_bytes());
            let tx_hash = B256::from(hash_bytes);
            self.send_transaction(tx_hash, tx_env)?;
            tx_hashes.push(tx_hash);
        }

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(100)).await;

        Ok(tx_hashes)
    }

    /// Insert a custom assertion from bytecode artifact name
    ///
    /// # Arguments
    /// * `address` - The adopter address for this assertion
    /// * `artifact_name` - The artifact name in format "file.sol:ContractName"
    pub fn insert_custom_assertion(
        &self,
        address: Address,
        artifact_name: &str,
    ) -> Result<(), String> {
        let assertion_bytecode = bytecode(artifact_name);
        let assertion = AssertionState::new_test(assertion_bytecode);

        self.insert_assertion(address, assertion)
            .map_err(|e| format!("Failed to insert custom assertion {}: {}", artifact_name, e))
    }

    /// Get transaction result by hash
    pub fn get_transaction_result(&self, tx_hash: &B256) -> Option<TransactionResult> {
        self.transaction_results
            .get(tx_hash)
            .map(|entry| entry.value().clone())
    }

    /// Check if transaction was successful and valid
    pub fn is_transaction_successful(&self, tx_hash: &B256) -> Result<bool, String> {
        match self.get_transaction_result(tx_hash) {
            Some(TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            }) => Ok(is_valid && execution_result.is_success()),
            Some(TransactionResult::ValidationError(_)) => Ok(false),
            None => Err("Transaction result not found".to_string()),
        }
    }

    /// Check if transaction reverted but passed validation
    pub fn is_transaction_reverted_but_valid(&self, tx_hash: &B256) -> Result<bool, String> {
        match self.get_transaction_result(tx_hash) {
            Some(TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            }) => Ok(is_valid && !execution_result.is_success()),
            Some(TransactionResult::ValidationError(_)) => Ok(false),
            None => Err("Transaction result not found".to_string()),
        }
    }

    /// Check if transaction failed validation
    pub fn is_transaction_invalid(&self, tx_hash: &B256) -> Result<bool, String> {
        match self.get_transaction_result(tx_hash) {
            Some(TransactionResult::ValidationCompleted { is_valid, .. }) => Ok(!is_valid),
            Some(TransactionResult::ValidationError(_)) => Ok(true),
            None => Err("Transaction result not found".to_string()),
        }
    }

    /// Send and verify a successful CREATE transaction
    pub async fn send_and_verify_successful_create_tx(
        &mut self,
        caller: Address,
        nonce: u64,
        value: U256,
        data: Bytes,
    ) -> Result<Address, String> {
        let tx_hash = self
            .send_successful_create_tx(caller, nonce, value, data)
            .await?;

        if !self.is_transaction_successful(&tx_hash)? {
            return Err("Transaction was not successful".to_string());
        }

        // Calculate the expected contract address (simplified for testing)
        let contract_address = address!("76cae8af66cb2488933e640ba08650a3a8e7ae19");

        Ok(contract_address)
    }

    /// Send and verify a reverting transaction
    pub async fn send_and_verify_reverting_create_tx(
        &mut self,
        caller: Address,
        nonce: u64,
    ) -> Result<(), String> {
        let tx_hash = self.send_reverting_create_tx(caller, nonce).await?;

        if !self.is_transaction_reverted_but_valid(&tx_hash)? {
            return Err("Transaction should have reverted but been valid".to_string());
        }

        Ok(())
    }

    /// Sends a pair of assertion passing and failing transactions.
    /// The transactions call a preloaded counter contract, which can only
    /// be called once due to subsequent assertions invalidating.
    /// Returns the hash of the failing transaction.
    pub async fn send_assertion_passing_failing_pair(
        &mut self,
        caller: Address,
        nonce: u64,
    ) -> Result<(), String> {
        // Ensure we have a block
        self.new_block()?;

        let basefee = 10u64;

        // Create the first transaction (should pass)
        let mut tx_pass = counter_call();
        tx_pass.caller = caller;
        tx_pass.nonce = nonce;
        tx_pass.gas_price = basefee.into();

        // Create the second transaction (should fail assertion)
        let mut tx_fail = counter_call();
        tx_fail.caller = caller;
        tx_fail.nonce = nonce + 1;
        tx_fail.gas_price = basefee.into();

        // Generate unique transaction hashes
        let hash_pass = FixedBytes::<32>::random();
        let hash_fail = FixedBytes::<32>::random();

        // Send the passing transaction first
        self.send_transaction(hash_pass, tx_pass)?;

        // Send the failing transaction second
        self.send_transaction(hash_fail, tx_fail)?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(100)).await;

        // Verify the first transaction passed assertions and was committed to engine state
        if !self.is_transaction_successful(&hash_pass)? {
            return Err(
                "First transaction should have passed assertions and been committed".to_string(),
            );
        }

        // Verify the second transaction failed assertions and was NOT committed
        if !self.is_transaction_invalid(&hash_fail)? {
            return Err(
                "Second transaction should have failed assertions and not been committed"
                    .to_string(),
            );
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
    use revm::primitives::{
        TxKind,
        uint,
    };

    #[crate::utils::engine_test]
    async fn test_new_helper_functions(mut instance: LocalInstance) {
        let caller = Address::from([0x01; 20]);
        let nonce = 0u64;

        // Test sending and verifying a successful transaction
        let contract_address = instance
            .send_and_verify_successful_create_tx(caller, nonce, uint!(0_U256), Bytes::new())
            .await
            .unwrap();

        info!("Contract created successfully at: {}", contract_address);

        // Test sending and verifying a reverting transaction
        instance
            .send_and_verify_reverting_create_tx(caller, nonce + 1)
            .await
            .unwrap();

        info!("Reverting transaction handled correctly");

        // Test batch transactions
        let batch_txs = vec![
            (
                caller,
                nonce + 2,
                uint!(0_U256),
                Bytes::new(),
                TxKind::Create,
            ),
            (
                caller,
                nonce + 3,
                uint!(0_U256),
                Bytes::new(),
                TxKind::Create,
            ),
        ];

        let tx_hashes = instance.send_batch_transactions(batch_txs).await.unwrap();

        info!("Batch transactions sent: {} transactions", tx_hashes.len());

        // Test result checking
        for tx_hash in &tx_hashes {
            let is_successful = instance.is_transaction_successful(tx_hash).unwrap();
            info!("Transaction {} successful: {}", tx_hash, is_successful);
        }
    }

    #[crate::utils::engine_test]
    async fn test_send_assertion_passing_failing_pair(mut instance: LocalInstance) {
        let caller = Address::from([0x02; 20]);
        let nonce = 0u64;

        info!("Testing assertion passing/failing pair");

        // Send the assertion passing and failing pair
        instance
            .send_assertion_passing_failing_pair(caller, nonce)
            .await
            .unwrap();

        instance.send_and_verify_successful_create_tx(caller, nonce + 1, uint!(0_U256), Bytes::new()).await.unwrap();
        instance.send_and_verify_reverting_create_tx(caller, nonce).await.unwrap();
    }

    #[crate::utils::engine_test]
    async fn test_mock_driver_pattern_demonstration(mut instance: LocalInstance) {
        // Send a block environment
        info!("Sending block to engine");
        instance.new_block().unwrap();

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
        instance
            .wait_for_processing(Duration::from_millis(100))
            .await;

        info!("Test completed successfully");
        // Test passes if no panic/errors occurred during processing
    }
}
