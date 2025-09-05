use crate::{
    CoreEngine,
    engine::{
        TransactionResult,
        queue::{
            QueueTransaction,
            TransactionQueueSender,
            TxQueueContents,
        },
    },
    transport::{
        Transport,
        http::{
            HttpTransport,
            config::HttpTransportConfig,
            server::{
                Transaction,
                TransactionEnv,
            },
        },
        mock::MockTransport,
    },
};
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    primitives::{
        AccountInfo,
        Address,
        B256,
        BlockEnv,
        FixedBytes,
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
use revm::{
    context_interface::block::BlobExcessGasAndPrice,
    database::{
        CacheDB,
        EmptyDBTyped,
    },
    primitives::{
        Bytes,
        TxKind,
        address,
    },
};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::{
    debug,
    error,
    info,
    warn,
};

/// Test database error type
type TestDbError = std::convert::Infallible;

pub trait TestTransport: Sized {
    /// Creates a `LocalInstance` with a specific transport
    async fn new() -> Result<LocalInstance<Self>, String>;
    /// Advance the core engine block by sending a new blockenv to it
    async fn new_block(&self, block_number: u64) -> Result<(), String>;
    /// Send a transaction to the core engine via the transport
    async fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String>;
}

/// Creates a test instance of the core engine with mock transport.
/// This struct manages the lifecycle of a test engine instance.
///
/// Used for testing of transaction processing, assertion validation,
/// and multi-block scenarios. Provides pre-funded accounts and loaded test assertions.
pub struct LocalInstance<T: TestTransport> {
    /// Channel for sending transactions and blocks to the mock transport
    // mock_sender: TransactionQueueSender,
    /// The underlying database
    db: Arc<CacheDB<EmptyDBTyped<TestDbError>>>,
    /// The assertion store
    assertion_store: Arc<AssertionStore>,
    /// Transport task handle
    transport_handle: Option<JoinHandle<()>>,
    /// Engine task handle  
    engine_handle: Option<JoinHandle<()>>,
    /// Current block number
    block_number: u64,
    /// Shared transaction results from engine
    transaction_results: Arc<crate::TransactionsState>,
    /// Default account for transactions
    default_account: Address,
    /// Current nonce for the default account
    current_nonce: u64,
    transport: T,
}

impl<T: TestTransport> LocalInstance<T> {
    /// Create a new local instance with mock transport
    pub async fn new() -> Result<LocalInstance<T>, String> {
        T::new().await
    }

    /// Send a block with multiple transactions
    pub async fn send_block_with_txs(
        &mut self,
        transactions: Vec<(B256, TxEnv)>,
    ) -> Result<(), String> {
        // Send the block environment first
        self.transport.new_block(self.block_number).await?;
        self.block_number += 1;

        // Then send all transactions
        for (tx_hash, tx_env) in transactions {
            self.transport.send_transaction(tx_hash, tx_env).await?;
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

    /// Get the default account address
    pub fn default_account(&self) -> Address {
        self.default_account
    }

    /// Get the current nonce for the default account
    pub fn current_nonce(&self) -> u64 {
        self.current_nonce
    }

    /// Get the current nonce and increment it
    pub fn next_nonce(&mut self) -> u64 {
        let nonce = self.current_nonce;
        self.current_nonce += 1;
        nonce
    }

    /// Reset the nonce to a specific value
    pub fn reset_nonce(&mut self, nonce: u64) {
        self.current_nonce = nonce;
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
            .map_err(|e| format!("Failed to insert assertion: {e}"))
    }

    /// Create a simple test transaction using the default account
    pub fn create_test_transaction(&mut self, value: U256, data: Bytes) -> TxEnv {
        let nonce = self.next_nonce();
        TxEnv {
            caller: self.default_account,
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
            let account_info = AccountInfo {
                balance: *balance,
                ..Default::default()
            };
            db.insert_account_info(*address, account_info);
        }

        Ok(())
    }

    /// Send a successful CREATE transaction using the default account
    pub async fn send_successful_create_tx(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<B256, String> {
        // Ensure we have a block
        self.transport.new_block(self.block_number).await?;
        self.block_number += 1;

        let nonce = self.next_nonce();
        let caller = self.default_account;

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
        self.transport.send_transaction(tx_hash, tx_env).await?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(2)).await;

        Ok(tx_hash)
    }

    /// Send a reverting CREATE transaction using the default account
    pub async fn send_reverting_create_tx(&mut self) -> Result<B256, String> {
        // Ensure we have a block
        self.transport.new_block(self.block_number).await?;
        self.block_number += 1;

        let current_nonce = self.current_nonce();

        let nonce = self.next_nonce();
        let caller = self.default_account;

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
        self.transport.send_transaction(tx_hash, tx_env).await?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(2)).await;

        self.reset_nonce(current_nonce);

        Ok(tx_hash)
    }

    /// Send a CALL transaction to an existing contract using the default account
    pub async fn send_call_tx(
        &mut self,
        to: Address,
        value: U256,
        data: Bytes,
    ) -> Result<B256, String> {
        // Ensure we have a block
        self.transport.new_block(self.block_number).await?;
        self.block_number += 1;

        let nonce = self.next_nonce();
        let caller = self.default_account;

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
        self.transport.send_transaction(tx_hash, tx_env).await?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(2)).await;

        Ok(tx_hash)
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
            .map_err(|e| format!("Failed to insert custom assertion {artifact_name}: {e}"))
    }

    /// Get transaction result by hash
    pub fn get_transaction_result(&self, tx_hash: &B256) -> Option<TransactionResult> {
        self.transaction_results
            .get_transaction_result(tx_hash)
            .map(|r| r.clone())
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

    /// Send and verify a successful CREATE transaction using the default account
    pub async fn send_and_verify_successful_create_tx(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<Address, String> {
        let tx_hash = self.send_successful_create_tx(value, data).await?;

        if !self.is_transaction_successful(&tx_hash)? {
            return Err("Transaction was not successful".to_string());
        }

        // Calculate the expected contract address (simplified for testing)
        let contract_address = address!("76cae8af66cb2488933e640ba08650a3a8e7ae19");

        Ok(contract_address)
    }

    /// Send and verify a reverting transaction using the default account
    pub async fn send_and_verify_reverting_create_tx(&mut self) -> Result<(), String> {
        let tx_hash = self.send_reverting_create_tx().await?;

        if !self.is_transaction_reverted_but_valid(&tx_hash)? {
            return Err("Transaction should have reverted but been valid".to_string());
        }

        Ok(())
    }

    /// Sends a pair of assertion passing and failing transactions.
    /// The transactions call a preloaded counter contract, which can only
    /// be called once due to subsequent assertions invalidating.
    pub async fn send_assertion_passing_failing_pair(&mut self) -> Result<(), String> {
        // Ensure we have a block
        self.transport.new_block(self.block_number).await?;
        self.block_number += 1;

        let basefee = 10u64;

        // Create the first transaction (should pass)
        let mut tx_pass = counter_call();
        tx_pass.nonce = 0;
        tx_pass.gas_price = basefee.into();

        // Create the second transaction (should fail assertion)
        let mut tx_fail = counter_call();
        tx_fail.nonce = 1;
        tx_fail.gas_price = basefee.into();

        // Generate unique transaction hashes
        let hash_pass = FixedBytes::<32>::random();
        let hash_fail = FixedBytes::<32>::random();

        // Send the passing transaction first
        self.transport.send_transaction(hash_pass, tx_pass).await?;

        // Send the failing transaction second
        self.transport.send_transaction(hash_fail, tx_fail).await?;

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(2)).await;

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

impl<T: TestTransport> Drop for LocalInstance<T> {
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

/// Wrapper over the LocalInstance that provides MockTransport functionality
pub struct LocalInstanceMockDriver {
    /// Channel for sending transactions and blocks to the mock transport
    mock_sender: TransactionQueueSender,
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

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
            info!(target: "test_transport", "Engine task started, waiting for items...");
            info!(target: "test_transport", "Engine about to call run()");
            let result = engine.run().await;
            match result {
                Ok(_) => info!(target: "test_transport", "Engine run() completed successfully"),
                Err(e) => error!(target: "test_transport", "Engine run() failed: {:?}", e),
            }
            info!(target: "test_transport", "Engine task completed");
        });

        // Create mock transport with the channels
        let transport = MockTransport::with_receiver(engine_tx, mock_rx, state_results.clone());

        // Spawn the transport task
        let transport_handle = tokio::spawn(async move {
            info!(target: "test_transport", "Transport task started");
            info!(target: "test_transport", "Transport about to call run()");
            let result = transport.run().await;
            match result {
                Ok(_) => info!(target: "test_transport", "Transport run() completed successfully"),
                Err(e) => warn!(target: "test_transport", "Transport stopped with error: {}", e),
            }
            info!(target: "test_transport", "Transport task completed");
        });

        Ok(LocalInstance {
            db: underlying_db,
            assertion_store,
            transport_handle: Some(transport_handle),
            engine_handle: Some(engine_handle),
            block_number: 0,
            transaction_results: state_results,
            default_account: Address::from([0x01; 20]),
            current_nonce: 0,
            transport: LocalInstanceMockDriver {
                mock_sender: mock_tx,
            },
        })
    }

    async fn new_block(&self, block_number: u64) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending block: {:?}", block_number);
        let block_env = BlockEnv {
            number: block_number,
            gas_limit: 50_000_000, // Set higher gas limit for assertions
            ..Default::default()
        };
        // Increment block number for next time we call new_block

        let result = self
            .mock_sender
            .send(TxQueueContents::Block(block_env))
            .map_err(|e| format!("Failed to send block: {e}"));
        match &result {
            Ok(_) => info!(target: "test_transport", "Successfully sent block to mock_sender"),
            Err(e) => error!(target: "test_transport", "Failed to send block: {}", e),
        }
        result
    }

    async fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending transaction: {:?}", tx_hash);
        let queue_tx = QueueTransaction { tx_hash, tx_env };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }
}

#[derive(Debug)]
pub struct LocalInstanceHttpDriver {
    client: reqwest::Client,
    address: SocketAddr,
}

impl TestTransport for LocalInstanceHttpDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceHttpDriver", "Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();

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
            info!(target: "LocalInstanceHttpDriver", "Engine task started, waiting for items...");
            info!(target: "LocalInstanceHttpDriver", "Engine about to call run()");
            let result = engine.run().await;
            match result {
                Ok(_) => {
                    info!(target: "LocalInstanceHttpDriver", "Engine run() completed successfully")
                }
                Err(e) => error!(target: "LocalInstanceHttpDriver", "Engine run() failed: {:?}", e),
            }
            info!(target: "LocalInstanceHttpDriver", "Engine task completed");
        });

        // Find an available port dynamically
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {}", e))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {}", e))?;

        // Drop the listener so the port becomes available for HttpTransport
        drop(listener);

        // Create HTTP transport config with the dynamically assigned address
        let config = HttpTransportConfig { bind_addr: address };
        let transport = HttpTransport::new(config, engine_tx, state_results.clone()).unwrap();

        // Spawn the transport task
        let transport_handle = tokio::spawn(async move {
            info!(target: "LocalInstanceHttpDriver", "Transport task started");
            info!(target: "LocalInstanceHttpDriver", "Transport about to call run()");
            let result = transport.run().await;
            match result {
                Ok(_) => {
                    info!(target: "LocalInstanceHttpDriver", "Transport run() completed successfully")
                }
                Err(e) => {
                    warn!(target: "LocalInstanceHttpDriver", "Transport stopped with error: {}", e)
                }
            }
            info!(target: "LocalInstanceHttpDriver", "Transport task completed");
        });

        Ok(LocalInstance {
            db: underlying_db,
            assertion_store,
            transport_handle: Some(transport_handle),
            engine_handle: Some(engine_handle),
            block_number: 0,
            transaction_results: state_results,
            default_account: Address::from([0x01; 20]),
            current_nonce: 0,
            transport: LocalInstanceHttpDriver {
                client: reqwest::Client::new(),
                address,
            },
        })
    }

    async fn new_block(&self, block_number: u64) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending block: {:?}", block_number);

        let blockenv = BlockEnv {
            number: block_number,
            gas_limit: 50_000_000, // Set higher gas limit for assertions
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,
                blob_gasprice: 0,
            }),
            ..Default::default()
        };

        // jsonrpc request for sending a blockenv to the sidecar
        let request = json!({
          "id": 1,
          "jsonrpc": "2.0",
          "method": "sendBlockEnv",
          "params": {
            "number": blockenv.number,
            "beneficiary": blockenv.beneficiary.to_string(),
            "timestamp": blockenv.timestamp,
            "gas_limit": blockenv.gas_limit,
            "basefee": blockenv.basefee,
            "difficulty": format!("0x{:x}", blockenv.difficulty),
            "prevrandao": blockenv.prevrandao.map(|h| h.to_string()),
            "blob_excess_gas_and_price": blockenv.blob_excess_gas_and_price.map(|blob| json!({
                "excess_blob_gas": blob.excess_blob_gas,
                "blob_gasprice": blob.blob_gasprice
            }))
          }
        });

        // Retry logic to wait for HTTP server to be ready
        let mut attempts = 0;
        let max_attempts = 10;
        let mut last_error = String::new();

        while attempts < max_attempts {
            attempts += 1;

            match self
                .client
                .post(format!("http://{}/tx", self.address))
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        return Err(format!("HTTP error: {}", response.status()));
                    }

                    let json_response: serde_json::Value = response
                        .json()
                        .await
                        .map_err(|e| format!("Failed to parse response: {}", e))?;

                    if let Some(error) = json_response.get("error") {
                        return Err(format!("JSON-RPC error: {}", error));
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {}", e);
                    if attempts < max_attempts {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/{}), retrying...", attempts, max_attempts);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }

        Err(format!(
            "Failed after {} attempts: {}",
            max_attempts, last_error
        ))
    }

    async fn send_transaction(&self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        debug!(target: "LocalInstanceHttpDriver", "Sending transaction: {}", tx_hash);

        // Create the transaction structure
        let transaction = Transaction {
            hash: tx_hash.to_string(),
            tx_env: TransactionEnv {
                caller: tx_env.caller.to_string(),
                gas_limit: tx_env.gas_limit,
                gas_price: tx_env.gas_price.to_string(),
                transact_to: match tx_env.kind {
                    TxKind::Call(addr) => Some(addr.to_string()),
                    TxKind::Create => None,
                },
                value: tx_env.value.to_string(),
                data: format!("0x{}", alloy::hex::encode(&tx_env.data)),
                nonce: tx_env.nonce,
                chain_id: tx_env.chain_id.unwrap_or_default(),
                access_list: Vec::new(), // Empty access list for now
            },
        };

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "sendTransactions",
            "params": {
                "transactions": [transaction]
            }
        });

        debug!(target: "LocalInstanceHttpDriver", "Sending HTTP request: {}", serde_json::to_string_pretty(&request).unwrap_or_default());

        let mut last_error = String::new();
        let mut attempts = 0;
        let max_attempts = 10;

        while attempts < max_attempts {
            attempts += 1;

            match self
                .client
                .post(format!("http://{}/tx", self.address))
                .header("content-type", "application/json")
                .json(&request)
                .send()
                .await
            {
                Ok(response) => {
                    if !response.status().is_success() {
                        return Err(format!("HTTP error: {}", response.status()));
                    }

                    let json_response: serde_json::Value = response
                        .json()
                        .await
                        .map_err(|e| format!("Failed to parse response: {}", e))?;

                    debug!(target: "LocalInstanceHttpDriver", "Received response: {}", serde_json::to_string_pretty(&json_response).unwrap_or_default());

                    if let Some(error) = json_response.get("error") {
                        return Err(format!("JSON-RPC error: {}", error));
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {}", e);
                    if attempts < max_attempts {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/{}), retrying...", attempts, max_attempts);
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                }
            }
        }

        Err(format!(
            "Failed after {} attempts: {}",
            max_attempts, last_error
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use revm::primitives::uint;

    #[crate::utils::engine_test(all)]
    async fn test_instance_send_assertion_passing_failing_pair(mut instance: LocalInstance<_>) {
        info!("Testing assertion passing/failing pair");

        // Send the assertion passing and failing pair
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
}
