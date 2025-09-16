use super::instance::{
    LocalInstance,
    TestTransport,
};
use crate::{
    Cache,
    CoreEngine,
    engine::queue::{
        QueueBlockEnv,
        QueueTransaction,
        TransactionQueueSender,
        TxQueueContents,
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
    utils::test_cache::{
        MockBesuClientDB,
        MockSequencerDB,
    },
};
use alloy::primitives::TxHash;
use assertion_executor::{
    AssertionExecutor,
    ExecutorConfig,
    db::overlay::OverlayDb,
    primitives::{
        AccountInfo,
        Address,
        B256,
        BlockEnv,
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
    database::CacheDB,
    primitives::TxKind,
};
use serde_json::json;
use std::{
    net::SocketAddr,
    sync::Arc,
};
use tracing::{
    Span,
    debug,
    error,
    info,
    warn,
};

/// Setup test database with common accounts pre-funded
fn populate_test_database(underlying_db: &mut CacheDB<Arc<Cache>>) -> Address {
    // Insert default counter contract into the underlying db
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

    default_account
}

/// Setup assertion store with test assertions pre-loaded
fn setup_assertion_store() -> Result<Arc<AssertionStore>, String> {
    let assertion_store = Arc::new(
        AssertionStore::new_ephemeral()
            .map_err(|e| format!("Failed to create assertion store: {e}"))?,
    );

    // Insert counter assertion into store
    let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
    assertion_store
        .insert(
            COUNTER_ADDRESS,
            AssertionState::new_test(&assertion_bytecode),
        )
        .unwrap();

    Ok(assertion_store)
}

// HTTP transport retry configuration constants
const MAX_HTTP_RETRY_ATTEMPTS: usize = 3;
const HTTP_RETRY_DELAY_MS: u64 = 100;

/// Wrapper over the `LocalInstance` that provides `MockTransport` functionality
pub struct LocalInstanceMockDriver {
    /// Channel for sending transactions and blocks to the mock transport
    mock_sender: TransactionQueueSender,
    n_transactions: u64,
    last_tx_hash: Option<TxHash>,
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Create the database and state
        let mock_sequencer_db = MockSequencerDB::build();
        let mock_besu_client_db = MockBesuClientDB::build();
        let cache = Arc::new(Cache::new(
            vec![mock_sequencer_db.clone(), mock_besu_client_db.clone()],
            10,
        ));
        let mut underlying_db = revm::database::CacheDB::new(cache.clone());
        let default_account = populate_test_database(&mut underlying_db);

        let underlying_db = Arc::new(underlying_db);

        let state = OverlayDb::new(Some(underlying_db.clone()), 1024);

        // Create assertion store and executor
        let assertion_store = setup_assertion_store()?;

        let assertion_executor =
            AssertionExecutor::new(ExecutorConfig::default(), (*assertion_store).clone());

        // Create the engine with TransactionsState
        let state_results = crate::TransactionsState::new();
        let mut engine = CoreEngine::new(
            state,
            cache,
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
                Ok(()) => info!(target: "test_transport", "Engine run() completed successfully"),
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
                Ok(()) => info!(target: "test_transport", "Transport run() completed successfully"),
                Err(e) => warn!(target: "test_transport", "Transport stopped with error: {}", e),
            }
            info!(target: "test_transport", "Transport task completed");
        });

        Ok(LocalInstance::new_internal(
            underlying_db,
            mock_sequencer_db,
            mock_besu_client_db,
            assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            state_results,
            default_account,
            0,
            None,
            LocalInstanceMockDriver {
                mock_sender: mock_tx,
                n_transactions: 0,
                last_tx_hash: None,
            },
        ))
    }

    async fn new_block(&mut self, block_number: u64) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending block: {:?}", block_number);
        let block_env = QueueBlockEnv {
            block_env: BlockEnv {
                number: block_number,
                gas_limit: 50_000_000, // Set higher gas limit for assertions
                ..Default::default()
            },
            last_tx_hash: self.last_tx_hash,
            n_transactions: self.n_transactions,
        };

        self.n_transactions = 0;

        // Increment block number for next time we call new_block

        let result = self
            .mock_sender
            .send(TxQueueContents::Block(block_env, Span::current()))
            .map_err(|e| format!("Failed to send block: {e}"));
        match &result {
            Ok(()) => info!(target: "test_transport", "Successfully sent block to mock_sender"),
            Err(e) => error!(target: "test_transport", "Failed to send block: {}", e),
        }
        result
    }

    async fn send_transaction(&mut self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        self.n_transactions += 1;
        self.last_tx_hash = Some(tx_hash);
        info!(target: "test_transport", "LocalInstance sending transaction: {:?}", tx_hash);
        let queue_tx = QueueTransaction { tx_hash, tx_env };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx, Span::current()))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }

    async fn reorg(&self, tx_hash: B256) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending reorg for: {:?}", tx_hash);
        self.mock_sender
            .send(TxQueueContents::Reorg(tx_hash, Span::current()))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }
}

#[derive(Debug)]
pub struct LocalInstanceHttpDriver {
    client: reqwest::Client,
    address: SocketAddr,
    n_transactions: u64,
    last_tx_hash: Option<TxHash>,
}

impl TestTransport for LocalInstanceHttpDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceHttpDriver", "Creating LocalInstance with HttpTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();

        // Create the database and state
        let mock_sequencer_db = MockSequencerDB::build();
        let mock_besu_client_db = MockBesuClientDB::build();
        let cache = Arc::new(Cache::new(
            vec![mock_sequencer_db.clone(), mock_besu_client_db.clone()],
            10,
        ));
        let mut underlying_db = revm::database::CacheDB::new(cache.clone());
        let default_account = populate_test_database(&mut underlying_db);

        let underlying_db = Arc::new(underlying_db);

        let state = OverlayDb::new(Some(underlying_db.clone()), 1024);

        // Create assertion store and executor
        let assertion_store = setup_assertion_store()?;

        let assertion_executor =
            AssertionExecutor::new(ExecutorConfig::default(), (*assertion_store).clone());

        // Create the engine with TransactionsState
        let state_results = crate::TransactionsState::new();
        let mut engine = CoreEngine::new(
            state,
            cache,
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
                Ok(()) => {
                    info!(target: "LocalInstanceHttpDriver", "Engine run() completed successfully");
                }
                Err(e) => error!(target: "LocalInstanceHttpDriver", "Engine run() failed: {:?}", e),
            }
            info!(target: "LocalInstanceHttpDriver", "Engine task completed");
        });

        // Find an available port dynamically
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

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
                Ok(()) => {
                    info!(target: "LocalInstanceHttpDriver", "Transport run() completed successfully");
                }
                Err(e) => {
                    warn!(target: "LocalInstanceHttpDriver", "Transport stopped with error: {e}");
                }
            }
            info!(target: "LocalInstanceHttpDriver", "Transport task completed");
        });

        Ok(LocalInstance::new_internal(
            underlying_db,
            mock_sequencer_db,
            mock_besu_client_db,
            assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            state_results,
            default_account,
            0,
            Some(&address),
            LocalInstanceHttpDriver {
                client: reqwest::Client::new(),
                address,
                n_transactions: 0,
                last_tx_hash: None,
            },
        ))
    }

    async fn new_block(&mut self, block_number: u64) -> Result<(), String> {
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
            })),
            "n_transactions": self.n_transactions,
            "last_tx_hash": self.last_tx_hash
          }
        });

        self.n_transactions = 0;
        self.last_tx_hash = None;

        // Retry logic to wait for HTTP server to be ready
        let mut attempts = 0;
        let mut last_error = String::new();

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
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
                        .map_err(|e| format!("Failed to parse response: {e}"))?;

                    if let Some(error) = json_response.get("error") {
                        return Err(format!("JSON-RPC error: {error}"));
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
                        tokio::time::sleep(tokio::time::Duration::from_millis(HTTP_RETRY_DELAY_MS))
                            .await;
                    }
                }
            }
        }

        Err(format!(
            "Failed after {MAX_HTTP_RETRY_ATTEMPTS} attempts: {last_error}",
        ))
    }

    async fn send_transaction(&mut self, tx_hash: B256, tx_env: TxEnv) -> Result<(), String> {
        debug!(target: "LocalInstanceHttpDriver", "Sending transaction: {}", tx_hash);
        self.n_transactions += 1;
        self.last_tx_hash = Some(tx_hash);

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

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
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
                        .map_err(|e| format!("Failed to parse response: {e}"))?;

                    debug!(target: "LocalInstanceHttpDriver", "Received response: {}", serde_json::to_string_pretty(&json_response).unwrap_or_default());

                    if let Some(error) = json_response.get("error") {
                        return Err(format!("JSON-RPC error: {error}"));
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
                        tokio::time::sleep(tokio::time::Duration::from_millis(HTTP_RETRY_DELAY_MS))
                            .await;
                    }
                }
            }
        }

        Err(format!(
            "Failed after {MAX_HTTP_RETRY_ATTEMPTS} attempts: {last_error}",
        ))
    }

    async fn reorg(&self, tx_hash: B256) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending reorg for: {:?}", tx_hash);

        let request = json!({
          "id": 1,
          "jsonrpc": "2.0",
          "method": "reorg",
          "params": {
            "removedTxHash": tx_hash,
          }
        });

        debug!(target: "LocalInstanceHttpDriver", "Sending HTTP request: {}", serde_json::to_string_pretty(&request).unwrap_or_default());

        let mut last_error = String::new();
        let mut attempts = 0;

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
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
                        .map_err(|e| format!("Failed to parse response: {e}"))?;

                    debug!(target: "LocalInstanceHttpDriver", "Received response: {}", serde_json::to_string_pretty(&json_response).unwrap_or_default());

                    if let Some(error) = json_response.get("error") {
                        return Err(format!("JSON-RPC error: {error}"));
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("HTTP request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceHttpDriver", "HTTP request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
                        tokio::time::sleep(tokio::time::Duration::from_millis(HTTP_RETRY_DELAY_MS))
                            .await;
                    }
                }
            }
        }

        Err(format!(
            "Failed after {MAX_HTTP_RETRY_ATTEMPTS} attempts: {last_error}",
        ))
    }
}
