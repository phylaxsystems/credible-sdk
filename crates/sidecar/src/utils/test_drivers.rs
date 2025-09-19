#![allow(clippy::too_many_lines)]
use super::instance::{
    LocalInstance,
    TestTransport,
};
use crate::{
    Cache,
    CoreEngine,
    cache::sources::{
        Source,
        besu_client::BesuClient,
        sequencer::Sequencer,
    },
    engine::queue::{
        QueueBlockEnv,
        QueueTransaction,
        TransactionQueueSender,
        TxQueueContents,
    },
    transport::{
        Transport,
        grpc::{
            GrpcTransport,
            config::GrpcTransportConfig,
            pb::{
                BlockEnvEnvelope,
                ReorgRequest,
                SendTransactionsRequest,
                Transaction as GrpcTransaction,
                TransactionEnv as GrpcTransactionEnv,
                sidecar_transport_client::SidecarTransportClient,
            },
        },
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
    utils::test_cache::DualProtocolMockServer,
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
use tonic::transport::Channel;
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
    /// Ordered hashes for transactions sent since the last block update
    block_tx_hashes: Vec<TxHash>,
    /// Explicit override for the next `n_transactions` value
    override_n_transactions: Option<u64>,
    /// Explicit override for the next `last_tx_hash` value
    /// We use Option<Option<TxHash>> to distinguish: None (use default), Some(None) (force no hash), Some(Some(hash)) (force specific hash)
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceMockDriver {
    fn next_block_metadata(&self) -> (u64, Option<TxHash>) {
        let n_transactions = self
            .override_n_transactions
            .unwrap_or(self.block_tx_hashes.len() as u64);

        let last_tx_hash = match &self.override_last_tx_hash {
            Some(value) => *value,
            None => self.block_tx_hashes.last().copied(),
        };

        (n_transactions, last_tx_hash)
    }
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Create the database and state
        let sequencer_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create sequencer mock");
        let besu_client_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create besu client mock");
        let mock_sequencer_db: Arc<dyn Source> = Arc::new(
            Sequencer::try_new(&sequencer_http_mock.http_url())
                .await
                .expect("Failed to create sequencer mock"),
        );
        let mock_besu_client_db: Arc<dyn Source> =
            BesuClient::try_build(besu_client_http_mock.ws_url())
                .await
                .expect("Failed to create besu client mock");
        let sources = vec![mock_besu_client_db, mock_sequencer_db];
        let cache = Arc::new(Cache::new(sources.clone(), 10));
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
            sequencer_http_mock,
            besu_client_http_mock,
            assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            state_results,
            default_account,
            0,
            None,
            sources,
            LocalInstanceMockDriver {
                mock_sender: mock_tx,
                block_tx_hashes: Vec::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    async fn new_block(&mut self, block_number: u64) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending block: {:?}", block_number);
        let (n_transactions, last_tx_hash) = self.next_block_metadata();
        let block_env = QueueBlockEnv {
            block_env: BlockEnv {
                number: block_number,
                gas_limit: 50_000_000, // Set higher gas limit for assertions
                ..Default::default()
            },
            last_tx_hash,
            n_transactions,
        };

        self.block_tx_hashes.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

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
        self.block_tx_hashes.push(tx_hash);
        info!(target: "test_transport", "LocalInstance sending transaction: {:?}", tx_hash);
        let queue_tx = QueueTransaction { tx_hash, tx_env };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx, Span::current()))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }

    async fn reorg(&mut self, tx_hash: B256) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending reorg for: {:?}", tx_hash);
        let tracked_hash: TxHash = tx_hash;
        if let Some(last_hash) = self.block_tx_hashes.last() {
            if last_hash == &tracked_hash {
                self.block_tx_hashes.pop();
            } else {
                debug!(
                    target: "test_transport",
                    "Reorg hash {:?} does not match last tracked transaction {:?}",
                    tracked_hash,
                    last_hash
                );
            }
        }
        self.mock_sender
            .send(TxQueueContents::Reorg(tx_hash, Span::current()))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }
}

#[derive(Debug)]
pub struct LocalInstanceHttpDriver {
    client: reqwest::Client,
    address: SocketAddr,
    block_tx_hashes: Vec<TxHash>,
    override_n_transactions: Option<u64>,
    /// We use Option<Option<TxHash>> to distinguish: None (use default), Some(None) (force no hash), Some(Some(hash)) (force specific hash)
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceHttpDriver {
    fn next_block_metadata(&self) -> (u64, Option<TxHash>) {
        let n_transactions = self
            .override_n_transactions
            .unwrap_or(self.block_tx_hashes.len() as u64);

        let last_tx_hash = match &self.override_last_tx_hash {
            Some(value) => *value,
            None => self.block_tx_hashes.last().copied(),
        };

        (n_transactions, last_tx_hash)
    }
}

impl TestTransport for LocalInstanceHttpDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceHttpDriver", "Creating LocalInstance with HttpTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();

        // Create the database and state
        let sequencer_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create sequencer mock");
        let besu_client_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create besu client mock");
        let mock_sequencer_db: Arc<dyn Source> = Arc::new(
            Sequencer::try_new(&sequencer_http_mock.http_url())
                .await
                .expect("Failed to create sequencer mock"),
        );
        let mock_besu_client_db: Arc<dyn Source> =
            BesuClient::try_build(besu_client_http_mock.ws_url())
                .await
                .expect("Failed to create besu client mock");

        let sources = vec![mock_besu_client_db, mock_sequencer_db];
        let cache = Arc::new(Cache::new(sources.clone(), 10));
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
            sequencer_http_mock,
            besu_client_http_mock,
            assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            state_results,
            default_account,
            0,
            Some(&address),
            sources,
            LocalInstanceHttpDriver {
                client: reqwest::Client::new(),
                address,
                block_tx_hashes: Vec::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    async fn new_block(&mut self, block_number: u64) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending block: {:?}", block_number);

        let (n_transactions, last_tx_hash) = self.next_block_metadata();
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
            "n_transactions": n_transactions,
            "last_tx_hash": last_tx_hash
          }
        });

        self.block_tx_hashes.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

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
        self.block_tx_hashes.push(tx_hash);

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

    async fn reorg(&mut self, tx_hash: B256) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending reorg for: {:?}", tx_hash);

        let tracked_hash: TxHash = tx_hash;
        if let Some(last_hash) = self.block_tx_hashes.last() {
            if last_hash == &tracked_hash {
                self.block_tx_hashes.pop();
            } else {
                debug!(
                    target: "LocalInstanceHttpDriver",
                    "Reorg hash {:?} does not match last tracked transaction {:?}",
                    tracked_hash,
                    last_hash
                );
            }
        }

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

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }
}

/// Wrapper over the `LocalInstance` that provides `GrpcTransport` functionality
pub struct LocalInstanceGrpcDriver {
    /// gRPC client for the transport
    client: SidecarTransportClient<Channel>,
    /// Ordered hashes for transactions sent since the last block announcement
    block_tx_hashes: Vec<TxHash>,
    /// Explicit override for the next `n_transactions` value
    override_n_transactions: Option<u64>,
    /// Explicit override for the next `last_tx_hash` value
    /// We use Option<Option<TxHash>> to distinguish: None (use default), Some(None) (force no hash), Some(Some(hash)) (force specific hash)
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceGrpcDriver {
    fn next_block_metadata(&self) -> (u64, Option<TxHash>) {
        let n_transactions = self
            .override_n_transactions
            .unwrap_or(self.block_tx_hashes.len() as u64);

        let last_tx_hash = match &self.override_last_tx_hash {
            Some(value) => *value,
            None => self.block_tx_hashes.last().copied(),
        };

        (n_transactions, last_tx_hash)
    }
}

impl TestTransport for LocalInstanceGrpcDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceGrpcDriver", "Creating LocalInstance with GrpcTransport");

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();

        // Create the database and state
        let sequencer_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create sequencer mock");
        let besu_client_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create besu client mock");
        let mock_sequencer_db: Arc<dyn Source> = Arc::new(
            Sequencer::try_new(&sequencer_http_mock.http_url())
                .await
                .expect("Failed to create sequencer mock"),
        );
        let mock_besu_client_db: Arc<dyn Source> =
            BesuClient::try_build(besu_client_http_mock.ws_url())
                .await
                .expect("Failed to create besu client mock");
        let sources = vec![mock_besu_client_db, mock_sequencer_db];
        let cache = Arc::new(Cache::new(sources.clone(), 10));
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

        // Spawn the engine task
        let engine_handle = tokio::spawn(async move {
            info!(target: "LocalInstanceGrpcDriver", "Engine task started, waiting for items...");
            info!(target: "LocalInstanceGrpcDriver", "Engine about to call run()");
            let result = engine.run().await;
            match result {
                Ok(()) => {
                    info!(target: "LocalInstanceGrpcDriver", "Engine run() completed successfully");
                }
                Err(e) => error!(target: "LocalInstanceGrpcDriver", "Engine run() failed: {:?}", e),
            }
            info!(target: "LocalInstanceGrpcDriver", "Engine task completed");
        });

        // Find an available port dynamically
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        // Drop the listener so the port becomes available for GrpcTransport
        drop(listener);

        // Create gRPC transport config with the dynamically assigned address
        let config = GrpcTransportConfig { bind_addr: address };
        let transport = GrpcTransport::new(config, engine_tx, state_results.clone()).unwrap();

        // Spawn the transport task
        let transport_handle = tokio::spawn(async move {
            info!(target: "LocalInstanceGrpcDriver", "Transport task started");
            info!(target: "LocalInstanceGrpcDriver", "Transport about to call run()");
            let result = transport.run().await;
            match result {
                Ok(()) => {
                    info!(target: "LocalInstanceGrpcDriver", "Transport run() completed successfully");
                }
                Err(e) => {
                    warn!(target: "LocalInstanceGrpcDriver", "Transport stopped with error: {e}");
                }
            }
            info!(target: "LocalInstanceGrpcDriver", "Transport task completed");
        });

        // Create gRPC client with retry logic
        let mut attempts = 0;
        let client = loop {
            attempts += 1;
            match SidecarTransportClient::connect(format!("http://{address}")).await {
                Ok(client) => break client,
                Err(e) => {
                    if attempts >= MAX_HTTP_RETRY_ATTEMPTS {
                        return Err(format!(
                            "Failed to connect to gRPC server after {MAX_HTTP_RETRY_ATTEMPTS} attempts: {e}"
                        ));
                    }
                    debug!(target: "LocalInstanceGrpcDriver", "gRPC connection failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
                    tokio::time::sleep(tokio::time::Duration::from_millis(HTTP_RETRY_DELAY_MS))
                        .await;
                }
            }
        };

        Ok(LocalInstance::new_internal(
            underlying_db,
            sequencer_http_mock,
            besu_client_http_mock,
            assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            state_results,
            default_account,
            0,
            Some(&address),
            sources,
            LocalInstanceGrpcDriver {
                client,
                block_tx_hashes: Vec::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    async fn new_block(&mut self, block_number: u64) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending block: {:?}", block_number);

        let (n_transactions, last_tx_hash) = self.next_block_metadata();
        let blockenv = BlockEnv {
            number: block_number,
            gas_limit: 50_000_000, // Set higher gas limit for assertions
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,
                blob_gasprice: 0,
            }),
            ..Default::default()
        };

        // Create JSON exactly like HTTP transport does - this must match QueueBlockEnv serialization format
        let mut block_env_json = serde_json::json!({
            "number": blockenv.number,
            "beneficiary": blockenv.beneficiary.to_string(),
            "timestamp": blockenv.timestamp,
            "gas_limit": blockenv.gas_limit,
            "basefee": blockenv.basefee,
            "difficulty": format!("0x{:x}", blockenv.difficulty),
            "prevrandao": blockenv.prevrandao.map(|h| h.to_string()),
            "blob_excess_gas_and_price": blockenv.blob_excess_gas_and_price.map(|blob| serde_json::json!({
                "excess_blob_gas": blob.excess_blob_gas,
                "blob_gasprice": blob.blob_gasprice
            }))
        });

        // Add n_transactions and last_tx_hash to match HTTP transport format
        if let Some(obj) = block_env_json.as_object_mut() {
            obj.insert(
                "n_transactions".to_string(),
                serde_json::Value::Number(serde_json::Number::from(n_transactions)),
            );
            if let Some(hash) = last_tx_hash {
                obj.insert(
                    "last_tx_hash".to_string(),
                    serde_json::Value::String(hash.to_string()),
                );
            }
        }

        let request = BlockEnvEnvelope {
            block_env_json: block_env_json.to_string(),
            last_tx_hash: last_tx_hash.map(|h| h.to_string()).unwrap_or_default(),
            n_transactions,
        };

        self.block_tx_hashes.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        // Send the gRPC request with retry logic
        let mut attempts = 0;
        let mut last_error = String::new();

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self.client.send_block_env(request.clone()).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    if !ack.accepted {
                        return Err(format!("Block env rejected: {}", ack.message));
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("gRPC request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceGrpcDriver", "gRPC request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
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
        debug!(target: "LocalInstanceGrpcDriver", "Sending transaction: {}", tx_hash);

        // Validate inputs
        if tx_env.gas_limit == 0 {
            return Err("Gas limit cannot be zero".to_string());
        }

        self.block_tx_hashes.push(tx_hash);

        // Create the gRPC transaction structure
        let transaction = GrpcTransaction {
            hash: tx_hash.to_string(),
            tx_env: Some(GrpcTransactionEnv {
                caller: tx_env.caller.to_string(),
                gas_limit: tx_env.gas_limit,
                gas_price: tx_env.gas_price.to_string(),
                transact_to: match tx_env.kind {
                    TxKind::Call(addr) => addr.to_string(),
                    TxKind::Create => "0x".to_string(), // Must be "0x" for create, not empty string
                },
                value: tx_env.value.to_string(),
                data: format!("0x{}", alloy::hex::encode(&tx_env.data)),
                nonce: tx_env.nonce,
                chain_id: tx_env.chain_id.unwrap_or_default(),
            }),
        };

        let request = SendTransactionsRequest {
            transactions: vec![transaction],
        };

        debug!(target: "LocalInstanceGrpcDriver", "Sending gRPC transaction request for hash: {}", tx_hash);

        let mut last_error = String::new();
        let mut attempts = 0;

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self.client.send_transactions(request.clone()).await {
                Ok(response) => {
                    let resp = response.into_inner();
                    if resp.accepted_count == 0 {
                        return Err(format!(
                            "Transaction rejected: {} (request_count: {})",
                            resp.message, resp.request_count
                        ));
                    }
                    if resp.accepted_count != resp.request_count {
                        return Err(format!(
                            "Partial acceptance: {}/{} transactions accepted: {}",
                            resp.accepted_count, resp.request_count, resp.message
                        ));
                    }
                    debug!(target: "LocalInstanceGrpcDriver", "Transaction accepted: {}/{} transactions", resp.accepted_count, resp.request_count);
                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("gRPC request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceGrpcDriver", "gRPC request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
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

    async fn reorg(&mut self, tx_hash: B256) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending reorg for: {:?}", tx_hash);

        let tracked_hash: TxHash = tx_hash;
        if let Some(last_hash) = self.block_tx_hashes.last() {
            if last_hash == &tracked_hash {
                self.block_tx_hashes.pop();
            } else {
                debug!(
                    target: "LocalInstanceGrpcDriver",
                    "Reorg hash {:?} does not match last tracked transaction {:?}",
                    tracked_hash,
                    last_hash
                );
            }
        }

        let request = ReorgRequest {
            removed_tx_hash: tx_hash.to_string(),
        };

        debug!(target: "LocalInstanceGrpcDriver", "Sending gRPC reorg request");

        let mut last_error = String::new();
        let mut attempts = 0;

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self.client.clone().reorg(request.clone()).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    if !ack.accepted {
                        return Err(format!("Reorg rejected: {}", ack.message));
                    }
                    return Ok(());
                }
                Err(e) => {
                    last_error = format!("gRPC request failed: {e}");
                    if attempts < MAX_HTTP_RETRY_ATTEMPTS {
                        debug!(target: "LocalInstanceGrpcDriver", "gRPC request failed (attempt {}/{}), retrying...", attempts, MAX_HTTP_RETRY_ATTEMPTS);
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

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }
}
