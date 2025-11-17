#![allow(clippy::too_many_lines)]
use super::instance::{
    LocalInstance,
    TestTransport,
};
use crate::{
    CoreEngine,
    Sources,
    cache::sources::{
        Source,
        eth_rpc_source::EthRpcSource,
        sequencer::Sequencer,
    },
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        TransactionQueueSender,
        TxQueueContents,
    },
    execution_ids::TxExecutionId,
    transport::{
        Transport,
        grpc::{
            GrpcTransport,
            config::GrpcTransportConfig,
            pb::{
                ReorgRequest,
                SendTransactionsRequest,
                Transaction as GrpcTransaction,
                TransactionEnv as GrpcTransactionEnv,
                TxExecutionId as GrpcTxExecutionId,
                sidecar_transport_client::SidecarTransportClient,
            },
        },
        http::{
            HttpTransport,
            config::HttpTransportConfig,
            server::Transaction,
        },
        mock::MockTransport,
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
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use revm::{
    context_interface::block::BlobExcessGasAndPrice,
    database::CacheDB,
    primitives::TxKind,
};
use serde_json::json;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
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
fn populate_test_database(underlying_db: &mut CacheDB<Arc<Sources>>) -> Address {
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
    /// Ordered hashes for transactions sent per iteration since the last block update
    block_tx_hashes_by_iteration: HashMap<u64, Vec<TxHash>>,
    /// Explicit override for the next `n_transactions` value
    override_n_transactions: Option<u64>,
    /// Explicit override for the next `last_tx_hash` value
    /// We use Option<Option<TxHash>> to distinguish: None (use default), Some(None) (force no hash), Some(Some(hash)) (force specific hash)
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceMockDriver {
    fn next_block_metadata(&self, selected_iteration_id: u64) -> (u64, Option<TxHash>) {
        let iteration_hashes: &[TxHash] = self
            .block_tx_hashes_by_iteration
            .get(&selected_iteration_id)
            .map_or(&[], |v| v.as_slice());

        let n_transactions = self
            .override_n_transactions
            .unwrap_or(iteration_hashes.len() as u64);

        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        (n_transactions, last_tx_hash)
    }

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            let iteration_hashes: &[TxHash] = self
                .block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(&[], |v| v.as_slice());
            iteration_hashes.last().copied()
        }
    }
}

/// Common initialization for all driver types
struct CommonSetup {
    underlying_db: Arc<CacheDB<Arc<Sources>>>,
    sources: Arc<Sources>,
    sequencer_http_mock: DualProtocolMockServer,
    eth_rpc_source_http_mock: DualProtocolMockServer,
    assertion_store: Arc<AssertionStore>,
    state_results: Arc<crate::TransactionsState>,
    default_account: Address,
    list_of_sources: Vec<Arc<dyn Source>>,
}

impl CommonSetup {
    /// Initialize common database, cache, and mocks for test drivers
    async fn new(assertion_store: Option<AssertionStore>) -> Result<Self, String> {
        // Create the database and state
        let sequencer_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create sequencer mock");
        let eth_rpc_source_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create eth rpc source mock");
        let mock_sequencer_db: Arc<dyn Source> = Arc::new(
            Sequencer::try_new(&sequencer_http_mock.http_url())
                .await
                .expect("Failed to create sequencer mock"),
        );
        let eth_rpc_source_db: Arc<dyn Source> = EthRpcSource::try_build(
            eth_rpc_source_http_mock.ws_url(),
            eth_rpc_source_http_mock.http_url(),
        )
        .await
        .expect("Failed to create eth rpc source mock");
        let sources = vec![eth_rpc_source_db, mock_sequencer_db];
        let cache = Arc::new(Sources::new(sources.clone(), 10));
        let mut underlying_db = revm::database::CacheDB::new(cache.clone());
        let default_account = populate_test_database(&mut underlying_db);

        let underlying_db = Arc::new(underlying_db);

        // Create assertion store if not provided
        let assertion_store = match assertion_store {
            Some(store) => Arc::new(store),
            None => setup_assertion_store()?,
        };

        let state_results = crate::TransactionsState::new();

        Ok(CommonSetup {
            underlying_db,
            sources: cache,
            sequencer_http_mock,
            eth_rpc_source_http_mock,
            assertion_store,
            state_results,
            default_account,
            list_of_sources: sources,
        })
    }

    /// Spawn the engine task with the provided receiver
    async fn spawn_engine(
        &self,
        engine_rx: channel::Receiver<TxQueueContents>,
    ) -> tokio::task::JoinHandle<()> {
        let state = OverlayDb::new(Some(self.underlying_db.clone()));
        let assertion_executor =
            AssertionExecutor::new(ExecutorConfig::default(), (*self.assertion_store).clone());

        let mut engine = CoreEngine::new(
            state,
            self.sources.clone(),
            engine_rx,
            assertion_executor,
            self.state_results.clone(),
            10,
            Duration::from_millis(100),
            Duration::from_millis(20),
            false,
            #[cfg(feature = "cache_validation")]
            Some(&self.eth_rpc_source_http_mock.ws_url()),
        )
        .await;

        tokio::spawn(async move {
            info!(target: "test_driver", "Engine task started, waiting for items...");
            info!(target: "test_driver", "Engine about to call run()");
            let result = engine.run().await;
            match result {
                Ok(()) => info!(target: "test_driver", "Engine run() completed successfully"),
                Err(e) => error!(target: "test_driver", "Engine run() failed: {:?}", e),
            }
            info!(target: "test_driver", "Engine task completed");
        })
    }
}

impl LocalInstanceMockDriver {
    /// Same as `new()`, but takes in an `AssertionStore` as an argument
    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        let setup = CommonSetup::new(Some(assertion_store)).await?;

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Spawn the engine task
        let engine_handle = setup.spawn_engine(engine_rx).await;

        // Create mock transport with the channels
        let transport =
            MockTransport::with_receiver(engine_tx, mock_rx, setup.state_results.clone());

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
            setup.underlying_db,
            setup.sources.clone(),
            setup.sequencer_http_mock,
            setup.eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            setup.state_results,
            setup.default_account,
            None,
            setup.list_of_sources,
            LocalInstanceMockDriver {
                mock_sender: mock_tx,
                block_tx_hashes_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        let setup = CommonSetup::new(None).await?;

        // Create channels for communication
        let (engine_tx, engine_rx) = channel::unbounded();
        let (mock_tx, mock_rx) = channel::unbounded();

        // Spawn the engine task
        let engine_handle = setup.spawn_engine(engine_rx).await;

        // Create mock transport with the channels
        let transport =
            MockTransport::with_receiver(engine_tx, mock_rx, setup.state_results.clone());

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
            setup.underlying_db,
            setup.sources.clone(),
            setup.sequencer_http_mock,
            setup.eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            setup.state_results,
            setup.default_account,
            None,
            setup.list_of_sources,
            LocalInstanceMockDriver {
                mock_sender: mock_tx,
                block_tx_hashes_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    async fn new_block(
        &mut self,
        block_number: u64,
        selected_iteration_id: u64,
        n_transactions: u64,
    ) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance finalizing block: {:?} with selected_iteration_id: {}", block_number, selected_iteration_id);
        let (n_transactions, last_tx_hash) = self.next_block_metadata(selected_iteration_id);
        let block_env = BlockEnv {
            number: block_number,
            gas_limit: 50_000_000, // Set higher gas limit for assertions
            ..Default::default()
        };

        self.block_tx_hashes_by_iteration.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        let commit_head = CommitHead::new(
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
        );

        self.mock_sender
            .send(TxQueueContents::CommitHead(commit_head, Span::current()))
            .map_err(|e| format!("Failed to send commit head: {e}"))?;

        info!(target: "test_transport", "Successfully sent  to mock_sender");
        Ok(())
    }

    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String> {
        let iteration_id = tx_execution_id.iteration_id;
        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

        let prev_tx_hash = self.get_last_tx_hash(iteration_id);

        info!(target: "test_transport", "LocalInstance sending transaction: {:?}", tx_execution_id);
        let queue_tx = QueueTransaction {
            tx_execution_id,
            tx_env,
            prev_tx_hash,
        };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx, Span::current()))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }

    async fn new_iteration(
        &mut self,
        iteration_id: u64,
        block_env: BlockEnv,
    ) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending new iteration: {}", iteration_id);
        self.block_tx_hashes_by_iteration
            .insert(iteration_id, Vec::new());

        let new_iteration = NewIteration::new(iteration_id, block_env);
        self.mock_sender
            .send(TxQueueContents::NewIteration(
                new_iteration,
                Span::current(),
            ))
            .map_err(|e| format!("Failed to send new iteration: {e}"))
    }

    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending reorg for: {:?}", tx_execution_id);
        let tracked_hash: TxHash = tx_execution_id.tx_hash;
        let iteration_id = tx_execution_id.iteration_id;

        if let Some(hashes) = self.block_tx_hashes_by_iteration.get_mut(&iteration_id)
            && let Some(last_hash) = hashes.last()
        {
            if last_hash == &tracked_hash {
                hashes.pop();
            } else {
                debug!(
                    target: "test_transport",
                    "Reorg hash {:?} does not match last tracked transaction {:?} in iteration {}",
                    tracked_hash,
                    last_hash,
                    iteration_id
                );
            }
        }

        self.mock_sender
            .send(TxQueueContents::Reorg(tx_execution_id, Span::current()))
            .map_err(|e| format!("Failed to send reorg: {e}"))
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
    block_tx_hashes_by_iteration: HashMap<u64, Vec<TxHash>>,
    override_n_transactions: Option<u64>,
    /// We use Option<Option<TxHash>> to distinguish: None (use default), Some(None) (force no hash), Some(Some(hash)) (force specific hash)
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceHttpDriver {
    fn next_block_metadata(&self, selected_iteration_id: u64) -> (u64, Option<TxHash>) {
        let iteration_hashes: &[TxHash] = self
            .block_tx_hashes_by_iteration
            .get(&selected_iteration_id)
            .map_or(&[], Vec::as_slice);

        let n_transactions = self
            .override_n_transactions
            .unwrap_or(iteration_hashes.len() as u64);

        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        (n_transactions, last_tx_hash)
    }

    // FIXME: Avoid code repetition
    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            let iteration_hashes: &[TxHash] = self
                .block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(&[], |v| v.as_slice());
            iteration_hashes.last().copied()
        }
    }

    fn block_env_to_json(blockenv: &BlockEnv) -> serde_json::Value {
        json!({
            "number": blockenv.number,
            "beneficiary": blockenv.beneficiary.to_string(),
            "timestamp": blockenv.timestamp,
            "gas_limit": blockenv.gas_limit,
            "basefee": blockenv.basefee,
            "difficulty": format!("0x{:x}", blockenv.difficulty),
            "prevrandao": blockenv.prevrandao.map(|h| h.to_string()),
            "blob_excess_gas_and_price": blockenv.blob_excess_gas_and_price.as_ref().map(|blob| json!({
                "excess_blob_gas": blob.excess_blob_gas,
                "blob_gasprice": blob.blob_gasprice
            })),
        })
    }

    async fn submit_json_request(&self, request: &serde_json::Value) -> Result<(), String> {
        let mut attempts = 0;
        let mut last_error = String::new();

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self
                .client
                .post(format!("http://{}/tx", self.address))
                .header("content-type", "application/json")
                .json(request)
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

    async fn create(
        assertion_store: Option<AssertionStore>,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceHttpDriver", "Creating LocalInstance with HttpTransport");

        let setup = CommonSetup::new(assertion_store).await?;

        let (engine_tx, engine_rx) = channel::unbounded();
        let engine_handle = setup.spawn_engine(engine_rx).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        drop(listener);

        let config = HttpTransportConfig { bind_addr: address };
        let transport = HttpTransport::new(config, engine_tx, setup.state_results.clone()).unwrap();

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
            setup.underlying_db,
            setup.sources.clone(),
            setup.sequencer_http_mock,
            setup.eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            setup.state_results,
            setup.default_account,
            Some(&address),
            setup.list_of_sources,
            LocalInstanceHttpDriver {
                client: reqwest::Client::new(),
                address,
                block_tx_hashes_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    /// Same as `new()`, but takes in an `AssertionStore` as an argument
    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store)).await
    }
}

impl TestTransport for LocalInstanceHttpDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        Self::create(None).await
    }

    async fn new_block(
        &mut self,
        block_number: u64,
        selected_iteration_id: u64,
        n_transactions: u64,
    ) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance finalizing block: {:?} with selected_iteration_id: {}", block_number, selected_iteration_id);

        let (n_transactions, last_tx_hash) = self.next_block_metadata(selected_iteration_id);
        let last_tx_hash = last_tx_hash.map(|hash| hash.to_string());

        // jsonrpc request for sending commit head + block env events to the sidecar
        let request = json!({
          "id": 1,
          "jsonrpc": "2.0",
          "method": "sendEvents",
          "params": {
                "events": [
                    {
                        "commit_head": {
                            "last_tx_hash": last_tx_hash,
                            "n_transactions": n_transactions,
                            "block_number": block_number,
                            "selected_iteration_id": selected_iteration_id
                        }
                    },
                ]
          }
        });

        self.block_tx_hashes_by_iteration.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        self.submit_json_request(&request).await
    }

    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String> {
        debug!(target: "LocalInstanceHttpDriver", "Sending transaction: {:?}", tx_execution_id);

        let iteration_id = tx_execution_id.iteration_id;
        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

        let prev_tx_hash = self.get_last_tx_hash(iteration_id);

        let transaction = Transaction {
            tx_execution_id,
            tx_env,
            prev_tx_hash,
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

    async fn new_iteration(
        &mut self,
        iteration_id: u64,
        block_env: BlockEnv,
    ) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending new iteration: {} with block {}", iteration_id, block_env.number);
        self.block_tx_hashes_by_iteration
            .insert(iteration_id, Vec::new());

        let block_env_json = Self::block_env_to_json(&block_env);
        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "sendEvents",
            "params": {
                "events": [
                    {
                        "new_iteration": {
                            "iteration_id": iteration_id,
                            "block_env": block_env_json
                        }
                    }
                ]
            }
        });

        self.submit_json_request(&request).await
    }

    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending reorg for: {:?}", tx_execution_id);

        let tracked_hash: TxHash = tx_execution_id.tx_hash;
        let iteration_id = tx_execution_id.iteration_id;

        if let Some(hashes) = self.block_tx_hashes_by_iteration.get_mut(&iteration_id)
            && let Some(last_hash) = hashes.last()
        {
            if last_hash == &tracked_hash {
                hashes.pop();
            } else {
                debug!(
                    target: "LocalInstanceHttpDriver",
                    "Reorg hash {:?} does not match last tracked transaction {:?} in iteration {}",
                    tracked_hash,
                    last_hash,
                    iteration_id
                );
            }
        }

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": serde_json::to_value(tx_execution_id).unwrap(),
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
    /// Ordered hashes for transactions sent per iteration since the last block announcement
    block_tx_hashes_by_iteration: HashMap<u64, Vec<TxHash>>,
    /// Explicit override for the next `n_transactions` value
    override_n_transactions: Option<u64>,
    /// Explicit override for the next `last_tx_hash` value
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceGrpcDriver {
    fn next_block_metadata(&self, selected_iteration_id: u64) -> (u64, Option<TxHash>) {
        let iteration_hashes: &[TxHash] = self
            .block_tx_hashes_by_iteration
            .get(&selected_iteration_id)
            .map_or(&[], |v| v.as_slice());

        let n_transactions = self
            .override_n_transactions
            .unwrap_or(iteration_hashes.len() as u64);

        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        (n_transactions, last_tx_hash)
    }

    // FIXME: Avoid code repetition
    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            let iteration_hashes: &[TxHash] = self
                .block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(&[], |v| v.as_slice());
            iteration_hashes.last().copied()
        }
    }

    async fn create(
        assertion_store: Option<AssertionStore>,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceGrpcDriver", "Creating LocalInstance with GrpcTransport");

        let setup = CommonSetup::new(assertion_store).await?;

        let (engine_tx, engine_rx) = channel::unbounded();
        let engine_handle = setup.spawn_engine(engine_rx).await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        drop(listener);

        let config = GrpcTransportConfig { bind_addr: address };
        let transport = GrpcTransport::new(config, engine_tx, setup.state_results.clone()).unwrap();

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
            setup.underlying_db,
            setup.sources.clone(),
            setup.sequencer_http_mock,
            setup.eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            0,
            setup.state_results,
            setup.default_account,
            Some(&address),
            setup.list_of_sources,
            LocalInstanceGrpcDriver {
                client,
                block_tx_hashes_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

    /// Same as `new()`, but takes in an `AssertionStore` as an argument
    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store)).await
    }
}

impl TestTransport for LocalInstanceGrpcDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        Self::create(None).await
    }

    async fn new_block(
        &mut self,
        block_number: u64,
        selected_iteration_id: u64,
        n_transactions: u64,
    ) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance finalizing block: {:?} with selected_iteration_id: {}", block_number, selected_iteration_id);

        let (n_transactions, last_tx_hash) = self.next_block_metadata(selected_iteration_id);

        let commit_head = crate::transport::grpc::pb::CommitHead {
            last_tx_hash: last_tx_hash.map(|h| h.to_string()).unwrap_or_default(),
            n_transactions,
            selected_iteration_id: Some(selected_iteration_id),
            block_number,
        };

        let request = crate::transport::grpc::pb::SendEvents {
            events: vec![crate::transport::grpc::pb::send_events::Event {
                event: Some(
                    crate::transport::grpc::pb::send_events::event::Event::CommitHead(commit_head),
                ),
            }],
        };

        self.block_tx_hashes_by_iteration.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        let mut attempts = 0;
        let mut last_error = String::new();

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self.client.send_events(request.clone()).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    if !ack.accepted {
                        return Err(format!("Events request rejected: {}", ack.message));
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

    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String> {
        debug!(target: "LocalInstanceGrpcDriver", "Sending transaction: {:?}", tx_execution_id);

        if tx_env.gas_limit == 0 {
            return Err("Gas limit cannot be zero".to_string());
        }

        let iteration_id = tx_execution_id.iteration_id;
        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

        let transaction = GrpcTransaction {
            tx_execution_id: Some(GrpcTxExecutionId {
                block_number: tx_execution_id.block_number,
                iteration_id: tx_execution_id.iteration_id,
                tx_hash: format!("{:#x}", tx_execution_id.tx_hash),
                index: tx_execution_id.index,
            }),
            tx_env: Some(GrpcTransactionEnv {
                tx_type: 0, // Default to legacy transaction type
                caller: tx_env.caller.to_string(),
                gas_limit: tx_env.gas_limit,
                gas_price: tx_env.gas_price.to_string(),
                kind: match tx_env.kind {
                    TxKind::Call(addr) => addr.to_string(),
                    TxKind::Create => "0x".to_string(), // Must be "0x" for create, not empty string
                },
                value: tx_env.value.to_string(),
                data: format!("0x{}", alloy::hex::encode(&tx_env.data)),
                nonce: tx_env.nonce,
                chain_id: tx_env.chain_id,
                access_list: tx_env
                    .access_list
                    .0
                    .iter()
                    .map(|item| {
                        crate::transport::grpc::pb::AccessListItem {
                            address: item.address.to_string(),
                            storage_keys: item
                                .storage_keys
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                        }
                    })
                    .collect(),
                authorization_list: tx_env
                    .authorization_list
                    .iter()
                    .filter_map(|auth| {
                        match auth {
                            alloy::signers::Either::Left(signed_auth) => {
                                let inner = signed_auth.inner();
                                Some(crate::transport::grpc::pb::Authorization {
                                    chain_id: inner.chain_id.to_string(),
                                    address: inner.address.to_string(),
                                    nonce: inner.nonce,
                                    y_parity: signed_auth.y_parity().to_string(),
                                    r: signed_auth.r().to_string(),
                                    s: signed_auth.s().to_string(),
                                })
                            }
                            alloy::signers::Either::Right(_recovered_auth) => {
                                // Skip RecoveredAuthorization for now since we can't easily access its fields
                                // In a real implementation, you'd need to handle this case properly
                                None
                            }
                        }
                    })
                    .collect(),
                blob_hashes: tx_env.blob_hashes.iter().map(ToString::to_string).collect(),
                gas_priority_fee: tx_env.gas_priority_fee.map(|fee| fee.to_string()),
                max_fee_per_blob_gas: tx_env.max_fee_per_blob_gas.to_string(),
            }),
            prev_tx_hash: self
                .get_last_tx_hash(iteration_id)
                .map(|h| format!("{h:#x}")),
        };

        let request = SendTransactionsRequest {
            transactions: vec![transaction],
        };

        debug!(target: "LocalInstanceGrpcDriver", "Sending gRPC transaction request for hash: {}", tx_execution_id.tx_hash);

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

    async fn new_iteration(
        &mut self,
        iteration_id: u64,
        block_env: BlockEnv,
    ) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending new iteration: {} with block {}", iteration_id, block_env.number);
        self.block_tx_hashes_by_iteration
            .insert(iteration_id, Vec::new());

        let block_env_pb = crate::transport::grpc::pb::BlockEnv {
            number: block_env.number,
            beneficiary: block_env.beneficiary.to_string(),
            timestamp: block_env.timestamp,
            gas_limit: block_env.gas_limit,
            basefee: block_env.basefee,
            difficulty: format!("0x{:x}", block_env.difficulty),
            prevrandao: block_env.prevrandao.map(|h| h.to_string()),
            blob_excess_gas_and_price: block_env.blob_excess_gas_and_price.map(|blob| {
                crate::transport::grpc::pb::BlobExcessGasAndPrice {
                    excess_blob_gas: blob.excess_blob_gas,
                    blob_gasprice: blob.blob_gasprice.to_string(),
                }
            }),
        };

        let new_iteration = crate::transport::grpc::pb::NewIteration {
            iteration_id,
            block_env: Some(block_env_pb),
        };

        let request = crate::transport::grpc::pb::SendEvents {
            events: vec![crate::transport::grpc::pb::send_events::Event {
                event: Some(
                    crate::transport::grpc::pb::send_events::event::Event::NewIteration(
                        new_iteration,
                    ),
                ),
            }],
        };

        let mut attempts = 0;
        let mut last_error = String::new();

        while attempts < MAX_HTTP_RETRY_ATTEMPTS {
            attempts += 1;

            match self.client.send_events(request.clone()).await {
                Ok(response) => {
                    let ack = response.into_inner();
                    if !ack.accepted {
                        return Err(format!("Events request rejected: {}", ack.message));
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

    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending reorg for: {:?}", tx_execution_id.tx_hash);

        let tracked_hash: TxHash = tx_execution_id.tx_hash;
        let iteration_id = tx_execution_id.iteration_id;

        if let Some(hashes) = self.block_tx_hashes_by_iteration.get_mut(&iteration_id)
            && let Some(last_hash) = hashes.last()
        {
            if last_hash == &tracked_hash {
                hashes.pop();
            } else {
                debug!(
                    target: "LocalInstanceGrpcDriver",
                    "Reorg hash {:?} does not match last tracked transaction {:?} in iteration {}",
                    tracked_hash,
                    last_hash,
                    iteration_id
                );
            }
        }

        let request = ReorgRequest {
            tx_execution_id: Some(GrpcTxExecutionId {
                block_number: tx_execution_id.block_number,
                iteration_id: tx_execution_id.iteration_id,
                tx_hash: format!("{:#x}", tx_execution_id.tx_hash),
                index: tx_execution_id.index,
            }),
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
