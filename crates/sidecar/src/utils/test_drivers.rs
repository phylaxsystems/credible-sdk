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
    },
    engine::queue::{
        CommitHead,
        NewIteration,
        QueueTransaction,
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    event_sequencing::EventSequencing,
    execution_ids::TxExecutionId,
    transactions_state::TransactionResultEvent,
    transport::{
        Transport,
        grpc::{
            GrpcTransport,
            config::GrpcTransportConfig,
            pb::{
                self,
                Event,
                TxExecutionId as GrpcTxExecutionId,
                event::Event as EventVariant,
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
use futures::StreamExt;
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use revm::{
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
use tokio::{
    sync::mpsc,
    time::sleep,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{
    Span,
    debug,
    error,
    info,
    warn,
};

mod grpc_encode {
    use super::*;

    #[inline]
    pub fn address(addr: Address) -> Vec<u8> {
        addr.as_slice().to_vec()
    }

    #[inline]
    pub fn b256(hash: B256) -> Vec<u8> {
        hash.as_slice().to_vec()
    }

    #[inline]
    pub fn u256_be(val: U256) -> Vec<u8> {
        val.to_be_bytes::<32>().to_vec()
    }

    #[inline]
    pub fn u128_be(val: u128) -> Vec<u8> {
        val.to_be_bytes().to_vec()
    }
}

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

const MAX_HTTP_RETRY_ATTEMPTS: usize = 10;
const HTTP_RETRY_DELAY_MS: u64 = 100;

/// Common initialization for all driver types
struct CommonSetup {
    underlying_db: Arc<CacheDB<Arc<Sources>>>,
    sources: Arc<Sources>,
    eth_rpc_source_http_mock: DualProtocolMockServer,
    fallback_eth_rpc_source_http_mock: DualProtocolMockServer,
    assertion_store: Arc<AssertionStore>,
    state_results: Arc<crate::TransactionsState>,
    default_account: Address,
    list_of_sources: Vec<Arc<dyn Source>>,
    event_id_buffer_capacity: usize,
}

impl CommonSetup {
    /// Initialize common database, cache, and mocks for test drivers.
    ///
    /// If `result_event_sender` is provided, the `TransactionsState` will broadcast
    /// transaction results to it (used for gRPC `SubscribeResults` streaming).
    async fn new(
        assertion_store: Option<AssertionStore>,
        result_event_sender: Option<flume::Sender<TransactionResultEvent>>,
    ) -> Result<Self, String> {
        let eth_rpc_source_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create eth rpc source mock");
        let fallback_eth_rpc_source_http_mock = DualProtocolMockServer::new()
            .await
            .expect("Failed to create eth rpc source mock");
        let eth_rpc_source_db: Arc<dyn Source> = EthRpcSource::try_build(
            eth_rpc_source_http_mock.ws_url(),
            eth_rpc_source_http_mock.http_url(),
        )
        .await
        .expect("Failed to create eth rpc source mock");
        let fallback_eth_rpc_source_db: Arc<dyn Source> = EthRpcSource::try_build(
            fallback_eth_rpc_source_http_mock.ws_url(),
            fallback_eth_rpc_source_http_mock.http_url(),
        )
        .await
        .expect("Failed to create eth rpc source mock");
        let sources = vec![eth_rpc_source_db, fallback_eth_rpc_source_db];
        let cache = Arc::new(Sources::new(sources.clone(), 10));
        let mut underlying_db = revm::database::CacheDB::new(cache.clone());
        let default_account = populate_test_database(&mut underlying_db);

        let underlying_db = Arc::new(underlying_db);

        let assertion_store = match assertion_store {
            Some(store) => Arc::new(store),
            None => setup_assertion_store()?,
        };

        // Create state with or without result sender
        let state_results = match result_event_sender {
            Some(sender) => crate::TransactionsState::with_result_sender(sender),
            None => crate::TransactionsState::new(),
        };

        Ok(CommonSetup {
            underlying_db,
            sources: cache,
            eth_rpc_source_http_mock,
            fallback_eth_rpc_source_http_mock,
            assertion_store,
            state_results,
            default_account,
            list_of_sources: sources,
            event_id_buffer_capacity: 100,
        })
    }

    /// Spawn the engine task and event sequencing with the provided receivers
    async fn spawn_engine_with_sequencing(
        &self,
        transport_rx: TransactionQueueReceiver,
    ) -> (tokio::task::JoinHandle<()>, tokio::task::JoinHandle<()>) {
        let (event_sequencing_tx_sender, core_engine_tx_receiver) = flume::unbounded();

        let state = OverlayDb::new(Some(self.underlying_db.clone()));
        let assertion_executor =
            AssertionExecutor::new(ExecutorConfig::default(), (*self.assertion_store).clone());

        let mut engine = CoreEngine::new(
            state,
            self.sources.clone(),
            core_engine_tx_receiver,
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

        let engine_handle = tokio::spawn(async move {
            info!(target: "test_driver", "Engine task started, waiting for items...");
            let result = engine.run().await;
            match result {
                Ok(()) => info!(target: "test_driver", "Engine run() completed successfully"),
                Err(e) => error!(target: "test_driver", "Engine run() failed: {:?}", e),
            }
        });

        let mut event_sequencing = EventSequencing::new(transport_rx, event_sequencing_tx_sender);

        let sequencing_handle = tokio::spawn(async move {
            info!(target: "test_driver", "Event sequencing task started");
            let result = event_sequencing.run().await;
            match result {
                Ok(()) => info!(target: "test_driver", "Event sequencing completed successfully"),
                Err(e) => error!(target: "test_driver", "Event sequencing failed: {:?}", e),
            }
        });

        (engine_handle, sequencing_handle)
    }
}

pub struct LocalInstanceMockDriver {
    mock_sender: TransactionQueueSender,
    block_tx_hashes_by_iteration: HashMap<u64, Vec<TxHash>>,
    override_n_transactions: Option<u64>,
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceMockDriver {
    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        let setup = CommonSetup::new(Some(assertion_store), None).await?;

        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();
        let (mock_tx, mock_rx) = flume::unbounded();

        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver)
            .await;

        let transport =
            MockTransport::with_receiver(transport_tx_sender, mock_rx, setup.state_results.clone());

        let transport_handle = tokio::spawn(async move {
            info!(target: "test_transport", "Transport task started");
            let result = transport.run().await;
            match result {
                Ok(()) => info!(target: "test_transport", "Transport run() completed successfully"),
                Err(e) => warn!(target: "test_transport", "Transport stopped with error: {}", e),
            }
        });

        Ok(LocalInstance::new_internal(
            setup.underlying_db,
            setup.sources.clone(),
            setup.eth_rpc_source_http_mock,
            setup.fallback_eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            U256::from(1),
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

        let setup = CommonSetup::new(None, None).await?;

        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();
        let (mock_tx, mock_rx) = flume::unbounded();

        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver)
            .await;

        let transport =
            MockTransport::with_receiver(transport_tx_sender, mock_rx, setup.state_results.clone());

        let transport_handle = tokio::spawn(async move {
            info!(target: "test_transport", "Transport task started");
            let result = transport.run().await;
            match result {
                Ok(()) => info!(target: "test_transport", "Transport run() completed successfully"),
                Err(e) => warn!(target: "test_transport", "Transport stopped with error: {}", e),
            }
        });

        Ok(LocalInstance::new_internal(
            setup.underlying_db,
            setup.sources.clone(),
            setup.eth_rpc_source_http_mock,
            setup.fallback_eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            U256::from(1),
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
        block_number: U256,
        selected_iteration_id: u64,
        _n_transactions: u64,
    ) -> Result<(), String> {
        let n_transactions = self.get_tx_count(selected_iteration_id);
        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        // Use random hashes for EIP-2935 and EIP-4788
        let block_hash = Some(B256::random());
        let timestamp = U256::from(1234567890);
        let mut parent_beacon_block_root = Some(B256::random());

        if block_number == U256::from(0) {
            parent_beacon_block_root = Some(B256::ZERO);
        }

        let commit_head = CommitHead::new(
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            parent_beacon_block_root,
            timestamp,
        );

        self.send_commit_head(commit_head).await
    }

    async fn send_commit_head(&mut self, commit_head: CommitHead) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending commit head for block: {:?}", commit_head.block_number);

        self.block_tx_hashes_by_iteration.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        self.mock_sender
            .send(TxQueueContents::CommitHead(commit_head, Span::current()))
            .map_err(|e| format!("Failed to send commit head: {e}"))?;

        info!(target: "test_transport", "Successfully sent commit head to mock_sender");
        Ok(())
    }

    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String> {
        let iteration_id = tx_execution_id.iteration_id;
        let prev_tx_hash = self.get_last_tx_hash(iteration_id);

        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

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
                    tracked_hash, last_hash, iteration_id
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

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .and_then(|v| v.last().copied())
        }
    }

    fn get_tx_count(&self, iteration_id: u64) -> u64 {
        self.override_n_transactions.unwrap_or_else(|| {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(0, |v| v.len() as u64)
        })
    }
}

#[derive(Debug)]
pub struct LocalInstanceHttpDriver {
    client: reqwest::Client,
    address: SocketAddr,
    block_tx_hashes_by_iteration: HashMap<u64, Vec<TxHash>>,
    override_n_transactions: Option<u64>,
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceHttpDriver {
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
                        tokio::time::sleep(Duration::from_millis(HTTP_RETRY_DELAY_MS)).await;
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

        let setup = CommonSetup::new(assertion_store, None).await?;

        // Create channels for transport -> event_sequencing -> engine
        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();

        // Spawn engine and event sequencing
        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver)
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        drop(listener);

        let config = HttpTransportConfig { bind_addr: address };
        let transport = HttpTransport::new(
            config,
            transport_tx_sender,
            setup.state_results.clone(),
            setup.event_id_buffer_capacity,
        )
        .unwrap();

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
            setup.eth_rpc_source_http_mock,
            setup.fallback_eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            U256::from(1),
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
        block_number: U256,
        selected_iteration_id: u64,
        _n_transactions: u64,
    ) -> Result<(), String> {
        let n_transactions = self.get_tx_count(selected_iteration_id);
        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        // Use random hashes for EIP-2935 and EIP-4788
        let block_hash = Some(B256::random());
        let mut parent_beacon_block_root = Some(B256::random());

        if block_number == U256::from(0) {
            parent_beacon_block_root = Some(B256::ZERO);
        }

        let commit_head = CommitHead::new(
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            parent_beacon_block_root,
            U256::from(1234567890),
        );

        self.send_commit_head(commit_head).await
    }

    async fn send_commit_head(&mut self, commit_head: CommitHead) -> Result<(), String> {
        info!(target: "LocalInstanceHttpDriver", "LocalInstance sending commit head for block: {:?}", commit_head.block_number);

        let last_tx_hash = commit_head.last_tx_hash.map(|hash| hash.to_string());

        // Build commit_head JSON with optional parent hashes
        let mut commit_head_json = json!({
            "last_tx_hash": last_tx_hash,
            "n_transactions": commit_head.n_transactions,
            "block_number": commit_head.block_number,
            "selected_iteration_id": commit_head.selected_iteration_id,
            "timestamp": commit_head.timestamp
        });

        // Add block_hash if provided (EIP-2935)
        if let Some(hash) = commit_head.block_hash {
            commit_head_json["block_hash"] = json!(hash.to_string());
        }

        // Add parent_beacon_block_root if provided (EIP-4788)
        if let Some(mut root) = commit_head.parent_beacon_block_root {
            if commit_head.block_number == U256::from(0) {
                root = B256::ZERO;
            }
            commit_head_json["parent_beacon_block_root"] = json!(root.to_string());
        }

        // jsonrpc request for sending commit head event to the sidecar
        let request = json!({
          "id": 1,
          "jsonrpc": "2.0",
          "method": "sendEvents",
          "params": {
                "events": [
                    {
                        "commit_head": commit_head_json
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
        let prev_tx_hash = self.get_last_tx_hash(iteration_id);
        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

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

        self.submit_json_request(&request).await
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
                    tracked_hash, last_hash, iteration_id
                );
            }
        }

        let request = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "reorg",
            "params": serde_json::to_value(tx_execution_id).unwrap(),
        });

        self.submit_json_request(&request).await
    }

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .and_then(|v| v.last().copied())
        }
    }

    fn get_tx_count(&self, iteration_id: u64) -> u64 {
        self.override_n_transactions.unwrap_or_else(|| {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(0, |v| v.len() as u64)
        })
    }
}

pub struct LocalInstanceGrpcDriver {
    event_sender: mpsc::Sender<Event>,
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
    fn build_pb_block_env(block_env: &BlockEnv) -> pb::BlockEnv {
        pb::BlockEnv {
            number: block_env.number.to_be_bytes::<32>().to_vec(),
            beneficiary: grpc_encode::address(block_env.beneficiary),
            timestamp: block_env.timestamp.to_be_bytes::<32>().to_vec(),
            gas_limit: block_env.gas_limit,
            basefee: block_env.basefee,
            difficulty: grpc_encode::u256_be(block_env.difficulty),
            prevrandao: block_env.prevrandao.map(grpc_encode::b256),
            blob_excess_gas_and_price: block_env.blob_excess_gas_and_price.as_ref().map(|blob| {
                pb::BlobExcessGasAndPrice {
                    excess_blob_gas: blob.excess_blob_gas,
                    blob_gasprice: grpc_encode::u128_be(blob.blob_gasprice),
                }
            }),
        }
    }

    fn build_pb_tx_execution_id(tx_execution_id: &TxExecutionId) -> GrpcTxExecutionId {
        GrpcTxExecutionId {
            block_number: tx_execution_id.block_number.to_be_bytes::<32>().to_vec(),
            iteration_id: tx_execution_id.iteration_id,
            tx_hash: grpc_encode::b256(tx_execution_id.tx_hash),
            index: tx_execution_id.index,
        }
    }

    fn build_pb_transaction(
        tx_execution_id: &TxExecutionId,
        tx_env: &TxEnv,
        prev_tx_hash: Option<TxHash>,
    ) -> pb::Transaction {
        pb::Transaction {
            tx_execution_id: Some(Self::build_pb_tx_execution_id(tx_execution_id)),
            tx_env: Some(pb::TransactionEnv {
                tx_type: tx_env.tx_type.into(),
                caller: grpc_encode::address(tx_env.caller),
                gas_limit: tx_env.gas_limit,
                gas_price: grpc_encode::u128_be(tx_env.gas_price),
                transact_to: match tx_env.kind {
                    TxKind::Call(addr) => grpc_encode::address(addr),
                    TxKind::Create => vec![],
                },
                value: grpc_encode::u256_be(tx_env.value),
                data: tx_env.data.to_vec(),
                nonce: tx_env.nonce,
                chain_id: tx_env.chain_id,
                access_list: tx_env
                    .access_list
                    .0
                    .iter()
                    .map(|item| {
                        pb::AccessListItem {
                            address: grpc_encode::address(item.address),
                            storage_keys: item
                                .storage_keys
                                .iter()
                                .map(|k| grpc_encode::b256(*k))
                                .collect(),
                        }
                    })
                    .collect(),
                gas_priority_fee: tx_env.gas_priority_fee.map(grpc_encode::u128_be),
                blob_hashes: tx_env
                    .blob_hashes
                    .iter()
                    .map(|h| grpc_encode::b256(*h))
                    .collect(),
                max_fee_per_blob_gas: grpc_encode::u128_be(tx_env.max_fee_per_blob_gas),
                authorization_list: tx_env
                    .authorization_list
                    .iter()
                    .filter_map(|auth| {
                        match auth {
                            alloy::signers::Either::Left(signed_auth) => {
                                let inner = signed_auth.inner();
                                Some(pb::Authorization {
                                    chain_id: grpc_encode::u256_be(inner.chain_id),
                                    address: grpc_encode::address(inner.address),
                                    nonce: inner.nonce,
                                    y_parity: signed_auth.y_parity().into(),
                                    r: grpc_encode::u256_be(signed_auth.r()),
                                    s: grpc_encode::u256_be(signed_auth.s()),
                                })
                            }
                            alloy::signers::Either::Right(_) => None,
                        }
                    })
                    .collect(),
            }),
            prev_tx_hash: prev_tx_hash.map(grpc_encode::b256),
        }
    }

    async fn send_event(&self, event: Event) -> Result<(), String> {
        self.event_sender
            .send(event)
            .await
            .map_err(|e| format!("Failed to send event to stream: {e}"))
    }

    async fn create(
        assertion_store: Option<AssertionStore>,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceGrpcDriver", "Creating LocalInstance with streaming GrpcTransport");

        // Create result event channel for SubscribeResults streaming
        let (result_event_tx, result_event_rx) = flume::bounded(4096);

        // Pass the sender to CommonSetup so state_results broadcasts results
        let setup = CommonSetup::new(assertion_store, Some(result_event_tx)).await?;

        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();

        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver)
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        drop(listener);

        let config = GrpcTransportConfig { bind_addr: address };

        // Use with_result_receiver to enable SubscribeResults streaming
        let transport = GrpcTransport::with_result_receiver(
            &config,
            transport_tx_sender,
            setup.state_results.clone(),
            result_event_rx,
            setup.event_id_buffer_capacity,
        )
        .map_err(|e| format!("Failed to create gRPC transport: {e}"))?;

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

        // Connect gRPC client with retries
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
                    tokio::time::sleep(Duration::from_millis(HTTP_RETRY_DELAY_MS)).await;
                }
            }
        };

        let (event_tx, event_rx) = mpsc::channel(256);
        let stream = ReceiverStream::new(event_rx);

        let mut client_clone = client.clone();
        let mut response_stream = client_clone
            .stream_events(stream)
            .await
            .map_err(|e| format!("Failed to start event stream: {e}"))?
            .into_inner();

        tokio::spawn(async move {
            while let Some(result) = response_stream.next().await {
                match result {
                    Ok(ack) => {
                        debug!(target: "LocalInstanceGrpcDriver", "Received ack: success={}, events_processed={}", ack.success, ack.events_processed);
                        if !ack.success {
                            error!(target: "LocalInstanceGrpcDriver", "Stream ack indicates failure: {}", ack.message);
                        }
                    }
                    Err(e) => {
                        warn!(target: "LocalInstanceGrpcDriver", "Stream error: {e}");
                        break;
                    }
                }
            }
        });

        Ok(LocalInstance::new_internal(
            setup.underlying_db,
            setup.sources.clone(),
            setup.eth_rpc_source_http_mock,
            setup.fallback_eth_rpc_source_http_mock,
            setup.assertion_store,
            Some(transport_handle),
            Some(engine_handle),
            U256::from(1),
            setup.state_results,
            setup.default_account,
            Some(&address),
            setup.list_of_sources,
            LocalInstanceGrpcDriver {
                event_sender: event_tx,
                client,
                block_tx_hashes_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
            },
        ))
    }

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
        block_number: U256,
        selected_iteration_id: u64,
        _n_transactions: u64,
    ) -> Result<(), String> {
        let n_transactions = self.get_tx_count(selected_iteration_id);
        let last_tx_hash = self.get_last_tx_hash(selected_iteration_id);

        // Use random hashes for EIP-2935 and EIP-4788
        let block_hash = Some(B256::random());
        let mut parent_beacon_block_root = Some(B256::random());

        if block_number == U256::from(0) {
            parent_beacon_block_root = Some(B256::ZERO);
        }

        let commit_head = CommitHead::new(
            block_number,
            selected_iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            parent_beacon_block_root,
            U256::from(1234567890),
        );

        self.send_commit_head(commit_head).await
    }

    async fn send_commit_head(&mut self, commit_head: CommitHead) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending commit head for block: {:?}", commit_head.block_number);

        let parent_beacon_block_root = if commit_head.block_number == U256::from(0) {
            Some(B256::ZERO)
        } else {
            commit_head.parent_beacon_block_root
        };

        let pb_commit_head = pb::CommitHead {
            last_tx_hash: commit_head.last_tx_hash.map(grpc_encode::b256),
            n_transactions: commit_head.n_transactions,
            block_number: grpc_encode::u256_be(commit_head.block_number),
            selected_iteration_id: commit_head.selected_iteration_id,
            block_hash: commit_head.block_hash.map(grpc_encode::b256),
            parent_beacon_block_root: parent_beacon_block_root.map(grpc_encode::b256),
            timestamp: grpc_encode::u256_be(commit_head.timestamp),
        };

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::CommitHead(pb_commit_head)),
        };

        self.block_tx_hashes_by_iteration.clear();
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;

        self.send_event(event).await
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
        let prev_tx_hash = self.get_last_tx_hash(iteration_id);

        self.block_tx_hashes_by_iteration
            .entry(iteration_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

        let transaction = Self::build_pb_transaction(&tx_execution_id, &tx_env, prev_tx_hash);

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::Transaction(transaction)),
        };

        self.send_event(event).await
    }

    async fn new_iteration(
        &mut self,
        iteration_id: u64,
        block_env: BlockEnv,
    ) -> Result<(), String> {
        info!(target: "LocalInstanceGrpcDriver", "LocalInstance sending new iteration: {} with block {}", iteration_id, block_env.number);

        self.block_tx_hashes_by_iteration
            .insert(iteration_id, Vec::new());

        let new_iteration = pb::NewIteration {
            iteration_id,
            block_env: Some(Self::build_pb_block_env(&block_env)),
        };

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::NewIteration(new_iteration)),
        };

        self.send_event(event).await
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
                    tracked_hash, last_hash, iteration_id
                );
            }
        }

        let reorg_event = pb::ReorgEvent {
            tx_execution_id: Some(Self::build_pb_tx_execution_id(&tx_execution_id)),
        };

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::Reorg(reorg_event)),
        };

        self.send_event(event).await
    }

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            *value
        } else {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .and_then(|v| v.last().copied())
        }
    }

    fn get_tx_count(&self, iteration_id: u64) -> u64 {
        self.override_n_transactions.unwrap_or_else(|| {
            self.block_tx_hashes_by_iteration
                .get(&iteration_id)
                .map_or(0, |v| v.len() as u64)
        })
    }
}
