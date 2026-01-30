#![allow(clippy::too_many_lines)]
use super::{
    instance::{
        EngineThreadHandle,
        LocalInstance,
        TestTransport,
    },
    local_instance_db::LocalInstanceDb,
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
        ReorgRequest,
        TransactionQueueReceiver,
        TransactionQueueSender,
        TxQueueContents,
    },
    event_sequencing::{
        EventSequencing,
        EventSequencingError,
    },
    execution_ids::{
        BlockExecutionId,
        TxExecutionId,
    },
    transaction_observer::IncidentReportSender,
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
    sync::{
        Arc,
        atomic::AtomicBool,
    },
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::sleep,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tracing::{
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
    // NOTE: Must not collide with COUNTER_ADDRESS.
    let default_account = Address::from([0x03; 20]);
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
fn setup_assertion_store() -> Arc<AssertionStore> {
    let assertion_store = Arc::new(AssertionStore::new_ephemeral());

    // Insert counter assertion into store
    let assertion_bytecode = bytecode(SIMPLE_ASSERTION_COUNTER);
    assertion_store
        .insert(
            COUNTER_ADDRESS,
            AssertionState::new_test(&assertion_bytecode),
        )
        .unwrap();

    assertion_store
}

const MAX_HTTP_RETRY_ATTEMPTS: usize = 10;
const HTTP_RETRY_DELAY_MS: u64 = 100;

/// Common initialization for all driver types
struct CommonSetup {
    underlying_db: Arc<LocalInstanceDb<CacheDB<Arc<Sources>>>>,
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

        let underlying_db = Arc::new(LocalInstanceDb::new(underlying_db));

        let assertion_store = match assertion_store {
            Some(store) => Arc::new(store),
            None => setup_assertion_store(),
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
        incident_sender: Option<IncidentReportSender>,
    ) -> (
        EngineThreadHandle,
        std::thread::JoinHandle<Result<(), EventSequencingError>>,
    ) {
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
            incident_sender,
            #[cfg(feature = "cache_validation")]
            Some(&self.eth_rpc_source_http_mock.ws_url()),
        )
        .await;
        engine.set_canonical_db_for_tests(self.underlying_db.clone());

        let shutdown = Arc::new(AtomicBool::new(false));
        let (engine_handle, _engine_exit_rx) = engine
            .spawn(shutdown.clone())
            .expect("failed to spawn engine thread");
        let engine_handle = EngineThreadHandle::new(engine_handle, shutdown);

        let event_sequencing = EventSequencing::new(transport_rx, event_sequencing_tx_sender);
        let sequencing_shutdown = Arc::new(AtomicBool::new(false));
        let (sequencing_handle, _sequencing_exit_rx) = event_sequencing
            .spawn(sequencing_shutdown)
            .expect("failed to spawn event sequencing thread");

        (engine_handle, sequencing_handle)
    }
}

pub struct LocalInstanceMockDriver {
    mock_sender: TransactionQueueSender,
    /// Tracks tx hashes per (block, iteration). This must not be keyed only by `iteration_id`
    /// because tests can send txs for future blocks before their `NewIteration` arrives.
    block_tx_hashes: HashMap<BlockExecutionId, Vec<TxHash>>,
    /// Tracks the "current" block number per `iteration_id` for `LocalInstance` helpers that only
    /// pass `iteration_id` (e.g. `LocalInstance::send_block_with_hashes_internal`).
    current_block_by_iteration: HashMap<u64, U256>,
    override_n_transactions: Option<u64>,
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
    #[allow(clippy::option_option)]
    override_prev_tx_hash: Option<Option<TxHash>>,
}

impl LocalInstanceMockDriver {
    async fn create(
        assertion_store: Option<AssertionStore>,
        incident_sender: Option<IncidentReportSender>,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "test_transport", "Creating LocalInstance with MockTransport");

        let setup = CommonSetup::new(assertion_store, None).await?;

        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();
        let (mock_tx, mock_rx) = flume::unbounded();

        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver, incident_sender)
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
                block_tx_hashes: HashMap::new(),
                current_block_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
                override_prev_tx_hash: None,
            },
        ))
    }

    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store), None).await
    }

    pub async fn new_with_store_and_incident_sender(
        assertion_store: AssertionStore,
        incident_sender: IncidentReportSender,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store), Some(incident_sender)).await
    }

    pub async fn new_with_incident_sender(
        incident_sender: IncidentReportSender,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(None, Some(incident_sender)).await
    }
}

impl TestTransport for LocalInstanceMockDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        Self::create(None, None).await
    }

    async fn new_block(
        &mut self,
        block_number: U256,
        selected_iteration_id: u64,
        _n_transactions: u64,
    ) -> Result<(), String> {
        let block_id = BlockExecutionId {
            block_number,
            iteration_id: selected_iteration_id,
        };
        let n_transactions = self.override_n_transactions.unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .map_or(0, |v| v.len() as u64)
        });
        let last_tx_hash = self.override_last_tx_hash.unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .and_then(|v| v.last().copied())
        });

        // Use random hashes for EIP-2935 and EIP-4788
        let block_hash = B256::random();
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

        // Clear only the committed block's tracking; keep future blocks.
        self.block_tx_hashes
            .retain(|k, _| k.block_number != commit_head.block_number);
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;
        self.override_prev_tx_hash = None;

        self.mock_sender
            .send(TxQueueContents::CommitHead(commit_head))
            .map_err(|e| format!("Failed to send commit head: {e}"))?;

        info!(target: "test_transport", "Successfully sent commit head to mock_sender");
        Ok(())
    }

    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String> {
        let block_id = tx_execution_id.as_block_execution_id();
        let prev_tx_hash = self.override_prev_tx_hash.take().unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .and_then(|v| v.last().copied())
        });

        self.block_tx_hashes
            .entry(block_id)
            .or_default()
            .push(tx_execution_id.tx_hash);

        info!(target: "test_transport", "LocalInstance sending transaction: {:?}", tx_execution_id);
        let queue_tx = QueueTransaction {
            tx_execution_id,
            tx_env,
            prev_tx_hash,
        };
        self.mock_sender
            .send(TxQueueContents::Tx(queue_tx))
            .map_err(|e| format!("Failed to send transaction: {e}"))
    }

    async fn new_iteration(
        &mut self,
        iteration_id: u64,
        block_env: BlockEnv,
    ) -> Result<(), String> {
        info!(target: "test_transport", "LocalInstance sending new iteration: {}", iteration_id);
        self.current_block_by_iteration
            .insert(iteration_id, block_env.number);
        self.block_tx_hashes
            .entry(BlockExecutionId {
                block_number: block_env.number,
                iteration_id,
            })
            .or_default();

        let new_iteration =
            NewIteration::with_beacon_root(iteration_id, block_env, Some(B256::ZERO));
        self.mock_sender
            .send(TxQueueContents::NewIteration(new_iteration))
            .map_err(|e| format!("Failed to send new iteration: {e}"))
    }

    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        self.reorg_depth(tx_execution_id, vec![tx_execution_id.tx_hash])
            .await
    }

    async fn reorg_depth(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_hashes: Vec<TxHash>,
    ) -> Result<(), String> {
        info!(
            target: "test_transport",
            "LocalInstance sending reorg for: {:?} with depth {}",
            tx_execution_id, tx_hashes.len()
        );
        let block_id = tx_execution_id.as_block_execution_id();

        // Remove the reorged transactions from our local tracking
        if let Some(hashes) = self.block_tx_hashes.get_mut(&block_id) {
            for tx_hash in tx_hashes.iter().rev() {
                if hashes.last() == Some(tx_hash) {
                    hashes.pop();
                } else {
                    debug!(
                        target: "test_transport",
                        "Reorg hash {:?} does not match last tracked transaction {:?} in iteration {}",
                        tx_hash, hashes.last(), block_id.iteration_id
                    );
                    break;
                }
            }
        }

        self.mock_sender
            .send(TxQueueContents::Reorg(ReorgRequest {
                tx_execution_id,
                tx_hashes,
            }))
            .map_err(|e| format!("Failed to send reorg: {e}"))
    }

    fn set_n_transactions(&mut self, n_transactions: u64) {
        self.override_n_transactions = Some(n_transactions);
    }

    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_last_tx_hash = Some(tx_hash);
    }

    fn set_prev_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_prev_tx_hash = Some(tx_hash);
    }

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            return *value;
        }
        let block_number = self
            .current_block_by_iteration
            .get(&iteration_id)
            .copied()?;
        self.block_tx_hashes
            .get(&BlockExecutionId {
                block_number,
                iteration_id,
            })
            .and_then(|v| v.last().copied())
    }

    fn get_tx_count(&self, iteration_id: u64) -> u64 {
        self.override_n_transactions.unwrap_or_else(|| {
            let Some(block_number) = self.current_block_by_iteration.get(&iteration_id).copied()
            else {
                return 0;
            };
            self.block_tx_hashes
                .get(&BlockExecutionId {
                    block_number,
                    iteration_id,
                })
                .map_or(0, |v| v.len() as u64)
        })
    }
}

pub struct LocalInstanceGrpcDriver {
    event_sender: mpsc::Sender<Event>,
    /// gRPC client for the transport
    client: SidecarTransportClient<Channel>,
    /// Tracks tx hashes per (block, iteration). This must not be keyed only by `iteration_id`
    /// because tests can send txs for future blocks before their `NewIteration` arrives.
    block_tx_hashes: HashMap<BlockExecutionId, Vec<TxHash>>,
    /// Tracks the "current" block number per `iteration_id` for `LocalInstance` helpers that only
    /// pass `iteration_id` (e.g. `LocalInstance::send_block_with_hashes_internal`).
    current_block_by_iteration: HashMap<u64, U256>,
    /// Explicit override for the next `n_transactions` value
    override_n_transactions: Option<u64>,
    #[allow(clippy::option_option)]
    override_last_tx_hash: Option<Option<TxHash>>,
    #[allow(clippy::option_option)]
    override_prev_tx_hash: Option<Option<TxHash>>,
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
        use tokio::time::{
            Duration,
            timeout,
        };

        // Add timeout to prevent indefinite blocking when channel is full
        // This can happen when tests run in parallel and gRPC server is slow
        timeout(Duration::from_secs(10), self.event_sender.send(event))
            .await
            .map_err(|_| {
                "Timeout sending event to gRPC stream - server may be overloaded".to_string()
            })?
            .map_err(|e| format!("Failed to send event to stream: {e}"))
    }

    async fn create(
        assertion_store: Option<AssertionStore>,
        incident_sender: Option<IncidentReportSender>,
    ) -> Result<LocalInstance<Self>, String> {
        info!(target: "LocalInstanceGrpcDriver", "Creating LocalInstance with streaming GrpcTransport");

        // Create result event channel for SubscribeResults streaming
        let (result_event_tx, result_event_rx) = flume::bounded(4096);

        // Pass the sender to CommonSetup so state_results broadcasts results
        let setup = CommonSetup::new(assertion_store, Some(result_event_tx)).await?;

        let (transport_tx_sender, event_sequencing_tx_receiver) = flume::unbounded();

        let (engine_handle, _sequencing_handle) = setup
            .spawn_engine_with_sequencing(event_sequencing_tx_receiver, incident_sender)
            .await;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| format!("Failed to bind to available port: {e}"))?;
        let address = listener
            .local_addr()
            .map_err(|e| format!("Failed to get local address: {e}"))?;

        drop(listener);

        let config = GrpcTransportConfig {
            bind_addr: address,
            pending_receive_ttl: Duration::from_secs(5),
        };

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

        // Use larger buffer to prevent blocking when running tests in parallel
        // Tests can send bursts of events that need to be queued
        let (event_tx, event_rx) = mpsc::channel(2048);
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
                block_tx_hashes: HashMap::new(),
                current_block_by_iteration: HashMap::new(),
                override_n_transactions: None,
                override_last_tx_hash: None,
                override_prev_tx_hash: None,
            },
        ))
    }

    pub async fn new_with_store(
        assertion_store: AssertionStore,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store), None).await
    }

    pub async fn new_with_store_and_incident_sender(
        assertion_store: AssertionStore,
        incident_sender: IncidentReportSender,
    ) -> Result<LocalInstance<Self>, String> {
        Self::create(Some(assertion_store), Some(incident_sender)).await
    }
}

impl TestTransport for LocalInstanceGrpcDriver {
    async fn new() -> Result<LocalInstance<Self>, String> {
        Self::create(None, None).await
    }

    async fn new_block(
        &mut self,
        block_number: U256,
        selected_iteration_id: u64,
        _n_transactions: u64,
    ) -> Result<(), String> {
        let block_id = BlockExecutionId {
            block_number,
            iteration_id: selected_iteration_id,
        };
        let n_transactions = self.override_n_transactions.unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .map_or(0, |v| v.len() as u64)
        });
        let last_tx_hash = self.override_last_tx_hash.unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .and_then(|v| v.last().copied())
        });

        // Use random hashes for EIP-2935 and EIP-4788
        let block_hash = B256::random();
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
            block_hash: grpc_encode::b256(commit_head.block_hash),
            parent_beacon_block_root: parent_beacon_block_root.map(grpc_encode::b256),
            timestamp: grpc_encode::u256_be(commit_head.timestamp),
        };

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::CommitHead(pb_commit_head)),
        };

        // Clear only the committed block's tracking; keep future blocks.
        self.block_tx_hashes
            .retain(|k, _| k.block_number != commit_head.block_number);
        self.override_n_transactions = None;
        self.override_last_tx_hash = None;
        self.override_prev_tx_hash = None;

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

        let block_id = tx_execution_id.as_block_execution_id();
        let prev_tx_hash = self.override_prev_tx_hash.take().unwrap_or_else(|| {
            self.block_tx_hashes
                .get(&block_id)
                .and_then(|v| v.last().copied())
        });

        self.block_tx_hashes
            .entry(block_id)
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

        self.current_block_by_iteration
            .insert(iteration_id, block_env.number);
        self.block_tx_hashes
            .entry(BlockExecutionId {
                block_number: block_env.number,
                iteration_id,
            })
            .or_default();

        let new_iteration = pb::NewIteration {
            iteration_id,
            block_env: Some(Self::build_pb_block_env(&block_env)),
            parent_beacon_block_root: Some(vec![0u8; 32]), // B256::ZERO for EIP-4788
        };

        let event = Event {
            event_id: rand::random(),
            event: Some(EventVariant::NewIteration(new_iteration)),
        };

        self.send_event(event).await
    }

    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        self.reorg_depth(tx_execution_id, vec![tx_execution_id.tx_hash])
            .await
    }

    async fn reorg_depth(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_hashes: Vec<TxHash>,
    ) -> Result<(), String> {
        info!(
            target: "LocalInstanceGrpcDriver",
            "LocalInstance sending reorg for: {:?} with depth {}",
            tx_execution_id.tx_hash, tx_hashes.len()
        );

        let block_id = tx_execution_id.as_block_execution_id();

        // Remove the reorged transactions from our local tracking
        if let Some(hashes) = self.block_tx_hashes.get_mut(&block_id) {
            for tx_hash in tx_hashes.iter().rev() {
                if hashes.last() == Some(tx_hash) {
                    hashes.pop();
                } else {
                    debug!(
                        target: "LocalInstanceGrpcDriver",
                        "Reorg hash {:?} does not match last tracked transaction {:?} in iteration {}",
                        tx_hash, hashes.last(), block_id.iteration_id
                    );
                    break;
                }
            }
        }

        let reorg_event = pb::ReorgEvent {
            tx_execution_id: Some(Self::build_pb_tx_execution_id(&tx_execution_id)),
            tx_hashes: tx_hashes.iter().map(|h| grpc_encode::b256(*h)).collect(),
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

    fn set_prev_tx_hash(&mut self, tx_hash: Option<TxHash>) {
        self.override_prev_tx_hash = Some(tx_hash);
    }

    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash> {
        if let Some(value) = &self.override_last_tx_hash {
            return *value;
        }
        let block_number = self
            .current_block_by_iteration
            .get(&iteration_id)
            .copied()?;
        self.block_tx_hashes
            .get(&BlockExecutionId {
                block_number,
                iteration_id,
            })
            .and_then(|v| v.last().copied())
    }

    fn get_tx_count(&self, iteration_id: u64) -> u64 {
        self.override_n_transactions.unwrap_or_else(|| {
            let Some(block_number) = self.current_block_by_iteration.get(&iteration_id).copied()
            else {
                return 0;
            };
            self.block_tx_hashes
                .get(&BlockExecutionId {
                    block_number,
                    iteration_id,
                })
                .map_or(0, |v| v.len() as u64)
        })
    }
}
