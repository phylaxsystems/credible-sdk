//! # `LocalInstance`
//!
//! The `LocalInstance` contains helpers for instantiating a sidecar and comminicating to it
//! via any viable transport. Transport communication is abstracted via the `TestTransport`
//! trait, and the `LocalInstance` can be initialized with any transport which impls it.
//!
//! Alongside the `engine_test` macro, the `LocalInstance` serves to abstract a lot of the
//! interactions and boilerplate one would have to write for the sidecar tests.
//!
//! In summary, the `LocalInstance`:
//!
//! - Instantiates a sidecar with all necessary components needed to run assertions and accept txs,
//! - Create new blocks and iterations,
//! - Create and send arbitrary transaction events over the transports,
//! - Contains helper functions to validate transaction inclusion and assertion execution.

use super::local_instance_db::LocalInstanceDb;
use crate::{
    cache::{
        Sources,
        sources::Source,
    },
    engine::{
        EngineError,
        TransactionResult,
        queue::CommitHead,
    },
    execution_ids::{
        BlockExecutionId,
        TxExecutionId,
    },
    transactions_state::RequestTransactionResult,
};
use alloy::{
    eips::eip7702::{
        RecoveredAuthority,
        RecoveredAuthorization,
    },
    primitives::TxHash,
    rpc::types::{
        AccessList,
        Authorization,
    },
    signers::Either,
};
use assertion_executor::{
    primitives::{
        AccountInfo,
        Address,
        B256,
        BlockEnv,
        TxEnv,
        TxKind,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::{
        COUNTER_ADDRESS,
        bytecode,
        counter_call,
    },
};
use int_test_utils::node_protocol_mock_server::DualProtocolMockServer;
use rand::Rng;
use revm::{
    DatabaseRef,
    context::{
        transaction::AccessListItem,
        tx::TxEnvBuilder,
    },
    context_interface::block::BlobExcessGasAndPrice,
    database::CacheDB,
    primitives::{
        Bytes,
        address,
    },
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{
            AtomicBool,
            Ordering,
        },
    },
    time::{
        Duration,
        Instant,
    },
};
use tokio::task::JoinHandle;
use tracing::info;

/// Test database error type
type TestDbError = std::convert::Infallible;

#[derive(Debug)]
enum WaitError {
    Timeout,
    ChannelClosed,
}

pub struct EngineThreadHandle {
    shutdown: Arc<AtomicBool>,
    handle: std::thread::JoinHandle<Result<(), EngineError>>,
}

impl EngineThreadHandle {
    pub fn new(
        handle: std::thread::JoinHandle<Result<(), EngineError>>,
        shutdown: Arc<AtomicBool>,
    ) -> Self {
        Self { shutdown, handle }
    }

    pub fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub fn shutdown_and_join(self) {
        self.shutdown.store(true, Ordering::Release);
        let _ = self.handle.join();
    }
}

#[allow(async_fn_in_trait)]
pub trait TestTransport: Sized {
    /// Creates a `LocalInstance` with a specific transport
    async fn new() -> Result<LocalInstance<Self>, String>;

    /// Send a commit head event to finalize a block.
    /// This is the core method that handles block finalization with all options
    /// including EIP-2935 `block_hash` and EIP-4788 `beacon_block_root`.
    async fn send_commit_head(&mut self, commit_head: CommitHead) -> Result<(), String>;

    /// Advance the core engine block by finalizing what iteration was selected,
    /// and sending a blockenv for the subsequent block.
    ///
    /// Should send a `CommitHead` event to the core engine.
    async fn new_block(
        &mut self,
        block_number: U256,
        selected_iteration_id: u64,
        n_transactions: u64,
    ) -> Result<(), String>;

    /// Send a transaction to the core engine via the transport
    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String>;

    /// Send a new iteration event with custom block environment data.
    ///
    /// Should send a `NewIteration` event to the core engine.
    async fn new_iteration(&mut self, iteration_id: u64, block_env: BlockEnv)
    -> Result<(), String>;

    /// Send a new transaction reorg event. Removes the most recently executed transaction.
    /// Transaction hash provided as an argument must match the tip tx.
    /// If not the call should succeed but the core engine should produce an error.
    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String>;

    /// Send a reorg event to remove the last `depth` transactions.
    ///
    /// `tx_execution_id` identifies the LAST (newest) transaction being reorged.
    /// `tx_hashes` must be non-empty and contain hashes in chronological order (oldest first),
    /// with `tx_hashes.last() == tx_execution_id.tx_hash`. The depth is derived from `tx_hashes.len()`.
    async fn reorg_depth(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_hashes: Vec<TxHash>,
    ) -> Result<(), String>;

    /// Set the number of transactions to be sent in the next blockEnv
    fn set_n_transactions(&mut self, n_transactions: u64);

    /// Set the last tx hash to be sent in the next `CommitHead`.
    ///
    /// This is used to intentionally create invalid `CommitHead` events in tests
    /// (e.g. "wrong last tx hash" scenarios).
    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>);

    /// Override the `prev_tx_hash` used for the next transaction sent via `send_transaction`.
    ///
    /// Test drivers treat this as a one-shot override to model broken or non-linear chains
    /// without affecting `CommitHead` construction.
    fn set_prev_tx_hash(&mut self, tx_hash: Option<TxHash>);

    /// Get the last transaction hash for a given iteration
    fn get_last_tx_hash(&self, iteration_id: u64) -> Option<TxHash>;

    /// Get the transaction count for a given iteration
    fn get_tx_count(&self, iteration_id: u64) -> u64;
}

/// `LocalInstance` is used to instantiate the core engine and a transport.
/// This struct manages the lifecycle of a test engine and transport instance.
///
/// Used for testing of transaction processing, assertion validation,
/// and multi-block scenarios. Provides pre-funded accounts and loaded test assertions.
/// Works with any type that implements `TestTransport`.
///
/// The `LocalInstance` is not designed to be instantiated by itself directly.
/// Instead we instantiate it with the `engine_test` macro. This allows us to
/// generate tests for many transports from a single test case.
///
/// Test cases should be written to use the `LocalInstance` methods instead of
/// manually sending/verifying because of this.
pub struct LocalInstance<T: TestTransport> {
    /// The underlying database
    db: Arc<LocalInstanceDb<CacheDB<Arc<Sources>>>>,
    /// Underlying cache
    sources: Arc<Sources>,
    /// List of cache sources
    pub list_of_sources: Vec<Arc<dyn Source>>,
    /// The mock HTTP representing the Eth RPC source
    pub eth_rpc_source_http_mock: DualProtocolMockServer,
    /// The mock HTTP representing the fallback Eth RPC source
    pub fallback_eth_rpc_source_http_mock: DualProtocolMockServer,
    /// The assertion store
    assertion_store: Arc<AssertionStore>,
    /// Content hash cache for deduplication (shared between engine and transport)
    content_hash_cache: crate::transport::invalidation_dupe::ContentHashCache,
    /// Transport task handle
    transport_handle: Option<JoinHandle<()>>,
    /// Engine task handle
    engine_handle: Option<EngineThreadHandle>,
    /// Current block number
    pub block_number: U256,
    /// Shared transaction results from engine
    transaction_results: Arc<crate::TransactionsState>,
    /// Default account for transactions
    pub default_account: Address,
    /// Nonce per address per iteration ID: (address, `iteration_id`) -> nonce
    iteration_nonce: HashMap<(Address, u64), u64>,
    /// Container type that holds the transport and impls `TestTransport`
    pub transport: T,
    /// Local address for the transport
    pub local_address: Option<SocketAddr>,
    /// Current iteration ID to be set in the transactions and `blockEnv`
    pub iteration_id: u64,
    /// Hashmap of number of transactions sent per iteration
    pub iteration_tx_map: HashMap<u64, u64>,
    /// Tracks which iteration IDs have already been seeded with a `BlockEnv` for the current block
    active_iterations: HashSet<u64>,
    /// Tracks the next committed nonce per address after the latest finalized block
    committed_nonce: HashMap<Address, u64>,
    /// Optional per-block timestamp overrides for tests
    block_timestamps: HashMap<u64, U256>,
}

impl<T: TestTransport> LocalInstance<T> {
    const TRANSACTION_RESULT_TIMEOUT: Duration = Duration::from_millis(250);
    const CONDITION_TIMEOUT: Duration = Duration::from_secs(10);
    const CONDITION_POLL_INTERVAL: Duration = Duration::from_millis(20);
    const BASE_BLOCK_TIMESTAMP: u64 = 1_234_567_890;

    /// Create a new local instance with mock transport
    pub async fn new() -> Result<LocalInstance<T>, String> {
        T::new().await
    }

    /// Internal constructor for creating `LocalInstance` with all fields
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_internal(
        db: Arc<LocalInstanceDb<CacheDB<Arc<Sources>>>>,
        sources: Arc<Sources>,
        eth_rpc_source_http_mock: DualProtocolMockServer,
        fallback_eth_rpc_source_http_mock: DualProtocolMockServer,
        assertion_store: Arc<AssertionStore>,
        content_hash_cache: crate::transport::invalidation_dupe::ContentHashCache,
        transport_handle: Option<JoinHandle<()>>,
        engine_handle: Option<EngineThreadHandle>,
        block_number: U256,
        transaction_results: Arc<crate::TransactionsState>,
        default_account: Address,
        local_address: Option<&SocketAddr>,
        list_of_sources: Vec<Arc<dyn Source>>,
        transport: T,
    ) -> Self {
        Self {
            db,
            sources,
            eth_rpc_source_http_mock,
            fallback_eth_rpc_source_http_mock,
            assertion_store,
            content_hash_cache,
            transport_handle,
            engine_handle,
            block_number,
            transaction_results,
            default_account,
            iteration_nonce: HashMap::new(),
            transport,
            list_of_sources,
            local_address: local_address.copied(),
            iteration_id: 1,
            iteration_tx_map: HashMap::new(),
            active_iterations: HashSet::new(),
            committed_nonce: HashMap::new(),
            block_timestamps: HashMap::new(),
        }
    }

    /// Sets the current iteration ID
    pub fn set_current_iteration_id(&mut self, iteration_id: u64) {
        self.iteration_id = iteration_id;
    }

    /// Override the timestamp used for a given block number.
    pub fn set_block_timestamp(&mut self, block_number: U256, timestamp: U256) {
        let block_number: u64 = block_number.saturating_to::<u64>();
        self.block_timestamps.insert(block_number, timestamp);
    }

    /// Send a block with multiple transactions
    pub async fn send_block_with_txs(
        &mut self,
        transactions: Vec<(B256, TxEnv)>,
    ) -> Result<(), String> {
        // Send the block environment first
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        // Then send all transactions
        for (tx_hash, tx_env) in transactions {
            let tx_execution_id = self.build_tx_id(tx_hash);
            self.transport
                .send_transaction(tx_execution_id, tx_env)
                .await?;
            self.iteration_tx_map
                .entry(self.iteration_id)
                .and_modify(|x| *x += 1)
                .or_insert(1);
        }

        Ok(())
    }

    /// Get a reference to the underlying database
    pub fn db(&self) -> &Arc<LocalInstanceDb<CacheDB<Arc<Sources>>>> {
        &self.db
    }

    /// Return the number of cache resets observed so far.
    pub fn cache_reset_count(&self) -> u64 {
        self.sources.reset_latest_unprocessed_block_count()
    }

    /// Get a reference to the assertion store
    pub fn assertion_store(&self) -> &Arc<AssertionStore> {
        &self.assertion_store
    }

    /// Get a reference to the content hash cache for testing
    pub fn content_hash_cache(&self) -> &crate::transport::invalidation_dupe::ContentHashCache {
        &self.content_hash_cache
    }

    /// Get the default account address
    pub fn default_account(&self) -> Address {
        self.default_account
    }

    /// Get the current nonce map
    pub fn current_nonce(&self) -> &HashMap<(Address, u64), u64> {
        &self.iteration_nonce
    }

    /// Get the current nonce for a specific address in a specific iteration and increment it
    pub fn next_nonce(&mut self, caller: Address, block_execution_id: BlockExecutionId) -> u64 {
        let committed = *self.committed_nonce.get(&caller).unwrap_or(&0);
        let key = (caller, block_execution_id.iteration_id);
        let entry = self.iteration_nonce.entry(key).or_insert(committed);

        let nonce = *entry;
        *entry += 1;
        nonce
    }

    /// Get the current block execution identifier for this instance.
    pub fn current_block_execution_id(&self) -> BlockExecutionId {
        BlockExecutionId {
            block_number: self.block_number,
            iteration_id: self.iteration_id,
        }
    }

    /// Reset the nonce to a specific value for a given address and iteration
    pub fn set_nonce(&mut self, caller: Address, nonce: u64, block_execution_id: BlockExecutionId) {
        let key = (caller, block_execution_id.iteration_id);
        self.iteration_nonce.insert(key, nonce);
    }

    /// Clear all nonces
    pub fn clear_nonce(&mut self) {
        self.iteration_nonce.clear();
        self.committed_nonce.clear();
    }

    fn resync_nonces_from_canonical_state(&mut self) -> Result<(), String> {
        let mut addresses = HashSet::new();
        addresses.insert(self.default_account);

        for address in self.committed_nonce.keys() {
            addresses.insert(*address);
        }
        for (address, _iteration_id) in self.iteration_nonce.keys() {
            addresses.insert(*address);
        }

        // Pending state is invalid after a cache flush.
        self.iteration_nonce.clear();
        self.committed_nonce.clear();

        for address in addresses {
            let nonce = self
                .db
                .with_read(|db| db.basic_ref(address))
                .map_err(|e| format!("Failed to read account nonce for {address}: {e:?}"))?
                .map_or(0, |info| info.nonce);
            self.committed_nonce.insert(address, nonce);
        }

        Ok(())
    }

    async fn wait_for_engine_head(&self, block_number: U256) -> Result<(), String> {
        let start = Instant::now();
        loop {
            if self.sources.get_latest_head() >= block_number {
                return Ok(());
            }
            if start.elapsed() > Self::CONDITION_TIMEOUT {
                return Err(format!(
                    "timed out waiting for engine to commit block {block_number}"
                ));
            }
            tokio::time::sleep(Self::CONDITION_POLL_INTERVAL).await;
        }
    }

    /// Wait for a short time to allow transaction processing
    /// Since we can't directly access engine results in this test setup,
    /// tests should verify behavior through other means (e.g., state changes)
    pub async fn wait_for_processing(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }

    /// Wait until a transaction is processed, retrying until timeout.
    pub async fn wait_for_transaction_processed(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Result<TransactionResult, String> {
        tokio::time::timeout(Self::CONDITION_TIMEOUT, async {
            loop {
                match self.wait_for_transaction_result(tx_execution_id).await {
                    Ok(result) => return Ok(result),
                    Err(WaitError::ChannelClosed) => {
                        return Err("transaction result channel closed".to_string());
                    }
                    Err(WaitError::Timeout) => {
                        tokio::time::sleep(Self::CONDITION_POLL_INTERVAL).await;
                    }
                }
            }
        })
        .await
        .map_err(|_| "timed out waiting for transaction result".to_string())?
    }

    /// Wait for a specific source to report as synced up to a block number
    pub async fn wait_for_source_synced(
        &self,
        source_index: usize,
        block_number: U256,
        min_synced_block: U256,
    ) -> Result<(), String> {
        let source = self
            .list_of_sources
            .get(source_index)
            .cloned()
            .ok_or_else(|| format!("source index {source_index} out of bounds"))?;

        tokio::time::timeout(Self::CONDITION_TIMEOUT, async move {
            loop {
                if source.is_synced(min_synced_block, block_number) {
                    return;
                }
                tokio::time::sleep(Self::CONDITION_POLL_INTERVAL).await;
            }
        })
        .await
        .map_err(|_| {
            format!("timed out waiting for source {source_index} to sync to block {block_number}")
        })
    }

    async fn wait_for_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Result<TransactionResult, WaitError> {
        match self
            .transaction_results
            .request_transaction_result(tx_execution_id)
        {
            RequestTransactionResult::Result(result) => Ok(result),
            RequestTransactionResult::Channel(mut receiver) => {
                match tokio::time::timeout(Self::TRANSACTION_RESULT_TIMEOUT, receiver.recv()).await
                {
                    Ok(Ok(result)) => Ok(result),
                    Ok(Err(_)) => Err(WaitError::ChannelClosed),
                    Err(_) => Err(WaitError::Timeout),
                }
            }
        }
    }

    /// Builds a `TxExecutionId` with the block hash and instance
    /// currently in use by the `LocalInstance`.
    fn build_tx_id(&self, tx_hash: B256) -> TxExecutionId {
        let index = *self.iteration_tx_map.get(&self.iteration_id).unwrap_or(&0);
        TxExecutionId {
            block_number: self.block_number,
            iteration_id: self.iteration_id,
            tx_hash,
            index,
        }
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
    ///
    /// # Panics
    ///
    /// Panics if the transaction builder fails to build a valid transaction.
    pub fn create_test_transaction(
        &mut self,
        caller: Address,
        value: U256,
        data: Bytes,
        block_execution_id: BlockExecutionId,
    ) -> TxEnv {
        let nonce = self.next_nonce(caller, block_execution_id);
        TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(5_000_000)
            .gas_price(0)
            .kind(TxKind::Create)
            .value(value)
            .data(data)
            .nonce(nonce)
            .build()
            .expect("failed to build create test transaction")
    }

    /// Generate a random transaction hash
    pub fn generate_random_tx_hash() -> B256 {
        let mut rng = rand::rng();
        let mut hash_bytes = [0u8; 32];
        rng.fill(&mut hash_bytes);
        B256::from(hash_bytes)
    }

    /// Generate a random address
    pub fn generate_random_address() -> Address {
        let mut rng = rand::rng();
        let mut hash_bytes = [0u8; 20];
        rng.fill(&mut hash_bytes);
        Address::from(hash_bytes)
    }

    /// Prefund accounts with ETH for testing
    pub fn fund_accounts(&mut self, accounts: &[(Address, U256)]) -> Result<(), String> {
        self.db.with_write(|db| {
            for (address, balance) in accounts {
                let account_info = AccountInfo {
                    balance: *balance,
                    ..Default::default()
                };
                db.insert_account_info(*address, account_info);
            }
        });

        Ok(())
    }

    /// Execute an async action and assert it results in a cache flush.
    pub async fn expect_cache_flush<F>(&mut self, action: F) -> Result<(), String>
    where
        for<'a> F: FnOnce(&'a mut Self) -> Pin<Box<dyn Future<Output = Result<(), String>> + 'a>>,
    {
        let before = self.cache_reset_count();
        action(self).await?;

        let start = Instant::now();
        loop {
            let after = self.cache_reset_count();
            tracing::debug!("Flushes before {}, after {}", before, after);
            if after > before {
                break;
            }
            if start.elapsed() > Self::CONDITION_TIMEOUT {
                return Err("cache flush was not observed within timeout".to_string());
            }
            tokio::time::sleep(Self::CONDITION_POLL_INTERVAL).await;
        }
        self.resync_nonces_from_canonical_state()?;

        Ok(())
    }

    /// Send a block with default parent hashes for EIP-2935 and EIP-4788
    async fn send_block(&mut self, block_number: U256, send_rpc_node: bool) -> Result<(), String> {
        // Always provide parent hashes for EIP-2935 and EIP-4788 system calls
        // These are required when running on Cancun+ specs
        let block_hash = Self::generate_random_tx_hash();
        let beacon_block_root = if block_number == U256::ZERO {
            Some(B256::ZERO)
        } else {
            Some(Self::generate_random_tx_hash())
        };
        self.send_block_with_hashes_internal(
            block_number,
            block_hash,
            beacon_block_root,
            send_rpc_node,
        )
        .await
    }

    /// Internal method to send a block finalization with optional parent hashes
    async fn send_block_with_hashes_internal(
        &mut self,
        block_number: U256,
        block_hash: B256,
        beacon_block_root: Option<B256>,
        send_rpc_node: bool,
    ) -> Result<(), String> {
        let n_transactions = self.transport.get_tx_count(self.iteration_id);
        let last_tx_hash = self.transport.get_last_tx_hash(self.iteration_id);

        let commit_head = CommitHead::new(
            block_number,
            self.iteration_id,
            last_tx_hash,
            n_transactions,
            block_hash,
            beacon_block_root,
            self.block_timestamp(block_number),
        );

        if send_rpc_node {
            self.eth_rpc_source_http_mock
                .send_new_head_with_block_number(u64::try_from(block_number).unwrap());
        }

        self.transport.send_commit_head(commit_head).await?;
        self.wait_for_engine_head(block_number).await?;

        // Update committed nonce snapshot using the iteration we just finalized.
        let selected_iteration_id = self.iteration_id;
        for ((address, iteration_id), &next_nonce) in &self.iteration_nonce {
            if *iteration_id == selected_iteration_id {
                self.committed_nonce.insert(*address, next_nonce);
            }
        }
        self.iteration_nonce.clear();

        self.active_iterations.clear();

        let next_block_number = block_number + U256::from(1);
        let block_env = self.default_block_env(next_block_number);
        self.transport
            .new_iteration(self.iteration_id, block_env)
            .await?;
        self.active_iterations.insert(self.iteration_id);

        // TODO: commit tx numbers to this map!
        self.iteration_tx_map.clear();
        Ok(())
    }

    /// Public method to create a new block with explicit parent hashes (EIP-2935 and EIP-4788)
    pub async fn new_block_with_hashes(
        &mut self,
        block_hash: B256,
        beacon_block_root: Option<B256>,
    ) -> Result<(), String> {
        let beacon_block_root = beacon_block_root.or_else(|| Some(Self::generate_random_tx_hash()));

        self.send_block_with_hashes_internal(
            self.block_number,
            block_hash,
            beacon_block_root,
            true,
        )
        .await?;
        self.block_number += U256::from(1);
        Ok(())
    }

    /// Send a successful CREATE transaction using the default account
    pub async fn send_successful_create_tx(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<TxExecutionId, String> {
        // Ensure we have a block
        self.send_block(self.block_number, true).await?;

        self.block_number += U256::from(1);

        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        let tx_execution_id = self.build_tx_id(tx_hash);

        // Create transaction
        let tx_env = self.create_test_transaction(
            self.default_account,
            value,
            data,
            tx_execution_id.as_block_execution_id(),
        );

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok(tx_execution_id)
    }

    /// Send a standalone new iteration event for the most recent block.
    ///
    /// Generates a default `BlockEnv` matching our other helpers so callers only
    /// need to provide the iteration identifier.
    ///
    /// Subsequent transactions will use the iteration id set here.
    pub async fn new_iteration(&mut self, iteration_id: u64) -> Result<(), String> {
        if self.block_number == 0 {
            return Err("new_iteration requires at least one block to have been sent".to_string());
        }

        self.iteration_id = iteration_id;

        if self.active_iterations.contains(&iteration_id) {
            return Ok(());
        }

        let current_block_number = self.block_number;
        let block_env = self.default_block_env(current_block_number);

        self.transport
            .new_iteration(iteration_id, block_env)
            .await?;
        self.active_iterations.insert(iteration_id);
        Ok(())
    }

    /// Send a successful CREATE transaction using the default account, without a new blockenv
    pub async fn send_successful_create_tx_dry(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<TxExecutionId, String> {
        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;

        // Create transaction
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(5_000_000)
            .gas_price(0)
            .kind(TxKind::Create)
            .value(value)
            .data(data)
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        // Send transaction
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        // Wait for processing
        self.wait_for_processing(Duration::from_millis(5)).await;

        // If the engine exited (e.g., received tx before blockenv), return an error
        if let Some(handle) = &self.engine_handle {
            if handle.is_finished() {
                return Err(
                    "Core engine exited while processing transaction (likely sent before blockenv)"
                        .to_string(),
                );
            }
        } else {
            return Err("Engine handle does not exist! Make sure the engine was initialized before calling fn!".to_string());
        }

        Ok(tx_execution_id)
    }

    /// Send a successful CREATE transaction using a random account to enforce a cache miss (in the
    /// in-memory cache) and therefore, enforcing a cache fetch from the providers (e.g., Eth RPC source)
    pub async fn send_create_tx_with_cache_miss(
        &mut self,
    ) -> Result<(Address, TxExecutionId), String> {
        // Ensure we have a block
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        // Send transaction
        let tx_execution_id = self.build_tx_id(tx_hash);

        let caller = Self::generate_random_address();
        let nonce = self.next_nonce(caller, tx_execution_id.as_block_execution_id());

        // Create transaction
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(0)
            .kind(TxKind::Create)
            .value(U256::ZERO)
            .data(Bytes::from(vec![0xff, 0xff, 0xff, 0xff, 0xff]))
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok((caller, tx_execution_id))
    }

    pub async fn send_create_tx_with_cache_miss_dry(
        &mut self,
    ) -> Result<(Address, TxExecutionId), String> {
        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);
        let caller = Self::generate_random_address();
        let nonce = self.next_nonce(caller, tx_execution_id.as_block_execution_id());

        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(0)
            .kind(TxKind::Create)
            .value(U256::ZERO)
            .data(Bytes::from(vec![0xff, 0xff, 0xff, 0xff, 0xff]))
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok((caller, tx_execution_id))
    }

    /// Send a reverting CREATE transaction using the default account
    pub async fn send_reverting_create_tx(&mut self) -> Result<TxExecutionId, String> {
        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        // Send transaction
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;

        // Create reverting transaction (PUSH1 0x00 PUSH1 0x00 REVERT)
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(0)
            .kind(TxKind::Create)
            .value(U256::ZERO)
            .data(Bytes::from(vec![0x60, 0x00, 0x60, 0x00, 0xfd]))
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok(tx_execution_id)
    }

    pub async fn send_call_tx_dry(
        &mut self,
        to: Address,
        value: U256,
        data: Bytes,
    ) -> Result<TxExecutionId, String> {
        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );

        let tx_env = TxEnvBuilder::new()
            .caller(self.default_account)
            .gas_limit(100_000)
            .gas_price(0)
            .kind(TxKind::Call(to))
            .value(value)
            .data(data)
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        // FIXME: Propagate correctly the prev tx hash
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok(tx_execution_id)
    }

    /// Send a CALL transaction to an existing contract using the default account
    pub async fn send_call_tx(
        &mut self,
        to: Address,
        value: U256,
        data: Bytes,
    ) -> Result<TxExecutionId, String> {
        // Ensure we have a block
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        // Send transaction
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;

        // Create call transaction
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(0)
            .kind(TxKind::Call(to))
            .value(value)
            .data(data)
            .nonce(nonce)
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        Ok(tx_execution_id)
    }

    /// Sends transactions of all tx types and verifies they pass
    /// and properly produce desired outcomes (proper gas accounting etc...).
    ///
    /// # Panics
    ///
    /// Panics if the transaction builder fails to build any of the test transactions.
    #[allow(clippy::too_many_lines)]
    pub async fn send_all_tx_types(&mut self) -> Result<(), String> {
        // legacy tx
        let legacy_tx = self
            .send_successful_create_tx(U256::default(), Bytes::default())
            .await?;
        let _ = self.wait_for_transaction_processed(&legacy_tx).await?;

        // type 1, eip-2930 optional access lists
        // verify that we spend less gas on storage reads
        // interact with the pre-loaded counter contract so the access list actually matters
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        // Send transaction
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;

        let access_list = AccessList::from(vec![AccessListItem {
            address: COUNTER_ADDRESS,
            // slot 0 stores the counter value and becomes warm via the access list
            storage_keys: vec![B256::ZERO],
        }]);
        let counter_call_data = counter_call().data;

        // Create call transaction
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(1)
            .kind(TxKind::Call(COUNTER_ADDRESS))
            .value(U256::default())
            .data(counter_call_data)
            .nonce(nonce)
            .access_list(access_list)
            .tx_type(Some(1))
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;
        let tx_id_eip2930 = tx_execution_id;
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);
        let _ = self.wait_for_transaction_processed(&tx_id_eip2930).await?;

        // type2, eip-1559
        // verify we correctly decrement gas for the account sending the tx
        // according to eip-1559 rules
        // TODO: send blockenv with base fee of 2 and verify we include the
        // tx below and properly decrement gas
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        // Generate transaction hash
        let tx_hash = Self::generate_random_tx_hash();

        // Send transaction
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;

        // Create call transaction
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(2)
            .kind(TxKind::Call(Address::default()))
            .value(U256::default())
            .data(Bytes::default())
            .nonce(nonce)
            .gas_priority_fee(Some(2))
            .tx_type(Some(2))
            .build()
            .map_err(|e| format!("Failed to build TxEnv: {e:?}"))?;

        let tx_id_eip1559 = tx_execution_id;
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);
        let _ = self.wait_for_transaction_processed(&tx_id_eip1559).await?;

        // type3, eip-4844
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);

        let nonce = self.next_nonce(
            self.default_account,
            tx_execution_id.as_block_execution_id(),
        );
        let caller = self.default_account;
        let mut blob_hash_bytes = [0u8; 32];
        blob_hash_bytes[0] = 0x01;
        let blob_hash = B256::from(blob_hash_bytes);

        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(5)
            .gas_priority_fee(Some(1))
            .kind(TxKind::Call(COUNTER_ADDRESS))
            .data(counter_call().data)
            .nonce(nonce)
            .blob_hashes(vec![blob_hash])
            .max_fee_per_blob_gas(1)
            .tx_type(Some(3))
            .build()
            .unwrap();
        // FIXME: Propagate correctly the prev tx hash
        let tx_id_eip4844 = tx_execution_id;
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);
        let _ = self.wait_for_transaction_processed(&tx_id_eip4844).await?;

        // type4, eip-7702
        // Authorization list present, should derive EIP-7702
        // TODO: check if the account has code
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        let auth = RecoveredAuthorization::new_unchecked(
            Authorization {
                chain_id: U256::from(1),
                nonce: 0,
                address: Address::default(),
            },
            RecoveredAuthority::Valid(Address::default()),
        );
        let caller = Address::from([1u8; 20]);
        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);
        let nonce = self.next_nonce(caller, tx_execution_id.as_block_execution_id());
        let tx_env = TxEnvBuilder::new()
            .caller(caller)
            .gas_limit(100_000)
            .gas_price(1)
            .gas_priority_fee(Some(10))
            .authorization_list(vec![Either::Right(auth)])
            .kind(TxKind::Call(Address::from([2u8; 20])))
            .value(U256::ZERO)
            .data(Bytes::default())
            .nonce(nonce)
            .tx_type(Some(4))
            .build()
            .unwrap();
        // FIXME: Propagate correctly the prev tx hash
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);
        let _ = self
            .wait_for_transaction_processed(&tx_execution_id)
            .await?;

        Ok(())
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
        let assertion = AssertionState::new_test(&assertion_bytecode);

        self.insert_assertion(address, assertion)
            .map_err(|e| format!("Failed to insert custom assertion {artifact_name}: {e}"))
    }

    /// Get transaction result by hash
    pub fn get_transaction_result(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Option<TransactionResult> {
        self.transaction_results
            .get_transaction_result(tx_execution_id)
            .map(|r| r.clone())
    }

    /// Check if transaction was successful and valid
    pub async fn is_transaction_successful(&self, tx_hash: &TxExecutionId) -> Result<bool, String> {
        match self.wait_for_transaction_result(tx_hash).await {
            Ok(TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            }) => Ok(is_valid && execution_result.is_success()),
            Ok(TransactionResult::ValidationError(e)) => Err(e),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// Check if transaction reverted but passed validation
    pub async fn is_transaction_reverted_but_valid(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Result<bool, String> {
        match self.wait_for_transaction_result(tx_execution_id).await {
            Ok(TransactionResult::ValidationCompleted {
                execution_result,
                is_valid,
            }) => Ok(is_valid && !execution_result.is_success()),
            Ok(TransactionResult::ValidationError(e)) => Err(e),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// Check if transaction failed validation
    pub async fn is_transaction_invalid(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Result<bool, String> {
        match self.wait_for_transaction_result(tx_execution_id).await {
            Ok(TransactionResult::ValidationCompleted { is_valid, .. }) => Ok(!is_valid),
            Ok(TransactionResult::ValidationError(e)) => Err(e),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    /// Check if transaction failed validation.
    /// This function will only return false if the engine exited.
    pub async fn is_transaction_removed(
        &self,
        tx_execution_id: &TxExecutionId,
    ) -> Result<bool, String> {
        loop {
            self.wait_for_processing(Duration::from_millis(5)).await;

            // Bail out if the engine stopped running while we were waiting for the
            // transaction to disappear from the results map. This can happen when a
            // reorg request is rejected and the engine exits early.
            if let Some(handle) = &self.engine_handle {
                if handle.is_finished() {
                    if self.get_transaction_result(tx_execution_id).is_some() {
                        return Ok(false);
                    }
                    return Ok(true);
                }
            } else {
                return Err("engine handle missing while checking transaction removal".to_string());
            }

            let rax = match self.wait_for_transaction_result(tx_execution_id).await {
                Ok(
                    TransactionResult::ValidationCompleted { .. }
                    | TransactionResult::ValidationError(_),
                ) => continue,
                Err(e) => Err(e),
            };

            match rax {
                Ok(()) => return Ok(false), // should never be hit
                Err(WaitError::Timeout) => return Ok(true),
                Err(WaitError::ChannelClosed) => {
                    return Err("transaction result channel closed".to_string());
                }
            }
        }
    }

    /// Send and verify a successful CREATE transaction using the default account
    pub async fn send_and_verify_successful_create_tx(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<Address, String> {
        let tx_hash = self.send_successful_create_tx(value, data).await?;

        if !self.is_transaction_successful(&tx_hash).await? {
            return Err("Transaction was not successful".to_string());
        }

        // Calculate the expected contract address (simplified for testing)
        let contract_address = address!("76cae8af66cb2488933e640ba08650a3a8e7ae19");

        Ok(contract_address)
    }

    /// Send and verify a reverting transaction using the default account
    pub async fn send_and_verify_reverting_create_tx(&mut self) -> Result<(), String> {
        let tx_hash = self.send_reverting_create_tx().await?;

        if !self.is_transaction_reverted_but_valid(&tx_hash).await? {
            return Err("Transaction should have reverted but been valid".to_string());
        }

        Ok(())
    }

    /// Sends a pair of assertion passing and failing transactions.
    /// The transactions call a preloaded counter contract, which can only
    /// be called once due to subsequent assertions invalidating.
    pub async fn send_assertion_passing_failing_pair(&mut self) -> Result<(), String> {
        // Ensure we have a block
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        let basefee = 10u64;
        let caller = counter_call().caller; // Get the caller address
        let block_exec_id = BlockExecutionId {
            block_number: self.block_number,
            iteration_id: self.iteration_id,
        };

        // Create the first transaction (should pass)
        let mut tx_pass = counter_call();
        tx_pass.nonce = self.next_nonce(caller, block_exec_id);
        tx_pass.gas_price = basefee.into();

        // Create the second transaction (should fail assertion)
        let mut tx_fail = counter_call();
        tx_fail.nonce = self.next_nonce(caller, block_exec_id);
        tx_fail.gas_price = basefee.into();

        // Generate unique transaction hashes
        let hash_pass = Self::generate_random_tx_hash();
        let hash_fail = Self::generate_random_tx_hash();

        // FIXME: Propagate correctly the prev tx hash
        // Send the passing transaction first
        let tx_execution_id_pass = self.build_tx_id(hash_pass);
        self.transport
            .send_transaction(tx_execution_id_pass, tx_pass)
            .await?;
        self.iteration_tx_map
            .entry(self.iteration_id)
            .and_modify(|x| *x += 1)
            .or_insert(1);

        // FIXME: Propagate correctly the prev tx hash
        // Send the failing transaction second
        let tx_execution_id_fail = self.build_tx_id(hash_fail);
        self.transport
            .send_transaction(tx_execution_id_fail, tx_fail)
            .await?;

        // Verify the first transaction passed assertions (execution may still revert).
        match self
            .wait_for_transaction_processed(&tx_execution_id_pass)
            .await?
        {
            TransactionResult::ValidationCompleted { is_valid, .. } => {
                if !is_valid {
                    return Err("First transaction should have passed assertions".to_string());
                }
            }
            TransactionResult::ValidationError(e) => return Err(e),
        }

        // Verify the second transaction failed assertions and was NOT committed
        let fail_invalid = self.is_transaction_invalid(&tx_execution_id_fail).await?;
        if !fail_invalid {
            return Err(
                "Second transaction should have failed assertions and not been committed"
                    .to_string(),
            );
        }

        let tx_execution_id_fail = self.build_tx_id(hash_fail);
        self.transport.reorg(tx_execution_id_fail).await?;

        Ok(())
    }

    /// Sends a reorg event to the core engine via a `Transport`.
    ///
    /// A reorg will remove the most recent transaction if the hash supplied
    /// matches the tip transaction hash.
    /// If not, the core engine should error out and this function will
    /// return an error.
    pub async fn send_reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        self.send_reorg_depth(tx_execution_id, vec![tx_execution_id.tx_hash])
            .await
    }

    /// Sends a reorg event to remove transactions.
    ///
    /// `tx_execution_id` identifies the LAST (newest) transaction being reorged.
    /// `tx_hashes` must be non-empty and contain hashes in chronological order (oldest first),
    /// with `tx_hashes.last() == tx_execution_id.tx_hash`. The depth is derived from `tx_hashes.len()`.
    ///
    /// All transactions in `tx_hashes` will be verified as removed after the reorg.
    pub async fn send_reorg_depth(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_hashes: Vec<TxHash>,
    ) -> Result<(), String> {
        // Make sure the tip tx exists
        let _ = self.is_transaction_invalid(&tx_execution_id).await?;

        let depth = tx_hashes.len() as u64;

        // Send reorg event
        self.transport
            .reorg_depth(tx_execution_id, tx_hashes.clone())
            .await?;

        // Reorg was accepted by the engine. Mirror this in our local nonce tracking
        // so that subsequent transactions use the correct nonce again.
        let key = (self.default_account, tx_execution_id.iteration_id);
        if let Some(current_nonce) = self.iteration_nonce.get_mut(&key) {
            *current_nonce = current_nonce.saturating_sub(depth);
        }

        // Make sure the tip transaction is gone (verifies reorg succeeded)
        if !self.is_transaction_removed(&tx_execution_id).await? {
            return Err("Tip transaction not removed!".to_string());
        }

        Ok(())
    }

    /// Advance the chain by 1 block by sending a new `BlockEnv` to the core engine.
    pub async fn new_block(&mut self) -> Result<(), String> {
        self.send_block(self.block_number, true).await?;
        self.block_number += U256::from(1);

        Ok(())
    }

    /// Advance the chain by 1 block by sending a new `BlockEnv` to the core engine but without
    /// advancing the rpc node client
    pub async fn new_block_unsynced(&mut self) -> Result<(), String> {
        self.send_block(self.block_number, false).await?;
        self.block_number += U256::from(1);

        Ok(())
    }

    fn block_timestamp(&self, block_number: U256) -> U256 {
        let block_number: u64 = block_number.saturating_to::<u64>();
        self.block_timestamps
            .get(&block_number)
            .copied()
            .unwrap_or_else(|| U256::from(Self::BASE_BLOCK_TIMESTAMP + block_number))
    }

    fn default_block_env(&self, block_number: U256) -> BlockEnv {
        BlockEnv {
            number: block_number,
            timestamp: self.block_timestamp(block_number),
            gas_limit: 50_000_000,
            blob_excess_gas_and_price: Some(BlobExcessGasAndPrice {
                excess_blob_gas: 0,
                blob_gasprice: 0,
            }),
            ..BlockEnv::default()
        }
    }
}

impl<T: TestTransport> Drop for LocalInstance<T> {
    fn drop(&mut self) {
        // Abort tasks if still running
        if let Some(handle) = self.transport_handle.take() {
            handle.abort();
        }
        if let Some(handle) = self.engine_handle.take() {
            handle.shutdown_and_join();
        }
    }
}
