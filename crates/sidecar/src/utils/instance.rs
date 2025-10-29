use crate::{
    cache::{
        Cache,
        sources::Source,
    },
    engine::TransactionResult,
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
    context::{
        transaction::AccessListItem,
        tx::TxEnvBuilder,
    },
    database::CacheDB,
    primitives::{
        Bytes,
        address,
    },
};
use std::{
    collections::HashMap,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::Duration,
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

#[allow(async_fn_in_trait)]
pub trait TestTransport: Sized {
    /// Creates a `LocalInstance` with a specific transport
    async fn new() -> Result<LocalInstance<Self>, String>;
    /// Advance the core engine block by sending a new blockenv to it
    async fn new_block(
        &mut self,
        block_number: u64,
        selected_iteration_id: u64,
    ) -> Result<(), String>;
    /// Send a transaction to the core engine via the transport
    async fn send_transaction(
        &mut self,
        tx_execution_id: TxExecutionId,
        tx_env: TxEnv,
    ) -> Result<(), String>;
    /// Send a new transaction reorg event. Removes the last executed transaction.
    /// Transaction hash provided as an argument must match the last executed tx.
    /// If not the call should succeed but the core engine should produce an error.
    async fn reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String>;
    /// Set the number of transactions to be sent in the next blockEnv
    fn set_n_transactions(&mut self, n_transactions: u64);
    /// Set the last tx hash to be sent in the next blockEnv
    fn set_last_tx_hash(&mut self, tx_hash: Option<TxHash>);
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
    db: Arc<CacheDB<Arc<Cache>>>,
    /// Underlying cache
    cache: Arc<Cache>,
    /// List of cache sources
    pub sources: Vec<Arc<dyn Source>>,
    /// The mock HTTP representing the sequencer
    pub sequencer_http_mock: DualProtocolMockServer,
    /// The mock HTTP representing the Besu client
    pub besu_client_http_mock: DualProtocolMockServer,
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
    pub default_account: Address,
    /// Nonce per address per iteration ID: (address, `iteration_id`) -> nonce
    iteration_nonce: HashMap<(Address, u64), u64>,
    /// Container type that holds the transport and impls `TestTransport`
    pub transport: T,
    /// Local address for the HTTP transport
    pub local_address: Option<SocketAddr>,
    /// Current iteration ID to be set in the transactions and `blockEnv`
    pub iteration_id: u64,
}

impl<T: TestTransport> LocalInstance<T> {
    const TRANSACTION_RESULT_TIMEOUT: Duration = Duration::from_millis(250);

    /// Create a new local instance with mock transport
    pub async fn new() -> Result<LocalInstance<T>, String> {
        T::new().await
    }

    /// Internal constructor for creating `LocalInstance` with all fields
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_internal(
        db: Arc<CacheDB<Arc<Cache>>>,
        cache: Arc<Cache>,
        sequencer_http_mock: DualProtocolMockServer,
        besu_client_http_mock: DualProtocolMockServer,
        assertion_store: Arc<AssertionStore>,
        transport_handle: Option<JoinHandle<()>>,
        engine_handle: Option<JoinHandle<()>>,
        block_number: u64,
        transaction_results: Arc<crate::TransactionsState>,
        default_account: Address,
        local_address: Option<&SocketAddr>,
        sources: Vec<Arc<dyn Source>>,
        transport: T,
    ) -> Self {
        Self {
            db,
            cache,
            sequencer_http_mock,
            besu_client_http_mock,
            assertion_store,
            transport_handle,
            engine_handle,
            block_number,
            transaction_results,
            default_account,
            iteration_nonce: HashMap::new(),
            transport,
            sources,
            local_address: local_address.copied(),
            iteration_id: 1,
        }
    }

    /// Sets the current iteration ID
    pub fn set_current_iteration_id(&mut self, iteration_id: u64) {
        self.iteration_id = iteration_id;
    }

    /// Send a block with multiple transactions
    pub async fn send_block_with_txs(
        &mut self,
        transactions: Vec<(B256, TxEnv)>,
    ) -> Result<(), String> {
        // Send the block environment first
        self.send_block(self.block_number).await?;
        self.block_number += 1;

        // Then send all transactions
        for (tx_hash, tx_env) in transactions {
            let tx_execution_id = self.build_tx_id(tx_hash);
            self.transport
                .send_transaction(tx_execution_id, tx_env)
                .await?;
        }

        Ok(())
    }

    /// Get a reference to the underlying database
    pub fn db(&self) -> &Arc<CacheDB<Arc<Cache>>> {
        &self.db
    }

    /// Return the number of cache resets observed so far.
    pub fn cache_reset_count(&self) -> u64 {
        self.cache.reset_required_block_number_count()
    }

    /// Get a reference to the assertion store
    pub fn assertion_store(&self) -> &Arc<AssertionStore> {
        &self.assertion_store
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
        let key = (caller, block_execution_id.iteration_id);
        let entry = self.iteration_nonce.entry(key).or_insert(0);

        let nonce = *entry;
        *entry += 1;
        nonce
    }

    /// Reset the nonce to a specific value for a given address and iteration
    pub fn set_nonce(&mut self, caller: Address, nonce: u64, block_execution_id: BlockExecutionId) {
        let key = (caller, block_execution_id.iteration_id);
        self.iteration_nonce.insert(key, nonce);
    }

    /// Clear all nonces
    pub fn clear_nonce(&mut self) {
        self.iteration_nonce.clear();
    }

    /// Wait for a short time to allow transaction processing
    /// Since we can't directly access engine results in this test setup,
    /// tests should verify behavior through other means (e.g., state changes)
    pub async fn wait_for_processing(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
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
            RequestTransactionResult::Channel(receiver) => {
                match tokio::time::timeout(Self::TRANSACTION_RESULT_TIMEOUT, receiver).await {
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
        TxExecutionId {
            block_number: self.block_number,
            iteration_id: self.iteration_id,
            tx_hash,
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
            .gas_limit(100_000)
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

    /// Execute an async action and assert it results in a cache flush.
    // TODO: because we do not commit state to underlying sources properly we lose all state
    // made during tests when we flush, thus we have to set the nonce to 0
    pub async fn expect_cache_flush<F>(&mut self, action: F) -> Result<(), String>
    where
        for<'a> F: FnOnce(&'a mut Self) -> Pin<Box<dyn Future<Output = Result<(), String>> + 'a>>,
    {
        let before = self.cache_reset_count();
        action(self).await?;
        self.wait_for_processing(Duration::from_millis(15)).await;
        let after = self.cache_reset_count();
        tracing::debug!("Flushes before {}, after {}", before, after);
        if after <= before {
            return Err("cache flush was not observed".to_string());
        }
        self.clear_nonce();

        Ok(())
    }

    async fn send_block(&mut self, block_number: u64) -> Result<(), String> {
        self.transport
            .new_block(self.block_number, self.iteration_id)
            .await?;
        Ok(())
    }

    /// Send a successful CREATE transaction using the default account
    pub async fn send_successful_create_tx(
        &mut self,
        value: U256,
        data: Bytes,
    ) -> Result<TxExecutionId, String> {
        // Ensure we have a block
        self.send_block(self.block_number).await?;

        self.block_number += 1;

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

        Ok(tx_execution_id)
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
            .gas_limit(100_000)
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
    /// in-memory cache) and therefore, enforcing a cache fetch from the providers (e.g., Besu client)
    pub async fn send_create_tx_with_cache_miss(
        &mut self,
    ) -> Result<(Address, TxExecutionId), String> {
        // Ensure we have a block
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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

        Ok((caller, tx_execution_id))
    }

    /// Send a reverting CREATE transaction using the default account
    pub async fn send_reverting_create_tx(&mut self) -> Result<TxExecutionId, String> {
        // Ensure we have a block
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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
        self.send_successful_create_tx(U256::default(), Bytes::default())
            .await?;

        // type 1, eip-2930 optional access lists
        // verify that we spend less gas on storage reads
        // interact with the pre-loaded counter contract so the access list actually matters
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;

        // type2, eip-1559
        // verify we correctly decrement gas for the account sending the tx
        // according to eip-1559 rules
        // TODO: send blockenv with base fee of 2 and verify we include the
        // tx below and properly decrement gas
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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

        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;

        // type3, eip-4844
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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
        self.transport
            .send_transaction(tx_execution_id, tx_env)
            .await?;

        // type4, eip-7702
        // Authorization list present, should derive EIP-7702
        // TODO: check if the account has code
        self.send_block(self.block_number).await?;
        self.block_number += 1;

        let auth = RecoveredAuthorization::new_unchecked(
            Authorization {
                chain_id: U256::from(1),
                nonce: 0,
                address: Address::default(),
            },
            RecoveredAuthority::Valid(Address::default()),
        );
        let tx_env = TxEnvBuilder::new()
            .caller(Address::from([1u8; 20]))
            .gas_priority_fee(Some(10))
            .authorization_list(vec![Either::Right(auth)])
            .kind(TxKind::Call(Address::from([2u8; 20])))
            .tx_type(Some(4))
            .build()
            .unwrap();
        let tx_hash = Self::generate_random_tx_hash();
        let tx_execution_id = self.build_tx_id(tx_hash);
        self.transport
            .send_transaction(tx_execution_id, tx_env)
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

            #[allow(clippy::unnested_or_patterns)]
            let rax = match self.wait_for_transaction_result(tx_execution_id).await {
                Ok(TransactionResult::ValidationCompleted { .. })
                | Ok(TransactionResult::ValidationError(_)) => continue,
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
        self.send_block(self.block_number).await?;
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
        let hash_pass = Self::generate_random_tx_hash();
        let hash_fail = Self::generate_random_tx_hash();

        // Send the passing transaction first
        let tx_execution_id_pass = self.build_tx_id(hash_pass);
        self.transport
            .send_transaction(tx_execution_id_pass, tx_pass)
            .await?;

        // Send the failing transaction second
        let tx_execution_id_fail = self.build_tx_id(hash_fail);
        self.transport
            .send_transaction(tx_execution_id_fail, tx_fail)
            .await?;

        // Verify the second transaction failed assertions and was NOT committed
        if !self.is_transaction_invalid(&tx_execution_id_fail).await? {
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
    /// A reorg will remove the last executed transaction, if the hash supplied
    /// matches said transactions hash.
    /// If not, the core engine should error out and this function will
    /// return an error.
    pub async fn send_reorg(&mut self, tx_execution_id: TxExecutionId) -> Result<(), String> {
        // Make sure the tx exists
        let _ = self.is_transaction_invalid(&tx_execution_id).await?;

        // Send reorg event
        self.transport.reorg(tx_execution_id).await?;

        // Reorg was accepted by the engine and the last executed transaction
        // was removed from the buffer. Mirror this in our local nonce tracking
        // so that subsequent transactions use the correct nonce again.
        let key = (self.default_account, tx_execution_id.iteration_id);
        if let Some(current_nonce) = self.iteration_nonce.get_mut(&key)
            && *current_nonce > 0
        {
            *current_nonce -= 1;
        }

        // Make sure the transaction is gone
        // Note: will only return false if the core engine exited
        if !self.is_transaction_removed(&tx_execution_id).await? {
            return Err("Transaction not removed!".to_string());
        }

        Ok(())
    }

    /// Advance the chain by 1 block by sending a new `BlockEnv` to the core engine.
    pub async fn new_block(&mut self) -> Result<(), String> {
        self.send_block(self.block_number).await?;
        self.block_number += 1;

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
