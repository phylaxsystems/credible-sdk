use crate::engine::TransactionResult;
use assertion_executor::{
    primitives::{
        AccountInfo,
        Address,
        B256,
        FixedBytes,
        TxEnv,
        TxKind,
        U256,
    },
    store::{
        AssertionState,
        AssertionStore,
    },
    test_utils::{
        bytecode,
        counter_call,
    },
};
use revm::{
    database::{
        CacheDB,
        EmptyDBTyped,
    },
    primitives::{
        Bytes,
        address,
    },
};
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::task::JoinHandle;
use tracing::info;

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
    /// Container type that holds the transport and impls `TestTransport`
    transport: T,
}

impl<T: TestTransport> LocalInstance<T> {
    /// Create a new local instance with mock transport
    pub async fn new() -> Result<LocalInstance<T>, String> {
        T::new().await
    }

    /// Internal constructor for creating LocalInstance with all fields
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_internal(
        db: Arc<CacheDB<EmptyDBTyped<TestDbError>>>,
        assertion_store: Arc<AssertionStore>,
        transport_handle: Option<JoinHandle<()>>,
        engine_handle: Option<JoinHandle<()>>,
        block_number: u64,
        transaction_results: Arc<crate::TransactionsState>,
        default_account: Address,
        current_nonce: u64,
        transport: T,
    ) -> Self {
        Self {
            db,
            assertion_store,
            transport_handle,
            engine_handle,
            block_number,
            transaction_results,
            default_account,
            current_nonce,
            transport,
        }
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
