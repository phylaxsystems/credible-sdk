//! Geth trace provider implementation
//!
//! This provider uses geth's prestateTracer to identify which accounts and storage
//! slots were touched during block execution, then combines existing state from
//! StateReader with POST diff changes to ensure 100% state correctness.
//!
//! ## Why use StateReader + POST diff?
//!
//! Geth's prestateTracer in diff mode only includes fields that CHANGED. If a
//! contract's storage changes but balance/nonce/code don't, those fields are
//! omitted entirely. This leads to incorrect state if we assume missing fields
//! are zero/empty.
//!
//! By reading existing state from StateReader at block N-1 and applying POST
//! diff changes on top, we get the correct state at block N without relying
//! on incomplete trace data.
//!
//! ## Fallback behavior
//!
//! If StateReader is not available or the previous block is not in the buffer,
//! the provider falls back to fetching state via RPC (eth_getBalance, eth_getCode, etc.).

use super::{
    BlockStateUpdateBuilder,
    TraceProvider,
    geth,
};
use alloy::{
    primitives::{
        Address,
        B256,
        U256,
        keccak256,
    },
    rpc::types::BlockNumberOrTag,
};
use alloy_provider::{
    Provider,
    RootProvider,
    ext::DebugApi,
};
use alloy_rpc_types_trace::geth::{
    AccountState as GethAccountState,
    GethDebugBuiltInTracerType,
    GethDebugTracerType,
    GethDebugTracingOptions,
    GethDefaultTracingOptions,
    PreStateConfig,
};
use anyhow::{
    Context,
    Result,
};
use async_trait::async_trait;
use revm::primitives::KECCAK_EMPTY;
use state_store::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
};
use std::{
    collections::{
        HashMap,
        HashSet,
    },
    sync::Arc,
    time::Duration,
};

pub struct GethTraceProvider<R: Reader> {
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
    state_reader: R,
}

impl<R: Reader> GethTraceProvider<R> {
    pub fn new(provider: Arc<RootProvider>, trace_timeout: Duration, reader: R) -> Self {
        Self {
            provider,
            trace_timeout,
            state_reader: reader,
        }
    }

    fn build_trace_options(&self) -> GethDebugTracingOptions {
        let prestate_config = PreStateConfig {
            diff_mode: Some(true),
            disable_code: None,
            disable_storage: None,
        };

        GethDebugTracingOptions {
            config: GethDefaultTracingOptions {
                enable_memory: Some(false),
                disable_stack: Some(true),
                disable_storage: Some(false),
                enable_return_data: Some(false),
                debug: Some(false),
                ..Default::default()
            },
            tracer: Some(GethDebugTracerType::BuiltInTracer(
                GethDebugBuiltInTracerType::PreStateTracer,
            )),
            tracer_config: prestate_config.into(),
            timeout: None,
        }
        .with_timeout(self.trace_timeout)
    }

    /// Build account state by reading from `StateReader` at block N-1 and applying POST diff.
    ///
    /// This ensures we preserve unchanged fields (balance, nonce, code) that are
    /// omitted from the trace when only storage changes.
    fn build_account_state_from_reader(
        &self,
        address_hash: AddressHash,
        storage_slots: &HashSet<B256>,
        prev_block: u64,
        post_state: Option<&GethAccountState>,
    ) -> Result<AccountState> {
        // Read existing account info from previous block
        let existing_account = self
            .state_reader
            .get_account(address_hash, prev_block)
            .ok()
            .flatten();

        // Determine final balance: use POST if present, else use existing
        let balance = post_state
            .and_then(|p| p.balance)
            .or_else(|| existing_account.as_ref().map(|a| a.balance))
            .unwrap_or_default();

        // Determine final nonce: use POST if present, else use existing
        let nonce = post_state
            .and_then(|p| p.nonce)
            .or_else(|| existing_account.as_ref().map(|a| a.nonce))
            .unwrap_or_default();

        // Determine final code: use POST if present, else fetch existing by code_hash
        let (code_hash, code) = if let Some(new_code) = post_state.and_then(|p| p.code.as_ref()) {
            // Code changed in POST
            if new_code.is_empty() {
                (KECCAK_EMPTY, None)
            } else {
                (keccak256(new_code), Some(new_code.clone()))
            }
        } else if let Some(existing) = &existing_account {
            // Code didn't change - fetch from reader using existing code_hash
            if existing.code_hash == KECCAK_EMPTY || existing.code_hash == B256::ZERO {
                (existing.code_hash, None)
            } else {
                let code = self
                    .state_reader
                    .get_code(existing.code_hash, prev_block)
                    .ok()
                    .flatten();
                (existing.code_hash, code)
            }
        } else {
            // New account with no code
            (KECCAK_EMPTY, None)
        };

        // Build storage map: POST values override, else read existing
        let mut storage = HashMap::new();
        for slot in storage_slots {
            let slot_hash = keccak256(slot.0);

            // Check if POST has this slot
            if let Some(new_value) = post_state.and_then(|p| p.storage.get(slot)) {
                // Slot changed in POST - convert B256 to U256
                storage.insert(slot_hash, U256::from_be_bytes(new_value.0));
            } else {
                // Slot not in POST - read existing value from StateReader
                if let Ok(Some(value)) =
                    self.state_reader
                        .get_storage(address_hash, slot_hash, prev_block)
                {
                    storage.insert(slot_hash, value);
                }
                // If not found in StateReader, slot is zero (new slot that was read but not written)
            }
        }

        Ok(AccountState {
            address_hash,
            balance,
            nonce,
            code_hash,
            code,
            storage,
            deleted: false,
        })
    }

    /// Fetch account state from RPC at the specified block.
    ///
    /// This is the fallback when `StateReader` is not available or the previous
    /// block is not in the buffer.
    async fn fetch_account_state_from_rpc(
        &self,
        address: Address,
        storage_slots: &HashSet<B256>,
        block_number: u64,
    ) -> Result<AccountState> {
        let block_id = alloy::rpc::types::BlockId::number(block_number);

        let (balance, nonce, code) = tokio::try_join!(
            async {
                self.provider
                    .get_balance(address)
                    .block_id(block_id)
                    .await
                    .context("failed to fetch balance")
            },
            async {
                self.provider
                    .get_transaction_count(address)
                    .block_id(block_id)
                    .await
                    .context("failed to fetch nonce")
            },
            async {
                self.provider
                    .get_code_at(address)
                    .block_id(block_id)
                    .await
                    .context("failed to fetch code")
            },
        )?;

        let mut storage = HashMap::new();
        for slot in storage_slots {
            let value = self
                .provider
                .get_storage_at(address, (*slot).into())
                .block_id(block_id)
                .await
                .with_context(|| format!("failed to fetch storage slot {slot}"))?;

            let slot_hash = keccak256(slot.0);
            storage.insert(slot_hash, value);
        }

        let (code_hash, code_bytes) = if code.is_empty() {
            (KECCAK_EMPTY, None)
        } else {
            (keccak256(&code), Some(code))
        };

        Ok(AccountState {
            address_hash: AddressHash::from(address),
            balance,
            nonce,
            code_hash,
            code: code_bytes,
            storage,
            deleted: false,
        })
    }

    /// Check if we can use `StateReader` for the given block.
    ///
    /// Returns the previous block number if available in the buffer.
    fn can_use_state_reader(&self, block_number: u64) -> Option<u64> {
        // Need previous block to exist
        if block_number == 0 {
            return None;
        }

        let prev_block = block_number - 1;

        // Check if previous block is available in the buffer
        if self
            .state_reader
            .is_block_available(prev_block)
            .unwrap_or(false)
        {
            Some(prev_block)
        } else {
            None
        }
    }
}

#[async_trait]
impl<R: Reader + Send + Sync + 'static> TraceProvider for GethTraceProvider<R> {
    async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate> {
        let block_id = BlockNumberOrTag::Number(block_number);

        let block = self
            .provider
            .get_block_by_number(block_id)
            .await?
            .with_context(|| format!("block {block_number} not found"))?;

        let block_hash = block.header.hash;
        let state_root = block.header.state_root;

        let trace_options = self.build_trace_options();
        let traces = self
            .provider
            .debug_trace_block_by_number(block_id, trace_options)
            .await
            .with_context(|| format!("failed to trace block {block_number}"))?;

        let touched_addresses = geth::extract_touched_addresses(&traces);
        let storage_by_address = geth::extract_touched_storage(&traces);
        let post_state = geth::extract_post_state(&traces);

        let mut accounts = Vec::with_capacity(touched_addresses.len());

        // Check if we can use StateReader
        let use_state_reader = self.can_use_state_reader(block_number);

        for address in touched_addresses {
            let address_hash = AddressHash::from(address);
            let storage_slots = storage_by_address
                .get(&address)
                .cloned()
                .unwrap_or_default();
            let addr_post_state = post_state.get(&address);

            let account_state = if let Some(prev_block) = use_state_reader {
                // Use StateReader + POST diff
                self.build_account_state_from_reader(
                    address_hash,
                    &storage_slots,
                    prev_block,
                    addr_post_state,
                )?
            } else {
                // Fallback to RPC
                self.fetch_account_state_from_rpc(address, &storage_slots, block_number)
                    .await
                    .with_context(|| format!("failed to fetch state for {address}"))?
            };

            accounts.push(account_state);
        }

        Ok(BlockStateUpdateBuilder::from_accounts(
            block_number,
            block_hash,
            state_root,
            accounts,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_rpc_types_trace::geth::AccountState as GethAccountState;
    use std::collections::BTreeMap;

    mod build_account_state_tests {
        use super::*;
        use alloy::primitives::{
            Address,
            Bytes,
        };
        use state_store::mdbx::{
            StateReader,
            StateWriter,
            common::CircularBufferConfig,
        };
        use tempfile::TempDir;

        fn setup_test_reader() -> (TempDir, StateReader) {
            let tmp = TempDir::new().unwrap();
            let config = CircularBufferConfig::new(10).unwrap();
            let path = tmp.path().join("state");

            // First create the database with a writer
            {
                let _writer = StateWriter::new(&path, config.clone()).unwrap();
                // Writer drops here, releasing the lock
            }

            // Now open the reader
            let reader = StateReader::new(&path, config).unwrap();
            (tmp, reader)
        }

        fn slot(n: u8) -> B256 {
            let mut bytes = [0u8; 32];
            bytes[31] = n;
            B256::from(bytes)
        }

        fn value_b256(n: u64) -> B256 {
            B256::from(U256::from(n))
        }

        // Test: POST has balance, StateReader has nothing (new account)
        #[test]
        fn test_new_account_from_post_only() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            let post_state = GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(0),
                code: None,
                storage: BTreeMap::new(),
            };

            let result = geth_provider
                .build_account_state_from_reader(
                    address_hash,
                    &storage_slots,
                    0, // prev_block (doesn't exist in empty DB)
                    Some(&post_state),
                )
                .unwrap();

            // POST values should be used
            assert_eq!(result.balance, U256::from(1000));
            assert_eq!(result.nonce, 0);
            // No code in POST, no existing account = KECCAK_EMPTY
            assert_eq!(result.code_hash, KECCAK_EMPTY);
            assert!(result.code.is_none());
        }

        // Test: POST has partial update (balance only), rest from StateReader
        #[test]
        fn test_partial_update_balance_only() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            // POST only has balance (nonce/code unchanged - omitted)
            let post_state = GethAccountState {
                balance: Some(U256::from(500)),
                nonce: None, // Omitted - unchanged
                code: None,  // Omitted - unchanged
                storage: BTreeMap::new(),
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            // Balance from POST
            assert_eq!(result.balance, U256::from(500));
            // Nonce defaults to 0 (no existing account in empty DB)
            assert_eq!(result.nonce, 0);
        }

        // Test: POST has storage update only
        #[test]
        fn test_storage_update_only() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);

            let mut storage_slots = HashSet::new();
            storage_slots.insert(slot(0));

            let mut post_storage = BTreeMap::new();
            post_storage.insert(slot(0), value_b256(42));

            let post_state = GethAccountState {
                balance: None, // Omitted
                nonce: None,   // Omitted
                code: None,    // Omitted
                storage: post_storage,
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            // Balance/nonce default (no existing account)
            assert_eq!(result.balance, U256::ZERO);
            assert_eq!(result.nonce, 0);

            // Storage should have the POST value (hashed slot key)
            let slot_hash = keccak256(slot(0).0);
            assert_eq!(result.storage.get(&slot_hash), Some(&U256::from(42)));
        }

        // Test: POST has new code (contract deployment)
        #[test]
        fn test_contract_deployment_with_code() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            let bytecode = Bytes::from(vec![0x60, 0x80, 0x60, 0x40, 0x52]);

            let post_state = GethAccountState {
                balance: Some(U256::ZERO),
                nonce: Some(1),
                code: Some(bytecode.clone()),
                storage: BTreeMap::new(),
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            assert_eq!(result.nonce, 1);
            assert_eq!(result.code_hash, keccak256(&bytecode));
            assert_eq!(result.code, Some(bytecode));
        }

        // Test: POST has empty code (edge case)
        #[test]
        fn test_post_with_empty_code() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            let post_state = GethAccountState {
                balance: Some(U256::from(100)),
                nonce: Some(0),
                code: Some(Bytes::new()), // Empty code
                storage: BTreeMap::new(),
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            // Empty code should result in KECCAK_EMPTY
            assert_eq!(result.code_hash, KECCAK_EMPTY);
            assert!(result.code.is_none());
        }

        // Test: No POST state (account only in PRE - read but not modified)
        #[test]
        fn test_no_post_state_read_only_account() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            // No POST state - account was read but not modified
            let result = geth_provider
                .build_account_state_from_reader(
                    address_hash,
                    &storage_slots,
                    0,
                    None, // No POST state
                )
                .unwrap();

            // All values should come from StateReader (empty DB = defaults)
            assert_eq!(result.balance, U256::ZERO);
            assert_eq!(result.nonce, 0);
            assert_eq!(result.code_hash, KECCAK_EMPTY);
        }

        // Test: Storage slot in touched set but not in POST (read but unchanged)
        #[test]
        fn test_storage_slot_touched_but_not_in_post() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);

            // Slot was touched (read) but not modified
            let mut storage_slots = HashSet::new();
            storage_slots.insert(slot(0));

            let post_state = GethAccountState {
                balance: Some(U256::from(100)),
                nonce: None,
                code: None,
                storage: BTreeMap::new(), // Slot not in POST - unchanged
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            // Slot not in result storage (not in StateReader, not in POST)
            let slot_hash = keccak256(slot(0).0);
            assert!(!result.storage.contains_key(&slot_hash));
        }

        // Test: Multiple storage slots, some changed, some not
        #[test]
        fn test_mixed_storage_slots() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);

            let mut storage_slots = HashSet::new();
            storage_slots.insert(slot(0)); // Changed in POST
            storage_slots.insert(slot(1)); // Not in POST (unchanged)
            storage_slots.insert(slot(2)); // Changed in POST

            let mut post_storage = BTreeMap::new();
            post_storage.insert(slot(0), value_b256(100));
            post_storage.insert(slot(2), value_b256(200));
            // slot(1) not in POST - value should come from StateReader

            let post_state = GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            let slot0_hash = keccak256(slot(0).0);
            let slot1_hash = keccak256(slot(1).0);
            let slot2_hash = keccak256(slot(2).0);

            // POST values used for changed slots
            assert_eq!(result.storage.get(&slot0_hash), Some(&U256::from(100)));
            assert_eq!(result.storage.get(&slot2_hash), Some(&U256::from(200)));
            // Unchanged slot not in result (not in StateReader)
            assert!(!result.storage.contains_key(&slot1_hash));
        }

        // Test: Zero balance in POST
        #[test]
        fn test_zero_balance_in_post() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);
            let storage_slots = HashSet::new();

            let post_state = GethAccountState {
                balance: Some(U256::ZERO), // Explicitly zero
                nonce: Some(5),
                code: None,
                storage: BTreeMap::new(),
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            // Zero should be used, not default
            assert_eq!(result.balance, U256::ZERO);
            assert_eq!(result.nonce, 5);
        }

        // Test: Storage slot set to zero in POST
        #[test]
        fn test_storage_slot_zeroed_in_post() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);

            let mut storage_slots = HashSet::new();
            storage_slots.insert(slot(0));

            let mut post_storage = BTreeMap::new();
            post_storage.insert(slot(0), B256::ZERO); // Explicitly zeroed

            let post_state = GethAccountState {
                balance: None,
                nonce: None,
                code: None,
                storage: post_storage,
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            let slot_hash = keccak256(slot(0).0);
            // Zero value should be recorded
            assert_eq!(result.storage.get(&slot_hash), Some(&U256::ZERO));
        }

        // Test: Large number of storage slots
        #[test]
        fn test_many_storage_slots() {
            let (_tmp, reader) = setup_test_reader();
            let provider = Arc::new(RootProvider::new_http(
                "http://localhost:8545".parse().unwrap(),
            ));
            let geth_provider =
                GethTraceProvider::<StateReader>::new(provider, Duration::from_secs(30), reader);

            let address = Address::from([0x42u8; 20]);
            let address_hash = AddressHash::from(address);

            let mut storage_slots = HashSet::new();
            let mut post_storage = BTreeMap::new();

            for i in 0u8..100 {
                storage_slots.insert(slot(i));
                post_storage.insert(slot(i), value_b256(u64::from(i) * 100));
            }

            let post_state = GethAccountState {
                balance: Some(U256::from(1000)),
                nonce: Some(1),
                code: None,
                storage: post_storage,
            };

            let result = geth_provider
                .build_account_state_from_reader(address_hash, &storage_slots, 0, Some(&post_state))
                .unwrap();

            assert_eq!(result.storage.len(), 100);
            for i in 0u8..100 {
                let slot_hash = keccak256(slot(i).0);
                assert_eq!(
                    result.storage.get(&slot_hash),
                    Some(&U256::from(u64::from(i) * 100))
                );
            }
        }
    }
}
