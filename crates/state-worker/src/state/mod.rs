//! Helpers that collapse per-transaction debug traces into per-account commits.
//!
//! ## Account Deletion Semantics
//!
//! Per EIP-1052 (EXTCODEHASH), there are three distinct account states:
//! - **Non-existent**: Never created or fully deleted → `code_hash = 0x0`
//! - **Exists, no code**: EOA or empty contract → `code_hash = keccak256("")`
//! - **Exists, has code**: Contract with bytecode → `code_hash = keccak256(code)`
//!
//! ### Pre-Cancun Behavior
//! SELFDESTRUCT always fully deletes the account (balance, nonce, code, storage).
//! The account becomes non-existent → `code_hash = 0x0`
//!
//! ### Post-Cancun Behavior (EIP-6780)
//! - **Same-tx SELFDESTRUCT**: Full deletion (same as pre-Cancun) → `code_hash = 0x0`
//! - **Different-tx SELFDESTRUCT**: Account persists, only balance zeroed.
//!   The trace won't show this as a deletion, just a balance change.
//!
//! The trace provider (Geth/Parity) already implements EIP-6780 correctly,
//! so we just need to interpret the traces accurately.
//!
//! ## State Fetching Strategy
//!
//! Geth's prestateTracer in diff mode only includes fields that CHANGED in each
//! transaction. This means if a contract's storage changes but balance/nonce/code
//! don't change, those fields are OMITTED from the trace entirely.
//!
//! To ensure 100% state correctness, we:
//! 1. Use traces to identify WHICH accounts and storage slots were touched
//! 2. Read existing state from StateReader at block N-1
//! 3. Apply POST diff values on top of existing state
//! 4. Fall back to RPC if `StateReader` unavailable
//!
//! ## Parity Provider Note
//!
//! The Parity provider still uses the old AccountSnapshot approach. It may have
//! the same bug if Parity's stateDiff traces omit unchanged fields. Consider
//! updating it to use the same fetch-from-chain approach as Geth.

pub mod geth;
pub mod geth_provider;
pub mod parity;
pub mod parity_provider;
#[cfg(test)]
mod tests;

use alloy::primitives::{
    B256,
    Bytes,
    U256,
    keccak256,
};
use alloy_provider::RootProvider;
use alloy_rpc_types_trace::parity::TraceResultsWithTransactionHash;
use anyhow::Result;
use async_trait::async_trait;
use revm::primitives::KECCAK_EMPTY;
use state_store::{
    AccountState,
    AddressHash,
    BlockStateUpdate,
    Reader,
};
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

/// Trait for fetching block state from different trace providers
#[async_trait]
pub trait TraceProvider: Send + Sync {
    async fn fetch_block_state(&self, block_number: u64) -> Result<BlockStateUpdate>;
}

/// `AccountSnapshot` is used by the Parity provider to accumulate state changes.
///
/// WARNING: This approach has a known bug when traces omit unchanged fields.
/// The Geth provider now uses a different approach (fetching state from chain).
/// Consider updating Parity provider to use the same approach.
#[derive(Default)]
pub(crate) struct AccountSnapshot {
    pub(crate) balance: Option<U256>,
    pub(crate) nonce: Option<u64>,
    pub(crate) code: Option<Bytes>,
    pub(crate) storage_updates: HashMap<B256, U256>,
    pub(crate) touched: bool,
}

impl AccountSnapshot {
    pub(crate) fn finalize(mut self, address: AddressHash) -> Option<AccountState> {
        if !self.touched && self.storage_updates.is_empty() {
            return None;
        }

        let code_bytes = self.code.take().unwrap_or_default();

        // WARNING: This assumes that if code is None/empty, the account has no code.
        // This is INCORRECT if the trace simply omitted the code because it didn't change.
        // The Geth provider now fetches state from chain to avoid this bug.
        //
        // Existing account without code: keccak256("") per EIP-1052
        // Existing account with code: keccak256(code)
        let code_hash = if code_bytes.is_empty() {
            KECCAK_EMPTY
        } else {
            keccak256(&code_bytes)
        };

        let code = if code_bytes.is_empty() {
            None
        } else {
            Some(code_bytes)
        };

        Some(AccountState {
            address_hash: address,
            balance: self.balance.unwrap_or_default(),
            nonce: self.nonce.unwrap_or_default(),
            code_hash,
            code,
            storage: self.storage_updates,
            deleted: false,
        })
    }
}

/// Builder for `BlockStateUpdate`
pub struct BlockStateUpdateBuilder;

impl BlockStateUpdateBuilder {
    pub fn from_parity_traces(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        traces: Vec<TraceResultsWithTransactionHash>,
    ) -> BlockStateUpdate {
        let accounts = parity::process_parity_traces(traces);
        BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        }
    }

    pub fn from_accounts(
        block_number: u64,
        block_hash: B256,
        state_root: B256,
        accounts: Vec<AccountState>,
    ) -> BlockStateUpdate {
        BlockStateUpdate {
            block_number,
            block_hash,
            state_root,
            accounts,
        }
    }
}

/// Create a trace provider based on the provider type.
///
/// # Arguments
///
/// * `provider_type` - The type of trace provider (Geth or Parity)
/// * `provider` - The RPC provider for blockchain access
/// * `trace_timeout` - Timeout for trace operations
/// * `state_reader` - Optional `StateReader` for reading existing state from local DB.
///   When provided, the Geth provider will read state at block N-1 and apply
///   POST diff changes instead of fetching everything from RPC.
pub fn create_trace_provider<R: Reader + Send + Sync + 'static>(
    provider_type: crate::cli::ProviderType,
    provider: Arc<RootProvider>,
    trace_timeout: Duration,
    state_reader: R,
) -> Box<dyn TraceProvider> {
    match provider_type {
        crate::cli::ProviderType::Geth => {
            let geth_provider =
                geth_provider::GethTraceProvider::new(provider, trace_timeout, state_reader);
            Box::new(geth_provider)
        }
        crate::cli::ProviderType::Parity => {
            // Note: Parity provider doesn't support StateReader yet.
            // It still uses the AccountSnapshot approach which has known bugs.
            Box::new(parity_provider::ParityTraceProvider::new(provider))
        }
    }
}

#[cfg(test)]
mod account_snapshot_tests {
    use super::*;
    use alloy::primitives::{
        B256,
        Bytes,
        U256,
    };
    use revm::primitives::KECCAK_EMPTY;

    fn test_address() -> AddressHash {
        AddressHash::new([0u8; 20])
    }

    #[test]
    fn test_untouched_snapshot_returns_none() {
        let snapshot = AccountSnapshot {
            balance: None,
            nonce: None,
            code: None,
            storage_updates: HashMap::new(),
            touched: false,
        };

        assert!(snapshot.finalize(test_address()).is_none());
    }

    #[test]
    fn test_touched_snapshot_returns_account() {
        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: Some(5),
            code: None,
            storage_updates: HashMap::new(),
            touched: true,
        };

        let account = snapshot.finalize(test_address()).unwrap();
        assert_eq!(account.balance, U256::from(100));
        assert_eq!(account.nonce, 5);
    }

    #[test]
    fn test_storage_only_returns_account() {
        let mut storage = HashMap::new();
        storage.insert(B256::from([0x01u8; 32]), U256::from(100));

        let snapshot = AccountSnapshot {
            balance: None,
            nonce: None,
            code: None,
            storage_updates: storage,
            touched: false,
        };

        let account = snapshot.finalize(test_address()).unwrap();
        assert_eq!(account.storage.len(), 1);
    }

    #[test]
    fn test_existing_account_without_code_has_keccak_empty() {
        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: Some(1),
            code: None,
            storage_updates: HashMap::new(),
            touched: true,
        };

        let state = snapshot.finalize(test_address()).unwrap();
        assert_eq!(state.code_hash, KECCAK_EMPTY);
        assert!(!state.deleted);
    }

    #[test]
    fn test_existing_contract_has_code_hash() {
        let code = vec![0x60, 0x80, 0x60, 0x40];
        let expected_hash = keccak256(&code);

        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: Some(1),
            code: Some(Bytes::from(code)),
            storage_updates: HashMap::new(),
            touched: true,
        };

        let state = snapshot.finalize(test_address()).unwrap();
        assert_eq!(state.code_hash, expected_hash);
        assert!(!state.deleted);
    }

    #[test]
    fn test_missing_balance_defaults_to_zero() {
        let snapshot = AccountSnapshot {
            balance: None,
            nonce: Some(1),
            code: None,
            storage_updates: HashMap::new(),
            touched: true,
        };

        let account = snapshot.finalize(test_address()).unwrap();
        assert_eq!(account.balance, U256::ZERO);
    }

    #[test]
    fn test_missing_nonce_defaults_to_zero() {
        let snapshot = AccountSnapshot {
            balance: Some(U256::from(100)),
            nonce: None,
            code: None,
            storage_updates: HashMap::new(),
            touched: true,
        };

        let account = snapshot.finalize(test_address()).unwrap();
        assert_eq!(account.nonce, 0);
    }

    #[test]
    fn test_bug_demonstration_code_hash_when_code_omitted() {
        let mut storage = HashMap::new();
        storage.insert(B256::from([0x01u8; 32]), U256::from(100));

        let snapshot = AccountSnapshot {
            balance: None,
            nonce: None,
            code: None,
            storage_updates: storage,
            touched: true,
        };

        let account = snapshot.finalize(test_address()).unwrap();
        assert_eq!(account.code_hash, KECCAK_EMPTY);
        assert_eq!(account.balance, U256::ZERO);
        assert_eq!(account.nonce, 0);
    }
}

#[cfg(test)]
mod builder_tests {
    use super::*;
    use alloy::primitives::{
        B256,
        Bytes,
        U256,
    };
    use revm::primitives::KECCAK_EMPTY;

    fn test_address() -> AddressHash {
        AddressHash::new([0u8; 20])
    }

    #[test]
    fn test_from_accounts_empty() {
        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![],
        );

        assert_eq!(update.block_number, 100);
        assert_eq!(update.block_hash, B256::ZERO);
        assert_eq!(update.state_root, B256::from([0x11u8; 32]));
        assert!(update.accounts.is_empty());
    }

    #[test]
    fn test_from_accounts_single() {
        let account = AccountState {
            address_hash: test_address(),
            balance: U256::from(100),
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        };

        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![account],
        );

        assert_eq!(update.accounts.len(), 1);
        assert_eq!(update.accounts[0].balance, U256::from(100));
        assert_eq!(update.accounts[0].nonce, 5);
    }

    #[test]
    fn test_from_accounts_multiple() {
        let account1 = AccountState {
            address_hash: AddressHash::new([0x01u8; 20]),
            balance: U256::from(100),
            nonce: 5,
            code_hash: KECCAK_EMPTY,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        };

        let account2 = AccountState {
            address_hash: AddressHash::new([0x02u8; 20]),
            balance: U256::from(200),
            nonce: 10,
            code_hash: KECCAK_EMPTY,
            code: None,
            storage: HashMap::new(),
            deleted: false,
        };

        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![account1, account2],
        );

        assert_eq!(update.accounts.len(), 2);
    }

    #[test]
    fn test_from_accounts_with_code() {
        let code = vec![0x60, 0x80, 0x60, 0x40];
        let code_hash = keccak256(&code);

        let account = AccountState {
            address_hash: test_address(),
            balance: U256::from(100),
            nonce: 1,
            code_hash,
            code: Some(Bytes::from(code.clone())),
            storage: HashMap::new(),
            deleted: false,
        };

        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![account],
        );

        assert_eq!(update.accounts[0].code_hash, code_hash);
        assert_eq!(update.accounts[0].code, Some(Bytes::from(code)));
    }

    #[test]
    fn test_from_accounts_with_storage() {
        let mut storage = HashMap::new();
        storage.insert(B256::from([0x01u8; 32]), U256::from(0xAABBCC));
        storage.insert(B256::from([0x02u8; 32]), U256::from(0xDDEEFF));

        let account = AccountState {
            address_hash: test_address(),
            balance: U256::from(100),
            nonce: 1,
            code_hash: KECCAK_EMPTY,
            code: None,
            storage: storage.clone(),
            deleted: false,
        };

        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![account],
        );

        assert_eq!(update.accounts[0].storage.len(), 2);
        assert_eq!(
            update.accounts[0].storage.get(&B256::from([0x01u8; 32])),
            Some(&U256::from(0xAABBCC))
        );
    }

    #[test]
    fn test_from_accounts_preserves_deleted_flag() {
        let account = AccountState {
            address_hash: test_address(),
            balance: U256::ZERO,
            nonce: 0,
            code_hash: B256::ZERO,
            code: None,
            storage: HashMap::new(),
            deleted: true,
        };

        let update = BlockStateUpdateBuilder::from_accounts(
            100,
            B256::ZERO,
            B256::from([0x11u8; 32]),
            vec![account],
        );

        assert!(update.accounts[0].deleted);
    }
}
